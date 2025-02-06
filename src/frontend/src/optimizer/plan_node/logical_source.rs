// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, Field};
use risingwave_connector::source::DataType;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::GeneratedColumnDesc;
use risingwave_sqlparser::ast::AsOf;

use super::generic::{GenericPlanRef, SourceNodeKind};
use super::stream_watermark_filter::StreamWatermarkFilter;
use super::utils::{childless_record, Distill};
use super::{
    generic, BatchProject, BatchSource, ColPrunable, ExprRewritable, Logical, LogicalFilter,
    LogicalProject, PlanBase, PlanRef, PredicatePushdown, StreamProject, StreamRowIdGen,
    StreamSource, StreamSourceScan, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::stream_fs_fetch::StreamFsFetch;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamDedup,
    ToStreamContext,
};
use crate::optimizer::property::Distribution::HashShard;
use crate::optimizer::property::{
    Distribution, MonotonicityMap, Order, RequiredDist, WatermarkColumns,
};
use crate::utils::{ColIndexMapping, Condition, IndexRewriter};

/// `LogicalSource` returns contents of a table or other equivalent object
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalSource {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,

    /// Expressions to output. This field presents and will be turned to a `Project` when
    /// converting to a physical plan, only if there are generated columns.
    pub(crate) output_exprs: Option<Vec<ExprImpl>>,
    /// When there are generated columns, the `StreamRowIdGen`'s `row_id_index` is different from
    /// the one in `core`. So we store the one in `output_exprs` here.
    pub(crate) output_row_id_index: Option<usize>,
}

impl LogicalSource {
    pub fn new(
        source_catalog: Option<Rc<SourceCatalog>>,
        column_catalog: Vec<ColumnCatalog>,
        row_id_index: Option<usize>,
        kind: SourceNodeKind,
        ctx: OptimizerContextRef,
        as_of: Option<AsOf>,
    ) -> Result<Self> {
        // XXX: should we reorder the columns?
        // The order may be strange if the schema is changed, e.g., [foo:Varchar, _rw_kafka_timestamp:Timestamptz, _row_id:Serial, bar:Int32]
        // related: https://github.com/risingwavelabs/risingwave/issues/16486
        // The order does not matter much. The columns field is essentially a map indexed by the column id.
        // It will affect what users will see in `SELECT *`.
        // But not sure if we rely on the position of hidden column like `_row_id` somewhere. For `projected_row_id` we do so...
        let core = generic::Source {
            catalog: source_catalog,
            column_catalog,
            row_id_index,
            kind,
            ctx,
            as_of,
        };

        if core.as_of.is_some() && !core.support_time_travel() {
            bail!("Time travel is not supported for the source")
        }

        let base = PlanBase::new_logical_with_core(&core);

        let output_exprs = Self::derive_output_exprs_from_generated_columns(&core.column_catalog)?;
        let (core, output_row_id_index) = core.exclude_generated_columns();

        Ok(LogicalSource {
            base,
            core,
            output_exprs,
            output_row_id_index,
        })
    }

    pub fn with_catalog(
        source_catalog: Rc<SourceCatalog>,
        kind: SourceNodeKind,
        ctx: OptimizerContextRef,
        as_of: Option<AsOf>,
    ) -> Result<Self> {
        let column_catalogs = source_catalog.columns.clone();
        let row_id_index = source_catalog.row_id_index;
        if !source_catalog.append_only {
            assert!(row_id_index.is_none());
        }

        Self::new(
            Some(source_catalog),
            column_catalogs,
            row_id_index,
            kind,
            ctx,
            as_of,
        )
    }

    /// If there are no generated columns, returns `None`.
    ///
    /// Otherwise, the returned expressions correspond to all columns.
    /// Non-generated columns are represented by `InputRef`.
    pub fn derive_output_exprs_from_generated_columns(
        columns: &[ColumnCatalog],
    ) -> Result<Option<Vec<ExprImpl>>> {
        if !columns.iter().any(|c| c.is_generated()) {
            return Ok(None);
        }

        let col_mapping = {
            let mut mapping = vec![None; columns.len()];
            let mut cur = 0;
            for (idx, column) in columns.iter().enumerate() {
                if !column.is_generated() {
                    mapping[idx] = Some(cur);
                    cur += 1;
                } else {
                    mapping[idx] = None;
                }
            }
            ColIndexMapping::new(mapping, columns.len())
        };

        let mut rewriter = IndexRewriter::new(col_mapping);
        let mut exprs = Vec::with_capacity(columns.len());
        let mut cur = 0;
        for column in columns {
            let column_desc = &column.column_desc;
            let ret_data_type = column_desc.data_type.clone();

            if let Some(GeneratedOrDefaultColumn::GeneratedColumn(generated_column)) =
                &column_desc.generated_or_default_column
            {
                let GeneratedColumnDesc { expr } = generated_column;
                // TODO(yuhao): avoid this `from_expr_proto`.
                let proj_expr =
                    rewriter.rewrite_expr(ExprImpl::from_expr_proto(expr.as_ref().unwrap())?);
                let casted_expr = proj_expr.cast_assign(ret_data_type)?;
                exprs.push(casted_expr);
            } else {
                let input_ref = InputRef {
                    data_type: ret_data_type,
                    index: cur,
                };
                cur += 1;
                exprs.push(ExprImpl::InputRef(Box::new(input_ref)));
            }
        }

        Ok(Some(exprs))
    }

    /// `StreamSource` (list) -> shuffle -> `StreamDedup`
    fn create_fs_list_plan(core: generic::Source) -> Result<PlanRef> {
        let logical_source = generic::Source {
            column_catalog: vec![
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field {
                            name: "filename".to_owned(),
                            data_type: DataType::Varchar,
                            sub_fields: vec![],
                            type_name: "".to_owned(),
                        },
                        0,
                    ),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field {
                            name: "last_edit_time".to_owned(),
                            data_type: DataType::Timestamptz,
                            sub_fields: vec![],
                            type_name: "".to_owned(),
                        },
                        1,
                    ),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field {
                            name: "file_size".to_owned(),
                            data_type: DataType::Int64,
                            sub_fields: vec![],
                            type_name: "".to_owned(),
                        },
                        0,
                    ),
                    is_hidden: false,
                },
            ],
            row_id_index: None,
            ..core
        };
        let mut new_s3_plan: PlanRef = StreamSource {
            base: PlanBase::new_stream_with_core(
                &logical_source,
                Distribution::Single,
                true, // `list` will keep listing all objects, it must be append-only
                false,
                WatermarkColumns::new(),
                MonotonicityMap::new(),
            ),
            core: logical_source,
        }
        .into();
        new_s3_plan = RequiredDist::shard_by_key(3, &[0])
            .enforce_if_not_satisfies(new_s3_plan, &Order::any())?;
        new_s3_plan = StreamDedup::new(generic::Dedup {
            input: new_s3_plan,
            dedup_cols: vec![0],
        })
        .into();

        Ok(new_s3_plan)
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn clone_with_column_catalog(&self, column_catalog: Vec<ColumnCatalog>) -> Result<Self> {
        let row_id_index = self.core.row_id_index;
        let kind = self.core.kind.clone();
        let ctx = self.core.ctx.clone();
        let as_of = self.core.as_of.clone();
        Self::new(
            self.source_catalog(),
            column_catalog,
            row_id_index,
            kind,
            ctx,
            as_of,
        )
    }
}

impl_plan_tree_node_for_leaf! {LogicalSource}
impl Distill for LogicalSource {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            let mut fields = vec![
                ("source", src),
                ("columns", column_names_pretty(self.schema())),
            ];
            if let Some(as_of) = &self.core.as_of {
                fields.push(("as_of", Pretty::debug(as_of)));
            }
            fields
        } else {
            vec![]
        };
        childless_record("LogicalSource", fields)
    }
}

impl ColPrunable for LogicalSource {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl ExprRewritable for LogicalSource {
    fn has_rewritable_expr(&self) -> bool {
        self.output_exprs.is_some()
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut output_exprs = self.output_exprs.clone();

        for expr in output_exprs.iter_mut().flatten() {
            *expr = r.rewrite_expr(expr.clone());
        }

        Self {
            output_exprs,
            ..self.clone()
        }
        .into()
    }
}

impl ExprVisitable for LogicalSource {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.output_exprs
            .iter()
            .flatten()
            .for_each(|e| v.visit_expr(e));
    }
}

impl PredicatePushdown for LogicalSource {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalSource {
    fn to_batch(&self) -> Result<PlanRef> {
        assert!(
            !self.core.is_kafka_connector(),
            "LogicalSource with a kafka property should be converted to LogicalKafkaScan"
        );
        assert!(
            !self.core.is_iceberg_connector(),
            "LogicalSource with a iceberg property should be converted to LogicalIcebergScan"
        );
        let mut plan: PlanRef = BatchSource::new(self.core.clone()).into();

        if let Some(exprs) = &self.output_exprs {
            let logical_project = generic::Project::new(exprs.to_vec(), plan);
            plan = BatchProject::new(logical_project).into();
        }

        Ok(plan)
    }
}

impl ToStream for LogicalSource {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let mut plan: PlanRef;

        match self.core.kind {
            SourceNodeKind::CreateTable | SourceNodeKind::CreateSharedSource => {
                // Note: for create table, row_id and generated columns is created in plan_root.gen_table_plan.
                // for shared source, row_id and generated columns is created after SourceBackfill node.
                if self.core.is_new_fs_connector() {
                    plan = Self::create_fs_list_plan(self.core.clone())?;
                    plan = StreamFsFetch::new(plan, self.core.clone()).into();
                } else {
                    plan = StreamSource::new(self.core.clone()).into()
                }
            }
            SourceNodeKind::CreateMViewOrBatch => {
                // Create MV on source.
                // We only check streaming_use_shared_source is true when `CREATE SOURCE`.
                // The value does not affect the behavior of `CREATE MATERIALIZED VIEW` here.
                let use_shared_source = self.source_catalog().is_some_and(|c| c.info.is_shared());
                if use_shared_source {
                    plan = StreamSourceScan::new(self.core.clone()).into();
                } else {
                    // non-shared source
                    if self.core.is_new_fs_connector() {
                        plan = Self::create_fs_list_plan(self.core.clone())?;
                        plan = StreamFsFetch::new(plan, self.core.clone()).into();
                    } else {
                        plan = StreamSource::new(self.core.clone()).into()
                    }
                }

                if let Some(exprs) = &self.output_exprs {
                    let logical_project = generic::Project::new(exprs.to_vec(), plan);
                    plan = StreamProject::new(logical_project).into();
                }

                if let Some(catalog) = self.source_catalog()
                    && !catalog.watermark_descs.is_empty()
                {
                    plan = StreamWatermarkFilter::new(plan, catalog.watermark_descs.clone()).into();
                }

                if let Some(row_id_index) = self.output_row_id_index {
                    plan = StreamRowIdGen::new_with_dist(
                        plan,
                        row_id_index,
                        HashShard(vec![row_id_index]),
                    )
                    .into();
                }
            }
        }
        Ok(plan)
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((
            self.clone().into(),
            ColIndexMapping::identity(self.schema().len()),
        ))
    }
}
