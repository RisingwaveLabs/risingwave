// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::ops::Bound;
use std::ops::Bound::Unbounded;
use std::rc::Rc;
use tonic::IntoRequest;

use risingwave_common::catalog::{ColumnDesc, Schema};
use risingwave_common::error::Result;
use risingwave_connector::source::DataType;

use super::generic::GenericPlanNode;
use super::{
    generic, BatchSource, ColPrunable, LogicalFilter, LogicalProject, PlanBase, PlanRef,
    PredicatePushdown, StreamRowIdGen, StreamSource, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::ColumnId;
use crate::expr::{Expr, ExprImpl, ExprType, input_ref_to_column_indices};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition};
use crate::TableCatalog;

/// For kafka source, we attach a hidden column [`KAFKA_TIMESTAMP_COLUMN_NAME`] to it, so that we
/// can limit the timestamp range when querying it directly with batch query. The column type is
/// [`DataType::Timestamptz`]. For more details, please refer to
/// [this rfc](https://github.com/risingwavelabs/rfcs/pull/20).
pub const KAFKA_TIMESTAMP_COLUMN_NAME: &'static str = "_rw_kafka_timestamp";

/// `LogicalSource` returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct LogicalSource {
    pub base: PlanBase,
    pub core: generic::Source,

    /// Kafka timestamp range, currently we only support kafka, so we just leave it like this.
    kafka_timestamp_range: (Bound<i64>, Bound<i64>),
}

impl LogicalSource {
    pub fn new(
        source_catalog: Option<Rc<SourceCatalog>>,
        column_descs: Vec<ColumnDesc>,
        pk_col_ids: Vec<ColumnId>,
        row_id_index: Option<usize>,
        gen_row_id: bool,
        ctx: OptimizerContextRef,
    ) -> Self {
        let core = generic::Source {
            catalog: source_catalog,
            column_descs,
            pk_col_ids,
            row_id_index,
            gen_row_id,
        };

        let schema = core.schema();
        let pk_indices = core.logical_pk();

        let (functional_dependency, pk_indices) = match pk_indices {
            Some(pk_indices) => (
                FunctionalDependencySet::with_key(schema.len(), &pk_indices),
                pk_indices,
            ),
            None => (FunctionalDependencySet::new(schema.len()), vec![]),
        };

        let base = PlanBase::new_logical(ctx, schema, pk_indices, functional_dependency);

        let kafka_timestamp_range = (Bound::Unbounded, Bound::Unbounded);
        LogicalSource {
            base,
            core,
            kafka_timestamp_range,
        }
    }

    pub(super) fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn infer_internal_table_catalog(&self) -> TableCatalog {
        generic::Source::infer_internal_table_catalog(&self.base)
    }
}

impl_plan_tree_node_for_leaf! {LogicalSource}

impl fmt::Display for LogicalSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(catalog) = self.source_catalog() {
            write!(
                f,
                "LogicalSource {{ source: {}, columns: [{}] }}",
                catalog.name,
                self.column_names().join(", ")
            )
        } else {
            write!(f, "LogicalSource")
        }
    }
}

impl ColPrunable for LogicalSource {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

fn expr_to_kafka_timestamp_range(expr: ExprImpl, mut range: (Bound<i64>, Bound<i64>),
                                 schema: &Schema) -> Option<ExprImpl> {
    match expr {
        ExprImpl::FunctionCall(function_call) if function_call.inputs().len() == 2  => {

            let () timestampz_literal: Option<i64> = {
                match (function_call.inputs()[0], function_call.inputs()[1]) {
                    (ExprImpl::InputRef(input_ref ), ExprImpl::Literal(literal)) if schema.fields[input_ref.index].name == KAFKA_TIMESTAMP_COLUMN_NAME &&
                        literal.return_type() == DataType::Timestamptz => {
                         {
                            Some(literal.get_data().into())
                        }
                    },
                    (ExprImpl::Literal(literal), ExprImpl::InputRef(input_ref )) if schema
                        .fields[input_ref.index].name == KAFKA_TIMESTAMP_COLUMN_NAME &&
                        literal.return_type() == DataType::Timestamptz => {
                        {
                            Some(literal.get_data().into())
                        }
                    },
                    _ => None
                }
            }

           if let Some(timestampz_literal) = timestampz_literal {
               match function_call.get_expr_type() {
                   ExprType::GreaterThan
               }
           }
        }
    }
}

impl PredicatePushdown for LogicalSource {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let (mut lower_bound, mut upper_bound) = (Unbounded, Unbounded);


        for expr in predicate.conjunctions {
        }

        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalSource {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchSource::new(self.clone()).into())
    }
}

impl ToStream for LogicalSource {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let mut plan: PlanRef = StreamSource::new(self.clone()).into();
        if let Some(row_id_index) = self.core.row_id_index  && self.core.gen_row_id{
            plan = StreamRowIdGen::new(plan, row_id_index).into();
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
