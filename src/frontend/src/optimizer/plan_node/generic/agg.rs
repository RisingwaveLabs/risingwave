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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::{fmt, vec};

use itertools::{Either, Itertools};
use pretty_xmlish::{Pretty, StrAssocArr};
use risingwave_common::catalog::{Field, FieldDisplay, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{ColumnOrder, ColumnOrderDisplay, OrderType};
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_expr::aggregate::{agg_types, AggType, PbAggKind};
use risingwave_expr::sig::{FuncBuilder, FUNCTION_REGISTRY};
use risingwave_pb::expr::{PbAggCall, PbConstant};
use risingwave_pb::stream_plan::{agg_call_state, AggCallState as PbAggCallState};

use super::super::utils::TableCatalogBuilder;
use super::{impl_distill_unit_from_fields, stream, GenericPlanNode, GenericPlanRef};
use crate::error::{ErrorCode, Result};
use crate::expr::{Expr, ExprRewriter, ExprVisitor, InputRef, InputRefDisplay, Literal};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::batch::BatchPlanRef;
use crate::optimizer::property::{
    Distribution, FunctionalDependencySet, RequiredDist, WatermarkColumns,
};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{
    ColIndexMapping, ColIndexMappingRewriteExt, Condition, ConditionDisplay, IndexRewriter,
    IndexSet,
};
use crate::TableCatalog;

/// [`Agg`] groups input data by their group key and computes aggregation functions.
///
/// It corresponds to the `GROUP BY` operator in a SQL query statement together with the aggregate
/// functions in the `SELECT` clause.
///
/// The output schema will first include the group key and then the aggregation calls.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Agg<PlanRef> {
    pub agg_calls: Vec<PlanAggCall>,
    pub group_key: IndexSet,
    pub grouping_sets: Vec<IndexSet>,
    pub input: PlanRef,
    pub enable_two_phase: bool,
}

impl<PlanRef: GenericPlanRef> Agg<PlanRef> {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.agg_calls.iter_mut().for_each(|call| {
            call.filter = call.filter.clone().rewrite_expr(r);
        });
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.agg_calls.iter().for_each(|call| {
            call.filter.visit_expr(v);
        });
    }

    pub(crate) fn output_len(&self) -> usize {
        self.group_key.len() + self.agg_calls.len()
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let mut map = vec![None; self.output_len()];
        for (i, key) in self.group_key.indices().enumerate() {
            map[i] = Some(key);
        }
        ColIndexMapping::new(map, self.input.schema().len())
    }

    /// get the Mapping of columnIndex from input column index to out column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        let mut map = vec![None; self.input.schema().len()];
        for (i, key) in self.group_key.indices().enumerate() {
            map[key] = Some(i);
        }
        ColIndexMapping::new(map, self.output_len())
    }

    fn two_phase_agg_forced(&self) -> bool {
        self.ctx().session_ctx().config().force_two_phase_agg()
    }

    pub fn two_phase_agg_enabled(&self) -> bool {
        self.enable_two_phase
    }

    pub(crate) fn can_two_phase_agg(&self) -> bool {
        self.two_phase_agg_enabled()
            && !self.agg_calls.is_empty()
            && self.agg_calls.iter().all(|call| {
                let agg_type_ok = !matches!(call.agg_type, agg_types::simply_cannot_two_phase!());
                let order_ok = matches!(
                    call.agg_type,
                    agg_types::result_unaffected_by_order_by!()
                        | AggType::Builtin(PbAggKind::ApproxPercentile)
                ) || call.order_by.is_empty();
                let distinct_ok =
                    matches!(call.agg_type, agg_types::result_unaffected_by_distinct!())
                        || !call.distinct;
                agg_type_ok && order_ok && distinct_ok
            })
    }

    /// Must try two phase agg iff we are forced to, and we satisfy the constraints.
    pub(crate) fn must_try_two_phase_agg(&self) -> bool {
        self.two_phase_agg_forced() && self.can_two_phase_agg()
    }

    /// Generally used by two phase hash agg.
    /// If input dist already satisfies hash agg distribution,
    /// it will be more expensive to do two phase agg, should just do shuffle agg.
    pub(crate) fn hash_agg_dist_satisfied_by_input_dist(&self, input_dist: &Distribution) -> bool {
        let required_dist =
            RequiredDist::shard_by_key(self.input.schema().len(), &self.group_key.to_vec());
        input_dist.satisfies(&required_dist)
    }

    /// See if all stream aggregation calls have a stateless local agg counterpart.
    pub(crate) fn all_local_aggs_are_stateless(&self, stream_input_append_only: bool) -> bool {
        self.agg_calls.iter().all(|c| {
            matches!(c.agg_type, agg_types::single_value_state!())
                || (matches!(c.agg_type, agg_types::single_value_state_iff_in_append_only!() if stream_input_append_only))
        })
    }

    pub(crate) fn eowc_window_column(
        &self,
        input_watermark_columns: &WatermarkColumns,
    ) -> Result<usize> {
        let group_key_with_wtmk = self
            .group_key
            .indices()
            .filter_map(|idx| {
                input_watermark_columns
                    .get_group(idx)
                    .map(|group| (idx, group))
            })
            .collect::<Vec<_>>();

        if group_key_with_wtmk.is_empty() {
            return Err(ErrorCode::NotSupported(
                "Emit-On-Window-Close mode requires a watermark column in GROUP BY.".to_owned(),
                "Please try to GROUP BY a watermark column".to_owned(),
            )
            .into());
        }
        if group_key_with_wtmk.len() == 1
            || group_key_with_wtmk
                .iter()
                .map(|(_, group)| group)
                .all_equal()
        {
            // 1. only one watermark column, should be the window column
            // 2. all watermark columns belong to the same group, choose the first one as the window column
            return Ok(group_key_with_wtmk[0].0);
        }
        Err(ErrorCode::NotSupported(
            "Emit-On-Window-Close mode requires that watermark columns in GROUP BY are derived from the same upstream column.".to_owned(),
            "Please try to remove undesired columns from GROUP BY".to_owned(),
        )
        .into())
    }

    pub fn new(agg_calls: Vec<PlanAggCall>, group_key: IndexSet, input: PlanRef) -> Self {
        let enable_two_phase = input.ctx().session_ctx().config().enable_two_phase_agg();
        Self {
            agg_calls,
            group_key,
            input,
            grouping_sets: vec![],
            enable_two_phase,
        }
    }

    pub fn with_grouping_sets(mut self, grouping_sets: Vec<IndexSet>) -> Self {
        self.grouping_sets = grouping_sets;
        self
    }

    pub fn with_enable_two_phase(mut self, enable_two_phase: bool) -> Self {
        self.enable_two_phase = enable_two_phase;
        self
    }
}

impl<PlanRef: BatchPlanRef> Agg<PlanRef> {
    // Check if the input is already sorted on group keys.
    pub(crate) fn input_provides_order_on_group_keys(&self) -> bool {
        let mut input_order_prefix = IndexSet::empty();
        for input_order_col in &self.input.order().column_orders {
            if !self.group_key.contains(input_order_col.column_index) {
                break;
            }
            input_order_prefix.insert(input_order_col.column_index);
        }
        self.group_key == input_order_prefix
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Agg<PlanRef> {
    fn schema(&self) -> Schema {
        let fields = self
            .group_key
            .indices()
            .map(|i| self.input.schema().fields()[i].clone())
            .chain(self.agg_calls.iter().map(|agg_call| {
                let plan_agg_call_display = PlanAggCallDisplay {
                    plan_agg_call: agg_call,
                    input_schema: self.input.schema(),
                };
                let name = format!("{:?}", plan_agg_call_display);
                Field::with_name(agg_call.return_type.clone(), name)
            }))
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some((0..self.group_key.len()).collect())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let output_len = self.output_len();
        let _input_len = self.input.schema().len();
        let mut fd_set =
            FunctionalDependencySet::with_key(output_len, &(0..self.group_key.len()).collect_vec());
        // take group keys from input_columns, then grow the target size to column_cnt
        let i2o = self.i2o_col_mapping();
        for fd in self.input.functional_dependency().as_dependencies() {
            if let Some(fd) = i2o.rewrite_functional_dependency(fd) {
                fd_set.add_functional_dependency(fd);
            }
        }
        fd_set
    }
}

pub enum AggCallState {
    Value,
    MaterializedInput(Box<MaterializedInputState>),
}

impl AggCallState {
    pub fn into_prost(self, state: &mut BuildFragmentGraphState) -> PbAggCallState {
        PbAggCallState {
            inner: Some(match self {
                AggCallState::Value => {
                    agg_call_state::Inner::ValueState(agg_call_state::ValueState {})
                }
                AggCallState::MaterializedInput(s) => {
                    agg_call_state::Inner::MaterializedInputState(
                        agg_call_state::MaterializedInputState {
                            table: Some(
                                s.table
                                    .with_id(state.gen_table_id_wrapped())
                                    .to_internal_table_prost(),
                            ),
                            included_upstream_indices: s
                                .included_upstream_indices
                                .into_iter()
                                .map(|x| x as _)
                                .collect(),
                            table_value_indices: s
                                .table_value_indices
                                .into_iter()
                                .map(|x| x as _)
                                .collect(),
                            order_columns: s
                                .order_columns
                                .into_iter()
                                .map(|x| x.to_protobuf())
                                .collect(),
                        },
                    )
                }
            }),
        }
    }
}

pub struct MaterializedInputState {
    pub table: TableCatalog,
    pub included_upstream_indices: Vec<usize>,
    pub table_value_indices: Vec<usize>,
    pub order_columns: Vec<ColumnOrder>,
}

impl<PlanRef: stream::StreamPlanRef> Agg<PlanRef> {
    pub fn infer_tables(
        &self,
        me: impl stream::StreamPlanRef,
        vnode_col_idx: Option<usize>,
        window_col_idx: Option<usize>,
    ) -> (
        TableCatalog,
        Vec<AggCallState>,
        HashMap<usize, TableCatalog>,
    ) {
        (
            self.infer_intermediate_state_table(&me, vnode_col_idx, window_col_idx),
            self.infer_stream_agg_state(&me, vnode_col_idx, window_col_idx),
            self.infer_distinct_dedup_tables(&me, vnode_col_idx, window_col_idx),
        )
    }

    fn get_ordered_group_key(&self, window_col_idx: Option<usize>) -> Vec<usize> {
        if let Some(window_col_idx) = window_col_idx {
            assert!(self.group_key.contains(window_col_idx));
            Either::Left(
                std::iter::once(window_col_idx).chain(
                    self.group_key
                        .indices()
                        .filter(move |&i| i != window_col_idx),
                ),
            )
        } else {
            Either::Right(self.group_key.indices())
        }
        .collect()
    }

    /// Create a new table builder with group key columns added.
    ///
    /// # Returns
    ///
    /// - table builder with group key columns added
    /// - included upstream indices
    /// - column mapping from upstream to table
    fn create_table_builder(
        &self,
        _ctx: OptimizerContextRef,
        window_col_idx: Option<usize>,
    ) -> (TableCatalogBuilder, Vec<usize>, BTreeMap<usize, usize>) {
        // NOTE: this function should be called to get a table builder, so that all state tables
        // created for Agg node have the same group key columns and pk ordering.
        let mut table_builder = TableCatalogBuilder::default();

        assert!(table_builder.columns().is_empty());
        assert_eq!(table_builder.get_current_pk_len(), 0);

        // add group key column to table builder
        let mut included_upstream_indices = vec![];
        let mut column_mapping = BTreeMap::new();
        let in_fields = self.input.schema().fields();
        for idx in self.group_key.indices() {
            let tbl_col_idx = table_builder.add_column(&in_fields[idx]);
            included_upstream_indices.push(idx);
            column_mapping.insert(idx, tbl_col_idx);
        }

        // configure state table primary key (ordering)
        let ordered_group_key = self.get_ordered_group_key(window_col_idx);
        for idx in ordered_group_key {
            table_builder.add_order_column(column_mapping[&idx], OrderType::ascending());
        }

        (table_builder, included_upstream_indices, column_mapping)
    }

    /// Infer `AggCallState`s for streaming agg.
    pub fn infer_stream_agg_state(
        &self,
        me: impl stream::StreamPlanRef,
        vnode_col_idx: Option<usize>,
        window_col_idx: Option<usize>,
    ) -> Vec<AggCallState> {
        let in_fields = self.input.schema().fields().to_vec();
        let in_pks = self.input.stream_key().unwrap().to_vec();
        let in_append_only = self.input.append_only();
        let in_dist_key = self.input.distribution().dist_column_indices().to_vec();

        let gen_materialized_input_state = |sort_keys: Vec<(OrderType, usize)>,
                                            extra_keys: Vec<usize>,
                                            include_keys: Vec<usize>|
         -> MaterializedInputState {
            let (mut table_builder, mut included_upstream_indices, mut column_mapping) =
                self.create_table_builder(me.ctx(), window_col_idx);
            let read_prefix_len_hint = table_builder.get_current_pk_len();

            let mut order_columns = vec![];
            let mut table_value_indices = BTreeSet::new(); // table column indices of value columns
            let mut add_column =
                |upstream_idx, order_type, table_builder: &mut TableCatalogBuilder| {
                    column_mapping.entry(upstream_idx).or_insert_with(|| {
                        let table_col_idx = table_builder.add_column(&in_fields[upstream_idx]);
                        if let Some(order_type) = order_type {
                            table_builder.add_order_column(table_col_idx, order_type);
                            order_columns.push(ColumnOrder::new(upstream_idx, order_type));
                        }
                        included_upstream_indices.push(upstream_idx);
                        table_col_idx
                    });
                    table_value_indices.insert(column_mapping[&upstream_idx]);
                };

            for (order_type, idx) in sort_keys {
                add_column(idx, Some(order_type), &mut table_builder);
            }
            for idx in extra_keys {
                add_column(idx, Some(OrderType::ascending()), &mut table_builder);
            }
            for idx in include_keys {
                add_column(idx, None, &mut table_builder);
            }

            let mapping =
                ColIndexMapping::with_included_columns(&included_upstream_indices, in_fields.len());
            let tb_dist = mapping.rewrite_dist_key(&in_dist_key);
            if let Some(tb_vnode_idx) = vnode_col_idx.and_then(|idx| mapping.try_map(idx)) {
                table_builder.set_vnode_col_idx(tb_vnode_idx);
            }

            // set value indices to reduce ser/de overhead
            let table_value_indices = table_value_indices.into_iter().collect_vec();
            table_builder.set_value_indices(table_value_indices.clone());

            MaterializedInputState {
                table: table_builder.build(tb_dist.unwrap_or_default(), read_prefix_len_hint),
                included_upstream_indices,
                table_value_indices,
                order_columns,
            }
        };

        self.agg_calls
            .iter()
            .map(|agg_call| match agg_call.agg_type {
                agg_types::single_value_state_iff_in_append_only!() if in_append_only => {
                    AggCallState::Value
                }
                agg_types::single_value_state!() => AggCallState::Value,
                agg_types::materialized_input_state!() => {
                    // columns with order requirement in state table
                    let sort_keys = {
                        match agg_call.agg_type {
                            AggType::Builtin(PbAggKind::Min) => {
                                vec![(OrderType::ascending(), agg_call.inputs[0].index)]
                            }
                            AggType::Builtin(PbAggKind::Max) => {
                                vec![(OrderType::descending(), agg_call.inputs[0].index)]
                            }
                            AggType::Builtin(
                                PbAggKind::FirstValue
                                | PbAggKind::LastValue
                                | PbAggKind::StringAgg
                                | PbAggKind::ArrayAgg
                                | PbAggKind::JsonbAgg,
                            )
                            | AggType::WrapScalar(_) => {
                                if agg_call.order_by.is_empty() {
                                    me.ctx().warn_to_user(format!(
                                        "{} without ORDER BY may produce non-deterministic result",
                                        agg_call.agg_type,
                                    ));
                                }
                                agg_call
                                    .order_by
                                    .iter()
                                    .map(|o| {
                                        (
                                            if matches!(
                                                agg_call.agg_type,
                                                AggType::Builtin(PbAggKind::LastValue)
                                            ) {
                                                o.order_type.reverse()
                                            } else {
                                                o.order_type
                                            },
                                            o.column_index,
                                        )
                                    })
                                    .collect()
                            }
                            AggType::Builtin(PbAggKind::JsonbObjectAgg) => agg_call
                                .order_by
                                .iter()
                                .map(|o| (o.order_type, o.column_index))
                                .collect(),
                            _ => unreachable!(),
                        }
                    };

                    // columns to ensure each row unique
                    let extra_keys = if agg_call.distinct {
                        // if distinct, use distinct keys as extra keys
                        let distinct_key = agg_call.inputs[0].index;
                        vec![distinct_key]
                    } else {
                        // if not distinct, use primary keys as extra keys
                        in_pks.clone()
                    };

                    // other columns that should be contained in state table
                    let include_keys = match agg_call.agg_type {
                        // `agg_types::materialized_input_state` except for `min`/`max`
                        AggType::Builtin(
                            PbAggKind::FirstValue
                            | PbAggKind::LastValue
                            | PbAggKind::StringAgg
                            | PbAggKind::ArrayAgg
                            | PbAggKind::JsonbAgg
                            | PbAggKind::JsonbObjectAgg,
                        )
                        | AggType::WrapScalar(_) => {
                            agg_call.inputs.iter().map(|i| i.index).collect()
                        }
                        _ => vec![],
                    };

                    let state = gen_materialized_input_state(sort_keys, extra_keys, include_keys);
                    AggCallState::MaterializedInput(Box::new(state))
                }
                agg_types::rewritten!() => {
                    unreachable!("should have been rewritten")
                }
                agg_types::unimplemented_in_stream!() => {
                    unreachable!("should have been banned")
                }
                AggType::Builtin(
                    PbAggKind::Unspecified | PbAggKind::UserDefined | PbAggKind::WrapScalar,
                ) => {
                    unreachable!("invalid agg kind")
                }
            })
            .collect()
    }

    /// table schema:
    /// group key | state for AGG1 | state for AGG2 | ...
    pub fn infer_intermediate_state_table(
        &self,
        me: impl GenericPlanRef,
        vnode_col_idx: Option<usize>,
        window_col_idx: Option<usize>,
    ) -> TableCatalog {
        let mut out_fields = me.schema().fields().to_vec();

        // rewrite data types in fields
        let in_append_only = self.input.append_only();
        for (agg_call, field) in self
            .agg_calls
            .iter()
            .zip_eq_fast(&mut out_fields[self.group_key.len()..])
        {
            let agg_kind = match agg_call.agg_type {
                AggType::UserDefined(_) => {
                    // for user defined aggregate, the state type is always BYTEA
                    field.data_type = DataType::Bytea;
                    continue;
                }
                AggType::WrapScalar(_) => {
                    // for wrapped scalar function, the state is always NULL
                    continue;
                }
                AggType::Builtin(kind) => kind,
            };
            let sig = FUNCTION_REGISTRY
                .get(
                    agg_kind,
                    &agg_call
                        .inputs
                        .iter()
                        .map(|input| input.data_type.clone())
                        .collect_vec(),
                    &agg_call.return_type,
                )
                .expect("agg not found");
            // in_append_only: whether the input is append-only
            // sig.is_append_only(): whether the agg function has append-only version
            match (in_append_only, sig.is_append_only()) {
                (false, true) => {
                    // we use materialized input state for non-retractable aggregate function.
                    // for backward compatibility, the state type is same as the return type.
                    // its values in the intermediate state table are always null.
                }
                (true, true) => {
                    // use append-only version
                    if let FuncBuilder::Aggregate {
                        append_only_state_type: Some(state_type),
                        ..
                    } = &sig.build
                    {
                        field.data_type = state_type.clone();
                    }
                }
                (_, false) => {
                    // there is only retractable version, use it
                    if let FuncBuilder::Aggregate {
                        retractable_state_type: Some(state_type),
                        ..
                    } = &sig.build
                    {
                        field.data_type = state_type.clone();
                    }
                }
            }
        }
        let in_dist_key = self.input.distribution().dist_column_indices().to_vec();
        let n_group_key_cols = self.group_key.len();

        let (mut table_builder, _, _) = self.create_table_builder(me.ctx(), window_col_idx);
        let read_prefix_len_hint = table_builder.get_current_pk_len();

        for field in out_fields.iter().skip(n_group_key_cols) {
            table_builder.add_column(field);
        }

        let mapping = self.i2o_col_mapping();
        let tb_dist = mapping.rewrite_dist_key(&in_dist_key).unwrap_or_default();
        if let Some(tb_vnode_idx) = vnode_col_idx.and_then(|idx| mapping.try_map(idx)) {
            table_builder.set_vnode_col_idx(tb_vnode_idx);
        }

        // the result_table is composed of group_key and all agg_call's values, so the value_indices
        // of this table should skip group_key.len().
        table_builder.set_value_indices((n_group_key_cols..out_fields.len()).collect());
        table_builder.build(tb_dist, read_prefix_len_hint)
    }

    /// Infer dedup tables for distinct agg calls, partitioned by distinct columns.
    /// Since distinct agg calls only dedup on the first argument, the key of the result map is
    /// `usize`, i.e. the distinct column index.
    ///
    /// Dedup table schema:
    /// group key | distinct key | count for AGG1(distinct x) | count for AGG2(distinct x) | ...
    pub fn infer_distinct_dedup_tables(
        &self,
        me: impl GenericPlanRef,
        vnode_col_idx: Option<usize>,
        window_col_idx: Option<usize>,
    ) -> HashMap<usize, TableCatalog> {
        let in_dist_key = self.input.distribution().dist_column_indices().to_vec();
        let in_fields = self.input.schema().fields();

        self.agg_calls
            .iter()
            .enumerate()
            .filter(|(_, call)| call.distinct) // only distinct agg calls need dedup table
            .into_group_map_by(|(_, call)| call.inputs[0].index) // one table per distinct column
            .into_iter()
            .map(|(distinct_col, indices_and_calls)| {
                let (mut table_builder, mut key_cols, _) =
                    self.create_table_builder(me.ctx(), window_col_idx);
                let table_col_idx = table_builder.add_column(&in_fields[distinct_col]);
                table_builder.add_order_column(table_col_idx, OrderType::ascending());
                key_cols.push(distinct_col);

                let read_prefix_len_hint = table_builder.get_current_pk_len();

                // Agg calls with same distinct column share the same dedup table, but they may have
                // different filter conditions, so the count of occurrence of one distinct key may
                // differ among different calls. We add one column for each call in the dedup table.
                for (call_index, _) in indices_and_calls {
                    table_builder.add_column(&Field {
                        data_type: DataType::Int64,
                        name: format!("count_for_agg_call_{}", call_index),
                    });
                }
                table_builder
                    .set_value_indices((key_cols.len()..table_builder.columns().len()).collect());

                let mapping = ColIndexMapping::with_included_columns(&key_cols, in_fields.len());
                if let Some(idx) = vnode_col_idx.and_then(|idx| mapping.try_map(idx)) {
                    table_builder.set_vnode_col_idx(idx);
                }
                let dist_key = mapping.rewrite_dist_key(&in_dist_key).unwrap_or_default();
                let table = table_builder.build(dist_key, read_prefix_len_hint);
                (distinct_col, table)
            })
            .collect()
    }

    pub fn decompose(self) -> (Vec<PlanAggCall>, IndexSet, Vec<IndexSet>, PlanRef, bool) {
        (
            self.agg_calls,
            self.group_key,
            self.grouping_sets,
            self.input,
            self.enable_two_phase,
        )
    }

    pub fn fields_pretty<'a>(&self) -> StrAssocArr<'a> {
        let last = ("aggs", self.agg_calls_pretty());
        if !self.group_key.is_empty() {
            let first = ("group_key", self.group_key_pretty());
            vec![first, last]
        } else {
            vec![last]
        }
    }

    fn agg_calls_pretty<'a>(&self) -> Pretty<'a> {
        let f = |plan_agg_call| {
            Pretty::debug(&PlanAggCallDisplay {
                plan_agg_call,
                input_schema: self.input.schema(),
            })
        };
        Pretty::Array(self.agg_calls.iter().map(f).collect())
    }

    fn group_key_pretty<'a>(&self) -> Pretty<'a> {
        let f = |i| Pretty::display(&FieldDisplay(self.input.schema().fields.get(i).unwrap()));
        Pretty::Array(self.group_key.indices().map(f).collect())
    }
}

impl_distill_unit_from_fields!(Agg, stream::StreamPlanRef);

/// Rewritten version of [`crate::expr::AggCall`] which uses `InputRef` instead of `ExprImpl`.
/// Refer to [`crate::optimizer::plan_node::logical_agg::LogicalAggBuilder::try_rewrite_agg_call`]
/// for more details.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PlanAggCall {
    /// Type of aggregation function
    pub agg_type: AggType,

    /// Data type of the returned column
    pub return_type: DataType,

    /// Column indexes of input columns.
    ///
    /// Its length can be:
    /// - 0 (`RowCount`)
    /// - 1 (`Max`, `Min`)
    /// - 2 (`StringAgg`).
    ///
    /// Usually, we mark the first column as the aggregated column.
    pub inputs: Vec<InputRef>,

    pub distinct: bool,
    pub order_by: Vec<ColumnOrder>,
    /// Selective aggregation: only the input rows for which
    /// `filter` evaluates to `true` will be fed to the aggregate function.
    pub filter: Condition,
    pub direct_args: Vec<Literal>,
}

impl fmt::Debug for PlanAggCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.agg_type)?;
        if !self.inputs.is_empty() {
            write!(f, "(")?;
            for (idx, input) in self.inputs.iter().enumerate() {
                if idx == 0 && self.distinct {
                    write!(f, "distinct ")?;
                }
                write!(f, "{:?}", input)?;
                if idx != (self.inputs.len() - 1) {
                    write!(f, ",")?;
                }
            }
            if !self.order_by.is_empty() {
                let clause_text = self.order_by.iter().map(|e| format!("{:?}", e)).join(", ");
                write!(f, " order_by({})", clause_text)?;
            }
            write!(f, ")")?;
        }
        if !self.filter.always_true() {
            write!(
                f,
                " filter({:?})",
                self.filter.as_expr_unless_true().unwrap()
            )?;
        }
        Ok(())
    }
}

impl PlanAggCall {
    pub fn rewrite_input_index(&mut self, mapping: ColIndexMapping) {
        // modify input
        self.inputs.iter_mut().for_each(|x| {
            x.index = mapping.map(x.index);
        });

        // modify order_by exprs
        self.order_by.iter_mut().for_each(|x| {
            x.column_index = mapping.map(x.column_index);
        });

        // modify filter
        let mut rewriter = IndexRewriter::new(mapping);
        self.filter.conjunctions.iter_mut().for_each(|x| {
            *x = rewriter.rewrite_expr(x.clone());
        });
    }

    pub fn to_protobuf(&self) -> PbAggCall {
        PbAggCall {
            kind: match &self.agg_type {
                AggType::Builtin(kind) => *kind,
                AggType::UserDefined(_) => PbAggKind::UserDefined,
                AggType::WrapScalar(_) => PbAggKind::WrapScalar,
            }
            .into(),
            return_type: Some(self.return_type.to_protobuf()),
            args: self.inputs.iter().map(InputRef::to_proto).collect(),
            distinct: self.distinct,
            order_by: self.order_by.iter().map(ColumnOrder::to_protobuf).collect(),
            filter: self.filter.as_expr_unless_true().map(|x| x.to_expr_proto()),
            direct_args: self
                .direct_args
                .iter()
                .map(|x| PbConstant {
                    datum: Some(x.get_data().to_protobuf()),
                    r#type: Some(x.return_type().to_protobuf()),
                })
                .collect(),
            udf: match &self.agg_type {
                AggType::UserDefined(udf) => Some(udf.clone()),
                _ => None,
            },
            scalar: match &self.agg_type {
                AggType::WrapScalar(expr) => Some(expr.clone()),
                _ => None,
            },
        }
    }

    pub fn partial_to_total_agg_call(&self, partial_output_idx: usize) -> PlanAggCall {
        let total_agg_type = self
            .agg_type
            .partial_to_total()
            .expect("unsupported kinds shouldn't get here");
        PlanAggCall {
            agg_type: total_agg_type,
            inputs: vec![InputRef::new(partial_output_idx, self.return_type.clone())],
            order_by: vec![], // order must make no difference when we use 2-phase agg
            filter: Condition::true_cond(),
            ..self.clone()
        }
    }

    pub fn count_star() -> Self {
        PlanAggCall {
            agg_type: PbAggKind::Count.into(),
            return_type: DataType::Int64,
            inputs: vec![],
            distinct: false,
            order_by: vec![],
            filter: Condition::true_cond(),
            direct_args: vec![],
        }
    }

    pub fn with_condition(mut self, filter: Condition) -> Self {
        self.filter = filter;
        self
    }

    pub fn input_indices(&self) -> Vec<usize> {
        self.inputs.iter().map(|input| input.index()).collect()
    }
}

pub struct PlanAggCallDisplay<'a> {
    pub plan_agg_call: &'a PlanAggCall,
    pub input_schema: &'a Schema,
}

impl fmt::Debug for PlanAggCallDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let that = self.plan_agg_call;
        write!(f, "{}", that.agg_type)?;
        if !that.inputs.is_empty() {
            write!(f, "(")?;
            for (idx, input) in that.inputs.iter().enumerate() {
                if idx == 0 && that.distinct {
                    write!(f, "distinct ")?;
                }
                write!(
                    f,
                    "{}",
                    InputRefDisplay {
                        input_ref: input,
                        input_schema: self.input_schema
                    }
                )?;
                if idx != (that.inputs.len() - 1) {
                    write!(f, ", ")?;
                }
            }
            if !that.order_by.is_empty() {
                write!(
                    f,
                    " order_by({})",
                    that.order_by.iter().format_with(", ", |o, f| {
                        f(&ColumnOrderDisplay {
                            column_order: o,
                            input_schema: self.input_schema,
                        })
                    })
                )?;
            }
            write!(f, ")")?;
        }

        if !that.filter.always_true() {
            write!(
                f,
                " filter({:?})",
                ConditionDisplay {
                    condition: &that.filter,
                    input_schema: self.input_schema,
                }
            )?;
        }
        Ok(())
    }
}
