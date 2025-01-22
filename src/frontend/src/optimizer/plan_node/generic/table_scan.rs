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

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use educe::Educe;
use pretty_xmlish::Pretty;
use risingwave_common::catalog::{ColumnDesc, Field, Schema, TableDesc};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_sqlparser::ast::AsOf;

use super::GenericPlanNode;
use crate::catalog::table_catalog::TableType;
use crate::catalog::{ColumnId, IndexCatalog};
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor, FunctionCall, InputRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{Cardinality, FunctionalDependencySet, Order, WatermarkColumns};
use crate::utils::{ColIndexMappingRewriteExt, Condition};
use crate::TableCatalog;

/// [`TableScan`] returns contents of a RisingWave Table.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct TableScan {
    pub table_name: String,
    /// Include `output_col_idx` and columns required in `predicate`
    pub required_col_idx: Vec<usize>,
    pub output_col_idx: Vec<usize>,
    /// Table Catalog of the upstream table that the descriptor is derived from.
    pub table_catalog: Arc<TableCatalog>,
    // FIXME(kwannoel): Currently many places in the code reference this,
    // but now we have table catalog.
    // We should remove this and use table catalog in those call-sites instead.
    // It's introduced in https://github.com/risingwavelabs/risingwave/pull/13622.
    // We kept this field to avoid extensive refactor in that PR.
    /// Table Desc (subset of table catalog).
    pub table_desc: Rc<TableDesc>,
    /// Descriptors of all indexes on this table
    pub indexes: Vec<Rc<IndexCatalog>>,
    /// The pushed down predicates. It refers to column indexes of the table.
    pub predicate: Condition,
    /// syntax `FOR SYSTEM_TIME AS OF PROCTIME()` is used for temporal join.
    /// syntax `FOR SYSTEM_TIME AS OF '1986-10-26 01:21:00'` is used for iceberg.
    /// syntax `FOR SYSTEM_TIME AS OF 499162860` is used for iceberg.
    /// syntax `FOR SYSTEM_VERSION AS OF 10963874102873;` is used for iceberg.
    pub as_of: Option<AsOf>,
    /// The cardinality of the table **without** applying the predicate.
    pub table_cardinality: Cardinality,
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl TableScan {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.predicate = self.predicate.clone().rewrite_expr(r);
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.predicate.visit_expr(v);
    }

    /// The mapped distribution key of the scan operator.
    ///
    /// The column indices in it is the position in the `output_col_idx`, instead of the position
    /// in all the columns of the table (which is the table's distribution key).
    ///
    /// Return `None` if the table's distribution key are not all in the `output_col_idx`.
    pub fn distribution_key(&self) -> Option<Vec<usize>> {
        let tb_idx_to_op_idx = self
            .output_col_idx
            .iter()
            .enumerate()
            .map(|(op_idx, tb_idx)| (*tb_idx, op_idx))
            .collect::<HashMap<_, _>>();
        self.table_desc
            .distribution_key
            .iter()
            .map(|&tb_idx| tb_idx_to_op_idx.get(&tb_idx).cloned())
            .collect()
    }

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.output_col_idx
            .iter()
            .map(|i| self.get_table_columns()[*i].column_id)
            .collect()
    }

    pub fn primary_key(&self) -> &[ColumnOrder] {
        &self.table_desc.pk
    }

    pub fn watermark_columns(&self) -> WatermarkColumns {
        // TODO(rc): For now, we still use `FixedBitSet` for watermark columns in `TableDesc`.
        // So when we scan from a table, we have to conservatively assign each watermark column
        // a separate watermark group. We should record the watermark group information in
        // `TableDesc` later.
        let mut watermark_columns = WatermarkColumns::new();
        for idx in self.table_desc.watermark_columns.ones() {
            watermark_columns.insert(idx, self.ctx.next_watermark_group_id());
        }
        watermark_columns.map_clone(&self.i2o_col_mapping())
    }

    pub(crate) fn column_names_with_table_prefix(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| format!("{}.{}", self.table_name, self.get_table_columns()[i].name))
            .collect()
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].name.clone())
            .collect()
    }

    pub(crate) fn out_fields(&self) -> FixedBitSet {
        let out_fields_vec = self.output_col_idx.clone();
        FixedBitSet::from_iter(out_fields_vec)
    }

    pub(crate) fn order_names(&self) -> Vec<String> {
        self.table_desc
            .order_column_indices()
            .iter()
            .map(|&i| self.get_table_columns()[i].name.clone())
            .collect()
    }

    pub(crate) fn order_names_with_table_prefix(&self) -> Vec<String> {
        self.table_desc
            .order_column_indices()
            .iter()
            .map(|&i| format!("{}.{}", self.table_name, self.get_table_columns()[i].name))
            .collect()
    }

    /// Return indices of fields the output is ordered by and
    /// corresponding direction
    pub fn get_out_column_index_order(&self) -> Order {
        let id_to_tb_idx = self.table_desc.get_id_to_op_idx_mapping();
        let order = Order::new(
            self.table_desc
                .pk
                .iter()
                .map(|order| {
                    let idx = id_to_tb_idx
                        .get(&self.table_desc.columns[order.column_index].column_id)
                        .unwrap();
                    ColumnOrder::new(*idx, order.order_type)
                })
                .collect(),
        );
        self.i2o_col_mapping().rewrite_provided_order(&order)
    }

    /// get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(
            &self.output_col_idx,
            self.get_table_columns().len(),
        )
    }

    /// Get the ids of the output columns and primary key columns.
    pub fn output_and_pk_column_ids(&self) -> Vec<ColumnId> {
        let mut ids = self.output_column_ids();
        for column_order in self.primary_key() {
            let id = self.get_table_columns()[column_order.column_index].column_id;
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids
    }

    /// Prerequisite: the caller should guarantee that `primary_to_secondary_mapping` must cover the
    /// scan.
    pub fn to_index_scan(
        &self,
        index_name: &str,
        index_table_catalog: Arc<TableCatalog>,
        primary_to_secondary_mapping: &BTreeMap<usize, usize>,
        function_mapping: &HashMap<FunctionCall, usize>,
    ) -> Self {
        let new_output_col_idx = self
            .output_col_idx
            .iter()
            .map(|col_idx| *primary_to_secondary_mapping.get(col_idx).unwrap())
            .collect();

        struct Rewriter<'a> {
            primary_to_secondary_mapping: &'a BTreeMap<usize, usize>,
            function_mapping: &'a HashMap<FunctionCall, usize>,
        }
        impl ExprRewriter for Rewriter<'_> {
            fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
                InputRef::new(
                    *self
                        .primary_to_secondary_mapping
                        .get(&input_ref.index)
                        .unwrap(),
                    input_ref.return_type(),
                )
                .into()
            }

            fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
                if let Some(index) = self.function_mapping.get(&func_call) {
                    return InputRef::new(*index, func_call.return_type()).into();
                }

                let (func_type, inputs, ret) = func_call.decompose();
                let inputs = inputs
                    .into_iter()
                    .map(|expr| self.rewrite_expr(expr))
                    .collect();
                FunctionCall::new_unchecked(func_type, inputs, ret).into()
            }
        }
        let mut rewriter = Rewriter {
            primary_to_secondary_mapping,
            function_mapping,
        };

        let new_predicate = self.predicate.clone().rewrite_expr(&mut rewriter);

        Self::new(
            index_name.to_owned(),
            new_output_col_idx,
            index_table_catalog,
            vec![],
            self.ctx.clone(),
            new_predicate,
            self.as_of.clone(),
            self.table_cardinality,
        )
    }

    /// Create a `LogicalScan` node. Used internally by optimizer.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        table_name: String,
        output_col_idx: Vec<usize>, // the column index in the table
        table_catalog: Arc<TableCatalog>,
        indexes: Vec<Rc<IndexCatalog>>,
        ctx: OptimizerContextRef,
        predicate: Condition, // refers to column indexes of the table
        as_of: Option<AsOf>,
        table_cardinality: Cardinality,
    ) -> Self {
        Self::new_inner(
            table_name,
            output_col_idx,
            table_catalog,
            indexes,
            ctx,
            predicate,
            as_of,
            table_cardinality,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_inner(
        table_name: String,
        output_col_idx: Vec<usize>, // the column index in the table
        table_catalog: Arc<TableCatalog>,
        indexes: Vec<Rc<IndexCatalog>>,
        ctx: OptimizerContextRef,
        predicate: Condition, // refers to column indexes of the table
        as_of: Option<AsOf>,
        table_cardinality: Cardinality,
    ) -> Self {
        // here we have 3 concepts
        // 1. column_id: ColumnId, stored in catalog and a ID to access data from storage.
        // 2. table_idx: usize, column index in the TableDesc or tableCatalog.
        // 3. operator_idx: usize, column index in the ScanOperator's schema.
        // In a query we get the same version of catalog, so the mapping from column_id and
        // table_idx will not change. And the `required_col_idx` is the `table_idx` of the
        // required columns, i.e., the mapping from operator_idx to table_idx.

        let mut required_col_idx = output_col_idx.clone();
        let predicate_col_idx = predicate.collect_input_refs(table_catalog.columns().len());
        predicate_col_idx.ones().for_each(|idx| {
            if !required_col_idx.contains(&idx) {
                required_col_idx.push(idx);
            }
        });

        let table_desc = Rc::new(table_catalog.table_desc());

        Self {
            table_name,
            required_col_idx,
            output_col_idx,
            table_catalog,
            table_desc,
            indexes,
            predicate,
            as_of,
            table_cardinality,
            ctx,
        }
    }

    pub(crate) fn columns_pretty<'a>(&self, verbose: bool) -> Pretty<'a> {
        Pretty::Array(
            match verbose {
                true => self.column_names_with_table_prefix(),
                false => self.column_names(),
            }
            .into_iter()
            .map(Pretty::from)
            .collect(),
        )
    }

    pub(crate) fn fields_pretty_schema(&self) -> Schema {
        let fields = self
            .table_desc
            .columns
            .iter()
            .map(|col| Field::from_with_table_name_prefix(col, &self.table_name))
            .collect();
        Schema { fields }
    }
}

impl GenericPlanNode for TableScan {
    fn schema(&self) -> Schema {
        let fields = self
            .output_col_idx
            .iter()
            .map(|tb_idx| {
                let col = &self.get_table_columns()[*tb_idx];
                Field::from_with_table_name_prefix(col, &self.table_name)
            })
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        if matches!(self.table_catalog.table_type, TableType::Internal) {
            return None;
        }
        let id_to_op_idx = Self::get_id_to_op_idx_mapping(&self.output_col_idx, &self.table_desc);
        self.table_desc
            .stream_key
            .iter()
            .map(|&c| {
                id_to_op_idx
                    .get(&self.table_desc.columns[c].column_id)
                    .copied()
            })
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.stream_key();
        let col_num = self.output_col_idx.len();
        match &pk_indices {
            Some(pk_indices) => FunctionalDependencySet::with_key(col_num, pk_indices),
            None => FunctionalDependencySet::new(col_num),
        }
    }
}

impl TableScan {
    pub fn get_table_columns(&self) -> &[ColumnDesc] {
        &self.table_desc.columns
    }

    pub fn append_only(&self) -> bool {
        self.table_desc.append_only
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].clone())
            .collect()
    }

    /// Helper function to create a mapping from `column_id` to `operator_idx`
    pub fn get_id_to_op_idx_mapping(
        output_col_idx: &[usize],
        table_desc: &Rc<TableDesc>,
    ) -> HashMap<ColumnId, usize> {
        let mut id_to_op_idx = HashMap::new();
        output_col_idx
            .iter()
            .enumerate()
            .for_each(|(op_idx, tb_idx)| {
                let col = &table_desc.columns[*tb_idx];
                id_to_op_idx.insert(col.column_id, op_idx);
            });
        id_to_op_idx
    }
}
