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

pub mod plan_node;
pub use plan_node::PlanRef;
pub mod property;

mod delta_join_solver;
mod heuristic;
mod max_one_row_visitor;
mod plan_correlated_id_finder;
mod plan_rewriter;
mod plan_visitor;
pub use plan_visitor::PlanVisitor;
mod rule;

use fixedbitset::FixedBitSet;
use itertools::Itertools as _;
use property::Order;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};

use self::heuristic::{ApplyOrder, HeuristicOptimizer};
use self::plan_node::{
    BatchProject, Convention, LogicalProject, LogicalSource, StreamDml, StreamMaterialize,
    StreamRowIdGen, StreamSink,
};
use self::plan_visitor::{
    has_batch_exchange, has_batch_seq_scan, has_batch_seq_scan_where, has_logical_apply,
    has_logical_over_agg,
};
use self::property::RequiredDist;
use self::rule::*;
use crate::optimizer::max_one_row_visitor::HasMaxOneRowApply;
use crate::optimizer::plan_node::{BatchExchange, PlanNodeType};
use crate::optimizer::plan_visitor::has_batch_source;
use crate::optimizer::property::Distribution;
use crate::utils::Condition;
use crate::WithOptions;

/// `PlanRoot` is used to describe a plan. planner will construct a `PlanRoot` with `LogicalNode`.
/// and required distribution and order. And `PlanRoot` can generate corresponding streaming or
/// batch plan with optimization. the required Order and Distribution columns might be more than the
/// output columns. for example:
/// ```sql
///    select v1 from t order by id;
/// ```
/// the plan will return two columns (id, v1), and the required order column is id. the id
/// column is required in optimization, but the final generated plan will remove the unnecessary
/// column in the result.
#[derive(Debug, Clone)]
pub struct PlanRoot {
    plan: PlanRef,
    required_dist: RequiredDist,
    required_order: Order,
    out_fields: FixedBitSet,
    out_names: Vec<String>,
    schema: Schema,
}

impl PlanRoot {
    pub fn new(
        plan: PlanRef,
        required_dist: RequiredDist,
        required_order: Order,
        out_fields: FixedBitSet,
        out_names: Vec<String>,
    ) -> Self {
        let input_schema = plan.schema();
        assert_eq!(input_schema.fields().len(), out_fields.len());
        assert_eq!(out_fields.count_ones(..), out_names.len());

        let schema = Schema {
            fields: out_fields
                .ones()
                .zip_eq(&out_names)
                .map(|(i, name)| {
                    let mut f = input_schema.fields()[i].clone();
                    f.name = name.clone();
                    f
                })
                .collect(),
        };
        Self {
            plan,
            required_dist,
            required_order,
            out_fields,
            out_names,
            schema,
        }
    }

    /// Get a reference to the plan root's schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get out fields of the plan root.
    pub fn out_fields(&self) -> &FixedBitSet {
        &self.out_fields
    }

    /// Transform the [`PlanRoot`] back to a [`PlanRef`] suitable to be used as a subplan, for
    /// example as insert source or subquery. This ignores Order but retains post-Order pruning
    /// (`out_fields`).
    pub fn into_subplan(self) -> PlanRef {
        if self.out_fields.count_ones(..) == self.out_fields.len() {
            return self.plan;
        }
        LogicalProject::with_out_fields(self.plan, &self.out_fields).into()
    }

    fn optimize_by_rules(
        &self,
        plan: PlanRef,
        stage_name: String,
        rules: Vec<BoxedRule>,
        apply_order: ApplyOrder,
    ) -> PlanRef {
        let mut heuristic_optimizer = HeuristicOptimizer::new(&apply_order, &rules);
        let plan = heuristic_optimizer.optimize(plan);
        let stats = heuristic_optimizer.get_stats();

        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();
        if explain_trace && stats.has_applied_rule() {
            ctx.trace(format!("{}:", stage_name));
            ctx.trace(format!("{}", stats));
            ctx.trace(plan.explain_to_string().unwrap());
        }

        plan
    }

    fn optimize_by_rules_until_fix_point(
        &self,
        plan: PlanRef,
        stage_name: String,
        rules: Vec<BoxedRule>,
        apply_order: ApplyOrder,
    ) -> PlanRef {
        let mut output_plan = plan;
        loop {
            let mut heuristic_optimizer = HeuristicOptimizer::new(&apply_order, &rules);
            output_plan = heuristic_optimizer.optimize(output_plan);
            let stats = heuristic_optimizer.get_stats();

            let ctx = output_plan.ctx();
            let explain_trace = ctx.is_explain_trace();
            if explain_trace && stats.has_applied_rule() {
                ctx.trace(format!("{}:", stage_name));
                ctx.trace(format!("{}", stats));
                ctx.trace(output_plan.explain_to_string().unwrap());
            }

            if !stats.has_applied_rule() {
                return output_plan;
            }
        }
    }

    /// Apply logical optimization to the plan.
    pub fn gen_optimized_logical_plan(&self) -> Result<PlanRef> {
        let mut plan = self.plan.clone();
        let ctx = plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        if explain_trace {
            ctx.trace("Begin:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // Simple Unnesting.
        plan = self.optimize_by_rules(
            plan,
            "Simple Unnesting".to_string(),
            vec![
                // Eliminate max one row
                MaxOneRowEliminateRule::create(),
                // Convert apply to join.
                ApplyToJoinRule::create(),
                // Pull correlated predicates up the algebra tree to unnest simple subquery.
                PullUpCorrelatedPredicateRule::create(),
            ],
            ApplyOrder::TopDown,
        );
        if HasMaxOneRowApply().visit(plan.clone()) {
            return Err(ErrorCode::InternalError(
                "Scalar subquery might produce more than one row.".into(),
            )
            .into());
        }

        // General Unnesting.
        // Translate Apply, push Apply down the plan and finally replace Apply with regular inner
        // join.
        plan = self.optimize_by_rules(
            plan,
            "General Unnesting(Translate Apply)".to_string(),
            vec![TranslateApplyRule::create()],
            ApplyOrder::BottomUp,
        );
        plan = self.optimize_by_rules_until_fix_point(
            plan,
            "General Unnesting(Push Down Apply)".to_string(),
            vec![
                ApplyAggRule::create(),
                ApplyFilterRule::create(),
                ApplyProjRule::create(),
                ApplyJoinRule::create(),
                ApplyScanRule::create(),
            ],
            ApplyOrder::TopDown,
        );
        if has_logical_apply(plan.clone()) {
            return Err(ErrorCode::InternalError("Subquery can not be unnested.".into()).into());
        }

        // Predicate Push-down
        plan = plan.predicate_pushdown(Condition::true_cond());
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // Merge inner joins and intermediate filters into multijoin
        // This rule assumes that filters have already been pushed down near to
        // their relevant joins.
        plan = self.optimize_by_rules(
            plan,
            "To MultiJoin".to_string(),
            vec![MergeMultiJoinRule::create()],
            ApplyOrder::TopDown,
        );

        // Reorder multijoin into left-deep join tree.
        plan = self.optimize_by_rules(
            plan,
            "Join Reorder".to_string(),
            vec![ReorderMultiJoinRule::create()],
            ApplyOrder::TopDown,
        );

        // Predicate Push-down: apply filter pushdown rules again since we pullup all join
        // conditions into a filter above the multijoin.
        plan = plan.predicate_pushdown(Condition::true_cond());
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // Push down the calculation of inputs of join's condition.
        plan = self.optimize_by_rules(
            plan,
            "Push Down the Calculation of Inputs of Join's Condition".to_string(),
            vec![PushCalculationOfJoinRule::create()],
            ApplyOrder::TopDown,
        );

        // Prune Columns
        //
        // Currently, the expressions in ORDER BY will be merged into the expressions in SELECT and
        // they shouldn't be a part of output columns, so we use `out_fields` to control the
        // visibility of these expressions. To avoid these expressions being pruned, we can't use
        // `self.out_fields` as `required_cols` here.
        let required_cols = (0..self.plan.schema().len()).collect_vec();
        plan = plan.prune_col(&required_cols);
        // Column pruning may introduce additional projects, and filter can be pushed again.
        if explain_trace {
            ctx.trace("Prune Columns:");
            ctx.trace(plan.explain_to_string().unwrap());
        }
        plan = plan.predicate_pushdown(Condition::true_cond());
        if explain_trace {
            ctx.trace("Predicate Push Down:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // Convert distinct aggregates.
        plan = self.optimize_by_rules(
            plan,
            "Convert Distinct Aggregation".to_string(),
            vec![UnionToDistinctRule::create(), DistinctAggRule::create()],
            ApplyOrder::TopDown,
        );

        plan = self.optimize_by_rules(
            plan,
            "Join Commute".to_string(),
            vec![JoinCommuteRule::create()],
            ApplyOrder::TopDown,
        );

        plan = self.optimize_by_rules(
            plan,
            "Project Remove".to_string(),
            vec![
                // merge should be applied before eliminate
                ProjectMergeRule::create(),
                ProjectEliminateRule::create(),
                // project-join merge should be applied after merge
                // and eliminate
                ProjectJoinRule::create(),
                AggProjectMergeRule::create(),
            ],
            ApplyOrder::BottomUp,
        );

        plan = self.optimize_by_rules(
            plan,
            "Convert Window Aggregation".to_string(),
            vec![
                OverAggToTopNRule::create(),
                ProjectMergeRule::create(),
                ProjectEliminateRule::create(),
            ],
            ApplyOrder::TopDown,
        );
        if has_logical_over_agg(plan.clone()) {
            return Err(ErrorCode::InternalError(format!(
                "OverAgg can not be transformed. Plan:\n{}",
                plan.explain_to_string().unwrap()
            ))
            .into());
        }

        Ok(plan)
    }

    /// Optimize and generate a singleton batch physical plan without exchange nodes.
    fn gen_batch_plan(&mut self) -> Result<PlanRef> {
        // Logical optimization
        let mut plan = self.gen_optimized_logical_plan()?;

        // Convert to physical plan node
        plan = plan.to_batch_with_order_required(&self.required_order)?;

        assert!(*plan.distribution() == Distribution::Single, "{}", plan);
        assert!(!has_batch_exchange(plan.clone()), "{}", plan);

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Physical Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        Ok(plan)
    }

    /// As we always run the root stage locally, we should ensure that singleton table scan is not
    /// the root stage. Returns `true` if we must insert an additional exchange to ensure this.
    fn require_additional_exchange_on_root(plan: PlanRef) -> bool {
        assert_eq!(plan.distribution(), &Distribution::Single);

        !has_batch_exchange(plan.clone()) // there's no (single) exchange
            && ((has_batch_seq_scan(plan.clone()) // but there's a seq scan (which must be single)
            && !has_batch_seq_scan_where(plan.clone(), |s| s.logical().is_sys_table())) // and it's not a system table
            || has_batch_source(plan.clone())) // or there's a source

        // TODO: join between a normal table and a system table is not supported yet
    }

    /// Optimize and generate a batch query plan for distributed execution.
    pub fn gen_batch_distributed_plan(&mut self) -> Result<PlanRef> {
        self.set_required_dist(RequiredDist::single());
        let mut plan = self.gen_batch_plan()?;

        // Convert to distributed plan
        plan = plan.to_distributed_with_required(&self.required_order, &self.required_dist)?;

        // Add Project if the any position of `self.out_fields` is set to zero.
        if self.out_fields.count_ones(..) != self.out_fields.len() {
            plan =
                BatchProject::new(LogicalProject::with_out_fields(plan, &self.out_fields)).into();
        }

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Distributed Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        let insert_exchange = match plan.node_type() {
            // Always insert a exchange singleton for batch dml.
            PlanNodeType::BatchInsert | PlanNodeType::BatchDelete | PlanNodeType::BatchUpdate => {
                true
            }
            _ => Self::require_additional_exchange_on_root(plan.clone()),
        };
        if insert_exchange {
            plan =
                BatchExchange::new(plan, self.required_order.clone(), Distribution::Single).into();
        }

        Ok(plan)
    }

    /// Optimize and generate a batch query plan for local execution.
    pub fn gen_batch_local_plan(&mut self) -> Result<PlanRef> {
        let mut plan = self.gen_batch_plan()?;

        // Convert to local plan node
        plan = plan.to_local_with_order_required(&self.required_order)?;

        // We remark that since the `to_local_with_order_required` does not enforce single
        // distribution, we enforce at the root if needed.
        let insert_exchange = match plan.distribution() {
            Distribution::Single => Self::require_additional_exchange_on_root(plan.clone()),
            _ => true,
        };
        if insert_exchange {
            plan =
                BatchExchange::new(plan, self.required_order.clone(), Distribution::Single).into()
        }

        // Add Project if the any position of `self.out_fields` is set to zero.
        if self.out_fields.count_ones(..) != self.out_fields.len() {
            plan =
                BatchProject::new(LogicalProject::with_out_fields(plan, &self.out_fields)).into();
        }

        let ctx = plan.ctx();
        if ctx.is_explain_trace() {
            ctx.trace("To Batch Local Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        Ok(plan)
    }

    /// Generate create index or create materialize view plan.
    fn gen_stream_plan(&mut self) -> Result<PlanRef> {
        let ctx = self.plan.ctx();
        let explain_trace = ctx.is_explain_trace();

        let plan = match self.plan.convention() {
            Convention::Logical => {
                let plan = self.gen_optimized_logical_plan()?;
                let (plan, out_col_change) = plan.logical_rewrite_for_stream()?;

                if explain_trace {
                    ctx.trace("Logical Rewrite For Stream:");
                    ctx.trace(plan.explain_to_string().unwrap());
                }

                self.required_dist =
                    out_col_change.rewrite_required_distribution(&self.required_dist);
                self.required_order = out_col_change
                    .rewrite_required_order(&self.required_order)
                    .unwrap();
                self.out_fields = out_col_change.rewrite_bitset(&self.out_fields);
                self.schema = plan.schema().clone();
                plan.to_stream_with_dist_required(&self.required_dist)
            }
            _ => unreachable!(),
        }?;

        if explain_trace {
            ctx.trace("To Stream Plan:");
            ctx.trace(plan.explain_to_string().unwrap());
        }

        // TODO: enable delta join
        // // Rewrite joins with index to delta join
        // plan = self.optimize_by_rules(
        //     plan,
        //     "To IndexDeltaJoin".to_string(),
        //     vec![IndexDeltaJoinRule::create()],
        //     ApplyOrder::BottomUp,
        // );

        Ok(plan)
    }

    /// Optimize and generate a create materialize view plan.
    pub fn gen_materialize_plan(
        &mut self,
        mv_name: String,
        definition: String,
        col_names: Option<Vec<String>>,
        handle_pk_conflict: bool,
        enable_dml: bool,
        row_id_index: Option<usize>,
    ) -> Result<StreamMaterialize> {
        let out_names = if let Some(col_names) = col_names {
            col_names
        } else {
            self.out_names.clone()
        };
        let mut stream_plan = self.gen_stream_plan()?;
        if enable_dml {
            // Insert a dml executor after the previous exector.
            // FIXME: Store `Field` or `Schema` in `TableSource` to avoid downcasting in the future.
            // Or do we have a better solution to this?
            let logical_source = self.plan.downcast_ref::<LogicalSource>().unwrap();
            let column_descs = logical_source
                .core
                .catalog
                .columns
                .iter()
                .map(|column_catalog| column_catalog.column_desc.clone())
                .collect_vec();
            stream_plan = StreamDml::new(stream_plan, column_descs).into();
        }
        if let Some(row_id_index) = row_id_index {
            // Insert a row id gen eexecutor after the previous executor.
            stream_plan = StreamRowIdGen::new(stream_plan, row_id_index).into();
        }

        StreamMaterialize::create(
            stream_plan,
            mv_name,
            self.required_dist.clone(),
            self.required_order.clone(),
            self.out_fields.clone(),
            out_names,
            false,
            definition,
            handle_pk_conflict,
        )
    }

    /// Optimize and generate a create index plan.
    pub fn gen_create_index_plan(&mut self, mv_name: String) -> Result<StreamMaterialize> {
        let stream_plan = self.gen_stream_plan()?;
        StreamMaterialize::create(
            stream_plan,
            mv_name,
            self.required_dist.clone(),
            self.required_order.clone(),
            self.out_fields.clone(),
            self.out_names.clone(),
            true,
            "".into(),
            false,
        )
    }

    /// Optimize and generate a create sink plan.
    pub fn gen_create_sink_plan(
        &mut self,
        sink_name: String,
        definition: String,
        col_names: Vec<String>,
        properties: WithOptions,
    ) -> Result<StreamSink> {
        let stream_plan = self.gen_stream_plan()?;
        StreamMaterialize::create(
            stream_plan,
            sink_name,
            self.required_dist.clone(),
            self.required_order.clone(),
            self.out_fields.clone(),
            col_names,
            false,
            definition,
            false,
        )
        .map(|plan| plan.rewrite_into_sink(properties))
    }

    /// Set the plan root's required dist.
    pub fn set_required_dist(&mut self, required_dist: RequiredDist) {
        self.required_dist = required_dist;
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::optimizer::plan_node::LogicalValues;
    use crate::session::OptimizerContext;

    #[tokio::test]
    async fn test_as_subplan() {
        let ctx = OptimizerContext::mock().await;
        let values = LogicalValues::new(
            vec![],
            Schema::new(vec![
                Field::with_name(DataType::Int32, "v1"),
                Field::with_name(DataType::Varchar, "v2"),
            ]),
            ctx,
        )
        .into();
        let out_fields = FixedBitSet::with_capacity_and_blocks(2, [1]);
        let out_names = vec!["v1".into()];
        let root = PlanRoot::new(
            values,
            RequiredDist::Any,
            Order::any(),
            out_fields,
            out_names,
        );
        let subplan = root.into_subplan();
        assert_eq!(
            subplan.schema(),
            &Schema::new(vec![Field::with_name(DataType::Int32, "v1"),])
        );
    }
}
