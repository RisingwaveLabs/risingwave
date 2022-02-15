use std::fmt;

use fixedbitset::FixedBitSet;
use risingwave_common::catalog::Schema;

use super::{BatchLimit, ColPrunable, IntoPlanRef, PlanRef, PlanTreeNodeUnary, ToBatch, ToStream};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct LogicalLimit {
    input: PlanRef,
    limit: usize,
    offset: usize,
    schema: Schema,
}

impl LogicalLimit {
    fn new(input: PlanRef, limit: usize, offset: usize) -> Self {
        let schema = input.schema().clone();
        LogicalLimit {
            input,
            limit,
            offset,
            schema,
        }
    }

    /// the function will check if the cond is bool expression
    pub fn create(input: PlanRef, limit: usize, offset: usize) -> PlanRef {
        Self::new(input, limit, offset).into_plan_ref()
    }
}
impl PlanTreeNodeUnary for LogicalLimit {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.limit, self.offset)
    }
}
impl_plan_tree_node_for_unary! {LogicalLimit}
impl fmt::Display for LogicalLimit {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
impl WithOrder for LogicalLimit {}
impl WithDistribution for LogicalLimit {}
impl WithSchema for LogicalLimit {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}
impl ColPrunable for LogicalLimit {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        let new_input = self.input.prune_col(required_cols);
        self.clone_with_input(new_input).into_plan_ref()
    }
}
impl ToBatch for LogicalLimit {
    fn to_batch(&self) -> PlanRef {
        let new_input = self.input().to_batch();
        let new_logical = self.clone_with_input(new_input);
        BatchLimit::new(new_logical).into_plan_ref()
    }
}
impl ToStream for LogicalLimit {
    fn to_stream(&self) -> PlanRef {
        panic!("there is no limit stream operator");
    }
}
