use fixedbitset::FixedBitSet;
use paste::paste;

use super::*;
use crate::expr::{ExprVisitor, InputRef};
use crate::{for_batch_plan_nodes, for_stream_plan_nodes};

/// The trait for column pruning, only logical plan node will use it, though all plan node impl it.
pub trait ColPrunable {
    /// transform the plan node to only output the required columns ordered by index number.
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef;
}

/// impl ColPrunable for batch and streaming node.
macro_rules! ban_prune_col {
  ([], $( { $convention:ident, $name:ident }),*) => {
    paste!{
      $(impl ColPrunable for [<$convention $name>] {
        fn prune_col(&self, _required_cols: &FixedBitSet) -> PlanRef {
          panic!("column pruning is only allowed on logical plan")
        }
     })*
    }
  }
}
for_batch_plan_nodes! { ban_prune_col }
for_stream_plan_nodes! { ban_prune_col }

/// Union all `InputRef`s in the expression with the initial `required_cols`.
pub struct CollectRequiredCols {
    pub required_cols: FixedBitSet,
}

impl ExprVisitor for CollectRequiredCols {
    fn visit_input_ref(&mut self, expr: &InputRef) {
        self.required_cols.insert(expr.index());
    }
}
