//! Define all [`Rule`]

use super::PlanRef;

/// A one-to-one transform for the PlanNode, every [`Rule`] should downcast and check if the node
/// matches the rule
#[allow(unused)]
#[allow(clippy::result_unit_err)]
pub trait Rule: Send + Sync + 'static {
    /// return err(()) if not match
    fn apply(&self, plan: PlanRef) -> Option<PlanRef>;
}

pub(super) type BoxedRule = Box<dyn Rule>;

mod project_join;
pub use project_join::*;
mod filter_join;
pub use filter_join::*;
mod filter_project;
pub use filter_project::*;
