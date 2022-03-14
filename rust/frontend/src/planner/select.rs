use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use crate::binder::BoundSelect;
use crate::expr::ExprImpl;
pub use crate::optimizer::plan_node::LogicalFilter;
use crate::optimizer::plan_node::{LogicalProject, LogicalValues, PlanRef};
use crate::planner::Planner;
impl Planner {
    pub(super) fn plan_select(&mut self, select: BoundSelect) -> Result<PlanRef> {
        let mut root = match select.from {
            None => self.create_dummy_values()?,
            Some(t) => self.plan_table_ref(t)?,
        };
        root = match select.selection {
            None => root,
            Some(t) => LogicalFilter::create(root, t)?,
        };
        root = self.plan_project(root, select.select_items)?;
        Ok(root)
    }

    /// Helper to create a dummy node as child of LogicalProject.
    /// For example, `select 1+2, 3*4` will be `Project([1+2, 3+4]) - Values([[]])`.
    fn create_dummy_values(&self) -> Result<PlanRef> {
        Ok(LogicalValues::create(
            vec![vec![]],
            Schema::default(),
            self.ctx.clone(),
        ))
    }

    fn plan_project(&mut self, input: PlanRef, project: Vec<ExprImpl>) -> Result<PlanRef> {
        // TODO: support alias.
        let expr_alias = vec![None; project.len()];
        Ok(LogicalProject::create(input, project, expr_alias))
    }
}
