// Copyright 2023 RisingWave Labs
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

use std::collections::HashSet;
use std::ops::{Mul, Sub};

use risingwave_pb::plan_common::JoinType;

use self::card::Cardinality;
use super::{DefaultBehavior, DefaultValue, PlanVisitor};
use crate::catalog::system_catalog::pg_catalog::PG_NAMESPACE_TABLE_NAME;
use crate::optimizer::plan_node::generic::Limit;
use crate::optimizer::plan_node::{self, PlanTreeNode, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanRef;

pub mod card {
    use std::cmp::{max, min, Ordering};
    use std::ops::{Add, Mul, Sub};

    /// The upper bound of the [`Cardinality`].
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Hi {
        Limited(usize),
        Unlimited,
    }

    impl PartialOrd for Hi {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for Hi {
        fn cmp(&self, other: &Self) -> Ordering {
            match (self, other) {
                (Self::Unlimited, Self::Unlimited) => Ordering::Equal,
                (Self::Unlimited, Self::Limited(_)) => Ordering::Greater,
                (Self::Limited(_), Self::Unlimited) => Ordering::Less,
                (Self::Limited(lhs), Self::Limited(rhs)) => lhs.cmp(rhs),
            }
        }
    }

    impl From<Option<usize>> for Hi {
        fn from(value: Option<usize>) -> Self {
            value.map_or(Self::Unlimited, Self::Limited)
        }
    }

    impl From<usize> for Hi {
        fn from(value: usize) -> Self {
            Self::Limited(value)
        }
    }

    /// The cardinality of the output rows of a plan node. Bounds are inclusive.
    ///
    /// The default value is `0..`, i.e. the number of rows is unknown.
    // TODO: Make this the property of each plan node.
    #[derive(Clone, Copy, Debug)]
    pub struct Cardinality {
        lo: usize,
        hi: Hi,
    }

    impl Default for Cardinality {
        fn default() -> Self {
            Self {
                lo: 0,
                hi: None.into(),
            }
        }
    }

    impl<T> From<T> for Cardinality
    where
        T: std::ops::RangeBounds<usize>,
    {
        fn from(value: T) -> Self {
            let lo = match value.start_bound() {
                std::ops::Bound::Included(lo) => *lo,
                std::ops::Bound::Excluded(lo) => lo.saturating_add(1),
                std::ops::Bound::Unbounded => 0,
            };
            let hi = match value.end_bound() {
                std::ops::Bound::Included(hi) => Some(*hi),
                std::ops::Bound::Excluded(hi) => Some(hi.saturating_sub(1)),
                std::ops::Bound::Unbounded => None,
            };
            Self::new(lo, hi)
        }
    }

    impl Cardinality {
        /// Creates a new [`Cardinality`] with the given lower and upper bounds.
        pub fn new(lo: usize, hi: impl Into<Hi>) -> Self {
            let hi: Hi = hi.into();
            debug_assert!(hi >= Hi::from(lo));

            Self { lo, hi }
        }

        /// Returns the lower bound of the cardinality.
        pub fn lo(self) -> usize {
            self.lo
        }

        /// Returns the upper bound of the cardinality, `None` if the upper bound is unlimited.
        pub fn hi(self) -> Option<usize> {
            match self.hi {
                Hi::Limited(hi) => Some(hi),
                Hi::Unlimited => None,
            }
        }

        /// Returns the minimum of the two cardinalities, where the lower and upper bounds are
        /// respectively the minimum of the lower and upper bounds of the two cardinalities.
        pub fn min(self, rhs: impl Into<Self>) -> Self {
            let rhs = rhs.into();
            Self::new(min(self.lo(), rhs.lo()), min(self.hi, rhs.hi))
        }

        /// Returns the maximum of the two cardinalities, where the lower and upper bounds are
        /// respectively the maximum of the lower and upper bounds of the two cardinalities.
        pub fn max(self, rhs: impl Into<Self>) -> Self {
            let rhs = rhs.into();
            Self::new(max(self.lo(), rhs.lo()), max(self.hi, rhs.hi))
        }

        /// Returns the cardinality with the upper bounds limited to `limit`. The lower bound will
        /// also be limited to `limit` if it is greater than `limit`.
        pub fn limit_ub_to(self, limit: usize) -> Self {
            self.min(limit..=limit)
        }

        /// Returns the cardinality with the lower bound limited to `limit`, upper bound unchanged.
        pub fn limit_lb_to(self, limit: usize) -> Self {
            self.min(limit..)
        }
    }

    impl Add<Cardinality> for Cardinality {
        type Output = Self;

        /// Returns the sum of the two cardinalities, where the lower and upper bounds are
        /// respectively the sum of the lower and upper bounds of the two cardinalities.
        fn add(self, rhs: Self) -> Self::Output {
            let lo = self.lo().saturating_add(rhs.lo());
            let hi = if let (Some(lhs), Some(rhs)) = (self.hi(), rhs.hi()) {
                lhs.checked_add(rhs)
            } else {
                None
            };
            Self::new(lo, hi)
        }
    }

    impl Sub<usize> for Cardinality {
        type Output = Self;

        /// Returns the cardinality with both lower and upper bounds subtracted by `rhs`.
        fn sub(self, rhs: usize) -> Self::Output {
            let lo = self.lo().saturating_sub(rhs);
            let hi = self.hi().map(|hi| hi.saturating_sub(rhs));
            Self::new(lo, hi)
        }
    }

    impl Mul<Cardinality> for Cardinality {
        type Output = Cardinality;

        /// Returns the product of the two cardinalities, where the lower and upper bounds are
        /// respectively the product of the lower and upper bounds of the two cardinalities.
        fn mul(self, rhs: Cardinality) -> Self::Output {
            let lo = self.lo().saturating_mul(rhs.lo());
            let hi = if let (Some(lhs), Some(rhs)) = (self.hi(), rhs.hi()) {
                lhs.checked_mul(rhs)
            } else {
                None
            };
            Self::new(lo, hi)
        }
    }

    impl Mul<usize> for Cardinality {
        type Output = Self;

        /// Returns the cardinality with both lower and upper bounds multiplied by `rhs`.
        fn mul(self, rhs: usize) -> Self::Output {
            let lo = self.lo().saturating_mul(rhs);
            let hi = self.hi().and_then(|hi| hi.checked_mul(rhs));
            Self::new(lo, hi)
        }
    }

    impl Cardinality {
        /// Returns the cardinality if it is exact, `None` otherwise.
        pub fn get_exact(self) -> Option<usize> {
            self.hi().filter(|hi| *hi == self.lo())
        }

        /// Returns `true` if the cardinality is at most `count` rows.
        pub fn is_at_most(self, count: usize) -> bool {
            self.hi().is_some_and(|hi| hi <= count)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_limit_ub_to() {
            let c = Cardinality::new(5, None);
            let c1 = c.limit_ub_to(3);
            assert_eq!(c1.lo(), 3);
            assert_eq!(c1.hi(), Some(3));
            assert_eq!(c1.get_exact(), Some(3));

            let c = Cardinality::new(5, None);
            let c1 = c.limit_ub_to(10);
            assert_eq!(c1.lo(), 5);
            assert_eq!(c1.hi(), Some(10));
            assert_eq!(c1.get_exact(), None);
        }

        #[test]
        fn test_limit_lb_to() {
            let c = Cardinality::new(5, None);
            let c1 = c.limit_lb_to(3);
            assert_eq!(c1.lo(), 3);
            assert_eq!(c1.hi(), None);

            let c = Cardinality::new(5, 10);
            let c1 = c.limit_lb_to(3);
            assert_eq!(c1.lo(), 3);
            assert_eq!(c1.hi(), Some(10));
        }

        #[test]
        fn test_ops() {
            // Sub
            let c = Cardinality::new(5, 10);
            let c1 = c.sub(7);
            assert_eq!(c1.lo(), 0);
            assert_eq!(c1.hi(), Some(3));

            // Add
            let c = Cardinality::new(5, 10);
            let c1 = Cardinality::new(10, None);
            let c2 = c + c1;
            assert_eq!(c2.lo(), 15);
            assert_eq!(c2.hi(), None);

            let c = Cardinality::new(5, usize::MAX - 1);
            let c1 = (2..=2).into();
            let c2 = c + c1;
            assert_eq!(c2.lo(), 7);
            assert_eq!(c2.hi(), None);

            // Mul
            let c = Cardinality::new(5, usize::MAX - 1);
            let c1 = c * 2;
            assert_eq!(c1.lo(), 10);
            assert_eq!(c1.hi(), None);

            let c = Cardinality::new(3, 5);
            let c1 = Cardinality::new(2, 4);
            let c2 = c * c1;
            assert_eq!(c2.lo(), 6);
            assert_eq!(c2.hi(), Some(20));
        }
    }
}

/// A visitor that computes the cardinality of a plan node.
pub struct CardinalityVisitor;

impl PlanVisitor<Cardinality> for CardinalityVisitor {
    type DefaultBehavior = impl DefaultBehavior<Cardinality>;

    fn default_behavior() -> Self::DefaultBehavior {
        // returns unknown cardinality for default behavior, which is always correct
        DefaultValue
    }

    fn visit_logical_values(&mut self, plan: &plan_node::LogicalValues) -> Cardinality {
        {
            let count = plan.rows().len();
            (count..=count).into()
        }
    }

    fn visit_logical_agg(&mut self, plan: &plan_node::LogicalAgg) -> Cardinality {
        let input = self.visit(plan.input());

        if plan.group_key().is_empty() {
            input.limit_ub_to(1)
        } else {
            input.limit_lb_to(1)
        }
    }

    fn visit_logical_limit(&mut self, plan: &plan_node::LogicalLimit) -> Cardinality {
        self.visit(plan.input()).limit_ub_to(plan.limit() as usize)
    }

    fn visit_logical_project(&mut self, plan: &plan_node::LogicalProject) -> Cardinality {
        self.visit(plan.input())
    }

    fn visit_logical_top_n(&mut self, plan: &plan_node::LogicalTopN) -> Cardinality {
        let input = self.visit(plan.input());

        match plan.limit_attr() {
            Limit::Simple(limit) => input
                .sub(plan.offset() as usize)
                .limit_ub_to(limit as usize),
            Limit::WithTies(limit) => {
                assert_eq!(plan.offset(), 0, "ties with offset is not supported yet");
                input.limit_lb_to(limit as usize)
            }
        }
    }

    fn visit_logical_filter(&mut self, plan: &plan_node::LogicalFilter) -> Cardinality {
        let input = plan.input();
        let eq_set: HashSet<_> = plan
            .predicate()
            .collect_input_refs(input.schema().len())
            .ones()
            .collect();

        let mut unique_keys: Vec<HashSet<_>> = vec![input.logical_pk().iter().copied().collect()];

        // We don't have UNIQUE key now. So we hack here to support some complex queries on
        // system tables.
        if let Some(scan) = input.as_logical_scan() && scan.is_sys_table() && scan.table_name() == PG_NAMESPACE_TABLE_NAME {
            let nspname = scan.output_col_idx().iter().find(|i| scan.table_desc().columns[**i].name == "nspname").unwrap();
            unique_keys.push([*nspname].into_iter().collect());
        }

        let input = self.visit(input);
        if unique_keys
            .iter()
            .any(|unique_key| eq_set.is_superset(unique_key))
        {
            (0..=1).into()
        } else {
            input.limit_lb_to(0)
        }
    }

    fn visit_logical_union(&mut self, plan: &plan_node::LogicalUnion) -> Cardinality {
        if plan.all() {
            plan.inputs()
                .into_iter()
                .map(|input| self.visit(input))
                .fold(Cardinality::default(), std::ops::Add::add)
        } else {
            Cardinality::default()
        }
    }

    fn visit_logical_join(&mut self, plan: &plan_node::LogicalJoin) -> Cardinality {
        let left = self.visit(plan.left());
        let right = self.visit(plan.right());

        match plan.join_type() {
            JoinType::Unspecified => unreachable!(),

            // lb = 0, ub multiplied
            JoinType::Inner => left.mul(right).limit_lb_to(0),

            // Each row of some side matches at least one row from the other side or `NULL`.
            // lb = lb of Preserved Row table,
            // ub multiplied
            JoinType::LeftOuter => left.mul(right).limit_lb_to(left.lo()),
            JoinType::RightOuter => right.mul(left).limit_ub_to(right.lo()),

            // Rows in the result set must be in some side.
            // lb = 0, ub unchanged
            JoinType::LeftSemi | JoinType::LeftAnti => left.limit_lb_to(0),
            JoinType::RightSemi | JoinType::RightAnti => right.limit_lb_to(0),

            // TODO: refine the cardinality of full outer join
            JoinType::FullOuter => Cardinality::default(),
        }
    }

    fn visit_logical_now(&mut self, _plan: &plan_node::LogicalNow) -> Cardinality {
        (1..=1).into()
    }

    fn visit_logical_expand(&mut self, plan: &plan_node::LogicalExpand) -> Cardinality {
        self.visit(plan.input()) * plan.column_subsets().len()
    }
}

#[easy_ext::ext(LogicalCardinalityExt)]
pub impl PlanRef {
    /// Returns `true` if the plan node is guaranteed to yield at most one row.
    fn max_one_row(&self) -> bool {
        CardinalityVisitor.visit(self.clone()).is_at_most(1)
    }

    /// Returns the number of rows the plan node is guaranteed to yield, if known exactly.
    fn row_count(&self) -> Option<usize> {
        CardinalityVisitor.visit(self.clone()).get_exact()
    }
}
