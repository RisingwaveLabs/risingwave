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

use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::Schema;

use crate::expr::{
    ExprRewriter, ExprType, ExprVisitor, FunctionCall, InequalityInputPair, InputRef,
    InputRefDisplay,
};
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// The join predicate used in optimizer
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EqJoinPredicate {
    /// Other conditions, linked with `AND` conjunction.
    other_cond: Condition,

    /// `Vec` of `(left_col_index, right_col_index, null_safe)`,
    /// representing a conjunction of `left_col_index = right_col_index`
    ///
    /// Note: `right_col_index` starts from `left_cols_num`
    eq_keys: Vec<(InputRef, InputRef, bool)>,

    left_cols_num: usize,
    right_cols_num: usize,
}

impl fmt::Display for EqJoinPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        let mut eq_keys = self.eq_keys().iter();
        if let Some((k1, k2, null_safe)) = eq_keys.next() {
            write!(
                f,
                "{} {} {}",
                k1,
                if *null_safe {
                    "IS NOT DISTINCT FROM"
                } else {
                    "="
                },
                k2
            )?;
        }
        for (k1, k2, null_safe) in eq_keys {
            write!(
                f,
                "AND {} {} {}",
                k1,
                if *null_safe {
                    "IS NOT DISTINCT FROM"
                } else {
                    "="
                },
                k2
            )?;
        }
        if !self.other_cond.always_true() {
            write!(f, " AND {}", self.other_cond)?;
        }

        Ok(())
    }
}

impl EqJoinPredicate {
    /// The new method for `JoinPredicate` without any analysis, check or rewrite.
    pub fn new(
        other_cond: Condition,
        eq_keys: Vec<(InputRef, InputRef, bool)>,
        left_cols_num: usize,
        right_cols_num: usize,
    ) -> Self {
        Self {
            other_cond,
            eq_keys,
            left_cols_num,
            right_cols_num,
        }
    }

    /// `create` will analyze the on clause condition and construct a `JoinPredicate`.
    /// e.g.
    /// ```sql
    ///   select a.v1, a.v2, b.v1, b.v2 from a,b on a.v1 = a.v2 and a.v1 = b.v1 and a.v2 > b.v2
    /// ```
    /// will call the `create` function with `left_colsnum` = 2 and `on_clause` is (supposed
    /// `input_ref` count start from 0)
    /// ```sql
    /// input_ref(0) = input_ref(1) and input_ref(0) = input_ref(2) and input_ref(1) > input_ref(3)
    /// ```
    /// And the `create functions` should return `JoinPredicate`
    /// ```sql
    ///   other_conds = Vec[input_ref(0) = input_ref(1), input_ref(1) > input_ref(3)],
    ///   keys= Vec[(0,2)]
    /// ```
    pub fn create(left_cols_num: usize, right_cols_num: usize, on_clause: Condition) -> Self {
        let (eq_keys, other_cond) = on_clause.split_eq_keys(left_cols_num, right_cols_num);
        Self::new(other_cond, eq_keys, left_cols_num, right_cols_num)
    }

    /// Get join predicate's eq conds.
    pub fn eq_cond(&self) -> Condition {
        Condition {
            conjunctions: self
                .eq_keys
                .iter()
                .cloned()
                .map(|(l, r, null_safe)| {
                    FunctionCall::new(
                        if null_safe {
                            ExprType::IsNotDistinctFrom
                        } else {
                            ExprType::Equal
                        },
                        vec![l.into(), r.into()],
                    )
                    .unwrap()
                    .into()
                })
                .collect(),
        }
    }

    pub fn non_eq_cond(&self) -> Condition {
        self.other_cond.clone()
    }

    pub fn all_cond(&self) -> Condition {
        let cond = self.eq_cond();
        cond.and(self.non_eq_cond())
    }

    pub fn has_eq(&self) -> bool {
        !self.eq_keys.is_empty()
    }

    pub fn has_non_eq(&self) -> bool {
        !self.other_cond.always_true()
    }

    /// Get a reference to the join predicate's other cond.
    pub fn other_cond(&self) -> &Condition {
        &self.other_cond
    }

    /// Get a mutable reference to the join predicate's other cond.
    pub fn other_cond_mut(&mut self) -> &mut Condition {
        &mut self.other_cond
    }

    /// Get the equal predicate
    pub fn eq_predicate(&self) -> Self {
        Self {
            other_cond: Condition::true_cond(),
            eq_keys: self.eq_keys.clone(),
            left_cols_num: self.left_cols_num,
            right_cols_num: self.right_cols_num,
        }
    }

    /// Get a reference to the join predicate's eq keys.
    ///
    /// Note: `right_col_index` starts from `left_cols_num`
    pub fn eq_keys(&self) -> &[(InputRef, InputRef, bool)] {
        self.eq_keys.as_ref()
    }

    /// `Vec` of `(left_col_index, right_col_index)`.
    ///
    /// Note: `right_col_index` starts from `0`
    pub fn eq_indexes(&self) -> Vec<(usize, usize)> {
        self.eq_keys
            .iter()
            .map(|(left, right, _)| (left.index(), right.index() - self.left_cols_num))
            .collect()
    }

    pub(crate) fn inequality_pairs(&self) -> (usize, Vec<(usize, InequalityInputPair)>) {
        (
            self.left_cols_num,
            self.other_cond()
                .extract_inequality_keys(self.left_cols_num, self.right_cols_num),
        )
    }

    /// Note: `right_col_index` starts from `0`
    pub fn eq_indexes_typed(&self) -> Vec<(InputRef, InputRef)> {
        self.eq_keys
            .iter()
            .cloned()
            .map(|(left, mut right, _)| {
                right.index -= self.left_cols_num;
                (left, right)
            })
            .collect()
    }

    pub fn eq_keys_are_type_aligned(&self) -> bool {
        let mut aligned = true;
        for (l, r, _) in &self.eq_keys {
            aligned &= l.data_type == r.data_type;
        }
        aligned
    }

    pub fn left_eq_indexes(&self) -> Vec<usize> {
        self.eq_keys
            .iter()
            .map(|(left, _, _)| left.index())
            .collect()
    }

    /// Note: `right_col_index` starts from `0`
    pub fn right_eq_indexes(&self) -> Vec<usize> {
        self.eq_keys
            .iter()
            .map(|(_, right, _)| right.index() - self.left_cols_num)
            .collect()
    }

    pub fn null_safes(&self) -> Vec<bool> {
        self.eq_keys
            .iter()
            .map(|(_, _, null_safe)| *null_safe)
            .collect()
    }

    /// return the eq columns index mapping from right inputs to left inputs
    pub fn r2l_eq_columns_mapping(
        &self,
        left_cols_num: usize,
        right_cols_num: usize,
    ) -> ColIndexMapping {
        let mut map = vec![None; right_cols_num];
        for (left, right, _) in self.eq_keys() {
            map[right.index - left_cols_num] = Some(left.index);
        }
        ColIndexMapping::new(map, left_cols_num)
    }

    /// return the eq columns index mapping from left inputs to right inputs
    pub fn l2r_eq_columns_mapping(
        &self,
        left_cols_num: usize,
        right_cols_num: usize,
    ) -> ColIndexMapping {
        let mut map = vec![None; left_cols_num];
        for (left, right, _) in self.eq_keys() {
            map[left.index] = Some(right.index - left_cols_num);
        }
        ColIndexMapping::new(map, right_cols_num)
    }

    /// Reorder the `eq_keys` according to the `reorder_idx`.
    pub fn reorder(self, reorder_idx: &[usize]) -> Self {
        assert!(reorder_idx.len() <= self.eq_keys.len());
        let mut new_eq_keys = Vec::with_capacity(self.eq_keys.len());
        for idx in reorder_idx {
            new_eq_keys.push(self.eq_keys[*idx].clone());
        }
        for idx in 0..self.eq_keys.len() {
            if !reorder_idx.contains(&idx) {
                new_eq_keys.push(self.eq_keys[idx].clone());
            }
        }

        Self::new(
            self.other_cond,
            new_eq_keys,
            self.left_cols_num,
            self.right_cols_num,
        )
    }

    /// Retain the prefix of `eq_keys` based on the `prefix_len`. The other part is moved to the
    /// other condition.
    pub fn retain_prefix_eq_key(self, prefix_len: usize) -> Self {
        assert!(prefix_len <= self.eq_keys.len());
        let (retain_eq_key, other_eq_key) = self.eq_keys.split_at(prefix_len);
        let mut new_other_conjunctions = self.other_cond.conjunctions;
        new_other_conjunctions.extend(
            other_eq_key
                .iter()
                .cloned()
                .map(|(l, r, null_safe)| {
                    FunctionCall::new(
                        if null_safe {
                            ExprType::IsNotDistinctFrom
                        } else {
                            ExprType::Equal
                        },
                        vec![l.into(), r.into()],
                    )
                    .unwrap()
                    .into()
                })
                .collect_vec(),
        );

        let new_other_cond = Condition {
            conjunctions: new_other_conjunctions,
        };

        Self::new(
            new_other_cond,
            retain_eq_key.to_owned(),
            self.left_cols_num,
            self.right_cols_num,
        )
    }

    pub fn rewrite_exprs(&self, rewriter: &mut (impl ExprRewriter + ?Sized)) -> Self {
        let mut new = self.clone();
        new.other_cond = new.other_cond.rewrite_expr(rewriter);
        new
    }

    pub fn visit_exprs(&self, v: &mut (impl ExprVisitor + ?Sized)) {
        self.other_cond.visit_expr(v);
    }
}

pub struct EqJoinPredicateDisplay<'a> {
    pub eq_join_predicate: &'a EqJoinPredicate,
    pub input_schema: &'a Schema,
}

impl EqJoinPredicateDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        let that = self.eq_join_predicate;
        let mut eq_keys = that.eq_keys().iter();
        if let Some((k1, k2, null_safe)) = eq_keys.next() {
            write!(
                f,
                "{} {} {}",
                InputRefDisplay {
                    input_ref: k1,
                    input_schema: self.input_schema
                },
                if *null_safe {
                    "IS NOT DISTINCT FROM"
                } else {
                    "="
                },
                InputRefDisplay {
                    input_ref: k2,
                    input_schema: self.input_schema
                }
            )?;
        }
        for (k1, k2, null_safe) in eq_keys {
            write!(
                f,
                " AND {} {} {}",
                InputRefDisplay {
                    input_ref: k1,
                    input_schema: self.input_schema
                },
                if *null_safe {
                    "IS NOT DISTINCT FROM"
                } else {
                    "="
                },
                InputRefDisplay {
                    input_ref: k2,
                    input_schema: self.input_schema
                }
            )?;
        }
        if !that.other_cond.always_true() {
            write!(
                f,
                " AND {}",
                ConditionDisplay {
                    condition: &that.other_cond,
                    input_schema: self.input_schema
                }
            )?;
        }

        Ok(())
    }
}

impl fmt::Display for EqJoinPredicateDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt(f)
    }
}

impl fmt::Debug for EqJoinPredicateDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt(f)
    }
}
