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

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{SetExpr, SetOperator};

use crate::binder::{BindContext, Binder, BoundQuery, BoundSelect, BoundValues};
use crate::expr::{CorrelatedId, Depth};

/// Part of a validated query, without order or limit clause. It may be composed of smaller
/// `BoundSetExpr`s via set operators (e.g. union).
#[derive(Debug, Clone)]
pub enum BoundSetExpr {
    Select(Box<BoundSelect>),
    Query(Box<BoundQuery>),
    Values(Box<BoundValues>),
    /// UNION/EXCEPT/INTERSECT of two queries
    SetOperation {
        op: BoundSetOperation,
        all: bool,
        left: Box<BoundSetExpr>,
        right: Box<BoundSetExpr>,
    },
}

#[derive(Debug, Clone)]
pub enum BoundSetOperation {
    UNION,
    Except,
    Intersect,
}

impl BoundSetExpr {
    /// The schema returned by this [`BoundSetExpr`].

    pub fn schema(&self) -> &Schema {
        match self {
            BoundSetExpr::Select(s) => s.schema(),
            BoundSetExpr::Values(v) => v.schema(),
            BoundSetExpr::Query(q) => q.schema(),
            BoundSetExpr::SetOperation { left, .. } => left.schema(),
        }
    }

    pub fn is_correlated(&self) -> bool {
        match self {
            BoundSetExpr::Select(s) => s.is_correlated(),
            BoundSetExpr::Values(v) => v.is_correlated(),
            BoundSetExpr::Query(q) => q.is_correlated(),
            BoundSetExpr::SetOperation { left, right, .. } => {
                left.is_correlated() || right.is_correlated()
            }
        }
    }

    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        match self {
            BoundSetExpr::Select(s) => {
                s.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id)
            }
            BoundSetExpr::Values(v) => {
                v.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id)
            }
            BoundSetExpr::Query(q) => {
                q.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id)
            }
            BoundSetExpr::SetOperation { left, right, .. } => {
                let mut correlated_indices = vec![];
                correlated_indices.extend(
                    left.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
                );
                correlated_indices.extend(
                    right.collect_correlated_indices_by_depth_and_assign_id(depth, correlated_id),
                );
                correlated_indices
            }
        }
    }
}

impl Binder {
    pub(super) fn bind_set_expr(&mut self, set_expr: SetExpr) -> Result<BoundSetExpr> {
        match set_expr {
            SetExpr::Select(s) => Ok(BoundSetExpr::Select(Box::new(self.bind_select(*s)?))),
            SetExpr::Values(v) => Ok(BoundSetExpr::Values(Box::new(self.bind_values(v, None)?))),
            SetExpr::Query(q) => Ok(BoundSetExpr::Query(Box::new(self.bind_query(*q)?))),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                match op {
                    SetOperator::Union => {
                        let left = Box::new(self.bind_set_expr(*left)?);
                        let mut new_context = BindContext::default();
                        // Swap context for the right side.
                        std::mem::swap(&mut self.context, &mut new_context);
                        let right = Box::new(self.bind_set_expr(*right)?);
                        // Swap context back to the left side.
                        std::mem::swap(&mut self.context, &mut new_context);
                        Ok(BoundSetExpr::SetOperation {
                            op: BoundSetOperation::UNION,
                            all: all,
                            left: left,
                            right: right,
                        })
                    }
                    SetOperator::Intersect | SetOperator::Except => Err(ErrorCode::NotImplemented(
                        format!("set expr: {:?}", op),
                        None.into(),
                    )
                    .into()),
                }
            }
        }
    }
}
