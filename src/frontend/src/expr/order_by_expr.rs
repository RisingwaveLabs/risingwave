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

use crate::expr::{ExprImpl, ExprRewriter};
use crate::optimizer::property::Direction;

/// A sort expression in the `ORDER BY` clause.
///
/// See also [`bind_order_by_expr`](`crate::binder::Binder::bind_order_by_expr`).
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct OrderByExpr {
    pub expr: ExprImpl,
    pub direction: Direction,
    pub nulls_first: bool,
}

/// See [`OrderByExpr`].
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct OrderBy {
    pub sort_exprs: Vec<OrderByExpr>,
}

impl OrderBy {
    pub fn any() -> Self {
        Self {
            sort_exprs: Vec::new(),
        }
    }

    pub fn new(sort_exprs: Vec<OrderByExpr>) -> Self {
        Self { sort_exprs }
    }

    pub fn rewrite_expr(self, rewriter: &mut (impl ExprRewriter + ?Sized)) -> Self {
        Self {
            sort_exprs: self
                .sort_exprs
                .into_iter()
                .map(|e| OrderByExpr {
                    expr: rewriter.rewrite_expr(e.expr),
                    direction: e.direction,
                    nulls_first: e.nulls_first,
                })
                .collect(),
        }
    }
}
