// Copyright 2024 RisingWave Labs
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

use either::Either;

use crate::binder::bind_context::RecursiveUnion;
use crate::binder::statement::RewriteExprsRecursive;
use crate::binder::{BoundQuery, ShareId};

/// Share a relation during binding and planning.
/// It could be used to share a (recursive) CTE, a source, a view and so on.
#[derive(Debug, Clone)]
pub struct BoundShare {
    pub(crate) share_id: ShareId,
    pub(crate) input: Either<BoundQuery, RecursiveUnion>,
}

impl RewriteExprsRecursive for BoundShare {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl crate::expr::ExprRewriter) {
        match &mut self.input {
            Either::Left(q) => q.rewrite_exprs_recursive(rewriter),
            Either::Right(r) => r.rewrite_exprs_recursive(rewriter),
        };
    }
}
