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

mod column_index_mapping;
use std::any::Any;
use std::hash::{Hash, Hasher};

pub use column_index_mapping::*;
mod condition;
pub use condition::*;
mod connected_components;
pub(crate) use connected_components::*;
mod stream_graph_formatter;
pub use stream_graph_formatter::*;
mod with_options;
pub use with_options::*;
mod rewrite_index;
pub use rewrite_index::*;

use crate::expr::{Expr, ExprImpl, ExprRewriter, InputRef};

/// Substitute `InputRef` with corresponding `ExprImpl`.
pub struct Substitute {
    pub mapping: Vec<ExprImpl>,
}

impl ExprRewriter for Substitute {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        assert_eq!(
            input_ref.return_type(),
            self.mapping[input_ref.index()].return_type(),
            "Type mismatch when substituting {:?} with {:?}",
            input_ref,
            self.mapping[input_ref.index()],
        );
        self.mapping[input_ref.index()].clone()
    }
}

// Workaround object safety rules for Eq and Hash, adopted from
// https://github.com/bevyengine/bevy/blob/f7fbfaf9c72035e98c6b6cec0c7d26ff9f5b1c82/crates/bevy_utils/src/label.rs

/// An object safe version of [`Eq`]. This trait is automatically implemented
/// for any `'static` type that implements `Eq`.
pub trait DynEq: Any {
    fn as_any(&self) -> &dyn Any;
    fn dyn_eq(&self, other: &dyn DynEq) -> bool;
}

impl<T: Any + Eq> DynEq for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_eq(&self, other: &dyn DynEq) -> bool {
        let other = other.as_any().downcast_ref::<T>();
        other.is_some_and(|other| self == other)
    }
}

impl PartialEq<dyn DynEq + 'static> for dyn DynEq {
    fn eq(&self, other: &Self) -> bool {
        self.dyn_eq(other)
    }
}

impl Eq for dyn DynEq {
    fn assert_receiver_is_total_eq(&self) {}
}

/// An object safe version of [`Hash`]. This trait is automatically implemented
/// for any `'static` type that implements `Hash`.
pub trait DynHash: DynEq {
    fn as_dyn_eq(&self) -> &dyn DynEq;
    fn dyn_hash(&self, state: &mut dyn Hasher);
}

impl<T: DynEq + Hash> DynHash for T {
    fn as_dyn_eq(&self) -> &dyn DynEq {
        self
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        T::hash(self, &mut state);
        self.type_id().hash(&mut state);
    }
}

impl Hash for dyn DynHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.dyn_hash(state);
    }
}
