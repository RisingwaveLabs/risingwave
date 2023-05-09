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

use std::collections::{BTreeSet, VecDeque};

use educe::Educe;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_common_proc_macro::EstimateSize;
use risingwave_expr::function::window::{WindowFuncCall, WindowFuncKind};
use smallvec::SmallVec;

use super::MemcmpEncoded;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

mod buffer;

mod aggregate;
mod lag;
mod lead;

/// Unique and ordered identifier for a row in internal states.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, EstimateSize)]
pub(super) struct StateKey {
    pub order_key: ScalarImpl,
    pub encoded_pk: MemcmpEncoded,
}

#[derive(Debug)]
pub(super) struct StatePos<'a> {
    /// Only 2 cases in which the `key` is `None`:
    /// 1. The state is empty.
    /// 2. It's a pure preceding window, and all ready outputs are consumed.
    pub key: Option<&'a StateKey>,
    pub is_ready: bool,
}

#[derive(Debug)]
pub(super) struct StateOutput {
    /// Window function return value for the current ready window frame.
    pub return_value: Datum,
    /// Hint for the executor to evict unneeded rows from the state table.
    pub evict_hint: StateEvictHint,
}

#[derive(Debug)]
pub(super) enum StateEvictHint {
    /// Use a set instead of a single key to avoid state table iter or too many range delete.
    /// Shouldn't be empty set.
    CanEvict(BTreeSet<StateKey>),
    /// State keys from the specified key are still required, so must be kept in the state table.
    CannotEvict(StateKey),
}

impl StateEvictHint {
    pub fn merge(self, other: StateEvictHint) -> StateEvictHint {
        use StateEvictHint::*;
        match (self, other) {
            (CanEvict(a), CanEvict(b)) => {
                // Example:
                // a = CanEvict({1, 2, 3})
                // b = CanEvict({2, 3, 4})
                // a.merge(b) = CanEvict({1, 2, 3})
                let a_last = a.last().unwrap();
                let b_last = b.last().unwrap();
                let last = std::cmp::min(a_last, b_last).clone();
                CanEvict(
                    a.into_iter()
                        .take_while(|k| k <= &last)
                        .chain(b.into_iter().take_while(|k| k <= &last))
                        .collect(),
                )
            }
            (CannotEvict(a), CannotEvict(b)) => {
                // Example:
                // a = CannotEvict(2), meaning keys < 2 can be evicted
                // b = CannotEvict(3), meaning keys < 3 can be evicted
                // a.merge(b) = CannotEvict(2)
                CannotEvict(std::cmp::min(a, b))
            }
            (CanEvict(mut keys), CannotEvict(still_required))
            | (CannotEvict(still_required), CanEvict(mut keys)) => {
                // Example:
                // a = CanEvict({1, 2, 3})
                // b = CannotEvict(3)
                // a.merge(b) = CanEvict({1, 2})
                keys.split_off(&still_required);
                CanEvict(keys)
            }
        }
    }
}

pub(super) trait WindowState: EstimateSize {
    // TODO(rc): may append rows in batch like in `hash_agg`.
    /// Append a new input row to the state. The `key` is expected to be increasing.
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>);

    /// Get the current window frame position.
    fn curr_window(&self) -> StatePos<'_>;

    // TODO(rc): split `output` into `curr_output` and `slide` to avoid unnecessary computation on
    // recovery.
    /// Return the output for the current ready window frame and push the window forward.
    fn output(&mut self) -> StreamExecutorResult<StateOutput>;
}

pub(super) fn create_window_state(
    call: &WindowFuncCall,
) -> StreamExecutorResult<Box<dyn WindowState + Send>> {
    assert!(call.frame.is_valid());

    use WindowFuncKind::*;
    Ok(match call.kind {
        RowNumber | Rank | DenseRank => {
            return Err(StreamExecutorError::not_implemented(
                format!(
                    "window function `{}` is only supported by converting to TopN",
                    call.kind
                ),
                None,
            ))
        }
        Lag => Box::new(lag::LagState::new(&call.frame)),
        Lead => Box::new(lead::LeadState::new(&call.frame)),
        Aggregate(_) => Box::new(aggregate::AggregateState::new(call)?),
    })
}

#[derive(Educe)]
#[educe(Default)]
pub struct EstimatedVecDeque<T: EstimateSize> {
    inner: VecDeque<T>,
    heap_size: usize,
}

impl<T: EstimateSize> EstimatedVecDeque<T> {
    #[expect(dead_code)]
    pub fn pop_back(&mut self) -> Option<T> {
        self.inner
            .pop_back()
            .inspect(|v| self.heap_size = self.heap_size.saturating_sub(v.estimated_heap_size()))
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.inner
            .pop_front()
            .inspect(|v| self.heap_size = self.heap_size.saturating_sub(v.estimated_heap_size()))
    }

    pub fn push_back(&mut self, value: T) {
        self.heap_size = self.heap_size.saturating_add(value.estimated_heap_size());
        self.inner.push_back(value)
    }

    #[expect(dead_code)]
    pub fn push_front(&mut self, value: T) {
        self.heap_size = self.heap_size.saturating_add(value.estimated_heap_size());
        self.inner.push_front(value)
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.inner.get(index)
    }

    pub fn front(&self) -> Option<&T> {
        self.inner.front()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<T: EstimateSize> std::ops::Index<usize> for EstimatedVecDeque<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.inner[index]
    }
}

impl<T: EstimateSize> EstimateSize for EstimatedVecDeque<T> {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add `VecDeque` internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.heap_size
    }
}
