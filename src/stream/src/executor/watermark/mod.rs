// Copyright 2023 Singularity Data
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

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashSet, VecDeque};
use std::hash::Hash;

use super::Watermark;

#[derive(Default)]
pub(super) struct StagedWatermarks {
    in_heap: bool,
    staged: VecDeque<Watermark>,
}

pub(super) struct BufferedWatermarks<ID> {
    /// We store the smallest watermark of each upstream, because the next watermark to emit is
    /// among them.
    pub first_buffered_watermarks: BinaryHeap<Reverse<(Watermark, ID)>>,
    /// We buffer other watermarks of each upstream. The next-to-smallest one will become the
    /// smallest when the smallest is emitted and be moved into heap.
    pub other_buffered_watermarks: BTreeMap<ID, StagedWatermarks>,
}

impl<ID: Ord + Hash> BufferedWatermarks<ID> {
    pub fn with_ids(buffer_ids: Vec<ID>) -> Self {
        BufferedWatermarks {
            first_buffered_watermarks: BinaryHeap::with_capacity(buffer_ids.len()),
            other_buffered_watermarks: BTreeMap::from_iter(
                buffer_ids.into_iter().map(|id| (id, Default::default())),
            ),
        }
    }

    pub fn add_buffers(&mut self, buffer_ids: Vec<ID>) {
        buffer_ids.into_iter().for_each(|id| {
            self.other_buffered_watermarks
                .insert(id, Default::default())
                .unwrap();
        });
    }

    /// Handle a new watermark message. Optionally returns the watermark message to emit and the
    /// buffer id.
    pub fn handle_watermark(
        &mut self,
        buffer_id: ID,
        watermark: Watermark,
    ) -> Option<(Watermark, ID)> {
        // Note: The staged watermark buffer should be created before handling the watermark.
        let mut staged = self.other_buffered_watermarks.get_mut(&buffer_id).unwrap();

        if staged.in_heap {
            staged.staged.push_back(watermark);
            None
        } else {
            staged.in_heap = true;
            self.first_buffered_watermarks
                .push(Reverse((watermark, buffer_id)));
            self.check_watermark_heap()
        }
    }

    /// Check the watermark heap and decide whether to emit a watermark message.
    pub fn check_watermark_heap(&mut self) -> Option<(Watermark, ID)> {
        let len = self.other_buffered_watermarks.len();
        let mut watermark_to_emit = None;
        while !self.first_buffered_watermarks.is_empty()
            && (self.first_buffered_watermarks.len() == len
                || watermark_to_emit.as_ref().map_or(false, |(watermark, _)| {
                    watermark == &self.first_buffered_watermarks.peek().unwrap().0 .0
                }))
        {
            let Reverse((watermark, id)) = self.first_buffered_watermarks.pop().unwrap();
            watermark_to_emit = Some((watermark, id));
            let staged = self.other_buffered_watermarks.get_mut(&id).unwrap();
            if let Some(first) = staged.staged.pop_front() {
                self.first_buffered_watermarks.push(Reverse((first, id)));
            } else {
                staged.in_heap = false;
            }
        }
        watermark_to_emit
    }

    /// Remove buffers and return watermark to emit.
    pub fn remove_buffer(&mut self, buffer_ids_to_remove: HashSet<ID>) -> Option<(Watermark, ID)> {
        self.first_buffered_watermarks
            .retain(|Reverse((_, id))| !buffer_ids_to_remove.contains(id));
        self.other_buffered_watermarks
            .retain(|id, _| !buffer_ids_to_remove.contains(id));
        // Call `check_watermark_heap` in case the only buffers(s) that does not have watermark in
        // heap is removed
        self.check_watermark_heap()
    }
}
