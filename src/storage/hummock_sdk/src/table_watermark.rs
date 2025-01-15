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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::mem::size_of;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::types::ToDatumRef;
use risingwave_common::util::sort_util::{cmp_datum, OrderType};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::hummock::table_watermarks::PbEpochNewWatermarks;
use risingwave_pb::hummock::{PbVnodeWatermark, TableWatermarks as PbTableWatermarks};
use tracing::{debug, warn};

use crate::key::{prefix_slice_with_vnode, vnode, TableKey, TableKeyRange};
use crate::HummockEpoch;

#[derive(Clone)]
pub struct ReadTableWatermark {
    pub direction: WatermarkDirection,
    pub vnode_watermarks: BTreeMap<VirtualNode, Bytes>,
}

#[derive(Clone)]
pub struct TableWatermarksIndex {
    pub watermark_direction: WatermarkDirection,
    // later epoch at the back
    pub staging_watermarks: VecDeque<(HummockEpoch, Arc<[VnodeWatermark]>)>,
    pub committed_watermarks: Option<Arc<TableWatermarks>>,
    latest_epoch: HummockEpoch,
    committed_epoch: Option<HummockEpoch>,
}

impl TableWatermarksIndex {
    pub fn new(
        watermark_direction: WatermarkDirection,
        first_epoch: HummockEpoch,
        first_vnode_watermark: Vec<VnodeWatermark>,
        committed_epoch: Option<HummockEpoch>,
    ) -> Self {
        if let Some(committed_epoch) = committed_epoch {
            assert!(first_epoch > committed_epoch);
        }
        Self {
            watermark_direction,
            staging_watermarks: VecDeque::from_iter([(
                first_epoch,
                Arc::from(first_vnode_watermark),
            )]),
            committed_watermarks: None,
            latest_epoch: first_epoch,
            committed_epoch,
        }
    }

    pub fn new_committed(
        committed_watermarks: Arc<TableWatermarks>,
        committed_epoch: HummockEpoch,
    ) -> Self {
        Self {
            watermark_direction: committed_watermarks.direction,
            staging_watermarks: VecDeque::new(),
            committed_epoch: Some(committed_epoch),
            latest_epoch: committed_epoch,
            committed_watermarks: Some(committed_watermarks),
        }
    }

    pub fn read_watermark(&self, vnode: VirtualNode, epoch: HummockEpoch) -> Option<Bytes> {
        // iterate from new epoch to old epoch
        for (watermark_epoch, vnode_watermark_list) in self.staging_watermarks.iter().rev().chain(
            self.committed_watermarks
                .iter()
                .flat_map(|watermarks| watermarks.watermarks.iter().rev()),
        ) {
            if *watermark_epoch > epoch {
                continue;
            }
            for vnode_watermark in vnode_watermark_list.as_ref() {
                if vnode_watermark.vnode_bitmap.is_set(vnode.to_index()) {
                    return Some(vnode_watermark.watermark.clone());
                }
            }
        }
        None
    }

    pub fn latest_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
        self.read_watermark(vnode, HummockEpoch::MAX)
    }

    pub fn rewrite_range_with_table_watermark(
        &self,
        epoch: HummockEpoch,
        key_range: &mut TableKeyRange,
    ) {
        let vnode = vnode(key_range);
        if let Some(watermark) = self.read_watermark(vnode, epoch) {
            match self.watermark_direction {
                WatermarkDirection::Ascending => {
                    let overwrite_start_key = match &key_range.0 {
                        Included(start_key) | Excluded(start_key) => {
                            start_key.key_part() < watermark
                        }
                        Unbounded => true,
                    };
                    if overwrite_start_key {
                        let watermark_key = TableKey(prefix_slice_with_vnode(vnode, &watermark));
                        let fully_filtered = match &key_range.1 {
                            Included(end_key) => end_key < &watermark_key,
                            Excluded(end_key) => end_key <= &watermark_key,
                            Unbounded => false,
                        };
                        if fully_filtered {
                            key_range.1 = Excluded(watermark_key.clone());
                        }
                        key_range.0 = Included(watermark_key);
                    }
                }
                WatermarkDirection::Descending => {
                    let overwrite_end_key = match &key_range.1 {
                        Included(end_key) | Excluded(end_key) => end_key.key_part() > watermark,
                        Unbounded => true,
                    };
                    if overwrite_end_key {
                        let watermark_key = TableKey(prefix_slice_with_vnode(vnode, &watermark));
                        let fully_filtered = match &key_range.0 {
                            Included(start_key) => start_key > &watermark_key,
                            Excluded(start_key) => start_key >= &watermark_key,
                            Unbounded => false,
                        };
                        if fully_filtered {
                            *key_range = (Included(watermark_key.clone()), Excluded(watermark_key));
                        } else {
                            key_range.1 = Included(watermark_key);
                        }
                    }
                }
            }
        }
    }

    pub fn filter_regress_watermarks(&self, watermarks: &mut Vec<VnodeWatermark>) {
        let mut ret = Vec::with_capacity(watermarks.len());
        for watermark in watermarks.drain(..) {
            let vnode_count = watermark.vnode_count();

            let mut regress_vnodes = None;
            for vnode in watermark.vnode_bitmap.iter_vnodes() {
                if let Some(prev_watermark) = self.latest_watermark(vnode) {
                    let is_regress = match self.direction() {
                        WatermarkDirection::Ascending => prev_watermark > watermark.watermark,
                        WatermarkDirection::Descending => prev_watermark < watermark.watermark,
                    };
                    if is_regress {
                        warn!(
                            "table watermark regress: {:?} {} {:?} {:?}",
                            self.direction(),
                            vnode,
                            watermark.watermark,
                            prev_watermark
                        );
                        regress_vnodes
                            .get_or_insert_with(|| BitmapBuilder::zeroed(vnode_count))
                            .set(vnode.to_index(), true);
                    }
                }
            }
            if let Some(regress_vnodes) = regress_vnodes {
                let mut bitmap_builder = None;
                for vnode in watermark.vnode_bitmap.iter_vnodes() {
                    let vnode_index = vnode.to_index();
                    if !regress_vnodes.is_set(vnode_index) {
                        bitmap_builder
                            .get_or_insert_with(|| BitmapBuilder::zeroed(vnode_count))
                            .set(vnode_index, true);
                    }
                }
                if let Some(bitmap_builder) = bitmap_builder {
                    ret.push(VnodeWatermark::new(
                        Arc::new(bitmap_builder.finish()),
                        watermark.watermark,
                    ));
                }
            } else {
                // no vnode has regress watermark
                ret.push(watermark);
            }
        }
        *watermarks = ret;
    }

    pub fn direction(&self) -> WatermarkDirection {
        self.watermark_direction
    }

    pub fn add_epoch_watermark(
        &mut self,
        epoch: HummockEpoch,
        vnode_watermark_list: Arc<[VnodeWatermark]>,
        direction: WatermarkDirection,
    ) {
        assert!(epoch > self.latest_epoch);
        assert_eq!(self.watermark_direction, direction);
        self.latest_epoch = epoch;
        #[cfg(debug_assertions)]
        if !vnode_watermark_list.is_empty() {
            let vnode_count = vnode_watermark_list[0].vnode_count();
            let mut vnode_is_set = BitmapBuilder::zeroed(vnode_count);
            for vnode_watermark in vnode_watermark_list.as_ref() {
                for vnode in vnode_watermark.vnode_bitmap.iter_ones() {
                    assert!(!vnode_is_set.is_set(vnode));
                    vnode_is_set.set(vnode, true);
                    let vnode = VirtualNode::from_index(vnode);
                    if let Some(prev_watermark) = self.latest_watermark(vnode) {
                        match self.watermark_direction {
                            WatermarkDirection::Ascending => {
                                assert!(vnode_watermark.watermark >= prev_watermark);
                            }
                            WatermarkDirection::Descending => {
                                assert!(vnode_watermark.watermark <= prev_watermark);
                            }
                        };
                    }
                }
            }
        }
        self.staging_watermarks
            .push_back((epoch, vnode_watermark_list));
    }

    pub fn apply_committed_watermarks(
        &mut self,
        committed_watermark: Arc<TableWatermarks>,
        committed_epoch: HummockEpoch,
    ) {
        assert_eq!(self.watermark_direction, committed_watermark.direction);
        if let Some(prev_committed_epoch) = self.committed_epoch {
            assert!(prev_committed_epoch <= committed_epoch);
            if prev_committed_epoch == committed_epoch {
                return;
            }
        }
        if self.latest_epoch < committed_epoch {
            debug!(
                latest_epoch = self.latest_epoch,
                committed_epoch, "committed_epoch exceed table watermark latest_epoch"
            );
            self.latest_epoch = committed_epoch;
        }
        self.committed_epoch = Some(committed_epoch);
        self.committed_watermarks = Some(committed_watermark);
        // keep only watermark higher than committed epoch
        while let Some((old_epoch, _)) = self.staging_watermarks.front()
            && *old_epoch <= committed_epoch
        {
            let _ = self.staging_watermarks.pop_front();
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WatermarkDirection {
    Ascending,
    Descending,
}

impl Display for WatermarkDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WatermarkDirection::Ascending => write!(f, "Ascending"),
            WatermarkDirection::Descending => write!(f, "Descending"),
        }
    }
}

impl WatermarkDirection {
    pub fn key_filter_by_watermark(
        &self,
        key: impl AsRef<[u8]>,
        watermark: impl AsRef<[u8]>,
    ) -> bool {
        let key = key.as_ref();
        let watermark = watermark.as_ref();
        match self {
            WatermarkDirection::Ascending => key < watermark,
            WatermarkDirection::Descending => key > watermark,
        }
    }

    pub fn datum_filter_by_watermark(
        &self,
        watermark_col_in_pk: impl ToDatumRef,
        watermark: impl ToDatumRef,
        order_type: OrderType,
    ) -> bool {
        let watermark_col_in_pk = watermark_col_in_pk.to_datum_ref();
        let watermark = watermark.to_datum_ref();
        match self {
            WatermarkDirection::Ascending => {
                // watermark_col_in_pk < watermark
                cmp_datum(watermark_col_in_pk, watermark, order_type).is_lt()
            }
            WatermarkDirection::Descending => {
                //  watermark_col_in_pk > watermark
                cmp_datum(watermark_col_in_pk, watermark, order_type).is_gt()
            }
        }
    }

    pub fn is_ascending(&self) -> bool {
        match self {
            WatermarkDirection::Ascending => true,
            WatermarkDirection::Descending => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, EstimateSize)]
pub struct VnodeWatermark {
    vnode_bitmap: Arc<Bitmap>,
    watermark: Bytes,
}

impl VnodeWatermark {
    pub fn new(vnode_bitmap: Arc<Bitmap>, watermark: Bytes) -> Self {
        Self {
            vnode_bitmap,
            watermark,
        }
    }

    pub fn vnode_bitmap(&self) -> &Bitmap {
        &self.vnode_bitmap
    }

    /// Vnode count derived from the bitmap.
    pub fn vnode_count(&self) -> usize {
        self.vnode_bitmap.len()
    }

    pub fn watermark(&self) -> &Bytes {
        &self.watermark
    }

    pub fn to_protobuf(&self) -> PbVnodeWatermark {
        self.into()
    }
}

impl From<PbVnodeWatermark> for VnodeWatermark {
    fn from(pb: PbVnodeWatermark) -> Self {
        Self {
            vnode_bitmap: Arc::new(Bitmap::from(pb.vnode_bitmap.as_ref().unwrap())),
            watermark: Bytes::from(pb.watermark),
        }
    }
}

impl From<&PbVnodeWatermark> for VnodeWatermark {
    fn from(pb: &PbVnodeWatermark) -> Self {
        Self {
            vnode_bitmap: Arc::new(Bitmap::from(pb.vnode_bitmap.as_ref().unwrap())),
            watermark: Bytes::from(pb.watermark.clone()),
        }
    }
}

impl From<VnodeWatermark> for PbVnodeWatermark {
    fn from(watermark: VnodeWatermark) -> Self {
        Self {
            watermark: watermark.watermark.into(),
            vnode_bitmap: Some(watermark.vnode_bitmap.to_protobuf()),
        }
    }
}

impl From<&VnodeWatermark> for PbVnodeWatermark {
    fn from(watermark: &VnodeWatermark) -> Self {
        Self {
            watermark: watermark.watermark.to_vec(),
            vnode_bitmap: Some(watermark.vnode_bitmap.to_protobuf()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TableWatermarks {
    // later epoch at the back
    pub watermarks: Vec<(HummockEpoch, Arc<[VnodeWatermark]>)>,
    pub direction: WatermarkDirection,
    pub watermark_type: WatermarkSerdeType,
}

impl TableWatermarks {
    pub fn single_epoch(
        epoch: HummockEpoch,
        watermarks: Vec<VnodeWatermark>,
        direction: WatermarkDirection,
        watermark_type: WatermarkSerdeType,
    ) -> Self {
        let mut this = Self {
            direction,
            watermarks: Vec::new(),
            watermark_type,
        };
        this.add_new_epoch_watermarks(epoch, watermarks.into(), direction, watermark_type);
        this
    }

    pub fn add_new_epoch_watermarks(
        &mut self,
        epoch: HummockEpoch,
        watermarks: Arc<[VnodeWatermark]>,
        direction: WatermarkDirection,
        watermark_type: WatermarkSerdeType,
    ) {
        assert_eq!(self.direction, direction);
        assert_eq!(self.watermark_type, watermark_type);

        if let Some((prev_epoch, _)) = self.watermarks.last() {
            assert!(*prev_epoch < epoch);
        }
        if !watermarks.is_empty() {
            let vnode_count = watermarks[0].vnode_count();
            for watermark in &*watermarks {
                assert_eq!(watermark.vnode_count(), vnode_count);
            }
            if let Some(existing_vnode_count) = self.vnode_count() {
                assert_eq!(existing_vnode_count, vnode_count);
            }
        }
        self.watermarks.push((epoch, watermarks));
    }

    /// Vnode count derived from existing watermarks. Returns `None` if there is no watermark.
    fn vnode_count(&self) -> Option<usize> {
        self.watermarks
            .iter()
            .flat_map(|(_, watermarks)| watermarks.as_ref())
            .next()
            .map(|w| w.vnode_count())
    }

    pub fn from_protobuf(pb: &PbTableWatermarks) -> Self {
        Self {
            watermarks: pb
                .epoch_watermarks
                .iter()
                .map(|epoch_watermark| {
                    let epoch = epoch_watermark.epoch;
                    let watermarks = epoch_watermark
                        .watermarks
                        .iter()
                        .map(VnodeWatermark::from)
                        .collect_vec();
                    (epoch, Arc::from(watermarks))
                })
                .collect(),
            direction: if pb.is_ascending {
                WatermarkDirection::Ascending
            } else {
                WatermarkDirection::Descending
            },
            watermark_type: if pb.is_non_pk_prefix {
                WatermarkSerdeType::NonPkPrefix
            } else {
                WatermarkSerdeType::PkPrefix
            },
        }
    }
}

pub fn merge_multiple_new_table_watermarks(
    table_watermarks_list: impl IntoIterator<Item = HashMap<TableId, TableWatermarks>>,
) -> HashMap<TableId, TableWatermarks> {
    #[allow(clippy::type_complexity)]
    let mut ret: HashMap<
        TableId,
        (
            WatermarkDirection,
            BTreeMap<u64, Vec<VnodeWatermark>>,
            WatermarkSerdeType,
        ),
    > = HashMap::new();
    for table_watermarks in table_watermarks_list {
        for (table_id, new_table_watermarks) in table_watermarks {
            let epoch_watermarks = match ret.entry(table_id) {
                Entry::Occupied(entry) => {
                    let (direction, epoch_watermarks, watermark_type) = entry.into_mut();
                    assert_eq!(&new_table_watermarks.direction, direction);
                    assert_eq!(&new_table_watermarks.watermark_type, watermark_type);
                    epoch_watermarks
                }
                Entry::Vacant(entry) => {
                    let (_, epoch_watermarks, _) = entry.insert((
                        new_table_watermarks.direction,
                        BTreeMap::new(),
                        new_table_watermarks.watermark_type,
                    ));
                    epoch_watermarks
                }
            };
            for (new_epoch, new_epoch_watermarks) in new_table_watermarks.watermarks {
                epoch_watermarks
                    .entry(new_epoch)
                    .or_insert_with(Vec::new)
                    .extend(new_epoch_watermarks.iter().cloned());
            }
        }
    }
    ret.into_iter()
        .map(
            |(table_id, (direction, epoch_watermarks, watermark_type))| {
                (
                    table_id,
                    TableWatermarks {
                        direction,
                        // ordered from earlier epoch to later epoch
                        watermarks: epoch_watermarks
                            .into_iter()
                            .map(|(epoch, watermarks)| (epoch, Arc::from(watermarks)))
                            .collect(),
                        watermark_type,
                    },
                )
            },
        )
        .collect()
}

impl TableWatermarks {
    pub fn apply_new_table_watermarks(&mut self, newly_added_watermarks: &TableWatermarks) {
        assert_eq!(self.direction, newly_added_watermarks.direction);
        assert!(self.watermarks.iter().map(|(epoch, _)| epoch).is_sorted());
        assert!(newly_added_watermarks
            .watermarks
            .iter()
            .map(|(epoch, _)| epoch)
            .is_sorted());
        // ensure that the newly added watermarks have a later epoch than the previous latest epoch.
        if let Some((prev_last_epoch, _)) = self.watermarks.last()
            && let Some((new_first_epoch, _)) = newly_added_watermarks.watermarks.first()
        {
            assert!(prev_last_epoch < new_first_epoch);
        }
        self.watermarks.extend(
            newly_added_watermarks
                .watermarks
                .iter()
                .map(|(epoch, new_watermarks)| (*epoch, new_watermarks.clone())),
        );
    }

    pub fn clear_stale_epoch_watermark(&mut self, safe_epoch: u64) {
        match self.watermarks.first() {
            None => {
                // return on empty watermark
                return;
            }
            Some((earliest_epoch, _)) => {
                if *earliest_epoch >= safe_epoch {
                    // No stale epoch watermark needs to be cleared.
                    return;
                }
            }
        }
        debug!("clear stale table watermark below epoch {}", safe_epoch);
        let mut result_epoch_watermark = Vec::with_capacity(self.watermarks.len());
        let mut set_vnode: HashSet<VirtualNode> = HashSet::new();
        let mut vnode_count: Option<usize> = None; // lazy initialized on first occurrence of vnode watermark
        while let Some((epoch, _)) = self.watermarks.last() {
            if *epoch >= safe_epoch {
                let (epoch, watermarks) = self.watermarks.pop().expect("have check Some");
                for watermark in watermarks.as_ref() {
                    vnode_count.get_or_insert_with(|| watermark.vnode_count());
                    for vnode in watermark.vnode_bitmap.iter_vnodes() {
                        set_vnode.insert(vnode);
                    }
                }
                result_epoch_watermark.push((epoch, watermarks));
            } else {
                break;
            }
        }
        while vnode_count.map_or(true, |vnode_count| set_vnode.len() != vnode_count)
            && let Some((_, watermarks)) = self.watermarks.pop()
        {
            let mut new_vnode_watermarks = Vec::new();
            for vnode_watermark in watermarks.as_ref() {
                let mut new_set_vnode = Vec::new();
                vnode_count.get_or_insert_with(|| vnode_watermark.vnode_count());
                for vnode in vnode_watermark.vnode_bitmap.iter_vnodes() {
                    if set_vnode.insert(vnode) {
                        new_set_vnode.push(vnode);
                    }
                }
                if !new_set_vnode.is_empty() {
                    let mut builder = BitmapBuilder::zeroed(vnode_watermark.vnode_count());
                    for vnode in new_set_vnode {
                        builder.set(vnode.to_index(), true);
                    }
                    let bitmap = Arc::new(builder.finish());
                    new_vnode_watermarks.push(VnodeWatermark {
                        vnode_bitmap: bitmap,
                        watermark: vnode_watermark.watermark.clone(),
                    })
                }
            }
            if !new_vnode_watermarks.is_empty() {
                if let Some((last_epoch, last_watermarks)) = result_epoch_watermark.last_mut()
                    && *last_epoch == safe_epoch
                {
                    *last_watermarks = Arc::from(
                        last_watermarks
                            .iter()
                            .cloned()
                            .chain(new_vnode_watermarks.into_iter())
                            .collect_vec(),
                    );
                } else {
                    result_epoch_watermark.push((safe_epoch, Arc::from(new_vnode_watermarks)));
                }
            }
        }
        // epoch watermark are added from later epoch to earlier epoch.
        // reverse to ensure that earlier epochs are at the front
        result_epoch_watermark.reverse();
        assert!(result_epoch_watermark
            .is_sorted_by(|(first_epoch, _), (second_epoch, _)| { first_epoch < second_epoch }));
        *self = TableWatermarks {
            watermarks: result_epoch_watermark,
            direction: self.direction,
            watermark_type: self.watermark_type,
        }
    }
}

impl TableWatermarks {
    pub fn estimated_encode_len(&self) -> usize {
        self.watermarks.len() * size_of::<HummockEpoch>()
            + self
                .watermarks
                .iter()
                .map(|(_, watermarks)| {
                    watermarks
                        .iter()
                        .map(|watermark| watermark.estimated_size())
                        .sum::<usize>()
                })
                .sum::<usize>()
            + size_of::<bool>() // for direction
    }

    pub fn to_protobuf(&self) -> PbTableWatermarks {
        self.into()
    }
}

impl From<&PbTableWatermarks> for TableWatermarks {
    fn from(pb: &PbTableWatermarks) -> Self {
        Self {
            watermarks: pb
                .epoch_watermarks
                .iter()
                .map(|epoch_watermark| {
                    let epoch = epoch_watermark.epoch;
                    let watermarks = epoch_watermark
                        .watermarks
                        .iter()
                        .map(VnodeWatermark::from)
                        .collect();
                    (epoch, watermarks)
                })
                .collect(),
            direction: if pb.is_ascending {
                WatermarkDirection::Ascending
            } else {
                WatermarkDirection::Descending
            },
            watermark_type: if pb.is_non_pk_prefix {
                WatermarkSerdeType::NonPkPrefix
            } else {
                WatermarkSerdeType::PkPrefix
            },
        }
    }
}

impl From<&TableWatermarks> for PbTableWatermarks {
    fn from(table_watermarks: &TableWatermarks) -> Self {
        Self {
            epoch_watermarks: table_watermarks
                .watermarks
                .iter()
                .map(|(epoch, watermarks)| PbEpochNewWatermarks {
                    watermarks: watermarks.iter().map(|wm| wm.into()).collect(),
                    epoch: *epoch,
                })
                .collect(),
            is_ascending: match table_watermarks.direction {
                WatermarkDirection::Ascending => true,
                WatermarkDirection::Descending => false,
            },
            is_non_pk_prefix: match table_watermarks.watermark_type {
                WatermarkSerdeType::NonPkPrefix => true,
                WatermarkSerdeType::PkPrefix => false,
            },
        }
    }
}

impl From<PbTableWatermarks> for TableWatermarks {
    fn from(pb: PbTableWatermarks) -> Self {
        Self {
            watermarks: pb
                .epoch_watermarks
                .into_iter()
                .map(|epoch_watermark| {
                    let epoch = epoch_watermark.epoch;
                    let watermarks = epoch_watermark
                        .watermarks
                        .into_iter()
                        .map(VnodeWatermark::from)
                        .collect();
                    (epoch, watermarks)
                })
                .collect(),
            direction: if pb.is_ascending {
                WatermarkDirection::Ascending
            } else {
                WatermarkDirection::Descending
            },
            watermark_type: if pb.is_non_pk_prefix {
                WatermarkSerdeType::NonPkPrefix
            } else {
                WatermarkSerdeType::PkPrefix
            },
        }
    }
}

impl From<TableWatermarks> for PbTableWatermarks {
    fn from(table_watermarks: TableWatermarks) -> Self {
        Self {
            epoch_watermarks: table_watermarks
                .watermarks
                .into_iter()
                .map(|(epoch, watermarks)| PbEpochNewWatermarks {
                    watermarks: watermarks.iter().map(PbVnodeWatermark::from).collect(),
                    epoch,
                })
                .collect(),
            is_ascending: match table_watermarks.direction {
                WatermarkDirection::Ascending => true,
                WatermarkDirection::Descending => false,
            },
            is_non_pk_prefix: match table_watermarks.watermark_type {
                WatermarkSerdeType::NonPkPrefix => true,
                WatermarkSerdeType::PkPrefix => false,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatermarkSerdeType {
    PkPrefix,
    NonPkPrefix,
}

#[cfg(test)]
mod tests {
    use std::collections::Bound::Included;
    use std::collections::{Bound, HashMap};
    use std::ops::Bound::Excluded;
    use std::sync::Arc;
    use std::vec;

    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::{test_epoch, EpochExt};
    use risingwave_pb::hummock::{PbHummockVersion, StateTableInfo};

    use crate::compaction_group::StaticCompactionGroupId;
    use crate::key::{is_empty_key_range, prefixed_range_with_vnode, TableKeyRange};
    use crate::table_watermark::{
        merge_multiple_new_table_watermarks, TableWatermarks, TableWatermarksIndex, VnodeWatermark,
        WatermarkDirection, WatermarkSerdeType,
    };
    use crate::version::HummockVersion;

    fn build_bitmap(vnodes: impl IntoIterator<Item = usize>) -> Arc<Bitmap> {
        let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
        for vnode in vnodes {
            builder.set(vnode, true);
        }
        Arc::new(builder.finish())
    }

    #[test]
    fn test_apply_new_table_watermark() {
        let epoch1 = test_epoch(1);
        let direction = WatermarkDirection::Ascending;
        let watermark1 = Bytes::from("watermark1");
        let watermark2 = Bytes::from("watermark2");
        let watermark3 = Bytes::from("watermark3");
        let watermark4 = Bytes::from("watermark4");
        let watermark_type = WatermarkSerdeType::PkPrefix;
        let mut table_watermarks = TableWatermarks::single_epoch(
            epoch1,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 1, 2]),
                watermark1.clone(),
            )],
            direction,
            watermark_type,
        );
        let epoch2 = epoch1.next_epoch();
        table_watermarks.add_new_epoch_watermarks(
            epoch2,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 1, 2, 3]),
                watermark2.clone(),
            )]
            .into(),
            direction,
            watermark_type,
        );

        let mut table_watermark_checkpoint = table_watermarks.clone();

        let epoch3 = epoch2.next_epoch();
        let mut second_table_watermark = TableWatermarks::single_epoch(
            epoch3,
            vec![VnodeWatermark::new(
                build_bitmap(0..VirtualNode::COUNT_FOR_TEST),
                watermark3.clone(),
            )],
            direction,
            watermark_type,
        );
        table_watermarks.add_new_epoch_watermarks(
            epoch3,
            vec![VnodeWatermark::new(
                build_bitmap(0..VirtualNode::COUNT_FOR_TEST),
                watermark3.clone(),
            )]
            .into(),
            direction,
            watermark_type,
        );
        let epoch4 = epoch3.next_epoch();
        let epoch5 = epoch4.next_epoch();
        table_watermarks.add_new_epoch_watermarks(
            epoch5,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 3, 4]),
                watermark4.clone(),
            )]
            .into(),
            direction,
            watermark_type,
        );
        second_table_watermark.add_new_epoch_watermarks(
            epoch5,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 3, 4]),
                watermark4.clone(),
            )]
            .into(),
            direction,
            watermark_type,
        );

        table_watermark_checkpoint.apply_new_table_watermarks(&second_table_watermark);
        assert_eq!(table_watermarks, table_watermark_checkpoint);
    }

    #[test]
    fn test_clear_stale_epoch_watmermark() {
        let epoch1 = test_epoch(1);
        let direction = WatermarkDirection::Ascending;
        let watermark1 = Bytes::from("watermark1");
        let watermark2 = Bytes::from("watermark2");
        let watermark3 = Bytes::from("watermark3");
        let watermark4 = Bytes::from("watermark4");
        let watermark_type = WatermarkSerdeType::PkPrefix;
        let mut table_watermarks = TableWatermarks::single_epoch(
            epoch1,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 1, 2]),
                watermark1.clone(),
            )],
            direction,
            watermark_type,
        );
        let epoch2 = epoch1.next_epoch();
        table_watermarks.add_new_epoch_watermarks(
            epoch2,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 1, 2, 3]),
                watermark2.clone(),
            )]
            .into(),
            direction,
            watermark_type,
        );
        let epoch3 = epoch2.next_epoch();
        table_watermarks.add_new_epoch_watermarks(
            epoch3,
            vec![VnodeWatermark::new(
                build_bitmap(0..VirtualNode::COUNT_FOR_TEST),
                watermark3.clone(),
            )]
            .into(),
            direction,
            watermark_type,
        );
        let epoch4 = epoch3.next_epoch();
        let epoch5 = epoch4.next_epoch();
        table_watermarks.add_new_epoch_watermarks(
            epoch5,
            vec![VnodeWatermark::new(
                build_bitmap(vec![0, 3, 4]),
                watermark4.clone(),
            )]
            .into(),
            direction,
            watermark_type,
        );

        let mut table_watermarks_checkpoint = table_watermarks.clone();
        table_watermarks_checkpoint.clear_stale_epoch_watermark(epoch1);
        assert_eq!(table_watermarks_checkpoint, table_watermarks);

        table_watermarks_checkpoint.clear_stale_epoch_watermark(epoch2);
        assert_eq!(
            table_watermarks_checkpoint,
            TableWatermarks {
                watermarks: vec![
                    (
                        epoch2,
                        vec![VnodeWatermark::new(
                            build_bitmap(vec![0, 1, 2, 3]),
                            watermark2.clone(),
                        )]
                        .into()
                    ),
                    (
                        epoch3,
                        vec![VnodeWatermark::new(
                            build_bitmap(0..VirtualNode::COUNT_FOR_TEST),
                            watermark3.clone(),
                        )]
                        .into()
                    ),
                    (
                        epoch5,
                        vec![VnodeWatermark::new(
                            build_bitmap(vec![0, 3, 4]),
                            watermark4.clone(),
                        )]
                        .into()
                    )
                ],
                direction,
                watermark_type,
            }
        );

        table_watermarks_checkpoint.clear_stale_epoch_watermark(epoch3);
        assert_eq!(
            table_watermarks_checkpoint,
            TableWatermarks {
                watermarks: vec![
                    (
                        epoch3,
                        vec![VnodeWatermark::new(
                            build_bitmap(0..VirtualNode::COUNT_FOR_TEST),
                            watermark3.clone(),
                        )]
                        .into()
                    ),
                    (
                        epoch5,
                        vec![VnodeWatermark::new(
                            build_bitmap(vec![0, 3, 4]),
                            watermark4.clone(),
                        )]
                        .into()
                    )
                ],
                direction,
                watermark_type,
            }
        );

        table_watermarks_checkpoint.clear_stale_epoch_watermark(epoch4);
        assert_eq!(
            table_watermarks_checkpoint,
            TableWatermarks {
                watermarks: vec![
                    (
                        epoch4,
                        vec![VnodeWatermark::new(
                            build_bitmap((1..3).chain(5..VirtualNode::COUNT_FOR_TEST)),
                            watermark3.clone()
                        )]
                        .into()
                    ),
                    (
                        epoch5,
                        vec![VnodeWatermark::new(
                            build_bitmap(vec![0, 3, 4]),
                            watermark4.clone(),
                        )]
                        .into()
                    )
                ],
                direction,
                watermark_type,
            }
        );

        table_watermarks_checkpoint.clear_stale_epoch_watermark(epoch5);
        assert_eq!(
            table_watermarks_checkpoint,
            TableWatermarks {
                watermarks: vec![(
                    epoch5,
                    vec![
                        VnodeWatermark::new(build_bitmap(vec![0, 3, 4]), watermark4.clone()),
                        VnodeWatermark::new(
                            build_bitmap((1..3).chain(5..VirtualNode::COUNT_FOR_TEST)),
                            watermark3.clone()
                        )
                    ]
                    .into()
                )],
                direction,
                watermark_type,
            }
        );
    }

    #[test]
    fn test_merge_multiple_new_table_watermarks() {
        fn epoch_new_watermark(epoch: u64, bitmaps: Vec<&Bitmap>) -> (u64, Arc<[VnodeWatermark]>) {
            (
                epoch,
                bitmaps
                    .into_iter()
                    .map(|bitmap| VnodeWatermark {
                        watermark: Bytes::from(vec![1, 2, epoch as _]),
                        vnode_bitmap: Arc::new(bitmap.clone()),
                    })
                    .collect_vec()
                    .into(),
            )
        }
        fn build_table_watermark(
            vnodes: impl IntoIterator<Item = usize>,
            epochs: impl IntoIterator<Item = u64>,
        ) -> TableWatermarks {
            let bitmap = build_bitmap(vnodes);
            TableWatermarks {
                watermarks: epochs
                    .into_iter()
                    .map(|epoch: u64| epoch_new_watermark(epoch, vec![&bitmap]))
                    .collect(),
                direction: WatermarkDirection::Ascending,
                watermark_type: WatermarkSerdeType::PkPrefix,
            }
        }
        let table1_watermark1 = build_table_watermark(0..3, vec![1, 2, 4]);
        let table1_watermark2 = build_table_watermark(4..6, vec![1, 2, 5]);
        let table2_watermark = build_table_watermark(0..4, 1..3);
        let table3_watermark = build_table_watermark(0..4, 3..5);
        let mut first = HashMap::new();
        first.insert(TableId::new(1), table1_watermark1);
        first.insert(TableId::new(2), table2_watermark.clone());
        let mut second = HashMap::new();
        second.insert(TableId::new(1), table1_watermark2);
        second.insert(TableId::new(3), table3_watermark.clone());
        let result = merge_multiple_new_table_watermarks(vec![first, second]);
        let mut expected = HashMap::new();
        expected.insert(
            TableId::new(1),
            TableWatermarks {
                watermarks: vec![
                    epoch_new_watermark(1, vec![&build_bitmap(0..3), &build_bitmap(4..6)]),
                    epoch_new_watermark(2, vec![&build_bitmap(0..3), &build_bitmap(4..6)]),
                    epoch_new_watermark(4, vec![&build_bitmap(0..3)]),
                    epoch_new_watermark(5, vec![&build_bitmap(4..6)]),
                ],
                direction: WatermarkDirection::Ascending,
                watermark_type: WatermarkSerdeType::PkPrefix,
            },
        );
        expected.insert(TableId::new(2), table2_watermark);
        expected.insert(TableId::new(3), table3_watermark);
        assert_eq!(result, expected);
    }

    const COMMITTED_EPOCH: u64 = test_epoch(1);
    const EPOCH1: u64 = test_epoch(2);
    const EPOCH2: u64 = test_epoch(3);
    const TEST_SINGLE_VNODE: VirtualNode = VirtualNode::from_index(1);

    fn build_watermark_range(
        direction: WatermarkDirection,
        (low, high): (Bound<Bytes>, Bound<Bytes>),
    ) -> TableKeyRange {
        let range = match direction {
            WatermarkDirection::Ascending => (low, high),
            WatermarkDirection::Descending => (high, low),
        };
        prefixed_range_with_vnode(range, TEST_SINGLE_VNODE)
    }

    /// Build and return a watermark index with the following watermarks
    /// EPOCH1 bitmap(0, 1, 2, 3) watermark1
    /// EPOCH2 bitmap(1, 2, 3, 4) watermark2
    fn build_and_test_watermark_index(
        direction: WatermarkDirection,
        watermark1: Bytes,
        watermark2: Bytes,
        watermark3: Bytes,
    ) -> TableWatermarksIndex {
        let mut index = TableWatermarksIndex::new(
            direction,
            EPOCH1,
            vec![VnodeWatermark::new(build_bitmap(0..4), watermark1.clone())],
            Some(COMMITTED_EPOCH),
        );
        index.add_epoch_watermark(
            EPOCH2,
            vec![VnodeWatermark::new(build_bitmap(1..5), watermark2.clone())].into(),
            direction,
        );

        assert_eq!(
            index.read_watermark(VirtualNode::from_index(0), EPOCH1),
            Some(watermark1.clone())
        );
        assert_eq!(
            index.read_watermark(VirtualNode::from_index(1), EPOCH1),
            Some(watermark1.clone())
        );
        assert_eq!(
            index.read_watermark(VirtualNode::from_index(4), EPOCH1),
            None
        );
        assert_eq!(
            index.read_watermark(VirtualNode::from_index(0), EPOCH2),
            Some(watermark1.clone())
        );
        assert_eq!(
            index.read_watermark(VirtualNode::from_index(1), EPOCH2),
            Some(watermark2.clone())
        );
        assert_eq!(
            index.read_watermark(VirtualNode::from_index(4), EPOCH2),
            Some(watermark2.clone())
        );
        assert_eq!(
            index.latest_watermark(VirtualNode::from_index(0)),
            Some(watermark1.clone())
        );
        assert_eq!(
            index.latest_watermark(VirtualNode::from_index(1)),
            Some(watermark2.clone())
        );
        assert_eq!(
            index.latest_watermark(VirtualNode::from_index(4)),
            Some(watermark2.clone())
        );

        // watermark is watermark2
        let check_watermark_range =
            |query_range: (Bound<Bytes>, Bound<Bytes>),
             output_range: Option<(Bound<Bytes>, Bound<Bytes>)>| {
                let mut range = build_watermark_range(direction, query_range);
                index.rewrite_range_with_table_watermark(EPOCH2, &mut range);
                if let Some(output_range) = output_range {
                    assert_eq!(range, build_watermark_range(direction, output_range));
                } else {
                    assert!(is_empty_key_range(&range));
                }
            };

        // test read from single vnode and truncate begin key range
        check_watermark_range(
            (Included(watermark1.clone()), Excluded(watermark3.clone())),
            Some((Included(watermark2.clone()), Excluded(watermark3.clone()))),
        );

        // test read from single vnode and begin key right at watermark
        check_watermark_range(
            (Included(watermark2.clone()), Excluded(watermark3.clone())),
            Some((Included(watermark2.clone()), Excluded(watermark3.clone()))),
        );
        check_watermark_range(
            (Excluded(watermark2.clone()), Excluded(watermark3.clone())),
            Some((Excluded(watermark2.clone()), Excluded(watermark3.clone()))),
        );

        // test read from single vnode and end key right at watermark
        check_watermark_range(
            (Excluded(watermark1.clone()), Excluded(watermark2.clone())),
            None,
        );
        check_watermark_range(
            (Excluded(watermark1.clone()), Included(watermark2.clone())),
            Some((Included(watermark2.clone()), Included(watermark2.clone()))),
        );

        index
    }

    #[test]
    fn test_watermark_index_ascending() {
        let watermark1 = Bytes::from_static(b"watermark1");
        let watermark2 = Bytes::from_static(b"watermark2");
        let watermark3 = Bytes::from_static(b"watermark3");
        build_and_test_watermark_index(
            WatermarkDirection::Ascending,
            watermark1.clone(),
            watermark2.clone(),
            watermark3.clone(),
        );
    }

    #[test]
    fn test_watermark_index_descending() {
        let watermark1 = Bytes::from_static(b"watermark254");
        let watermark2 = Bytes::from_static(b"watermark253");
        let watermark3 = Bytes::from_static(b"watermark252");
        build_and_test_watermark_index(
            WatermarkDirection::Descending,
            watermark1.clone(),
            watermark2.clone(),
            watermark3.clone(),
        );
    }

    #[test]
    fn test_apply_committed_index() {
        let watermark1 = Bytes::from_static(b"watermark1");
        let watermark2 = Bytes::from_static(b"watermark2");
        let watermark3 = Bytes::from_static(b"watermark3");
        let mut index = build_and_test_watermark_index(
            WatermarkDirection::Ascending,
            watermark1.clone(),
            watermark2.clone(),
            watermark3.clone(),
        );

        let test_table_id = TableId::from(233);

        let mut version = HummockVersion::from_rpc_protobuf(&PbHummockVersion {
            state_table_info: HashMap::from_iter([(
                test_table_id.table_id,
                StateTableInfo {
                    committed_epoch: EPOCH1,
                    compaction_group_id: StaticCompactionGroupId::StateDefault as _,
                },
            )]),
            ..Default::default()
        });
        version.table_watermarks.insert(
            test_table_id,
            TableWatermarks {
                watermarks: vec![(
                    EPOCH1,
                    vec![VnodeWatermark {
                        watermark: watermark1.clone(),
                        vnode_bitmap: build_bitmap(0..VirtualNode::COUNT_FOR_TEST),
                    }]
                    .into(),
                )],
                direction: WatermarkDirection::Ascending,
                watermark_type: WatermarkSerdeType::PkPrefix,
            }
            .into(),
        );
        index.apply_committed_watermarks(
            version
                .table_watermarks
                .get(&test_table_id)
                .unwrap()
                .clone(),
            EPOCH1,
        );
        assert_eq!(EPOCH1, index.committed_epoch.unwrap());
        assert_eq!(EPOCH2, index.latest_epoch);
        for vnode in 0..VirtualNode::COUNT_FOR_TEST {
            let vnode = VirtualNode::from_index(vnode);
            if (1..5).contains(&vnode.to_index()) {
                assert_eq!(watermark1, index.read_watermark(vnode, EPOCH1).unwrap());
                assert_eq!(watermark2, index.read_watermark(vnode, EPOCH2).unwrap());
            } else {
                assert_eq!(watermark1, index.read_watermark(vnode, EPOCH1).unwrap());
            }
        }
    }

    #[test]
    fn test_filter_regress_watermark() {
        let watermark1 = Bytes::from_static(b"watermark1");
        let watermark2 = Bytes::from_static(b"watermark2");
        let watermark3 = Bytes::from_static(b"watermark3");
        let index = build_and_test_watermark_index(
            WatermarkDirection::Ascending,
            watermark1.clone(),
            watermark2.clone(),
            watermark3.clone(),
        );

        let mut new_watermarks = vec![
            // Partial regress
            VnodeWatermark {
                vnode_bitmap: build_bitmap(0..2),
                watermark: watermark1.clone(),
            },
            // All not regress
            VnodeWatermark {
                vnode_bitmap: build_bitmap(2..4),
                watermark: watermark3.clone(),
            },
            // All regress
            VnodeWatermark {
                vnode_bitmap: build_bitmap(4..5),
                watermark: watermark1.clone(),
            },
            // All newly set vnode
            VnodeWatermark {
                vnode_bitmap: build_bitmap(5..6),
                watermark: watermark3.clone(),
            },
        ];

        index.filter_regress_watermarks(&mut new_watermarks);

        assert_eq!(
            new_watermarks,
            vec![
                VnodeWatermark {
                    vnode_bitmap: build_bitmap(0..1),
                    watermark: watermark1,
                },
                VnodeWatermark {
                    vnode_bitmap: build_bitmap(2..4),
                    watermark: watermark3.clone(),
                },
                VnodeWatermark {
                    vnode_bitmap: build_bitmap(5..6),
                    watermark: watermark3,
                },
            ]
        );
    }
}
