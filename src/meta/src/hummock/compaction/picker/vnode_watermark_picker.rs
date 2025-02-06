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

use std::collections::BTreeMap;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_hummock_sdk::level::{InputLevel, Levels};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_watermark::ReadTableWatermark;

use crate::hummock::compaction::picker::CompactionInput;
use crate::hummock::level_handler::LevelHandler;

pub struct VnodeWatermarkCompactionPicker {}

impl VnodeWatermarkCompactionPicker {
    pub fn new() -> Self {
        Self {}
    }

    /// The current implementation only picks trivial reclaim task for the bottommost level.
    /// Must modify `is_trivial_reclaim`, if non-trivial reclaim is supported in the future.
    pub fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        table_watermarks: &BTreeMap<TableId, ReadTableWatermark>,
    ) -> Option<CompactionInput> {
        let level = levels.levels.last()?;
        let mut select_input_ssts = vec![];
        for sst_info in &level.table_infos {
            if !level_handlers[level.level_idx as usize].is_pending_compact(&sst_info.sst_id)
                && should_delete_sst_by_watermark(sst_info, table_watermarks)
            {
                select_input_ssts.push(sst_info.clone());
            }
        }
        if select_input_ssts.is_empty() {
            return None;
        }
        Some(CompactionInput {
            select_input_size: select_input_ssts.iter().map(|sst| sst.sst_size).sum(),
            total_file_count: select_input_ssts.len() as u64,
            input_levels: vec![
                InputLevel {
                    level_idx: level.level_idx,
                    level_type: level.level_type,
                    table_infos: select_input_ssts,
                },
                InputLevel {
                    level_idx: level.level_idx,
                    level_type: level.level_type,
                    table_infos: vec![],
                },
            ],
            target_level: level.level_idx as usize,
            target_sub_level_id: level.sub_level_id,
            ..Default::default()
        })
    }
}

fn should_delete_sst_by_watermark(
    sst_info: &SstableInfo,
    table_watermarks: &BTreeMap<TableId, ReadTableWatermark>,
) -> bool {
    // Both table id and vnode must be identical for both the left and right keys in a SST.
    // As more data is written to the bottommost level, they will eventually become identical.
    let left_key = FullKey::decode(&sst_info.key_range.left);
    let right_key = FullKey::decode(&sst_info.key_range.right);
    if left_key.user_key.table_id != right_key.user_key.table_id {
        return false;
    }
    if left_key.user_key.table_key.vnode_part() != right_key.user_key.table_key.vnode_part() {
        return false;
    }
    let Some(watermarks) = table_watermarks.get(&left_key.user_key.table_id) else {
        return false;
    };
    should_delete_key_by_watermark(&left_key.user_key.table_key, watermarks)
        && should_delete_key_by_watermark(&right_key.user_key.table_key, watermarks)
}

fn should_delete_key_by_watermark(
    table_key: &TableKey<&[u8]>,
    watermark: &ReadTableWatermark,
) -> bool {
    let (vnode, key) = table_key.split_vnode();
    let Some(w) = watermark.vnode_watermarks.get(&vnode) else {
        return false;
    };
    watermark.direction.filter_by_watermark(key, w)
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, Bytes, BytesMut};
    use risingwave_common::hash::VirtualNode;
    use risingwave_hummock_sdk::key::{FullKey, TableKey};
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
    use risingwave_hummock_sdk::table_watermark::{ReadTableWatermark, WatermarkDirection};

    use crate::hummock::compaction::picker::vnode_watermark_picker::should_delete_sst_by_watermark;

    #[test]
    fn test_should_delete_sst_by_watermark() {
        let table_watermarks = maplit::btreemap! {
            1.into() => ReadTableWatermark {
                direction: WatermarkDirection::Ascending,
                vnode_watermarks: maplit::btreemap! {
                    VirtualNode::from_index(16) => "some_watermark_key_8".into(),
                    VirtualNode::from_index(17) => "some_watermark_key_8".into(),
                },
            },
        };
        let table_key = |vnode_part: usize, key_part: &str| {
            let mut builder = BytesMut::new();
            builder.put_slice(&VirtualNode::from_index(vnode_part).to_be_bytes());
            builder.put_slice(&Bytes::copy_from_slice(key_part.as_bytes()));
            TableKey(builder.freeze())
        };

        let sst_info = SstableInfoInner {
            object_id: 1,
            sst_id: 1,
            key_range: KeyRange {
                left: FullKey::new(2.into(), table_key(16, "some_watermark_key_1"), 0)
                    .encode()
                    .into(),
                right: FullKey::new(2.into(), table_key(16, "some_watermark_key_2"), 0)
                    .encode()
                    .into(),
                right_exclusive: true,
            },
            table_ids: vec![2],
            ..Default::default()
        }
        .into();
        assert!(
            !should_delete_sst_by_watermark(&sst_info, &table_watermarks),
            "should fail because no matching watermark found"
        );

        let sst_info = SstableInfoInner {
            object_id: 1,
            sst_id: 1,
            key_range: KeyRange {
                left: FullKey::new(1.into(), table_key(13, "some_watermark_key_1"), 0)
                    .encode()
                    .into(),
                right: FullKey::new(1.into(), table_key(14, "some_watermark_key_2"), 0)
                    .encode()
                    .into(),
                right_exclusive: true,
            },
            table_ids: vec![1],
            ..Default::default()
        }
        .into();
        assert!(
            !should_delete_sst_by_watermark(&sst_info, &table_watermarks),
            "should fail because no matching vnode found"
        );

        let sst_info = SstableInfoInner {
            object_id: 1,
            sst_id: 1,
            key_range: KeyRange {
                left: FullKey::new(1.into(), table_key(16, "some_watermark_key_1"), 0)
                    .encode()
                    .into(),
                right: FullKey::new(1.into(), table_key(17, "some_watermark_key_2"), 0)
                    .encode()
                    .into(),
                right_exclusive: true,
            },
            table_ids: vec![1],
            ..Default::default()
        }
        .into();
        assert!(
            !should_delete_sst_by_watermark(&sst_info, &table_watermarks),
            "should fail because different vnodes found"
        );

        let sst_info = SstableInfoInner {
            object_id: 1,
            sst_id: 1,
            key_range: KeyRange {
                left: FullKey::new(1.into(), table_key(16, "some_watermark_key_1"), 0)
                    .encode()
                    .into(),
                right: FullKey::new(1.into(), table_key(16, "some_watermark_key_9"), 0)
                    .encode()
                    .into(),
                right_exclusive: true,
            },
            table_ids: vec![1],
            ..Default::default()
        }
        .into();
        assert!(
            !should_delete_sst_by_watermark(&sst_info, &table_watermarks),
            "should fail because right key is greater than watermark"
        );

        let sst_info = SstableInfoInner {
            object_id: 1,
            sst_id: 1,
            key_range: KeyRange {
                left: FullKey::new(1.into(), table_key(16, "some_watermark_key_1"), 0)
                    .encode()
                    .into(),
                right: FullKey::new(1.into(), table_key(16, "some_watermark_key_2"), 0)
                    .encode()
                    .into(),
                right_exclusive: true,
            },
            table_ids: vec![1],
            ..Default::default()
        }
        .into();
        assert!(should_delete_sst_by_watermark(&sst_info, &table_watermarks));
    }
}
