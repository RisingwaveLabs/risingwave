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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    safe_epoch_read_table_watermarks_impl, safe_epoch_table_watermarks_impl,
};
use risingwave_hummock_sdk::table_watermark::{ReadTableWatermark, TableWatermarks};
use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
use risingwave_hummock_sdk::HummockCompactionTaskId;
use risingwave_pb::hummock::compact_task::TaskType;

use crate::hummock::compaction::picker::VnodeWatermarkCompactionPicker;
use crate::hummock::compaction::selector::{CompactionSelectorContext, DynamicLevelSelectorCore};
use crate::hummock::compaction::{create_compaction_task, CompactionSelector, CompactionTask};
#[derive(Default)]
pub struct VnodeWatermarkCompactionSelector {}

impl CompactionSelector for VnodeWatermarkCompactionSelector {
    fn pick_compaction(
        &mut self,
        task_id: HummockCompactionTaskId,
        context: CompactionSelectorContext<'_>,
    ) -> Option<CompactionTask> {
        let CompactionSelectorContext {
            group,
            levels,
            level_handlers,
            developer_config,
            table_watermarks,
            state_table_info,
            member_table_ids,
            ..
        } = context;
        let dynamic_level_core =
            DynamicLevelSelectorCore::new(group.compaction_config.clone(), developer_config);
        let ctx = dynamic_level_core.calculate_level_base_size(levels);
        let mut picker = VnodeWatermarkCompactionPicker::new();
        let table_watermarks =
            safe_epoch_read_table_watermarks(table_watermarks, state_table_info, member_table_ids);
        let compaction_input = picker.pick_compaction(levels, level_handlers, &table_watermarks)?;
        compaction_input.add_pending_task(task_id, level_handlers);
        Some(create_compaction_task(
            dynamic_level_core.get_config(),
            compaction_input,
            ctx.base_level,
            self.task_type(),
        ))
    }

    fn name(&self) -> &'static str {
        "VnodeWatermarkCompaction"
    }

    fn task_type(&self) -> TaskType {
        TaskType::VnodeWatermark
    }
}

fn safe_epoch_read_table_watermarks(
    table_watermarks: &HashMap<TableId, Arc<TableWatermarks>>,
    state_table_info: &HummockVersionStateTableInfo,
    member_table_ids: &BTreeSet<TableId>,
) -> BTreeMap<TableId, ReadTableWatermark> {
    safe_epoch_read_table_watermarks_impl(safe_epoch_table_watermarks_impl(
        table_watermarks,
        state_table_info,
        &member_table_ids
            .iter()
            .map(TableId::table_id)
            .collect::<Vec<_>>(),
    ))
}
