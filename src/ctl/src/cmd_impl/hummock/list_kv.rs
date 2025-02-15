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

use core::ops::Bound::Unbounded;

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::is_max_epoch;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::store::{
    NewReadSnapshotOptions, PrefetchOptions, ReadOptions, StateStoreIter, StateStoreRead,
};
use risingwave_storage::StateStore;

use crate::common::HummockServiceOpts;
use crate::CtlContext;

pub async fn list_kv(
    context: &CtlContext,
    epoch: u64,
    table_id: u32,
    data_dir: Option<String>,
    use_new_object_prefix_strategy: bool,
) -> anyhow::Result<()> {
    let hummock = context
        .hummock_store(HummockServiceOpts::from_env(
            data_dir,
            use_new_object_prefix_strategy,
        )?)
        .await?;
    if is_max_epoch(epoch) {
        tracing::info!("using MAX EPOCH as epoch");
    }
    let range = (Unbounded, Unbounded);
    let read_snapshot = hummock
        .new_read_snapshot(
            HummockReadEpoch::Committed(epoch),
            NewReadSnapshotOptions {
                table_id: TableId { table_id },
            },
        )
        .await?;
    let mut scan_result = read_snapshot
        .iter(
            range,
            ReadOptions {
                table_id: TableId { table_id },
                prefetch_options: PrefetchOptions::prefetch_for_large_range_scan(),
                cache_policy: CachePolicy::NotFill,
                ..Default::default()
            },
        )
        .await?;
    while let Some(item) = scan_result.try_next().await? {
        let (k, v) = item;
        let print_string = format!("[t{}]", k.user_key.table_id.table_id());
        println!("{} {:?} => {:?}", print_string, k, v)
    }
    Ok(())
}
