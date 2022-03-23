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

use std::mem::size_of_val;
use std::time::Instant;

use itertools::Itertools;
use rand::distributions::Uniform;
use rand::prelude::Distribution;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::StateStore;

use super::{Batch, Operations, PerfMetrics};
use crate::utils::latency_stat::LatencyStat;
use crate::utils::workload::{get_epoch, Workload};
use crate::Opts;

impl Operations {
    pub(crate) async fn write_batch(&mut self, store: &impl StateStore, opts: &Opts) {
        let (prefixes, keys) = Workload::new_random_keys(opts, opts.writes as u64, &mut self.rng);
        let values = Workload::new_values(opts, opts.writes as u64, &mut self.rng);

        // add new prefixes and keys to global prefixes and keys
        self.track_prefixes(prefixes);
        self.track_keys(keys.clone());

        let batches = Workload::make_batches(opts, keys, values);

        let perf = self.run_batches(store, opts, batches).await;

        println!(
            "
    writebatch
      {}
      KV ingestion OPS: {}  {} bytes/sec",
            perf.stat, perf.qps, perf.bytes_pre_sec
        );
    }

    pub(crate) async fn delete_random(&mut self, store: &impl StateStore, opts: &Opts) {
        let delete_keys = match self.keys.is_empty() {
            true => Workload::new_random_keys(opts, opts.deletes as u64, &mut self.rng).1,
            false => {
                let dist = Uniform::from(0..self.keys.len());
                (0..opts.deletes)
                    .into_iter()
                    .map(|_| self.keys[dist.sample(&mut self.rng)].clone())
                    .collect_vec()
            }
        };
        self.untrack_keys(&delete_keys);

        let values = vec![None; opts.deletes as usize];

        let batches = Workload::make_batches(opts, delete_keys, values);

        let perf = self.run_batches(store, opts, batches).await;

        println!(
            "
    deleterandom
      {}
      KV ingestion OPS: {}  {} bytes/sec",
            perf.stat, perf.qps, perf.bytes_pre_sec
        );
    }

    async fn run_batches(
        &mut self,
        store: &impl StateStore,
        opts: &Opts,
        mut batches: Vec<Batch>,
    ) -> PerfMetrics {
        let batches_len = batches.len();
        // TODO(Ting Sun): use sizes from metrics directly
        let size = batches
            .iter()
            .flat_map(|batch| batch.iter())
            .map(|(key, value)| size_of_val(key) + size_of_val(value))
            .sum::<usize>();

        // partitioned these batches for each concurrency
        let mut grouped_batches = vec![vec![]; opts.concurrency_num as usize];
        for (i, batch) in batches.drain(..).enumerate() {
            grouped_batches[i % opts.concurrency_num as usize].push(batch);
        }

        let mut args = grouped_batches
            .into_iter()
            .map(|batches| (batches, store.clone()))
            .collect_vec();

        let futures = args
            .drain(..)
            .map(|(batches, store)| async move {
                let mut latencies: Vec<u128> = vec![];
                for batch in batches {
                    let start = Instant::now();
                    let batch = batch
                        .into_iter()
                        .map(|(k, v)| (k, v.map(StorageValue::from)))
                        .collect_vec();
                    store.ingest_batch(batch, get_epoch()).await.unwrap();
                    let time_nano = start.elapsed().as_nanos();
                    latencies.push(time_nano);
                }
                latencies
            })
            .collect_vec();

        let total_start = Instant::now();

        let handles = futures.into_iter().map(tokio::spawn).collect_vec();
        let latencies_list = futures::future::join_all(handles).await;

        let total_time_nano = total_start.elapsed().as_nanos();

        // calculate metrics
        let latencies: Vec<u128> = latencies_list
            .into_iter()
            .flat_map(|res| res.unwrap())
            .collect_vec();
        let stat = LatencyStat::new(latencies);
        // calculate operation per second
        let ops = opts.batch_size as u128 * 1_000_000_000 * batches_len as u128 / total_time_nano;
        let bytes_pre_sec = size as u128 * 1_000_000_000 / total_time_nano;

        PerfMetrics {
            stat,
            qps: ops,
            bytes_pre_sec,
        }
    }
}
