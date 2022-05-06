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

use std::fmt::Debug;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use log::error;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result as RwResult, RwError};
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::{Keyspace, StateStore};
#[allow(unused_imports)]
use serde::{Deserialize, Serialize};

use crate::{ConnectorState, SplitImpl, SplitMetaData};

/// `SourceState` Represents an abstraction of state,
/// e.g. if the Kafka Source state consists of `topic` `partition_id` and `offset`.
pub trait SourceState: Debug + Clone {
    /// A value that can uniquely represent a state trait object
    fn identifier(&self) -> String;
    /// The binary code for the state. In the concrete implementation it is recommended to
    /// `serialize` only the required fields. And the other fields can be `transient`
    fn encode(&self) -> Bytes;
    fn decode(&self, values: Bytes) -> Self;
}

#[derive(Clone)]
pub struct SourceStateHandler<S: StateStore> {
    keyspace: Keyspace<S>,
}

impl<S: StateStore> Debug for SourceStateHandler<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceStateHandler").finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
struct StateStoredKey {
    state_identifier: String,
    epoch: u64,
}

impl Default for StateStoredKey {
    fn default() -> Self {
        Self {
            state_identifier: "".to_string(),
            epoch: u64::MIN,
        }
    }
}

impl StateStoredKey {
    fn new(state_identifier: String, epoch: u64) -> Self {
        Self {
            state_identifier,
            epoch,
        }
    }

    // default key format [state_identifier | epoch]
    fn build_stored_key(&self) -> String {
        [
            self.state_identifier.clone(),
            "|".to_string(),
            self.epoch.clone().to_string(),
        ]
        .concat()
    }

    fn build_from_bytes(&self, key_bytes: Bytes) -> Option<Self> {
        if key_bytes.is_empty() {
            None
        } else {
            let key_string = String::from_utf8(key_bytes.to_vec()).unwrap();
            let key_string_len = key_string.len();
            let delimiter_idx = key_string.find('|').unwrap();

            let identifier = &(*key_string.as_str())[0..delimiter_idx];
            let epoch_string = &(*key_string.as_str())[delimiter_idx + 1..key_string_len];
            let epoch = epoch_string.parse::<u64>().unwrap();
            Some(StateStoredKey::new(String::from(identifier), epoch))
        }
    }
}

impl<S: StateStore> SourceStateHandler<S> {
    pub fn new(keyspace: Keyspace<S>) -> Self {
        Self { keyspace }
    }

    /// This function provides the ability to persist the source state
    /// and needs to be invoked by the ``SourceReader`` to call it,
    /// and will return the error when the dependent ``StateStore`` handles the error.
    /// The caller should ensure that the passed parameters are not empty.
    pub async fn take_snapshot<SS>(&self, states: Vec<SS>, epoch: u64) -> Result<()>
    where
        SS: SplitMetaData,
    {
        if states.is_empty() {
            // TODO should be a clear Error Code
            Err(anyhow!("states require not null"))
        } else {
            let mut write_batch = self.keyspace.state_store().start_write_batch();
            let mut local_batch = write_batch.prefixify(&self.keyspace);
            states.iter().for_each(|state| {
                // state inner key format (state_identifier | epoch)
                let inner_key = StateStoredKey::new(state.id(), epoch).build_stored_key();
                let value = state.to_json_bytes();
                // TODO(Yuanxin): Implement value meta
                local_batch.put(inner_key, StorageValue::new_default_put(value));
            });
            // If an error is returned, the underlying state should be rollback
            let ingest_rs = write_batch.ingest(epoch).await;
            match ingest_rs {
                Err(e) => {
                    error!(
                        "SourceStateHandler take_snapshot() batch.ingest Error,cause by {:?}",
                        e
                    );
                    Err(anyhow!(e))
                }
                _ => Ok(()),
            }
        }
    }

    /// Initializes the state of the specified ``state_identifier`` and returns an empty vec
    /// if it does not exist (e.g., the first accessible source).
    ///
    /// The function returns a collection of tuple (epoch, state).
    pub async fn restore_states(&self, state_identifier: String) -> Result<Vec<(u64, Bytes)>> {
        // TODO: do we need snapshot read here?
        let epoch = u64::MAX;
        let scan_rs = self.keyspace.scan_strip_prefix(Option::None, epoch).await;
        let mut restore_values = Vec::new();
        match scan_rs {
            Ok(scan_list) => {
                for pair in scan_list {
                    let stored_key = StateStoredKey::default().build_from_bytes(pair.0).unwrap();
                    if stored_key
                        .clone()
                        .state_identifier
                        .eq(&state_identifier.clone())
                    {
                        restore_values.push((stored_key.epoch, pair.1))
                    }
                }
                Ok(restore_values)
            }
            Err(e) => Err(anyhow!(e)),
        }
    }

    pub async fn try_recover_from_state_store(
        &self,
        stream_source_splits: &SplitImpl,
        epoch: u64,
    ) -> RwResult<ConnectorState> {
        let mut state = self
            .restore_states(stream_source_splits.id())
            .await
            .map_err(|e| RwError::from(InternalError(e.to_string())))?
            .iter()
            .filter(|(e, _)| e == &epoch)
            .map(|(_, s)| {
                ConnectorState::restore_from_bytes(s)
                    .map_err(|e| RwError::from(InternalError(e.to_string())))
            })
            .collect::<RwResult<Vec<ConnectorState>>>()?;

        assert!(state.len() <= 1);

        if state.len() == 1 {
            Ok(state.pop().unwrap())
        } else if state.is_empty() {
            Err(RwError::from(InternalError(format!(
                "cannot found state for {:?}, epoch: {:?}",
                stream_source_splits, epoch
            ))))
        } else {
            Err(RwError::from(InternalError(format!(
                "expected load one state from epoch {:?}, got {:?}: {:?}",
                epoch,
                state.len(),
                state
            ))))
        }
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    const TEST_EPOCH: u64 = 1000_u64;
    const TEST_STATE_IDENTIFIER: &str = "t-p01";

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestSourceState {
        partition: String,
        offset: i64,
    }

    impl TestSourceState {
        fn new(partition: String, offset: i64) -> Self {
            Self { partition, offset }
        }
    }

    impl SplitMetaData for TestSourceState {
        fn id(&self) -> String {
            self.partition.clone()
        }

        fn to_json_bytes(&self) -> Bytes {
            Bytes::from(serde_json::to_string(self).unwrap())
        }

        fn restore_from_bytes(bytes: &[u8]) -> Result<Self> {
            serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
        }
    }

    #[test]
    fn test_new_state_stored_key() {
        let state_inner_key = StateStoredKey::new(TEST_STATE_IDENTIFIER.to_string(), TEST_EPOCH);
        assert_eq!(
            TEST_STATE_IDENTIFIER.to_string(),
            state_inner_key.state_identifier
        );
        assert_eq!(TEST_EPOCH, state_inner_key.epoch);
    }

    #[test]
    fn test_build_stored_key() {
        assert_eq!(
            [
                TEST_STATE_IDENTIFIER.to_string(),
                "|".to_string(),
                TEST_EPOCH.to_string()
            ]
            .concat(),
            StateStoredKey::new(TEST_STATE_IDENTIFIER.to_string(), TEST_EPOCH).build_stored_key()
        );
    }

    #[test]
    fn test_stored_key_from_bytes() {
        let ss_key = StateStoredKey::new(TEST_STATE_IDENTIFIER.to_string(), TEST_EPOCH);
        let key_bytes = Bytes::from(
            [
                TEST_STATE_IDENTIFIER.to_string(),
                "|".to_string(),
                TEST_EPOCH.to_string(),
            ]
            .concat(),
        );
        let ss_key_from_bytes = ss_key.build_from_bytes(key_bytes);
        if let Some(ss_val) = ss_key_from_bytes {
            assert_eq!(ss_key, ss_val)
        };
    }

    #[test]
    fn test_state_encode() -> Result<()> {
        let offset = 100_i64;
        let partition = String::from("p0");
        let state_instance = TestSourceState::new(partition.clone(), offset);
        assert_eq!(offset, state_instance.offset);
        assert_eq!(partition, state_instance.partition);
        println!("TestSourceState = {:?}", state_instance);
        let encode_value = state_instance.to_json_bytes();
        let decode_value = TestSourceState::restore_from_bytes(&encode_value)?;
        println!("decode from Bytes instance = {:?}", decode_value);
        assert_eq!(offset, decode_value.offset);
        assert_eq!(partition, decode_value.partition);

        Ok(())
    }

    fn new_test_keyspace() -> Keyspace<MemoryStateStore> {
        let test_mem_state_store = MemoryStateStore::new();
        Keyspace::executor_root(test_mem_state_store, 1)
    }

    fn test_state_store_vec() -> Vec<TestSourceState> {
        (0..2)
            .into_iter()
            .map(|i| TestSourceState::new(["p".to_string(), i.to_string()].concat(), i))
            .collect_vec()
    }

    async fn take_snapshot_and_get_states(
        state_handler: SourceStateHandler<MemoryStateStore>,
    ) -> (Vec<TestSourceState>, Result<()>) {
        let current_epoch = 1000;
        let states = test_state_store_vec();
        println!("Vec<TestSourceStat>={:?}", states.clone());
        let rs = state_handler
            .take_snapshot(states.clone(), current_epoch)
            .await;
        (states, rs)
    }

    #[tokio::test]
    async fn test_take_snapshot() {
        let current_epoch = 1000;
        let state_store_handler = SourceStateHandler::new(new_test_keyspace());
        let _rs = take_snapshot_and_get_states(state_store_handler.clone()).await;
        let stored_states = state_store_handler
            .keyspace
            .scan(None, current_epoch)
            .await
            .unwrap();
        assert_ne!(0, stored_states.len());
    }

    #[tokio::test]
    async fn test_empty_state_restore() {
        let partition = "p01".to_string();
        let state_store_handler = SourceStateHandler::new(new_test_keyspace());

        let list_states = state_store_handler.restore_states(partition).await.unwrap();
        println!("current list_states={:?}", list_states);
        assert_eq!(0, list_states.len())
    }

    #[tokio::test]
    async fn test_state_restore() {
        let state_store_handler = SourceStateHandler::new(new_test_keyspace());
        let saved_states = take_snapshot_and_get_states(state_store_handler.clone())
            .await
            .0;

        for state in saved_states {
            let identifier = state.id();
            let state_pair = state_store_handler
                .restore_states(identifier)
                .await
                .unwrap();
            println!("source_state_handler restore state_pair={:?}", state_pair);
            state_pair.into_iter().for_each(|s| {
                assert_eq!(
                    state.offset,
                    TestSourceState::restore_from_bytes(&s.1).unwrap().offset
                );
                assert_eq!(
                    state.partition,
                    TestSourceState::restore_from_bytes(&s.1).unwrap().partition
                );
            });
        }
    }
}
