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

use std::ops::Bound;

use futures::{pin_mut, StreamExt};
use risingwave_common::row;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{ScalarImpl, ScalarRef, ScalarRefImpl};
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::source::SplitId;
use risingwave_pb::catalog::PbTable;
use risingwave_storage::StateStore;

use super::source_backfill_executor::{BackfillStateWithProgress, BackfillStates};
use crate::common::table::state_table::StateTable;
use crate::executor::error::StreamExecutorError;
use crate::executor::StreamExecutorResult;

pub struct BackfillStateTableHandler<S: StateStore> {
    state_store: StateTable<S>,
}

impl<S: StateStore> BackfillStateTableHandler<S> {
    /// See also [`super::SourceStateTableHandler::from_table_catalog`] for how the state table looks like.
    pub async fn from_table_catalog(table_catalog: &PbTable, store: S) -> Self {
        Self {
            state_store: StateTable::from_table_catalog(table_catalog, store, None).await,
        }
    }

    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_store.init_epoch(epoch).await
    }

    fn string_to_scalar(rhs: impl Into<String>) -> ScalarImpl {
        ScalarImpl::Utf8(rhs.into().into_boxed_str())
    }

    pub(crate) async fn get(&self, key: &SplitId) -> StreamExecutorResult<Option<OwnedRow>> {
        self.state_store
            .get_row(row::once(Some(Self::string_to_scalar(key.as_ref()))))
            .await
            .map_err(StreamExecutorError::from)
    }

    /// XXX: we might get stale data for other actors' writes, but it's fine?
    pub async fn scan_may_stale(&self) -> StreamExecutorResult<Vec<BackfillStateWithProgress>> {
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);

        let state_table_iter = self
            .state_store
            .iter_with_prefix(None::<OwnedRow>, sub_range, Default::default())
            .await?;
        pin_mut!(state_table_iter);

        let mut ret = vec![];
        while let Some(item) = state_table_iter.next().await {
            let row = item?.into_owned_row();
            let state = match row.datum_at(1) {
                Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                    BackfillStateWithProgress::restore_from_json(jsonb_ref.to_owned_scalar())?
                }
                _ => unreachable!(),
            };
            ret.push(state);
        }
        tracing::trace!("scan SourceBackfill state table: {:?}", ret);
        Ok(ret)
    }

    async fn set(
        &mut self,
        key: SplitId,
        state: BackfillStateWithProgress,
    ) -> StreamExecutorResult<()> {
        let row = [
            Some(Self::string_to_scalar(key.as_ref())),
            Some(ScalarImpl::Jsonb(state.encode_to_json())),
        ];
        match self.get(&key).await? {
            Some(prev_row) => {
                self.state_store.update(prev_row, row);
            }
            None => {
                self.state_store.insert(row);
            }
        }
        Ok(())
    }

    pub async fn delete(&mut self, key: &SplitId) -> StreamExecutorResult<()> {
        if let Some(prev_row) = self.get(key).await? {
            self.state_store.delete(prev_row);
        }

        Ok(())
    }

    pub async fn set_states(&mut self, states: BackfillStates) -> StreamExecutorResult<()> {
        for (split_id, state) in states {
            self.set(split_id, state).await?;
        }
        Ok(())
    }

    pub async fn trim_state(
        &mut self,
        to_trim: impl IntoIterator<Item = SplitId>,
    ) -> StreamExecutorResult<()> {
        for split_id in to_trim {
            tracing::info!("trimming source state for split {}", split_id);
            self.delete(&split_id).await?;
        }

        Ok(())
    }

    pub(super) fn state_store(&self) -> &StateTable<S> {
        &self.state_store
    }

    pub(super) async fn commit(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.state_store
            .commit_assert_no_update_vnode_bitmap(epoch)
            .await?;
        Ok(())
    }

    pub(super) async fn new_committed_reader(
        &self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<BackfillStateTableCommittedReader<'_, S>> {
        self.state_store
            .try_wait_committed_epoch(epoch.prev)
            .await?;
        Ok(BackfillStateTableCommittedReader { handle: self })
    }
}

pub(super) struct BackfillStateTableCommittedReader<'a, S: StateStore> {
    handle: &'a BackfillStateTableHandler<S>,
}

impl<S: StateStore> BackfillStateTableCommittedReader<'_, S> {
    pub(super) async fn try_recover_from_state_store(
        &self,
        split_id: &SplitId,
    ) -> StreamExecutorResult<Option<BackfillStateWithProgress>> {
        Ok(self
            .handle
            .get(split_id)
            .await?
            .map(|row| match row.datum_at(1) {
                Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                    BackfillStateWithProgress::restore_from_json(jsonb_ref.to_owned_scalar())
                }
                _ => unreachable!(),
            })
            .transpose()?)
    }
}
