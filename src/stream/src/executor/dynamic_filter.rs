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

use std::ops::Bound::{self, *};

use futures::stream;
use risingwave_common::array::{Array, ArrayImpl, Op};
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{self, once, OwnedRow as RowData};
use risingwave_common::types::{DefaultOrd, ToDatumRef, ToOwnedDatum};
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_expr::expr::{
    build_func_non_strict, InputRefExpression, LiteralExpression, NonStrictExpression,
};
use risingwave_pb::expr::expr_node::Type as PbExprNodeType;
use risingwave_pb::expr::expr_node::Type::{
    GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
};
use risingwave_storage::store::PrefetchOptions;

use super::barrier_align::*;
use crate::common::table::state_table::WatermarkCacheParameterizedStateTable;
use crate::consistency::consistency_panic;
use crate::executor::prelude::*;
use crate::task::ActorEvalErrorReport;

pub struct DynamicFilterExecutor<S: StateStore, const USE_WATERMARK_CACHE: bool> {
    ctx: ActorContextRef,

    eval_error_report: ActorEvalErrorReport,

    schema: Schema,
    source_l: Option<Executor>,
    source_r: Option<Executor>,
    key_l: usize,
    comparator: PbExprNodeType,
    left_table: WatermarkCacheParameterizedStateTable<S, USE_WATERMARK_CACHE>,
    right_table: StateTable<S>,
    metrics: Arc<StreamingMetrics>,
    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,
    cleaned_by_watermark: bool,
}

impl<S: StateStore, const USE_WATERMARK_CACHE: bool> DynamicFilterExecutor<S, USE_WATERMARK_CACHE> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        eval_error_report: ActorEvalErrorReport,
        schema: Schema,
        source_l: Executor,
        source_r: Executor,
        key_l: usize,
        comparator: PbExprNodeType,
        state_table_l: WatermarkCacheParameterizedStateTable<S, USE_WATERMARK_CACHE>,
        state_table_r: StateTable<S>,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        cleaned_by_watermark: bool,
    ) -> Self {
        Self {
            ctx,
            eval_error_report,
            schema,
            source_l: Some(source_l),
            source_r: Some(source_r),
            key_l,
            comparator,
            left_table: state_table_l,
            right_table: state_table_r,
            metrics,
            chunk_size,
            cleaned_by_watermark,
        }
    }

    async fn apply_batch(
        &mut self,
        chunk: &StreamChunk,
        filter_condition: Option<NonStrictExpression>,
        below_watermark_condition: Option<NonStrictExpression>,
    ) -> Result<(Vec<Op>, Bitmap), StreamExecutorError> {
        let mut new_ops = Vec::with_capacity(chunk.capacity());
        let mut new_visibility = BitmapBuilder::with_capacity(chunk.capacity());
        let mut last_res = false;

        let filter_results = if let Some(cond) = filter_condition {
            Some(cond.eval_infallible(chunk).await)
        } else {
            None
        };

        let below_watermark = if let Some(cond) = below_watermark_condition {
            Some(cond.eval_infallible(chunk).await)
        } else {
            None
        };

        for (idx, (op, row)) in chunk.rows().enumerate() {
            let left_val = row.datum_at(self.key_l).to_owned_datum();

            let satisfied_dyn_filter_cond = if let Some(array) = &filter_results {
                if let ArrayImpl::Bool(results) = &**array {
                    results.value_at(idx).unwrap_or(false)
                } else {
                    panic!("dynamic filter condition eval must return bool array")
                }
            } else {
                // A NULL right value implies a false evaluation for all rows
                false
            };
            let below_watermark = if let Some(array) = &below_watermark {
                if let ArrayImpl::Bool(results) = &**array {
                    results.value_at(idx).unwrap_or(false)
                } else {
                    panic!("below watermark check condition eval must return bool array")
                }
            } else {
                // there was no state cleaning watermark before
                false
            };

            match op {
                Op::Insert | Op::Delete => {
                    new_ops.push(op);
                    if satisfied_dyn_filter_cond {
                        new_visibility.append(true);
                    } else {
                        new_visibility.append(false);
                    }
                }
                Op::UpdateDelete => {
                    last_res = satisfied_dyn_filter_cond;
                }
                Op::UpdateInsert => match (last_res, satisfied_dyn_filter_cond) {
                    (true, false) => {
                        new_ops.push(Op::Delete);
                        new_ops.push(Op::UpdateInsert);
                        new_visibility.append(true);
                        new_visibility.append(false);
                    }
                    (false, true) => {
                        new_ops.push(Op::UpdateDelete);
                        new_ops.push(Op::Insert);
                        new_visibility.append(false);
                        new_visibility.append(true);
                    }
                    (true, true) => {
                        new_ops.push(Op::UpdateDelete);
                        new_ops.push(Op::UpdateInsert);
                        new_visibility.append(true);
                        new_visibility.append(true);
                    }
                    (false, false) => {
                        new_ops.push(Op::UpdateDelete);
                        new_ops.push(Op::UpdateInsert);
                        new_visibility.append(false);
                        new_visibility.append(false);
                    }
                },
            }

            // No need to maintain the records in the left table after the records become
            // below state cleaning watermark.
            // Here's why:
            // 1. For >= and >, watermark on rhs means that any future changes that satisfy the
            //    filter condition should >= state clean watermark, and those below the watermark
            //    will never satisfy the filter condition.
            // 2. For < and <=, watermark on rhs means that any future changes that are below
            //    the watermark will always satisfy the filter condition, so those changes should
            //    always be directly sent to downstream without any state table maintenance.
            if below_watermark {
                continue;
            }

            // Store the rows without a null left key
            // null key in left side of predicate should never be stored
            // (it will never satisfy the filter condition)
            if left_val.is_some() {
                // Note that when updating `key_l` column, in most cases the timestamp column, if the
                // timestamp is updated from a value below watermark to a value above watermark, it's
                // possible that, due to state cleaning strategy, the old row may still exist in the
                // state table. We have to be careful about the state table insertion. Now it works
                // because the `key_l` column is in the state table pk as inferred by the frontend.
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        self.left_table.insert(row);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.left_table.delete(row);
                    }
                }
            }
        }

        let new_visibility = new_visibility.finish();
        self.left_table.try_flush().await?;
        Ok((new_ops, new_visibility))
    }

    /// Returns the required range, whether the latest value is in lower bound (rather than upper)
    /// and whether to insert or delete the range.
    fn get_range(
        &self,
        curr: &Datum,
        prev: Datum,
    ) -> ((Bound<ScalarImpl>, Bound<ScalarImpl>), bool, bool) {
        debug_assert_ne!(curr, &prev);
        let curr_is_some = curr.is_some();
        match (curr.clone(), prev) {
            (Some(c), None) | (None, Some(c)) => {
                let range = match self.comparator {
                    GreaterThan => (Excluded(c), Unbounded),
                    GreaterThanOrEqual => (Included(c), Unbounded),
                    LessThan => (Unbounded, Excluded(c)),
                    LessThanOrEqual => (Unbounded, Included(c)),
                    _ => unreachable!(),
                };
                let is_insert = curr_is_some;
                // The new bound is always towards the last known value
                let is_lower = matches!(self.comparator, GreaterThan | GreaterThanOrEqual);
                (range, is_lower, is_insert)
            }
            (Some(c), Some(p)) => {
                if c.default_cmp(&p).is_lt() {
                    let range = match self.comparator {
                        GreaterThan | LessThanOrEqual => (Excluded(c), Included(p)),
                        GreaterThanOrEqual | LessThan => (Included(c), Excluded(p)),
                        _ => unreachable!(),
                    };
                    let is_insert = matches!(self.comparator, GreaterThan | GreaterThanOrEqual);
                    (range, true, is_insert)
                } else {
                    // c > p
                    let range = match self.comparator {
                        GreaterThan | LessThanOrEqual => (Excluded(p), Included(c)),
                        GreaterThanOrEqual | LessThan => (Included(p), Excluded(c)),
                        _ => unreachable!(),
                    };
                    let is_insert = matches!(self.comparator, LessThan | LessThanOrEqual);
                    (range, false, is_insert)
                }
            }
            (None, None) => unreachable!(), // prev != curr
        }
    }

    async fn recover_rhs(&mut self) -> Result<Option<RowData>, StreamExecutorError> {
        // Recover value for RHS if available
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);
        let rhs_stream = self
            .right_table
            .iter_with_prefix(row::empty(), sub_range, Default::default())
            .await?;
        pin_mut!(rhs_stream);

        if let Some(res) = rhs_stream.next().await {
            let value = res?.into_owned_row();
            assert!(rhs_stream.next().await.is_none());
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }

    fn to_row_bound(bound: Bound<ScalarImpl>) -> Bound<impl Row> {
        bound.map(|s| once(Some(s)))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let input_l = self.source_l.take().unwrap();
        let input_r = self.source_r.take().unwrap();

        // Derive the dynamic expression
        let l_data_type = input_l.schema().data_types()[self.key_l].clone();
        let r_data_type = input_r.schema().data_types()[0].clone();
        // The types are aligned by frontend.
        assert_eq!(l_data_type, r_data_type);

        let build_cond = {
            let l_data_type = l_data_type.clone();
            let r_data_type = r_data_type.clone();
            let eval_error_report = self.eval_error_report.clone();
            move |cmp: PbExprNodeType, literal: Datum| {
                literal.map(|scalar| {
                    build_func_non_strict(
                        cmp,
                        DataType::Boolean,
                        vec![
                            Box::new(InputRefExpression::new(l_data_type.clone(), self.key_l)),
                            Box::new(LiteralExpression::new(r_data_type.clone(), Some(scalar))),
                        ],
                        eval_error_report.clone(),
                    )
                })
            }
        };

        let aligned_stream = barrier_align(
            input_l.execute(),
            input_r.execute(),
            self.ctx.id,
            self.ctx.fragment_id,
            self.metrics.clone(),
            "Dynamic Filter",
        );

        pin_mut!(aligned_stream);

        let barrier = expect_first_barrier_from_aligned_stream(&mut aligned_stream).await?;
        let first_epoch = barrier.epoch;
        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);
        self.right_table.init_epoch(first_epoch).await?;
        self.left_table.init_epoch(first_epoch).await?;

        let recovered_rhs = self.recover_rhs().await?;
        let recovered_rhs_value = recovered_rhs.as_ref().map(|r| r[0].clone());
        // At the beginning of an epoch, the `committed_rhs_value` == `staging_rhs_value`
        let mut committed_rhs_value: Option<Datum> = recovered_rhs_value.clone();
        let mut staging_rhs_value: Option<Datum> = recovered_rhs_value;
        // This is only required to be some if the row arrived during this epoch.
        let mut committed_rhs_row = recovered_rhs.clone();
        let mut staging_rhs_row = recovered_rhs;

        let mut stream_chunk_builder =
            StreamChunkBuilder::new(self.chunk_size, self.schema.data_types());

        let mut staging_state_watermark = None;
        let mut watermark_to_propagate = None;
        let can_propagate_watermark = matches!(self.comparator, GreaterThan | GreaterThanOrEqual);

        #[for_await]
        for msg in aligned_stream {
            match msg? {
                AlignedMessage::Left(chunk) => {
                    // Reuse the logic from `FilterExecutor`
                    let chunk = chunk.compact(); // Is this unnecessary work?

                    // The condition is `None` if it is always false by virtue of a NULL right
                    // input, so we save evaluating it on the datachunk.
                    let filter_condition =
                        build_cond(self.comparator, committed_rhs_value.clone().flatten())
                            .transpose()?;

                    // The condition is `None` if there's no committed state cleaning watermark before.
                    // Note that we should not use `state_cleaning_watermark` variable here, because
                    // it represents outstanding watermark to be applied. Here we need the watermark
                    // that has been applied to the state table, just like why we use `prev_epoch_value`
                    // instead of `current_epoch_value` for `filter_condition`.
                    let below_watermark_condition =
                        build_cond(LessThan, self.left_table.get_committed_watermark().cloned())
                            .transpose()?;

                    let (new_ops, new_visibility) = self
                        .apply_batch(&chunk, filter_condition, below_watermark_condition)
                        .await?;

                    let columns = chunk.into_parts().0.into_parts().0;

                    if new_visibility.count_ones() > 0 {
                        let new_chunk =
                            StreamChunk::with_visibility(new_ops, columns, new_visibility);
                        yield Message::Chunk(new_chunk)
                    }
                }
                AlignedMessage::Right(chunk) => {
                    // Record the latest update to the right value
                    let chunk = chunk.compact(); // Is this unnecessary work?
                    let (data_chunk, ops) = chunk.into_parts();

                    for (row, op) in data_chunk.rows().zip_eq_debug(ops.iter()) {
                        match *op {
                            Op::UpdateInsert | Op::Insert => {
                                staging_rhs_value = Some(row.datum_at(0).to_owned_datum());
                                staging_rhs_row = Some(row.into_owned_row());
                            }
                            Op::UpdateDelete | Op::Delete => {
                                // To be consistent, there must be an existing `current_epoch_value`
                                // equivalent to row indicated for
                                // deletion.
                                if Some(row.datum_at(0))
                                    != staging_rhs_value.as_ref().map(ToDatumRef::to_datum_ref)
                                {
                                    consistency_panic!(
                                        current = ?staging_rhs_value,
                                        to_delete = ?row,
                                        "inconsistent delete",
                                    );
                                }
                                staging_rhs_value = None;
                                staging_rhs_row = None;
                            }
                        }
                    }
                }
                AlignedMessage::WatermarkLeft(_) => {
                    // Do nothing.
                }
                AlignedMessage::WatermarkRight(watermark) => {
                    if self.cleaned_by_watermark {
                        staging_state_watermark = Some(watermark.val.clone());
                    }
                    if can_propagate_watermark {
                        watermark_to_propagate = Some(watermark);
                    }
                }
                AlignedMessage::Barrier(barrier) => {
                    // Commit the staging RHS value.
                    //
                    // This block is guaranteed to be idempotent even if we may encounter multiple
                    // barriers since `committed_rhs_value` is always be reset to the equivalent of
                    // `staging_rhs_value` at the end of this block. Likewise, `committed_rhs_row`
                    // will always be equal to `staging_rhs_row`. It is thus guaranteed not to commit
                    // state or produce chunks as long as no new chunks have arrived since the previous
                    // barrier.
                    let curr: Datum = staging_rhs_value.clone().flatten();
                    let prev: Datum = committed_rhs_value.flatten();
                    if prev != curr {
                        let (range, _latest_is_lower, is_insert) = self.get_range(&curr, prev);

                        let range = (Self::to_row_bound(range.0), Self::to_row_bound(range.1));
                        // TODO: prefetching for append-only case.
                        let streams = futures::future::try_join_all(
                            self.left_table.vnodes().iter_vnodes().map(|vnode| {
                                self.left_table.iter_with_vnode(
                                    vnode,
                                    &range,
                                    PrefetchOptions::prefetch_for_small_range_scan(),
                                )
                            }),
                        )
                        .await?
                        .into_iter()
                        .map(Box::pin);

                        #[for_await]
                        for res in stream::select_all(streams) {
                            let row = res?;
                            if let Some(chunk) = stream_chunk_builder.append_row(
                                // All rows have a single identity at this point
                                if is_insert { Op::Insert } else { Op::Delete },
                                row.as_ref(),
                            ) {
                                yield Message::Chunk(chunk);
                            }
                        }

                        if let Some(chunk) = stream_chunk_builder.take() {
                            yield Message::Chunk(chunk);
                        }
                    }

                    if let Some(watermark) = staging_state_watermark.take() {
                        self.left_table.update_watermark(watermark.clone());
                    };

                    if let Some(watermark) = watermark_to_propagate.take() {
                        yield Message::Watermark(watermark.with_idx(self.key_l));
                    };

                    // Update the committed value on RHS if it has changed.
                    if committed_rhs_row != staging_rhs_row {
                        // Only write the RHS value if this actor is in charge of vnode 0 on LHS
                        // Otherwise, we only actively replicate the changes.
                        if self.left_table.vnodes().is_set(0) {
                            // If both `None`, then this branch is inactive.
                            // Hence, at least one is `Some`, hence at least one update.
                            if let Some(old_row) = committed_rhs_row.take() {
                                self.right_table.delete(old_row);
                            }
                            if let Some(row) = &staging_rhs_row {
                                self.right_table.insert(row);
                            }
                        }
                    }

                    self.left_table.commit(barrier.epoch).await?;
                    self.right_table.commit(barrier.epoch).await?;

                    // Update the last committed RHS row and value.
                    committed_rhs_row.clone_from(&staging_rhs_row);
                    committed_rhs_value = Some(curr);

                    // Update the vnode bitmap for the left state table if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        let (_previous_vnode_bitmap, _cache_may_stale) =
                            self.left_table.update_vnode_bitmap(vnode_bitmap);
                    }

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

impl<S: StateStore, const USE_WATERMARK_CACHE: bool> Execute
    for DynamicFilterExecutor<S, USE_WATERMARK_CACHE>
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::batch_table::BatchTable;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};

    async fn create_in_memory_state_table(
        mem_state: MemoryStateStore,
    ) -> (StateTable<MemoryStateStore>, StateTable<MemoryStateStore>) {
        let column_descs = vec![ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64)];
        let order_types = vec![OrderType::ascending()];
        let pk_indices = vec![0];
        // TODO: use consistent operations for dynamic filter <https://github.com/risingwavelabs/risingwave/issues/3893>
        let state_table_l = StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::new(0),
                column_descs.clone(),
                order_types.clone(),
                pk_indices.clone(),
                0,
            ),
            mem_state.clone(),
            None,
        )
        .await;
        let state_table_r = StateTable::from_table_catalog(
            &gen_pbtable(TableId::new(1), column_descs, order_types, pk_indices, 0),
            mem_state,
            None,
        )
        .await;
        (state_table_l, state_table_r)
    }

    async fn create_executor(
        comparator: PbExprNodeType,
        store: MemoryStateStore,
        cleaned_by_watermark: bool,
    ) -> (MessageSender, MessageSender, BoxedMessageStream) {
        let (mem_state_l, mem_state_r) = create_in_memory_state_table(store).await;
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let (tx_l, source_l) = MockSource::channel();
        let source_l = source_l.into_executor(schema.clone(), vec![0]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r.into_executor(schema, vec![]);

        let ctx = ActorContext::for_test(123);
        let eval_error_report = ActorEvalErrorReport {
            actor_context: ctx.clone(),
            identity: "DynamicFilterExecutor".into(),
        };
        let executor = DynamicFilterExecutor::<MemoryStateStore, false>::new(
            ctx,
            eval_error_report,
            source_l.schema().clone(),
            source_l,
            source_r,
            0,
            comparator,
            mem_state_l,
            mem_state_r,
            Arc::new(StreamingMetrics::unused()),
            1024,
            cleaned_by_watermark,
        );
        (tx_l, tx_r, executor.boxed().execute())
    }

    #[tokio::test]
    async fn test_dynamic_filter_rhs_recovery_gt() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 1
             + 2
             + 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 4
             - 3",
        );
        let chunk_r0 = StreamChunk::from_pretty(
            "  I
             + 1",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             - 1
             + 2",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 1",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 4",
        );
        let mem_store = MemoryStateStore::new();
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(PbExprNodeType::GreaterThan, mem_store.clone(), false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 0th right chunk
        tx_r.push_chunk(chunk_r0);

        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // Drop executor corresponding to node failure
        drop(tx_l);
        drop(tx_r);
        drop(dynamic_filter);

        // Recover executor from state store
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(PbExprNodeType::GreaterThan, mem_store.clone(), false).await;

        // push the recovery barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);

        // Get recovery barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);
        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(3), false);
        tx_r.push_barrier(test_epoch(3), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2
                + 3"
            )
        );

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // Drop executor corresponding to node failure
        drop(tx_l);
        drop(tx_r);
        drop(dynamic_filter);

        // Recover executor from state store
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(PbExprNodeType::GreaterThan, mem_store.clone(), false).await;

        // push recovery barrier
        tx_l.push_barrier(test_epoch(3), false);
        tx_r.push_barrier(test_epoch(3), false);

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 4
                - 3"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(4), false);
        tx_r.push_barrier(test_epoch(4), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(5), false);
        tx_r.push_barrier(test_epoch(5), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 2
                - 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_filter_greater_than() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 1
             + 2
             + 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 4
             - 3",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             + 2",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 1",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 4",
        );
        let mem_store = MemoryStateStore::new();
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(PbExprNodeType::GreaterThan, mem_store, false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 3"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 4
                - 3"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(3), false);
        tx_r.push_barrier(test_epoch(3), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(4), false);
        tx_r.push_barrier(test_epoch(4), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 2
                - 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_filter_greater_than_or_equal() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 1
             + 2
             + 3",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 4
             - 3",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             + 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 2",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 5",
        );
        let mem_store = MemoryStateStore::new();
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(PbExprNodeType::GreaterThanOrEqual, mem_store, false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 3"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 4
                - 3"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(3), false);
        tx_r.push_barrier(test_epoch(3), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(4), false);
        tx_r.push_barrier(test_epoch(4), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 2
                - 4"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_filter_less_than() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 2
             + 3
             + 4",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 1
             - 2",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             + 3",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 4",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 1",
        );
        let mem_store = MemoryStateStore::new();
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(PbExprNodeType::LessThan, mem_store, false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 1
                - 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(3), false);
        tx_r.push_barrier(test_epoch(3), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 3"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(4), false);
        tx_r.push_barrier(test_epoch(4), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 1
                - 3"
            )
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_dynamic_filter_less_than_or_equal() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 2
             + 3
             + 4",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 1
             - 2",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             + 2",
        );
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 3",
        );
        let chunk_r3 = StreamChunk::from_pretty(
            "  I
             + 0",
        );
        let mem_store = MemoryStateStore::new();
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(PbExprNodeType::LessThanOrEqual, mem_store, false).await;

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st left chunk
        tx_l.push_chunk(chunk_l1);

        // push the 1st right chunk
        tx_r.push_chunk(chunk_r1);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 2"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 2nd left chunk
        tx_l.push_chunk(chunk_l2);
        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 1
                - 2"
            )
        );

        // push the 2nd right chunk
        tx_r.push_chunk(chunk_r2);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(3), false);
        tx_r.push_barrier(test_epoch(3), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 3"
            )
        );

        // Get the barrier
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 3rd right chunk
        tx_r.push_chunk(chunk_r3);

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(4), false);
        tx_r.push_barrier(test_epoch(4), false);

        let chunk = dynamic_filter.next_unwrap_ready_chunk()?.compact();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                - 1
                - 3"
            )
        );

        Ok(())
    }

    async fn in_table(table: &BatchTable<MemoryStateStore>, x: i64) -> bool {
        let row = table
            .get_row(
                &OwnedRow::new(vec![Some(x.into())]),
                HummockReadEpoch::NoWait(u64::MAX),
            )
            .await
            .unwrap();
        row.is_some()
    }

    #[tokio::test]
    async fn test_dynamic_filter_state_cleaning() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I
             + 1
             + 2
             + 3
             + 4
             + 5",
        );
        let chunk_l2 = StreamChunk::from_pretty(
            "  I
             + 1
             - 2
             - 3",
        );
        let chunk_r1 = StreamChunk::from_pretty(
            "  I
             + 2",
        );
        let watermark_r1 = 2;
        let chunk_r2 = StreamChunk::from_pretty(
            "  I
             + 5",
        );
        let watermark_r2 = 5;

        let mem_store = MemoryStateStore::new();
        let (mut tx_l, mut tx_r, mut dynamic_filter) =
            create_executor(PbExprNodeType::LessThanOrEqual, mem_store.clone(), true).await;
        let column_descs = ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64);
        let table = BatchTable::for_test(
            mem_store.clone(),
            TableId::new(0),
            vec![column_descs],
            vec![OrderType::ascending()],
            vec![0],
            vec![0],
        );

        // push the init barrier for left and right
        tx_l.push_barrier(test_epoch(1), false);
        tx_r.push_barrier(test_epoch(1), false);
        dynamic_filter.next_unwrap_ready_barrier()?;

        // push the 1st set of messages
        tx_l.push_chunk(chunk_l1);
        tx_r.push_chunk(chunk_r1);
        tx_r.push_int64_watermark(0, watermark_r1);
        tx_l.push_barrier(test_epoch(2), false);
        tx_r.push_barrier(test_epoch(2), false);

        let chunk = dynamic_filter.expect_chunk().await;
        assert_eq!(
            chunk.compact(),
            StreamChunk::from_pretty(
                " I
                + 1
                + 2"
            )
        );

        dynamic_filter.expect_barrier().await;

        assert!(!in_table(&table, 1).await); // `1` should be cleaned because it's less than watermark
        assert!(in_table(&table, 2).await);
        assert!(in_table(&table, 3).await);
        assert!(in_table(&table, 4).await);

        // push the 2nd set of messages
        tx_l.push_chunk(chunk_l2);
        tx_r.push_chunk(chunk_r2);
        tx_r.push_int64_watermark(0, watermark_r2);
        tx_l.push_barrier(test_epoch(3), false);
        tx_r.push_barrier(test_epoch(3), false);

        let chunk = dynamic_filter.expect_chunk().await;
        assert_eq!(
            chunk.compact(),
            StreamChunk::from_pretty(
                // the two rows are directly sent to the output cuz they satisfy the condition of previously committed rhs
                " I
                + 1
                - 2"
            )
        );
        let chunk = dynamic_filter.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I
                + 4
                + 5"
            )
        );

        dynamic_filter.expect_barrier().await;

        assert!(!in_table(&table, 2).await);
        assert!(!in_table(&table, 3).await);
        assert!(!in_table(&table, 4).await);
        assert!(in_table(&table, 5).await);

        Ok(())
    }
}
