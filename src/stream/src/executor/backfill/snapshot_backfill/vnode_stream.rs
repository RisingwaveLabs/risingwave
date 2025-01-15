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

use std::collections::HashMap;
use std::mem::{replace, take};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures::stream::{FuturesUnordered, Peekable, StreamFuture};
use futures::{Stream, StreamExt, TryStreamExt};
use pin_project::pin_project;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_storage::table::ChangeLogRow;

use crate::executor::StreamExecutorResult;

pub(super) trait ChangeLogRowStream =
    Stream<Item = StreamExecutorResult<ChangeLogRow>> + Sized + 'static;

#[pin_project]
struct StreamWithVnode<St: ChangeLogRowStream> {
    #[pin]
    stream: Peekable<St>,
    vnode: VirtualNode,
    row_count: usize,
}

impl<St: ChangeLogRowStream> Stream for StreamWithVnode<St> {
    type Item = St::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let poll_result = this.stream.poll_next(cx);
        if let Poll::Ready(Some(Ok(change_log_row))) = &poll_result {
            match change_log_row {
                ChangeLogRow::Insert(_) | ChangeLogRow::Delete(_) => {
                    *this.row_count += 1;
                }
                ChangeLogRow::Update { .. } => {
                    *this.row_count += 2;
                }
            }
        }
        poll_result
    }
}

type ChangeLogRowVnodeStream<St> = Pin<Box<StreamWithVnode<St>>>;

pub(super) struct VnodeStream<St: ChangeLogRowStream> {
    streams: FuturesUnordered<StreamFuture<ChangeLogRowVnodeStream<St>>>,
    pk_indices: Vec<usize>,
    finished_vnode: HashMap<VirtualNode, usize>,
    data_chunk_builder: DataChunkBuilder,
    ops: Vec<Op>,
}

impl<St: ChangeLogRowStream> VnodeStream<St> {
    pub(super) fn new(
        vnode_streams: impl IntoIterator<Item = (VirtualNode, St, usize)>,
        pk_indices: Vec<usize>,
        data_chunk_builder: DataChunkBuilder,
    ) -> Self {
        assert!(data_chunk_builder.is_empty());
        assert!(data_chunk_builder.batch_size() > 2);
        let streams = FuturesUnordered::from_iter(vnode_streams.into_iter().map(
            |(vnode, stream, row_count)| {
                let stream = stream.peekable();
                Box::pin(StreamWithVnode {
                    stream,
                    vnode,
                    row_count,
                })
                .into_future()
            },
        ));
        let ops = Vec::with_capacity(data_chunk_builder.batch_size());
        Self {
            streams,
            pk_indices,
            finished_vnode: HashMap::new(),
            data_chunk_builder,
            ops,
        }
    }

    pub(super) fn take_finished_vnodes(&mut self) -> HashMap<VirtualNode, usize> {
        assert!(self.streams.is_empty());
        assert!(self.data_chunk_builder.is_empty());
        take(&mut self.finished_vnode)
    }
}

impl<St: ChangeLogRowStream> VnodeStream<St> {
    fn poll_next_row(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<StreamExecutorResult<Option<ChangeLogRow>>> {
        loop {
            let ready_item = match ready!(self.streams.poll_next_unpin(cx)) {
                None => Ok(None),
                Some((None, stream)) => {
                    self.finished_vnode
                        .try_insert(stream.vnode, stream.row_count)
                        .expect("non-duplicate");
                    continue;
                }
                Some((Some(Ok(item)), stream)) => {
                    // TODO: may avoid generating a `StreamFuture` for each row, because
                    // `FuturesUnordered::push` involve memory allocation of `Arc`, and may
                    // incur some unnecessary costs.
                    self.streams.push(stream.into_future());
                    Ok(Some(item))
                }
                Some((Some(Err(e)), _stream)) => Err(e),
            };
            break Poll::Ready(ready_item);
        }
    }

    pub(super) fn consume_builder(&mut self) -> Option<StreamChunk> {
        self.data_chunk_builder.consume_all().map(|chunk| {
            let ops = replace(
                &mut self.ops,
                Vec::with_capacity(self.data_chunk_builder.batch_size()),
            );
            StreamChunk::from_parts(ops, chunk)
        })
    }

    pub(super) async fn for_vnode_pk_progress(
        &mut self,
        mut on_vnode_progress: impl FnMut(VirtualNode, usize, Option<OwnedRow>),
    ) -> StreamExecutorResult<()> {
        assert!(self.data_chunk_builder.is_empty());
        for (vnode, row_count) in &self.finished_vnode {
            on_vnode_progress(*vnode, *row_count, None);
        }
        let streams = take(&mut self.streams);
        // When the `VnodeStream` is polled, the `FuturesUnordered` will generate a special cx to poll the futures.
        // The pending futures will be stored in a separate linked list and will not be polled until the special cx is awakened
        // and move the awakened future from the linked list to a ready queue.
        //
        // However, here if we use `FuturesUnordered::iter_mut` to in place access the pending futures and call `peek` directly,
        // the cx specially generated in `FuturedUnordered::poll_next` will be overwritten, and then even the peeked future is ready,
        // it will not be moved to the ready queue, and the `FuturesUnordered` will be stuck forever.
        //
        // Therefore, to avoid this, we will take all stream futures out and push them back again, so that all futures will be in the ready queue.
        for vnode_stream_future in streams {
            let mut vnode_stream = vnode_stream_future.into_inner().expect("should exist");
            match vnode_stream.as_mut().project().stream.peek().await {
                Some(Ok(change_log_row)) => {
                    let row = match change_log_row {
                        ChangeLogRow::Insert(row) | ChangeLogRow::Delete(row) => row,
                        ChangeLogRow::Update {
                            new_value,
                            old_value,
                        } => {
                            if cfg!(debug_assertions) {
                                assert_eq!(
                                    old_value.project(&self.pk_indices),
                                    new_value.project(&self.pk_indices)
                                );
                            }
                            new_value
                        }
                    };
                    let pk = row.project(&self.pk_indices).to_owned_row();
                    on_vnode_progress(vnode_stream.vnode, vnode_stream.row_count, Some(pk));
                    self.streams.push(vnode_stream.into_future());
                }
                Some(Err(_)) => {
                    return Err(vnode_stream.try_next().await.expect_err("checked Err"));
                }
                None => {
                    self.finished_vnode
                        .try_insert(vnode_stream.vnode, vnode_stream.row_count)
                        .expect("non-duplicate");
                    on_vnode_progress(vnode_stream.vnode, vnode_stream.row_count, None);
                }
            }
        }
        Ok(())
    }
}

impl<St: ChangeLogRowStream> Stream for VnodeStream<St> {
    type Item = StreamExecutorResult<StreamChunk>;

    // Here we implement the stream on our own instead of generating the stream with
    // `try_stream` macro, because we want to access the state of the streams on the flight.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let capacity = this.data_chunk_builder.batch_size();
        loop {
            match ready!(this.poll_next_row(cx)) {
                Ok(Some(change_log_row)) => {
                    let may_chunk = match change_log_row {
                        ChangeLogRow::Insert(row) => {
                            this.ops.push(Op::Insert);
                            this.data_chunk_builder.append_one_row(row)
                        }
                        ChangeLogRow::Delete(row) => {
                            this.ops.push(Op::Delete);
                            this.data_chunk_builder.append_one_row(row)
                        }
                        ChangeLogRow::Update {
                            new_value,
                            old_value,
                        } => {
                            if this.data_chunk_builder.can_append_update() {
                                this.ops.extend([Op::UpdateDelete, Op::UpdateInsert]);
                                assert!(this
                                    .data_chunk_builder
                                    .append_one_row(old_value)
                                    .is_none());
                                this.data_chunk_builder.append_one_row(new_value)
                            } else {
                                let chunk = this
                                    .data_chunk_builder
                                    .consume_all()
                                    .expect("should be Some when not can_append");
                                let ops = replace(&mut this.ops, Vec::with_capacity(capacity));
                                this.ops.extend([Op::UpdateDelete, Op::UpdateInsert]);
                                assert!(this
                                    .data_chunk_builder
                                    .append_one_row(old_value)
                                    .is_none());
                                assert!(this
                                    .data_chunk_builder
                                    .append_one_row(new_value)
                                    .is_none());
                                break Poll::Ready(Some(Ok(StreamChunk::from_parts(ops, chunk))));
                            }
                        }
                    };
                    if let Some(chunk) = may_chunk {
                        let ops = replace(&mut this.ops, Vec::with_capacity(capacity));
                        break Poll::Ready(Some(Ok(StreamChunk::from_parts(ops, chunk))));
                    }
                }
                Ok(None) => {
                    break if let Some(chunk) = this.data_chunk_builder.consume_all() {
                        let ops = take(&mut this.ops);
                        Poll::Ready(Some(Ok(StreamChunk::from_parts(ops, chunk))))
                    } else {
                        Poll::Ready(None)
                    };
                }
                Err(e) => {
                    break Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::poll_fn;
    use std::sync::LazyLock;
    use std::task::Poll;

    use anyhow::anyhow;
    use futures::{pin_mut, Future, FutureExt};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
    use risingwave_storage::table::ChangeLogRow;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tokio_stream::StreamExt;

    use crate::executor::backfill::snapshot_backfill::vnode_stream::VnodeStream;

    static DATA_TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| vec![DataType::Int64]);

    fn test_row(index: i64) -> OwnedRow {
        OwnedRow::new(vec![Some(ScalarImpl::Int64(index))])
    }

    impl<St: super::ChangeLogRowStream> VnodeStream<St> {
        async fn assert_progress(
            &mut self,
            progress: impl IntoIterator<Item = (VirtualNode, usize, Option<OwnedRow>)>,
        ) {
            let expected_progress_map: HashMap<_, _> = progress
                .into_iter()
                .map(|(vnode, row_count, progress)| (vnode, (row_count, progress)))
                .collect();
            let mut progress_map = HashMap::new();
            self.for_vnode_pk_progress(|vnode, row_count, progress| {
                progress_map
                    .try_insert(vnode, (row_count, progress))
                    .unwrap();
            })
            .await
            .unwrap();
            assert_eq!(expected_progress_map, progress_map);
        }
    }

    #[tokio::test]
    async fn test_basic() {
        let [vnode1, vnode2] = [1, 2].map(VirtualNode::from_index);
        let (tx1, rx1) = unbounded_channel();
        let (tx2, rx2) = unbounded_channel();
        let mut stream = VnodeStream::<UnboundedReceiverStream<_>>::new(
            [
                (vnode1, UnboundedReceiverStream::new(rx1), 10),
                (vnode2, UnboundedReceiverStream::new(rx2), 0),
            ]
            .into_iter(),
            vec![0],
            DataChunkBuilder::new(DATA_TYPES.clone(), 3),
        );
        assert!(stream.next().now_or_never().is_none());
        tx1.send(Ok(ChangeLogRow::Insert(test_row(0)))).unwrap();
        assert!(stream.next().now_or_never().is_none());
        tx2.send(Ok(ChangeLogRow::Insert(test_row(0)))).unwrap();
        tx2.send(Ok(ChangeLogRow::Insert(test_row(1)))).unwrap();
        assert_eq!(3, stream.next().await.unwrap().unwrap().cardinality());

        let next_row = test_row(1);
        {
            let future =
                stream.assert_progress([(vnode1, 11, Some(next_row.clone())), (vnode2, 2, None)]);
            pin_mut!(future);
            assert!((&mut future).now_or_never().is_none());
            tx1.send(Ok(ChangeLogRow::Insert(next_row.clone())))
                .unwrap();
            assert!((&mut future).now_or_never().is_none());
            drop(tx2);
            future.await;
        }
        assert!(stream.next().now_or_never().is_none());
        assert_eq!(1, stream.consume_builder().unwrap().cardinality());
        {
            let future = stream.assert_progress([(vnode1, 12, None), (vnode2, 2, None)]);
            pin_mut!(future);
            assert!((&mut future).now_or_never().is_none());
            drop(tx1);
            future.await;
        }
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_update() {
        let (tx, rx) = unbounded_channel();
        let mut stream = VnodeStream::new(
            [(VirtualNode::ZERO, UnboundedReceiverStream::new(rx), 0)].into_iter(),
            vec![0],
            DataChunkBuilder::new(DATA_TYPES.clone(), 3),
        );
        assert!(stream.next().now_or_never().is_none());
        tx.send(Ok(ChangeLogRow::Insert(test_row(0)))).unwrap();
        tx.send(Ok(ChangeLogRow::Insert(test_row(1)))).unwrap();
        assert!(stream.next().now_or_never().is_none());
        tx.send(Ok(ChangeLogRow::Update {
            new_value: test_row(2),
            old_value: test_row(3),
        }))
        .unwrap();
        assert_eq!(2, stream.next().await.unwrap().unwrap().cardinality());
        drop(tx);
        assert_eq!(2, stream.next().await.unwrap().unwrap().cardinality());
        assert!(stream.next().await.is_none());
        stream.assert_progress([(VirtualNode::ZERO, 4, None)]).await;
    }

    #[tokio::test]
    async fn test_empty() {
        let mut stream = VnodeStream::<UnboundedReceiverStream<_>>::new(
            [].into_iter(),
            vec![0],
            DataChunkBuilder::new(DATA_TYPES.clone(), 1024),
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_err() {
        {
            let (tx, rx) = unbounded_channel();
            let mut stream = VnodeStream::new(
                [(VirtualNode::ZERO, UnboundedReceiverStream::new(rx), 0)].into_iter(),
                vec![0],
                DataChunkBuilder::new(DATA_TYPES.clone(), 3),
            );
            assert!(stream.next().now_or_never().is_none());
            tx.send(Err(anyhow!("err").into())).unwrap();
            assert!(stream.next().await.unwrap().is_err());
        }
        {
            let (tx, rx) = unbounded_channel();
            let mut stream = VnodeStream::new(
                [(VirtualNode::ZERO, UnboundedReceiverStream::new(rx), 0)].into_iter(),
                vec![0],
                DataChunkBuilder::new(DATA_TYPES.clone(), 3),
            );
            assert!(stream.next().now_or_never().is_none());
            let future = stream.for_vnode_pk_progress(|_, _, _| unreachable!());
            pin_mut!(future);
            assert!((&mut future).now_or_never().is_none());
            tx.send(Err(anyhow!("err").into())).unwrap();
            assert!(future.await.is_err());
        }
    }

    #[tokio::test]
    async fn test_futures_unordered_peek() {
        let (tx, rx) = unbounded_channel();
        let mut stream = VnodeStream::new(
            [(VirtualNode::ZERO, UnboundedReceiverStream::new(rx), 0)].into_iter(),
            vec![0],
            DataChunkBuilder::new(DATA_TYPES.clone(), 1024),
        );
        // poll the stream for once, and then the stream future inside it will be stored in the pending list.
        assert!(stream.next().now_or_never().is_none());
        let row = test_row(1);
        {
            let fut = stream.assert_progress([(VirtualNode::ZERO, 0, Some(row.clone()))]);
            pin_mut!(fut);
            assert!(poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx)))
                .await
                .is_pending());
            tx.send(Ok(ChangeLogRow::Insert(row.clone()))).unwrap();
            drop(tx);
            fut.await;
        }
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk.capacity(), 1);
    }
}
