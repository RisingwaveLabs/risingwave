use std::collections::hash_map::{Entry, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    Array, ArrayBuilderImpl, ArrayRef, DataChunk, Op, Row, RowRef, StreamChunk,
};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::expr::RowExpression;
use risingwave_common::types::{DataType, ToOwnedDatum};
use risingwave_storage::keyspace::Segment;
use risingwave_storage::{Keyspace, StateStore};

use super::barrier_align::{AlignedMessage, BarrierAligner};
use super::managed_state::join::*;
use super::{Executor, Message, PkIndices, PkIndicesRef};

/// The `JoinType` and `SideType` are to mimic a enum, because currently
/// enum is not supported in const generic.
// TODO: Use enum to replace this once [feature(adt_const_params)](https://github.com/rust-lang/rust/issues/44580) get completed.
type JoinTypePrimitive = u8;
#[allow(non_snake_case, non_upper_case_globals)]
pub mod JoinType {
    use super::JoinTypePrimitive;
    pub const Inner: JoinTypePrimitive = 0;
    pub const LeftOuter: JoinTypePrimitive = 1;
    pub const RightOuter: JoinTypePrimitive = 2;
    pub const FullOuter: JoinTypePrimitive = 3;
}

/// Build a array and it's corresponding operations.
struct StreamChunkBuilder {
    /// operations in the data chunk to build
    ops: Vec<Op>,
    /// arrays in the data chunk to build
    column_builders: Vec<ArrayBuilderImpl>,
    /// The start position of the columns of the side
    /// stream coming from. If the coming side is the
    /// left, the `update_start_pos` should be 0.
    /// If the coming side is the right, the `update_start_pos`
    /// is the number of columns of the left side.
    update_start_pos: usize,
    /// The start position of the columns of the opposite side
    /// stream coming from. If the coming side is the
    /// left, the `matched_start_pos` should be the number of columns of the left side.
    /// If the coming side is the right, the `matched_start_pos`
    /// should be 0.
    matched_start_pos: usize,
}

impl StreamChunkBuilder {
    fn new(
        capacity: usize,
        data_types: &[DataType],
        update_start_pos: usize,
        matched_start_pos: usize,
    ) -> Result<Self> {
        let ops = Vec::with_capacity(capacity);
        let column_builders = data_types
            .iter()
            .map(|datatype| datatype.create_array_builder(capacity))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            ops,
            column_builders,
            update_start_pos,
            matched_start_pos,
        })
    }

    /// append a row with coming update value and matched value.
    fn append_row(&mut self, op: Op, row_update: &RowRef<'_>, row_matched: &Row) -> Result<()> {
        self.ops.push(op);
        for i in 0..row_update.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(row_update[i])?;
        }
        for i in 0..row_matched.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&row_matched[i])?;
        }
        Ok(())
    }

    /// append a row with coming update value and fill the other side with null.
    fn append_row_update(&mut self, op: Op, row_update: &RowRef<'_>) -> Result<()> {
        self.ops.push(op);
        for i in 0..row_update.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(row_update[i])?;
        }
        for i in 0..self.column_builders.len() - row_update.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&None)?;
        }
        Ok(())
    }

    /// append a row with matched value and fill the coming side with null.
    fn append_row_matched(&mut self, op: Op, row_matched: &Row) -> Result<()> {
        self.ops.push(op);
        for i in 0..self.column_builders.len() - row_matched.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(None)?;
        }
        for i in 0..row_matched.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&row_matched[i])?;
        }
        Ok(())
    }

    fn finish(self) -> Result<StreamChunk> {
        let new_arrays = self
            .column_builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<Result<Vec<_>>>()?;

        let new_columns = new_arrays
            .into_iter()
            .map(|array_impl| Column::new(Arc::new(array_impl)))
            .collect::<Vec<_>>();

        Ok(StreamChunk::new(self.ops, new_columns, None))
    }
}

type SideTypePrimitive = u8;
#[allow(non_snake_case, non_upper_case_globals)]
mod SideType {
    use super::SideTypePrimitive;
    pub const Left: SideTypePrimitive = 0;
    pub const Right: SideTypePrimitive = 1;
}

const JOIN_LEFT_PATH: &[u8] = b"l";
const JOIN_RIGHT_PATH: &[u8] = b"r";

const fn outer_side_keep(join_type: JoinTypePrimitive, side_type: SideTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || (join_type == JoinType::LeftOuter && side_type == SideType::Left)
        || (join_type == JoinType::RightOuter && side_type == SideType::Right)
}

const fn outer_side_null(join_type: JoinTypePrimitive, side_type: SideTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || (join_type == JoinType::LeftOuter && side_type == SideType::Right)
        || (join_type == JoinType::RightOuter && side_type == SideType::Left)
}

type HashKeyType = Row;
type HashValueType<S> = AllOrNoneState<S>;

pub struct JoinParams {
    /// Indices of the join columns
    key_indices: Vec<usize>,
}

impl JoinParams {
    pub fn new(key_indices: Vec<usize>) -> Self {
        Self { key_indices }
    }
}

struct JoinSide<S: StateStore> {
    /// Store all data from a one side stream
    // TODO: use `EvictableHashMap`.
    ht: HashMap<HashKeyType, HashValueType<S>>,
    /// Indices of the join key columns
    key_indices: Vec<usize>,
    /// The primary key indices of this side, used for state store
    pk_indices: Vec<usize>,
    /// The date type of each columns to join on
    col_types: Vec<DataType>,
    /// The start position for the side in output new columns
    start_pos: usize,
    /// The join side operates on this keyspace.
    keyspace: Keyspace<S>,
}

impl<S: StateStore> std::fmt::Debug for JoinSide<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinSide")
            .field("key_indices", &self.key_indices)
            .field("pk_indices", &self.pk_indices)
            .field("col_types", &self.col_types)
            .field("start_pos", &self.start_pos)
            .finish()
    }
}

impl<S: StateStore> JoinSide<S> {
    #[allow(dead_code)]
    fn is_dirty(&self) -> bool {
        self.ht.values().any(|state| state.is_dirty())
    }

    fn clear_cache(&mut self) {
        // TODO: should clear the hashmap entirely after cache eviction of hash join is fixed,
        // instead of clear cache for all states.
        self.ht.values_mut().for_each(|s| s.clear_cache());
    }
}

/// `HashJoinExecutor` takes two input streams and runs equal hash join on them.
/// The output columns are the concatenation of left and right columns.
pub struct HashJoinExecutor<S: StateStore, const T: JoinTypePrimitive> {
    /// Barrier aligner that combines two input streams and aligns their barriers
    aligner: BarrierAligner,
    /// the data types of the formed new columns
    output_data_types: Vec<DataType>,
    /// The schema of the hash join executor
    schema: Schema,
    /// The primary key indices of the schema
    pk_indices: PkIndices,
    /// The parameters of the left join executor
    side_l: JoinSide<S>,
    /// The parameters of the right join executor
    side_r: JoinSide<S>,
    /// Optional non-equi join conditions
    cond: Option<RowExpression>,
    /// Debug info for the left executor
    debug_l: String,
    /// Debug info for the right executor
    debug_r: String,

    /// Identity string
    identity: String,
}

impl<S: StateStore, const T: JoinTypePrimitive> std::fmt::Debug for HashJoinExecutor<S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashJoinExecutor")
            .field("join_type", &T)
            .field("input_left", &format_args!("{}", &self.debug_l))
            .field("input_right", &format_args!("{}", &self.debug_r))
            .field("side_l", &self.side_l)
            .field("side_r", &self.side_r)
            .field("pk_indices", &self.pk_indices)
            .field("schema", &self.schema)
            .field("output_data_types", &self.output_data_types)
            .finish()
    }
}

#[async_trait]
impl<S: StateStore, const T: JoinTypePrimitive> Executor for HashJoinExecutor<S, T> {
    async fn next(&mut self) -> Result<Message> {
        match self.aligner.next().await {
            AlignedMessage::Left(message) => match message {
                Ok(chunk) => self.consume_chunk_left(chunk).await,
                Err(e) => Err(e),
            },
            AlignedMessage::Right(message) => match message {
                Ok(chunk) => self.consume_chunk_right(chunk).await,
                Err(e) => Err(e),
            },
            AlignedMessage::Barrier(barrier) => {
                self.flush_data(barrier.epoch).await?;
                Ok(Message::Barrier(barrier))
            }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }

    fn clear_cache(&mut self) -> Result<()> {
        self.side_l.clear_cache();
        self.side_r.clear_cache();

        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
impl<S: StateStore, const T: JoinTypePrimitive> HashJoinExecutor<S, T> {
    pub fn new(
        input_l: Box<dyn Executor>,
        input_r: Box<dyn Executor>,
        params_l: JoinParams,
        params_r: JoinParams,
        pk_indices: PkIndices,
        keyspace: Keyspace<S>,
        executor_id: u64,
        cond: Option<RowExpression>,
    ) -> Self {
        let debug_l = format!("{:#?}", &input_l);
        let debug_r = format!("{:#?}", &input_r);

        let new_column_n = input_l.schema().len() + input_r.schema().len();
        let side_l_column_n = input_l.schema().len();

        let schema_fields = [
            input_l.schema().fields.clone(),
            input_r.schema().fields.clone(),
        ]
        .concat();

        assert_eq!(schema_fields.len(), new_column_n);

        let output_data_types = schema_fields.iter().map(|field| field.data_type).collect();
        let col_l_datatypes = input_l
            .schema()
            .fields
            .iter()
            .map(|field| field.data_type)
            .collect();
        let col_r_datatypes = input_r
            .schema()
            .fields
            .iter()
            .map(|field| field.data_type)
            .collect();
        let pk_indices_l = input_l.pk_indices().to_vec();
        let pk_indices_r = input_r.pk_indices().to_vec();

        let ks_l = keyspace.with_segment(Segment::FixedLength(JOIN_LEFT_PATH.to_vec()));
        let ks_r = keyspace.with_segment(Segment::FixedLength(JOIN_RIGHT_PATH.to_vec()));
        Self {
            aligner: BarrierAligner::new(input_l, input_r),
            output_data_types,
            schema: Schema {
                fields: schema_fields,
            },
            side_l: JoinSide {
                ht: HashMap::new(),
                key_indices: params_l.key_indices,
                col_types: col_l_datatypes,
                pk_indices: pk_indices_l,
                start_pos: 0,
                keyspace: ks_l,
            },
            side_r: JoinSide {
                ht: HashMap::new(),
                key_indices: params_r.key_indices,
                col_types: col_r_datatypes,
                pk_indices: pk_indices_r,
                start_pos: side_l_column_n,
                keyspace: ks_r,
            },
            pk_indices,
            cond,
            debug_l,
            debug_r,
            identity: format!("HashJoinExecutor {:X}", executor_id),
        }
    }

    async fn flush_data(&mut self, epoch: u64) -> Result<()> {
        for side in [&mut self.side_l, &mut self.side_r] {
            let mut write_batch = side.keyspace.state_store().start_write_batch();
            for state in side.ht.values_mut() {
                state.flush(&mut write_batch)?;
            }
            write_batch.ingest(epoch).await.unwrap();
        }
        Ok(())
    }

    /// the data the hash table and match the coming
    /// data chunk with the executor state
    fn hash_eq_match<'a>(
        key: &Row,
        ht: &'a mut HashMap<HashKeyType, HashValueType<S>>,
    ) -> Option<&'a mut HashValueType<S>> {
        ht.get_mut(key)
    }

    fn hash_key_from_row_ref(row: &RowRef, key_indices: &[usize]) -> HashKeyType {
        let key = key_indices
            .iter()
            .map(|idx| row[*idx].to_owned_datum())
            .collect_vec();
        Row(key)
    }

    fn row_from_row_ref(row: &RowRef) -> Row {
        let value = (0..row.size())
            .map(|idx| row[idx].to_owned_datum())
            .collect_vec();
        Row(value)
    }

    fn pk_from_row_ref(row: &RowRef, pk_indices: &[usize]) -> Row {
        row.row_by_slice(pk_indices)
    }

    fn row_concat(
        row_update: &RowRef<'_>,
        update_start_pos: usize,
        row_matched: &Row,
        matched_start_pos: usize,
    ) -> Row {
        let mut new_row = vec![None; row_update.size() + row_matched.size()];
        for i in 0..row_update.size() {
            new_row[i + update_start_pos] = row_update[i].to_owned_datum();
        }
        for i in 0..row_matched.size() {
            new_row[i + matched_start_pos] = row_matched[i].clone();
        }
        Row(new_row)
    }

    fn bool_from_array_ref(array_ref: ArrayRef) -> bool {
        let bool_array = array_ref.as_ref().as_bool();
        bool_array.value_at(0).unwrap_or_else(|| {
            panic!(
                "Some thing wrong with the expression result. Bool array: {:?}",
                bool_array
            )
        })
    }

    async fn consume_chunk_left(&mut self, chunk: StreamChunk) -> Result<Message> {
        let result = self.eq_join_oneside::<{ SideType::Left }>(chunk).await?;
        Ok(result)
    }

    async fn consume_chunk_right(&mut self, chunk: StreamChunk) -> Result<Message> {
        let result = self.eq_join_oneside::<{ SideType::Right }>(chunk).await?;
        Ok(result)
    }

    async fn eq_join_oneside<const SIDE: SideTypePrimitive>(
        &mut self,
        chunk: StreamChunk,
    ) -> Result<Message> {
        let chunk = chunk.compact()?;
        let (ops, columns, visibility) = chunk.into_inner();

        let data_chunk = {
            let data_chunk_builder = DataChunk::builder().columns(columns);
            if let Some(visibility) = visibility {
                data_chunk_builder.visibility(visibility).build()
            } else {
                data_chunk_builder.build()
            }
        };

        let (side_update, side_match) = if SIDE == SideType::Left {
            (&mut self.side_l, &mut self.side_r)
        } else {
            (&mut self.side_r, &mut self.side_l)
        };

        // TODO: find a better capacity number, the actual array length
        // is likely to be larger than the current capacity
        let capacity = data_chunk.capacity();

        let mut stream_chunk_builder = StreamChunkBuilder::new(
            capacity,
            &self.output_data_types,
            side_update.start_pos,
            side_match.start_pos,
        )?;

        for (row, op) in data_chunk.rows().zip_eq(ops.iter()) {
            let key = Self::hash_key_from_row_ref(&row, &side_update.key_indices);
            let value = Self::row_from_row_ref(&row);
            let matched_rows = Self::hash_eq_match(&key, &mut side_match.ht);
            if let Some(matched_rows) = matched_rows {
                // if there are non-equi expressions
                if let Some(ref mut cond) = self.cond {
                    match *op {
                        Op::Insert | Op::UpdateInsert => {
                            let entry_value =
                                side_update.ht.entry(key.clone()).or_insert_with(|| {
                                    create_hash_join_state(
                                        key,
                                        &side_update.keyspace.clone(),
                                        side_update.pk_indices.clone(),
                                        side_update.col_types.clone(),
                                    )
                                });
                            let mut degree = 0;
                            for matched_row in matched_rows.values_mut().await {
                                // TODO(yuhao-su): We should find a better way to eval the
                                // expression without concat
                                // two rows.
                                let new_row = Self::row_concat(
                                    &row,
                                    side_update.start_pos,
                                    &matched_row.row,
                                    side_match.start_pos,
                                );
                                let cond_match = Self::bool_from_array_ref(
                                    cond.eval(&new_row, &self.output_data_types)?,
                                );
                                if cond_match {
                                    degree += 1;
                                    if matched_row.is_zero_degree() && outer_side_null(T, SIDE) {
                                        // if the matched_row does not have any current matches
                                        stream_chunk_builder.append_row_matched(
                                            Op::UpdateDelete,
                                            &matched_row.row,
                                        )?;
                                        stream_chunk_builder.append_row(
                                            Op::UpdateInsert,
                                            &row,
                                            &matched_row.row,
                                        )?;
                                    } else {
                                        // concat with the matched_row and append the new row
                                        stream_chunk_builder.append_row(
                                            *op,
                                            &row,
                                            &matched_row.row,
                                        )?;
                                    }
                                    matched_row.inc_degree();
                                } else {
                                    // not matched
                                    if outer_side_keep(T, SIDE) {
                                        stream_chunk_builder.append_row_update(*op, &row)?;
                                    }
                                }
                            }
                            entry_value.insert(JoinRow::new(value, degree));
                        }
                        Op::Delete | Op::UpdateDelete => {
                            if let Some(v) = side_update.ht.get_mut(&key) {
                                // remove the row by it's primary key
                                let pk = Self::pk_from_row_ref(&row, &side_update.pk_indices);
                                v.remove(pk);

                                for matched_row in matched_rows.values_mut().await {
                                    let new_row = Self::row_concat(
                                        &row,
                                        side_update.start_pos,
                                        &matched_row.row,
                                        side_match.start_pos,
                                    );
                                    let expr_match = Self::bool_from_array_ref(
                                        cond.eval(&new_row, &self.output_data_types)?,
                                    );
                                    if expr_match {
                                        if matched_row.is_zero_degree() && outer_side_null(T, SIDE)
                                        {
                                            // if the matched_row does not have any current matches
                                            stream_chunk_builder.append_row(
                                                Op::UpdateDelete,
                                                &row,
                                                &matched_row.row,
                                            )?;
                                            stream_chunk_builder.append_row_matched(
                                                Op::UpdateInsert,
                                                &matched_row.row,
                                            )?;
                                        } else {
                                            // concat with the matched_row and append the new row
                                            stream_chunk_builder.append_row(
                                                *op,
                                                &row,
                                                &matched_row.row,
                                            )?;
                                        }
                                        matched_row.dec_degree();
                                    } else {
                                        // not matched
                                        if outer_side_keep(T, SIDE) {
                                            stream_chunk_builder.append_row_update(*op, &row)?;
                                        }
                                    }
                                }
                            }
                        }
                    };
                } else {
                    let mut null_row_updated = false;
                    match *op {
                        Op::Insert | Op::UpdateInsert => {
                            let entry = side_update.ht.entry(key.clone());
                            // FIXME: when state is empty for the entry, the entry is not deleted.
                            let entry_value = match entry {
                                Entry::Occupied(entry) => entry.into_mut(),
                                // if outer join and not its the first to insert, meaning there must
                                // be corresponding nulls.
                                Entry::Vacant(entry) => {
                                    if outer_side_null(T, SIDE) {
                                        for matched_row in matched_rows.values().await {
                                            stream_chunk_builder.append_row_matched(
                                                Op::UpdateDelete,
                                                &matched_row.row,
                                            )?;
                                            stream_chunk_builder.append_row(
                                                Op::UpdateInsert,
                                                &row,
                                                &matched_row.row,
                                            )?;
                                        }
                                        null_row_updated = true;
                                    };
                                    entry.insert(create_hash_join_state(
                                        key,
                                        &side_update.keyspace.clone(),
                                        side_update.pk_indices.clone(),
                                        side_update.col_types.clone(),
                                    ))
                                }
                            };
                            entry_value
                                .insert(JoinRow::new(value, matched_rows.len().await as u64));
                        }
                        Op::Delete | Op::UpdateDelete => {
                            if let Some(v) = side_update.ht.get_mut(&key) {
                                let pk = Self::pk_from_row_ref(&row, &side_update.pk_indices);
                                v.remove(pk);
                                if outer_side_null(T, SIDE) && v.is_empty().await {
                                    for matched_row in matched_rows.values().await {
                                        stream_chunk_builder.append_row(
                                            Op::UpdateDelete,
                                            &row,
                                            &matched_row.row,
                                        )?;
                                        stream_chunk_builder.append_row_matched(
                                            Op::UpdateInsert,
                                            &matched_row.row,
                                        )?;
                                    }
                                    null_row_updated = true;
                                }
                            }
                        }
                    };
                    if !outer_side_null(T, SIDE) || !null_row_updated {
                        for matched_row in matched_rows.values().await {
                            assert_eq!(matched_row.size(), side_match.col_types.len());
                            stream_chunk_builder.append_row(*op, &row, &matched_row.row)?;
                        }
                    }
                }
            } else {
                // if there are no matched rows, just update the hash table
                //
                // FIXME: matched rows can still be there but just evicted from the memory cache, we
                // should handle this!
                match *op {
                    Op::Insert | Op::UpdateInsert => {
                        side_update
                            .ht
                            .entry(key.clone())
                            .or_insert_with(|| {
                                create_hash_join_state(
                                    key,
                                    &side_update.keyspace.clone(),
                                    side_update.pk_indices.clone(),
                                    side_update.col_types.clone(),
                                )
                            })
                            .insert(JoinRow::new(value, 0));
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if let Some(v) = side_update.ht.get_mut(&key) {
                            let pk = Self::pk_from_row_ref(&row, &side_update.pk_indices);
                            v.remove(pk);
                        }
                    }
                };
                // if it's outer join and the side needs maintained.
                if outer_side_keep(T, SIDE) {
                    stream_chunk_builder.append_row_update(*op, &row)?;
                }
            }
        }

        let new_chunk = stream_chunk_builder.finish()?;

        Ok(Message::Chunk(new_chunk))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::*;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::expr::expr_binary_nonnull::new_binary_expr;
    use risingwave_common::expr::{InputRefExpression, RowExpression};
    use risingwave_pb::expr::expr_node::Type;
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;

    use super::{HashJoinExecutor, JoinParams, JoinType, *};
    use crate::executor::test_utils::MockAsyncSource;
    use crate::executor::{Barrier, Executor, Message};

    fn create_in_memory_keyspace() -> Keyspace<MemoryStateStore> {
        Keyspace::executor_root(MemoryStateStore::new(), 0x2333)
    }

    fn create_cond() -> Option<RowExpression> {
        let left_expr = InputRefExpression::new(DataType::Int64, 1);
        let right_expr = InputRefExpression::new(DataType::Int64, 3);
        let cond = new_binary_expr(
            Type::LessThan,
            DataType::Boolean,
            Box::new(left_expr),
            Box::new(right_expr),
        );
        Some(RowExpression::new(cond))
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join() {
        let chunk_l1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 3] },
                column_nonnull! { I64Array, [4, 5, 6] },
            ],
            None,
        );
        let chunk_l2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [3, 3] },
                column_nonnull! { I64Array, [8, 8] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let chunk_r1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [2, 4, 6] },
                column_nonnull! { I64Array, [7, 8, 9] },
            ],
            None,
        );
        let chunk_r2 = StreamChunk::new(
            vec![Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [3, 6] },
                column_nonnull! { I64Array, [10, 11] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::with_pk_indices(schema.clone(), rx_l, vec![0, 1]);
        let source_r = MockAsyncSource::with_pk_indices(schema.clone(), rx_r, vec![0, 1]);

        let keyspace = create_in_memory_keyspace();

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<_, { JoinType::Inner }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
            vec![],
            keyspace,
            1,
            None,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops().len(), 0);
            assert_eq!(chunk.columns().len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.column(i).array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops().len(), 0);
            assert_eq!(chunk.columns().len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.column(i).array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(2)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(5)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(2)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(7)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(6)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(10)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_barrier() {
        let chunk_l1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 3] },
                column_nonnull! { I64Array, [4, 5, 6] },
            ],
            None,
        );
        let chunk_l2 = StreamChunk::new(
            vec![Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [6, 3] },
                column_nonnull! { I64Array, [8, 8] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let chunk_r1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [2, 4, 6] },
                column_nonnull! { I64Array, [7, 8, 9] },
            ],
            None,
        );
        let chunk_r2 = StreamChunk::new(
            vec![Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [3, 6] },
                column_nonnull! { I64Array, [10, 11] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::with_pk_indices(schema.clone(), rx_l, vec![0, 1]);
        let source_r = MockAsyncSource::with_pk_indices(schema.clone(), rx_r, vec![0, 1]);

        let keyspace = create_in_memory_keyspace();

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<_, { JoinType::Inner }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
            vec![],
            keyspace,
            1,
            None,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops().len(), 0);
            assert_eq!(chunk.columns().len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.column(i).array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push a barrier to left side
        MockAsyncSource::push_barrier(&mut tx_l, 0, false);

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);

        // join the first right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(2)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(5)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(2)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(7)]
            );
        } else {
            unreachable!();
        }

        // push a barrier to right side
        MockAsyncSource::push_barrier(&mut tx_r, 0, false);

        // get the aligned barrier here
        assert!(matches!(
            hash_join.next().await.unwrap(),
            Message::Barrier(Barrier {
                epoch: 0,
                mutation: None,
                ..
            })
        ));

        // join the 2nd left chunk
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(6)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(8)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(6)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(9)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3), Some(6)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(6), Some(8), Some(8)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3), Some(6)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(10), Some(10), Some(11)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_left_join() {
        let chunk_l1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 3] },
                column_nonnull! { I64Array, [4, 5, 6] },
            ],
            None,
        );
        let chunk_l2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [3, 3] },
                column_nonnull! { I64Array, [8, 8] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let chunk_r1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [2, 4, 6] },
                column_nonnull! { I64Array, [7, 8, 9] },
            ],
            None,
        );
        let chunk_r2 = StreamChunk::new(
            vec![Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [3, 6] },
                column_nonnull! { I64Array, [10, 11] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::with_pk_indices(schema.clone(), rx_l, vec![0, 1]);
        let source_r = MockAsyncSource::with_pk_indices(schema.clone(), rx_r, vec![0, 1]);

        let keyspace = create_in_memory_keyspace();

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<_, { JoinType::LeftOuter }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
            vec![],
            keyspace,
            1,
            None,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(3)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(4), Some(5), Some(6)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(8), Some(8)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::UpdateDelete, Op::UpdateInsert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(2), Some(2)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(2)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(7)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::UpdateDelete, Op::UpdateInsert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(6), Some(6)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(3)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(10)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_right_join() {
        let chunk_l1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 3] },
                column_nonnull! { I64Array, [4, 5, 6] },
            ],
            None,
        );
        let chunk_l2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [3, 3] },
                column_nonnull! { I64Array, [8, 8] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let chunk_r1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [2, 4, 6] },
                column_nonnull! { I64Array, [7, 8, 9] },
            ],
            None,
        );
        let chunk_r2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [5, 5] },
                column_nonnull! { I64Array, [10, 10] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::with_pk_indices(schema.clone(), rx_l, vec![0, 1]);
        let source_r = MockAsyncSource::with_pk_indices(schema.clone(), rx_r, vec![0, 1]);

        let keyspace = create_in_memory_keyspace();

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<_, { JoinType::RightOuter }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
            vec![],
            keyspace,
            1,
            None,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops().len(), 0);
            assert_eq!(chunk.columns().len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.column(i).array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops().len(), 0);
            assert_eq!(chunk.columns().len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.column(i).array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(2), None, None]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), None, None]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(2), Some(4), Some(6)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(7), Some(8), Some(9)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(10), Some(10)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join() {
        let chunk_l1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 3] },
                column_nonnull! { I64Array, [4, 5, 6] },
            ],
            None,
        );
        let chunk_l2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [3, 3] },
                column_nonnull! { I64Array, [8, 8] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let chunk_r1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [2, 4, 6] },
                column_nonnull! { I64Array, [7, 8, 9] },
            ],
            None,
        );
        let chunk_r2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [5, 5] },
                column_nonnull! { I64Array, [10, 10] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::with_pk_indices(schema.clone(), rx_l, vec![0, 1]);
        let source_r = MockAsyncSource::with_pk_indices(schema.clone(), rx_r, vec![0, 1]);

        let keyspace = create_in_memory_keyspace();

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<_, { JoinType::FullOuter }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
            vec![],
            keyspace,
            1,
            None,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(3)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(4), Some(5), Some(6)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(8), Some(8)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(
                chunk.ops(),
                vec![Op::UpdateDelete, Op::UpdateInsert, Op::Insert, Op::Insert]
            );
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(2), Some(2), None, None]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5), None, None]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(2), Some(4), Some(6)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(7), Some(8), Some(9)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(10), Some(10)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_full_outer_join_with_nonequi_condition() {
        let chunk_l1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 3] },
                column_nonnull! { I64Array, [4, 5, 6] },
            ],
            None,
        );
        let chunk_l2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [3, 3] },
                column_nonnull! { I64Array, [8, 8] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let chunk_r1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [2, 4, 3] },
                column_nonnull! { I64Array, [6, 8, 4] },
            ],
            None,
        );
        let chunk_r2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [5, 5] },
                column_nonnull! { I64Array, [10, 10] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::with_pk_indices(schema.clone(), rx_l, vec![0, 1]);
        let source_r = MockAsyncSource::with_pk_indices(schema.clone(), rx_r, vec![0, 1]);

        let keyspace = create_in_memory_keyspace();

        let cond = create_cond();

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let mut hash_join = HashJoinExecutor::<_, { JoinType::FullOuter }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
            vec![],
            keyspace,
            1,
            cond,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Insert, Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(1), Some(2), Some(3)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(4), Some(5), Some(6)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, None, None]
            );
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3), Some(3)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(8), Some(8)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(
                chunk.ops(),
                vec![Op::UpdateDelete, Op::UpdateInsert, Op::Insert, Op::Insert]
            );
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(2), Some(2), None, None]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5), None, None]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(2), Some(4), Some(3)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![None, Some(6), Some(8), Some(4)]
            );
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert, Op::Delete]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![None, None]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(5), Some(5)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(10), Some(10)]
            );
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_streaming_hash_inner_join_with_nonequi_condition() {
        let chunk_l1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [1, 2, 3] },
                column_nonnull! { I64Array, [4, 10, 6] },
            ],
            None,
        );
        let chunk_l2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I64Array, [3, 3] },
                column_nonnull! { I64Array, [8, 8] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let chunk_r1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [2, 4, 6] },
                column_nonnull! { I64Array, [7, 8, 9] },
            ],
            None,
        );
        let chunk_r2 = StreamChunk::new(
            vec![Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [3, 6] },
                column_nonnull! { I64Array, [10, 11] },
            ],
            Some((vec![true, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (mut tx_l, rx_l) = unbounded_channel();
        let (mut tx_r, rx_r) = unbounded_channel();

        let source_l = MockAsyncSource::with_pk_indices(schema.clone(), rx_l, vec![0, 1]);
        let source_r = MockAsyncSource::with_pk_indices(schema.clone(), rx_r, vec![0, 1]);

        let keyspace = create_in_memory_keyspace();

        let params_l = JoinParams::new(vec![0]);
        let params_r = JoinParams::new(vec![0]);

        let cond = create_cond();

        let mut hash_join = HashJoinExecutor::<_, { JoinType::Inner }>::new(
            Box::new(source_l),
            Box::new(source_r),
            params_l,
            params_r,
            vec![],
            keyspace,
            1,
            cond,
        );

        // push the 1st left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops().len(), 0);
            assert_eq!(chunk.columns().len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.column(i).array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 2nd left chunk
        MockAsyncSource::push_chunks(&mut tx_l, vec![chunk_l2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops().len(), 0);
            assert_eq!(chunk.columns().len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.column(i).array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 1st right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r1]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![]);
            assert_eq!(chunk.columns().len(), 4);
            for i in 0..4 {
                assert_eq!(
                    chunk.column(i).array_ref().as_int64().iter().collect_vec(),
                    vec![]
                );
            }
        } else {
            unreachable!();
        }

        // push the 2nd right chunk
        MockAsyncSource::push_chunks(&mut tx_r, vec![chunk_r2]);
        if let Message::Chunk(chunk) = hash_join.next().await.unwrap() {
            assert_eq!(chunk.ops(), vec![Op::Insert]);
            assert_eq!(chunk.columns().len(), 4);
            assert_eq!(
                chunk.column(0).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3)]
            );
            assert_eq!(
                chunk.column(1).array_ref().as_int64().iter().collect_vec(),
                vec![Some(6)]
            );
            assert_eq!(
                chunk.column(2).array_ref().as_int64().iter().collect_vec(),
                vec![Some(3)]
            );
            assert_eq!(
                chunk.column(3).array_ref().as_int64().iter().collect_vec(),
                vec![Some(10)]
            );
        } else {
            unreachable!();
        }
    }
}
