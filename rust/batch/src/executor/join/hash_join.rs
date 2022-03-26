use std::convert::TryInto;
use std::mem::take;

use either::Either;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::expr::BoxedExpression;
use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_pb::plan::plan_node::NodeBody;

use crate::executor::join::hash_join_state::{BuildTable, ProbeTable};
use crate::executor::join::JoinType;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::task::TaskId;

/// Parameters of equi-join.
///
/// We use following sql as an example in comments:
/// ```sql
/// select a.a1, a.a2, b.b1, b.b2 from a inner join b where a.a3 = b.b3 and a.a1 = b.b1
/// ```
#[derive(Default)]
pub(super) struct EquiJoinParams {
    join_type: JoinType,
    /// Column indexes of left keys in equi join, e.g., the column indexes of `b1` and `b3` in `b`.
    left_key_columns: Vec<usize>,
    /// Data types of left keys in equi join, e.g., the column types of `b1` and `b3` in `b`.
    left_key_types: Vec<DataType>,
    /// Column indexes of right keys in equi join, e.g., the column indexes of `a1` and `a3` in
    /// `a`.
    right_key_columns: Vec<usize>,
    /// Data types of right keys in equi join, e.g., the column types of `a1` and `a3` in `a`.
    right_key_types: Vec<DataType>,
    /// Column indexes of outputs in equi join, e.g. the column indexes of `a1`, `a2`, `b1`, `b2`.
    /// [`Either::Left`] is used to mark left side input, and [`Either::Right`] is used to mark
    /// right side input.
    output_columns: Vec<Either<usize, usize>>,
    /// Column types of outputs in equi join, e.g. the column types of `a1`, `a2`, `b1`, `b2`.
    output_data_types: Vec<DataType>,
    /// Data chunk buffer size
    batch_size: usize,
    /// Non-equi condition
    pub cond: Option<BoxedExpression>,
}

/// Different states when executing a hash join.
enum HashJoinState<K> {
    /// Invalid state
    Invalid,
    /// Initial state of hash join.
    ///
    /// In this state, the executor [`Executor::init`] build side input, and calls
    /// [`Executor::next`] of build side input till [`None`] is returned to create
    /// `BuildTable`.
    Build(BuildTable),
    /// First state after finishing build state.
    ///
    /// It's different from [`Probe`] in that we need to [`Executor::init`] probe side input.
    FirstProbe(ProbeTable<K>),
    /// State for executing join.
    ///
    /// In this state, the executor calls [`Executor::init`]  method of probe side input, and
    /// executes joining with the chunk against build table to create output.
    Probe(ProbeTable<K>),
    /// State for executing join remaining.
    ///
    /// See [`JoinType::need_join_remaining`]
    ProbeRemaining(ProbeTable<K>),
    /// Final state of hash join.
    Done,
}

impl<K> Default for HashJoinState<K> {
    fn default() -> Self {
        HashJoinState::Invalid
    }
}

pub(super) struct HashJoinExecutor<K> {
    /// Probe side
    left_child: BoxedExecutor,
    /// Build side
    right_child: BoxedExecutor,
    state: HashJoinState<K>,
    schema: Schema,
    identity: String,
}

impl EquiJoinParams {
    #[inline(always)]
    pub(super) fn probe_key_columns(&self) -> &[usize] {
        &self.left_key_columns
    }

    #[inline(always)]
    pub(super) fn join_type(&self) -> JoinType {
        self.join_type
    }

    #[inline(always)]
    pub(super) fn output_types(&self) -> &[DataType] {
        &self.output_data_types
    }

    #[inline(always)]
    pub(super) fn output_columns(&self) -> &[Either<usize, usize>] {
        &self.output_columns
    }

    #[inline(always)]
    pub(super) fn build_key_columns(&self) -> &[usize] {
        &self.right_key_columns
    }

    #[inline(always)]
    pub(super) fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[inline(always)]
    pub(super) fn has_non_equi_cond(&self) -> bool {
        self.cond.is_some()
    }
}

#[async_trait::async_trait]
impl<K: HashKey + Send + Sync> Executor for HashJoinExecutor<K> {
    async fn open(&mut self) -> Result<()> {
        match take(&mut self.state) {
            HashJoinState::Build(build_table) => self.build(build_table).await?,
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        loop {
            match take(&mut self.state) {
                HashJoinState::FirstProbe(probe_table) => {
                    let ret = self.probe(true, probe_table).await?;
                    if let Some(data_chunk) = ret {
                        return Ok(Some(data_chunk));
                    }
                }
                HashJoinState::Probe(probe_table) => {
                    let ret = self.probe(false, probe_table).await?;
                    if let Some(data_chunk) = ret {
                        return Ok(Some(data_chunk));
                    }
                }
                HashJoinState::ProbeRemaining(probe_table) => {
                    let ret = self.probe_remaining(probe_table).await?;
                    if let Some(data_chunk) = ret {
                        return Ok(Some(data_chunk));
                    }
                }
                HashJoinState::Done => return Ok(None),
                _ => unreachable!(),
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.left_child.close().await?;
        self.right_child.close().await?;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl<K: HashKey> HashJoinExecutor<K> {
    async fn build(&mut self, mut build_table: BuildTable) -> Result<()> {
        self.right_child.open().await?;
        while let Some(chunk) = self.right_child.next().await? {
            build_table.append_build_chunk(chunk)?;
        }

        let probe_table = build_table.try_into()?;

        self.state = HashJoinState::FirstProbe(probe_table);
        Ok(())
    }

    async fn probe(
        &mut self,
        first_probe: bool,
        mut probe_table: ProbeTable<K>,
    ) -> Result<Option<DataChunk>> {
        if first_probe {
            self.left_child.open().await?;

            match self.left_child.next().await? {
                Some(data_chunk) => {
                    probe_table.set_probe_data(data_chunk)?;
                }
                None => {
                    self.state = HashJoinState::Done;
                    return Ok(None);
                }
            }
        }

        loop {
            if let Some(mut ret_data_chunk) = probe_table.join()? {
                if probe_table.has_non_equi_cond() {
                    probe_table.process_non_equi_condition(&mut ret_data_chunk)?
                }

                self.state = HashJoinState::Probe(probe_table);

                return Ok(Some(ret_data_chunk));
            } else {
                match self.left_child.next().await? {
                    Some(data_chunk) => {
                        probe_table.set_probe_data(data_chunk)?;
                    }
                    None => {
                        let mut ret_data_chunk = probe_table.consume_left()?;
                        if probe_table.has_non_equi_cond() {
                            probe_table.process_non_equi_condition(&mut ret_data_chunk)?
                        }

                        if probe_table.join_type().need_join_remaining() {
                            self.state = HashJoinState::ProbeRemaining(probe_table);
                        } else {
                            self.state = HashJoinState::Done;
                        }
                        return Ok(Some(ret_data_chunk));
                    }
                }
            }
        }
    }

    async fn probe_remaining(
        &mut self,
        mut probe_table: ProbeTable<K>,
    ) -> Result<Option<DataChunk>> {
        if let Some(ret_data_chunk) = probe_table.join_remaining()? {
            self.state = HashJoinState::ProbeRemaining(probe_table);
            Ok(Some(ret_data_chunk))
        } else {
            self.state = HashJoinState::Done;
            probe_table.consume_left().map(|chunk| Some(chunk))
        }
    }
}

impl<K> HashJoinExecutor<K> {
    fn new(
        left_child: BoxedExecutor,
        right_child: BoxedExecutor,
        params: EquiJoinParams,
        schema: Schema,
        identity: String,
    ) -> Self {
        HashJoinExecutor {
            left_child,
            right_child,
            state: HashJoinState::Build(BuildTable::with_params(params)),
            schema,
            identity,
        }
    }
}

pub struct HashJoinExecutorBuilder {
    params: EquiJoinParams,
    left_child: BoxedExecutor,
    right_child: BoxedExecutor,
    schema: Schema,
    task_id: TaskId,
}

struct HashJoinExecutorBuilderDispatcher;

/// A dispatcher to help create specialized hash join executor.
impl HashKeyDispatcher for HashJoinExecutorBuilderDispatcher {
    type Input = HashJoinExecutorBuilder;
    type Output = BoxedExecutor;

    fn dispatch<K: HashKey>(input: HashJoinExecutorBuilder) -> Self::Output {
        Box::new(HashJoinExecutor::<K>::new(
            input.left_child,
            input.right_child,
            input.params,
            input.schema,
            format!("HashJoinExecutor{:?}", input.task_id),
        ))
    }
}

/// Hash join executor builder.
impl BoxedExecutorBuilder for HashJoinExecutorBuilder {
    fn new_boxed_executor(context: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(context.plan_node().get_children().len() == 2);

        let left_child = context
            .clone_for_plan(&context.plan_node.get_children()[0])
            .build()?;
        let right_child = context
            .clone_for_plan(&context.plan_node.get_children()[1])
            .build()?;

        let hash_join_node = try_match_expand!(
            context.plan_node().get_node_body().unwrap(),
            NodeBody::HashJoin
        )?;

        let mut params = EquiJoinParams {
            batch_size: DEFAULT_CHUNK_BUFFER_SIZE,
            ..Default::default()
        };
        for left_key in hash_join_node.get_left_key() {
            let left_key = *left_key as usize;
            params.left_key_columns.push(left_key);
            params
                .left_key_types
                .push(left_child.schema()[left_key].data_type());
        }

        for right_key in hash_join_node.get_right_key() {
            let right_key = *right_key as usize;
            params.right_key_columns.push(right_key);
            params
                .right_key_types
                .push(right_child.schema()[right_key].data_type());
        }

        ensure!(params.left_key_columns.len() == params.right_key_columns.len());

        for left_output in hash_join_node.get_left_output() {
            let left_output = *left_output as usize;
            params.output_columns.push(Either::Left(left_output));
            params
                .output_data_types
                .push(left_child.schema()[left_output].data_type());
        }

        for right_output in hash_join_node.get_right_output() {
            let right_output = *right_output as usize;
            params.output_columns.push(Either::Right(right_output));
            params
                .output_data_types
                .push(right_child.schema()[right_output].data_type());
        }

        params.join_type = JoinType::from_prost(hash_join_node.get_join_type()?);

        let hash_key_kind = calc_hash_key_kind(&params.right_key_types);

        let fields = params
            .output_columns
            .iter()
            .map(|c| match c {
                Either::Left(idx) => left_child.schema().fields[*idx].clone(),
                Either::Right(idx) => right_child.schema().fields[*idx].clone(),
            })
            .collect::<Vec<Field>>();
        let builder = HashJoinExecutorBuilder {
            params,
            left_child,
            right_child,
            schema: Schema { fields },
            task_id: context.task_id.clone(),
        };

        Ok(HashJoinExecutorBuilderDispatcher::dispatch_by_kind(
            hash_key_kind,
            builder,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use either::Either;
    use itertools::Itertools;
    use risingwave_common::array;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayBuilderImpl, DataChunk, F32Array, F64Array, I32Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::error::Result;
    use risingwave_common::hash::Key32;
    use risingwave_common::types::DataType;

    use crate::executor::join::hash_join::{EquiJoinParams, HashJoinExecutor};
    use crate::executor::join::JoinType;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::BoxedExecutor;

    struct DataChunkMerger {
        data_types: Vec<DataType>,
        array_builders: Vec<ArrayBuilderImpl>,
    }

    impl DataChunkMerger {
        fn new(data_types: Vec<DataType>) -> Result<Self> {
            let array_builders = data_types
                .iter()
                .map(|data_type| data_type.create_array_builder(1024))
                .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

            Ok(Self {
                data_types,
                array_builders,
            })
        }

        fn append(&mut self, data_chunk: &DataChunk) -> Result<()> {
            ensure!(self.array_builders.len() == data_chunk.dimension());
            for idx in 0..self.array_builders.len() {
                self.array_builders[idx].append_array(data_chunk.column_at(idx).array_ref())?;
            }

            Ok(())
        }

        fn finish(self) -> Result<DataChunk> {
            let columns = self
                .array_builders
                .into_iter()
                .map(|array_builder| array_builder.finish().map(|arr| Column::new(Arc::new(arr))))
                .collect::<Result<Vec<Column>>>()?;

            DataChunk::try_from(columns)
        }
    }

    fn is_data_chunk_eq(left: &DataChunk, right: &DataChunk) -> bool {
        assert!(left.visibility().is_none());
        assert!(right.visibility().is_none());

        if left.cardinality() != right.cardinality() {
            return false;
        }

        left.rows()
            .zip_eq(right.rows())
            .all(|(row1, row2)| row1 == row2)
    }

    struct TestFixture {
        left_types: Vec<DataType>,
        right_types: Vec<DataType>,
        join_type: JoinType,
    }

    /// Sql for creating test data:
    /// ```sql
    /// drop table t1 if exists;
    /// create table t1(v1 int, v2 float);
    /// insert into t1 values
    /// (1, 6.1::FLOAT), (2, null), (null, 8.4::FLOAT), (3, 3.9::FLOAT), (null, null),
    /// (4, 6.6::FLOAT), (3, null), (null, 0.7::FLOAT), (5, null), (null, 5.5::FLOAT);
    ///
    /// drop table t2 if exists;
    /// create table t2(v1 int, v2 real);
    /// insert into t2 values
    /// (8, 6.1::REAL), (2, null), (null, 8.9::REAL), (3, null), (null, 3.5::REAL),
    /// (6, null), (4, 7.5::REAL), (6, null), (null, 8::REAL), (7, null),
    /// (null, 9.1::REAL), (9, null), (3, 5.7::REAL), (9, null), (null, 9.6::REAL),
    /// (100, null), (null, 8.18::REAL), (200, null);
    /// ```
    impl TestFixture {
        fn with_join_type(join_type: JoinType) -> Self {
            Self {
                left_types: vec![DataType::Int32, DataType::Float32],
                right_types: vec![DataType::Int32, DataType::Float64],
                join_type,
            }
        }
        fn create_left_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Float32),
                ],
            };
            let mut executor = MockExecutor::new(schema);

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(1), Some(2), None, Some(3), None]}.into(),
                ));
                let column2 = Column::new(Arc::new(
                    array! {F32Array, [Some(6.1f32), None, Some(8.4f32), Some(3.9f32), None]}
                        .into(),
                ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(4), Some(3), None, Some(5), None]}.into(),
                ));
                let column2 = Column::new(Arc::new(
                    array! {F32Array, [Some(6.6f32), None, Some(0.7f32), None, Some(5.5f32)]}
                        .into(),
                ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            Box::new(executor)
        }

        fn create_right_executor(&self) -> BoxedExecutor {
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Float64),
                ],
            };
            let mut executor = MockExecutor::new(schema);

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(8), Some(2), None, Some(3), None, Some(6)]}.into(),
                ));

                let column2 = Column::new(Arc::new(
                    array! {F64Array, [Some(6.1f64), None, Some(8.9f64), None, Some(3.5f64), None]}
                        .into(),
                ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(4), Some(6), None, Some(7), None, Some(9)]}.into(),
                ));

                let column2 = Column::new(Arc::new(
                    array! {F64Array, [Some(7.5f64), None, Some(8f64), None, Some(9.1f64), None]}
                        .into(),
                ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            {
                let column1 = Column::new(Arc::new(
                    array! {I32Array, [Some(3), Some(9), None, Some(100), None, Some(200)]}.into(),
                ));

                let column2 = Column::new(Arc::new(
          array! {F64Array, [Some(5.7f64), None, Some(9.6f64), None, Some(8.18f64), None]}.into(),
        ));

                let chunk =
                    DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");
                executor.add(chunk);
            }

            Box::new(executor)
        }

        fn output_columns(&self) -> Vec<Either<usize, usize>> {
            match self.join_type {
                JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter => {
                    vec![Either::Left(1), Either::Right(1)]
                }
                JoinType::LeftAnti | JoinType::LeftSemi => vec![Either::Left(1)],
                JoinType::RightAnti | JoinType::RightSemi => vec![Either::Right(1)],
            }
        }

        fn output_data_types(&self) -> Vec<DataType> {
            let output_columns = self.output_columns();

            output_columns
                .iter()
                .map(|column| match column {
                    Either::Left(idx) => self.left_types[*idx].clone(),
                    Either::Right(idx) => self.right_types[*idx].clone(),
                })
                .collect::<Vec<DataType>>()
        }

        fn create_join_executor(&self) -> BoxedExecutor {
            let join_type = self.join_type;

            let left_child = self.create_left_executor();
            let right_child = self.create_right_executor();

            let output_columns = self.output_columns();

            let output_data_types = self.output_data_types();

            let params = EquiJoinParams {
                join_type,
                left_key_columns: vec![0],
                left_key_types: vec![self.left_types[0].clone()],
                right_key_columns: vec![0],
                right_key_types: vec![self.right_types[0].clone()],
                output_columns,
                output_data_types,
                batch_size: 2,
                cond: None,
            };

            let fields = params
                .output_columns
                .iter()
                .map(|c| match c {
                    Either::Left(idx) => left_child.schema().fields[*idx].clone(),
                    Either::Right(idx) => right_child.schema().fields[*idx].clone(),
                })
                .collect::<Vec<Field>>();

            let schema = Schema { fields };

            Box::new(HashJoinExecutor::<Key32>::new(
                left_child,
                right_child,
                params,
                schema,
                "HashJoinExecutor".to_string(),
            )) as BoxedExecutor
        }

        async fn do_test(&self, expected: DataChunk) {
            let mut join_executor = self.create_join_executor();
            join_executor
                .open()
                .await
                .expect("Failed to init join executor.");

            let mut data_chunk_merger = DataChunkMerger::new(self.output_data_types()).unwrap();

            let fields = &join_executor.schema().fields;
            match self.join_type {
                JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::RightOuter
                | JoinType::FullOuter => {
                    assert_eq!(fields[0].data_type, DataType::Float32);
                    assert_eq!(fields[1].data_type, DataType::Float64);
                }
                JoinType::LeftAnti | JoinType::LeftSemi => {
                    assert_eq!(fields[0].data_type, DataType::Float32)
                }
                JoinType::RightAnti | JoinType::RightSemi => {
                    assert_eq!(fields[0].data_type, DataType::Float64)
                }
            };

            while let Some(data_chunk) = join_executor.next().await.unwrap() {
                data_chunk_merger.append(&data_chunk).unwrap();
            }

            let result_chunk = data_chunk_merger.finish().unwrap();
            // TODO: Replace this with unsorted comparison
            // assert_eq!(expected, result_chunk);
            println!("Expected data chunk: {:?}", expected);
            println!("Result data chunk: {:?}", result_chunk);
            assert!(is_data_chunk_eq(&expected, &result_chunk));
        }
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_inner_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::Inner);

        let column1 = Column::new(Arc::new(
            array! {F32Array, [None, Some(3.9f32), Some(3.9f32), Some(6.6f32), None, None]}.into(),
        ));

        let column2 = Column::new(Arc::new(
            array! {F64Array, [None, Some(5.7f64), None,  Some(7.5f64), Some(5.7f64),  None]}
                .into(),
        ));

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 left outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_left_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftOuter);

        let column1 = Column::new(Arc::new(
            array! {F32Array, [Some(6.1f32), None, Some(8.4f32), Some(3.9f32), Some(3.9f32), None,
            Some(6.6f32), None, None, Some(0.7f32), None, Some(5.5f32)]}
            .into(),
        ));

        let column2 = Column::new(Arc::new(
      array! {F64Array, [None, None, None, Some(5.7f64), None, None, Some(7.5f64), Some(5.7f64),
      None, None, None, None]}
      .into(),
    ));

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    /// Sql:
    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 right outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_right_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightOuter);

        let column1 = Column::new(Arc::new(
            array! {F32Array, [
              None, Some(3.9f32), Some(3.9f32), Some(6.6), None,
              None, None, None, None, None,
              None, None, None, None, None,
              None, None, None, None, None
            ]}
            .into(),
        ));

        let column2 = Column::new(Arc::new(
            array! {F64Array, [
            None, Some(5.7f64), None, Some(7.5f64), Some(5.7f64),
            None, Some(6.1f64), Some(8.9f64), Some(3.5f64), None,
            None, Some(8.0f64), None, Some(9.1f64), None,
            None, Some(9.6f64),None, Some(8.18f64), None]}
            .into(),
        ));

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    /// ```sql
    /// select t1.v2 as t1_v2, t2.v2 as t2_v2 from t1 full outer join t2 on t1.v1 = t2.v1;
    /// ```
    #[tokio::test]
    async fn test_full_outer_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::FullOuter);

        let column1 = Column::new(Arc::new(
            array! {F32Array, [
              Some(6.1f32), None, Some(8.4f32), Some(3.9f32), Some(3.9f32),
              None, Some(6.6f32), None, None, Some(0.7f32),
              None, Some(5.5f32), None, None, None,
              None, None, None, None, None,
              None, None, None, None, None,
              None
            ]}
            .into(),
        ));

        let column2 = Column::new(Arc::new(
            array! {F64Array, [
              None, None, None, Some(5.7f64), None,
              None, Some(7.5f64), Some(5.7f64), None, None,
              None, None, Some(6.1f64), Some(8.9f64), Some(3.5f64),
              None, None, Some(8.0f64), None, Some(9.1f64),
              None, None, Some(9.6f64), None, Some(8.18f64),
              None
            ]}
            .into(),
        ));

        let expected_chunk =
            DataChunk::try_from(vec![column1, column2]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_left_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftAnti);

        let column1 = Column::new(Arc::new(
            array! {F32Array, [
              Some(6.1f32), Some(8.4f32), None, Some(0.7f32), None, Some(5.5f32)
            ]}
            .into(),
        ));

        let expected_chunk = DataChunk::try_from(vec![column1]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_left_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::LeftSemi);

        let column1 = Column::new(Arc::new(
            array! {F32Array, [
              None, Some(3.9f32), Some(6.6f32), None
            ]}
            .into(),
        ));

        let expected_chunk = DataChunk::try_from(vec![column1]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_right_anti_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightAnti);

        let column1 = Column::new(Arc::new(
            array! {F64Array, [
              Some(6.1f64), Some(8.9f64), Some(3.5f64), None, None,
              Some(8.0f64), None, Some(9.1f64), None, None,
              Some(9.6f64), None, Some(8.18f64), None
            ]}
            .into(),
        ));

        let expected_chunk = DataChunk::try_from(vec![column1]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }

    #[tokio::test]
    async fn test_right_semi_join() {
        let test_fixture = TestFixture::with_join_type(JoinType::RightSemi);

        let column1 = Column::new(Arc::new(
            array! {F64Array, [
              None, Some(5.7f64), None, Some(7.5f64)
            ]}
            .into(),
        ));

        let expected_chunk = DataChunk::try_from(vec![column1]).expect("Failed to create chunk!");

        test_fixture.do_test(expected_chunk).await;
    }
}
