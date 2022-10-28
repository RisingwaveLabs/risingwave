use std::sync::Arc;

use risingwave_storage::table::streaming_table::state_table::StateTable;

use super::*;
use crate::executor::SortExecutor;

pub struct SortExecutorBuilder;

impl ExecutorBuilder for SortExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::Sort)?;
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let vnodes = Arc::new(params.vnode_bitmap.expect("vnodes not set for sort"));
        let state_table =
            StateTable::from_table_catalog(node.get_state_table()?, store, Some(vnodes));
        Ok(Box::new(SortExecutor::new(
            params.actor_context,
            input,
            params.pk_indices,
            params.executor_id,
            state_table,
            node.sort_column_index as _,
        )))
    }
}
