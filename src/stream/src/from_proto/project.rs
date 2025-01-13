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

use multimap::MultiMap;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::build_non_strict_from_prost;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::stream_plan::ProjectNode;

use super::*;
use crate::executor::ProjectExecutor;

pub struct ProjectExecutorBuilder;

impl ExecutorBuilder for ProjectExecutorBuilder {
    type Node = ProjectNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let project_exprs: Vec<_> = node
            .get_select_list()
            .iter()
            .map(|e| build_non_strict_from_prost(e, params.eval_error_report.clone()))
            .try_collect()?;

        let watermark_derivations = MultiMap::from_iter(
            node.get_watermark_input_cols()
                .iter()
                .map(|idx| *idx as usize)
                .zip_eq_fast(
                    node.get_watermark_output_cols()
                        .iter()
                        .map(|idx| *idx as usize),
                ),
        );
        let nondecreasing_expr_indices = node
            .get_nondecreasing_exprs()
            .iter()
            .map(|idx| *idx as usize)
            .collect();
        let extremely_light = node.get_select_list().iter().all(|expr| {
            matches!(
                expr.get_rex_node().unwrap(),
                RexNode::InputRef(_) | RexNode::Constant(_)
            )
        });
        let materialize_selectivity_threshold = if extremely_light { 0.0 } else { 0.5 };
        let exec = ProjectExecutor::new(
            params.actor_context,
            input,
            project_exprs,
            watermark_derivations,
            nondecreasing_expr_indices,
            materialize_selectivity_threshold,
            node.noop_update_hint,
        );
        Ok((params.info, exec).into())
    }
}
