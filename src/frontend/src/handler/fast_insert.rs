// Copyright 2024 RisingWave Labs
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

use std::collections::HashSet;
use std::sync::Arc;

use pgwire::pg_response::StatementType;
use risingwave_common::error::BoxedError;
use risingwave_pb::batch_plan::{FastInsertNode, PlanFragment};
use risingwave_sqlparser::ast::Statement;
use tokio::sync::mpsc;

use crate::catalog::TableId;
use crate::error::Result;
use crate::handler::query::{
    gen_batch_plan_by_statement, gen_batch_plan_fragmenter, BatchPlanFragmenterResult,
};
use crate::handler::HandlerArgs;
use crate::optimizer::OptimizerContext;
use crate::scheduler::plan_fragmenter::Query;
use crate::scheduler::FastInsertExecution;
use crate::session::SessionImpl;

// pub async fn handle_fast_insert(
//     handler_args: HandlerArgs,
//     node: FastInsertNode,
// ) -> Result<FastInsertExecution> {
//     let session = handler_args.session.clone();

//     // Acquire the write guard for DML statements.
//     // session.txn_write_guard()?;

//     gen_fast_insert_plan_inner(session, query, &read_storage_tables)
// }

// fn gen_fast_insert_plan_inner(
//     session: Arc<SessionImpl>,
//     query: Query,
//     read_storage_tables: &HashSet<TableId>,
// ) -> Result<FastInsertExecution> {
//     let front_env = session.env();
//     let snapshot = session.pinned_snapshot();

//     // TODO: Passing sql here
//     let execution = FastInsertExecution::new(
//         query,
//         front_env.clone(),
//         snapshot.support_barrier_read(),
//         snapshot.batch_query_epoch(read_storage_tables)?,
//         session,
//     );

//     // Ok(execution.gen_plan()?)
//     Ok(execution)
// }

// pub async fn run_fast_insert(plan: PlanFragment) -> Result<()> {
//     let compute_runtime = self.front_env.compute_runtime();
//     let (sender, mut receiver) = mpsc::channel(10);
//     // let shutdown_rx = self.shutdown_rx().clone();
//     let sender1 = sender.clone();
//     let exec = async move {
//         let mut data_stream = self.run().map(|r| r.map_err(|e| Box::new(e) as BoxedError));
//         while let Some(r) = data_stream.next().await {
//             // append a query cancelled error if the query is cancelled.
//             // if r.is_err() && shutdown_rx.is_cancelled() {
//             //     r = Err(Box::new(SchedulerError::QueryCancelled(
//             //         "Cancelled by user".to_string(),
//             //     )) as BoxedError);
//             // }
//             if sender1.send(r).await.is_err() {
//                 tracing::info!("Receiver closed.");
//                 return;
//             }
//         }
//     };

//     compute_runtime.spawn(exec);

//     while let Some(result) = receiver.recv().await {
//         result?;
//     }
//     Ok(())
// }
