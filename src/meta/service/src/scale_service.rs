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

use risingwave_common::catalog::TableId;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta::model::TableParallelism;
use risingwave_meta::stream::{
    JobReschedulePlan, JobReschedulePostUpdates, RescheduleOptions, ScaleControllerRef,
    WorkerReschedule,
};
use risingwave_meta_model::FragmentId;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::scale_service_server::ScaleService;
use risingwave_pb::meta::{
    GetClusterInfoRequest, GetClusterInfoResponse, GetServerlessStreamingJobsStatusRequest,
    GetServerlessStreamingJobsStatusResponse, PbWorkerReschedule, RescheduleRequest,
    RescheduleResponse, UpdateStreamingJobNodeLabelsRequest, UpdateStreamingJobNodeLabelsResponse,
};
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::stream::{GlobalStreamManagerRef, SourceManagerRef};

pub struct ScaleServiceImpl {
    metadata_manager: MetadataManager,
    source_manager: SourceManagerRef,
    stream_manager: GlobalStreamManagerRef,
    barrier_manager: BarrierManagerRef,
}

impl ScaleServiceImpl {
    pub fn new(
        metadata_manager: MetadataManager,
        source_manager: SourceManagerRef,
        stream_manager: GlobalStreamManagerRef,
        barrier_manager: BarrierManagerRef,
        _scale_controller: ScaleControllerRef,
    ) -> Self {
        Self {
            metadata_manager,
            source_manager,
            stream_manager,
            barrier_manager,
        }
    }
}

#[async_trait::async_trait]
impl ScaleService for ScaleServiceImpl {
    #[cfg_attr(coverage, coverage(off))]
    async fn get_cluster_info(
        &self,
        _: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        let _reschedule_job_lock = self.stream_manager.reschedule_lock_read_guard().await;

        let table_fragments = self
            .metadata_manager
            .catalog_controller
            .table_fragments()
            .await?
            .values()
            .cloned()
            .collect();

        let worker_nodes = self
            .metadata_manager
            .list_worker_node(Some(WorkerType::ComputeNode), None)
            .await?;

        let actor_splits = self
            .source_manager
            .list_assignments()
            .await
            .into_iter()
            .map(|(actor_id, splits)| {
                (
                    actor_id,
                    ConnectorSplits {
                        splits: splits.iter().map(ConnectorSplit::from).collect(),
                    },
                )
            })
            .collect();

        let sources = self.metadata_manager.list_sources().await?;
        let source_infos = sources.into_iter().map(|s| (s.id, s)).collect();

        Ok(Response::new(GetClusterInfoResponse {
            worker_nodes,
            table_fragments,
            actor_splits,
            source_infos,
            revision: 0,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn reschedule(
        &self,
        request: Request<RescheduleRequest>,
    ) -> Result<Response<RescheduleResponse>, Status> {
        self.barrier_manager.check_status_running()?;

        let RescheduleRequest {
            worker_reschedules,
            resolve_no_shuffle_upstream,
            ..
        } = request.into_inner();

        let _reschedule_job_lock = self.stream_manager.reschedule_lock_write_guard().await;
        for (database_id, worker_reschedules) in self
            .metadata_manager
            .split_fragment_map_by_database(worker_reschedules)
            .await?
        {
            let streaming_job_ids = self
                .metadata_manager
                .catalog_controller
                .get_fragment_job_id(
                    worker_reschedules
                        .keys()
                        .map(|id| *id as FragmentId)
                        .collect(),
                )
                .await?;

            let table_parallelisms = streaming_job_ids
                .into_iter()
                .map(|id| (TableId::new(id as _), TableParallelism::Custom))
                .collect();

            self.stream_manager
                .reschedule_actors(
                    database_id,
                    JobReschedulePlan {
                        reschedules: worker_reschedules
                            .into_iter()
                            .map(|(fragment_id, reschedule)| {
                                let PbWorkerReschedule { worker_actor_diff } = reschedule;
                                (
                                    fragment_id,
                                    WorkerReschedule {
                                        worker_actor_diff: worker_actor_diff
                                            .into_iter()
                                            .map(|(worker_id, diff)| (worker_id as _, diff as _))
                                            .collect(),
                                    },
                                )
                            })
                            .collect(),
                        post_updates: JobReschedulePostUpdates {
                            parallelism_updates: table_parallelisms,
                            resource_group_updates: Default::default(),
                        },
                    },
                    RescheduleOptions {
                        resolve_no_shuffle_upstream,
                        skip_create_new_actors: false,
                    },
                )
                .await?;
        }

        Ok(Response::new(RescheduleResponse {
            success: true,
            revision: 0,
        }))
    }

    async fn update_streaming_job_node_labels(
        &self,
        _request: Request<UpdateStreamingJobNodeLabelsRequest>,
    ) -> Result<Response<UpdateStreamingJobNodeLabelsResponse>, Status> {
        todo!()
    }

    async fn get_serverless_streaming_jobs_status(
        &self,
        _request: Request<GetServerlessStreamingJobsStatusRequest>,
    ) -> Result<Response<GetServerlessStreamingJobsStatusResponse>, Status> {
        todo!()
    }
}
