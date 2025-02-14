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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_connector::source::SplitMetaData;
use risingwave_meta::barrier::BarrierManagerRef;
use risingwave_meta::controller::fragment::StreamingJobInfo;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta::model;
use risingwave_meta::model::ActorId;
use risingwave_meta::stream::{SourceManagerRunningInfo, ThrottleConfig};
use risingwave_meta_model::{ObjectId, SinkId, SourceId, StreamingParallelism};
use risingwave_pb::meta::cancel_creating_jobs_request::Jobs;
use risingwave_pb::meta::list_actor_splits_response::FragmentType;
use risingwave_pb::meta::list_table_fragments_response::{
    ActorInfo, FragmentInfo, TableFragmentInfo,
};
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
use risingwave_pb::meta::table_fragments::PbState;
use risingwave_pb::meta::*;
use tonic::{Request, Response, Status};

use crate::barrier::{BarrierScheduler, Command};
use crate::manager::MetaSrvEnv;
use crate::stream::GlobalStreamManagerRef;

pub type TonicResponse<T> = Result<Response<T>, Status>;

#[derive(Clone)]
pub struct StreamServiceImpl {
    env: MetaSrvEnv,
    barrier_scheduler: BarrierScheduler,
    barrier_manager: BarrierManagerRef,
    stream_manager: GlobalStreamManagerRef,
    metadata_manager: MetadataManager,
}

impl StreamServiceImpl {
    pub fn new(
        env: MetaSrvEnv,
        barrier_scheduler: BarrierScheduler,
        barrier_manager: BarrierManagerRef,
        stream_manager: GlobalStreamManagerRef,
        metadata_manager: MetadataManager,
    ) -> Self {
        StreamServiceImpl {
            env,
            barrier_scheduler,
            barrier_manager,
            stream_manager,
            metadata_manager,
        }
    }
}

#[async_trait::async_trait]
impl StreamManagerService for StreamServiceImpl {
    #[cfg_attr(coverage, coverage(off))]
    async fn flush(&self, request: Request<FlushRequest>) -> TonicResponse<FlushResponse> {
        self.env.idle_manager().record_activity();
        let req = request.into_inner();

        let version_id = self.barrier_scheduler.flush(req.database_id.into()).await?;
        Ok(Response::new(FlushResponse {
            status: None,
            hummock_version_id: version_id.to_u64(),
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn pause(&self, _: Request<PauseRequest>) -> Result<Response<PauseResponse>, Status> {
        for database_id in self.metadata_manager.list_active_database_ids().await? {
            self.barrier_scheduler
                .run_command(database_id, Command::pause())
                .await?;
        }
        Ok(Response::new(PauseResponse {}))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn resume(&self, _: Request<ResumeRequest>) -> Result<Response<ResumeResponse>, Status> {
        for database_id in self.metadata_manager.list_active_database_ids().await? {
            self.barrier_scheduler
                .run_command(database_id, Command::resume())
                .await?;
        }
        Ok(Response::new(ResumeResponse {}))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn apply_throttle(
        &self,
        request: Request<ApplyThrottleRequest>,
    ) -> Result<Response<ApplyThrottleResponse>, Status> {
        let request = request.into_inner();

        let actor_to_apply = match request.kind() {
            ThrottleTarget::Source | ThrottleTarget::TableWithSource => {
                self.metadata_manager
                    .update_source_rate_limit_by_source_id(request.id as SourceId, request.rate)
                    .await?
            }
            ThrottleTarget::Mv => {
                self.metadata_manager
                    .update_backfill_rate_limit_by_table_id(TableId::from(request.id), request.rate)
                    .await?
            }
            ThrottleTarget::CdcTable => {
                self.metadata_manager
                    .update_backfill_rate_limit_by_table_id(TableId::from(request.id), request.rate)
                    .await?
            }
            ThrottleTarget::TableDml => {
                self.metadata_manager
                    .update_dml_rate_limit_by_table_id(TableId::from(request.id), request.rate)
                    .await?
            }
            ThrottleTarget::Sink => {
                self.metadata_manager
                    .update_sink_rate_limit_by_sink_id(request.id as SinkId, request.rate)
                    .await?
            }
            ThrottleTarget::Unspecified => {
                return Err(Status::invalid_argument("unspecified throttle target"))
            }
        };

        let database_id = self
            .metadata_manager
            .catalog_controller
            .get_object_database_id(request.id as ObjectId)
            .await?;
        let database_id = DatabaseId::new(database_id as _);
        // TODO: check whether shared source is correct
        let mutation: ThrottleConfig = actor_to_apply
            .iter()
            .map(|(fragment_id, actors)| {
                (
                    *fragment_id,
                    actors
                        .iter()
                        .map(|actor_id| (*actor_id, request.rate))
                        .collect::<HashMap<ActorId, Option<u32>>>(),
                )
            })
            .collect();
        let _i = self
            .barrier_scheduler
            .run_command(database_id, Command::Throttle(mutation))
            .await?;

        Ok(Response::new(ApplyThrottleResponse { status: None }))
    }

    async fn cancel_creating_jobs(
        &self,
        request: Request<CancelCreatingJobsRequest>,
    ) -> TonicResponse<CancelCreatingJobsResponse> {
        let req = request.into_inner();
        let table_ids = match req.jobs.unwrap() {
            Jobs::Infos(infos) => self
                .metadata_manager
                .catalog_controller
                .find_creating_streaming_job_ids(infos.infos)
                .await?
                .into_iter()
                .map(|id| id as _)
                .collect(),
            Jobs::Ids(jobs) => jobs.job_ids,
        };

        let canceled_jobs = self
            .stream_manager
            .cancel_streaming_jobs(table_ids.into_iter().map(TableId::from).collect_vec())
            .await
            .into_iter()
            .map(|id| id.table_id)
            .collect_vec();
        Ok(Response::new(CancelCreatingJobsResponse {
            status: None,
            canceled_jobs,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_table_fragments(
        &self,
        request: Request<ListTableFragmentsRequest>,
    ) -> Result<Response<ListTableFragmentsResponse>, Status> {
        let req = request.into_inner();
        let table_ids = HashSet::<u32>::from_iter(req.table_ids);

        let mut info = HashMap::new();
        for job_id in table_ids {
            let table_fragments = self
                .metadata_manager
                .catalog_controller
                .get_job_fragments_by_id(job_id as _)
                .await?;
            info.insert(
                table_fragments.stream_job_id().table_id,
                TableFragmentInfo {
                    fragments: table_fragments
                        .fragments
                        .into_iter()
                        .map(|(id, fragment)| FragmentInfo {
                            id,
                            actors: fragment
                                .actors
                                .into_iter()
                                .map(|actor| ActorInfo {
                                    id: actor.actor_id,
                                    node: fragment.nodes.clone(),
                                    dispatcher: actor.dispatcher,
                                })
                                .collect_vec(),
                        })
                        .collect_vec(),
                    ctx: Some(table_fragments.ctx.to_protobuf()),
                },
            );
        }

        Ok(Response::new(ListTableFragmentsResponse {
            table_fragments: info,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_streaming_job_states(
        &self,
        _request: Request<ListStreamingJobStatesRequest>,
    ) -> Result<Response<ListStreamingJobStatesResponse>, Status> {
        let job_infos = self
            .metadata_manager
            .catalog_controller
            .list_streaming_job_infos()
            .await?;
        let states = job_infos
            .into_iter()
            .map(
                |StreamingJobInfo {
                     job_id,
                     job_status,
                     name,
                     parallelism,
                     max_parallelism,
                     resource_group,
                     ..
                 }| {
                    let parallelism = match parallelism {
                        StreamingParallelism::Adaptive => model::TableParallelism::Adaptive,
                        StreamingParallelism::Custom => model::TableParallelism::Custom,
                        StreamingParallelism::Fixed(n) => model::TableParallelism::Fixed(n as _),
                    };

                    list_streaming_job_states_response::StreamingJobState {
                        table_id: job_id as _,
                        name,
                        state: PbState::from(job_status) as _,
                        parallelism: Some(parallelism.into()),
                        max_parallelism: max_parallelism as _,
                        resource_group,
                    }
                },
            )
            .collect_vec();

        Ok(Response::new(ListStreamingJobStatesResponse { states }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_fragment_distribution(
        &self,
        _request: Request<ListFragmentDistributionRequest>,
    ) -> Result<Response<ListFragmentDistributionResponse>, Status> {
        let fragment_descs = self
            .metadata_manager
            .catalog_controller
            .list_fragment_descs()
            .await?;
        let distributions = fragment_descs
            .into_iter()
            .map(
                |fragment_desc| list_fragment_distribution_response::FragmentDistribution {
                    fragment_id: fragment_desc.fragment_id as _,
                    table_id: fragment_desc.job_id as _,
                    distribution_type: PbFragmentDistributionType::from(
                        fragment_desc.distribution_type,
                    ) as _,
                    state_table_ids: fragment_desc.state_table_ids.into_u32_array(),
                    upstream_fragment_ids: fragment_desc.upstream_fragment_id.into_u32_array(),
                    fragment_type_mask: fragment_desc.fragment_type_mask as _,
                    parallelism: fragment_desc.parallelism as _,
                    vnode_count: fragment_desc.vnode_count as _,
                    node: Some(fragment_desc.stream_node.to_protobuf()),
                },
            )
            .collect_vec();

        Ok(Response::new(ListFragmentDistributionResponse {
            distributions,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_actor_states(
        &self,
        _request: Request<ListActorStatesRequest>,
    ) -> Result<Response<ListActorStatesResponse>, Status> {
        let actor_locations = self
            .metadata_manager
            .catalog_controller
            .list_actor_locations()
            .await?;
        let states = actor_locations
            .into_iter()
            .map(|actor_location| list_actor_states_response::ActorState {
                actor_id: actor_location.actor_id as _,
                fragment_id: actor_location.fragment_id as _,
                state: PbActorState::from(actor_location.status) as _,
                worker_id: actor_location.worker_id as _,
            })
            .collect_vec();

        Ok(Response::new(ListActorStatesResponse { states }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn list_object_dependencies(
        &self,
        _request: Request<ListObjectDependenciesRequest>,
    ) -> Result<Response<ListObjectDependenciesResponse>, Status> {
        let dependencies = self
            .metadata_manager
            .catalog_controller
            .list_created_object_dependencies()
            .await?;

        Ok(Response::new(ListObjectDependenciesResponse {
            dependencies,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn recover(
        &self,
        _request: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        self.barrier_manager.adhoc_recovery().await?;
        Ok(Response::new(RecoverResponse {}))
    }

    async fn list_actor_splits(
        &self,
        _request: Request<ListActorSplitsRequest>,
    ) -> Result<Response<ListActorSplitsResponse>, Status> {
        let SourceManagerRunningInfo {
            source_fragments,
            backfill_fragments,
            mut actor_splits,
        } = self.stream_manager.source_manager.get_running_info().await;

        let source_actors = self
            .metadata_manager
            .catalog_controller
            .list_source_actors()
            .await?;

        let is_shared_source = self
            .metadata_manager
            .catalog_controller
            .list_source_id_with_shared_types()
            .await?;

        let fragment_to_source: HashMap<_, _> = source_fragments
            .into_iter()
            .flat_map(|(source_id, fragment_ids)| {
                let source_type = if is_shared_source
                    .get(&(source_id as _))
                    .copied()
                    .unwrap_or(false)
                {
                    FragmentType::SharedSource
                } else {
                    FragmentType::NonSharedSource
                };

                fragment_ids
                    .into_iter()
                    .map(move |fragment_id| (fragment_id, (source_id, source_type)))
            })
            .chain(
                backfill_fragments
                    .into_iter()
                    .flat_map(|(source_id, fragment_ids)| {
                        fragment_ids.into_iter().flat_map(
                            move |(fragment_id, upstream_fragment_id)| {
                                [
                                    (fragment_id, (source_id, FragmentType::SharedSourceBackfill)),
                                    (
                                        upstream_fragment_id,
                                        (source_id, FragmentType::SharedSource),
                                    ),
                                ]
                            },
                        )
                    }),
            )
            .collect();

        let actor_splits = source_actors
            .into_iter()
            .flat_map(|(actor_id, fragment_id)| {
                let (source_id, fragment_type) = fragment_to_source
                    .get(&(fragment_id as _))
                    .copied()
                    .unwrap_or_default();

                actor_splits
                    .remove(&(actor_id as _))
                    .unwrap_or_default()
                    .into_iter()
                    .map(move |split| list_actor_splits_response::ActorSplit {
                        actor_id: actor_id as _,
                        source_id: source_id as _,
                        fragment_id: fragment_id as _,
                        split_id: split.id().to_string(),
                        fragment_type: fragment_type.into(),
                    })
            })
            .collect_vec();

        Ok(Response::new(ListActorSplitsResponse { actor_splits }))
    }

    async fn list_rate_limits(
        &self,
        _request: Request<ListRateLimitsRequest>,
    ) -> Result<Response<ListRateLimitsResponse>, Status> {
        let rate_limits = self
            .metadata_manager
            .catalog_controller
            .list_rate_limits()
            .await?;
        Ok(Response::new(ListRateLimitsResponse { rate_limits }))
    }
}
