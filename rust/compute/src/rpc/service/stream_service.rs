use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::tonic_err;
use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::*;
use risingwave_stream::executor::Barrier;
use risingwave_stream::task::{StreamEnvironment, StreamManager};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct StreamServiceImpl {
    mgr: Arc<StreamManager>,
    env: StreamEnvironment,
}

impl StreamServiceImpl {
    pub fn new(mgr: Arc<StreamManager>, env: StreamEnvironment) -> Self {
        StreamServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl StreamService for StreamServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn update_actors(
        &self,
        request: Request<UpdateActorsRequest>,
    ) -> std::result::Result<Response<UpdateActorsResponse>, Status> {
        let req = request.into_inner();
        let res = self.mgr.update_actors(&req.actors, &req.hanging_channels);
        match res {
            Err(e) => {
                error!("failed to update stream actor {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(UpdateActorsResponse { status: None })),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn build_actors(
        &self,
        request: Request<BuildActorsRequest>,
    ) -> std::result::Result<Response<BuildActorsResponse>, Status> {
        let req = request.into_inner();

        let actor_id = req.actor_id;
        let res = self.mgr.build_actors(actor_id.as_slice(), self.env.clone());
        match res {
            Err(e) => {
                error!("failed to build actors {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(BuildActorsResponse {
                request_id: req.request_id,
                status: None,
            })),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn broadcast_actor_info_table(
        &self,
        request: Request<BroadcastActorInfoTableRequest>,
    ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
        let table = request.into_inner();

        let res = self.mgr.update_actor_info(table);
        match res {
            Err(e) => {
                error!("failed to update actor info table actor {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            })),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn drop_actors(
        &self,
        request: Request<DropActorsRequest>,
    ) -> std::result::Result<Response<DropActorsResponse>, Status> {
        let req = request.into_inner();
        let actors = req.actor_ids;
        self.mgr
            .drop_actor(&actors)
            .map_err(|e| e.to_grpc_status())?;
        self.mgr
            .drop_materialized_view(&TableId::from(&req.table_ref_id), self.env.clone())
            .await
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(DropActorsResponse {
            request_id: req.request_id,
            status: None,
        }))
    }

    async fn inject_barrier(
        &self,
        request: Request<InjectBarrierRequest>,
    ) -> Result<Response<InjectBarrierResponse>, Status> {
        let req = request.into_inner();
        let barrier =
            Barrier::from_protobuf(req.get_barrier().map_err(tonic_err)?).map_err(tonic_err)?;

        self.mgr
            .send_and_collect_barrier(&barrier, req.actor_ids_to_send, req.actor_ids_to_collect)
            .await
            .map_err(|e| e.to_grpc_status())?;

        Ok(Response::new(InjectBarrierResponse {
            request_id: req.request_id,
            status: None,
        }))
    }

    async fn create_source(
        &self,
        request: Request<CreateSourceRequest>,
    ) -> Result<Response<CreateSourceResponse>, Status> {
        use risingwave_pb::catalog::source::Info;

        let source = request.into_inner().source.unwrap();
        let id = TableId::new(source.id); // TODO: use SourceId instead

        match source.info.unwrap() {
            Info::StreamSource(_) => todo!("ddl v2: create stream source"),

            Info::TableSource(info) => {
                let columns = info
                    .columns
                    .into_iter()
                    .map(|c| c.column_desc.unwrap().into())
                    .collect_vec();

                self.env
                    .source_manager()
                    .create_table_source_v2(&id, columns)
                    .map_err(tonic_err)?;

                info!("Create table source id: {}", id);
            }
        };

        Ok(Response::new(CreateSourceResponse { status: None }))
    }
}
