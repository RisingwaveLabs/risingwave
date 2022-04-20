// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::util::epoch::Epoch;
use risingwave_pb::meta::epoch_service_server::EpochService;
use risingwave_pb::meta::{GetEpochRequest, GetEpochResponse};
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
pub struct EpochServiceImpl;

// TODO: remove EpochService when Java frontend deprecated.
#[async_trait::async_trait]
impl EpochService for EpochServiceImpl {
    #[cfg_attr(coverage, no_coverage)]
    async fn get_epoch(
        &self,
        request: Request<GetEpochRequest>,
    ) -> Result<Response<GetEpochResponse>, Status> {
        let _req = request.into_inner();
        Ok(Response::new(GetEpochResponse {
            status: None,
            epoch: Epoch::now().0,
        }))
    }
}
