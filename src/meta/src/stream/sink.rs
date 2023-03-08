// Copyright 2023 RisingWave Labs
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

use anyhow::anyhow;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_connector::sink::{SinkConfig, SinkImpl};
use risingwave_pb::catalog::Sink as ProstSinkCatalog;

use crate::{MetaError, MetaResult};

pub async fn validate_sink(
    prost_sink_catalog: &ProstSinkCatalog,
    connector_rpc_endpoint: Option<String>,
) -> MetaResult<()> {
    let sink_catalog = SinkCatalog::from(prost_sink_catalog);
    let sink_config = SinkConfig::from_hashmap(sink_catalog.properties.clone())
        .map_err(|err| MetaError::from(anyhow!(err.to_string())))?;
    SinkImpl::validate(sink_config, sink_catalog, connector_rpc_endpoint)
        .await
        .map_err(|err| MetaError::from(anyhow!(err.to_string())))
}
