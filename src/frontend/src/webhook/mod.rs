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

use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use axum::body::Bytes;
use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
use axum::Router;
use risingwave_common::array::{Array, ArrayBuilder, DataChunk};
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::{DataType, JsonbVal, Scalar};
use risingwave_pb::catalog::WebhookSourceInfo;
use risingwave_pb::task_service::{FastInsertRequest, FastInsertResponse};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

use crate::webhook::utils::{err, Result};
mod utils;
use risingwave_rpc_client::ComputeClient;

pub type Service = Arc<WebhookService>;

// We always use the `root` user to connect to the database to allow the webhook service to access all tables.
const USER: &str = "root";

#[derive(Clone)]
pub struct FastInsertContext {
    pub webhook_source_info: WebhookSourceInfo,
    pub fast_insert_request: FastInsertRequest,
    pub compute_client: ComputeClient,
}

pub struct WebhookService {
    webhook_addr: SocketAddr,
    counter: AtomicU32,
}

pub(super) mod handlers {
    use jsonbb::Value;
    use risingwave_common::array::JsonbArrayBuilder;
    use risingwave_common::session_config::SearchPath;
    use risingwave_pb::catalog::WebhookSourceInfo;
    use risingwave_pb::task_service::fast_insert_response;
    use utils::{header_map_to_json, verify_signature};

    use super::*;
    use crate::catalog::root_catalog::SchemaPath;
    use crate::scheduler::choose_fast_insert_client;
    use crate::session::SESSION_MANAGER;

    pub async fn handle_post_request(
        Extension(srv): Extension<Service>,
        headers: HeaderMap,
        Path((database, schema, table)): Path<(String, String, String)>,
        body: Bytes,
    ) -> Result<()> {
        let request_id = srv
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let FastInsertContext {
            webhook_source_info,
            mut fast_insert_request,
            compute_client,
        } = acquire_table_info(request_id, &database, &schema, &table).await?;

        let WebhookSourceInfo {
            signature_expr,
            secret_ref,
            wait_for_persistence: _,
        } = webhook_source_info;

        let secret_string = LocalSecretManager::global()
            .fill_secret(secret_ref.unwrap())
            .map_err(|e| err(e, StatusCode::NOT_FOUND))?;

        // Once limitation here is that the key is no longer case-insensitive, users must user the lowercase key when defining the webhook source table.
        let headers_jsonb = header_map_to_json(&headers);

        // verify the signature
        let is_valid = verify_signature(
            headers_jsonb,
            secret_string.as_str(),
            body.as_ref(),
            signature_expr.unwrap(),
        )
        .await?;

        if !is_valid {
            return Err(err(
                anyhow!("Signature verification failed"),
                StatusCode::UNAUTHORIZED,
            ));
        }

        // Use builder to obtain a single column & single row DataChunk
        let mut builder = JsonbArrayBuilder::with_type(1, DataType::Jsonb);
        let json_value = Value::from_text(&body).map_err(|e| {
            err(
                anyhow!(e).context("Failed to parse body"),
                StatusCode::UNPROCESSABLE_ENTITY,
            )
        })?;
        let jsonb_val = JsonbVal::from(json_value);
        builder.append(Some(jsonb_val.as_scalar_ref()));
        let data_chunk = DataChunk::new(vec![builder.finish().into_ref()], 1);

        // fill the data_chunk
        fast_insert_request.data_chunk = Some(data_chunk.to_protobuf());
        // execute on the compute node
        let res = execute(fast_insert_request, compute_client).await?;

        if res.status == fast_insert_response::Status::Succeeded as i32 {
            Ok(())
        } else {
            Err(err(
                anyhow!("Failed to fast insert: {}", res.error_message),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    }

    async fn acquire_table_info(
        request_id: u32,
        database: &String,
        schema: &String,
        table: &String,
    ) -> Result<FastInsertContext> {
        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");

        let frontend_env = session_mgr.env();

        let search_path = SearchPath::default();
        let schema_path = SchemaPath::new(Some(schema.as_str()), &search_path, USER);

        let (webhook_source_info, table_id, version_id) = {
            let reader = frontend_env.catalog_reader().read_guard();
            let (table_catalog, _schema) = reader
                .get_any_table_by_name(database.as_str(), schema_path, table)
                .map_err(|e| err(e, StatusCode::NOT_FOUND))?;

            let webhook_source_info = table_catalog
                .webhook_info
                .as_ref()
                .ok_or_else(|| {
                    err(
                        anyhow!("Table `{}` is not with webhook source", table),
                        StatusCode::FORBIDDEN,
                    )
                })?
                .clone();
            (
                webhook_source_info,
                table_catalog.id(),
                table_catalog.version_id().expect("table must be versioned"),
            )
        };

        let fast_insert_request = FastInsertRequest {
            table_id: table_id.table_id,
            table_version_id: version_id,
            column_indices: vec![0],
            // leave the data_chunk empty for now
            data_chunk: None,
            row_id_index: Some(1),
            request_id,
            wait_for_persistence: webhook_source_info.wait_for_persistence,
        };

        let compute_client = choose_fast_insert_client(&table_id, frontend_env, request_id)
            .await
            .unwrap();

        Ok(FastInsertContext {
            webhook_source_info,
            fast_insert_request,
            compute_client,
        })
    }

    async fn execute(
        request: FastInsertRequest,
        client: ComputeClient,
    ) -> Result<FastInsertResponse> {
        let response = client.fast_insert(request).await.map_err(|e| {
            err(
                anyhow!(e).context("Failed to execute on compute node"),
                StatusCode::INTERNAL_SERVER_ERROR,
            )
        })?;
        Ok(response)
    }
}

impl WebhookService {
    pub fn new(webhook_addr: SocketAddr) -> Self {
        Self {
            webhook_addr,
            counter: AtomicU32::new(0),
        }
    }

    pub async fn serve(self) -> anyhow::Result<()> {
        use handlers::*;
        let srv = Arc::new(self);

        let cors_layer = CorsLayer::new()
            .allow_origin(cors::Any)
            .allow_methods(vec![Method::POST]);

        let api_router: Router = Router::new()
            .route("/:database/:schema/:table", post(handle_post_request))
            .layer(
                ServiceBuilder::new()
                    .layer(AddExtensionLayer::new(srv.clone()))
                    .into_inner(),
            )
            .layer(cors_layer);

        let app: Router = Router::new()
            .nest("/webhook", api_router)
            .layer(CompressionLayer::new());

        let listener = TcpListener::bind(&srv.webhook_addr)
            .await
            .context("Failed to bind dashboard address")?;

        #[cfg(not(madsim))]
        axum::serve(listener, app)
            .await
            .context("Failed to serve dashboard service")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    #[tokio::test]
    #[ignore]
    async fn test_webhook_server() -> anyhow::Result<()> {
        let addr = SocketAddr::from(([127, 0, 0, 1], 4560));
        let service = crate::webhook::WebhookService::new(addr);
        service.serve().await?;
        Ok(())
    }
}
