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

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use axum::body::Bytes;
use axum::extract::{Extension, Path};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::routing::post;
use axum::Router;
use pgwire::net::Address;
use pgwire::pg_server::SessionManager;
use risingwave_common::array::{Array, ArrayBuilder, DataChunk};
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::{DataType, JsonbVal, Scalar};
use risingwave_pb::batch_plan::FastInsertNode;
use risingwave_pb::catalog::WebhookSourceInfo;
use risingwave_sqlparser::ast::{Expr, ObjectName};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::add_extension::AddExtensionLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{self, CorsLayer};

// use crate::handler::fast_insert::handle_fast_insert;
use crate::handler::{handle, HandlerArgs};
use crate::scheduler::{FastInsertExecution, FrontendBatchTaskContext};
use crate::webhook::utils::{err, Result};
mod utils;

pub type Service = Arc<WebhookService>;

// We always use the `root` user to connect to the database to allow the webhook service to access all tables.
const USER: &str = "root";

#[derive(Clone)]
pub struct TableWebhookContext {
    pub webhook_source_info: WebhookSourceInfo,
    pub fast_insert_node: FastInsertNode,
}

#[derive(Clone)]
pub struct WebhookService {
    webhook_addr: SocketAddr,
}

pub(super) mod handlers {
    use std::net::Ipv4Addr;

    use iceberg::table::Table;
    use jsonbb::Value;
    use pgwire::pg_server::Session;
    use risingwave_common::array::JsonbArrayBuilder;
    use risingwave_common::catalog::TableId;
    use risingwave_pb::batch_plan::FastInsertNode;
    use risingwave_pb::catalog::WebhookSourceInfo;
    use risingwave_pb::plan_common::DefaultColumns;
    // use risingwave_sqlparser::ast::{Query, SetExpr, Statement, Value, Values};
    use utils::{header_map_to_json, verify_signature};

    use super::*;
    use crate::catalog::root_catalog::SchemaPath;
    use crate::session::{SessionImpl, SESSION_MANAGER};

    pub async fn handle_post_request(
        Extension(_srv): Extension<Service>,
        headers: HeaderMap,
        Path((database, schema, table)): Path<(String, String, String)>,
        body: Bytes,
    ) -> Result<()> {
        let session_mgr = SESSION_MANAGER
            .get()
            .expect("session manager has been initialized");

        // Can be any address, we use the port of meta to indicate that it's a internal request.
        let dummy_addr = Address::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 5691));

        // println!(
        //     "WKXLOG Received webhook request for {}/{}/{}",
        //     database, schema, table
        // );
        // TODO(kexiang): optimize this
        // get a session object for the corresponding database
        let session = session_mgr
            .connect(database.as_str(), USER, Arc::new(dummy_addr))
            .map_err(|e| {
                err(
                    anyhow!(e).context(format!(
                        "Failed to create session for database `{}` with user `{}`",
                        database, USER
                    )),
                    StatusCode::UNAUTHORIZED,
                )
            })?;

        // println!("WKXLOG session created");
        let TableWebhookContext {
            webhook_source_info,
            mut fast_insert_node,
        } = acquire_table_info(&session, &database, &schema, &table).await?;

        let WebhookSourceInfo {
            signature_expr,
            secret_ref,
            ..
        } = webhook_source_info;
        // println!("WKXLOG webhook source info: {:?}", secret_ref);

        // let secret_string = LocalSecretManager::global()
        //     .fill_secret(secret_ref.unwrap())
        //     .map_err(|e| err(e, StatusCode::NOT_FOUND))?;

        let secret_string = String::from("TEST_WEBHOOK");

        // println!("WKXLOG secret string: {:?}", secret_string);
        // Once limitation here is that the key is no longer case-insensitive, users must user the lowercase key when defining the webhook source table.
        let headers_jsonb = header_map_to_json(&headers);

        let is_valid = verify_signature(
            headers_jsonb,
            secret_string.as_str(),
            body.as_ref(),
            signature_expr.unwrap(),
        )
        .await?;

        // println!("WKXLOG is_valid: {:?}", is_valid);

        if !is_valid {
            return Err(err(
                anyhow!("Signature verification failed"),
                StatusCode::UNAUTHORIZED,
            ));
        }

        // let payload = String::from_utf8(body.to_vec()).map_err(|e| {
        //     err(
        //         anyhow!(e).context("Failed to parse body"),
        //         StatusCode::UNPROCESSABLE_ENTITY,
        //     )
        // })?;

        let mut builder = JsonbArrayBuilder::with_type(1, DataType::Jsonb);
        // TODO(kexiang): handle errors
        let json_value = Value::from_text(&body.to_vec()).unwrap();
        let jsonb_val = JsonbVal::from(json_value);
        // Add 4 ListValues to ArrayBuilder
        builder.append(Some(jsonb_val.as_scalar_ref()));

        // Use builder to obtain a single (List) column DataChunk
        let data_chunk = DataChunk::new(vec![builder.finish().into_ref()], 1);
        fast_insert_node.data_chunk = Some(data_chunk.to_protobuf());
        // let context = FrontendBatchTaskContext::new(session.clone());
        let execution =
            FastInsertExecution::new(fast_insert_node, session.env().clone(), session.clone());
        let res = execution.my_execute().await.unwrap();

        Ok(())
    }

    async fn acquire_table_info(
        session: &Arc<SessionImpl>,
        database: &String,
        schema: &String,
        table: &String,
    ) -> Result<TableWebhookContext> {
        // println!("WKXLOG session created");

        let search_path = session.config().search_path();
        let schema_path = SchemaPath::new(Some(schema.as_str()), &search_path, USER);

        let reader = session.env().catalog_reader().read_guard();
        let (table_catalog, _schema) = reader
            .get_any_table_by_name(database.as_str(), schema_path, &table)
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

        let fast_insert_node = FastInsertNode {
            table_id: table_catalog.id().table_id,
            table_version_id: table_catalog.version_id().expect("table must be versioned"),
            column_indices: vec![0],
            data_chunk: None,
            default_columns: Some(DefaultColumns {
                default_columns: vec![],
            }),
            row_id_index: Some(1),
            session_id: session.id().0 as u32,
        };

        let table_webhook_context = TableWebhookContext {
            webhook_source_info,
            fast_insert_node,
        };
        Ok(table_webhook_context)
    }
}

impl WebhookService {
    pub fn new(webhook_addr: SocketAddr) -> Self {
        Self { webhook_addr }
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
