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

use std::sync::Arc;

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_protocol::truncated_fmt;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::pg_server::Session;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc};
use risingwave_common::session_config::{SearchPath, USER_NAME_WILD_CARD};
use risingwave_common::types::{DataType, Fields, Timestamptz};
use risingwave_common::util::addr::HostAddr;
use risingwave_connector::source::kafka::PRIVATELINK_CONNECTION;
use risingwave_expr::scalar::like::{i_like_default, like_default};
use risingwave_pb::catalog::connection;
use risingwave_sqlparser::ast::{
    display_comma_separated, Ident, ObjectName, ShowCreateType, ShowObject, ShowStatementFilter,
};

use super::{fields_to_descriptors, RwPgResponse, RwPgResponseBuilderExt};
use crate::binder::{Binder, Relation};
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::{CatalogError, IndexCatalog};
use crate::error::{Result, RwError};
use crate::handler::create_connection::print_connection_params;
use crate::handler::HandlerArgs;
use crate::session::cursor_manager::SubscriptionCursor;
use crate::session::SessionImpl;
use crate::user::has_access_to_object;

pub fn get_columns_from_table(
    session: &SessionImpl,
    table_name: ObjectName,
) -> Result<Vec<ColumnCatalog>> {
    let mut binder = Binder::new_for_system(session);
    let relation = binder.bind_relation_by_name(table_name.clone(), None, None)?;
    let column_catalogs = match relation {
        Relation::Source(s) => s.catalog.columns,
        Relation::BaseTable(t) => t.table_catalog.columns.clone(),
        Relation::SystemTable(t) => t.sys_table_catalog.columns.clone(),
        _ => {
            return Err(CatalogError::NotFound("table or source", table_name.to_string()).into());
        }
    };

    Ok(column_catalogs)
}

pub fn get_columns_from_sink(
    session: &SessionImpl,
    sink_name: ObjectName,
) -> Result<Vec<ColumnCatalog>> {
    let binder = Binder::new_for_system(session);
    let sink = binder.bind_sink_by_name(sink_name.clone())?;
    Ok(sink.sink_catalog.full_columns().to_vec())
}

pub fn get_columns_from_view(
    session: &SessionImpl,
    view_name: ObjectName,
) -> Result<Vec<ColumnCatalog>> {
    let binder = Binder::new_for_system(session);
    let view = binder.bind_view_by_name(view_name.clone())?;

    Ok(view
        .view_catalog
        .columns
        .iter()
        .enumerate()
        .map(|(idx, field)| ColumnCatalog {
            column_desc: ColumnDesc::from_field_with_column_id(field, idx as _),
            is_hidden: false,
        })
        .collect())
}

pub fn get_indexes_from_table(
    session: &SessionImpl,
    table_name: ObjectName,
) -> Result<Vec<Arc<IndexCatalog>>> {
    let mut binder = Binder::new_for_system(session);
    let relation = binder.bind_relation_by_name(table_name.clone(), None, None)?;
    let indexes = match relation {
        Relation::BaseTable(t) => t.table_indexes,
        _ => {
            return Err(CatalogError::NotFound("table or source", table_name.to_string()).into());
        }
    };

    Ok(indexes)
}

fn schema_or_search_path(
    session: &Arc<SessionImpl>,
    schema: &Option<Ident>,
    search_path: &SearchPath,
) -> Vec<String> {
    if let Some(s) = schema {
        vec![s.real_value()]
    } else {
        search_path
            .real_path()
            .iter()
            .map(|s| {
                if s.eq(USER_NAME_WILD_CARD) {
                    session.user_name()
                } else {
                    s.to_string()
                }
            })
            .collect()
    }
}

fn iter_schema_items<F, T>(
    session: &Arc<SessionImpl>,
    schema: &Option<Ident>,
    reader: &CatalogReadGuard,
    mut f: F,
) -> Vec<T>
where
    F: FnMut(&SchemaCatalog) -> Vec<T>,
{
    let search_path = session.config().search_path();

    schema_or_search_path(session, schema, &search_path)
        .into_iter()
        .filter_map(|schema| {
            reader
                .get_schema_by_name(&session.database(), schema.as_ref())
                .ok()
        })
        .flat_map(|s| f(s).into_iter())
        .collect()
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowObjectRow {
    name: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
pub struct ShowColumnRow {
    pub name: String,
    pub r#type: String,
    pub is_hidden: Option<String>,
    pub description: Option<String>,
}

impl ShowColumnRow {
    pub fn from_catalog(col: ColumnCatalog) -> Vec<Self> {
        col.column_desc
            .flatten()
            .into_iter()
            .map(|c| {
                let type_name = if let DataType::Struct { .. } = c.data_type {
                    c.type_name.clone()
                } else {
                    c.data_type.to_string()
                };
                ShowColumnRow {
                    name: c.name,
                    r#type: type_name,
                    is_hidden: Some(col.is_hidden.to_string()),
                    description: c.description,
                }
            })
            .collect()
    }
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowConnectionRow {
    name: String,
    r#type: String,
    properties: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowFunctionRow {
    name: String,
    arguments: String,
    return_type: String,
    language: String,
    link: Option<String>,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowIndexRow {
    name: String,
    on: String,
    key: String,
    include: String,
    distributed_by: String,
}

impl From<Arc<IndexCatalog>> for ShowIndexRow {
    fn from(index: Arc<IndexCatalog>) -> Self {
        let index_display = index.display();
        ShowIndexRow {
            name: index.name.clone(),
            on: index.primary_table.name.clone(),
            key: display_comma_separated(&index_display.index_columns_with_ordering).to_string(),
            include: display_comma_separated(&index_display.include_columns).to_string(),
            distributed_by: display_comma_separated(&index_display.distributed_by_columns)
                .to_string(),
        }
    }
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowClusterRow {
    id: i32,
    addr: String,
    r#type: String,
    state: String,
    parallelism: Option<i32>,
    is_streaming: Option<bool>,
    is_serving: Option<bool>,
    is_unschedulable: Option<bool>,
    started_at: Option<Timestamptz>,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowJobRow {
    id: i64,
    statement: String,
    progress: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowProcessListRow {
    id: String,
    user: String,
    host: String,
    database: String,
    time: Option<String>,
    info: Option<String>,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowCreateObjectRow {
    name: String,
    create_sql: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowSubscriptionRow {
    name: String,
    retention_seconds: i64,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowCursorRow {
    session_id: String,
    user: String,
    host: String,
    database: String,
    cursor_name: String,
}

#[derive(Fields)]
#[fields(style = "Title Case")]
struct ShowSubscriptionCursorRow {
    session_id: String,
    user: String,
    host: String,
    database: String,
    cursor_name: String,
    subscription_name: String,
    state: String,
    idle_duration_ms: i64,
}

/// Infer the row description for different show objects.
pub fn infer_show_object(objects: &ShowObject) -> Vec<PgFieldDescriptor> {
    fields_to_descriptors(match objects {
        ShowObject::Columns { .. } => ShowColumnRow::fields(),
        ShowObject::Connection { .. } => ShowConnectionRow::fields(),
        ShowObject::Function { .. } => ShowFunctionRow::fields(),
        ShowObject::Indexes { .. } => ShowIndexRow::fields(),
        ShowObject::Cluster => ShowClusterRow::fields(),
        ShowObject::Jobs => ShowJobRow::fields(),
        ShowObject::ProcessList => ShowProcessListRow::fields(),
        _ => ShowObjectRow::fields(),
    })
}

pub async fn handle_show_object(
    handler_args: HandlerArgs,
    command: ShowObject,
    filter: Option<ShowStatementFilter>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    if let Some(ShowStatementFilter::Where(..)) = filter {
        bail_not_implemented!("WHERE clause in SHOW statement");
    }

    let catalog_reader = session.env().catalog_reader();

    let names = match command {
        ShowObject::Table { schema } => {
            let reader = catalog_reader.read_guard();
            iter_schema_items(&session, &schema, &reader, |schema| {
                schema.iter_user_table().map(|t| t.name.clone()).collect()
            })
        }
        ShowObject::InternalTable { schema } => {
            let reader = catalog_reader.read_guard();
            iter_schema_items(&session, &schema, &reader, |schema| {
                schema
                    .iter_internal_table()
                    .map(|t| t.name.clone())
                    .collect()
            })
        }
        ShowObject::Database => catalog_reader.read_guard().get_all_database_names(),
        ShowObject::Schema => catalog_reader
            .read_guard()
            .get_all_schema_names(&session.database())?,
        ShowObject::View { schema } => {
            let reader = catalog_reader.read_guard();
            iter_schema_items(&session, &schema, &reader, |schema| {
                schema.iter_view().map(|t| t.name.clone()).collect()
            })
        }
        ShowObject::MaterializedView { schema } => {
            let reader = catalog_reader.read_guard();
            iter_schema_items(&session, &schema, &reader, |schema| {
                schema.iter_created_mvs().map(|t| t.name.clone()).collect()
            })
        }
        ShowObject::Source { schema } => {
            let reader = catalog_reader.read_guard();
            let mut sources = iter_schema_items(&session, &schema, &reader, |schema| {
                schema.iter_source().map(|t| t.name.clone()).collect()
            });
            sources.extend(session.temporary_source_manager().keys());
            sources
        }
        ShowObject::Sink { schema } => {
            let reader = catalog_reader.read_guard();
            iter_schema_items(&session, &schema, &reader, |schema| {
                schema.iter_sink().map(|t| t.name.clone()).collect()
            })
        }
        ShowObject::Subscription { schema } => {
            let reader = catalog_reader.read_guard();
            let rows = iter_schema_items(&session, &schema, &reader, |schema| {
                schema
                    .iter_subscription()
                    .map(|t| ShowSubscriptionRow {
                        name: t.name.clone(),
                        retention_seconds: t.retention_seconds as i64,
                    })
                    .collect()
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Secret { schema } => {
            let reader = catalog_reader.read_guard();
            iter_schema_items(&session, &schema, &reader, |schema| {
                schema.iter_secret().map(|t| t.name.clone()).collect()
            })
        }
        ShowObject::Columns { table } => {
            let Ok(columns) = get_columns_from_table(&session, table.clone())
                .or(get_columns_from_sink(&session, table.clone()))
                .or(get_columns_from_view(&session, table.clone()))
            else {
                return Err(CatalogError::NotFound(
                    "table, source, sink or view",
                    table.to_string(),
                )
                .into());
            };

            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(columns.into_iter().flat_map(ShowColumnRow::from_catalog))
                .into());
        }
        ShowObject::Indexes { table } => {
            let indexes = get_indexes_from_table(&session, table)?;

            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(indexes.into_iter().map(ShowIndexRow::from))
                .into());
        }
        ShowObject::Connection { schema } => {
            let reader = catalog_reader.read_guard();
            let rows = iter_schema_items(&session, &schema, &reader, |schema| {
                schema.iter_connections()
                .map(|c| {
                    let name = c.name.clone();
                    let r#type = match &c.info {
                        connection::Info::PrivateLinkService(_) => {
                            PRIVATELINK_CONNECTION.to_owned()
                        },
                        connection::Info::ConnectionParams(params) => {
                            params.get_connection_type().unwrap().as_str_name().to_owned()
                        }
                    };
                    let source_names = schema
                        .get_source_ids_by_connection(c.id)
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|sid| schema.get_source_by_id(&sid).map(|catalog| catalog.name.as_str()))
                        .collect_vec();
                    let sink_names = schema
                        .get_sink_ids_by_connection(c.id)
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|sid| schema.get_sink_by_id(&sid).map(|catalog| catalog.name.as_str()))
                        .collect_vec();
                    let properties = match &c.info {
                        connection::Info::PrivateLinkService(i) => {
                            format!(
                                "provider: {}\nservice_name: {}\nendpoint_id: {}\navailability_zones: {}\nsources: {}\nsinks: {}",
                                i.get_provider().unwrap().as_str_name(),
                                i.service_name,
                                i.endpoint_id,
                                serde_json::to_string(&i.dns_entries.keys().collect_vec()).unwrap(),
                                serde_json::to_string(&source_names).unwrap(),
                                serde_json::to_string(&sink_names).unwrap(),
                            )
                        }
                        connection::Info::ConnectionParams(params) => {
                            // todo: show dep relations
                            print_connection_params(params, schema)
                        }
                    };
                    ShowConnectionRow {
                        name,
                        r#type,
                        properties,
                    }
                }).collect_vec()
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Function { schema } => {
            let reader = catalog_reader.read_guard();
            let rows = iter_schema_items(&session, &schema, &reader, |schema| {
                schema
                    .iter_function()
                    .map(|t| ShowFunctionRow {
                        name: t.name.clone(),
                        arguments: t.arg_types.iter().map(|t| t.to_string()).join(", "),
                        return_type: t.return_type.to_string(),
                        language: t.language.clone(),
                        link: t.link.clone(),
                    })
                    .collect()
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Cluster => {
            let workers = session.env().meta_client().list_all_nodes().await?;
            let rows = workers.into_iter().sorted_by_key(|w| w.id).map(|worker| {
                let addr: HostAddr = worker.host.as_ref().unwrap().into();
                let property = worker.property.as_ref();
                ShowClusterRow {
                    id: worker.id as _,
                    addr: addr.to_string(),
                    r#type: worker.get_type().unwrap().as_str_name().into(),
                    state: worker.get_state().unwrap().as_str_name().to_owned(),
                    parallelism: worker.parallelism().map(|parallelism| parallelism as i32),
                    is_streaming: property.map(|p| p.is_streaming),
                    is_serving: property.map(|p| p.is_serving),
                    is_unschedulable: property.map(|p| p.is_unschedulable),
                    started_at: worker
                        .started_at
                        .map(|ts| Timestamptz::from_secs(ts as i64).unwrap()),
                }
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Jobs => {
            let resp = session.env().meta_client().get_ddl_progress().await?;
            let rows = resp.into_iter().map(|job| ShowJobRow {
                id: job.id as i64,
                statement: job.statement,
                progress: job.progress,
            });
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::ProcessList => {
            let sessions_map = session.env().sessions_map().read();
            let rows = sessions_map.values().map(|s| {
                ShowProcessListRow {
                    // Since process id and the secret id in the session id are the same in RisingWave, just display the process id.
                    id: format!("{}", s.id().0),
                    user: s.user_name(),
                    host: format!("{}", s.peer_addr()),
                    database: s.database(),
                    time: s
                        .elapse_since_running_sql()
                        .map(|mills| format!("{}ms", mills)),
                    info: s
                        .running_sql()
                        .map(|sql| format!("{}", truncated_fmt::TruncatedFmt(&sql, 1024))),
                }
            });

            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::Cursor => {
            let sessions = session
                .env()
                .sessions_map()
                .read()
                .values()
                .cloned()
                .collect_vec();
            let mut rows = vec![];
            for s in sessions {
                let session_id = format!("{}", s.id().0);
                let user = s.user_name();
                let host = format!("{}", s.peer_addr());
                let database = s.database();

                s.get_cursor_manager()
                    .iter_query_cursors(|cursor_name: &String, _| {
                        rows.push(ShowCursorRow {
                            session_id: session_id.clone(),
                            user: user.clone(),
                            host: host.clone(),
                            database: database.clone(),
                            cursor_name: cursor_name.to_owned(),
                        });
                    })
                    .await;
            }
            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
        ShowObject::SubscriptionCursor => {
            let sessions = session
                .env()
                .sessions_map()
                .read()
                .values()
                .cloned()
                .collect_vec();
            let mut rows = vec![];
            for s in sessions {
                let session_id = format!("{}", s.id().0);
                let user = s.user_name();
                let host = format!("{}", s.peer_addr());
                let database = s.database().to_owned();

                s.get_cursor_manager()
                    .iter_subscription_cursors(
                        |cursor_name: &String, cursor: &SubscriptionCursor| {
                            rows.push(ShowSubscriptionCursorRow {
                                session_id: session_id.clone(),
                                user: user.clone(),
                                host: host.clone(),
                                database: database.clone(),
                                cursor_name: cursor_name.to_owned(),
                                subscription_name: cursor.subscription_name().to_owned(),
                                state: cursor.state_info_string(),
                                idle_duration_ms: cursor.idle_duration().as_millis() as i64,
                            });
                        },
                    )
                    .await;
            }

            return Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
                .rows(rows)
                .into());
        }
    };

    let rows = names
        .into_iter()
        .filter(|arg| match &filter {
            Some(ShowStatementFilter::Like(pattern)) => like_default(arg, pattern),
            Some(ShowStatementFilter::ILike(pattern)) => i_like_default(arg, pattern),
            Some(ShowStatementFilter::Where(..)) => unreachable!(),
            None => true,
        })
        .map(|name| ShowObjectRow { name });

    Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
        .rows(rows)
        .into())
}

pub fn infer_show_create_object() -> Vec<PgFieldDescriptor> {
    fields_to_descriptors(ShowCreateObjectRow::fields())
}

pub fn handle_show_create_object(
    handle_args: HandlerArgs,
    show_create_type: ShowCreateType,
    name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handle_args.session;
    let catalog_reader = session.env().catalog_reader().read_guard();
    let database = session.database();
    let (schema_name, object_name) =
        Binder::resolve_schema_qualified_name(&database, name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.user_name();
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
    let user_reader = session.env().user_info_reader().read_guard();
    let current_user = user_reader
        .get_user_by_name(user_name)
        .expect("user not found");

    let (sql, schema_name) = match show_create_type {
        ShowCreateType::MaterializedView => {
            let (mv, schema) = schema_path
                .try_find(|schema_name| {
                    Ok::<_, RwError>(
                        catalog_reader
                            .get_schema_by_name(&database, schema_name)?
                            .get_created_table_by_name(&object_name)
                            .filter(|t| {
                                t.is_mview()
                                    && has_access_to_object(
                                        current_user,
                                        schema_name,
                                        t.id.table_id,
                                        t.owner,
                                    )
                            }),
                    )
                })?
                .ok_or_else(|| CatalogError::NotFound("materialized view", name.to_string()))?;
            (mv.create_sql(), schema)
        }
        ShowCreateType::View => {
            let (view, schema) =
                catalog_reader.get_view_by_name(&database, schema_path, &object_name)?;
            if !view.is_system_view()
                && !has_access_to_object(current_user, schema, view.id, view.owner)
            {
                return Err(CatalogError::NotFound("view", name.to_string()).into());
            }
            (view.create_sql(schema.to_owned()), schema)
        }
        ShowCreateType::Table => {
            let (table, schema) = schema_path
                .try_find(|schema_name| {
                    Ok::<_, RwError>(
                        catalog_reader
                            .get_schema_by_name(&database, schema_name)?
                            .get_created_table_by_name(&object_name)
                            .filter(|t| {
                                t.is_user_table()
                                    && has_access_to_object(
                                        current_user,
                                        schema_name,
                                        t.id.table_id,
                                        t.owner,
                                    )
                            }),
                    )
                })?
                .ok_or_else(|| CatalogError::NotFound("table", name.to_string()))?;

            (table.create_sql_purified(), schema)
        }
        ShowCreateType::Sink => {
            let (sink, schema) =
                catalog_reader.get_sink_by_name(&database, schema_path, &object_name)?;
            if !has_access_to_object(current_user, schema, sink.id.sink_id, sink.owner.user_id) {
                return Err(CatalogError::NotFound("sink", name.to_string()).into());
            }
            (sink.create_sql(), schema)
        }
        ShowCreateType::Source => {
            let (source, schema) = schema_path
                .try_find(|schema_name| {
                    Ok::<_, RwError>(
                        catalog_reader
                            .get_schema_by_name(&database, schema_name)?
                            .get_source_by_name(&object_name)
                            .filter(|s| {
                                s.associated_table_id.is_none()
                                    && has_access_to_object(
                                        current_user,
                                        schema_name,
                                        s.id,
                                        s.owner,
                                    )
                            }),
                    )
                })?
                .ok_or_else(|| CatalogError::NotFound("source", name.to_string()))?;
            (source.create_sql_purified(), schema)
        }
        ShowCreateType::Index => {
            let (index, schema) = schema_path
                .try_find(|schema_name| {
                    Ok::<_, RwError>(
                        catalog_reader
                            .get_schema_by_name(&database, schema_name)?
                            .get_created_table_by_name(&object_name)
                            .filter(|t| {
                                t.is_index()
                                    && has_access_to_object(
                                        current_user,
                                        schema_name,
                                        t.id.table_id,
                                        t.owner,
                                    )
                            }),
                    )
                })?
                .ok_or_else(|| CatalogError::NotFound("index", name.to_string()))?;
            (index.create_sql(), schema)
        }
        ShowCreateType::Function => {
            bail_not_implemented!("show create on: {}", show_create_type);
        }
        ShowCreateType::Subscription => {
            let (subscription, schema) =
                catalog_reader.get_subscription_by_name(&database, schema_path, &object_name)?;
            if !has_access_to_object(
                current_user,
                schema,
                subscription.id.subscription_id,
                subscription.owner.user_id,
            ) {
                return Err(CatalogError::NotFound("subscription", name.to_string()).into());
            }
            (subscription.create_sql(), schema)
        }
    };
    let name = format!("{}.{}", schema_name, object_name);

    Ok(PgResponse::builder(StatementType::SHOW_COMMAND)
        .rows([ShowCreateObjectRow {
            name,
            create_sql: sql,
        }])
        .into())
}

#[cfg(test)]
mod tests {
    use std::ops::Index;

    use futures_async_stream::for_await;

    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    #[tokio::test]
    async fn test_show_source() {
        let frontend = LocalFrontend::new(Default::default()).await;

        let sql = r#"CREATE SOURCE t1 (column1 varchar)
        WITH (connector = 'kafka', kafka.topic = 'abc', kafka.brokers = 'localhost:1001')
        FORMAT PLAIN ENCODE JSON"#;
        frontend.run_sql(sql).await.unwrap();

        let mut rows = frontend.query_formatted_result("SHOW SOURCES").await;
        rows.sort();
        assert_eq!(rows, vec!["Row([Some(b\"t1\")])".to_owned(),]);
    }

    #[tokio::test]
    async fn test_show_column() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.brokers = 'localhost:1001')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let sql = "show columns from t";
        let mut pg_response = frontend.run_sql(sql).await.unwrap();

        let mut columns = Vec::new();
        #[for_await]
        for row_set in pg_response.values_stream() {
            let row_set = row_set.unwrap();
            for row in row_set {
                columns.push((
                    std::str::from_utf8(row.index(0).as_ref().unwrap())
                        .unwrap()
                        .to_owned(),
                    std::str::from_utf8(row.index(1).as_ref().unwrap())
                        .unwrap()
                        .to_owned(),
                ));
            }
        }

        expect_test::expect![[r#"
            [
                (
                    "id",
                    "integer",
                ),
                (
                    "country",
                    "test.Country",
                ),
                (
                    "country.address",
                    "character varying",
                ),
                (
                    "country.city",
                    "test.City",
                ),
                (
                    "country.city.address",
                    "character varying",
                ),
                (
                    "country.city.zipcode",
                    "character varying",
                ),
                (
                    "country.zipcode",
                    "character varying",
                ),
                (
                    "zipcode",
                    "bigint",
                ),
                (
                    "rate",
                    "real",
                ),
                (
                    "_rw_kafka_timestamp",
                    "timestamp with time zone",
                ),
                (
                    "_rw_kafka_partition",
                    "character varying",
                ),
                (
                    "_rw_kafka_offset",
                    "character varying",
                ),
                (
                    "_row_id",
                    "serial",
                ),
            ]
        "#]]
        .assert_debug_eq(&columns);
    }
}
