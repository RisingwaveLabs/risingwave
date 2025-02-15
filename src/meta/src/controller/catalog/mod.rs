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

mod alter_op;
mod create_op;
mod drop_op;
mod get_op;
mod list_op;
mod test;
mod util;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter;
use std::mem::take;
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{TableOption, DEFAULT_SCHEMA_NAME, SYSTEM_SCHEMAS};
use risingwave_common::current_cluster_version;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont_mut;
use risingwave_connector::source::cdc::build_cdc_table_id;
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::*;
use risingwave_meta_model::table::TableType;
use risingwave_meta_model::{
    connection, database, fragment, function, index, object, object_dependency, schema, secret,
    sink, source, streaming_job, subscription, table, user_privilege, view, ActorId,
    ColumnCatalogArray, ConnectionId, CreateType, DatabaseId, FragmentId, I32Array, IndexId,
    JobStatus, ObjectId, Property, SchemaId, SecretId, SinkId, SourceId, StreamNode,
    StreamSourceInfo, StreamingParallelism, SubscriptionId, TableId, UserId, ViewId,
};
use risingwave_pb::catalog::connection::Info as ConnectionInfo;
use risingwave_pb::catalog::subscription::SubscriptionState;
use risingwave_pb::catalog::table::PbTableType;
use risingwave_pb::catalog::{
    PbComment, PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSecret, PbSink, PbSource,
    PbStreamJobStatus, PbSubscription, PbTable, PbView,
};
use risingwave_pb::meta::cancel_creating_jobs_request::PbCreatingJobInfo;
use risingwave_pb::meta::list_object_dependencies_response::PbObjectDependencies;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Info, Operation as NotificationOperation, Operation,
};
use risingwave_pb::meta::{PbFragmentWorkerSlotMapping, PbObject, PbObjectGroup};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::FragmentTypeFlag;
use risingwave_pb::telemetry::PbTelemetryEventStage;
use risingwave_pb::user::PbUserInfo;
use sea_orm::sea_query::{Expr, Query, SimpleExpr};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, JoinType, PaginatorTrait, QueryFilter, QuerySelect, RelationTrait,
    SelectColumns, TransactionTrait, Value,
};
use tokio::sync::oneshot::Sender;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::info;

use super::utils::{
    check_subscription_name_duplicate, get_internal_tables_by_id, rename_relation,
    rename_relation_refer,
};
use crate::controller::catalog::util::update_internal_tables;
use crate::controller::utils::*;
use crate::controller::ObjectModel;
use crate::manager::{
    get_referred_connection_ids_from_source, get_referred_secret_ids_from_source, MetaSrvEnv,
    NotificationVersion, IGNORED_NOTIFICATION_VERSION,
};
use crate::rpc::ddl_controller::DropMode;
use crate::telemetry::{report_event, MetaTelemetryJobDesc};
use crate::{MetaError, MetaResult};

pub type Catalog = (
    Vec<PbDatabase>,
    Vec<PbSchema>,
    Vec<PbTable>,
    Vec<PbSource>,
    Vec<PbSink>,
    Vec<PbSubscription>,
    Vec<PbIndex>,
    Vec<PbView>,
    Vec<PbFunction>,
    Vec<PbConnection>,
    Vec<PbSecret>,
);

pub type CatalogControllerRef = Arc<CatalogController>;

/// `CatalogController` is the controller for catalog related operations, including database, schema, table, view, etc.
pub struct CatalogController {
    pub(crate) env: MetaSrvEnv,
    pub(crate) inner: RwLock<CatalogControllerInner>,
}

#[derive(Clone, Default)]
pub struct ReleaseContext {
    pub(crate) database_id: DatabaseId,
    pub(crate) removed_streaming_job_ids: Vec<ObjectId>,
    /// Dropped state table list, need to unregister from hummock.
    pub(crate) removed_state_table_ids: Vec<TableId>,

    /// Dropped secrets, need to remove from secret manager.
    pub(crate) removed_secret_ids: Vec<SecretId>,
    /// Dropped sources (when `DROP SOURCE`), need to unregister from source manager.
    pub(crate) removed_source_ids: Vec<SourceId>,
    /// Dropped Source fragments (when `DROP MATERIALIZED VIEW` referencing sources),
    /// need to unregister from source manager.
    pub(crate) removed_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,

    pub(crate) removed_actors: HashSet<ActorId>,
    pub(crate) removed_fragments: HashSet<FragmentId>,
}

impl CatalogController {
    pub async fn new(env: MetaSrvEnv) -> MetaResult<Self> {
        let meta_store = env.meta_store();
        let catalog_controller = Self {
            env,
            inner: RwLock::new(CatalogControllerInner {
                db: meta_store.conn,
                creating_table_finish_notifier: HashMap::new(),
            }),
        };

        catalog_controller.init().await?;
        Ok(catalog_controller)
    }

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_inner_read_guard(&self) -> RwLockReadGuard<'_, CatalogControllerInner> {
        self.inner.read().await
    }

    pub async fn get_inner_write_guard(&self) -> RwLockWriteGuard<'_, CatalogControllerInner> {
        self.inner.write().await
    }
}

pub struct CatalogControllerInner {
    pub(crate) db: DatabaseConnection,
    /// Registered finish notifiers for creating tables.
    ///
    /// `DdlController` will update this map, and pass the `tx` side to `CatalogController`.
    /// On notifying, we can remove the entry from this map.
    pub creating_table_finish_notifier:
        HashMap<ObjectId, Vec<Sender<MetaResult<NotificationVersion>>>>,
}

impl CatalogController {
    pub(crate) async fn notify_frontend(
        &self,
        operation: NotificationOperation,
        info: NotificationInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }

    pub(crate) async fn notify_frontend_relation_info(
        &self,
        operation: NotificationOperation,
        relation_info: PbObjectInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend_object_info(operation, relation_info)
            .await
    }

    pub(crate) async fn current_notification_version(&self) -> NotificationVersion {
        self.env.notification_manager().current_version().await
    }
}

impl CatalogController {
    pub async fn finish_create_subscription_catalog(&self, subscription_id: u32) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let job_id = subscription_id as i32;

        // update `created_at` as now() and `created_at_cluster_version` as current cluster version.
        let res = Object::update_many()
            .col_expr(object::Column::CreatedAt, Expr::current_timestamp().into())
            .col_expr(
                object::Column::CreatedAtClusterVersion,
                current_cluster_version().into(),
            )
            .filter(object::Column::Oid.eq(job_id))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("subscription", job_id));
        }

        // mark the target subscription as `Create`.
        let job = subscription::ActiveModel {
            subscription_id: Set(job_id),
            subscription_state: Set(SubscriptionState::Created.into()),
            ..Default::default()
        };
        job.update(&txn).await?;
        txn.commit().await?;

        Ok(())
    }

    pub async fn notify_create_subscription(
        &self,
        subscription_id: u32,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.read().await;
        let job_id = subscription_id as i32;
        let (subscription, obj) = Subscription::find_by_id(job_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("subscription", job_id))?;

        let version = self
            .notify_frontend(
                Operation::Add,
                Info::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: PbObjectInfo::Subscription(
                            ObjectModel(subscription, obj.unwrap()).into(),
                        )
                        .into(),
                    }],
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn clean_dirty_subscription(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let filter_condition = object::Column::ObjType.eq(ObjectType::Subscription).and(
            object::Column::Oid.not_in_subquery(
                Query::select()
                    .column(subscription::Column::SubscriptionId)
                    .from(Subscription)
                    .and_where(
                        subscription::Column::SubscriptionState
                            .eq(SubscriptionState::Created as i32),
                    )
                    .take(),
            ),
        );

        let filter_condition = if let Some(database_id) = database_id {
            filter_condition.and(object::Column::DatabaseId.eq(database_id))
        } else {
            filter_condition
        };

        Object::delete_many()
            .filter(filter_condition)
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }

    /// `clean_dirty_creating_jobs` cleans up creating jobs that are creating in Foreground mode or in Initial status.
    pub async fn clean_dirty_creating_jobs(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<Vec<SourceId>> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let filter_condition = streaming_job::Column::JobStatus.eq(JobStatus::Initial).or(
            streaming_job::Column::JobStatus
                .eq(JobStatus::Creating)
                .and(streaming_job::Column::CreateType.eq(CreateType::Foreground)),
        );

        let filter_condition = if let Some(database_id) = database_id {
            filter_condition.and(object::Column::DatabaseId.eq(database_id))
        } else {
            filter_condition
        };

        let dirty_job_objs: Vec<PartialObject> = streaming_job::Entity::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .columns([
                object::Column::Oid,
                object::Column::ObjType,
                object::Column::SchemaId,
                object::Column::DatabaseId,
            ])
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .filter(filter_condition)
            .into_partial_model()
            .all(&txn)
            .await?;

        let changed = Self::clean_dirty_sink_downstreams(&txn).await?;

        if dirty_job_objs.is_empty() {
            if changed {
                txn.commit().await?;
            }

            return Ok(vec![]);
        }

        self.log_cleaned_dirty_jobs(&dirty_job_objs, &txn).await?;

        let dirty_job_ids = dirty_job_objs.iter().map(|obj| obj.oid).collect::<Vec<_>>();

        // Filter out dummy objs for replacement.
        // todo: we'd better introduce a new dummy object type for replacement.
        let all_dirty_table_ids = dirty_job_objs
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Table)
            .map(|obj| obj.oid)
            .collect_vec();
        let dirty_table_type_map: HashMap<ObjectId, TableType> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .column(table::Column::TableType)
            .filter(table::Column::TableId.is_in(all_dirty_table_ids))
            .into_tuple::<(ObjectId, TableType)>()
            .all(&txn)
            .await?
            .into_iter()
            .collect();

        // Only notify delete for failed materialized views.
        let dirty_mview_objs = dirty_job_objs
            .into_iter()
            .filter(|obj| {
                matches!(
                    dirty_table_type_map.get(&obj.oid),
                    Some(TableType::MaterializedView)
                )
            })
            .collect_vec();

        // The source ids for dirty tables with connector.
        // FIXME: we should also clean dirty sources.
        let dirty_associated_source_ids: Vec<SourceId> = Table::find()
            .select_only()
            .column(table::Column::OptionalAssociatedSourceId)
            .filter(
                table::Column::TableId
                    .is_in(dirty_job_ids.clone())
                    .and(table::Column::OptionalAssociatedSourceId.is_not_null()),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let dirty_state_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(table::Column::BelongsToJobId.is_in(dirty_job_ids.clone()))
            .into_tuple()
            .all(&txn)
            .await?;

        let dirty_mview_internal_table_objs = Object::find()
            .select_only()
            .columns([
                object::Column::Oid,
                object::Column::ObjType,
                object::Column::SchemaId,
                object::Column::DatabaseId,
            ])
            .join(JoinType::InnerJoin, object::Relation::Table.def())
            .filter(table::Column::BelongsToJobId.is_in(dirty_mview_objs.iter().map(|obj| obj.oid)))
            .into_partial_model()
            .all(&txn)
            .await?;

        let to_delete_objs: HashSet<ObjectId> = dirty_job_ids
            .clone()
            .into_iter()
            .chain(dirty_state_table_ids.into_iter())
            .chain(dirty_associated_source_ids.clone().into_iter())
            .collect();

        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_delete_objs))
            .exec(&txn)
            .await?;
        assert!(res.rows_affected > 0);

        txn.commit().await?;

        let object_group = build_object_group_for_delete(
            dirty_mview_objs
                .into_iter()
                .chain(dirty_mview_internal_table_objs.into_iter())
                .collect_vec(),
        );

        let _version = self
            .notify_frontend(NotificationOperation::Delete, object_group)
            .await;

        Ok(dirty_associated_source_ids)
    }

    pub async fn comment_on(&self, comment: PbComment) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_object_id(ObjectType::Database, comment.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, comment.schema_id as _, &txn).await?;
        let table_obj = Object::find_by_id(comment.table_id as ObjectId)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("table", comment.table_id))?;

        let table = if let Some(col_idx) = comment.column_index {
            let columns: ColumnCatalogArray = Table::find_by_id(comment.table_id as TableId)
                .select_only()
                .column(table::Column::Columns)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", comment.table_id))?;
            let mut pb_columns = columns.to_protobuf();

            let column = pb_columns
                .get_mut(col_idx as usize)
                .ok_or_else(|| MetaError::catalog_id_not_found("column", col_idx))?;
            let column_desc = column.column_desc.as_mut().ok_or_else(|| {
                anyhow!(
                    "column desc at index {} for table id {} not found",
                    col_idx,
                    comment.table_id
                )
            })?;
            column_desc.description = comment.description;
            table::ActiveModel {
                table_id: Set(comment.table_id as _),
                columns: Set(pb_columns.into()),
                ..Default::default()
            }
            .update(&txn)
            .await?
        } else {
            table::ActiveModel {
                table_id: Set(comment.table_id as _),
                description: Set(comment.description),
                ..Default::default()
            }
            .update(&txn)
            .await?
        };
        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Update,
                PbObjectInfo::Table(ObjectModel(table, table_obj).into()),
            )
            .await;

        Ok(version)
    }
}

/// `CatalogStats` is a struct to store the statistics of all catalogs.
pub struct CatalogStats {
    pub table_num: u64,
    pub mview_num: u64,
    pub index_num: u64,
    pub source_num: u64,
    pub sink_num: u64,
    pub function_num: u64,
    pub streaming_job_num: u64,
    pub actor_num: u64,
}

impl CatalogControllerInner {
    pub async fn snapshot(&self) -> MetaResult<(Catalog, Vec<PbUserInfo>)> {
        let databases = self.list_databases().await?;
        let schemas = self.list_schemas().await?;
        let tables = self.list_tables().await?;
        let sources = self.list_sources().await?;
        let sinks = self.list_sinks().await?;
        let subscriptions = self.list_subscriptions().await?;
        let indexes = self.list_indexes().await?;
        let views = self.list_views().await?;
        let functions = self.list_functions().await?;
        let connections = self.list_connections().await?;
        let secrets = self.list_secrets().await?;

        let users = self.list_users().await?;

        Ok((
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                subscriptions,
                indexes,
                views,
                functions,
                connections,
                secrets,
            ),
            users,
        ))
    }

    pub async fn stats(&self) -> MetaResult<CatalogStats> {
        let mut table_num_map: HashMap<_, _> = Table::find()
            .select_only()
            .column(table::Column::TableType)
            .column_as(table::Column::TableId.count(), "num")
            .group_by(table::Column::TableType)
            .having(table::Column::TableType.ne(TableType::Internal))
            .into_tuple::<(TableType, i64)>()
            .all(&self.db)
            .await?
            .into_iter()
            .map(|(table_type, num)| (table_type, num as u64))
            .collect();

        let source_num = Source::find().count(&self.db).await?;
        let sink_num = Sink::find().count(&self.db).await?;
        let function_num = Function::find().count(&self.db).await?;
        let streaming_job_num = StreamingJob::find().count(&self.db).await?;
        let actor_num = Actor::find().count(&self.db).await?;

        Ok(CatalogStats {
            table_num: table_num_map.remove(&TableType::Table).unwrap_or(0),
            mview_num: table_num_map
                .remove(&TableType::MaterializedView)
                .unwrap_or(0),
            index_num: table_num_map.remove(&TableType::Index).unwrap_or(0),
            source_num,
            sink_num,
            function_num,
            streaming_job_num,
            actor_num,
        })
    }

    async fn list_databases(&self) -> MetaResult<Vec<PbDatabase>> {
        let db_objs = Database::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;
        Ok(db_objs
            .into_iter()
            .map(|(db, obj)| ObjectModel(db, obj.unwrap()).into())
            .collect())
    }

    async fn list_schemas(&self) -> MetaResult<Vec<PbSchema>> {
        let schema_objs = Schema::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(schema_objs
            .into_iter()
            .map(|(schema, obj)| ObjectModel(schema, obj.unwrap()).into())
            .collect())
    }

    async fn list_users(&self) -> MetaResult<Vec<PbUserInfo>> {
        let mut user_infos: Vec<PbUserInfo> = User::find()
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();

        for user_info in &mut user_infos {
            user_info.grant_privileges = get_user_privilege(user_info.id as _, &self.db).await?;
        }
        Ok(user_infos)
    }

    /// `list_all_tables` return all tables and internal tables.
    pub async fn list_all_state_tables(&self) -> MetaResult<Vec<PbTable>> {
        let table_objs = Table::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    /// `list_tables` return all `CREATED` tables, `CREATING` materialized views and internal tables that belong to them.
    async fn list_tables(&self) -> MetaResult<Vec<PbTable>> {
        let table_objs = Table::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                streaming_job::Column::JobStatus
                    .eq(JobStatus::Created)
                    .or(table::Column::TableType.eq(TableType::MaterializedView)),
            )
            .all(&self.db)
            .await?;

        let created_streaming_job_ids: Vec<ObjectId> = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .into_tuple()
            .all(&self.db)
            .await?;

        let job_ids: HashSet<ObjectId> = table_objs
            .iter()
            .map(|(t, _)| t.table_id)
            .chain(created_streaming_job_ids.iter().cloned())
            .collect();

        let internal_table_objs = Table::find()
            .find_also_related(Object)
            .filter(
                table::Column::TableType
                    .eq(TableType::Internal)
                    .and(table::Column::BelongsToJobId.is_in(job_ids)),
            )
            .all(&self.db)
            .await?;

        Ok(table_objs
            .into_iter()
            .chain(internal_table_objs.into_iter())
            .map(|(table, obj)| {
                // Correctly set the stream job status for creating materialized views and internal tables.
                let is_created = created_streaming_job_ids.contains(&table.table_id)
                    || (table.table_type == TableType::Internal
                        && created_streaming_job_ids.contains(&table.belongs_to_job_id.unwrap()));
                let mut pb_table: PbTable = ObjectModel(table, obj.unwrap()).into();
                pb_table.stream_job_status = if is_created {
                    PbStreamJobStatus::Created.into()
                } else {
                    PbStreamJobStatus::Creating.into()
                };
                pb_table
            })
            .collect())
    }

    /// `list_sources` return all sources and `CREATED` ones if contains any streaming jobs.
    async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        let mut source_objs = Source::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                streaming_job::Column::JobStatus
                    .is_null()
                    .or(streaming_job::Column::JobStatus.eq(JobStatus::Created)),
            )
            .all(&self.db)
            .await?;

        // filter out inner connector sources that are still under creating.
        let created_table_ids: HashSet<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                table::Column::OptionalAssociatedSourceId
                    .is_not_null()
                    .and(streaming_job::Column::JobStatus.eq(JobStatus::Created)),
            )
            .into_tuple()
            .all(&self.db)
            .await?
            .into_iter()
            .collect();
        source_objs.retain_mut(|(source, _)| {
            source.optional_associated_table_id.is_none()
                || created_table_ids.contains(&source.optional_associated_table_id.unwrap())
        });

        Ok(source_objs
            .into_iter()
            .map(|(source, obj)| ObjectModel(source, obj.unwrap()).into())
            .collect())
    }

    /// `list_sinks` return all `CREATED` sinks.
    async fn list_sinks(&self) -> MetaResult<Vec<PbSink>> {
        let sink_objs = Sink::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .all(&self.db)
            .await?;

        Ok(sink_objs
            .into_iter()
            .map(|(sink, obj)| ObjectModel(sink, obj.unwrap()).into())
            .collect())
    }

    /// `list_subscriptions` return all `CREATED` subscriptions.
    async fn list_subscriptions(&self) -> MetaResult<Vec<PbSubscription>> {
        let subscription_objs = Subscription::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(subscription_objs
            .into_iter()
            .map(|(subscription, obj)| ObjectModel(subscription, obj.unwrap()).into())
            .collect())
    }

    async fn list_views(&self) -> MetaResult<Vec<PbView>> {
        let view_objs = View::find().find_also_related(Object).all(&self.db).await?;

        Ok(view_objs
            .into_iter()
            .map(|(view, obj)| ObjectModel(view, obj.unwrap()).into())
            .collect())
    }

    /// `list_indexes` return all `CREATED` indexes.
    async fn list_indexes(&self) -> MetaResult<Vec<PbIndex>> {
        let index_objs = Index::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .all(&self.db)
            .await?;

        Ok(index_objs
            .into_iter()
            .map(|(index, obj)| ObjectModel(index, obj.unwrap()).into())
            .collect())
    }

    async fn list_connections(&self) -> MetaResult<Vec<PbConnection>> {
        let conn_objs = Connection::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(conn_objs
            .into_iter()
            .map(|(conn, obj)| ObjectModel(conn, obj.unwrap()).into())
            .collect())
    }

    pub async fn list_secrets(&self) -> MetaResult<Vec<PbSecret>> {
        let secret_objs = Secret::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;
        Ok(secret_objs
            .into_iter()
            .map(|(secret, obj)| ObjectModel(secret, obj.unwrap()).into())
            .collect())
    }

    async fn list_functions(&self) -> MetaResult<Vec<PbFunction>> {
        let func_objs = Function::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(func_objs
            .into_iter()
            .map(|(func, obj)| ObjectModel(func, obj.unwrap()).into())
            .collect())
    }

    pub(crate) fn register_finish_notifier(
        &mut self,
        id: i32,
        sender: Sender<MetaResult<NotificationVersion>>,
    ) {
        self.creating_table_finish_notifier
            .entry(id)
            .or_default()
            .push(sender);
    }

    pub(crate) async fn streaming_job_is_finished(&mut self, id: i32) -> MetaResult<bool> {
        let status = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobStatus)
            .filter(streaming_job::Column::JobId.eq(id))
            .into_tuple::<JobStatus>()
            .one(&self.db)
            .await?;

        status
            .map(|status| status == JobStatus::Created)
            .ok_or_else(|| {
                MetaError::catalog_id_not_found("streaming job", "may have been cancelled/dropped")
            })
    }

    pub(crate) fn notify_finish_failed(&mut self, err: &MetaError) {
        for tx in take(&mut self.creating_table_finish_notifier)
            .into_values()
            .flatten()
        {
            let _ = tx.send(Err(err.clone()));
        }
    }

    pub async fn list_time_travel_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .filter(table::Column::TableType.is_in(vec![
                TableType::Table,
                TableType::MaterializedView,
                TableType::Index,
            ]))
            .column(table::Column::TableId)
            .into_tuple()
            .all(&self.db)
            .await?;
        Ok(table_ids)
    }
}
