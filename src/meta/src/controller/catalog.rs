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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter;
use std::mem::take;
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{TableOption, DEFAULT_SCHEMA_NAME, SYSTEM_SCHEMAS};
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont_mut;
use risingwave_common::{bail, current_cluster_version};
use risingwave_connector::source::cdc::build_cdc_table_id;
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::*;
use risingwave_meta_model::table::TableType;
use risingwave_meta_model::{
    actor, connection, database, fragment, function, index, object, object_dependency, schema,
    secret, sink, source, streaming_job, subscription, table, user_privilege, view, ActorId,
    ActorUpstreamActors, ColumnCatalogArray, ConnectionId, CreateType, DatabaseId, FragmentId,
    FunctionId, I32Array, IndexId, JobStatus, ObjectId, Property, SchemaId, SecretId, SinkId,
    SourceId, StreamNode, StreamSourceInfo, StreamingParallelism, SubscriptionId, TableId, UserId,
    ViewId,
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
use risingwave_pb::meta::relation::PbRelationInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Info, Operation as NotificationOperation, Operation,
};
use risingwave_pb::meta::{PbFragmentWorkerSlotMapping, PbRelation, PbRelationGroup};
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
    check_subscription_name_duplicate, get_fragment_ids_by_jobs, get_internal_tables_by_id,
    rename_relation, rename_relation_refer,
};
use crate::controller::utils::{
    build_relation_group_for_delete, check_connection_name_duplicate,
    check_database_name_duplicate, check_function_signature_duplicate,
    check_relation_name_duplicate, check_schema_name_duplicate, check_secret_name_duplicate,
    ensure_object_id, ensure_object_not_refer, ensure_schema_empty, ensure_user_id,
    extract_external_table_name_from_definition, get_referring_objects,
    get_referring_objects_cascade, get_user_privilege, list_user_info_by_ids,
    resolve_source_register_info_for_jobs, PartialObject,
};
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
    pub(crate) streaming_job_ids: Vec<ObjectId>,
    /// Dropped state table list, need to unregister from hummock.
    pub(crate) state_table_ids: Vec<TableId>,
    /// Dropped source list, need to unregister from source manager.
    pub(crate) source_ids: Vec<SourceId>,
    /// Dropped connection list, need to delete from vpc endpoints.
    #[allow(dead_code)]
    pub(crate) connections: Vec<ConnectionId>,

    /// Dropped fragments that are fetching data from the target source.
    pub(crate) source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    /// Dropped actors.
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

    async fn init(&self) -> MetaResult<()> {
        self.table_catalog_cdc_table_id_update().await?;
        Ok(())
    }

    /// Fill in the `cdc_table_id` field for Table with empty `cdc_table_id` and parent Source job.
    /// NOTES: We assume Table with a parent Source job is a CDC table
    async fn table_catalog_cdc_table_id_update(&self) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        // select Tables which cdc_table_id is empty and has a parent Source job
        let table_and_source_id: Vec<(TableId, String, SourceId)> = Table::find()
            .join(JoinType::InnerJoin, table::Relation::ObjectDependency.def())
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Source.def(),
            )
            .select_only()
            .columns([table::Column::TableId, table::Column::Definition])
            .columns([source::Column::SourceId])
            .filter(
                table::Column::TableType.eq(TableType::Table).and(
                    table::Column::CdcTableId
                        .is_null()
                        .or(table::Column::CdcTableId.eq("")),
                ),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        // return directly if the result set is empty.
        if table_and_source_id.is_empty() {
            return Ok(());
        }

        info!(table_and_source_id = ?table_and_source_id, "cdc table with empty cdc_table_id");

        let mut cdc_table_ids = HashMap::new();
        for (table_id, definition, source_id) in table_and_source_id {
            match extract_external_table_name_from_definition(&definition) {
                None => {
                    tracing::warn!(
                        table_id = table_id,
                        definition = definition,
                        "failed to extract cdc table name from table definition.",
                    )
                }
                Some(external_table_name) => {
                    cdc_table_ids.insert(
                        table_id,
                        build_cdc_table_id(source_id as u32, &external_table_name),
                    );
                }
            }
        }

        for (table_id, cdc_table_id) in cdc_table_ids {
            table::ActiveModel {
                table_id: Set(table_id as _),
                cdc_table_id: Set(Some(cdc_table_id)),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        txn.commit().await?;
        Ok(())
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
        relation_info: PbRelationInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend_relation_info(operation, relation_info)
            .await
    }

    pub(crate) async fn current_notification_version(&self) -> NotificationVersion {
        self.env.notification_manager().current_version().await
    }
}

impl CatalogController {
    pub(crate) async fn create_object(
        txn: &DatabaseTransaction,
        obj_type: ObjectType,
        owner_id: UserId,
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
    ) -> MetaResult<object::Model> {
        let active_db = object::ActiveModel {
            oid: Default::default(),
            obj_type: Set(obj_type),
            owner_id: Set(owner_id),
            schema_id: Set(schema_id),
            database_id: Set(database_id),
            initialized_at: Default::default(),
            created_at: Default::default(),
            initialized_at_cluster_version: Set(Some(current_cluster_version())),
            created_at_cluster_version: Set(Some(current_cluster_version())),
        };
        Ok(active_db.insert(txn).await?)
    }

    pub async fn create_database(&self, db: PbDatabase) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = db.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        check_database_name_duplicate(&db.name, &txn).await?;

        let db_obj = Self::create_object(&txn, ObjectType::Database, owner_id, None, None).await?;
        let mut db: database::ActiveModel = db.into();
        db.database_id = Set(db_obj.oid);
        let db = db.insert(&txn).await?;

        let mut schemas = vec![];
        for schema_name in iter::once(DEFAULT_SCHEMA_NAME).chain(SYSTEM_SCHEMAS) {
            let schema_obj =
                Self::create_object(&txn, ObjectType::Schema, owner_id, Some(db_obj.oid), None)
                    .await?;
            let schema = schema::ActiveModel {
                schema_id: Set(schema_obj.oid),
                name: Set(schema_name.into()),
            };
            let schema = schema.insert(&txn).await?;
            schemas.push(ObjectModel(schema, schema_obj).into());
        }
        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Database(ObjectModel(db, db_obj).into()),
            )
            .await;
        for schema in schemas {
            version = self
                .notify_frontend(NotificationOperation::Add, NotificationInfo::Schema(schema))
                .await;
        }

        Ok(version)
    }

    pub async fn drop_database(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_object_id(ObjectType::Database, database_id, &txn).await?;

        let streaming_jobs: Vec<ObjectId> = StreamingJob::find()
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(object::Column::DatabaseId.eq(Some(database_id)))
            .into_tuple()
            .all(&txn)
            .await?;

        let (source_fragments, removed_actors, removed_fragments) =
            resolve_source_register_info_for_jobs(&txn, streaming_jobs.clone()).await?;

        let state_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(
                table::Column::BelongsToJobId
                    .is_in(streaming_jobs.clone())
                    .or(table::Column::TableId.is_in(streaming_jobs.clone())),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let source_ids: Vec<SourceId> = Object::find()
            .select_only()
            .column(object::Column::Oid)
            .filter(
                object::Column::DatabaseId
                    .eq(Some(database_id))
                    .and(object::Column::ObjType.eq(ObjectType::Source)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let connections = Connection::find()
            .inner_join(Object)
            .filter(object::Column::DatabaseId.eq(Some(database_id)))
            .all(&txn)
            .await?
            .into_iter()
            .map(|conn| conn.connection_id)
            .collect_vec();

        // Find affect users with privileges on the database and the objects in the database.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .join(JoinType::InnerJoin, user_privilege::Relation::Object.def())
            .filter(
                object::Column::DatabaseId
                    .eq(database_id)
                    .or(user_privilege::Column::Oid.eq(database_id)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let fragment_mappings = get_fragment_ids_by_jobs(&txn, streaming_jobs.clone())
            .await?
            .into_iter()
            .map(|fragment_id| PbFragmentWorkerSlotMapping {
                fragment_id: fragment_id as _,
                mapping: None,
            })
            .collect();

        // The schema and objects in the database will be delete cascade.
        let res = Object::delete_by_id(database_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("database", database_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Database(PbDatabase {
                    id: database_id as _,
                    ..Default::default()
                }),
            )
            .await;

        self.notify_fragment_mapping(NotificationOperation::Delete, fragment_mappings)
            .await;
        Ok((
            ReleaseContext {
                database_id,
                streaming_job_ids: streaming_jobs,
                state_table_ids,
                source_ids,
                connections,
                source_fragments,
                removed_actors,
                removed_fragments,
            },
            version,
        ))
    }

    pub async fn get_object_database_id(&self, object_id: ObjectId) -> MetaResult<DatabaseId> {
        let inner = self.inner.read().await;
        let (database_id,): (Option<DatabaseId>,) = Object::find_by_id(object_id)
            .select_only()
            .select_column(object::Column::DatabaseId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("object", object_id))?;
        Ok(database_id.ok_or_else(|| anyhow!("object has no database id: {object_id}"))?)
    }

    pub async fn create_schema(&self, schema: PbSchema) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = schema.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, schema.database_id as _, &txn).await?;
        check_schema_name_duplicate(&schema.name, schema.database_id as _, &txn).await?;

        let schema_obj = Self::create_object(
            &txn,
            ObjectType::Schema,
            owner_id,
            Some(schema.database_id as _),
            None,
        )
        .await?;
        let mut schema: schema::ActiveModel = schema.into();
        schema.schema_id = Set(schema_obj.oid);
        let schema = schema.insert(&txn).await?;
        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Schema(ObjectModel(schema, schema_obj).into()),
            )
            .await;
        Ok(version)
    }

    pub async fn drop_schema(
        &self,
        schema_id: SchemaId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let schema_obj = Object::find_by_id(schema_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("schema", schema_id))?;
        // TODO: support drop schema cascade.
        if drop_mode == DropMode::Restrict {
            ensure_schema_empty(schema_id, &txn).await?;
        } else {
            return Err(MetaError::permission_denied(
                "drop schema cascade is not supported yet".to_owned(),
            ));
        }

        // Find affect users with privileges on the schema and the objects in the schema.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .join(JoinType::InnerJoin, user_privilege::Relation::Object.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .or(user_privilege::Column::Oid.eq(schema_id)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete(object::ActiveModel {
            oid: Set(schema_id),
            ..Default::default()
        })
        .exec(&txn)
        .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("schema", schema_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Schema(PbSchema {
                    id: schema_id as _,
                    database_id: schema_obj.database_id.unwrap() as _,
                    ..Default::default()
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn create_subscription_catalog(
        &self,
        pb_subscription: &mut PbSubscription,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        ensure_user_id(pb_subscription.owner as _, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_subscription.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_subscription.schema_id as _, &txn).await?;
        check_subscription_name_duplicate(pb_subscription, &txn).await?;

        let obj = Self::create_object(
            &txn,
            ObjectType::Subscription,
            pb_subscription.owner as _,
            Some(pb_subscription.database_id as _),
            Some(pb_subscription.schema_id as _),
        )
        .await?;
        pb_subscription.id = obj.oid as _;
        let subscription: subscription::ActiveModel = pb_subscription.clone().into();
        Subscription::insert(subscription).exec(&txn).await?;

        // record object dependency.
        ObjectDependency::insert(object_dependency::ActiveModel {
            oid: Set(pb_subscription.dependent_table_id as _),
            used_by: Set(pb_subscription.id as _),
            ..Default::default()
        })
        .exec(&txn)
        .await?;
        txn.commit().await?;
        Ok(())
    }

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
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let job_id = subscription_id as i32;
        let (subscription, obj) = Subscription::find_by_id(job_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("subscription", job_id))?;
        txn.commit().await?;

        let version = self
            .notify_frontend(
                Operation::Add,
                Info::RelationGroup(PbRelationGroup {
                    relations: vec![PbRelation {
                        relation_info: PbRelationInfo::Subscription(
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

    pub async fn list_background_creating_mviews(
        &self,
        include_initial: bool,
    ) -> MetaResult<Vec<table::Model>> {
        let inner = self.inner.read().await;
        let status_cond = if include_initial {
            streaming_job::Column::JobStatus.is_in([JobStatus::Initial, JobStatus::Creating])
        } else {
            streaming_job::Column::JobStatus.eq(JobStatus::Creating)
        };
        let tables = Table::find()
            .join(JoinType::LeftJoin, table::Relation::Object1.def())
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                table::Column::TableType
                    .eq(TableType::MaterializedView)
                    .and(
                        streaming_job::Column::CreateType
                            .eq(CreateType::Background)
                            .and(status_cond),
                    ),
            )
            .all(&inner.db)
            .await?;
        Ok(tables)
    }

    pub async fn list_all_object_dependencies(&self) -> MetaResult<Vec<PbObjectDependencies>> {
        self.list_object_dependencies(true).await
    }

    pub async fn list_created_object_dependencies(&self) -> MetaResult<Vec<PbObjectDependencies>> {
        self.list_object_dependencies(false).await
    }

    async fn list_object_dependencies(
        &self,
        include_creating: bool,
    ) -> MetaResult<Vec<PbObjectDependencies>> {
        let inner = self.inner.read().await;

        let dependencies: Vec<(ObjectId, ObjectId)> = {
            let filter = if include_creating {
                Expr::value(true)
            } else {
                streaming_job::Column::JobStatus.eq(JobStatus::Created)
            };
            ObjectDependency::find()
                .select_only()
                .columns([
                    object_dependency::Column::Oid,
                    object_dependency::Column::UsedBy,
                ])
                .join(
                    JoinType::InnerJoin,
                    object_dependency::Relation::Object1.def(),
                )
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        let mut obj_dependencies = dependencies
            .into_iter()
            .map(|(oid, used_by)| PbObjectDependencies {
                object_id: used_by as _,
                referenced_object_id: oid as _,
            })
            .collect_vec();

        let view_dependencies: Vec<(ObjectId, ObjectId)> = ObjectDependency::find()
            .select_only()
            .columns([
                object_dependency::Column::Oid,
                object_dependency::Column::UsedBy,
            ])
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Object1.def(),
            )
            .join(JoinType::InnerJoin, object::Relation::View.def())
            .into_tuple()
            .all(&inner.db)
            .await?;

        obj_dependencies.extend(view_dependencies.into_iter().map(|(view_id, table_id)| {
            PbObjectDependencies {
                object_id: table_id as _,
                referenced_object_id: view_id as _,
            }
        }));

        let sink_dependencies: Vec<(SinkId, TableId)> = {
            let filter = if include_creating {
                sink::Column::TargetTable.is_not_null()
            } else {
                streaming_job::Column::JobStatus
                    .eq(JobStatus::Created)
                    .and(sink::Column::TargetTable.is_not_null())
            };
            Sink::find()
                .select_only()
                .columns([sink::Column::SinkId, sink::Column::TargetTable])
                .join(JoinType::InnerJoin, sink::Relation::Object.def())
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        obj_dependencies.extend(sink_dependencies.into_iter().map(|(sink_id, table_id)| {
            PbObjectDependencies {
                object_id: table_id as _,
                referenced_object_id: sink_id as _,
            }
        }));

        let subscription_dependencies: Vec<(SubscriptionId, TableId)> = {
            let filter = if include_creating {
                subscription::Column::DependentTableId.is_not_null()
            } else {
                subscription::Column::SubscriptionState
                    .eq(Into::<i32>::into(SubscriptionState::Created))
                    .and(subscription::Column::DependentTableId.is_not_null())
            };
            Subscription::find()
                .select_only()
                .columns([
                    subscription::Column::SubscriptionId,
                    subscription::Column::DependentTableId,
                ])
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        obj_dependencies.extend(subscription_dependencies.into_iter().map(
            |(subscription_id, table_id)| PbObjectDependencies {
                object_id: subscription_id as _,
                referenced_object_id: table_id as _,
            },
        ));

        Ok(obj_dependencies)
    }

    pub async fn has_any_streaming_jobs(&self) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let count = streaming_job::Entity::find().count(&inner.db).await?;
        Ok(count > 0)
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

        let associated_source_ids: Vec<SourceId> = Table::find()
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
            .chain(associated_source_ids.clone().into_iter())
            .collect();

        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_delete_objs))
            .exec(&txn)
            .await?;
        assert!(res.rows_affected > 0);

        txn.commit().await?;

        let relation_group = build_relation_group_for_delete(
            dirty_mview_objs
                .into_iter()
                .chain(dirty_mview_internal_table_objs.into_iter())
                .collect_vec(),
        );

        let _version = self
            .notify_frontend(NotificationOperation::Delete, relation_group)
            .await;

        Ok(associated_source_ids)
    }

    async fn log_cleaned_dirty_jobs(
        &self,
        dirty_objs: &[PartialObject],
        txn: &DatabaseTransaction,
    ) -> MetaResult<()> {
        // Record cleaned streaming jobs in event logs.
        let mut dirty_table_ids = vec![];
        let mut dirty_source_ids = vec![];
        let mut dirty_sink_ids = vec![];
        for dirty_job_obj in dirty_objs {
            let job_id = dirty_job_obj.oid;
            let job_type = dirty_job_obj.obj_type;
            match job_type {
                ObjectType::Table | ObjectType::Index => dirty_table_ids.push(job_id),
                ObjectType::Source => dirty_source_ids.push(job_id),
                ObjectType::Sink => dirty_sink_ids.push(job_id),
                _ => unreachable!("unexpected streaming job type"),
            }
        }

        let mut event_logs = vec![];
        if !dirty_table_ids.is_empty() {
            let table_info: Vec<(TableId, String, String)> = Table::find()
                .select_only()
                .columns([
                    table::Column::TableId,
                    table::Column::Name,
                    table::Column::Definition,
                ])
                .filter(table::Column::TableId.is_in(dirty_table_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (table_id, name, definition) in table_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: table_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_source_ids.is_empty() {
            let source_info: Vec<(SourceId, String, String)> = Source::find()
                .select_only()
                .columns([
                    source::Column::SourceId,
                    source::Column::Name,
                    source::Column::Definition,
                ])
                .filter(source::Column::SourceId.is_in(dirty_source_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (source_id, name, definition) in source_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: source_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_sink_ids.is_empty() {
            let sink_info: Vec<(SinkId, String, String)> = Sink::find()
                .select_only()
                .columns([
                    sink::Column::SinkId,
                    sink::Column::Name,
                    sink::Column::Definition,
                ])
                .filter(sink::Column::SinkId.is_in(dirty_sink_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (sink_id, name, definition) in sink_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: sink_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        self.env.event_log_manager_ref().add_event_logs(event_logs);
        Ok(())
    }

    async fn clean_dirty_sink_downstreams(txn: &DatabaseTransaction) -> MetaResult<bool> {
        // clean incoming sink from (table)
        // clean upstream fragment ids from (fragment)
        // clean stream node from (fragment)
        // clean upstream actor ids from (actor)
        let all_fragment_ids: Vec<FragmentId> = Fragment::find()
            .select_only()
            .columns(vec![fragment::Column::FragmentId])
            .into_tuple()
            .all(txn)
            .await?;

        let all_fragment_ids: HashSet<_> = all_fragment_ids.into_iter().collect();

        let table_sink_ids: Vec<ObjectId> = Sink::find()
            .select_only()
            .column(sink::Column::SinkId)
            .filter(sink::Column::TargetTable.is_not_null())
            .into_tuple()
            .all(txn)
            .await?;

        let all_table_with_incoming_sinks: Vec<(ObjectId, I32Array)> = Table::find()
            .select_only()
            .columns(vec![table::Column::TableId, table::Column::IncomingSinks])
            .into_tuple()
            .all(txn)
            .await?;

        let table_incoming_sinks_to_update = all_table_with_incoming_sinks
            .into_iter()
            .filter(|(_, incoming_sinks)| {
                let inner_ref = incoming_sinks.inner_ref();
                !inner_ref.is_empty()
                    && inner_ref
                        .iter()
                        .any(|sink_id| !table_sink_ids.contains(sink_id))
            })
            .collect_vec();

        let new_table_incoming_sinks = table_incoming_sinks_to_update
            .into_iter()
            .map(|(table_id, incoming_sinks)| {
                let new_incoming_sinks = incoming_sinks
                    .into_inner()
                    .extract_if(|id| table_sink_ids.contains(id))
                    .collect_vec();
                (table_id, I32Array::from(new_incoming_sinks))
            })
            .collect_vec();

        // no need to update, returning
        if new_table_incoming_sinks.is_empty() {
            return Ok(false);
        }

        for (table_id, new_incoming_sinks) in new_table_incoming_sinks {
            tracing::info!("cleaning dirty table sink downstream table {}", table_id);
            Table::update_many()
                .col_expr(table::Column::IncomingSinks, new_incoming_sinks.into())
                .filter(table::Column::TableId.eq(table_id))
                .exec(txn)
                .await?;

            let fragments: Vec<(FragmentId, I32Array, StreamNode, i32)> = Fragment::find()
                .select_only()
                .columns(vec![
                    fragment::Column::FragmentId,
                    fragment::Column::UpstreamFragmentId,
                    fragment::Column::StreamNode,
                    fragment::Column::FragmentTypeMask,
                ])
                .filter(fragment::Column::JobId.eq(table_id))
                .into_tuple()
                .all(txn)
                .await?;

            for (fragment_id, upstream_fragment_ids, stream_node, fragment_mask) in fragments {
                let mut upstream_fragment_ids = upstream_fragment_ids.into_inner();

                let dirty_upstream_fragment_ids = upstream_fragment_ids
                    .extract_if(|id| !all_fragment_ids.contains(id))
                    .collect_vec();

                if !dirty_upstream_fragment_ids.is_empty() {
                    // dirty downstream should be materialize fragment of table
                    assert!(fragment_mask & FragmentTypeFlag::Mview as i32 > 0);

                    tracing::info!(
                        "cleaning dirty table sink fragment {:?} from downstream fragment {}",
                        dirty_upstream_fragment_ids,
                        fragment_id
                    );

                    let mut pb_stream_node = stream_node.to_protobuf();

                    visit_stream_node_cont_mut(&mut pb_stream_node, |node| {
                        if let Some(NodeBody::Union(_)) = node.node_body {
                            node.input.retain_mut(|input| {
                                if let Some(NodeBody::Merge(merge_node)) = &mut input.node_body
                                    && all_fragment_ids
                                        .contains(&(merge_node.upstream_fragment_id as i32))
                                {
                                    true
                                } else {
                                    false
                                }
                            });
                        }
                        true
                    });

                    Fragment::update_many()
                        .col_expr(
                            fragment::Column::UpstreamFragmentId,
                            I32Array::from(upstream_fragment_ids).into(),
                        )
                        .col_expr(
                            fragment::Column::StreamNode,
                            StreamNode::from(&pb_stream_node).into(),
                        )
                        .filter(fragment::Column::FragmentId.eq(fragment_id))
                        .exec(txn)
                        .await?;

                    let actors: Vec<(ActorId, ActorUpstreamActors)> = Actor::find()
                        .select_only()
                        .columns(vec![
                            actor::Column::ActorId,
                            actor::Column::UpstreamActorIds,
                        ])
                        .filter(actor::Column::FragmentId.eq(fragment_id))
                        .into_tuple()
                        .all(txn)
                        .await?;

                    for (actor_id, upstream_actor_ids) in actors {
                        let mut upstream_actor_ids = upstream_actor_ids.into_inner();

                        let dirty_actor_upstreams = upstream_actor_ids
                            .extract_if(|id, _| !all_fragment_ids.contains(id))
                            .map(|(id, _)| id)
                            .collect_vec();

                        if !dirty_actor_upstreams.is_empty() {
                            tracing::debug!(
                                "cleaning dirty table sink fragment {:?} from downstream fragment {} actor {}",
                                dirty_actor_upstreams,
                                fragment_id,
                                actor_id,
                            );

                            Actor::update_many()
                                .col_expr(
                                    actor::Column::UpstreamActorIds,
                                    ActorUpstreamActors::from(upstream_actor_ids).into(),
                                )
                                .filter(actor::Column::ActorId.eq(actor_id))
                                .exec(txn)
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(true)
    }

    pub async fn create_source(
        &self,
        mut pb_source: PbSource,
    ) -> MetaResult<(SourceId, NotificationVersion)> {
        let inner = self.inner.write().await;
        let owner_id = pb_source.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_source.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_source.schema_id as _, &txn).await?;
        check_relation_name_duplicate(
            &pb_source.name,
            pb_source.database_id as _,
            pb_source.schema_id as _,
            &txn,
        )
        .await?;

        // handle secret ref
        let secret_ids = get_referred_secret_ids_from_source(&pb_source)?;
        let connection_ids = get_referred_connection_ids_from_source(&pb_source);

        let source_obj = Self::create_object(
            &txn,
            ObjectType::Source,
            owner_id,
            Some(pb_source.database_id as _),
            Some(pb_source.schema_id as _),
        )
        .await?;
        let source_id = source_obj.oid;
        pb_source.id = source_id as _;
        let source: source::ActiveModel = pb_source.clone().into();
        Source::insert(source).exec(&txn).await?;

        // add secret dependency
        let dep_relation_ids = secret_ids.iter().chain(connection_ids.iter());
        if !secret_ids.is_empty() || !connection_ids.is_empty() {
            ObjectDependency::insert_many(dep_relation_ids.map(|id| {
                object_dependency::ActiveModel {
                    oid: Set(*id as _),
                    used_by: Set(source_id as _),
                    ..Default::default()
                }
            }))
            .exec(&txn)
            .await?;
        }

        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Add,
                PbRelationInfo::Source(pb_source),
            )
            .await;
        Ok((source_id, version))
    }

    pub async fn create_function(
        &self,
        mut pb_function: PbFunction,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_function.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_function.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_function.schema_id as _, &txn).await?;
        check_function_signature_duplicate(&pb_function, &txn).await?;

        let function_obj = Self::create_object(
            &txn,
            ObjectType::Function,
            owner_id,
            Some(pb_function.database_id as _),
            Some(pb_function.schema_id as _),
        )
        .await?;
        pb_function.id = function_obj.oid as _;
        let function: function::ActiveModel = pb_function.clone().into();
        Function::insert(function).exec(&txn).await?;
        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Function(pb_function),
            )
            .await;
        Ok(version)
    }

    pub async fn drop_function(&self, function_id: FunctionId) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let function_obj = Object::find_by_id(function_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("function", function_id))?;
        ensure_object_not_refer(ObjectType::Function, function_id, &txn).await?;

        // Find affect users with privileges on the function.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.eq(function_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete_by_id(function_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("function", function_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Function(PbFunction {
                    id: function_id as _,
                    schema_id: function_obj.schema_id.unwrap() as _,
                    database_id: function_obj.database_id.unwrap() as _,
                    ..Default::default()
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn create_secret(
        &self,
        mut pb_secret: PbSecret,
        secret_plain_payload: Vec<u8>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_secret.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_secret.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_secret.schema_id as _, &txn).await?;
        check_secret_name_duplicate(&pb_secret, &txn).await?;

        let secret_obj = Self::create_object(
            &txn,
            ObjectType::Secret,
            owner_id,
            Some(pb_secret.database_id as _),
            Some(pb_secret.schema_id as _),
        )
        .await?;
        pb_secret.id = secret_obj.oid as _;
        let secret: secret::ActiveModel = pb_secret.clone().into();
        Secret::insert(secret).exec(&txn).await?;

        txn.commit().await?;

        // Notify the compute and frontend node plain secret
        let mut secret_plain = pb_secret;
        secret_plain.value.clone_from(&secret_plain_payload);

        LocalSecretManager::global().add_secret(secret_plain.id, secret_plain_payload);
        self.env
            .notification_manager()
            .notify_compute_without_version(Operation::Add, Info::Secret(secret_plain.clone()));

        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Secret(secret_plain),
            )
            .await;

        Ok(version)
    }

    pub async fn alter_secret(
        &self,
        pb_secret: PbSecret,
        secret_plain_payload: Vec<u8>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_secret.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_secret.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_secret.schema_id as _, &txn).await?;

        ensure_object_id(ObjectType::Secret, pb_secret.id as _, &txn).await?;
        let secret: secret::ActiveModel = pb_secret.clone().into();
        Secret::update(secret).exec(&txn).await?;

        txn.commit().await?;

        // Notify the compute and frontend node plain secret
        let mut secret_plain = pb_secret;
        secret_plain.value.clone_from(&secret_plain_payload);

        LocalSecretManager::global().update_secret(secret_plain.id, secret_plain_payload);
        self.env
            .notification_manager()
            .notify_compute_without_version(Operation::Update, Info::Secret(secret_plain.clone()));

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Secret(secret_plain),
            )
            .await;

        Ok(version)
    }

    pub async fn get_secret_by_id(&self, secret_id: SecretId) -> MetaResult<PbSecret> {
        let inner = self.inner.read().await;
        let (secret, obj) = Secret::find_by_id(secret_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("secret", secret_id))?;
        Ok(ObjectModel(secret, obj.unwrap()).into())
    }

    pub async fn drop_secret(&self, secret_id: SecretId) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let (secret, secret_obj) = Secret::find_by_id(secret_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("secret", secret_id))?;
        ensure_object_not_refer(ObjectType::Secret, secret_id, &txn).await?;

        // Find affect users with privileges on the secret.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.eq(secret_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete_by_id(secret_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("secret", secret_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        let pb_secret: PbSecret = ObjectModel(secret, secret_obj.unwrap()).into();

        self.notify_users_update(user_infos).await;

        LocalSecretManager::global().remove_secret(pb_secret.id);
        self.env
            .notification_manager()
            .notify_compute_without_version(Operation::Delete, Info::Secret(pb_secret.clone()));
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Secret(pb_secret),
            )
            .await;
        Ok(version)
    }

    pub async fn create_connection(
        &self,
        mut pb_connection: PbConnection,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_connection.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_connection.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_connection.schema_id as _, &txn).await?;
        check_connection_name_duplicate(&pb_connection, &txn).await?;

        let mut dep_secrets = HashSet::new();
        if let Some(ConnectionInfo::ConnectionParams(params)) = &pb_connection.info {
            dep_secrets.extend(
                params
                    .secret_refs
                    .values()
                    .map(|secret_ref| secret_ref.secret_id),
            );
        }

        let conn_obj = Self::create_object(
            &txn,
            ObjectType::Connection,
            owner_id,
            Some(pb_connection.database_id as _),
            Some(pb_connection.schema_id as _),
        )
        .await?;
        pb_connection.id = conn_obj.oid as _;
        let connection: connection::ActiveModel = pb_connection.clone().into();
        Connection::insert(connection).exec(&txn).await?;

        for secret_id in dep_secrets {
            ObjectDependency::insert(object_dependency::ActiveModel {
                oid: Set(secret_id as _),
                used_by: Set(conn_obj.oid),
                ..Default::default()
            })
            .exec(&txn)
            .await?;
        }

        txn.commit().await?;

        {
            // call meta telemetry here to report the connection creation
            report_event(
                PbTelemetryEventStage::Unspecified,
                "connection_create",
                pb_connection.get_id() as _,
                {
                    pb_connection.info.as_ref().and_then(|info| match info {
                        ConnectionInfo::ConnectionParams(params) => {
                            Some(params.connection_type().as_str_name().to_owned())
                        }
                        _ => None,
                    })
                },
                None,
                None,
            );
        }

        let version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::Connection(pb_connection),
            )
            .await;
        Ok(version)
    }

    pub async fn get_connection_by_id(
        &self,
        connection_id: ConnectionId,
    ) -> MetaResult<PbConnection> {
        let inner = self.inner.read().await;
        let (conn, obj) = Connection::find_by_id(connection_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("connection", connection_id))?;

        Ok(ObjectModel(conn, obj.unwrap()).into())
    }

    pub async fn drop_connection(
        &self,
        connection_id: ConnectionId,
    ) -> MetaResult<(NotificationVersion, PbConnection)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let (conn, conn_obj) = Connection::find_by_id(connection_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("connection", connection_id))?;
        ensure_object_not_refer(ObjectType::Connection, connection_id, &txn).await?;

        // Find affect users with privileges on the connection.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.eq(connection_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete_by_id(connection_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("connection", connection_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        let pb_connection: PbConnection = ObjectModel(conn, conn_obj.unwrap()).into();

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Connection(pb_connection.clone()),
            )
            .await;
        Ok((version, pb_connection))
    }

    pub async fn create_view(&self, mut pb_view: PbView) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let owner_id = pb_view.owner as _;
        let txn = inner.db.begin().await?;
        ensure_user_id(owner_id, &txn).await?;
        ensure_object_id(ObjectType::Database, pb_view.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, pb_view.schema_id as _, &txn).await?;
        check_relation_name_duplicate(
            &pb_view.name,
            pb_view.database_id as _,
            pb_view.schema_id as _,
            &txn,
        )
        .await?;

        let view_obj = Self::create_object(
            &txn,
            ObjectType::View,
            owner_id,
            Some(pb_view.database_id as _),
            Some(pb_view.schema_id as _),
        )
        .await?;
        pb_view.id = view_obj.oid as _;
        let view: view::ActiveModel = pb_view.clone().into();
        View::insert(view).exec(&txn).await?;

        // todo: change `dependent_relations` to `dependent_objects`, which should includes connection and function as well.
        // todo: shall we need to check existence of them Or let database handle it by FOREIGN KEY constraint.
        for obj_id in &pb_view.dependent_relations {
            ObjectDependency::insert(object_dependency::ActiveModel {
                oid: Set(*obj_id as _),
                used_by: Set(view_obj.oid),
                ..Default::default()
            })
            .exec(&txn)
            .await?;
        }

        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Add,
                PbRelationInfo::View(pb_view),
            )
            .await;
        Ok(version)
    }

    pub async fn alter_owner(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        new_owner: UserId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_user_id(new_owner, &txn).await?;

        let obj = Object::find_by_id(object_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        if obj.owner_id == new_owner {
            return Ok(IGNORED_NOTIFICATION_VERSION);
        }
        let mut obj = obj.into_active_model();
        obj.owner_id = Set(new_owner);
        let obj = obj.update(&txn).await?;

        let mut relations = vec![];
        match object_type {
            ObjectType::Database => {
                let db = Database::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("database", object_id))?;

                txn.commit().await?;

                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Database(ObjectModel(db, obj).into()),
                    )
                    .await;
                return Ok(version);
            }
            ObjectType::Schema => {
                let schema = Schema::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("schema", object_id))?;

                txn.commit().await?;

                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Schema(ObjectModel(schema, obj).into()),
                    )
                    .await;
                return Ok(version);
            }
            ObjectType::Table => {
                let table = Table::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;

                // associated source.
                if let Some(associated_source_id) = table.optional_associated_source_id {
                    let src_obj = object::ActiveModel {
                        oid: Set(associated_source_id as _),
                        owner_id: Set(new_owner),
                        ..Default::default()
                    }
                    .update(&txn)
                    .await?;
                    let source = Source::find_by_id(associated_source_id)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| {
                            MetaError::catalog_id_not_found("source", associated_source_id)
                        })?;
                    relations.push(PbRelationInfo::Source(ObjectModel(source, src_obj).into()));
                }

                // indexes.
                let (index_ids, mut table_ids): (Vec<IndexId>, Vec<TableId>) =
                    if table.table_type == TableType::Table {
                        Index::find()
                            .select_only()
                            .columns([index::Column::IndexId, index::Column::IndexTableId])
                            .filter(index::Column::PrimaryTableId.eq(object_id))
                            .into_tuple::<(IndexId, TableId)>()
                            .all(&txn)
                            .await?
                            .into_iter()
                            .unzip()
                    } else {
                        (vec![], vec![])
                    };
                relations.push(PbRelationInfo::Table(ObjectModel(table, obj).into()));

                // internal tables.
                let internal_tables: Vec<TableId> = Table::find()
                    .select_only()
                    .column(table::Column::TableId)
                    .filter(
                        table::Column::BelongsToJobId
                            .is_in(table_ids.iter().cloned().chain(std::iter::once(object_id))),
                    )
                    .into_tuple()
                    .all(&txn)
                    .await?;
                table_ids.extend(internal_tables);

                if !index_ids.is_empty() || !table_ids.is_empty() {
                    Object::update_many()
                        .col_expr(
                            object::Column::OwnerId,
                            SimpleExpr::Value(Value::Int(Some(new_owner))),
                        )
                        .filter(
                            object::Column::Oid
                                .is_in(index_ids.iter().cloned().chain(table_ids.iter().cloned())),
                        )
                        .exec(&txn)
                        .await?;
                }

                if !table_ids.is_empty() {
                    let table_objs = Table::find()
                        .find_also_related(Object)
                        .filter(table::Column::TableId.is_in(table_ids))
                        .all(&txn)
                        .await?;
                    for (table, table_obj) in table_objs {
                        relations.push(PbRelationInfo::Table(
                            ObjectModel(table, table_obj.unwrap()).into(),
                        ));
                    }
                }
                // FIXME: frontend will update index/primary table from cache, requires apply updates of indexes after tables.
                if !index_ids.is_empty() {
                    let index_objs = Index::find()
                        .find_also_related(Object)
                        .filter(index::Column::IndexId.is_in(index_ids))
                        .all(&txn)
                        .await?;
                    for (index, index_obj) in index_objs {
                        relations.push(PbRelationInfo::Index(
                            ObjectModel(index, index_obj.unwrap()).into(),
                        ));
                    }
                }
            }
            ObjectType::Source => {
                let source = Source::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", object_id))?;
                let is_shared = source.is_shared();
                relations.push(PbRelationInfo::Source(ObjectModel(source, obj).into()));

                // Note: For non-shared source, we don't update their state tables, which
                // belongs to the MV.
                if is_shared {
                    update_internal_tables(
                        &txn,
                        object_id,
                        object::Column::OwnerId,
                        Value::Int(Some(new_owner)),
                        &mut relations,
                    )
                    .await?;
                }
            }
            ObjectType::Sink => {
                let sink = Sink::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", object_id))?;
                relations.push(PbRelationInfo::Sink(ObjectModel(sink, obj).into()));

                update_internal_tables(
                    &txn,
                    object_id,
                    object::Column::OwnerId,
                    Value::Int(Some(new_owner)),
                    &mut relations,
                )
                .await?;
            }
            ObjectType::Subscription => {
                let subscription = Subscription::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("subscription", object_id))?;
                relations.push(PbRelationInfo::Subscription(
                    ObjectModel(subscription, obj).into(),
                ));
            }
            ObjectType::View => {
                let view = View::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("view", object_id))?;
                relations.push(PbRelationInfo::View(ObjectModel(view, obj).into()));
            }
            _ => unreachable!("not supported object type: {:?}", object_type),
        };

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::RelationGroup(PbRelationGroup {
                    relations: relations
                        .into_iter()
                        .map(|relation| PbRelation {
                            relation_info: Some(relation),
                        })
                        .collect(),
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn alter_schema(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        new_schema: SchemaId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_object_id(ObjectType::Schema, new_schema, &txn).await?;

        let obj = Object::find_by_id(object_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        if obj.schema_id == Some(new_schema) {
            return Ok(IGNORED_NOTIFICATION_VERSION);
        }
        let database_id = obj.database_id.unwrap();

        let mut relations = vec![];
        match object_type {
            ObjectType::Table => {
                let table = Table::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;
                check_relation_name_duplicate(&table.name, database_id, new_schema, &txn).await?;
                let (associated_src_id, table_type) =
                    (table.optional_associated_source_id, table.table_type);

                let mut obj = obj.into_active_model();
                obj.schema_id = Set(Some(new_schema));
                let obj = obj.update(&txn).await?;
                relations.push(PbRelationInfo::Table(ObjectModel(table, obj).into()));

                // associated source.
                if let Some(associated_source_id) = associated_src_id {
                    let src_obj = object::ActiveModel {
                        oid: Set(associated_source_id as _),
                        schema_id: Set(Some(new_schema)),
                        ..Default::default()
                    }
                    .update(&txn)
                    .await?;
                    let source = Source::find_by_id(associated_source_id)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| {
                            MetaError::catalog_id_not_found("source", associated_source_id)
                        })?;
                    relations.push(PbRelationInfo::Source(ObjectModel(source, src_obj).into()));
                }

                // indexes.
                let (index_ids, (index_names, mut table_ids)): (
                    Vec<IndexId>,
                    (Vec<String>, Vec<TableId>),
                ) = if table_type == TableType::Table {
                    Index::find()
                        .select_only()
                        .columns([
                            index::Column::IndexId,
                            index::Column::Name,
                            index::Column::IndexTableId,
                        ])
                        .filter(index::Column::PrimaryTableId.eq(object_id))
                        .into_tuple::<(IndexId, String, TableId)>()
                        .all(&txn)
                        .await?
                        .into_iter()
                        .map(|(id, name, t_id)| (id, (name, t_id)))
                        .unzip()
                } else {
                    (vec![], (vec![], vec![]))
                };

                // internal tables.
                let internal_tables: Vec<TableId> = Table::find()
                    .select_only()
                    .column(table::Column::TableId)
                    .filter(
                        table::Column::BelongsToJobId
                            .is_in(table_ids.iter().cloned().chain(std::iter::once(object_id))),
                    )
                    .into_tuple()
                    .all(&txn)
                    .await?;
                table_ids.extend(internal_tables);

                if !index_ids.is_empty() || !table_ids.is_empty() {
                    for index_name in index_names {
                        check_relation_name_duplicate(&index_name, database_id, new_schema, &txn)
                            .await?;
                    }

                    Object::update_many()
                        .col_expr(
                            object::Column::SchemaId,
                            SimpleExpr::Value(Value::Int(Some(new_schema))),
                        )
                        .filter(
                            object::Column::Oid
                                .is_in(index_ids.iter().cloned().chain(table_ids.iter().cloned())),
                        )
                        .exec(&txn)
                        .await?;
                }

                if !table_ids.is_empty() {
                    let table_objs = Table::find()
                        .find_also_related(Object)
                        .filter(table::Column::TableId.is_in(table_ids))
                        .all(&txn)
                        .await?;
                    for (table, table_obj) in table_objs {
                        relations.push(PbRelationInfo::Table(
                            ObjectModel(table, table_obj.unwrap()).into(),
                        ));
                    }
                }
                if !index_ids.is_empty() {
                    let index_objs = Index::find()
                        .find_also_related(Object)
                        .filter(index::Column::IndexId.is_in(index_ids))
                        .all(&txn)
                        .await?;
                    for (index, index_obj) in index_objs {
                        relations.push(PbRelationInfo::Index(
                            ObjectModel(index, index_obj.unwrap()).into(),
                        ));
                    }
                }
            }
            ObjectType::Source => {
                let source = Source::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", object_id))?;
                check_relation_name_duplicate(&source.name, database_id, new_schema, &txn).await?;
                let is_shared = source.is_shared();

                let mut obj = obj.into_active_model();
                obj.schema_id = Set(Some(new_schema));
                let obj = obj.update(&txn).await?;
                relations.push(PbRelationInfo::Source(ObjectModel(source, obj).into()));

                // Note: For non-shared source, we don't update their state tables, which
                // belongs to the MV.
                if is_shared {
                    update_internal_tables(
                        &txn,
                        object_id,
                        object::Column::SchemaId,
                        Value::Int(Some(new_schema)),
                        &mut relations,
                    )
                    .await?;
                }
            }
            ObjectType::Sink => {
                let sink = Sink::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", object_id))?;
                check_relation_name_duplicate(&sink.name, database_id, new_schema, &txn).await?;

                let mut obj = obj.into_active_model();
                obj.schema_id = Set(Some(new_schema));
                let obj = obj.update(&txn).await?;
                relations.push(PbRelationInfo::Sink(ObjectModel(sink, obj).into()));

                update_internal_tables(
                    &txn,
                    object_id,
                    object::Column::SchemaId,
                    Value::Int(Some(new_schema)),
                    &mut relations,
                )
                .await?;
            }
            ObjectType::Subscription => {
                let subscription = Subscription::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("subscription", object_id))?;
                check_relation_name_duplicate(&subscription.name, database_id, new_schema, &txn)
                    .await?;

                let mut obj = obj.into_active_model();
                obj.schema_id = Set(Some(new_schema));
                let obj = obj.update(&txn).await?;
                relations.push(PbRelationInfo::Subscription(
                    ObjectModel(subscription, obj).into(),
                ));
            }
            ObjectType::View => {
                let view = View::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("view", object_id))?;
                check_relation_name_duplicate(&view.name, database_id, new_schema, &txn).await?;

                let mut obj = obj.into_active_model();
                obj.schema_id = Set(Some(new_schema));
                let obj = obj.update(&txn).await?;
                relations.push(PbRelationInfo::View(ObjectModel(view, obj).into()));
            }
            ObjectType::Function => {
                let function = Function::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("function", object_id))?;

                let mut pb_function: PbFunction = ObjectModel(function, obj).into();
                pb_function.schema_id = new_schema as _;
                check_function_signature_duplicate(&pb_function, &txn).await?;

                object::ActiveModel {
                    oid: Set(object_id),
                    schema_id: Set(Some(new_schema)),
                    ..Default::default()
                }
                .update(&txn)
                .await?;

                txn.commit().await?;
                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Function(pb_function),
                    )
                    .await;
                return Ok(version);
            }
            ObjectType::Connection => {
                let connection = Connection::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("connection", object_id))?;

                let mut pb_connection: PbConnection = ObjectModel(connection, obj).into();
                pb_connection.schema_id = new_schema as _;
                check_connection_name_duplicate(&pb_connection, &txn).await?;

                object::ActiveModel {
                    oid: Set(object_id),
                    schema_id: Set(Some(new_schema)),
                    ..Default::default()
                }
                .update(&txn)
                .await?;

                txn.commit().await?;
                let version = self
                    .notify_frontend(
                        NotificationOperation::Update,
                        NotificationInfo::Connection(pb_connection),
                    )
                    .await;
                return Ok(version);
            }
            _ => unreachable!("not supported object type: {:?}", object_type),
        }

        txn.commit().await?;
        let version = self
            .notify_frontend(
                Operation::Update,
                Info::RelationGroup(PbRelationGroup {
                    relations: relations
                        .into_iter()
                        .map(|relation_info| PbRelation {
                            relation_info: Some(relation_info),
                        })
                        .collect_vec(),
                }),
            )
            .await;
        Ok(version)
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
                PbRelationInfo::Table(ObjectModel(table, table_obj).into()),
            )
            .await;

        Ok(version)
    }

    pub async fn drop_relation(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        drop_mode: DropMode,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let obj: PartialObject = Object::find_by_id(object_id)
            .into_partial_model()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        assert_eq!(obj.obj_type, object_type);
        let database_id = obj
            .database_id
            .ok_or_else(|| anyhow!("dropped object should have database_id"))?;

        let mut to_drop_objects = match drop_mode {
            DropMode::Cascade => get_referring_objects_cascade(object_id, &txn).await?,
            DropMode::Restrict => {
                ensure_object_not_refer(object_type, object_id, &txn).await?;
                if obj.obj_type == ObjectType::Table {
                    let indexes = get_referring_objects(object_id, &txn).await?;
                    assert!(
                        indexes.iter().all(|obj| obj.obj_type == ObjectType::Index),
                        "only index could be dropped in restrict mode"
                    );
                    indexes
                } else {
                    vec![]
                }
            }
        };
        assert!(
            to_drop_objects.iter().all(|obj| matches!(
                obj.obj_type,
                ObjectType::Table
                    | ObjectType::Index
                    | ObjectType::Sink
                    | ObjectType::View
                    | ObjectType::Subscription
            )),
            "only these objects will depends on others"
        );
        to_drop_objects.push(obj);

        // Special handling for 'sink into table'.
        if object_type != ObjectType::Sink {
            // When dropping a table downstream, all incoming sinks of the table should be dropped as well.
            if object_type == ObjectType::Table {
                let table = Table::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;

                let incoming_sinks = table.incoming_sinks.into_inner();

                if !incoming_sinks.is_empty() {
                    let objs: Vec<PartialObject> = Object::find()
                        .filter(object::Column::Oid.is_in(incoming_sinks))
                        .into_partial_model()
                        .all(&txn)
                        .await?;

                    to_drop_objects.extend(objs);
                }
            }

            let to_drop_object_ids: HashSet<_> =
                to_drop_objects.iter().map(|obj| obj.oid).collect();

            // When there is a table sink in the dependency chain of drop cascade, an error message needs to be returned currently to manually drop the sink.
            for obj in &to_drop_objects {
                if obj.obj_type == ObjectType::Sink {
                    let sink = Sink::find_by_id(obj.oid)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("sink", obj.oid))?;

                    // Since dropping the sink into the table requires the frontend to handle some of the logic (regenerating the plan), it’s not compatible with the current cascade dropping.
                    if let Some(target_table) = sink.target_table
                        && !to_drop_object_ids.contains(&target_table)
                    {
                        bail!(
                            "Found sink into table with sink id {} in dependency, please drop them manually",
                            obj.oid,
                        );
                    }
                }
            }
        }

        let to_drop_table_ids = to_drop_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Table || obj.obj_type == ObjectType::Index)
            .map(|obj| obj.oid);
        let mut to_drop_streaming_jobs = to_drop_objects
            .iter()
            .filter(|obj| {
                obj.obj_type == ObjectType::Table
                    || obj.obj_type == ObjectType::Sink
                    || obj.obj_type == ObjectType::Index
            })
            .map(|obj| obj.oid)
            .collect_vec();

        // source streaming job.
        if object_type == ObjectType::Source {
            let source_info: Option<StreamSourceInfo> = Source::find_by_id(object_id)
                .select_only()
                .column(source::Column::SourceInfo)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("source", object_id))?;
            if let Some(source_info) = source_info
                && source_info.to_protobuf().is_shared()
            {
                to_drop_streaming_jobs.push(object_id);
            }
        }

        let creating = StreamingJob::find()
            .filter(
                streaming_job::Column::JobStatus
                    .ne(JobStatus::Created)
                    .and(streaming_job::Column::JobId.is_in(to_drop_streaming_jobs.clone())),
            )
            .count(&txn)
            .await?;
        if creating != 0 {
            return Err(MetaError::permission_denied(format!(
                "can not drop {creating} creating streaming job, please cancel them firstly"
            )));
        }

        let mut to_drop_state_table_ids = to_drop_table_ids.clone().collect_vec();

        // Add associated sources.
        let mut to_drop_source_ids: Vec<SourceId> = Table::find()
            .select_only()
            .column(table::Column::OptionalAssociatedSourceId)
            .filter(
                table::Column::TableId
                    .is_in(to_drop_table_ids)
                    .and(table::Column::OptionalAssociatedSourceId.is_not_null()),
            )
            .into_tuple()
            .all(&txn)
            .await?;
        let to_drop_source_objs: Vec<PartialObject> = Object::find()
            .filter(object::Column::Oid.is_in(to_drop_source_ids.clone()))
            .into_partial_model()
            .all(&txn)
            .await?;
        to_drop_objects.extend(to_drop_source_objs);
        if object_type == ObjectType::Source {
            to_drop_source_ids.push(object_id);
        }

        if !to_drop_streaming_jobs.is_empty() {
            let to_drop_internal_table_objs: Vec<PartialObject> = Object::find()
                .select_only()
                .columns([
                    object::Column::Oid,
                    object::Column::ObjType,
                    object::Column::SchemaId,
                    object::Column::DatabaseId,
                ])
                .join(JoinType::InnerJoin, object::Relation::Table.def())
                .filter(table::Column::BelongsToJobId.is_in(to_drop_streaming_jobs.clone()))
                .into_partial_model()
                .all(&txn)
                .await?;

            to_drop_state_table_ids.extend(to_drop_internal_table_objs.iter().map(|obj| obj.oid));
            to_drop_objects.extend(to_drop_internal_table_objs);
        }

        let to_drop_objects = to_drop_objects;

        to_drop_objects.iter().for_each(|obj| {
            if let Some(obj_database_id) = obj.database_id {
                assert_eq!(
                    database_id, obj_database_id,
                    "dropped objects not in the same database: {:?}",
                    obj
                );
            }
        });

        let (source_fragments, removed_actors, removed_fragments) =
            resolve_source_register_info_for_jobs(&txn, to_drop_streaming_jobs.clone()).await?;

        let fragment_ids = get_fragment_ids_by_jobs(&txn, to_drop_streaming_jobs.clone()).await?;

        // Find affect users with privileges on all this objects.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .into_tuple()
            .all(&txn)
            .await?;

        // delete all in to_drop_objects.
        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found(
                object_type.as_str(),
                object_id,
            ));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        // notify about them.
        self.notify_users_update(user_infos).await;
        let relation_group = build_relation_group_for_delete(to_drop_objects);

        let version = self
            .notify_frontend(NotificationOperation::Delete, relation_group)
            .await;

        let fragment_mappings = fragment_ids
            .into_iter()
            .map(|fragment_id| PbFragmentWorkerSlotMapping {
                fragment_id: fragment_id as _,
                mapping: None,
            })
            .collect();

        self.notify_fragment_mapping(NotificationOperation::Delete, fragment_mappings)
            .await;

        Ok((
            ReleaseContext {
                database_id,
                streaming_job_ids: to_drop_streaming_jobs,
                state_table_ids: to_drop_state_table_ids,
                source_ids: to_drop_source_ids,
                connections: vec![],
                source_fragments,
                removed_actors,
                removed_fragments,
            },
            version,
        ))
    }

    async fn alter_database_name(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        check_database_name_duplicate(name, &txn).await?;

        let active_model = database::ActiveModel {
            database_id: Set(database_id),
            name: Set(name.to_owned()),
        };
        let database = active_model.update(&txn).await?;

        let obj = Object::find_by_id(database_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("database", database_id))?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Database(ObjectModel(database, obj).into()),
            )
            .await;
        Ok(version)
    }

    async fn alter_schema_name(
        &self,
        schema_id: SchemaId,
        name: &str,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let obj = Object::find_by_id(schema_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("schema", schema_id))?;
        check_schema_name_duplicate(name, obj.database_id.unwrap(), &txn).await?;

        let active_model = schema::ActiveModel {
            schema_id: Set(schema_id),
            name: Set(name.to_owned()),
        };
        let schema = active_model.update(&txn).await?;

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::Schema(ObjectModel(schema, obj).into()),
            )
            .await;
        Ok(version)
    }

    pub async fn alter_name(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        object_name: &str,
    ) -> MetaResult<NotificationVersion> {
        if object_type == ObjectType::Database {
            return self.alter_database_name(object_id as _, object_name).await;
        } else if object_type == ObjectType::Schema {
            return self.alter_schema_name(object_id as _, object_name).await;
        }

        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let obj: PartialObject = Object::find_by_id(object_id)
            .into_partial_model()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        assert_eq!(obj.obj_type, object_type);
        check_relation_name_duplicate(
            object_name,
            obj.database_id.unwrap(),
            obj.schema_id.unwrap(),
            &txn,
        )
        .await?;

        // rename relation.
        let (mut to_update_relations, old_name) =
            rename_relation(&txn, object_type, object_id, object_name).await?;
        // rename referring relation name.
        to_update_relations.extend(
            rename_relation_refer(&txn, object_type, object_id, object_name, &old_name).await?,
        );

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::RelationGroup(PbRelationGroup {
                    relations: to_update_relations,
                }),
            )
            .await;

        Ok(version)
    }

    pub async fn alter_swap_rename(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        dst_object_id: ObjectId,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let dst_name: String = match object_type {
            ObjectType::Table => Table::find_by_id(dst_object_id)
                .select_only()
                .column(table::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                })?,
            ObjectType::Source => Source::find_by_id(dst_object_id)
                .select_only()
                .column(source::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                })?,
            ObjectType::Sink => Sink::find_by_id(dst_object_id)
                .select_only()
                .column(sink::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                })?,
            ObjectType::View => View::find_by_id(dst_object_id)
                .select_only()
                .column(view::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                })?,
            ObjectType::Subscription => Subscription::find_by_id(dst_object_id)
                .select_only()
                .column(subscription::Column::Name)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    MetaError::catalog_id_not_found(object_type.as_str(), dst_object_id)
                })?,
            _ => {
                return Err(MetaError::permission_denied(format!(
                    "swap rename not supported for object type: {:?}",
                    object_type
                )));
            }
        };

        // rename relations.
        let (mut to_update_relations, src_name) =
            rename_relation(&txn, object_type, object_id, &dst_name).await?;
        let (to_update_relations2, _) =
            rename_relation(&txn, object_type, dst_object_id, &src_name).await?;
        to_update_relations.extend(to_update_relations2);
        // rename referring relation name.
        to_update_relations.extend(
            rename_relation_refer(&txn, object_type, object_id, &dst_name, &src_name).await?,
        );
        to_update_relations.extend(
            rename_relation_refer(&txn, object_type, dst_object_id, &src_name, &dst_name).await?,
        );

        txn.commit().await?;

        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::RelationGroup(PbRelationGroup {
                    relations: to_update_relations,
                }),
            )
            .await;

        Ok(version)
    }

    pub async fn list_stream_job_desc_for_telemetry(
        &self,
    ) -> MetaResult<Vec<MetaTelemetryJobDesc>> {
        let inner = self.inner.read().await;
        let info: Vec<(TableId, Option<Property>)> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .column(source::Column::WithProperties)
            .join(JoinType::LeftJoin, table::Relation::Source.def())
            .filter(
                table::Column::TableType
                    .eq(TableType::Table)
                    .or(table::Column::TableType.eq(TableType::MaterializedView)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(info
            .into_iter()
            .map(|(table_id, properties)| {
                let connector_info = if let Some(inner_props) = properties {
                    inner_props
                        .inner_ref()
                        .get(UPSTREAM_SOURCE_KEY)
                        .map(|v| v.to_lowercase())
                } else {
                    None
                };
                MetaTelemetryJobDesc {
                    table_id,
                    connector: connector_info,
                    optimization: vec![],
                }
            })
            .collect())
    }

    pub async fn alter_non_shared_source(
        &self,
        pb_source: PbSource,
    ) -> MetaResult<NotificationVersion> {
        let source_id = pb_source.id as SourceId;
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let original_version: i64 = Source::find_by_id(source_id)
            .select_only()
            .column(source::Column::Version)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
        if original_version + 1 != pb_source.version as i64 {
            return Err(MetaError::permission_denied(
                "source version is stale".to_owned(),
            ));
        }

        let source: source::ActiveModel = pb_source.clone().into();
        source.update(&txn).await?;
        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Update,
                PbRelationInfo::Source(pb_source),
            )
            .await;
        Ok(version)
    }

    pub async fn list_databases(&self) -> MetaResult<Vec<PbDatabase>> {
        let inner = self.inner.read().await;
        inner.list_databases().await
    }

    pub async fn list_schemas(&self) -> MetaResult<Vec<PbSchema>> {
        let inner = self.inner.read().await;
        inner.list_schemas().await
    }

    pub async fn list_all_state_tables(&self) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        inner.list_all_state_tables().await
    }

    pub async fn list_all_state_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        inner.list_all_state_table_ids().await
    }

    pub async fn list_readonly_table_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .and(table::Column::TableType.ne(TableType::Table)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_ids)
    }

    pub async fn list_dml_table_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .and(table::Column::TableType.eq(TableType::Table)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_ids)
    }

    pub async fn list_view_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<ViewId>> {
        let inner = self.inner.read().await;
        let view_ids: Vec<ViewId> = View::find()
            .select_only()
            .column(view::Column::ViewId)
            .join(JoinType::InnerJoin, view::Relation::Object.def())
            .filter(object::Column::SchemaId.eq(schema_id))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(view_ids)
    }

    pub async fn list_tables_by_type(&self, table_type: TableType) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableType.eq(table_type))
            .all(&inner.db)
            .await?;
        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    pub async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        let inner = self.inner.read().await;
        inner.list_sources().await
    }

    // Return a hashmap to distinguish whether each source is shared or not.
    pub async fn list_source_id_with_shared_types(&self) -> MetaResult<HashMap<SourceId, bool>> {
        let inner = self.inner.read().await;
        let source_ids: Vec<(SourceId, Option<StreamSourceInfo>)> = Source::find()
            .select_only()
            .columns([source::Column::SourceId, source::Column::SourceInfo])
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(source_ids
            .into_iter()
            .map(|(source_id, info)| {
                (
                    source_id,
                    info.map(|info| info.to_protobuf().cdc_source_job)
                        .unwrap_or(false),
                )
            })
            .collect())
    }

    pub async fn list_source_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<SourceId>> {
        let inner = self.inner.read().await;
        let source_ids: Vec<SourceId> = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .join(JoinType::InnerJoin, source::Relation::Object.def())
            .filter(object::Column::SchemaId.eq(schema_id))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(source_ids)
    }

    pub async fn list_sinks(&self) -> MetaResult<Vec<PbSink>> {
        let inner = self.inner.read().await;
        inner.list_sinks().await
    }

    pub async fn list_subscriptions(&self) -> MetaResult<Vec<PbSubscription>> {
        let inner = self.inner.read().await;
        inner.list_subscriptions().await
    }

    pub async fn list_views(&self) -> MetaResult<Vec<PbView>> {
        let inner = self.inner.read().await;
        inner.list_views().await
    }

    pub async fn list_users(&self) -> MetaResult<Vec<PbUserInfo>> {
        let inner = self.inner.read().await;
        inner.list_users().await
    }

    pub async fn get_table_by_name(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> MetaResult<Option<PbTable>> {
        let inner = self.inner.read().await;
        let table_obj = Table::find()
            .find_also_related(Object)
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .filter(
                table::Column::Name
                    .eq(table_name)
                    .and(database::Column::Name.eq(database_name)),
            )
            .one(&inner.db)
            .await?;
        Ok(table_obj.map(|(table, obj)| ObjectModel(table, obj.unwrap()).into()))
    }

    pub async fn get_table_by_ids(&self, table_ids: Vec<TableId>) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableId.is_in(table_ids))
            .all(&inner.db)
            .await?;
        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    pub async fn get_sink_by_ids(&self, sink_ids: Vec<SinkId>) -> MetaResult<Vec<PbSink>> {
        let inner = self.inner.read().await;
        let sink_objs = Sink::find()
            .find_also_related(Object)
            .filter(sink::Column::SinkId.is_in(sink_ids))
            .all(&inner.db)
            .await?;
        Ok(sink_objs
            .into_iter()
            .map(|(sink, obj)| ObjectModel(sink, obj.unwrap()).into())
            .collect())
    }

    pub async fn get_subscription_by_id(
        &self,
        subscription_id: SubscriptionId,
    ) -> MetaResult<PbSubscription> {
        let inner = self.inner.read().await;
        let subscription_objs = Subscription::find()
            .find_also_related(Object)
            .filter(subscription::Column::SubscriptionId.eq(subscription_id))
            .all(&inner.db)
            .await?;
        let subscription: PbSubscription = subscription_objs
            .into_iter()
            .map(|(subscription, obj)| ObjectModel(subscription, obj.unwrap()).into())
            .find_or_first(|_| true)
            .ok_or_else(|| anyhow!("cannot find subscription with id {}", subscription_id))?;

        Ok(subscription)
    }

    pub async fn get_mv_depended_subscriptions(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<SubscriptionId, u64>>>> {
        let inner = self.inner.read().await;
        let select = Subscription::find()
            .select_only()
            .select_column(subscription::Column::SubscriptionId)
            .select_column(subscription::Column::DependentTableId)
            .select_column(subscription::Column::RetentionSeconds)
            .select_column(object::Column::DatabaseId)
            .join(JoinType::InnerJoin, subscription::Relation::Object.def());
        let select = if let Some(database_id) = database_id {
            select.filter(object::Column::DatabaseId.eq(database_id))
        } else {
            select
        };
        let subscription_objs: Vec<(SubscriptionId, ObjectId, i64, DatabaseId)> =
            select.into_tuple().all(&inner.db).await?;
        let mut map: HashMap<_, HashMap<_, HashMap<_, _>>> = HashMap::new();
        // Write object at the same time we write subscription, so we must be able to get obj
        for (subscription_id, dependent_table_id, retention_seconds, database_id) in
            subscription_objs
        {
            map.entry(database_id)
                .or_default()
                .entry(dependent_table_id)
                .or_default()
                .insert(subscription_id, retention_seconds as _);
        }
        Ok(map)
    }

    pub async fn find_creating_streaming_job_ids(
        &self,
        infos: Vec<PbCreatingJobInfo>,
    ) -> MetaResult<Vec<ObjectId>> {
        let inner = self.inner.read().await;

        type JobKey = (DatabaseId, SchemaId, String);

        // Index table is already included if we still assign the same name for index table as the index.
        let creating_tables: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_sinks: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Sink::find()
            .select_only()
            .columns([sink::Column::SinkId, sink::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, sink::Relation::Object.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_subscriptions: Vec<(ObjectId, String, DatabaseId, SchemaId)> =
            Subscription::find()
                .select_only()
                .columns([
                    subscription::Column::SubscriptionId,
                    subscription::Column::Name,
                ])
                .columns([object::Column::DatabaseId, object::Column::SchemaId])
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
                .into_tuple()
                .all(&inner.db)
                .await?;

        let mut job_mapping: HashMap<JobKey, ObjectId> = creating_tables
            .into_iter()
            .chain(creating_sinks.into_iter())
            .chain(creating_subscriptions.into_iter())
            .map(|(id, name, database_id, schema_id)| ((database_id, schema_id, name), id))
            .collect();

        Ok(infos
            .into_iter()
            .flat_map(|info| {
                job_mapping.remove(&(
                    info.database_id as _,
                    info.schema_id as _,
                    info.name.clone(),
                ))
            })
            .collect())
    }

    pub async fn list_connections(&self) -> MetaResult<Vec<PbConnection>> {
        let inner = self.inner.read().await;
        let conn_objs = Connection::find()
            .find_also_related(Object)
            .all(&inner.db)
            .await?;
        Ok(conn_objs
            .into_iter()
            .map(|(conn, obj)| ObjectModel(conn, obj.unwrap()).into())
            .collect())
    }

    pub async fn get_all_table_options(&self) -> MetaResult<HashMap<TableId, TableOption>> {
        let inner = self.inner.read().await;
        let table_options: Vec<(TableId, Option<i32>)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::RetentionSeconds])
            .into_tuple::<(TableId, Option<i32>)>()
            .all(&inner.db)
            .await?;

        Ok(table_options
            .into_iter()
            .map(|(id, retention_seconds)| {
                (
                    id,
                    TableOption {
                        retention_seconds: retention_seconds.map(|i| i.try_into().unwrap()),
                    },
                )
            })
            .collect())
    }

    pub async fn get_all_created_streaming_parallelisms(
        &self,
    ) -> MetaResult<HashMap<ObjectId, StreamingParallelism>> {
        let inner = self.inner.read().await;

        let job_parallelisms = StreamingJob::find()
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .select_only()
            .columns([
                streaming_job::Column::JobId,
                streaming_job::Column::Parallelism,
            ])
            .into_tuple::<(ObjectId, StreamingParallelism)>()
            .all(&inner.db)
            .await?;

        Ok(job_parallelisms
            .into_iter()
            .collect::<HashMap<ObjectId, StreamingParallelism>>())
    }

    pub async fn get_table_name_type_mapping(
        &self,
    ) -> MetaResult<HashMap<TableId, (String, String)>> {
        let inner = self.inner.read().await;
        let table_name_types: Vec<(TableId, String, TableType)> = Table::find()
            .select_only()
            .columns([
                table::Column::TableId,
                table::Column::Name,
                table::Column::TableType,
            ])
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_name_types
            .into_iter()
            .map(|(id, name, table_type)| {
                (
                    id,
                    (name, PbTableType::from(table_type).as_str_name().to_owned()),
                )
            })
            .collect())
    }

    pub async fn get_table_by_cdc_table_id(
        &self,
        cdc_table_id: &String,
    ) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::CdcTableId.eq(cdc_table_id))
            .all(&inner.db)
            .await?;
        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    pub async fn get_created_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;

        // created table ids.
        let mut table_ids: Vec<TableId> = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .into_tuple()
            .all(&inner.db)
            .await?;

        // internal table ids.
        let internal_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(table::Column::BelongsToJobId.is_in(table_ids.clone()))
            .into_tuple()
            .all(&inner.db)
            .await?;
        table_ids.extend(internal_table_ids);

        Ok(table_ids)
    }

    /// Returns column ids of versioned tables.
    /// Being versioned implies using `ColumnAwareSerde`.
    pub async fn get_versioned_table_schemas(&self) -> MetaResult<HashMap<TableId, Vec<i32>>> {
        let res = self
            .list_all_state_tables()
            .await?
            .into_iter()
            .filter_map(|t| {
                if t.version.is_some() {
                    let ret = (
                        t.id.try_into().unwrap(),
                        t.columns
                            .iter()
                            .map(|c| c.column_desc.as_ref().unwrap().column_id)
                            .collect_vec(),
                    );
                    return Some(ret);
                }
                None
            })
            .collect();
        Ok(res)
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

    /// `list_all_tables` return all ids of state tables.
    pub async fn list_all_state_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .into_tuple()
            .all(&self.db)
            .await?;
        Ok(table_ids)
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
}

async fn update_internal_tables(
    txn: &DatabaseTransaction,
    object_id: i32,
    column: object::Column,
    new_value: Value,
    relations_to_notify: &mut Vec<PbRelationInfo>,
) -> MetaResult<()> {
    let internal_tables = get_internal_tables_by_id(object_id, txn).await?;

    if !internal_tables.is_empty() {
        Object::update_many()
            .col_expr(column, SimpleExpr::Value(new_value))
            .filter(object::Column::Oid.is_in(internal_tables.clone()))
            .exec(txn)
            .await?;

        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableId.is_in(internal_tables))
            .all(txn)
            .await?;
        for (table, table_obj) in table_objs {
            relations_to_notify.push(PbRelationInfo::Table(
                ObjectModel(table, table_obj.unwrap()).into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use risingwave_pb::catalog::StreamSourceInfo;

    use super::*;

    const TEST_DATABASE_ID: DatabaseId = 1;
    const TEST_SCHEMA_ID: SchemaId = 2;
    const TEST_OWNER_ID: UserId = 1;

    #[tokio::test]
    async fn test_database_func() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_database = PbDatabase {
            name: "db1".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        };
        mgr.create_database(pb_database).await?;

        let database_id: DatabaseId = Database::find()
            .select_only()
            .column(database::Column::DatabaseId)
            .filter(database::Column::Name.eq("db1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Database, database_id, "db2")
            .await?;
        let database = Database::find_by_id(database_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(database.name, "db2");

        mgr.drop_database(database_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_schema_func() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_schema = PbSchema {
            database_id: TEST_DATABASE_ID as _,
            name: "schema1".to_owned(),
            owner: TEST_OWNER_ID as _,
            ..Default::default()
        };
        mgr.create_schema(pb_schema.clone()).await?;
        assert!(mgr.create_schema(pb_schema).await.is_err());

        let schema_id: SchemaId = Schema::find()
            .select_only()
            .column(schema::Column::SchemaId)
            .filter(schema::Column::Name.eq("schema1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Schema, schema_id, "schema2")
            .await?;
        let schema = Schema::find_by_id(schema_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(schema.name, "schema2");
        mgr.drop_schema(schema_id, DropMode::Restrict).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_view() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "view".to_owned(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW view AS SELECT 1".to_owned(),
            ..Default::default()
        };
        mgr.create_view(pb_view.clone()).await?;
        assert!(mgr.create_view(pb_view).await.is_err());

        let view = View::find().one(&mgr.inner.read().await.db).await?.unwrap();
        mgr.drop_relation(ObjectType::View, view.view_id, DropMode::Cascade)
            .await?;
        assert!(View::find_by_id(view.view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_create_function() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let test_data_type = risingwave_pb::data::DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Int32 as _,
            ..Default::default()
        };
        let arg_types = vec![test_data_type.clone()];
        let pb_function = PbFunction {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "test_function".to_owned(),
            owner: TEST_OWNER_ID as _,
            arg_types,
            return_type: Some(test_data_type.clone()),
            language: "python".to_owned(),
            kind: Some(risingwave_pb::catalog::function::Kind::Scalar(
                Default::default(),
            )),
            ..Default::default()
        };
        mgr.create_function(pb_function.clone()).await?;
        assert!(mgr.create_function(pb_function).await.is_err());

        let function = Function::find()
            .inner_join(Object)
            .filter(
                object::Column::DatabaseId
                    .eq(TEST_DATABASE_ID)
                    .and(object::Column::SchemaId.eq(TEST_SCHEMA_ID))
                    .add(function::Column::Name.eq("test_function")),
            )
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(function.return_type.to_protobuf(), test_data_type);
        assert_eq!(function.arg_types.to_protobuf().len(), 1);
        assert_eq!(function.language, "python");

        mgr.drop_function(function.function_id).await?;
        assert!(Object::find_by_id(function.function_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_alter_relation_rename() -> MetaResult<()> {
        let mgr = CatalogController::new(MetaSrvEnv::for_test().await).await?;
        let pb_source = PbSource {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "s1".to_owned(),
            owner: TEST_OWNER_ID as _,
            definition: r#"CREATE SOURCE s1 (v1 int) with (
  connector = 'kafka',
  topic = 'kafka_alter',
  properties.bootstrap.server = 'message_queue:29092',
  scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON"#
                .to_owned(),
            info: Some(StreamSourceInfo {
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.create_source(pb_source).await?;
        let source_id: SourceId = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .filter(source::Column::Name.eq("s1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        let pb_view = PbView {
            schema_id: TEST_SCHEMA_ID as _,
            database_id: TEST_DATABASE_ID as _,
            name: "view_1".to_owned(),
            owner: TEST_OWNER_ID as _,
            sql: "CREATE VIEW view_1 AS SELECT v1 FROM s1".to_owned(),
            dependent_relations: vec![source_id as _],
            ..Default::default()
        };
        mgr.create_view(pb_view).await?;
        let view_id: ViewId = View::find()
            .select_only()
            .column(view::Column::ViewId)
            .filter(view::Column::Name.eq("view_1"))
            .into_tuple()
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();

        mgr.alter_name(ObjectType::Source, source_id, "s2").await?;
        let source = Source::find_by_id(source_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(source.name, "s2");
        assert_eq!(
            source.definition,
            "CREATE SOURCE \"s2\" (v1 INT) WITH (\
  connector = 'kafka', \
  topic = 'kafka_alter', \
  properties.bootstrap.server = 'message_queue:29092', \
  scan.startup.mode = 'earliest'\
) FORMAT PLAIN ENCODE JSON"
        );

        let view = View::find_by_id(view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .unwrap();
        assert_eq!(
            view.definition,
            "CREATE VIEW view_1 AS SELECT v1 FROM \"s2\" AS \"s1\""
        );

        mgr.drop_relation(ObjectType::Source, source_id, DropMode::Cascade)
            .await?;
        assert!(View::find_by_id(view_id)
            .one(&mgr.inner.read().await.db)
            .await?
            .is_none());

        Ok(())
    }
}
