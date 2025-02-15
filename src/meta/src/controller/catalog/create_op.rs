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

use super::*;
use crate::barrier::SnapshotBackfillInfo;

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
                PbObjectInfo::Source(pb_source),
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
            .notify_frontend_relation_info(NotificationOperation::Add, PbObjectInfo::View(pb_view))
            .await;
        Ok(version)
    }

    pub async fn validate_cross_db_snapshot_backfill(
        &self,
        cross_db_snapshot_backfill_info: &SnapshotBackfillInfo,
    ) -> MetaResult<()> {
        if cross_db_snapshot_backfill_info
            .upstream_mv_table_id_to_backfill_epoch
            .is_empty()
        {
            return Ok(());
        }

        let inner = self.inner.read().await;
        let table_ids = cross_db_snapshot_backfill_info
            .upstream_mv_table_id_to_backfill_epoch
            .keys()
            .map(|t| t.table_id as ObjectId)
            .collect_vec();
        let cnt = Subscription::find()
            .select_only()
            .column(subscription::Column::DependentTableId)
            .distinct()
            .filter(subscription::Column::DependentTableId.is_in(table_ids))
            .count(&inner.db)
            .await? as usize;

        if cnt
            < cross_db_snapshot_backfill_info
                .upstream_mv_table_id_to_backfill_epoch
                .keys()
                .count()
        {
            return Err(MetaError::permission_denied(
                "Some upstream tables are not subscribed".to_owned(),
            ));
        }

        Ok(())
    }
}
