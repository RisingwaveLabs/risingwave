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

impl CatalogController {
    // drop table associated source is a special case of drop relation, which just remove the source object and associated state table, keeping the streaming job and fragments.
    pub async fn drop_table_associated_source(
        &self,
        txn: &DatabaseTransaction,
        release_ctx: &ReleaseContext,
    ) -> MetaResult<(Vec<PbUserInfo>, Vec<PartialObject>)> {
        {
            // alter the original table to remove the ref to the state table catalog
            let table_id = *release_ctx.removed_streaming_job_ids.first().unwrap();
            Table::update(table::ActiveModel {
                table_id: Set(table_id as _),
                optional_associated_source_id: Set(None),
                ..Default::default()
            })
            .exec(txn)
            .await?;
        }

        let to_drop_source_objects: Vec<PartialObject> = Object::find()
            .filter(object::Column::Oid.is_in(release_ctx.removed_source_ids.clone()))
            .into_partial_model()
            .all(txn)
            .await?;
        let to_drop_internal_table_objs: Vec<PartialObject> = Object::find()
            .select_only()
            .columns([
                object::Column::Oid,
                object::Column::ObjType,
                object::Column::SchemaId,
                object::Column::DatabaseId,
            ])
            .filter(object::Column::Oid.is_in(release_ctx.removed_state_table_ids.clone()))
            .into_partial_model()
            .all(txn)
            .await?;
        let to_drop_objects = to_drop_source_objects
            .into_iter()
            .chain(to_drop_internal_table_objs.into_iter())
            .collect_vec();
        // Find affect users with privileges on all this objects.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .into_tuple()
            .all(txn)
            .await?;

        tracing::info!(
            "drop_table_associated_source: to_drop_objects: {:?}",
            to_drop_objects
        );

        // delete all in to_drop_objects.
        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .exec(txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found(
                ObjectType::Source.as_str(),
                release_ctx.removed_source_ids.first().unwrap(),
            ));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, txn).await?;

        Ok((user_infos, to_drop_objects))
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

        let (removed_source_fragments, removed_actors, removed_fragments) =
            get_fragments_for_jobs(&txn, to_drop_streaming_jobs.clone()).await?;

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

        let fragment_mappings = removed_fragments
            .iter()
            .map(|fragment_id| PbFragmentWorkerSlotMapping {
                fragment_id: *fragment_id as _,
                mapping: None,
            })
            .collect();

        self.notify_fragment_mapping(NotificationOperation::Delete, fragment_mappings)
            .await;

        Ok((
            ReleaseContext {
                database_id,
                removed_streaming_job_ids: to_drop_streaming_jobs,
                removed_state_table_ids: to_drop_state_table_ids,
                removed_source_ids: to_drop_source_ids,
                removed_source_fragments,
                removed_actors,
                removed_fragments,
            },
            version,
        ))
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

        let (removed_source_fragments, removed_actors, removed_fragments) =
            get_fragments_for_jobs(&txn, streaming_jobs.clone()).await?;

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
                removed_streaming_job_ids: streaming_jobs,
                removed_state_table_ids: state_table_ids,
                removed_source_ids: source_ids,
                removed_source_fragments,
                removed_actors,
                removed_fragments,
            },
            version,
        ))
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
}
