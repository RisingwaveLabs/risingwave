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
use crate::controller::utils::{get_database_resource_group, get_existing_job_resource_group};

impl CatalogController {
    pub async fn get_secret_by_id(&self, secret_id: SecretId) -> MetaResult<PbSecret> {
        let inner = self.inner.read().await;
        let (secret, obj) = Secret::find_by_id(secret_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("secret", secret_id))?;
        Ok(ObjectModel(secret, obj.unwrap()).into())
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

    pub async fn get_table_associated_source_id(
        &self,
        table_id: TableId,
    ) -> MetaResult<Option<SourceId>> {
        let inner = self.inner.read().await;
        Table::find_by_id(table_id)
            .select_only()
            .select_column(table::Column::OptionalAssociatedSourceId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))
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

    pub async fn get_all_streaming_parallelisms(
        &self,
    ) -> MetaResult<HashMap<ObjectId, StreamingParallelism>> {
        let inner = self.inner.read().await;

        let job_parallelisms = StreamingJob::find()
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

    pub async fn get_existing_job_resource_group(
        &self,
        streaming_job_id: ObjectId,
    ) -> MetaResult<String> {
        let inner = self.inner.read().await;
        get_existing_job_resource_group(&inner.db, streaming_job_id).await
    }

    pub async fn get_database_resource_group(&self, database_id: ObjectId) -> MetaResult<String> {
        let inner = self.inner.read().await;
        get_database_resource_group(&inner.db, database_id).await
    }

    pub async fn get_existing_job_resource_groups(
        &self,
        streaming_job_ids: Vec<ObjectId>,
    ) -> MetaResult<HashMap<ObjectId, String>> {
        let inner = self.inner.read().await;
        let mut resource_groups = HashMap::new();
        for job_id in streaming_job_ids {
            let resource_group = get_existing_job_resource_group(&inner.db, job_id).await?;
            resource_groups.insert(job_id, resource_group);
        }

        Ok(resource_groups)
    }

    pub async fn get_existing_job_database_resource_group(
        &self,
        streaming_job_id: ObjectId,
    ) -> MetaResult<String> {
        let inner = self.inner.read().await;
        let database_id: ObjectId = StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .column(object::Column::DatabaseId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", streaming_job_id))?;

        get_database_resource_group(&inner.db, database_id).await
    }

    pub async fn get_job_streaming_parallelisms(
        &self,
        streaming_job_id: ObjectId,
    ) -> MetaResult<StreamingParallelism> {
        let inner = self.inner.read().await;

        let job_parallelism: StreamingParallelism = StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .column(streaming_job::Column::Parallelism)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", streaming_job_id))?;

        Ok(job_parallelism)
    }
}
