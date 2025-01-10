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

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::anyhow;
use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{RawRwLock, RwLock};
use risingwave_common::catalog::{CatalogVersion, FunctionId, IndexId, ObjectId};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_pb::catalog::{
    PbComment, PbCreateType, PbDatabase, PbFunction, PbIndex, PbSchema, PbSink, PbSource,
    PbSubscription, PbTable, PbView,
};
use risingwave_pb::ddl_service::replace_job_plan::{ReplaceJob, ReplaceSource, ReplaceTable};
use risingwave_pb::ddl_service::{
    alter_name_request, alter_owner_request, alter_set_schema_request, alter_swap_rename_request,
    create_connection_request, PbReplaceJobPlan, PbTableJobType, ReplaceJobPlan, TableJobType,
    WaitVersion,
};
use risingwave_pb::meta::PbTableParallelism;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_rpc_client::MetaClient;
use tokio::sync::watch::Receiver;

use super::root_catalog::Catalog;
use super::{DatabaseId, SecretId, TableId};
use crate::error::Result;
use crate::scheduler::HummockSnapshotManagerRef;
use crate::user::UserId;

pub type CatalogReadGuard = ArcRwLockReadGuard<RawRwLock, Catalog>;

/// [`CatalogReader`] can read catalog from local catalog and force the holder can not modify it.
#[derive(Clone)]
pub struct CatalogReader(Arc<RwLock<Catalog>>);

impl CatalogReader {
    pub fn new(inner: Arc<RwLock<Catalog>>) -> Self {
        CatalogReader(inner)
    }

    pub fn read_guard(&self) -> CatalogReadGuard {
        // Make this recursive so that one can get this guard in the same thread without fear.
        self.0.read_arc_recursive()
    }
}

/// [`CatalogWriter`] initiate DDL operations (create table/schema/database/function/connection).
/// It will only send rpc to meta and get the catalog version as response.
/// Then it will wait for the local catalog to be synced to the version, which is performed by
/// [observer](`crate::observer::FrontendObserverNode`).
#[async_trait::async_trait]
pub trait CatalogWriter: Send + Sync {
    async fn create_database(&self, db_name: &str, owner: UserId) -> Result<()>;

    async fn create_schema(
        &self,
        db_id: DatabaseId,
        schema_name: &str,
        owner: UserId,
    ) -> Result<()>;

    async fn create_view(&self, view: PbView) -> Result<()>;

    async fn create_materialized_view(
        &self,
        table: PbTable,
        graph: StreamFragmentGraph,
        dependencies: HashSet<ObjectId>,
    ) -> Result<()>;

    async fn create_table(
        &self,
        source: Option<PbSource>,
        table: PbTable,
        graph: StreamFragmentGraph,
        job_type: PbTableJobType,
    ) -> Result<()>;

    async fn replace_table(
        &self,
        source: Option<PbSource>,
        table: PbTable,
        graph: StreamFragmentGraph,
        mapping: ColIndexMapping,
        job_type: TableJobType,
    ) -> Result<()>;

    async fn replace_source(
        &self,
        source: PbSource,
        graph: StreamFragmentGraph,
        mapping: ColIndexMapping,
    ) -> Result<()>;

    async fn create_index(
        &self,
        index: PbIndex,
        table: PbTable,
        graph: StreamFragmentGraph,
    ) -> Result<()>;

    async fn create_source(
        &self,
        source: PbSource,
        graph: Option<StreamFragmentGraph>,
    ) -> Result<()>;

    async fn create_sink(
        &self,
        sink: PbSink,
        graph: StreamFragmentGraph,
        affected_table_change: Option<PbReplaceJobPlan>,
        dependencies: HashSet<ObjectId>,
    ) -> Result<()>;

    async fn create_subscription(&self, subscription: PbSubscription) -> Result<()>;

    async fn create_function(&self, function: PbFunction) -> Result<()>;

    async fn create_connection(
        &self,
        connection_name: String,
        database_id: u32,
        schema_id: u32,
        owner_id: u32,
        connection: create_connection_request::Payload,
    ) -> Result<()>;

    async fn create_secret(
        &self,
        secret_name: String,
        database_id: u32,
        schema_id: u32,
        owner_id: u32,
        payload: Vec<u8>,
    ) -> Result<()>;

    async fn comment_on(&self, comment: PbComment) -> Result<()>;

    async fn drop_table(
        &self,
        source_id: Option<u32>,
        table_id: TableId,
        cascade: bool,
    ) -> Result<()>;

    async fn drop_materialized_view(&self, table_id: TableId, cascade: bool) -> Result<()>;

    async fn drop_view(&self, view_id: u32, cascade: bool) -> Result<()>;

    async fn drop_source(&self, source_id: u32, cascade: bool) -> Result<()>;

    async fn drop_sink(
        &self,
        sink_id: u32,
        cascade: bool,
        affected_table_change: Option<PbReplaceJobPlan>,
    ) -> Result<()>;

    async fn drop_subscription(&self, subscription_id: u32, cascade: bool) -> Result<()>;

    async fn drop_database(&self, database_id: u32) -> Result<()>;

    async fn drop_schema(&self, schema_id: u32, cascade: bool) -> Result<()>;

    async fn drop_index(&self, index_id: IndexId, cascade: bool) -> Result<()>;

    async fn drop_function(&self, function_id: FunctionId) -> Result<()>;

    async fn drop_connection(&self, connection_id: u32) -> Result<()>;

    async fn drop_secret(&self, secret_id: SecretId) -> Result<()>;

    async fn alter_secret(
        &self,
        secret_id: u32,
        secret_name: String,
        database_id: u32,
        schema_id: u32,
        owner_id: u32,
        payload: Vec<u8>,
    ) -> Result<()>;

    async fn alter_name(
        &self,
        object_id: alter_name_request::Object,
        object_name: &str,
    ) -> Result<()>;

    async fn alter_owner(&self, object: alter_owner_request::Object, owner_id: u32) -> Result<()>;

    /// Replace the source in the catalog.
    async fn alter_source(&self, source: PbSource) -> Result<()>;

    async fn alter_parallelism(
        &self,
        table_id: u32,
        parallelism: PbTableParallelism,
        deferred: bool,
    ) -> Result<()>;

    async fn alter_set_schema(
        &self,
        object: alter_set_schema_request::Object,
        new_schema_id: u32,
    ) -> Result<()>;

    async fn alter_swap_rename(&self, object: alter_swap_rename_request::Object) -> Result<()>;
}

#[derive(Clone)]
pub struct CatalogWriterImpl {
    meta_client: MetaClient,
    catalog_updated_rx: Receiver<CatalogVersion>,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
}

#[async_trait::async_trait]
impl CatalogWriter for CatalogWriterImpl {
    async fn create_database(&self, db_name: &str, owner: UserId) -> Result<()> {
        let version = self
            .meta_client
            .create_database(PbDatabase {
                name: db_name.to_owned(),
                id: 0,
                owner,
            })
            .await?;
        self.wait_version(version).await
    }

    async fn create_schema(
        &self,
        db_id: DatabaseId,
        schema_name: &str,
        owner: UserId,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_schema(PbSchema {
                id: 0,
                name: schema_name.to_owned(),
                database_id: db_id,
                owner,
            })
            .await?;
        self.wait_version(version).await
    }

    // TODO: maybe here to pass a materialize plan node
    async fn create_materialized_view(
        &self,
        table: PbTable,
        graph: StreamFragmentGraph,
        dependencies: HashSet<ObjectId>,
    ) -> Result<()> {
        let create_type = table.get_create_type().unwrap_or(PbCreateType::Foreground);
        let version = self
            .meta_client
            .create_materialized_view(table, graph, dependencies)
            .await?;
        if matches!(create_type, PbCreateType::Foreground) {
            self.wait_version(version).await?
        }
        Ok(())
    }

    async fn create_view(&self, view: PbView) -> Result<()> {
        let version = self.meta_client.create_view(view).await?;
        self.wait_version(version).await
    }

    async fn create_index(
        &self,
        index: PbIndex,
        table: PbTable,
        graph: StreamFragmentGraph,
    ) -> Result<()> {
        let version = self.meta_client.create_index(index, table, graph).await?;
        self.wait_version(version).await
    }

    async fn create_table(
        &self,
        source: Option<PbSource>,
        table: PbTable,
        graph: StreamFragmentGraph,
        job_type: PbTableJobType,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_table(source, table, graph, job_type)
            .await?;
        self.wait_version(version).await
    }

    async fn replace_table(
        &self,
        source: Option<PbSource>,
        table: PbTable,
        graph: StreamFragmentGraph,
        mapping: ColIndexMapping,
        job_type: TableJobType,
    ) -> Result<()> {
        let version = self
            .meta_client
            .replace_job(
                graph,
                mapping,
                ReplaceJob::ReplaceTable(ReplaceTable {
                    source,
                    table: Some(table),
                    job_type: job_type as _,
                }),
            )
            .await?;
        self.wait_version(version).await
    }

    async fn replace_source(
        &self,
        source: PbSource,
        graph: StreamFragmentGraph,
        mapping: ColIndexMapping,
    ) -> Result<()> {
        let version = self
            .meta_client
            .replace_job(
                graph,
                mapping,
                ReplaceJob::ReplaceSource(ReplaceSource {
                    source: Some(source),
                }),
            )
            .await?;
        self.wait_version(version).await
    }

    async fn create_source(
        &self,
        source: PbSource,
        graph: Option<StreamFragmentGraph>,
    ) -> Result<()> {
        let version = self.meta_client.create_source(source, graph).await?;
        self.wait_version(version).await
    }

    async fn create_sink(
        &self,
        sink: PbSink,
        graph: StreamFragmentGraph,
        affected_table_change: Option<ReplaceJobPlan>,
        dependencies: HashSet<ObjectId>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_sink(sink, graph, affected_table_change, dependencies)
            .await?;
        self.wait_version(version).await
    }

    async fn create_subscription(&self, subscription: PbSubscription) -> Result<()> {
        let version = self.meta_client.create_subscription(subscription).await?;
        self.wait_version(version).await
    }

    async fn create_function(&self, function: PbFunction) -> Result<()> {
        let version = self.meta_client.create_function(function).await?;
        self.wait_version(version).await
    }

    async fn create_connection(
        &self,
        connection_name: String,
        database_id: u32,
        schema_id: u32,
        owner_id: u32,
        connection: create_connection_request::Payload,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_connection(
                connection_name,
                database_id,
                schema_id,
                owner_id,
                connection,
            )
            .await?;
        self.wait_version(version).await
    }

    async fn create_secret(
        &self,
        secret_name: String,
        database_id: u32,
        schema_id: u32,
        owner_id: u32,
        payload: Vec<u8>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .create_secret(secret_name, database_id, schema_id, owner_id, payload)
            .await?;
        self.wait_version(version).await
    }

    async fn comment_on(&self, comment: PbComment) -> Result<()> {
        let version = self.meta_client.comment_on(comment).await?;
        self.wait_version(version).await
    }

    async fn drop_table(
        &self,
        source_id: Option<u32>,
        table_id: TableId,
        cascade: bool,
    ) -> Result<()> {
        let version = self
            .meta_client
            .drop_table(source_id, table_id, cascade)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_materialized_view(&self, table_id: TableId, cascade: bool) -> Result<()> {
        let version = self
            .meta_client
            .drop_materialized_view(table_id, cascade)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_view(&self, view_id: u32, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_view(view_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn drop_source(&self, source_id: u32, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_source(source_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn drop_sink(
        &self,
        sink_id: u32,
        cascade: bool,
        affected_table_change: Option<ReplaceJobPlan>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .drop_sink(sink_id, cascade, affected_table_change)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_subscription(&self, subscription_id: u32, cascade: bool) -> Result<()> {
        let version = self
            .meta_client
            .drop_subscription(subscription_id, cascade)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_index(&self, index_id: IndexId, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_index(index_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn drop_function(&self, function_id: FunctionId) -> Result<()> {
        let version = self.meta_client.drop_function(function_id).await?;
        self.wait_version(version).await
    }

    async fn drop_schema(&self, schema_id: u32, cascade: bool) -> Result<()> {
        let version = self.meta_client.drop_schema(schema_id, cascade).await?;
        self.wait_version(version).await
    }

    async fn drop_database(&self, database_id: u32) -> Result<()> {
        let version = self.meta_client.drop_database(database_id).await?;
        self.wait_version(version).await
    }

    async fn drop_connection(&self, connection_id: u32) -> Result<()> {
        let version = self.meta_client.drop_connection(connection_id).await?;
        self.wait_version(version).await
    }

    async fn drop_secret(&self, secret_id: SecretId) -> Result<()> {
        let version = self.meta_client.drop_secret(secret_id).await?;
        self.wait_version(version).await
    }

    async fn alter_name(
        &self,
        object_id: alter_name_request::Object,
        object_name: &str,
    ) -> Result<()> {
        let version = self.meta_client.alter_name(object_id, object_name).await?;
        self.wait_version(version).await
    }

    async fn alter_owner(&self, object: alter_owner_request::Object, owner_id: u32) -> Result<()> {
        let version = self.meta_client.alter_owner(object, owner_id).await?;
        self.wait_version(version).await
    }

    async fn alter_set_schema(
        &self,
        object: alter_set_schema_request::Object,
        new_schema_id: u32,
    ) -> Result<()> {
        let version = self
            .meta_client
            .alter_set_schema(object, new_schema_id)
            .await?;
        self.wait_version(version).await
    }

    async fn alter_source(&self, source: PbSource) -> Result<()> {
        let version = self.meta_client.alter_source(source).await?;
        self.wait_version(version).await
    }

    async fn alter_parallelism(
        &self,
        table_id: u32,
        parallelism: PbTableParallelism,
        deferred: bool,
    ) -> Result<()> {
        self.meta_client
            .alter_parallelism(table_id, parallelism, deferred)
            .await
            .map_err(|e| anyhow!(e))?;

        Ok(())
    }

    async fn alter_swap_rename(&self, object: alter_swap_rename_request::Object) -> Result<()> {
        let version = self.meta_client.alter_swap_rename(object).await?;
        self.wait_version(version).await
    }

    async fn alter_secret(
        &self,
        secret_id: u32,
        secret_name: String,
        database_id: u32,
        schema_id: u32,
        owner_id: u32,
        payload: Vec<u8>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .alter_secret(
                secret_id,
                secret_name,
                database_id,
                schema_id,
                owner_id,
                payload,
            )
            .await?;
        self.wait_version(version).await
    }
}

impl CatalogWriterImpl {
    pub fn new(
        meta_client: MetaClient,
        catalog_updated_rx: Receiver<CatalogVersion>,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
    ) -> Self {
        Self {
            meta_client,
            catalog_updated_rx,
            hummock_snapshot_manager,
        }
    }

    async fn wait_version(&self, version: WaitVersion) -> Result<()> {
        let mut rx = self.catalog_updated_rx.clone();
        while *rx.borrow_and_update() < version.catalog_version {
            rx.changed().await.map_err(|e| anyhow!(e))?;
        }
        self.hummock_snapshot_manager
            .wait(HummockVersionId::new(version.hummock_version_id))
            .await;
        Ok(())
    }
}
