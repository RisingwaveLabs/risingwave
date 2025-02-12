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

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{anyhow, Context};
use risingwave_common::hash::VnodeCount;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::{
    connection, database, function, index, object, schema, secret, sink, source, subscription,
    table, view, PrivateLinkService,
};
use risingwave_meta_model_migration::{MigrationStatus, Migrator, MigratorTrait};
use risingwave_pb::catalog::connection::PbInfo as PbConnectionInfo;
use risingwave_pb::catalog::source::PbOptionalAssociatedTableId;
use risingwave_pb::catalog::subscription::PbSubscriptionState;
use risingwave_pb::catalog::table::{PbEngine, PbOptionalAssociatedSourceId, PbTableType};
use risingwave_pb::catalog::{
    PbConnection, PbCreateType, PbDatabase, PbFunction, PbHandleConflictBehavior, PbIndex,
    PbSchema, PbSecret, PbSink, PbSinkType, PbSource, PbStreamJobStatus, PbSubscription, PbTable,
    PbView,
};
use sea_orm::{ConnectOptions, DatabaseConnection, DbBackend, ModelTrait};

use crate::{MetaError, MetaResult, MetaStoreBackend};

pub mod catalog;
pub mod cluster;
pub mod fragment;
pub mod id;
pub mod rename;
pub mod scale;
pub mod session_params;
pub mod streaming_job;
pub mod system_param;
pub mod user;
pub mod utils;

// todo: refine the error transform.
impl From<sea_orm::DbErr> for MetaError {
    fn from(err: sea_orm::DbErr) -> Self {
        if let Some(err) = err.sql_err() {
            return anyhow!(err).into();
        }
        anyhow!(err).into()
    }
}

#[derive(Clone)]
pub struct SqlMetaStore {
    pub conn: DatabaseConnection,
    pub endpoint: String,
}

impl SqlMetaStore {
    /// Connect to the SQL meta store based on the given configuration.
    pub async fn connect(backend: MetaStoreBackend) -> Result<Self, sea_orm::DbErr> {
        const MAX_DURATION: Duration = Duration::new(u64::MAX / 4, 0);

        #[easy_ext::ext]
        impl ConnectOptions {
            /// Apply common settings for `SQLite` connections.
            fn sqlite_common(&mut self) -> &mut Self {
                self
                    // Since Sqlite is prone to the error "(code: 5) database is locked" under concurrent access,
                    // here we forcibly specify the number of connections as 1.
                    .min_connections(1)
                    .max_connections(1)
                    // Workaround for https://github.com/risingwavelabs/risingwave/issues/18966.
                    // Note: don't quite get the point but `acquire_timeout` and `connect_timeout` maps to the
                    //       same underlying setting in `sqlx` under current implementation.
                    .acquire_timeout(MAX_DURATION)
                    .connect_timeout(MAX_DURATION)
            }
        }

        Ok(match backend {
            MetaStoreBackend::Mem => {
                const IN_MEMORY_STORE: &str = "sqlite::memory:";

                let mut options = ConnectOptions::new(IN_MEMORY_STORE);

                options
                    .sqlite_common()
                    // Releasing the connection to in-memory SQLite database is unacceptable
                    // because it will clear the database. Set a large enough timeout to prevent it.
                    // `sqlx` actually supports disabling these timeouts by passing a `None`, but
                    // `sea-orm` does not expose this option.
                    .idle_timeout(MAX_DURATION)
                    .max_lifetime(MAX_DURATION);

                let conn = sea_orm::Database::connect(options).await?;
                Self {
                    conn,
                    endpoint: IN_MEMORY_STORE.to_owned(),
                }
            }
            MetaStoreBackend::Sql { endpoint, config } => {
                let mut options = ConnectOptions::new(endpoint.clone());
                options
                    .max_connections(config.max_connections)
                    .min_connections(config.min_connections)
                    .connect_timeout(Duration::from_secs(config.connection_timeout_sec))
                    .idle_timeout(Duration::from_secs(config.idle_timeout_sec))
                    .acquire_timeout(Duration::from_secs(config.acquire_timeout_sec));

                if DbBackend::Sqlite.is_prefix_of(&endpoint) {
                    options.sqlite_common();
                }

                let conn = sea_orm::Database::connect(options).await?;
                Self { conn, endpoint }
            }
        })
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn for_test() -> Self {
        let this = Self::connect(MetaStoreBackend::Mem).await.unwrap();
        Migrator::up(&this.conn, None).await.unwrap();
        this
    }

    /// Check whether the cluster, which uses SQL as the backend, is a new cluster.
    /// It determines this by inspecting the applied migrations. If the migration `m20230908_072257_init` has been applied,
    /// then it is considered an old cluster.
    ///
    /// Note: this check should be performed before [`Self::up()`].
    async fn is_first_launch(&self) -> MetaResult<bool> {
        let migrations = Migrator::get_applied_migrations(&self.conn)
            .await
            .context("failed to get applied migrations")?;
        for migration in migrations {
            if migration.name() == "m20230908_072257_init"
                && migration.status() == MigrationStatus::Applied
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Apply all the migrations to the meta store before starting the service.
    ///
    /// Returns whether the cluster is the first launch.
    pub async fn up(&self) -> MetaResult<bool> {
        let cluster_first_launch = self.is_first_launch().await?;
        // Try to upgrade if any new model changes are added.
        Migrator::up(&self.conn, None)
            .await
            .context("failed to upgrade models in meta store")?;

        Ok(cluster_first_launch)
    }
}

pub struct ObjectModel<M: ModelTrait>(M, object::Model);

impl From<ObjectModel<database::Model>> for PbDatabase {
    fn from(value: ObjectModel<database::Model>) -> Self {
        Self {
            id: value.0.database_id as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
            resource_group: value.0.resource_group.clone(),
        }
    }
}

impl From<ObjectModel<secret::Model>> for PbSecret {
    fn from(value: ObjectModel<secret::Model>) -> Self {
        Self {
            id: value.0.secret_id as _,
            name: value.0.name,
            database_id: value.1.database_id.unwrap() as _,
            value: value.0.value,
            owner: value.1.owner_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
        }
    }
}

impl From<ObjectModel<schema::Model>> for PbSchema {
    fn from(value: ObjectModel<schema::Model>) -> Self {
        Self {
            id: value.0.schema_id as _,
            name: value.0.name,
            database_id: value.1.database_id.unwrap() as _,
            owner: value.1.owner_id as _,
        }
    }
}

impl From<ObjectModel<table::Model>> for PbTable {
    fn from(value: ObjectModel<table::Model>) -> Self {
        Self {
            id: value.0.table_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            columns: value.0.columns.to_protobuf(),
            pk: value.0.pk.to_protobuf(),
            dependent_relations: vec![], // todo: deprecate it.
            table_type: PbTableType::from(value.0.table_type) as _,
            distribution_key: value.0.distribution_key.0,
            stream_key: value.0.stream_key.0,
            append_only: value.0.append_only,
            owner: value.1.owner_id as _,
            fragment_id: value.0.fragment_id.unwrap_or_default() as u32,
            vnode_col_index: value.0.vnode_col_index.map(|index| index as _),
            row_id_index: value.0.row_id_index.map(|index| index as _),
            value_indices: value.0.value_indices.0,
            definition: value.0.definition,
            handle_pk_conflict_behavior: PbHandleConflictBehavior::from(
                value.0.handle_pk_conflict_behavior,
            ) as _,
            version_column_index: value.0.version_column_index.map(|x| x as u32),
            read_prefix_len_hint: value.0.read_prefix_len_hint as _,
            watermark_indices: value.0.watermark_indices.0,
            dist_key_in_pk: value.0.dist_key_in_pk.0,
            dml_fragment_id: value.0.dml_fragment_id.map(|id| id as u32),
            cardinality: value
                .0
                .cardinality
                .map(|cardinality| cardinality.to_protobuf()),
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(value.1.initialized_at.and_utc().timestamp_millis() as _).0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(value.1.created_at.and_utc().timestamp_millis() as _).0,
            ),
            cleaned_by_watermark: value.0.cleaned_by_watermark,
            stream_job_status: PbStreamJobStatus::Created as _,
            create_type: PbCreateType::Foreground as _,
            version: value.0.version.map(|v| v.to_protobuf()),
            optional_associated_source_id: value
                .0
                .optional_associated_source_id
                .map(|id| PbOptionalAssociatedSourceId::AssociatedSourceId(id as _)),
            description: value.0.description,
            incoming_sinks: value.0.incoming_sinks.into_u32_array(),
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
            retention_seconds: value.0.retention_seconds.map(|id| id as u32),
            cdc_table_id: value.0.cdc_table_id,
            maybe_vnode_count: VnodeCount::set(value.0.vnode_count).to_protobuf(),
            webhook_info: value.0.webhook_info.map(|info| info.to_protobuf()),
            job_id: value.0.belongs_to_job_id.map(|id| id as _),
            engine: value.0.engine.map(|engine| PbEngine::from(engine) as i32),
            clean_watermark_index_in_pk: value.0.clean_watermark_index_in_pk,
        }
    }
}

impl From<ObjectModel<source::Model>> for PbSource {
    fn from(value: ObjectModel<source::Model>) -> Self {
        let mut secret_ref_map = BTreeMap::new();
        if let Some(secret_ref) = value.0.secret_ref {
            secret_ref_map = secret_ref.to_protobuf();
        }
        Self {
            id: value.0.source_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            row_id_index: value.0.row_id_index.map(|id| id as _),
            columns: value.0.columns.to_protobuf(),
            pk_column_ids: value.0.pk_column_ids.0,
            with_properties: value.0.with_properties.0,
            owner: value.1.owner_id as _,
            info: value.0.source_info.map(|info| info.to_protobuf()),
            watermark_descs: value.0.watermark_descs.to_protobuf(),
            definition: value.0.definition,
            connection_id: value.0.connection_id.map(|id| id as _),
            // todo: using the timestamp from the database directly.
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(value.1.initialized_at.and_utc().timestamp_millis() as _).0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(value.1.created_at.and_utc().timestamp_millis() as _).0,
            ),
            version: value.0.version as _,
            optional_associated_table_id: value
                .0
                .optional_associated_table_id
                .map(|id| PbOptionalAssociatedTableId::AssociatedTableId(id as _)),
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
            secret_refs: secret_ref_map,
            rate_limit: value.0.rate_limit.map(|v| v as _),
        }
    }
}

impl From<ObjectModel<sink::Model>> for PbSink {
    fn from(value: ObjectModel<sink::Model>) -> Self {
        let mut secret_ref_map = BTreeMap::new();
        if let Some(secret_ref) = value.0.secret_ref {
            secret_ref_map = secret_ref.to_protobuf();
        }
        #[allow(deprecated)] // for `dependent_relations`
        Self {
            id: value.0.sink_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            columns: value.0.columns.to_protobuf(),
            plan_pk: value.0.plan_pk.to_protobuf(),
            dependent_relations: vec![],
            distribution_key: value.0.distribution_key.0,
            downstream_pk: value.0.downstream_pk.0,
            sink_type: PbSinkType::from(value.0.sink_type) as _,
            owner: value.1.owner_id as _,
            properties: value.0.properties.0,
            definition: value.0.definition,
            connection_id: value.0.connection_id.map(|id| id as _),
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(value.1.initialized_at.and_utc().timestamp_millis() as _).0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(value.1.created_at.and_utc().timestamp_millis() as _).0,
            ),
            db_name: value.0.db_name,
            sink_from_name: value.0.sink_from_name,
            stream_job_status: PbStreamJobStatus::Created as _,
            format_desc: value.0.sink_format_desc.map(|desc| desc.to_protobuf()),
            target_table: value.0.target_table.map(|id| id as _),
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
            create_type: PbCreateType::Foreground as _,
            secret_refs: secret_ref_map,
            original_target_columns: value
                .0
                .original_target_columns
                .map(|cols| cols.to_protobuf())
                .unwrap_or_default(),
        }
    }
}

impl From<ObjectModel<subscription::Model>> for PbSubscription {
    fn from(value: ObjectModel<subscription::Model>) -> Self {
        Self {
            id: value.0.subscription_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
            retention_seconds: value.0.retention_seconds as _,
            definition: value.0.definition,
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(value.1.initialized_at.and_utc().timestamp_millis() as _).0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(value.1.created_at.and_utc().timestamp_millis() as _).0,
            ),
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
            dependent_table_id: value.0.dependent_table_id as _,
            subscription_state: PbSubscriptionState::Init as _,
        }
    }
}

impl From<ObjectModel<index::Model>> for PbIndex {
    fn from(value: ObjectModel<index::Model>) -> Self {
        Self {
            id: value.0.index_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
            index_table_id: value.0.index_table_id as _,
            primary_table_id: value.0.primary_table_id as _,
            index_item: value.0.index_items.to_protobuf(),
            index_column_properties: value
                .0
                .index_column_properties
                .map(|p| p.to_protobuf())
                .unwrap_or_default(),
            index_columns_len: value.0.index_columns_len as _,
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(value.1.initialized_at.and_utc().timestamp_millis() as _).0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(value.1.created_at.and_utc().timestamp_millis() as _).0,
            ),
            stream_job_status: PbStreamJobStatus::Created as _,
            initialized_at_cluster_version: value.1.initialized_at_cluster_version,
            created_at_cluster_version: value.1.created_at_cluster_version,
        }
    }
}

impl From<ObjectModel<view::Model>> for PbView {
    fn from(value: ObjectModel<view::Model>) -> Self {
        Self {
            id: value.0.view_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
            properties: value.0.properties.0,
            sql: value.0.definition,
            dependent_relations: vec![], // todo: deprecate it.
            columns: value.0.columns.to_protobuf(),
        }
    }
}

impl From<ObjectModel<connection::Model>> for PbConnection {
    fn from(value: ObjectModel<connection::Model>) -> Self {
        let info: PbConnectionInfo = if value.0.info == PrivateLinkService::default() {
            PbConnectionInfo::ConnectionParams(value.0.params.to_protobuf())
        } else {
            PbConnectionInfo::PrivateLinkService(value.0.info.to_protobuf())
        };
        Self {
            id: value.1.oid as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
            info: Some(info),
        }
    }
}

impl From<ObjectModel<function::Model>> for PbFunction {
    fn from(value: ObjectModel<function::Model>) -> Self {
        Self {
            id: value.0.function_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
            arg_names: value.0.arg_names.split(',').map(|s| s.to_owned()).collect(),
            arg_types: value.0.arg_types.to_protobuf(),
            return_type: Some(value.0.return_type.to_protobuf()),
            language: value.0.language,
            runtime: value.0.runtime,
            link: value.0.link,
            name_in_runtime: value.0.name_in_runtime,
            body: value.0.body,
            compressed_binary: value.0.compressed_binary,
            kind: Some(value.0.kind.into()),
            always_retry_on_network_error: value.0.always_retry_on_network_error,
        }
    }
}
