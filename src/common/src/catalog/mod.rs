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

mod column;
mod external_table;
mod internal_table;
mod physical_table;
mod schema;
pub mod test_utils;

use std::sync::Arc;

pub use column::*;
pub use external_table::*;
use futures::stream::BoxStream;
pub use internal_table::*;
use parse_display::Display;
pub use physical_table::*;
use risingwave_pb::catalog::table::PbEngine;
use risingwave_pb::catalog::{
    CreateType as PbCreateType, HandleConflictBehavior as PbHandleConflictBehavior,
    StreamJobStatus as PbStreamJobStatus,
};
use risingwave_pb::plan_common::ColumnDescVersion;
pub use schema::{test_utils as schema_test_utils, Field, FieldDisplay, FieldLike, Schema};
use serde::{Deserialize, Serialize};

use crate::array::DataChunk;
pub use crate::constants::hummock;
use crate::error::BoxedError;
use crate::types::DataType;

/// The global version of the catalog.
pub type CatalogVersion = u64;

/// The version number of the per-table catalog.
pub type TableVersionId = u64;
/// The default version ID for a new table.
pub const INITIAL_TABLE_VERSION_ID: u64 = 0;
/// The version number of the per-source catalog.
pub type SourceVersionId = u64;
/// The default version ID for a new source.
pub const INITIAL_SOURCE_VERSION_ID: u64 = 0;

pub const DEFAULT_DATABASE_NAME: &str = "dev";
pub const DEFAULT_SCHEMA_NAME: &str = "public";
pub const PG_CATALOG_SCHEMA_NAME: &str = "pg_catalog";
pub const INFORMATION_SCHEMA_SCHEMA_NAME: &str = "information_schema";
pub const RW_CATALOG_SCHEMA_NAME: &str = "rw_catalog";
pub const RESERVED_PG_SCHEMA_PREFIX: &str = "pg_";
pub const DEFAULT_SUPER_USER: &str = "root";
pub const DEFAULT_SUPER_USER_ID: u32 = 1;
// This is for compatibility with customized utils for PostgreSQL.
pub const DEFAULT_SUPER_USER_FOR_PG: &str = "postgres";
pub const DEFAULT_SUPER_USER_FOR_PG_ID: u32 = 2;

pub const NON_RESERVED_USER_ID: i32 = 11;

pub const MAX_SYS_CATALOG_NUM: i32 = 5000;
pub const SYS_CATALOG_START_ID: i32 = i32::MAX - MAX_SYS_CATALOG_NUM;

pub const OBJECT_ID_PLACEHOLDER: u32 = u32::MAX - 1;

pub const SYSTEM_SCHEMAS: [&str; 3] = [
    PG_CATALOG_SCHEMA_NAME,
    INFORMATION_SCHEMA_SCHEMA_NAME,
    RW_CATALOG_SCHEMA_NAME,
];

pub const RW_RESERVED_COLUMN_NAME_PREFIX: &str = "_rw_";

// When there is no primary key specified while creating source, will use the
// the message key as primary key in `BYTEA` type with this name.
// Note: the field has version to track, please refer to `default_key_column_name_version_mapping`
pub const DEFAULT_KEY_COLUMN_NAME: &str = "_rw_key";

pub fn default_key_column_name_version_mapping(version: &ColumnDescVersion) -> &str {
    match version {
        ColumnDescVersion::Unspecified => DEFAULT_KEY_COLUMN_NAME,
        _ => DEFAULT_KEY_COLUMN_NAME,
    }
}

/// For kafka source, we attach a hidden column [`KAFKA_TIMESTAMP_COLUMN_NAME`] to it, so that we
/// can limit the timestamp range when querying it directly with batch query. The column type is
/// [`DataType::Timestamptz`]. For more details, please refer to
/// [this rfc](https://github.com/risingwavelabs/rfcs/pull/20).
pub const KAFKA_TIMESTAMP_COLUMN_NAME: &str = "_rw_kafka_timestamp";

pub const RISINGWAVE_ICEBERG_ROW_ID: &str = "_risingwave_iceberg_row_id";
pub fn is_system_schema(schema_name: &str) -> bool {
    SYSTEM_SCHEMAS.iter().any(|s| *s == schema_name)
}

pub const ROWID_PREFIX: &str = "_row_id";

pub fn is_row_id_column_name(name: &str) -> bool {
    name.starts_with(ROWID_PREFIX)
}

/// The column ID preserved for the row ID column.
pub const ROW_ID_COLUMN_ID: ColumnId = ColumnId::new(0);

/// The column ID offset for user-defined columns.
///
/// All IDs of user-defined columns must be greater or equal to this value.
pub const USER_COLUMN_ID_OFFSET: i32 = ROW_ID_COLUMN_ID.next().get_id();

/// Creates a row ID column (for implicit primary key). It'll always have the ID `0` for now.
pub fn row_id_column_desc() -> ColumnDesc {
    ColumnDesc::named(ROWID_PREFIX, ROW_ID_COLUMN_ID, DataType::Serial)
}

pub const RW_TIMESTAMP_COLUMN_NAME: &str = "_rw_timestamp";
pub const RW_TIMESTAMP_COLUMN_ID: ColumnId = ColumnId::new(-1);
pub fn rw_timestamp_column_desc() -> ColumnDesc {
    ColumnDesc::named_with_system_column(
        RW_TIMESTAMP_COLUMN_NAME,
        RW_TIMESTAMP_COLUMN_ID,
        DataType::Timestamptz,
        SystemColumn::RwTimestamp,
    )
}

pub const OFFSET_COLUMN_NAME: &str = "_rw_offset";
pub const ICEBERG_SEQUENCE_NUM_COLUMN_NAME: &str = "_iceberg_sequence_number";

// The number of columns output by the cdc source job
// see `debezium_cdc_source_schema()` for details
pub const CDC_SOURCE_COLUMN_NUM: u32 = 3;
pub const TABLE_NAME_COLUMN_NAME: &str = "_rw_table_name";

pub fn is_offset_column_name(name: &str) -> bool {
    name.starts_with(OFFSET_COLUMN_NAME)
}
/// Creates a offset column for storing upstream offset
/// Used in cdc source currently
pub fn offset_column_desc() -> ColumnDesc {
    ColumnDesc::named(
        OFFSET_COLUMN_NAME,
        ColumnId::placeholder(),
        DataType::Varchar,
    )
}

/// A column to store the upstream table name of the cdc table
pub fn cdc_table_name_column_desc() -> ColumnDesc {
    ColumnDesc::named(
        TABLE_NAME_COLUMN_NAME,
        ColumnId::placeholder(),
        DataType::Varchar,
    )
}

pub fn iceberg_sequence_num_column_desc() -> ColumnDesc {
    ColumnDesc::named(
        ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
        ColumnId::placeholder(),
        DataType::Int64,
    )
}

/// The local system catalog reader in the frontend node.
pub trait SysCatalogReader: Sync + Send + 'static {
    /// Reads the data of the system catalog table.
    fn read_table(&self, table_id: TableId) -> BoxStream<'_, Result<DataChunk, BoxedError>>;
}

pub type SysCatalogReaderRef = Arc<dyn SysCatalogReader>;

pub type ObjectId = u32;

#[derive(Clone, Debug, Default, Display, Hash, PartialOrd, PartialEq, Eq, Copy)]
#[display("{database_id}")]
pub struct DatabaseId {
    pub database_id: u32,
}

impl DatabaseId {
    pub const fn new(database_id: u32) -> Self {
        DatabaseId { database_id }
    }

    pub fn placeholder() -> Self {
        DatabaseId {
            database_id: OBJECT_ID_PLACEHOLDER,
        }
    }
}

impl From<u32> for DatabaseId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for DatabaseId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<DatabaseId> for u32 {
    fn from(id: DatabaseId) -> Self {
        id.database_id
    }
}

#[derive(Clone, Debug, Default, Display, Hash, PartialOrd, PartialEq, Eq)]
#[display("{schema_id}")]
pub struct SchemaId {
    pub schema_id: u32,
}

impl SchemaId {
    pub fn new(schema_id: u32) -> Self {
        SchemaId { schema_id }
    }

    pub fn placeholder() -> Self {
        SchemaId {
            schema_id: OBJECT_ID_PLACEHOLDER,
        }
    }
}

impl From<u32> for SchemaId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for SchemaId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<SchemaId> for u32 {
    fn from(id: SchemaId) -> Self {
        id.schema_id
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Display,
    Default,
    Hash,
    PartialOrd,
    PartialEq,
    Eq,
    Ord,
    Serialize,
    Deserialize,
)]
#[display("{table_id}")]
pub struct TableId {
    pub table_id: u32,
}

impl TableId {
    pub const fn new(table_id: u32) -> Self {
        TableId { table_id }
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        TableId {
            table_id: OBJECT_ID_PLACEHOLDER,
        }
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }
}

impl From<u32> for TableId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for TableId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<TableId> for u32 {
    fn from(id: TableId) -> Self {
        id.table_id
    }
}

#[derive(Clone, Debug, PartialEq, Default, Copy)]
pub struct TableOption {
    pub retention_seconds: Option<u32>, // second
}

impl From<&risingwave_pb::hummock::TableOption> for TableOption {
    fn from(table_option: &risingwave_pb::hummock::TableOption) -> Self {
        Self {
            retention_seconds: table_option.retention_seconds,
        }
    }
}

impl From<&TableOption> for risingwave_pb::hummock::TableOption {
    fn from(table_option: &TableOption) -> Self {
        Self {
            retention_seconds: table_option.retention_seconds,
        }
    }
}

impl TableOption {
    pub fn new(retention_seconds: Option<u32>) -> Self {
        // now we only support ttl for TableOption
        TableOption { retention_seconds }
    }
}

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq)]
#[display("{index_id}")]
pub struct IndexId {
    pub index_id: u32,
}

impl IndexId {
    pub const fn new(index_id: u32) -> Self {
        IndexId { index_id }
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        IndexId {
            index_id: OBJECT_ID_PLACEHOLDER,
        }
    }

    pub fn index_id(&self) -> u32 {
        self.index_id
    }
}

impl From<u32> for IndexId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}
impl From<IndexId> for u32 {
    fn from(id: IndexId) -> Self {
        id.index_id
    }
}

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
pub struct FunctionId(pub u32);

impl FunctionId {
    pub const fn new(id: u32) -> Self {
        FunctionId(id)
    }

    pub const fn placeholder() -> Self {
        FunctionId(OBJECT_ID_PLACEHOLDER)
    }

    pub fn function_id(&self) -> u32 {
        self.0
    }
}

impl From<u32> for FunctionId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for FunctionId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<FunctionId> for u32 {
    fn from(id: FunctionId) -> Self {
        id.0
    }
}

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
#[display("{user_id}")]
pub struct UserId {
    pub user_id: u32,
}

impl UserId {
    pub const fn new(user_id: u32) -> Self {
        UserId { user_id }
    }

    pub const fn placeholder() -> Self {
        UserId {
            user_id: OBJECT_ID_PLACEHOLDER,
        }
    }
}

impl From<u32> for UserId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for UserId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<UserId> for u32 {
    fn from(id: UserId) -> Self {
        id.user_id
    }
}

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
pub struct ConnectionId(pub u32);

impl ConnectionId {
    pub const fn new(id: u32) -> Self {
        ConnectionId(id)
    }

    pub const fn placeholder() -> Self {
        ConnectionId(OBJECT_ID_PLACEHOLDER)
    }

    pub fn connection_id(&self) -> u32 {
        self.0
    }
}

impl From<u32> for ConnectionId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for ConnectionId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<ConnectionId> for u32 {
    fn from(id: ConnectionId) -> Self {
        id.0
    }
}

#[derive(Clone, Copy, Debug, Display, Default, Hash, PartialOrd, PartialEq, Eq, Ord)]
pub struct SecretId(pub u32);

impl SecretId {
    pub const fn new(id: u32) -> Self {
        SecretId(id)
    }

    pub const fn placeholder() -> Self {
        SecretId(OBJECT_ID_PLACEHOLDER)
    }

    pub fn secret_id(&self) -> u32 {
        self.0
    }
}

impl From<u32> for SecretId {
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<&u32> for SecretId {
    fn from(id: &u32) -> Self {
        Self::new(*id)
    }
}

impl From<SecretId> for u32 {
    fn from(id: SecretId) -> Self {
        id.0
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConflictBehavior {
    #[default]
    NoCheck,
    Overwrite,
    IgnoreConflict,
    DoUpdateIfNotNull,
}

impl ConflictBehavior {
    pub fn from_protobuf(tb_conflict_behavior: &PbHandleConflictBehavior) -> Self {
        match tb_conflict_behavior {
            PbHandleConflictBehavior::Overwrite => ConflictBehavior::Overwrite,
            PbHandleConflictBehavior::Ignore => ConflictBehavior::IgnoreConflict,
            PbHandleConflictBehavior::DoUpdateIfNotNull => ConflictBehavior::DoUpdateIfNotNull,
            // This is for backward compatibility, in the previous version
            // `HandleConflictBehavior::Unspecified` represented `NoCheck`, so just treat it as `NoCheck`.
            PbHandleConflictBehavior::NoCheck | PbHandleConflictBehavior::Unspecified => {
                ConflictBehavior::NoCheck
            }
        }
    }

    pub fn to_protobuf(self) -> PbHandleConflictBehavior {
        match self {
            ConflictBehavior::NoCheck => PbHandleConflictBehavior::NoCheck,
            ConflictBehavior::Overwrite => PbHandleConflictBehavior::Overwrite,
            ConflictBehavior::IgnoreConflict => PbHandleConflictBehavior::Ignore,
            ConflictBehavior::DoUpdateIfNotNull => PbHandleConflictBehavior::DoUpdateIfNotNull,
        }
    }

    pub fn debug_to_string(self) -> String {
        match self {
            ConflictBehavior::NoCheck => "NoCheck".to_owned(),
            ConflictBehavior::Overwrite => "Overwrite".to_owned(),
            ConflictBehavior::IgnoreConflict => "IgnoreConflict".to_owned(),
            ConflictBehavior::DoUpdateIfNotNull => "DoUpdateIfNotNull".to_owned(),
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Engine {
    #[default]
    Hummock,
    Iceberg,
}

impl Engine {
    pub fn from_protobuf(engine: &PbEngine) -> Self {
        match engine {
            PbEngine::Hummock | PbEngine::Unspecified => Engine::Hummock,
            PbEngine::Iceberg => Engine::Iceberg,
        }
    }

    pub fn to_protobuf(self) -> PbEngine {
        match self {
            Engine::Hummock => PbEngine::Hummock,
            Engine::Iceberg => PbEngine::Iceberg,
        }
    }

    pub fn debug_to_string(self) -> String {
        match self {
            Engine::Hummock => "Hummock".to_owned(),
            Engine::Iceberg => "Iceberg".to_owned(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Display, Hash, PartialOrd, PartialEq, Eq, Ord)]
pub enum StreamJobStatus {
    #[default]
    Creating,
    Created,
}

impl StreamJobStatus {
    pub fn from_proto(stream_job_status: PbStreamJobStatus) -> Self {
        match stream_job_status {
            PbStreamJobStatus::Creating => StreamJobStatus::Creating,
            PbStreamJobStatus::Created | PbStreamJobStatus::Unspecified => StreamJobStatus::Created,
        }
    }

    pub fn to_proto(self) -> PbStreamJobStatus {
        match self {
            StreamJobStatus::Creating => PbStreamJobStatus::Creating,
            StreamJobStatus::Created => PbStreamJobStatus::Created,
        }
    }
}

#[derive(Clone, Copy, Debug, Display, Hash, PartialOrd, PartialEq, Eq, Ord)]
pub enum CreateType {
    Foreground,
    Background,
}

impl Default for CreateType {
    fn default() -> Self {
        Self::Foreground
    }
}

impl CreateType {
    pub fn from_proto(pb_create_type: PbCreateType) -> Self {
        match pb_create_type {
            PbCreateType::Foreground | PbCreateType::Unspecified => CreateType::Foreground,
            PbCreateType::Background => CreateType::Background,
        }
    }

    pub fn to_proto(self) -> PbCreateType {
        match self {
            CreateType::Foreground => PbCreateType::Foreground,
            CreateType::Background => PbCreateType::Background,
        }
    }
}
