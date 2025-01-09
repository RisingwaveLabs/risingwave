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

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::{CatalogVersion, FunctionId, IndexId, TableId};
use risingwave_common::session_config::{SearchPath, USER_NAME_WILD_CARD};
use risingwave_common::types::DataType;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_pb::catalog::{
    PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSecret, PbSink, PbSource,
    PbSubscription, PbTable, PbView,
};
use risingwave_pb::hummock::HummockVersionStats;

use super::function_catalog::FunctionCatalog;
use super::source_catalog::SourceCatalog;
use super::subscription_catalog::SubscriptionCatalog;
use super::view_catalog::ViewCatalog;
use super::{
    CatalogError, CatalogResult, ConnectionId, SecretId, SinkId, SourceId, SubscriptionId, ViewId,
};
use crate::catalog::connection_catalog::ConnectionCatalog;
use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::secret_catalog::SecretCatalog;
use crate::catalog::system_catalog::{
    get_sys_tables_in_schema, get_sys_views_in_schema, SystemTableCatalog,
};
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{DatabaseId, IndexCatalog, SchemaId};
use crate::expr::{Expr, ExprImpl};

#[derive(Copy, Clone)]
pub enum SchemaPath<'a> {
    Name(&'a str),
    /// (`search_path`, `user_name`).
    Path(&'a SearchPath, &'a str),
}

impl<'a> SchemaPath<'a> {
    pub fn new(
        schema_name: Option<&'a str>,
        search_path: &'a SearchPath,
        user_name: &'a str,
    ) -> Self {
        match schema_name {
            Some(schema_name) => SchemaPath::Name(schema_name),
            None => SchemaPath::Path(search_path, user_name),
        }
    }

    /// Call function `f` for each schema name. Return the first `Some` result.
    pub fn try_find<T, E>(
        &self,
        mut f: impl FnMut(&str) -> Result<Option<T>, E>,
    ) -> Result<Option<(T, &'a str)>, E> {
        match self {
            SchemaPath::Name(schema_name) => Ok(f(schema_name)?.map(|t| (t, *schema_name))),
            SchemaPath::Path(search_path, user_name) => {
                for schema_name in search_path.path() {
                    let mut schema_name: &str = schema_name;
                    if schema_name == USER_NAME_WILD_CARD {
                        schema_name = user_name;
                    }
                    if let Ok(Some(res)) = f(schema_name) {
                        return Ok(Some((res, schema_name)));
                    }
                }
                Ok(None)
            }
        }
    }
}

/// Root catalog of database catalog. It manages all database/schema/table in memory on frontend.
/// It is protected by a `RwLock`. Only [`crate::observer::FrontendObserverNode`]
/// will acquire the write lock and sync it with the meta catalog. In other situations, it is
/// read only.
///
/// - catalog (root catalog)
///   - database catalog
///     - schema catalog
///       - function catalog (i.e., user defined function)
///       - table/sink/source/index/view catalog
///        - column catalog
pub struct Catalog {
    version: CatalogVersion,
    database_by_name: HashMap<String, DatabaseCatalog>,
    db_name_by_id: HashMap<DatabaseId, String>,
    /// all table catalogs in the cluster identified by universal unique table id.
    table_by_id: HashMap<TableId, Arc<TableCatalog>>,
    table_stats: HummockVersionStats,
}

#[expect(clippy::derivable_impls)]
impl Default for Catalog {
    fn default() -> Self {
        Self {
            version: 0,
            database_by_name: HashMap::new(),
            db_name_by_id: HashMap::new(),
            table_by_id: HashMap::new(),
            table_stats: HummockVersionStats::default(),
        }
    }
}

impl Catalog {
    fn get_database_mut(&mut self, db_id: DatabaseId) -> Option<&mut DatabaseCatalog> {
        let name = self.db_name_by_id.get(&db_id)?;
        self.database_by_name.get_mut(name)
    }

    pub fn clear(&mut self) {
        self.database_by_name.clear();
        self.db_name_by_id.clear();
        self.table_by_id.clear();
    }

    pub fn create_database(&mut self, db: &PbDatabase) {
        let name = db.name.clone();
        let id = db.id;

        self.database_by_name
            .try_insert(name.clone(), db.into())
            .unwrap();
        self.db_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn create_schema(&mut self, proto: &PbSchema) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .create_schema(proto);

        for sys_table in get_sys_tables_in_schema(proto.name.as_str()) {
            self.get_database_mut(proto.database_id)
                .unwrap()
                .get_schema_mut(proto.id)
                .unwrap()
                .create_sys_table(sys_table);
        }
        for sys_view in get_sys_views_in_schema(proto.name.as_str()) {
            self.get_database_mut(proto.database_id)
                .unwrap()
                .get_schema_mut(proto.id)
                .unwrap()
                .create_sys_view(sys_view);
        }
    }

    pub fn create_table(&mut self, proto: &PbTable) {
        let table = self
            .get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_table(proto);
        self.table_by_id.insert(proto.id.into(), table);
    }

    pub fn create_index(&mut self, proto: &PbIndex) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_index(proto);
    }

    pub fn create_source(&mut self, proto: &PbSource) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_source(proto);
    }

    pub fn create_sink(&mut self, proto: &PbSink) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_sink(proto);
    }

    pub fn create_subscription(&mut self, proto: &PbSubscription) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_subscription(proto);
    }

    pub fn create_secret(&mut self, proto: &PbSecret) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_secret(proto);
    }

    pub fn create_view(&mut self, proto: &PbView) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_view(proto);
    }

    pub fn create_function(&mut self, proto: &PbFunction) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_function(proto);
    }

    pub fn create_connection(&mut self, proto: &PbConnection) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_connection(proto);
    }

    pub fn drop_connection(
        &mut self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        connection_id: ConnectionId,
    ) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_connection(connection_id);
    }

    pub fn update_connection(&mut self, proto: &PbConnection) {
        let database = self.get_database_mut(proto.database_id).unwrap();
        let schema = database.get_schema_mut(proto.schema_id).unwrap();
        if schema.get_connection_by_id(&proto.id).is_some() {
            schema.update_connection(proto);
        } else {
            // Enter this branch when schema is changed by `ALTER ... SET SCHEMA ...` statement.
            schema.create_connection(proto);
            database
                .iter_schemas_mut()
                .find(|schema| {
                    schema.id() != proto.schema_id
                        && schema.get_connection_by_id(&proto.id).is_some()
                })
                .unwrap()
                .drop_connection(proto.id);
        }
    }

    pub fn update_secret(&mut self, proto: &PbSecret) {
        let database = self.get_database_mut(proto.database_id).unwrap();
        let schema = database.get_schema_mut(proto.schema_id).unwrap();
        let secret_id = SecretId::new(proto.id);
        if schema.get_secret_by_id(&secret_id).is_some() {
            schema.update_secret(proto);
        } else {
            // Enter this branch when schema is changed by `ALTER ... SET SCHEMA ...` statement.
            schema.create_secret(proto);
            database
                .iter_schemas_mut()
                .find(|schema| {
                    schema.id() != proto.schema_id && schema.get_secret_by_id(&secret_id).is_some()
                })
                .unwrap()
                .drop_secret(secret_id);
        }
    }

    pub fn drop_database(&mut self, db_id: DatabaseId) {
        let name = self.db_name_by_id.remove(&db_id).unwrap();
        let database = self.database_by_name.remove(&name).unwrap();
        database.iter_all_table_ids().for_each(|table| {
            self.table_by_id.remove(&table);
        });
    }

    pub fn drop_schema(&mut self, db_id: DatabaseId, schema_id: SchemaId) {
        self.get_database_mut(db_id).unwrap().drop_schema(schema_id);
    }

    pub fn drop_table(&mut self, db_id: DatabaseId, schema_id: SchemaId, tb_id: TableId) {
        self.table_by_id.remove(&tb_id);
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_table(tb_id);
    }

    pub fn update_table(&mut self, proto: &PbTable) {
        let database = self.get_database_mut(proto.database_id).unwrap();
        let schema = database.get_schema_mut(proto.schema_id).unwrap();
        let table = if schema.get_table_by_id(&proto.id.into()).is_some() {
            schema.update_table(proto)
        } else {
            // Enter this branch when schema is changed by `ALTER ... SET SCHEMA ...` statement.
            let new_table = schema.create_table(proto);
            database
                .iter_schemas_mut()
                .find(|schema| {
                    schema.id() != proto.schema_id
                        && schema.get_created_table_by_id(&proto.id.into()).is_some()
                })
                .unwrap()
                .drop_table(proto.id.into());
            new_table
        };

        self.table_by_id.insert(proto.id.into(), table);
    }

    pub fn update_database(&mut self, proto: &PbDatabase) {
        let id = proto.id;
        let name = proto.name.clone();

        let old_database_name = self.db_name_by_id.get(&id).unwrap().to_owned();
        if old_database_name != name {
            let mut database = self.database_by_name.remove(&old_database_name).unwrap();
            database.name.clone_from(&name);
            database.owner = proto.owner;
            self.database_by_name.insert(name.clone(), database);
            self.db_name_by_id.insert(id, name);
        } else {
            let database = self.get_database_mut(id).unwrap();
            database.name = name;
            database.owner = proto.owner;
        }
    }

    pub fn update_schema(&mut self, proto: &PbSchema) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .update_schema(proto);
    }

    pub fn update_index(&mut self, proto: &PbIndex) {
        let database = self.get_database_mut(proto.database_id).unwrap();
        let schema = database.get_schema_mut(proto.schema_id).unwrap();
        if schema.get_index_by_id(&proto.id.into()).is_some() {
            schema.update_index(proto);
        } else {
            // Enter this branch when schema is changed by `ALTER ... SET SCHEMA ...` statement.
            schema.create_index(proto);
            database
                .iter_schemas_mut()
                .find(|schema| {
                    schema.id() != proto.schema_id
                        && schema.get_index_by_id(&proto.id.into()).is_some()
                })
                .unwrap()
                .drop_index(proto.id.into());
        }
    }

    pub fn drop_source(&mut self, db_id: DatabaseId, schema_id: SchemaId, source_id: SourceId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_source(source_id);
    }

    pub fn update_source(&mut self, proto: &PbSource) {
        let database = self.get_database_mut(proto.database_id).unwrap();
        let schema = database.get_schema_mut(proto.schema_id).unwrap();
        if schema.get_source_by_id(&proto.id).is_some() {
            schema.update_source(proto);
        } else {
            // Enter this branch when schema is changed by `ALTER ... SET SCHEMA ...` statement.
            schema.create_source(proto);
            database
                .iter_schemas_mut()
                .find(|schema| {
                    schema.id() != proto.schema_id && schema.get_source_by_id(&proto.id).is_some()
                })
                .unwrap()
                .drop_source(proto.id);
        }
    }

    pub fn drop_sink(&mut self, db_id: DatabaseId, schema_id: SchemaId, sink_id: SinkId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_sink(sink_id);
    }

    pub fn drop_secret(&mut self, db_id: DatabaseId, schema_id: SchemaId, secret_id: SecretId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_secret(secret_id);
    }

    pub fn update_sink(&mut self, proto: &PbSink) {
        let database = self.get_database_mut(proto.database_id).unwrap();
        let schema = database.get_schema_mut(proto.schema_id).unwrap();
        if schema.get_sink_by_id(&proto.id).is_some() {
            schema.update_sink(proto);
        } else {
            // Enter this branch when schema is changed by `ALTER ... SET SCHEMA ...` statement.
            schema.create_sink(proto);
            database
                .iter_schemas_mut()
                .find(|schema| {
                    schema.id() != proto.schema_id && schema.get_sink_by_id(&proto.id).is_some()
                })
                .unwrap()
                .drop_sink(proto.id);
        }
    }

    pub fn drop_subscription(
        &mut self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        subscription_id: SubscriptionId,
    ) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_subscription(subscription_id);
    }

    pub fn update_subscription(&mut self, proto: &PbSubscription) {
        let database = self.get_database_mut(proto.database_id).unwrap();
        let schema = database.get_schema_mut(proto.schema_id).unwrap();
        if schema.get_subscription_by_id(&proto.id).is_some() {
            schema.update_subscription(proto);
        } else {
            // Enter this branch when schema is changed by `ALTER ... SET SCHEMA ...` statement.
            schema.create_subscription(proto);
            database
                .iter_schemas_mut()
                .find(|schema| {
                    schema.id() != proto.schema_id
                        && schema.get_subscription_by_id(&proto.id).is_some()
                })
                .unwrap()
                .drop_subscription(proto.id);
        }
    }

    pub fn drop_index(&mut self, db_id: DatabaseId, schema_id: SchemaId, index_id: IndexId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_index(index_id);
    }

    pub fn drop_view(&mut self, db_id: DatabaseId, schema_id: SchemaId, view_id: ViewId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_view(view_id);
    }

    pub fn update_view(&mut self, proto: &PbView) {
        let database = self.get_database_mut(proto.database_id).unwrap();
        let schema = database.get_schema_mut(proto.schema_id).unwrap();
        if schema.get_view_by_id(&proto.id).is_some() {
            schema.update_view(proto);
        } else {
            // Enter this branch when schema is changed by `ALTER ... SET SCHEMA ...` statement.
            schema.create_view(proto);
            database
                .iter_schemas_mut()
                .find(|schema| {
                    schema.id() != proto.schema_id && schema.get_view_by_id(&proto.id).is_some()
                })
                .unwrap()
                .drop_view(proto.id);
        }
    }

    pub fn drop_function(
        &mut self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        function_id: FunctionId,
    ) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_function(function_id);
    }

    pub fn update_function(&mut self, proto: &PbFunction) {
        let database = self.get_database_mut(proto.database_id).unwrap();
        let schema = database.get_schema_mut(proto.schema_id).unwrap();
        if schema.get_function_by_id(proto.id.into()).is_some() {
            schema.update_function(proto);
        } else {
            // Enter this branch when schema is changed by `ALTER ... SET SCHEMA ...` statement.
            schema.create_function(proto);
            database
                .iter_schemas_mut()
                .find(|schema| {
                    schema.id() != proto.schema_id
                        && schema.get_function_by_id(proto.id.into()).is_some()
                })
                .unwrap()
                .drop_function(proto.id.into());
        }

        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .update_function(proto);
    }

    pub fn get_database_by_name(&self, db_name: &str) -> CatalogResult<&DatabaseCatalog> {
        self.database_by_name
            .get(db_name)
            .ok_or_else(|| CatalogError::NotFound("database", db_name.to_owned()))
    }

    pub fn get_database_by_id(&self, db_id: &DatabaseId) -> CatalogResult<&DatabaseCatalog> {
        let db_name = self
            .db_name_by_id
            .get(db_id)
            .ok_or_else(|| CatalogError::NotFound("db_id", db_id.to_string()))?;
        self.database_by_name
            .get(db_name)
            .ok_or_else(|| CatalogError::NotFound("database", db_name.to_string()))
    }

    pub fn get_all_schema_names(&self, db_name: &str) -> CatalogResult<Vec<String>> {
        Ok(self.get_database_by_name(db_name)?.get_all_schema_names())
    }

    pub fn get_all_schema_info(&self, db_name: &str) -> CatalogResult<Vec<PbSchema>> {
        Ok(self.get_database_by_name(db_name)?.get_all_schema_info())
    }

    pub fn iter_schemas(
        &self,
        db_name: &str,
    ) -> CatalogResult<impl Iterator<Item = &SchemaCatalog>> {
        Ok(self.get_database_by_name(db_name)?.iter_schemas())
    }

    pub fn get_all_database_names(&self) -> Vec<String> {
        self.database_by_name.keys().cloned().collect_vec()
    }

    pub fn iter_databases(&self) -> impl Iterator<Item = &DatabaseCatalog> {
        self.database_by_name.values()
    }

    pub fn get_schema_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
    ) -> CatalogResult<&SchemaCatalog> {
        self.get_database_by_name(db_name)?
            .get_schema_by_name(schema_name)
            .ok_or_else(|| CatalogError::NotFound("schema", schema_name.to_owned()))
    }

    pub fn get_table_name_by_id(&self, table_id: TableId) -> CatalogResult<String> {
        self.get_any_table_by_id(&table_id)
            .map(|table| table.name.clone())
    }

    pub fn get_schema_by_id(
        &self,
        db_id: &DatabaseId,
        schema_id: &SchemaId,
    ) -> CatalogResult<&SchemaCatalog> {
        self.get_database_by_id(db_id)?
            .get_schema_by_id(schema_id)
            .ok_or_else(|| CatalogError::NotFound("schema_id", schema_id.to_string()))
    }

    pub fn get_source_by_id(
        &self,
        db_id: &DatabaseId,
        schema_id: &SchemaId,
        source_id: &SourceId,
    ) -> CatalogResult<&Arc<SourceCatalog>> {
        self.get_schema_by_id(db_id, schema_id)?
            .get_source_by_id(source_id)
            .ok_or_else(|| CatalogError::NotFound("source_id", source_id.to_string()))
    }

    /// Refer to [`SearchPath`].
    pub fn first_valid_schema(
        &self,
        db_name: &str,
        search_path: &SearchPath,
        user_name: &str,
    ) -> CatalogResult<&SchemaCatalog> {
        for path in search_path.real_path() {
            let mut schema_name: &str = path;
            if schema_name == USER_NAME_WILD_CARD {
                schema_name = user_name;
            }

            if let schema_catalog @ Ok(_) = self.get_schema_by_name(db_name, schema_name) {
                return schema_catalog;
            }
        }
        Err(CatalogError::NotFound(
            "first valid schema",
            "no schema has been selected to create in".to_owned(),
        ))
    }

    /// Used to get `TableCatalog` for Materialized Views, Tables and Indexes.
    /// Retrieves all tables, created or creating.
    pub fn get_any_table_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        table_name: &str,
    ) -> CatalogResult<(&Arc<TableCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_table_by_name(table_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("table", table_name.to_owned()))
    }

    /// Used to get `TableCatalog` for Materialized Views, Tables and Indexes.
    /// Retrieves only created tables.
    pub fn get_created_table_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        table_name: &str,
    ) -> CatalogResult<(&Arc<TableCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_created_table_by_name(table_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("table", table_name.to_owned()))
    }

    pub fn get_any_table_by_id(&self, table_id: &TableId) -> CatalogResult<&Arc<TableCatalog>> {
        self.table_by_id
            .get(table_id)
            .ok_or_else(|| CatalogError::NotFound("table id", table_id.to_string()))
    }

    /// This function is similar to `get_table_by_id` expect that a table must be in a given database.
    pub fn get_created_table_by_id_with_db(
        &self,
        db_name: &str,
        table_id: u32,
    ) -> CatalogResult<&Arc<TableCatalog>> {
        let table_id = TableId::from(table_id);
        for schema in self.get_database_by_name(db_name)?.iter_schemas() {
            if let Some(table) = schema.get_created_table_by_id(&table_id) {
                return Ok(table);
            }
        }
        Err(CatalogError::NotFound("table id", table_id.to_string()))
    }

    pub fn iter_table_names_with_filter(
        &self,
        filter: impl Fn(&TableCatalog) -> bool,
    ) -> impl Iterator<Item = &str> {
        self.table_by_id
            .values()
            .filter(|table| filter(table))
            .map(|table| table.name.as_str())
            .collect::<Vec<_>>()
            .into_iter()
    }

    pub fn get_schema_by_table_id(
        &self,
        db_name: &str,
        table_id: &TableId,
    ) -> CatalogResult<&SchemaCatalog> {
        self.database_by_name
            .get(db_name)
            .and_then(|db| db.find_schema_containing_table_id(table_id))
            .ok_or_else(|| CatalogError::NotFound("schema with table", table_id.to_string()))
    }

    // Used by test_utils only.
    pub fn alter_table_name_by_id(&mut self, table_id: &TableId, table_name: &str) {
        let (mut database_id, mut schema_id) = (0, 0);
        let mut found = false;
        for database in self.database_by_name.values() {
            if !found {
                for schema in database.iter_schemas() {
                    if schema.iter_user_table().any(|t| t.id() == *table_id) {
                        found = true;
                        database_id = database.id();
                        schema_id = schema.id();
                        break;
                    }
                }
            }
        }

        if found {
            let mut table = self
                .get_any_table_by_id(table_id)
                .unwrap()
                .to_prost(schema_id, database_id);
            table.name = table_name.to_owned();
            self.update_table(&table);
        }
    }

    #[cfg(test)]
    pub fn insert_table_id_mapping(&mut self, table_id: TableId, fragment_id: super::FragmentId) {
        self.table_by_id.insert(
            table_id,
            Arc::new(TableCatalog {
                fragment_id,
                ..Default::default()
            }),
        );
    }

    pub fn get_sys_table_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> CatalogResult<&Arc<SystemTableCatalog>> {
        self.get_schema_by_name(db_name, schema_name)
            .unwrap()
            .get_system_table_by_name(table_name)
            .ok_or_else(|| CatalogError::NotFound("table", table_name.to_owned()))
    }

    pub fn get_source_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        source_name: &str,
    ) -> CatalogResult<(&Arc<SourceCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_source_by_name(source_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("source", source_name.to_owned()))
    }

    pub fn get_sink_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        sink_name: &str,
    ) -> CatalogResult<(&Arc<SinkCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_sink_by_name(sink_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("sink", sink_name.to_owned()))
    }

    pub fn get_subscription_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        subscription_name: &str,
    ) -> CatalogResult<(&Arc<SubscriptionCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_subscription_by_name(subscription_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("subscription", subscription_name.to_owned()))
    }

    pub fn get_index_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        index_name: &str,
    ) -> CatalogResult<(&Arc<IndexCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_index_by_name(index_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("index", index_name.to_owned()))
    }

    pub fn get_index_by_id(
        &self,
        db_name: &str,
        index_id: u32,
    ) -> CatalogResult<&Arc<IndexCatalog>> {
        let index_id = IndexId::from(index_id);
        for schema in self.get_database_by_name(db_name)?.iter_schemas() {
            if let Some(index) = schema.get_index_by_id(&index_id) {
                return Ok(index);
            }
        }
        Err(CatalogError::NotFound("index", index_id.to_string()))
    }

    pub fn get_view_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        view_name: &str,
    ) -> CatalogResult<(&Arc<ViewCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_view_by_name(view_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("view", view_name.to_owned()))
    }

    pub fn get_view_by_id(&self, db_name: &str, view_id: u32) -> CatalogResult<Arc<ViewCatalog>> {
        for schema in self.get_database_by_name(db_name)?.iter_schemas() {
            if let Some(view) = schema.get_view_by_id(&ViewId::from(view_id)) {
                return Ok(view.clone());
            }
        }
        Err(CatalogError::NotFound("view", view_id.to_string()))
    }

    pub fn get_secret_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        secret_name: &str,
    ) -> CatalogResult<(&Arc<SecretCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_secret_by_name(secret_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("secret", secret_name.to_owned()))
    }

    pub fn get_connection_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        connection_name: &str,
    ) -> CatalogResult<(&Arc<ConnectionCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_connection_by_name(connection_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("connection", connection_name.to_owned()))
    }

    pub fn get_function_by_name_inputs<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        function_name: &str,
        inputs: &mut [ExprImpl],
    ) -> CatalogResult<(&Arc<FunctionCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_function_by_name_inputs(function_name, inputs))
            })?
            .ok_or_else(|| {
                CatalogError::NotFound(
                    "function",
                    format!(
                        "{}({})",
                        function_name,
                        inputs
                            .iter()
                            .map(|a| a.return_type().to_string())
                            .join(", ")
                    ),
                )
            })
    }

    pub fn get_function_by_name_args<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        function_name: &str,
        args: &[DataType],
    ) -> CatalogResult<(&Arc<FunctionCatalog>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_function_by_name_args(function_name, args))
            })?
            .ok_or_else(|| {
                CatalogError::NotFound(
                    "function",
                    format!(
                        "{}({})",
                        function_name,
                        args.iter().map(|a| a.to_string()).join(", ")
                    ),
                )
            })
    }

    /// Gets all functions with the given name.
    pub fn get_functions_by_name<'a>(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'a>,
        function_name: &str,
    ) -> CatalogResult<(Vec<&Arc<FunctionCatalog>>, &'a str)> {
        schema_path
            .try_find(|schema_name| {
                Ok(self
                    .get_schema_by_name(db_name, schema_name)?
                    .get_functions_by_name(function_name))
            })?
            .ok_or_else(|| CatalogError::NotFound("function", function_name.to_owned()))
    }

    /// Check if the name duplicates with existing table, materialized view or source.
    pub fn check_relation_name_duplicated(
        &self,
        db_name: &str,
        schema_name: &str,
        relation_name: &str,
    ) -> CatalogResult<()> {
        let schema = self.get_schema_by_name(db_name, schema_name)?;

        if let Some(table) = schema.get_created_table_by_name(relation_name) {
            if table.is_index() {
                Err(CatalogError::Duplicated("index", relation_name.to_owned()))
            } else if table.is_mview() {
                Err(CatalogError::Duplicated(
                    "materialized view",
                    relation_name.to_owned(),
                ))
            } else {
                Err(CatalogError::Duplicated("table", relation_name.to_owned()))
            }
        } else if schema.get_source_by_name(relation_name).is_some() {
            Err(CatalogError::Duplicated("source", relation_name.to_owned()))
        } else if schema.get_sink_by_name(relation_name).is_some() {
            Err(CatalogError::Duplicated("sink", relation_name.to_owned()))
        } else if schema.get_view_by_name(relation_name).is_some() {
            Err(CatalogError::Duplicated("view", relation_name.to_owned()))
        } else if schema.get_subscription_by_name(relation_name).is_some() {
            Err(CatalogError::Duplicated(
                "subscription",
                relation_name.to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    pub fn check_function_name_duplicated(
        &self,
        db_name: &str,
        schema_name: &str,
        function_name: &str,
        arg_types: &[DataType],
    ) -> CatalogResult<()> {
        let schema = self.get_schema_by_name(db_name, schema_name)?;

        if schema
            .get_function_by_name_args(function_name, arg_types)
            .is_some()
        {
            let name = format!(
                "{function_name}({})",
                arg_types.iter().map(|t| t.to_string()).join(",")
            );
            Err(CatalogError::Duplicated("function", name))
        } else {
            Ok(())
        }
    }

    /// Check if the name duplicates with existing connection.
    pub fn check_connection_name_duplicated(
        &self,
        db_name: &str,
        schema_name: &str,
        connection_name: &str,
    ) -> CatalogResult<()> {
        let schema = self.get_schema_by_name(db_name, schema_name)?;

        if schema.get_connection_by_name(connection_name).is_some() {
            Err(CatalogError::Duplicated(
                "connection",
                connection_name.to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    pub fn check_secret_name_duplicated(
        &self,
        db_name: &str,
        schema_name: &str,
        secret_name: &str,
    ) -> CatalogResult<()> {
        let schema = self.get_schema_by_name(db_name, schema_name)?;

        if schema.get_secret_by_name(secret_name).is_some() {
            Err(CatalogError::Duplicated("secret", secret_name.to_owned()))
        } else {
            Ok(())
        }
    }

    /// Get the catalog cache's catalog version.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Set the catalog cache's catalog version.
    pub fn set_version(&mut self, catalog_version: CatalogVersion) {
        self.version = catalog_version;
    }

    pub fn table_stats(&self) -> &HummockVersionStats {
        &self.table_stats
    }

    pub fn set_table_stats(&mut self, table_stats: HummockVersionStats) {
        self.table_stats = table_stats;
    }

    pub fn get_all_indexes_related_to_object(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
        mv_id: TableId,
    ) -> Vec<Arc<IndexCatalog>> {
        self.get_database_by_id(&db_id)
            .unwrap()
            .get_schema_by_id(&schema_id)
            .unwrap()
            .get_indexes_by_table_id(&mv_id)
    }

    pub fn get_id_by_class_name(
        &self,
        db_name: &str,
        schema_path: SchemaPath<'_>,
        class_name: &str,
    ) -> CatalogResult<u32> {
        schema_path
            .try_find(|schema_name| {
                let schema = self.get_schema_by_name(db_name, schema_name)?;
                #[allow(clippy::manual_map)]
                if let Some(item) = schema.get_system_table_by_name(class_name) {
                    Ok(Some(item.id().into()))
                } else if let Some(item) = schema.get_created_table_by_name(class_name) {
                    Ok(Some(item.id().into()))
                } else if let Some(item) = schema.get_index_by_name(class_name) {
                    Ok(Some(item.id.into()))
                } else if let Some(item) = schema.get_source_by_name(class_name) {
                    Ok(Some(item.id))
                } else if let Some(item) = schema.get_view_by_name(class_name) {
                    Ok(Some(item.id))
                } else {
                    Ok(None)
                }
            })?
            .map(|(id, _)| id)
            .ok_or_else(|| CatalogError::NotFound("class", class_name.to_owned()))
    }
}
