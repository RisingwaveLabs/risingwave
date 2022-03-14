use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::catalog::{
    Database as ProstDatabase, Schema as ProstSchema, Table as ProstTable,
};
use risingwave_pb::plan::{ColumnDesc, DatabaseRefId, SchemaRefId, TableRefId};
use risingwave_rpc_client::MetaClient;
use tokio::sync::watch::Receiver;

use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::table_catalog::{TableCatalog, ROWID_NAME};
use crate::catalog::{CatalogError, DatabaseId, SchemaId};

pub const DEFAULT_DATABASE_NAME: &str = "dev";
pub const DEFAULT_SCHEMA_NAME: &str = "dev";

#[derive(Default)]
pub struct CatalogCache {
    catalog_version: CatalogVersion,
    database_by_name: HashMap<String, DatabaseCatalog>,
    db_name_by_id: HashMap<DatabaseId, String>,
}

/// Root catalog of database catalog. Manage all database/schema/table in memory.
/// It is used in [`CatalogConnector`] and [`ObserverManager`].
/// `pub` create/delete methods are used in [`ObserverManager`].
///
/// - catalog cache (root catalog)
///   - database catalog
///     - schema catalog
///       - table catalog
///        - column catalog
impl CatalogCache {
    pub async fn new(client: MetaClient) -> Result<Self> {
        client.get_catalog().await.and_then(TryInto::try_into)
    }
    fn get_database_mut(&mut self, db_id: DatabaseId) -> Option<&mut DatabaseCatalog> {
        let name = self.db_name_by_id.get(&db_id)?;
        self.database_by_name.get_mut(name)
    }

    pub fn create_database(&mut self, db: &ProstDatabase) {
        let name = db.name;
        let id = db.id.into();

        self.database_by_name.try_insert(name, db.into()).unwrap();
        self.db_name_by_id.try_insert(id, name).unwrap();
    }
    pub fn create_table(&mut self, proto: &ProstTable) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_table(proto);
    }
    pub fn create_schema(&mut self, proto: &ProstSchema) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .create_schema(proto);
    }

    pub fn get_database_by_name(&self, db_name: &str) -> Option<&DatabaseCatalog> {
        self.database_by_name.get(db_name)
    }

    pub fn get_schema_by_name(&self, db_name: &str, schema_name: &str) -> Option<&SchemaCatalog> {
        self.get_database_by_name(db_name)?
            .get_schema_by_name(schema_name)
    }

    pub fn get_table_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<&TableCatalog> {
        self.get_schema_by_name(db_name, schema_name)?
            .get_table_by_name(table_name)
    }
    fn get_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<&TableCatalog> {
        self.get_schema(db_name, schema_name)
            .and_then(|schema| schema.get_table_by_name(table_name))
    }

    pub fn drop_table(&mut self, db_name: &str, schema_name: &str, table_name: &str) -> Result<()> {
        self.get_schema_mut(db_name, schema_name).map_or(
            Err(CatalogError::NotFound("schema", schema_name.to_string()).into()),
            |schema| schema.drop_table(table_name),
        )
    }

    pub fn drop_schema(&mut self, db_name: &str, schema_name: &str) -> Result<()> {
        self.get_database_mut(db_name).map_or(
            Err(CatalogError::NotFound("database", db_name.to_string()).into()),
            |db| db.drop_schema(schema_name),
        )
    }

    pub fn drop_database(&mut self, db_name: &str) -> Result<()> {
        let database = self.database_by_name.remove(db_name).ok_or_else(|| {
            RwError::from(CatalogError::NotFound("database", db_name.to_string()))
        })?;
        self.db_name_by_id.remove(&database.id()).ok_or_else(|| {
            RwError::from(CatalogError::NotFound(
                "database id",
                database.id().to_string(),
            ))
        })?;
        Ok(())
    }
}

impl TryFrom<Catalog> for CatalogCache {
    type Error = RwError;

    fn try_from(catalog: Catalog) -> Result<Self> {
        let mut cache = CatalogCache {
            catalog_version: catalog.get_version(),
            database_by_name: HashMap::new(),
            db_name_by_id: HashMap::new(),
        };

        for database in catalog.get_databases() {
            let db_name = database.get_database_name();
            let db_id = database.get_database_ref_id()?.database_id as DatabaseId;
            cache.create_database(db_name, db_id)?;
        }

        for schema in catalog.get_schemas() {
            let schema_ref_id = schema.get_schema_ref_id()?;
            let db_id = schema_ref_id.get_database_ref_id()?.database_id as DatabaseId;
            let db_name = cache
                .get_database_name(db_id)
                .ok_or_else(|| CatalogError::NotFound("database id", db_id.to_string()))?;
            let schema_name = schema.get_schema_name();
            let schema_id = schema_ref_id.schema_id as SchemaId;
            cache.create_schema(&db_name, schema_name, schema_id)?;
        }

        for table in catalog.get_tables() {
            let schema_ref_id = table.get_table_ref_id()?.get_schema_ref_id()?;
            let db_id = schema_ref_id.get_database_ref_id()?.database_id as DatabaseId;
            let db_name = cache
                .get_database_name(db_id)
                .ok_or_else(|| CatalogError::NotFound("database id", db_id.to_string()))?;
            let schema_id = schema_ref_id.schema_id as SchemaId;
            let schema_name = cache
                .get_database(&db_name)
                .unwrap()
                .get_schema_name(schema_id)
                .ok_or_else(|| CatalogError::NotFound("schema id", schema_id.to_string()))?;
            cache.create_table(&db_name, &schema_name, table)?;
        }

        Ok(cache)
    }
}

/// For DDL (create table/schema/database), only send rpc to meta. Create and delete actions will be
/// done by `ObserverManager`. For get catalog request (get table/schema/database), check the root
/// catalog cache only. Should be used by DDL handler.
///
/// Some changes need to be done in future:
/// 1. Support more fields for ddl in future (#2473)
/// 2. MVCC of schema (`version` flag in message) (#2474).
#[derive(Clone)]
pub struct CatalogConnector {
    meta_client: MetaClient,
    catalog_cache: Arc<RwLock<CatalogCache>>,
    catalog_updated_rx: Receiver<CatalogVersion>,
}

impl CatalogConnector {
    pub fn new(
        meta_client: MetaClient,
        catalog_cache: Arc<RwLock<CatalogCache>>,
        catalog_updated_rx: Receiver<CatalogVersion>,
    ) -> Self {
        Self {
            meta_client,
            catalog_cache,
            catalog_updated_rx,
        }
    }

    async fn wait_version(&self, version: CatalogVersion) -> Result<()> {
        let mut rx = self.catalog_updated_rx.clone();
        while *rx.borrow_and_update() < version {
            rx.changed()
                .await
                .map_err(|e| RwError::from(InternalError(e.to_string())))?;
        }
        Ok(())
    }

    pub async fn create_database(&self, db_name: &str) -> Result<()> {
        let (_, version) = self
            .meta_client
            .create_database(Database {
                database_name: db_name.to_string(),
                // Do not support MVCC DDL now.
                ..Default::default()
            })
            .await?;
        self.wait_version(version).await
    }

    pub async fn create_schema(&self, db_name: &str, schema_name: &str) -> Result<()> {
        let database_id = self
            .catalog_cache
            .read()
            .unwrap()
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id();
        let (_, version) = self
            .meta_client
            .create_schema(Schema {
                schema_name: schema_name.to_string(),
                version: 0,
                schema_ref_id: Some(SchemaRefId {
                    database_ref_id: Some(DatabaseRefId {
                        database_id: database_id as i32,
                    }),
                    schema_id: 0,
                }),
            })
            .await?;
        self.wait_version(version).await
    }

    pub async fn create_table(
        &self,
        db_name: &str,
        schema_name: &str,
        mut table: Table,
    ) -> Result<()> {
        let database_id = self
            .catalog_cache
            .read()
            .unwrap()
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id() as i32;
        let schema_id = self
            .catalog_cache
            .read()
            .unwrap()
            .get_schema(db_name, schema_name)
            .ok_or_else(|| {
                RwError::from(CatalogError::NotFound("schema", schema_name.to_string()))
            })?
            .id() as i32;
        let schema_ref_id = Some(SchemaRefId {
            database_ref_id: Some(DatabaseRefId { database_id }),
            schema_id,
        });
        table.table_ref_id = Some(TableRefId {
            schema_ref_id: schema_ref_id.clone(),
            table_id: 0,
        });
        // Append hidden column ROWID.
        table.column_descs.insert(
            0,
            ColumnDesc {
                name: ROWID_NAME.to_string(),
                column_type: Some(DataType::Int64.to_protobuf()?),
                ..Default::default()
            },
        );
        let (_, version) = self.meta_client.create_table(table.clone()).await?;
        self.wait_version(version).await
    }

    pub async fn drop_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let table_id = self
            .catalog_cache
            .read()
            .unwrap()
            .get_table(db_name, schema_name, table_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("table", table_name.to_string())))?
            .id();

        let table_ref_id = TableRefId::from(&table_id);
        let version = self.meta_client.drop_table(table_ref_id).await?;
        self.wait_version(version).await
    }

    pub async fn drop_schema(&self, db_name: &str, schema_name: &str) -> Result<()> {
        let database_id = self
            .catalog_cache
            .read()
            .unwrap()
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id() as i32;
        let schema_id = self
            .catalog_cache
            .read()
            .unwrap()
            .get_schema(db_name, schema_name)
            .ok_or_else(|| {
                RwError::from(CatalogError::NotFound("schema", schema_name.to_string()))
            })?
            .id() as i32;

        let schema_ref_id = SchemaRefId {
            database_ref_id: Some(DatabaseRefId { database_id }),
            schema_id,
        };
        let version = self.meta_client.drop_schema(schema_ref_id).await?;
        self.wait_version(version).await
    }

    pub async fn drop_database(&self, db_name: &str) -> Result<()> {
        let database_id = self
            .catalog_cache
            .read()
            .unwrap()
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id() as i32;
        let database_ref_id = DatabaseRefId { database_id };
        let version = self.meta_client.drop_database(database_ref_id).await?;
        self.wait_version(version).await
    }

    pub fn get_database_snapshot(&self, db_name: &str) -> Option<Arc<DatabaseCatalog>> {
        self.catalog_cache
            .read()
            .unwrap()
            .get_database_snapshot(db_name)
    }

    /// Get catalog will not query meta service. The sync of schema is done by periodically push of
    /// meta. Frontend should not pull and update the catalog voluntarily.
    #[cfg(test)]
    pub fn get_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<TableCatalog> {
        self.catalog_cache
            .read()
            .unwrap()
            .get_table(db_name, schema_name, table_name)
            .cloned()
    }

    pub fn get_database(&self, db_name: &str) -> Option<DatabaseCatalog> {
        self.catalog_cache
            .read()
            .unwrap()
            .get_database(db_name)
            .cloned()
    }

    pub fn get_schema(&self, db_name: &str, schema_name: &str) -> Option<SchemaCatalog> {
        self.catalog_cache
            .read()
            .unwrap()
            .get_schema(db_name, schema_name)
            .cloned()
    }
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, RwLock};

    use risingwave_common::types::DataType;
    use risingwave_pb::meta::table::Info;
    use risingwave_pb::plan::{ColumnDesc, TableSourceInfo};
    use risingwave_rpc_client::MetaClient;
    use tokio::sync::watch;

    use crate::catalog::catalog_service::{
        CatalogCache, CatalogConnector, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };
    use crate::observer::observer_manager::ObserverManager;
    use crate::scheduler::schedule::WorkerNodeManager;
    use crate::test_utils::FrontendMockMetaClient;

    fn create_test_table(test_table_name: &str, columns: Vec<(String, DataType)>) -> Table {
        let column_descs = columns
            .iter()
            .map(|c| ColumnDesc {
                name: c.0.clone(),
                column_type: Some(c.1.to_protobuf().unwrap()),
                ..Default::default()
            })
            .collect();
        Table {
            table_name: test_table_name.to_string(),
            column_descs,
            info: Info::TableSource(TableSourceInfo::default()).into(),
            ..Default::default()
        }
    }

    use risingwave_pb::meta::Table;

    use crate::catalog::table_catalog::ROWID_NAME;

    #[tokio::test]
    async fn test_create_and_drop_table() {
        // Init meta and catalog.
        let meta_client = MetaClient::mock(FrontendMockMetaClient::new().await);

        let (catalog_updated_tx, catalog_updated_rx) = watch::channel(0);
        let catalog_cache = Arc::new(RwLock::new(
            CatalogCache::new(meta_client.clone()).await.unwrap(),
        ));
        let catalog_mgr = CatalogConnector::new(
            meta_client.clone(),
            catalog_cache.clone(),
            catalog_updated_rx,
        );

        let worker_node_manager =
            Arc::new(WorkerNodeManager::new(meta_client.clone()).await.unwrap());

        let observer_manager = ObserverManager::new(
            meta_client.clone(),
            "127.0.0.1:12345".parse().unwrap(), // Random value, not used here.
            worker_node_manager,
            catalog_cache,
            catalog_updated_tx,
        )
        .await;
        observer_manager.start();

        // Create db and schema.
        catalog_mgr
            .create_database(DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        assert!(catalog_mgr.get_database(DEFAULT_DATABASE_NAME).is_some());
        catalog_mgr
            .create_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .await
            .unwrap();
        assert!(catalog_mgr
            .get_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .is_some());

        // Create table.
        let test_table_name = "t";
        let table = create_test_table(
            test_table_name,
            vec![
                ("v1".to_string(), DataType::Int32),
                ("v2".to_string(), DataType::Int32),
            ],
        );
        catalog_mgr
            .create_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, table)
            .await
            .unwrap();
        assert!(catalog_mgr
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .is_some());

        // Get catalog from meta and check the table info.
        let catalog = meta_client.get_catalog().await.unwrap();
        assert_eq!(catalog.tables.len(), 1);
        assert_eq!(catalog.tables[0].table_name, test_table_name);
        let expected_table = create_test_table(
            test_table_name,
            vec![
                (ROWID_NAME.to_string(), DataType::Int64),
                ("v1".to_string(), DataType::Int32),
                ("v2".to_string(), DataType::Int32),
            ],
        );
        assert_eq!(catalog.tables[0].column_descs, expected_table.column_descs);

        // -----  test drop table, schema and database  -----

        catalog_mgr
            .drop_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .await
            .unwrap();
        // Ensure the table has been dropped from cache.
        assert!(catalog_mgr
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .is_none());
        // Ensure the table has been dropped from meta.
        let catalog = meta_client.get_catalog().await.unwrap();
        assert_eq!(catalog.tables.len(), 0);

        catalog_mgr
            .drop_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .await
            .unwrap();
        // Ensure the schema has been dropped from cache.
        assert!(catalog_mgr
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .is_none());
        // Ensure the schema has been dropped from meta.
        let catalog = meta_client.get_catalog().await.unwrap();
        assert_eq!(catalog.schemas.len(), 0);

        catalog_mgr
            .drop_database(DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        // Ensure the db has been dropped from cache.
        assert!(catalog_mgr
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .is_none());
        // Ensure the db has been dropped from meta.
        let catalog = meta_client.get_catalog().await.unwrap();
        assert_eq!(catalog.databases.len(), 0);
    }
}
