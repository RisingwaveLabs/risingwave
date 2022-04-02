// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::Result;
use risingwave_pb::meta::{Database, Schema, Table};
use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};

use crate::model::MetadataModel;
use crate::storage::MetaStore;

/// Column family name for table.
const TABLE_CF_NAME: &str = "cf/table";
/// Column family name for schema.
const SCHEMA_CF_NAME: &str = "cf/schema";
/// Column family name for database.
const DATABASE_CF_NAME: &str = "cf/database";
/// Column family name for catalog version.
const CATALOG_VERSION_CF_NAME: &str = "cf/catalog_version";
/// Catalog version id key.
/// `cf(catalog_version)`: `catalog_version_id_key` -> `CatalogVersion`
const CATALOG_VERSION_ID_KEY: &str = "version_id";
/// First catalog version id.
const FIRST_VERSION_ID: CatalogVersion = 1;

macro_rules! impl_model_for_catalog {
    ($name:ident, $cf:ident, $key_ty:ty, $key_fn:ident) => {
        impl MetadataModel for $name {
            type ProstType = Self;
            type KeyType = $key_ty;

            fn cf_name() -> String {
                $cf.to_string()
            }

            fn to_protobuf(&self) -> Self::ProstType {
                self.clone()
            }

            fn from_protobuf(prost: Self::ProstType) -> Self {
                prost
            }

            fn key(&self) -> Result<Self::KeyType> {
                Ok(self.$key_fn()?.clone())
            }
        }
    };
}

impl_model_for_catalog!(
    Database,
    DATABASE_CF_NAME,
    DatabaseRefId,
    get_database_ref_id
);
impl_model_for_catalog!(Schema, SCHEMA_CF_NAME, SchemaRefId, get_schema_ref_id);
impl_model_for_catalog!(Table, TABLE_CF_NAME, TableRefId, get_table_ref_id);

/// Generate catalog version id.
#[derive(Debug)]
pub struct CatalogVersionGenerator(CatalogVersion);

impl CatalogVersionGenerator {
    pub async fn new<S>(store: &S) -> Result<Self>
    where
        S: MetaStore,
    {
        Ok(Self::select(store, &CATALOG_VERSION_ID_KEY.to_string())
            .await?
            .unwrap_or(Self(FIRST_VERSION_ID)))
    }

    pub fn version(&self) -> CatalogVersion {
        self.0
    }

    pub fn next(&mut self) -> CatalogVersion {
        self.0 += 1;
        self.0
    }
}

impl MetadataModel for CatalogVersionGenerator {
    type ProstType = CatalogVersion;
    type KeyType = String;

    fn cf_name() -> String {
        CATALOG_VERSION_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.0
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self(prost)
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(CATALOG_VERSION_ID_KEY.to_string())
    }
}

#[cfg(test)]
mod tests {
    use futures::future;

    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_database() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let store = env.meta_store();
        let databases = Database::list(store).await?;
        assert!(databases.is_empty());
        assert!(Database::select(store, &DatabaseRefId { database_id: 0 })
            .await
            .unwrap()
            .is_none());

        future::join_all((0..100).map(|i| async move {
            Database {
                database_ref_id: Some(DatabaseRefId { database_id: i }),
                database_name: format!("database_{}", i),
                version: i as u64,
            }
            .insert(store)
            .await
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        for i in 0..100 {
            let database = Database::select(
                store,
                &DatabaseRefId {
                    database_id: i as i32,
                },
            )
            .await?
            .unwrap();
            assert_eq!(
                database,
                Database {
                    database_ref_id: Some(DatabaseRefId { database_id: i }),
                    database_name: format!("database_{}", i),
                    version: i as u64,
                }
            );
        }

        Database {
            database_ref_id: Some(DatabaseRefId { database_id: 0 }),
            database_name: "database_0".to_string(),
            version: 101,
        }
        .insert(store)
        .await?;

        let databases = Database::list(store).await?;
        assert_eq!(databases.len(), 100);

        for i in 0..100 {
            assert!(Database::delete(store, &DatabaseRefId { database_id: i })
                .await
                .is_ok());
        }
        let databases = Database::list(store).await?;
        assert!(databases.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_catalog_version_generator() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let store = env.meta_store();
        let mut version = CatalogVersionGenerator::new(store).await.unwrap();
        assert_eq!(FIRST_VERSION_ID, version.version());

        assert_eq!(FIRST_VERSION_ID + 1, version.next());
        assert_eq!(FIRST_VERSION_ID + 1, version.version());
        version.insert(store).await?;

        let version = CatalogVersionGenerator::new(store).await.unwrap();
        assert_eq!(FIRST_VERSION_ID + 1, version.version());

        Ok(())
    }
}
