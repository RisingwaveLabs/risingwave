mod catalog;
mod cluster;
mod stream;

use async_trait::async_trait;
pub use catalog::*;
pub use cluster::*;
use prost::Message;
use risingwave_common::error::Result;
pub use stream::*;

use crate::storage::{self, ColumnFamilyUtils, MetaStore, Operation, Transaction};

pub type ActorId = u32;
pub type FragmentId = u32;

/// `MetadataModel` defines basic model operations in CRUD.
#[async_trait]
pub trait MetadataModel: Sized {
    /// Serialized prost message type.
    type ProstType: Message + Default;
    /// Serialized key type.
    type KeyType: Message;

    /// Column family for this model.
    fn cf_name() -> String;

    /// Serialize to protobuf.
    fn to_protobuf(&self) -> Self::ProstType;

    /// Serialize to protobuf encoded byte vector.
    fn to_protobuf_encoded_vec(&self) -> Vec<u8> {
        self.to_protobuf().encode_to_vec()
    }

    /// Deserialize from protobuf.
    fn from_protobuf(prost: Self::ProstType) -> Self;

    /// Current record key.
    fn key(&self) -> Result<Self::KeyType>;

    /// `list` returns all records in this model.
    async fn list<S>(store: &S) -> Result<Vec<Self>>
    where
        S: MetaStore,
    {
        let bytes_vec = store.list_cf(&Self::cf_name()).await?;
        Ok(bytes_vec
            .iter()
            .map(|bytes| Self::from_protobuf(Self::ProstType::decode(bytes.as_slice()).unwrap()))
            .collect::<Vec<_>>())
    }

    /// `insert` insert a new record in meta store, replaced it if the record already exist.
    async fn insert<S>(&self, store: &S) -> Result<()>
    where
        S: MetaStore,
    {
        store
            .put_cf(
                &Self::cf_name(),
                self.key()?.encode_to_vec(),
                self.to_protobuf().encode_to_vec(),
            )
            .await
            .map_err(Into::into)
    }

    /// `delete` drop records from meta store with associated key.
    async fn delete<S>(store: &S, key: &Self::KeyType) -> Result<()>
    where
        S: MetaStore,
    {
        store
            .delete_cf(&Self::cf_name(), &key.encode_to_vec())
            .await
            .map_err(Into::into)
    }

    /// `select` query a record with associated key and version.
    async fn select<S>(store: &S, key: &Self::KeyType) -> Result<Option<Self>>
    where
        S: MetaStore,
    {
        let byte_vec = match store.get_cf(&Self::cf_name(), &key.encode_to_vec()).await {
            Ok(byte_vec) => byte_vec,
            Err(err) => {
                if !matches!(err, storage::Error::ItemNotFound(_)) {
                    return Err(err.into());
                }
                return Ok(None);
            }
        };
        let model = Self::from_protobuf(Self::ProstType::decode(byte_vec.as_slice())?);
        Ok(Some(model))
    }
}

#[async_trait]
pub trait MetadataUserCfModel: MetadataModel {
    /// use `list_with_cf_suffix` instead in `MetadataUserCfModel`
    async fn list<S>(_: &S) -> Result<Vec<Self>>
    where
        S: MetaStore,
    {
        unimplemented!();
    }

    /// `list_with_cf_suffix` returns all records in this model which satisfies extending cf.
    async fn list_with_cf_suffix<S>(store: &S, cf_name: &str) -> Result<Vec<Self>>
    where
        S: MetaStore,
    {
        let bytes_vec = store
            .list_cf(&ColumnFamilyUtils::get_composed_cf(
                &Self::cf_name(),
                cf_name,
            ))
            .await?;
        Ok(bytes_vec
            .iter()
            .map(|bytes| Self::from_protobuf(Self::ProstType::decode(bytes.as_slice()).unwrap()))
            .collect::<Vec<_>>())
    }

    /// use `select_with_cf_suffix` instead in `MetadataUserCfModel`
    async fn select<S>(_: &S, _: &Self::KeyType) -> Result<Option<Self>>
    where
        S: MetaStore,
    {
        unimplemented!();
    }

    /// `select_with_cf_suffix` query a record with associated key and version.
    async fn select_with_cf_suffix<S>(
        store: &S,
        cf_name: &str,
        key: &Self::KeyType,
    ) -> Result<Option<Self>>
    where
        S: MetaStore,
    {
        let byte_vec = match store
            .get_cf(
                &ColumnFamilyUtils::get_composed_cf(&Self::cf_name(), cf_name),
                &key.encode_to_vec(),
            )
            .await
        {
            Ok(byte_vec) => byte_vec,
            Err(err) => {
                if !matches!(err, storage::Error::ItemNotFound(_)) {
                    return Err(err.into());
                }
                return Ok(None);
            }
        };
        let model = Self::from_protobuf(Self::ProstType::decode(byte_vec.as_slice())?);
        Ok(Some(model))
    }
}

/// `Transactional` defines operations supported in a transaction.
/// Read operations can be supported if necessary.
pub trait Transactional: MetadataModel {
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> risingwave_common::error::Result<()> {
        trx.add_operations(vec![Operation::Put {
            cf: Self::cf_name(),
            key: self.key()?.encode_to_vec(),
            value: self.to_protobuf_encoded_vec(),
        }]);
        Ok(())
    }

    fn delete_in_transaction(&self, trx: &mut Transaction) -> risingwave_common::error::Result<()> {
        trx.add_operations(vec![Operation::Delete {
            cf: Self::cf_name(),
            key: self.key()?.encode_to_vec(),
        }]);
        Ok(())
    }
}

pub trait TransactionalUserCf: Transactional + MetadataUserCfModel {
    /// use `upsert_in_transaction_with_cf` instead in `TransactionalUserCf`
    fn upsert_in_transaction(&self, _: &mut Transaction) -> risingwave_common::error::Result<()> {
        unimplemented!();
    }

    fn upsert_in_transaction_with_cf(
        &self,
        cf_name: &str,
        trx: &mut Transaction,
    ) -> risingwave_common::error::Result<()> {
        trx.add_operations(vec![Operation::Put {
            cf: ColumnFamilyUtils::get_composed_cf(&Self::cf_name(), cf_name),
            key: self.key()?.encode_to_vec(),
            value: self.to_protobuf_encoded_vec(),
        }]);
        Ok(())
    }

    /// use `delete_in_transaction_with_cf` instead in `TransactionalUserCf`
    fn delete_in_transaction(&self, _: &mut Transaction) -> risingwave_common::error::Result<()> {
        unimplemented!();
    }

    fn delete_in_transaction_with_cf(
        &self,
        cf_name: &str,
        trx: &mut Transaction,
    ) -> risingwave_common::error::Result<()> {
        trx.add_operations(vec![Operation::Delete {
            cf: ColumnFamilyUtils::get_composed_cf(&Self::cf_name(), cf_name),
            key: self.key()?.encode_to_vec(),
        }]);
        Ok(())
    }
}
