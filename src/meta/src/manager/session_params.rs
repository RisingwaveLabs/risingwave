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

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use risingwave_common::session_config::{SessionConfig, SessionConfigError};
use thiserror_ext::AsReport;
use tokio::sync::RwLock;
use tracing::info;

use crate::model::{ValTransaction, VarTransaction};
use crate::storage::{MetaStore, MetaStoreRef, Snapshot, Transaction};
use crate::{MetaError, MetaResult};

pub type SessionParamsManagerRef = Arc<SessionParamsManager>;

/// Manages the global default session params on meta.
/// Note that the session params in each session will be initialized from the default value here.
pub struct SessionParamsManager {
    meta_store: MetaStoreRef,
    // Cached parameters.
    params: RwLock<SessionConfig>,
}

impl SessionParamsManager {
    /// Return error if `init_params` conflict with persisted system params.
    pub async fn new(
        meta_store: MetaStoreRef,
        init_params: SessionConfig,
        cluster_first_launch: bool,
    ) -> MetaResult<Self> {
        let params = if cluster_first_launch {
            init_params
        } else if let Some(params) =
            <SessionConfig as SessionParamsModel>::get(&meta_store, init_params).await?
        {
            params
        } else {
            return Err(MetaError::system_params(
                "cluster is not newly created but no system parameters can be found",
            ));
        };

        info!("system parameters: {:?}", params);

        Ok(Self {
            meta_store,
            params: RwLock::new(params.clone()),
        })
    }

    pub async fn get_params(&self) -> SessionConfig {
        self.params.read().await.clone()
    }

    pub async fn set_param(&self, name: &str, value: Option<String>) -> MetaResult<String> {
        let mut params_guard = self.params.write().await;
        let params = params_guard.deref_mut();
        let mut mem_txn = VarTransaction::new(params);

        // FIXME: use a real reporter
        let reporter = &mut ();
        if let Some(value) = value {
            mem_txn.set(name, value, reporter)?;
        } else {
            mem_txn.reset(name, reporter)?;
        }
        let mut store_txn = Transaction::default();
        mem_txn.apply_to_txn(&mut store_txn).await?;
        self.meta_store.txn(store_txn).await?;

        mem_txn.commit();

        Ok(params.get(name)?)
    }

    /// Flush the cached params to meta store.
    pub async fn flush_params(&self) -> MetaResult<()> {
        Ok(SessionConfig::insert(self.params.read().await.deref(), &self.meta_store).await?)
    }
}

use async_trait::async_trait;

use crate::model::{MetadataModelResult, Transactional};

const SESSION_PARAMS_CF_NAME: &str = "cf/session_params";

// A dummy trait to implement custom methods on `SessionParams`.
#[async_trait]
pub trait SessionParamsModel: Sized {
    fn cf_name() -> String;
    async fn get<S: MetaStore>(
        store: &S,
        init_params: SessionConfig,
    ) -> MetadataModelResult<Option<Self>>;
    async fn get_at_snapshot<S: MetaStore>(
        store: &S::Snapshot,
        init_params: SessionConfig,
    ) -> MetadataModelResult<Option<Self>>;
    async fn insert<S: MetaStore>(&self, store: &S) -> MetadataModelResult<()>;
}

#[async_trait]
impl SessionParamsModel for SessionConfig {
    fn cf_name() -> String {
        SESSION_PARAMS_CF_NAME.to_string()
    }

    /// Return error if there are missing or unrecognized fields.
    async fn get<S>(store: &S, init_params: SessionConfig) -> MetadataModelResult<Option<Self>>
    where
        S: MetaStore,
    {
        Self::get_at_snapshot::<S>(&store.snapshot().await, init_params).await
    }

    async fn get_at_snapshot<S>(
        snapshot: &S::Snapshot,
        mut init_params: SessionConfig,
    ) -> MetadataModelResult<Option<SessionConfig>>
    where
        S: MetaStore,
    {
        let kvs = snapshot.list_cf(&Self::cf_name()).await?;
        if kvs.is_empty() {
            Ok(None)
        } else {
            for (k, v) in kvs {
                let k = std::str::from_utf8(k.as_ref()).unwrap();
                let v = std::str::from_utf8(v.as_ref()).unwrap();
                if let Err(e) = init_params.set(k, v.to_string(), &mut ()) {
                    match e {
                        SessionConfigError::InvalidValue { .. } => {
                            tracing::error!(error = %e.as_report(), "failed to set parameter from meta database, using default value {}", init_params.get(k).unwrap())
                        }
                        SessionConfigError::UnrecognizedEntry(_) => {
                            tracing::error!(error = %e.as_report(), "failed to set parameter from meta database")
                        }
                    }
                }
            }
            Ok(Some(init_params))
        }
    }

    /// All undeprecated fields must be `Some`.
    /// Return error if there are missing fields.
    async fn insert<S>(&self, store: &S) -> MetadataModelResult<()>
    where
        S: MetaStore,
    {
        let mut txn = Transaction::default();
        self.upsert_in_transaction(&mut txn).await?;
        Ok(store.txn(txn).await?)
    }
}

#[async_trait]
impl Transactional<Transaction> for SessionConfig {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        for (k, v) in self
            .list_all()
            .iter()
            .map(|info| (info.name.clone(), info.setting.clone()))
        {
            trx.put(Self::cf_name(), k.into_bytes(), v.into_bytes());
        }
        Ok(())
    }

    async fn delete_in_transaction(&self, _trx: &mut Transaction) -> MetadataModelResult<()> {
        unreachable!()
    }
}
