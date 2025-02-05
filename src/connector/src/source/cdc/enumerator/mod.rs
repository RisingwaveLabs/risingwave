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

use std::marker::PhantomData;
use std::ops::Deref;
use std::str::FromStr;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use itertools::Itertools;
use prost::Message;
use risingwave_common::util::addr::HostAddr;
use risingwave_jni_core::call_static_method;
use risingwave_jni_core::jvm_runtime::{execute_with_jni_env, JVM};
use risingwave_pb::connector_service::{SourceType, ValidateSourceRequest, ValidateSourceResponse};

use crate::error::ConnectorResult;
use crate::source::cdc::{
    table_schema_exclude_additional_columns, CdcProperties, CdcSourceTypeTrait, Citus,
    DebeziumCdcSplit, Mongodb, Mysql, Postgres, SqlServer,
};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub const DATABASE_SERVERS_KEY: &str = "database.servers";

#[derive(Debug)]
pub struct DebeziumSplitEnumerator<T: CdcSourceTypeTrait> {
    /// The `source_id` in the catalog
    source_id: u32,
    worker_node_addrs: Vec<HostAddr>,
    _phantom: PhantomData<T>,
}

#[async_trait]
impl<T: CdcSourceTypeTrait> SplitEnumerator for DebeziumSplitEnumerator<T>
where
    Self: ListCdcSplits<CdcSourceType = T>,
{
    type Properties = CdcProperties<T>;
    type Split = DebeziumCdcSplit<T>;

    async fn new(
        props: CdcProperties<T>,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        let server_addrs = props
            .properties
            .get(DATABASE_SERVERS_KEY)
            .map(|s| {
                s.split(',')
                    .map(HostAddr::from_str)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();

        assert_eq!(
            props.get_source_type_pb(),
            SourceType::from(T::source_type())
        );

        let jvm = JVM.get_or_init()?;
        let source_id = context.info.source_id;
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            execute_with_jni_env(jvm, |env| {
                let validate_source_request = ValidateSourceRequest {
                    source_id: source_id as u64,
                    source_type: props.get_source_type_pb() as _,
                    properties: props.properties,
                    table_schema: Some(table_schema_exclude_additional_columns(
                        &props.table_schema,
                    )),
                    is_source_job: props.is_cdc_source_job,
                    is_backfill_table: props.is_backfill_table,
                };
                println!(
                    "WKXLOG after validate cdc source properties, request: {:?}",
                    validate_source_request
                );

                let validate_source_request_bytes =
                    env.byte_array_from_slice(&Message::encode_to_vec(&validate_source_request))?;

                let validate_source_response_bytes = call_static_method!(
                    env,
                    {com.risingwave.connector.source.JniSourceValidateHandler},
                    {byte[] validate(byte[] validateSourceRequestBytes)},
                    &validate_source_request_bytes
                )?;

                let validate_source_response: ValidateSourceResponse = Message::decode(
                    risingwave_jni_core::to_guarded_slice(&validate_source_response_bytes, env)?
                        .deref(),
                )?;

                if let Some(error) = validate_source_response.error {
                    return Err(
                        anyhow!(error.error_message).context("source cannot pass validation")
                    );
                }

                Ok(())
            })
        })
        .await
        .context("failed to validate source")??;

        tracing::debug!("validate cdc source properties success");
        Ok(Self {
            source_id,
            worker_node_addrs: server_addrs,
            _phantom: PhantomData,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<DebeziumCdcSplit<T>>> {
        Ok(self.list_cdc_splits())
    }
}

pub trait ListCdcSplits {
    type CdcSourceType: CdcSourceTypeTrait;
    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>>;
}

impl ListCdcSplits for DebeziumSplitEnumerator<Mysql> {
    type CdcSourceType = Mysql;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id,
            None,
            None,
        )]
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Postgres> {
    type CdcSourceType = Postgres;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id,
            None,
            None,
        )]
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<Citus> {
    type CdcSourceType = Citus;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        self.worker_node_addrs
            .iter()
            .enumerate()
            .map(|(id, addr)| {
                DebeziumCdcSplit::<Self::CdcSourceType>::new(
                    id as u32,
                    None,
                    Some(addr.to_string()),
                )
            })
            .collect_vec()
    }
}
impl ListCdcSplits for DebeziumSplitEnumerator<Mongodb> {
    type CdcSourceType = Mongodb;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        // CDC source only supports single split
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id,
            None,
            None,
        )]
    }
}

impl ListCdcSplits for DebeziumSplitEnumerator<SqlServer> {
    type CdcSourceType = SqlServer;

    fn list_cdc_splits(&mut self) -> Vec<DebeziumCdcSplit<Self::CdcSourceType>> {
        vec![DebeziumCdcSplit::<Self::CdcSourceType>::new(
            self.source_id,
            None,
            None,
        )]
    }
}
