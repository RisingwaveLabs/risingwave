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

use core::num::NonZeroU64;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use deltalake::aws::storage::s3_constants::{
    AWS_ACCESS_KEY_ID, AWS_ALLOW_HTTP, AWS_ENDPOINT_URL, AWS_REGION, AWS_S3_ALLOW_UNSAFE_RENAME,
    AWS_SECRET_ACCESS_KEY,
};
use deltalake::kernel::{Action, Add, DataType as DeltaLakeDataType, PrimitiveType, StructType};
use deltalake::operations::transaction::CommitBuilder;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::DeltaTable;
use risingwave_common::array::arrow::DeltaLakeConvert;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_pb::connector_service::sink_metadata::Metadata::Serialized;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::connector_service::SinkMetadata;
use serde_derive::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use with_options::WithOptions;

use super::coordinate::CoordinatedSinkWriter;
use super::decouple_checkpoint_log_sink::{
    default_commit_checkpoint_interval, DecoupleCheckpointLogSinkerOf,
};
use super::writer::SinkWriter;
use super::{
    Result, Sink, SinkCommitCoordinator, SinkError, SinkParam, SinkWriterMetrics, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_USER_FORCE_APPEND_ONLY_OPTION,
};
use crate::connector_common::AwsAuthProps;

pub const DELTALAKE_SINK: &str = "deltalake";
pub const DEFAULT_REGION: &str = "us-east-1";
pub const GCS_SERVICE_ACCOUNT: &str = "service_account_key";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct DeltaLakeCommon {
    #[serde(rename = "location")]
    pub location: String,
    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(rename = "gcs.service.account")]
    pub gcs_service_account: Option<String>,
    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    pub commit_checkpoint_interval: u64,
}

impl DeltaLakeCommon {
    pub async fn create_deltalake_client(&self) -> Result<DeltaTable> {
        let table = match Self::get_table_url(&self.location)? {
            DeltaTableUrl::S3(s3_path) => {
                let storage_options = self.build_delta_lake_config_for_aws().await?;
                deltalake::aws::register_handlers(None);
                deltalake::open_table_with_storage_options(s3_path.clone(), storage_options).await?
            }
            DeltaTableUrl::Local(local_path) => deltalake::open_table(local_path).await?,
            DeltaTableUrl::Gcs(gcs_path) => {
                let mut storage_options = HashMap::new();
                storage_options.insert(
                    GCS_SERVICE_ACCOUNT.to_owned(),
                    self.gcs_service_account.clone().ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "gcs.service.account is required with Google Cloud Storage (GCS)"
                        ))
                    })?,
                );
                deltalake::gcp::register_handlers(None);
                deltalake::open_table_with_storage_options(gcs_path.clone(), storage_options)
                    .await?
            }
        };
        Ok(table)
    }

    fn get_table_url(path: &str) -> Result<DeltaTableUrl> {
        if path.starts_with("s3://") || path.starts_with("s3a://") {
            Ok(DeltaTableUrl::S3(path.to_owned()))
        } else if path.starts_with("gs://") {
            Ok(DeltaTableUrl::Gcs(path.to_owned()))
        } else if let Some(path) = path.strip_prefix("file://") {
            Ok(DeltaTableUrl::Local(path.to_owned()))
        } else {
            Err(SinkError::DeltaLake(anyhow!(
                "path should start with 's3://','s3a://'(s3) ,gs://(gcs) or file://(local)"
            )))
        }
    }

    async fn build_delta_lake_config_for_aws(&self) -> Result<HashMap<String, String>> {
        let mut storage_options = HashMap::new();
        storage_options.insert(AWS_ALLOW_HTTP.to_owned(), "true".to_owned());
        storage_options.insert(AWS_S3_ALLOW_UNSAFE_RENAME.to_owned(), "true".to_owned());
        let sdk_config = self.aws_auth_props.build_config().await?;
        let credentials = sdk_config
            .credentials_provider()
            .ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "s3.access.key and s3.secret.key is required with aws s3"
                ))
            })?
            .as_ref()
            .provide_credentials()
            .await
            .map_err(|e| SinkError::Config(e.into()))?;
        let region = sdk_config.region();
        let endpoint = sdk_config.endpoint_url();
        storage_options.insert(
            AWS_ACCESS_KEY_ID.to_owned(),
            credentials.access_key_id().to_owned(),
        );
        storage_options.insert(
            AWS_SECRET_ACCESS_KEY.to_owned(),
            credentials.secret_access_key().to_owned(),
        );
        if endpoint.is_none() && region.is_none() {
            return Err(SinkError::Config(anyhow!(
                "s3.endpoint and s3.region need to be filled with at least one"
            )));
        }
        storage_options.insert(
            AWS_REGION.to_owned(),
            region
                .map(|r| r.as_ref().to_owned())
                .clone()
                .unwrap_or_else(|| DEFAULT_REGION.to_owned()),
        );
        if let Some(s3_endpoint) = endpoint {
            storage_options.insert(AWS_ENDPOINT_URL.to_owned(), s3_endpoint.to_owned());
        }
        Ok(storage_options)
    }
}

enum DeltaTableUrl {
    S3(String),
    Local(String),
    Gcs(String),
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct DeltaLakeConfig {
    #[serde(flatten)]
    pub common: DeltaLakeCommon,

    pub r#type: String,
}

impl DeltaLakeConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<DeltaLakeConfig>(
            serde_json::to_value(properties).map_err(|e| SinkError::DeltaLake(e.into()))?,
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

#[derive(Debug)]
pub struct DeltaLakeSink {
    pub config: DeltaLakeConfig,
    param: SinkParam,
}

impl DeltaLakeSink {
    pub fn new(config: DeltaLakeConfig, param: SinkParam) -> Result<Self> {
        Ok(Self { config, param })
    }
}

fn check_field_type(rw_data_type: &DataType, dl_data_type: &DeltaLakeDataType) -> Result<bool> {
    let result = match rw_data_type {
        DataType::Boolean => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Boolean)
            )
        }
        DataType::Int16 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Short)
            )
        }
        DataType::Int32 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Integer)
            )
        }
        DataType::Int64 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Long)
            )
        }
        DataType::Float32 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Float)
            )
        }
        DataType::Float64 => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Double)
            )
        }
        DataType::Decimal => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Decimal(_, _))
            )
        }
        DataType::Date => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Date)
            )
        }
        DataType::Varchar => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::String)
            )
        }
        DataType::Timestamptz => {
            matches!(
                dl_data_type,
                DeltaLakeDataType::Primitive(PrimitiveType::Timestamp)
            )
        }
        DataType::Struct(rw_struct) => {
            if let DeltaLakeDataType::Struct(dl_struct) = dl_data_type {
                let mut result = true;
                for ((rw_name, rw_type), dl_field) in
                    rw_struct.iter().zip_eq_debug(dl_struct.fields())
                {
                    result = check_field_type(rw_type, dl_field.data_type())?
                        && result
                        && rw_name.eq(dl_field.name());
                }
                result
            } else {
                false
            }
        }
        DataType::List(rw_list) => {
            if let DeltaLakeDataType::Array(dl_list) = dl_data_type {
                check_field_type(rw_list, dl_list.element_type())?
            } else {
                false
            }
        }
        _ => {
            return Err(SinkError::DeltaLake(anyhow!(
                "deltalake cannot support type {:?}",
                rw_data_type.to_owned()
            )))
        }
    };
    Ok(result)
}

impl Sink for DeltaLakeSink {
    type Coordinator = DeltaLakeSinkCommitter;
    type LogSinker = DecoupleCheckpointLogSinkerOf<CoordinatedSinkWriter<DeltaLakeSinkWriter>>;

    const SINK_NAME: &'static str = DELTALAKE_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let inner = DeltaLakeSinkWriter::new(
            self.config.clone(),
            self.param.schema().clone(),
            self.param.downstream_pk.clone(),
        )
        .await?;

        let metrics = SinkWriterMetrics::new(&writer_param);
        let writer = CoordinatedSinkWriter::new(
            writer_param
                .meta_client
                .expect("should have meta client")
                .sink_coordinate_client()
                .await,
            self.param.clone(),
            writer_param.vnode_bitmap.ok_or_else(|| {
                SinkError::Remote(anyhow!(
                    "sink needs coordination should not have singleton input"
                ))
            })?,
            inner,
        )
        .await?;

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.common.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );

        Ok(DecoupleCheckpointLogSinkerOf::new(
            writer,
            metrics,
            commit_checkpoint_interval,
        ))
    }

    async fn validate(&self) -> Result<()> {
        if self.config.r#type != SINK_TYPE_APPEND_ONLY
            && self.config.r#type != SINK_USER_FORCE_APPEND_ONLY_OPTION
        {
            return Err(SinkError::Config(anyhow!(
                "only append-only delta lake sink is supported",
            )));
        }
        let table = self.config.common.create_deltalake_client().await?;
        let deltalake_fields: HashMap<&String, &DeltaLakeDataType> = table
            .get_schema()?
            .fields()
            .map(|f| (f.name(), f.data_type()))
            .collect();
        if deltalake_fields.len() != self.param.schema().fields().len() {
            return Err(SinkError::DeltaLake(anyhow!(
                "Columns mismatch. RisingWave is {}, DeltaLake is {}",
                self.param.schema().fields().len(),
                deltalake_fields.len()
            )));
        }
        for field in self.param.schema().fields() {
            if !deltalake_fields.contains_key(&field.name) {
                return Err(SinkError::DeltaLake(anyhow!(
                    "column {} not found in deltalake table",
                    field.name
                )));
            }
            let deltalake_field_type: &&DeltaLakeDataType = deltalake_fields
                .get(&field.name)
                .ok_or_else(|| SinkError::DeltaLake(anyhow!("cannot find field type")))?;
            if !check_field_type(&field.data_type, deltalake_field_type)? {
                return Err(SinkError::DeltaLake(anyhow!(
                    "column {} type is not match, deltalake is {:?}, rw is{:?}",
                    field.name,
                    deltalake_field_type,
                    field.data_type
                )));
            }
        }
        if self.config.common.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }
        Ok(())
    }

    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        Ok(DeltaLakeSinkCommitter {
            table: self.config.common.create_deltalake_client().await?,
        })
    }
}

impl TryFrom<SinkParam> for DeltaLakeSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = DeltaLakeConfig::from_btreemap(param.properties.clone())?;
        DeltaLakeSink::new(config, param)
    }
}

pub struct DeltaLakeSinkWriter {
    pub config: DeltaLakeConfig,
    #[expect(dead_code)]
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    writer: RecordBatchWriter,
    dl_schema: Arc<deltalake::arrow::datatypes::Schema>,
    #[expect(dead_code)]
    dl_table: DeltaTable,
}

impl DeltaLakeSinkWriter {
    pub async fn new(
        config: DeltaLakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Result<Self> {
        let dl_table = config.common.create_deltalake_client().await?;
        let writer = RecordBatchWriter::for_table(&dl_table)?;
        let dl_schema: Arc<deltalake::arrow::datatypes::Schema> =
            Arc::new(convert_schema(dl_table.get_schema()?)?);

        Ok(Self {
            config,
            schema,
            pk_indices,
            writer,
            dl_schema,
            dl_table,
        })
    }

    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        let a = DeltaLakeConvert
            .to_record_batch(self.dl_schema.clone(), &chunk)
            .context("convert record batch error")
            .map_err(SinkError::DeltaLake)?;
        self.writer.write(a).await?;
        Ok(())
    }
}

fn convert_schema(schema: &StructType) -> Result<deltalake::arrow::datatypes::Schema> {
    let mut builder = deltalake::arrow::datatypes::SchemaBuilder::new();
    for field in schema.fields() {
        let dl_field = deltalake::arrow::datatypes::Field::new(
            field.name(),
            deltalake::arrow::datatypes::DataType::try_from(field.data_type())
                .context("convert schema error")
                .map_err(SinkError::DeltaLake)?,
            field.is_nullable(),
        );
        builder.push(dl_field);
    }
    Ok(builder.finish())
}

#[async_trait]
impl SinkWriter for DeltaLakeSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.write(chunk).await
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        if !is_checkpoint {
            return Ok(None);
        }

        let adds = self.writer.flush().await?;
        Ok(Some(SinkMetadata::try_from(&DeltaLakeWriteResult {
            adds,
        })?))
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

pub struct DeltaLakeSinkCommitter {
    table: DeltaTable,
}

#[async_trait::async_trait]
impl SinkCommitCoordinator for DeltaLakeSinkCommitter {
    async fn init(&mut self) -> Result<()> {
        tracing::info!("DeltaLake commit coordinator inited.");
        Ok(())
    }

    async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()> {
        tracing::info!("Starting DeltaLake commit in epoch {epoch}.");

        let deltalake_write_result = metadata
            .iter()
            .map(DeltaLakeWriteResult::try_from)
            .collect::<Result<Vec<DeltaLakeWriteResult>>>()?;
        let write_adds: Vec<Action> = deltalake_write_result
            .into_iter()
            .flat_map(|v| v.adds.into_iter())
            .map(Action::Add)
            .collect();

        if write_adds.is_empty() {
            return Ok(());
        }
        let partition_cols = self.table.metadata()?.partition_columns.clone();
        let partition_by = if !partition_cols.is_empty() {
            Some(partition_cols)
        } else {
            None
        };
        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by,
            predicate: None,
        };
        let version = CommitBuilder::default()
            .with_actions(write_adds)
            .build(
                Some(self.table.snapshot()?),
                self.table.log_store().clone(),
                operation,
            )
            .await?
            .version();
        self.table.update().await?;
        tracing::info!(
            "Succeeded to commit ti DeltaLake table in epoch {epoch} version {version}."
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct DeltaLakeWriteResult {
    adds: Vec<Add>,
}

impl<'a> TryFrom<&'a DeltaLakeWriteResult> for SinkMetadata {
    type Error = SinkError;

    fn try_from(value: &'a DeltaLakeWriteResult) -> std::prelude::v1::Result<Self, Self::Error> {
        let metadata =
            serde_json::to_vec(&value.adds).context("cannot serialize deltalake sink metadata")?;
        Ok(SinkMetadata {
            metadata: Some(Serialized(SerializedMetadata { metadata })),
        })
    }
}

impl DeltaLakeWriteResult {
    fn try_from(value: &SinkMetadata) -> Result<Self> {
        if let Some(Serialized(v)) = &value.metadata {
            let adds = serde_json::from_slice::<Vec<Add>>(&v.metadata)
                .context("Can't deserialize deltalake sink metadata")?;
            Ok(DeltaLakeWriteResult { adds })
        } else {
            bail!("Can't create deltalake sink write result from empty data!")
        }
    }
}

#[cfg(all(test, not(madsim)))]
mod test {
    use deltalake::kernel::DataType as SchemaDataType;
    use deltalake::operations::create::CreateBuilder;
    use maplit::btreemap;
    use risingwave_common::array::{Array, I32Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::catalog::{Field, Schema};

    use super::{DeltaLakeConfig, DeltaLakeSinkWriter};
    use crate::sink::deltalake::DeltaLakeSinkCommitter;
    use crate::sink::writer::SinkWriter;
    use crate::sink::SinkCommitCoordinator;
    use crate::source::DataType;

    #[tokio::test]
    async fn test_deltalake() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        CreateBuilder::new()
            .with_location(path)
            .with_column(
                "id",
                SchemaDataType::Primitive(deltalake::kernel::PrimitiveType::Integer),
                false,
                Default::default(),
            )
            .with_column(
                "name",
                SchemaDataType::Primitive(deltalake::kernel::PrimitiveType::String),
                false,
                Default::default(),
            )
            .await
            .unwrap();

        let properties = btreemap! {
            "connector".to_owned() => "deltalake".to_owned(),
            "force_append_only".to_owned() => "true".to_owned(),
            "type".to_owned() => "append-only".to_owned(),
            "location".to_owned() => format!("file://{}", path),
        };

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "id".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "name".into(),
            },
        ]);

        let deltalake_config = DeltaLakeConfig::from_btreemap(properties).unwrap();
        let deltalake_table = deltalake_config
            .common
            .create_deltalake_client()
            .await
            .unwrap();

        let mut deltalake_writer = DeltaLakeSinkWriter::new(deltalake_config, schema, vec![0])
            .await
            .unwrap();
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                I32Array::from_iter(vec![1, 2, 3]).into_ref(),
                Utf8Array::from_iter(vec!["Alice", "Bob", "Clare"]).into_ref(),
            ],
        );
        deltalake_writer.write(chunk).await.unwrap();
        let mut committer = DeltaLakeSinkCommitter {
            table: deltalake_table,
        };
        let metadata = deltalake_writer.barrier(true).await.unwrap().unwrap();
        committer.commit(1, vec![metadata]).await.unwrap();

        // The following code is to test reading the deltalake data table written with test data.
        // To enable the following code, add `deltalake = { workspace = true, features = ["datafusion"] }`
        // to the `dev-dependencies` section of the `Cargo.toml` file of this crate.
        //
        // The feature is commented and disabled because enabling the `datafusion` feature of `deltalake`
        // will increase the compile time and output binary size in release build, even though it is a
        // dev dependency.

        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let table = deltalake::open_table(path).await.unwrap();
        ctx.register_table("demo", std::sync::Arc::new(table))
            .unwrap();

        let batches = ctx
            .sql("SELECT * FROM demo")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(3, batches.get(0).unwrap().column(0).len());
        assert_eq!(3, batches.get(0).unwrap().column(1).len());
    }
}
