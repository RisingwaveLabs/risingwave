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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::rc::Rc;
use std::sync::LazyLock;

use anyhow::{anyhow, Context};
use either::Either;
use external_schema::debezium::extract_debezium_avro_table_pk_columns;
use external_schema::iceberg::check_iceberg_source;
use external_schema::nexmark::check_nexmark_schema;
use itertools::Itertools;
use maplit::{convert_args, hashmap, hashset};
use pgwire::pg_response::{PgResponse, StatementType};
use rand::Rng;
use risingwave_common::array::arrow::{arrow_schema_iceberg, IcebergArrowConvert};
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{
    debug_assert_column_ids_distinct, ColumnCatalog, ColumnDesc, ColumnId, Schema, TableId,
    ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_FILE_POS_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
    INITIAL_SOURCE_VERSION_ID, KAFKA_TIMESTAMP_COLUMN_NAME, ROWID_PREFIX,
};
use risingwave_common::license::Feature;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::parser::additional_columns::{
    build_additional_column_desc, get_supported_additional_columns,
    source_add_partition_offset_cols,
};
use risingwave_connector::parser::{
    fetch_json_schema_and_map_to_columns, AvroParserConfig, DebeziumAvroParserConfig,
    ProtobufParserConfig, SchemaLocation, SpecificParserConfig, TimestamptzHandling,
    DEBEZIUM_IGNORE_KEY,
};
use risingwave_connector::schema::schema_registry::{
    name_strategy_from_str, SchemaRegistryAuth, SCHEMA_REGISTRY_PASSWORD, SCHEMA_REGISTRY_USERNAME,
};
use risingwave_connector::schema::AWS_GLUE_SCHEMA_ARN_KEY;
use risingwave_connector::source::cdc::{
    CDC_AUTO_SCHEMA_CHANGE_KEY, CDC_SHARING_MODE_KEY, CDC_SNAPSHOT_BACKFILL, CDC_SNAPSHOT_MODE_KEY,
    CDC_TRANSACTIONAL_KEY, CDC_WAIT_FOR_STREAMING_START_TIMEOUT, CITUS_CDC_CONNECTOR,
    MONGODB_CDC_CONNECTOR, MYSQL_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR, SQL_SERVER_CDC_CONNECTOR,
};
use risingwave_connector::source::datagen::DATAGEN_CONNECTOR;
use risingwave_connector::source::iceberg::ICEBERG_CONNECTOR;
use risingwave_connector::source::nexmark::source::{get_event_data_types_with_names, EventType};
use risingwave_connector::source::test_source::TEST_CONNECTOR;
use risingwave_connector::source::{
    ConnectorProperties, AZBLOB_CONNECTOR, GCS_CONNECTOR, GOOGLE_PUBSUB_CONNECTOR, KAFKA_CONNECTOR,
    KINESIS_CONNECTOR, LEGACY_S3_CONNECTOR, MQTT_CONNECTOR, NATS_CONNECTOR, NEXMARK_CONNECTOR,
    OPENDAL_S3_CONNECTOR, POSIX_FS_CONNECTOR, PULSAR_CONNECTOR,
};
pub use risingwave_connector::source::{UPSTREAM_SOURCE_KEY, WEBHOOK_CONNECTOR};
use risingwave_connector::WithPropertiesExt;
use risingwave_pb::catalog::connection_params::PbConnectionType;
use risingwave_pb::catalog::{PbSchemaRegistryNameStrategy, StreamSourceInfo, WatermarkDesc};
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use risingwave_pb::plan_common::{EncodeType, FormatType};
use risingwave_pb::stream_plan::PbStreamFragmentGraph;
use risingwave_pb::telemetry::TelemetryDatabaseObject;
use risingwave_sqlparser::ast::{
    get_delimiter, AstString, ColumnDef, CreateSourceStatement, Encode, Format,
    FormatEncodeOptions, ObjectName, ProtobufSchema, SourceWatermark, TableConstraint,
};
use risingwave_sqlparser::parser::{IncludeOption, IncludeOptionItem};
use thiserror_ext::AsReport;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::{CatalogError, DatabaseId, SchemaId};
use crate::error::ErrorCode::{self, Deprecated, InvalidInputSyntax, NotSupported, ProtocolError};
use crate::error::{Result, RwError};
use crate::expr::Expr;
use crate::handler::create_table::{
    bind_pk_and_row_id_on_relation, bind_sql_column_constraints, bind_sql_columns,
    bind_sql_pk_names, bind_table_constraints, ColumnIdGenerator,
};
use crate::handler::util::{
    check_connector_match_connection_type, ensure_connection_type_allowed, SourceSchemaCompatExt,
};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::generic::SourceNodeKind;
use crate::optimizer::plan_node::{LogicalSource, ToStream, ToStreamContext};
use crate::session::current::notice_to_user;
use crate::session::SessionImpl;
use crate::utils::{
    resolve_connection_ref_and_secret_ref, resolve_privatelink_in_with_option,
    resolve_secret_ref_in_with_options, OverwriteOptions,
};
use crate::{bind_data_type, build_graph, OptimizerContext, WithOptions, WithOptionsSecResolved};

mod external_schema;
pub use external_schema::{
    bind_columns_from_source, get_schema_location, schema_has_schema_registry,
};
mod validate;
pub use validate::validate_compatibility;
use validate::{ALLOWED_CONNECTION_CONNECTOR, ALLOWED_CONNECTION_SCHEMA_REGISTRY};
mod additional_column;
use additional_column::check_and_add_timestamp_column;
pub use additional_column::handle_addition_columns;

fn non_generated_sql_columns(columns: &[ColumnDef]) -> Vec<ColumnDef> {
    columns
        .iter()
        .filter(|c| !c.is_generated())
        .cloned()
        .collect()
}

fn try_consume_string_from_options(
    format_encode_options: &mut BTreeMap<String, String>,
    key: &str,
) -> Option<AstString> {
    format_encode_options.remove(key).map(AstString)
}

fn consume_string_from_options(
    format_encode_options: &mut BTreeMap<String, String>,
    key: &str,
) -> Result<AstString> {
    try_consume_string_from_options(format_encode_options, key).ok_or(RwError::from(ProtocolError(
        format!("missing field {} in options", key),
    )))
}

fn consume_aws_config_from_options(format_encode_options: &mut BTreeMap<String, String>) {
    format_encode_options.retain(|key, _| !key.starts_with("aws."))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateSourceType {
    SharedCdc,
    /// e.g., shared Kafka source
    SharedNonCdc,
    NonShared,
    /// create table with connector
    Table,
}

impl CreateSourceType {
    /// Note: shouldn't be used for `ALTER SOURCE`, since session variables should not affect existing source. We should respect the original type instead.
    pub fn for_newly_created(
        session: &SessionImpl,
        with_properties: &impl WithPropertiesExt,
    ) -> Self {
        if with_properties.is_shareable_cdc_connector() {
            CreateSourceType::SharedCdc
        } else if with_properties.is_shareable_non_cdc_connector()
            && session
                .env()
                .streaming_config()
                .developer
                .enable_shared_source
            && session.config().streaming_use_shared_source()
        {
            CreateSourceType::SharedNonCdc
        } else {
            CreateSourceType::NonShared
        }
    }

    pub fn for_replace(catalog: &SourceCatalog) -> Self {
        if !catalog.info.is_shared() {
            CreateSourceType::NonShared
        } else if catalog.with_properties.is_shareable_cdc_connector() {
            CreateSourceType::SharedCdc
        } else {
            CreateSourceType::SharedNonCdc
        }
    }

    pub fn is_shared(&self) -> bool {
        matches!(
            self,
            CreateSourceType::SharedCdc | CreateSourceType::SharedNonCdc
        )
    }
}

/// Bind columns from both source and sql defined.
pub(crate) fn bind_all_columns(
    format_encode: &FormatEncodeOptions,
    cols_from_source: Option<Vec<ColumnCatalog>>,
    cols_from_sql: Vec<ColumnCatalog>,
    col_defs_from_sql: &[ColumnDef],
    wildcard_idx: Option<usize>,
    sql_column_strategy: SqlColumnStrategy,
) -> Result<Vec<ColumnCatalog>> {
    if let Some(cols_from_source) = cols_from_source {
        // Need to check `col_defs` to see if a column is generated, as we haven't bind the
        // `GeneratedColumnDesc` in `ColumnCatalog` yet.
        let generated_cols_from_sql = cols_from_sql
            .iter()
            .filter(|c| {
                col_defs_from_sql
                    .iter()
                    .find(|d| d.name.real_value() == c.name())
                    .unwrap()
                    .is_generated()
            })
            .cloned()
            .collect_vec();

        #[allow(clippy::collapsible_else_if)]
        match sql_column_strategy {
            // Ignore `cols_from_source`, follow `cols_from_sql` without checking.
            SqlColumnStrategy::FollowUnchecked => {
                assert!(
                    wildcard_idx.is_none(),
                    "wildcard still exists while strategy is Follows, not correctly purified?"
                );
                return Ok(cols_from_sql);
            }

            // Will merge `generated_cols_from_sql` into `cols_from_source`.
            SqlColumnStrategy::Ignore => {}

            SqlColumnStrategy::FollowChecked => {
                let has_regular_cols_from_sql =
                    generated_cols_from_sql.len() != cols_from_sql.len();

                if has_regular_cols_from_sql {
                    if wildcard_idx.is_some() {
                        // (*, normal_column INT)
                        return Err(RwError::from(NotSupported(
                            "When there's a wildcard (\"*\"), \
                             only generated columns are allowed in user-defined schema from SQL"
                                .to_owned(),
                            "Remove the non-generated columns".to_owned(),
                        )));
                    } else {
                        // (normal_column INT)
                        // Follow `cols_from_sql` with name & type checking.
                        for col in &cols_from_sql {
                            if generated_cols_from_sql.contains(col) {
                                continue;
                            }
                            let Some(col_from_source) =
                                cols_from_source.iter().find(|c| c.name() == col.name())
                            else {
                                return Err(RwError::from(ProtocolError(format!(
                                    "Column \"{}\" is defined in SQL but not found in the source",
                                    col.name()
                                ))));
                            };

                            if col_from_source.data_type() != col.data_type() {
                                return Err(RwError::from(ProtocolError(format!(
                                    "Data type mismatch for column \"{}\". \
                                     Defined in SQL as \"{}\", but found in the source as \"{}\"",
                                    col.name(),
                                    col.data_type(),
                                    col_from_source.data_type()
                                ))));
                            }
                        }
                        return Ok(cols_from_sql);
                    }
                } else {
                    if wildcard_idx.is_some() {
                        // (*)
                        // (*, generated_column INT)
                        // Good, expand the wildcard later.
                    } else {
                        // ()
                        // (generated_column INT)
                        // Interpreted as if there's a wildcard for backward compatibility.
                        // TODO: the behavior is not that consistent, making it impossible to ingest no
                        //       columns from the source (though not useful in practice). Currently we
                        //       just notice the user but we may want to interpret it as empty columns
                        //       in the future.
                        notice_to_user("\
                            Neither wildcard (\"*\") nor regular (non-generated) columns appear in the user-defined schema from SQL. \
                            For backward compatibility, all columns from the source will be included at the beginning. \
                            For clarity, consider adding a wildcard (\"*\") to indicate where the columns from the source should be included, \
                            or specifying the columns you want to include from the source.
                        ");
                    }
                }
            }
        }

        // In some cases the wildcard may be absent:
        // - plan based on a purified SQL
        // - interpret `()` as `(*)` for backward compatibility (see notice above)
        // Default to 0 to expand the wildcard at the beginning.
        let wildcard_idx = wildcard_idx.unwrap_or(0).min(generated_cols_from_sql.len());

        // Merge `generated_cols_from_sql` with `cols_from_source`.
        let mut merged_cols = generated_cols_from_sql;
        let merged_cols_r = merged_cols.split_off(wildcard_idx);
        merged_cols.extend(cols_from_source);
        merged_cols.extend(merged_cols_r);

        Ok(merged_cols)
    } else {
        if wildcard_idx.is_some() {
            return Err(RwError::from(NotSupported(
                "Wildcard in user-defined schema is only allowed when there exists columns from external schema".to_owned(),
                "Remove the wildcard or use a source with external schema".to_owned(),
            )));
        }
        let non_generated_sql_defined_columns = non_generated_sql_columns(col_defs_from_sql);
        match (&format_encode.format, &format_encode.row_encode) {
            (Format::DebeziumMongo, Encode::Json) => {
                let mut columns = vec![
                    ColumnCatalog {
                        column_desc: ColumnDesc::named("_id", 0.into(), DataType::Varchar),
                        is_hidden: false,
                    },
                    ColumnCatalog {
                        column_desc: ColumnDesc::named("payload", 0.into(), DataType::Jsonb),
                        is_hidden: false,
                    },
                ];
                if non_generated_sql_defined_columns.len() != 2
                    || non_generated_sql_defined_columns[0].name.real_value() != columns[0].name()
                    || non_generated_sql_defined_columns[1].name.real_value() != columns[1].name()
                {
                    return Err(RwError::from(ProtocolError(
                        "the not generated columns of the source with row format DebeziumMongoJson
        must be (_id [Jsonb | Varchar | Int32 | Int64], payload jsonb)."
                            .to_owned(),
                    )));
                }
                // ok to unwrap since it was checked at `bind_sql_columns`
                let key_data_type = bind_data_type(
                    non_generated_sql_defined_columns[0]
                        .data_type
                        .as_ref()
                        .unwrap(),
                )?;
                match key_data_type {
                    DataType::Jsonb | DataType::Varchar | DataType::Int32 | DataType::Int64 => {
                        columns[0].column_desc.data_type = key_data_type.clone();
                    }
                    _ => {
                        return Err(RwError::from(ProtocolError(
                            "the `_id` column of the source with row format DebeziumMongoJson
        must be [Jsonb | Varchar | Int32 | Int64]"
                                .to_owned(),
                        )));
                    }
                }

                // ok to unwrap since it was checked at `bind_sql_columns`
                let value_data_type = bind_data_type(
                    non_generated_sql_defined_columns[1]
                        .data_type
                        .as_ref()
                        .unwrap(),
                )?;
                if !matches!(value_data_type, DataType::Jsonb) {
                    return Err(RwError::from(ProtocolError(
                        "the `payload` column of the source with row format DebeziumMongoJson
        must be Jsonb datatype"
                            .to_owned(),
                    )));
                }
                Ok(columns)
            }
            (Format::Plain, Encode::Bytes) => {
                let err = Err(RwError::from(ProtocolError(
                    "ENCODE BYTES only accepts one BYTEA type column".to_owned(),
                )));
                if non_generated_sql_defined_columns.len() == 1 {
                    // ok to unwrap `data_type`` since it was checked at `bind_sql_columns`
                    let col_data_type = bind_data_type(
                        non_generated_sql_defined_columns[0]
                            .data_type
                            .as_ref()
                            .unwrap(),
                    )?;
                    if col_data_type == DataType::Bytea {
                        Ok(cols_from_sql)
                    } else {
                        err
                    }
                } else {
                    err
                }
            }
            (_, _) => Ok(cols_from_sql),
        }
    }
}

/// TODO: perhaps put the hint in notice is better. The error message format might be not that reliable.
fn hint_upsert(encode: &Encode) -> String {
    format!(
        r#"Hint: For FORMAT UPSERT ENCODE {encode:}, INCLUDE KEY must be specified and the key column must be used as primary key.
example:
    CREATE TABLE <table_name> ( PRIMARY KEY ([rw_key | <key_name>]) )
    INCLUDE KEY [AS <key_name>]
    WITH (...)
    FORMAT UPSERT ENCODE {encode:} (...)
"#
    )
}

/// Bind column from source. Add key column to table columns if necessary.
/// Return `pk_names`.
pub(crate) async fn bind_source_pk(
    format_encode: &FormatEncodeOptions,
    source_info: &StreamSourceInfo,
    columns: &mut [ColumnCatalog],
    sql_defined_pk_names: Vec<String>,
    with_properties: &WithOptionsSecResolved,
) -> Result<Vec<String>> {
    let sql_defined_pk = !sql_defined_pk_names.is_empty();
    let include_key_column_name: Option<String> = {
        // iter columns to check if contains additional columns from key part
        // return the key column names if exists
        columns.iter().find_map(|catalog| {
            if matches!(
                catalog.column_desc.additional_column.column_type,
                Some(AdditionalColumnType::Key(_))
            ) {
                Some(catalog.name().to_owned())
            } else {
                None
            }
        })
    };
    let additional_column_names = columns
        .iter()
        .filter_map(|col| {
            if col.column_desc.additional_column.column_type.is_some() {
                Some(col.name().to_owned())
            } else {
                None
            }
        })
        .collect_vec();

    let res = match (&format_encode.format, &format_encode.row_encode) {
        (Format::Native, Encode::Native) | (Format::None, Encode::None) | (Format::Plain, _) => {
            sql_defined_pk_names
        }

        // For all Upsert formats, we only accept one and only key column as primary key.
        // Additional KEY columns must be set in this case and must be primary key.
        (
            Format::Upsert,
            encode @ Encode::Json | encode @ Encode::Avro | encode @ Encode::Protobuf,
        ) => {
            if let Some(ref key_column_name) = include_key_column_name
                && sql_defined_pk
            {
                // pk is set. check if it's valid

                // the column name have been converted to real value in `handle_addition_columns`
                // so we don't ignore ascii case here
                if sql_defined_pk_names.len() != 1
                    || !key_column_name.eq(sql_defined_pk_names[0].as_str())
                {
                    return Err(RwError::from(ProtocolError(format!(
                        "Only \"{}\" can be used as primary key\n\n{}",
                        key_column_name,
                        hint_upsert(encode)
                    ))));
                }
                sql_defined_pk_names
            } else {
                // pk not set, or even key not included
                return if let Some(include_key_column_name) = include_key_column_name {
                    Err(RwError::from(ProtocolError(format!(
                        "Primary key must be specified to {}\n\n{}",
                        include_key_column_name,
                        hint_upsert(encode)
                    ))))
                } else {
                    Err(RwError::from(ProtocolError(format!(
                        "INCLUDE KEY clause not set\n\n{}",
                        hint_upsert(encode)
                    ))))
                };
            }
        }

        (Format::Debezium, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT DEBEZIUM forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if !sql_defined_pk {
                return Err(RwError::from(ProtocolError(
                    "Primary key must be specified when creating source with FORMAT DEBEZIUM."
                        .to_owned(),
                )));
            }
            sql_defined_pk_names
        }
        (Format::Debezium, Encode::Avro) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT DEBEZIUM forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if sql_defined_pk {
                sql_defined_pk_names
            } else {
                let pk_names =
                    extract_debezium_avro_table_pk_columns(source_info, with_properties).await?;
                // extract pk(s) from schema registry
                for pk_name in &pk_names {
                    columns
                        .iter()
                        .find(|c: &&ColumnCatalog| c.name().eq(pk_name))
                        .ok_or_else(|| {
                            RwError::from(ProtocolError(format!(
                                "avro's key column {} not exists in avro's row schema",
                                pk_name
                            )))
                        })?;
                }
                pk_names
            }
        }
        (Format::DebeziumMongo, Encode::Json) => {
            if sql_defined_pk {
                sql_defined_pk_names
            } else {
                vec!["_id".to_owned()]
            }
        }

        (Format::Maxwell, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT MAXWELL forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if !sql_defined_pk {
                return Err(RwError::from(ProtocolError(
    "Primary key must be specified when creating source with FORMAT MAXWELL ENCODE JSON.".to_owned(),
    )));
            }
            sql_defined_pk_names
        }

        (Format::Canal, Encode::Json) => {
            if !additional_column_names.is_empty() {
                return Err(RwError::from(ProtocolError(format!(
                    "FORMAT CANAL forbids additional columns, but got {:?}",
                    additional_column_names
                ))));
            }
            if !sql_defined_pk {
                return Err(RwError::from(ProtocolError(
    "Primary key must be specified when creating source with FORMAT CANAL ENCODE JSON.".to_owned(),
    )));
            }
            sql_defined_pk_names
        }
        (format, encoding) => {
            return Err(RwError::from(ProtocolError(format!(
                "Unknown combination {:?} {:?}",
                format, encoding
            ))));
        }
    };
    Ok(res)
}

pub(super) fn bind_source_watermark(
    session: &SessionImpl,
    name: String,
    source_watermarks: Vec<SourceWatermark>,
    column_catalogs: &[ColumnCatalog],
) -> Result<Vec<WatermarkDesc>> {
    let mut binder = Binder::new_for_ddl(session);
    binder.bind_columns_to_context(name.clone(), column_catalogs)?;

    let watermark_descs = source_watermarks
        .into_iter()
        .map(|source_watermark| {
            let col_name = source_watermark.column.real_value();
            let watermark_idx = binder.get_column_binding_index(name.clone(), &col_name)?;

            let expr = binder.bind_expr(source_watermark.expr)?;
            let watermark_col_type = column_catalogs[watermark_idx].data_type();
            let watermark_expr_type = &expr.return_type();
            if watermark_col_type != watermark_expr_type {
                Err(RwError::from(ErrorCode::BindError(
                    format!("The return value type of the watermark expression must be identical to the watermark column data type. Current data type of watermark return value: `{}`, column `{}`",watermark_expr_type, watermark_col_type),
                )))
            } else {
                let expr_proto = expr.to_expr_proto();
                Ok::<_, RwError>(WatermarkDesc {
                    watermark_idx: watermark_idx as u32,
                    expr: Some(expr_proto),
                })
            }
        })
        .try_collect()?;
    Ok(watermark_descs)
}

/// Performs early stage checking in frontend to see if the schema of the given `columns` is
/// compatible with the connector extracted from the properties.
///
/// One should only call this function after all properties of all columns are resolved, like
/// generated column descriptors.
pub(super) async fn check_format_encode(
    props: &WithOptionsSecResolved,
    row_id_index: Option<usize>,
    columns: &[ColumnCatalog],
) -> Result<()> {
    let Some(connector) = props.get_connector() else {
        return Ok(());
    };

    if connector == NEXMARK_CONNECTOR {
        check_nexmark_schema(props, row_id_index, columns)
    } else if connector == ICEBERG_CONNECTOR {
        Ok(check_iceberg_source(props, columns)
            .await
            .map_err(|err| ProtocolError(err.to_report_string()))?)
    } else {
        Ok(())
    }
}

pub fn bind_connector_props(
    handler_args: &HandlerArgs,
    format_encode: &FormatEncodeOptions,
    is_create_source: bool,
) -> Result<WithOptions> {
    let mut with_properties = handler_args.with_options.clone().into_connector_props();
    validate_compatibility(format_encode, &mut with_properties)?;
    let create_cdc_source_job = with_properties.is_shareable_cdc_connector();

    if !is_create_source && with_properties.is_shareable_only_cdc_connector() {
        return Err(RwError::from(ProtocolError(format!(
            "connector {} does not support `CREATE TABLE`, please use `CREATE SOURCE` instead",
            with_properties.get_connector().unwrap(),
        ))));
    }
    if is_create_source && create_cdc_source_job {
        if let Some(value) = with_properties.get(CDC_AUTO_SCHEMA_CHANGE_KEY)
            && value
                .parse::<bool>()
                .map_err(|_| anyhow!("invalid value of '{}' option", CDC_AUTO_SCHEMA_CHANGE_KEY))?
        {
            Feature::CdcAutoSchemaChange
                .check_available()
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        // set connector to backfill mode
        with_properties.insert(CDC_SNAPSHOT_MODE_KEY.into(), CDC_SNAPSHOT_BACKFILL.into());
        // enable cdc sharing mode, which will capture all tables in the given `database.name`
        with_properties.insert(CDC_SHARING_MODE_KEY.into(), "true".into());
        // enable transactional cdc
        if with_properties.enable_transaction_metadata() {
            with_properties.insert(CDC_TRANSACTIONAL_KEY.into(), "true".into());
        }
        with_properties.insert(
            CDC_WAIT_FOR_STREAMING_START_TIMEOUT.into(),
            handler_args
                .session
                .config()
                .cdc_source_wait_streaming_start_timeout()
                .to_string(),
        );
    }
    if with_properties.is_mysql_cdc_connector() {
        // Generate a random server id for mysql cdc source if needed
        // `server.id` (in the range from 1 to 2^32 - 1). This value MUST be unique across whole replication
        // group (that is, different from any other server id being used by any master or slave)
        with_properties
            .entry("server.id".to_owned())
            .or_insert(rand::thread_rng().gen_range(1..u32::MAX).to_string());
    }
    Ok(with_properties)
}

/// When the schema can be inferred from external system (like schema registry),
/// how to handle the regular columns (i.e., non-generated) defined in SQL?
pub enum SqlColumnStrategy {
    /// Follow all columns defined in SQL, ignore the columns from external system.
    /// This ensures that no accidental side effect will change the schema.
    ///
    /// This is the behavior when re-planning the target table of `SINK INTO`.
    FollowUnchecked,

    /// Follow all column defined in SQL, check the columns from external system with name & type.
    /// This ensures that no accidental side effect will change the schema.
    ///
    /// This is the behavior when creating a new table or source with a resolvable schema;
    /// adding, or dropping columns on that table.
    // TODO(purify): `ALTER SOURCE` currently has its own code path and does not check.
    FollowChecked,

    /// Merge the generated columns defined in SQL and columns from external system. If there
    /// are also regular columns defined in SQL, ignore silently.
    ///
    /// This is the behavior when `REFRESH SCHEMA` atop the purified SQL.
    Ignore,
}

#[allow(clippy::too_many_arguments)]
pub async fn bind_create_source_or_table_with_connector(
    handler_args: HandlerArgs,
    full_name: ObjectName,
    format_encode: FormatEncodeOptions,
    with_properties: WithOptions,
    sql_columns_defs: &[ColumnDef],
    constraints: Vec<TableConstraint>,
    wildcard_idx: Option<usize>,
    source_watermarks: Vec<SourceWatermark>,
    columns_from_resolve_source: Option<Vec<ColumnCatalog>>,
    source_info: StreamSourceInfo,
    include_column_options: IncludeOption,
    col_id_gen: &mut ColumnIdGenerator,
    create_source_type: CreateSourceType,
    source_rate_limit: Option<u32>,
    sql_column_strategy: SqlColumnStrategy,
) -> Result<(SourceCatalog, DatabaseId, SchemaId)> {
    let session = &handler_args.session;
    let db_name: &str = &session.database();
    let (schema_name, source_name) = Binder::resolve_schema_qualified_name(db_name, full_name)?;
    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    let is_create_source = create_source_type != CreateSourceType::Table;
    if !is_create_source && with_properties.is_iceberg_connector() {
        return Err(ErrorCode::BindError(
            "can't CREATE TABLE with iceberg connector\n\nHint: use CREATE SOURCE instead"
                .to_owned(),
        )
        .into());
    }
    if is_create_source {
        match format_encode.format {
            Format::Upsert => {
                return Err(ErrorCode::BindError(format!(
                    "can't CREATE SOURCE with FORMAT UPSERT\n\nHint: use CREATE TABLE instead\n\n{}",
                    hint_upsert(&format_encode.row_encode)
                ))
                .into());
            }
            _ => {
                // TODO: enhance error message for other formats
            }
        }
    }

    let sql_pk_names = bind_sql_pk_names(sql_columns_defs, bind_table_constraints(&constraints)?)?;

    let columns_from_sql = bind_sql_columns(sql_columns_defs)?;

    let mut columns = bind_all_columns(
        &format_encode,
        columns_from_resolve_source,
        columns_from_sql,
        sql_columns_defs,
        wildcard_idx,
        sql_column_strategy,
    )?;

    // add additional columns before bind pk, because `format upsert` requires the key column
    handle_addition_columns(
        Some(&format_encode),
        &with_properties,
        include_column_options,
        &mut columns,
        false,
    )?;

    if columns.is_empty() {
        return Err(RwError::from(ProtocolError(
            "Schema definition is required, either from SQL or schema registry.".to_owned(),
        )));
    }

    // compatible with the behavior that add a hidden column `_rw_kafka_timestamp` to each message from Kafka source
    if is_create_source {
        // must behind `handle_addition_columns`
        check_and_add_timestamp_column(&with_properties, &mut columns);

        // For shared sources, we will include partition and offset cols in the SourceExecutor's *output*, to be used by the SourceBackfillExecutor.
        // For shared CDC source, the schema is different. See debezium_cdc_source_schema, CDC_BACKFILL_TABLE_ADDITIONAL_COLUMNS
        if create_source_type == CreateSourceType::SharedNonCdc {
            let (columns_exist, additional_columns) = source_add_partition_offset_cols(
                &columns,
                &with_properties.get_connector().unwrap(),
                true, // col_id filled below at col_id_gen.generate
            );
            for (existed, c) in columns_exist.into_iter().zip_eq_fast(additional_columns) {
                if !existed {
                    columns.push(ColumnCatalog::hidden(c));
                }
            }
        }
    }

    // resolve privatelink connection for Kafka
    let mut with_properties = with_properties;
    resolve_privatelink_in_with_option(&mut with_properties)?;

    let (with_properties, connection_type, connector_conn_ref) =
        resolve_connection_ref_and_secret_ref(
            with_properties,
            session,
            TelemetryDatabaseObject::Source,
        )?;
    ensure_connection_type_allowed(connection_type, &ALLOWED_CONNECTION_CONNECTOR)?;

    // if not using connection, we don't need to check connector match connection type
    if !matches!(connection_type, PbConnectionType::Unspecified) {
        let connector = with_properties.get_connector().unwrap();
        check_connector_match_connection_type(connector.as_str(), &connection_type)?;
    }

    let pk_names = bind_source_pk(
        &format_encode,
        &source_info,
        &mut columns,
        sql_pk_names,
        &with_properties,
    )
    .await?;

    if is_create_source && !pk_names.is_empty() {
        return Err(ErrorCode::InvalidInputSyntax(
            "Source does not support PRIMARY KEY constraint, please use \"CREATE TABLE\" instead"
                .to_owned(),
        )
        .into());
    }

    // User may specify a generated or additional column with the same name as one from the external schema.
    // Ensure duplicated column names are handled here.
    if let Some(duplicated_name) = columns.iter().map(|c| c.name()).duplicates().next() {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{}\" specified more than once",
            duplicated_name
        ))
        .into());
    }

    // XXX: why do we use col_id_gen here? It doesn't seem to be very necessary.
    // XXX: should we also change the col id for struct fields?
    for c in &mut columns {
        c.column_desc.column_id = col_id_gen.generate(&*c)?;
    }
    debug_assert_column_ids_distinct(&columns);

    let must_need_pk = if is_create_source {
        with_properties.connector_need_pk()
    } else {
        // For those connectors that do not need generate a `row_id`` column in the source schema such as iceberg.
        // But in such case, we can not create mv or table on the source because there is not a pk.
        assert!(with_properties.connector_need_pk());

        true
    };

    let (mut columns, pk_col_ids, row_id_index) =
        bind_pk_and_row_id_on_relation(columns, pk_names, must_need_pk)?;

    let watermark_descs =
        bind_source_watermark(session, source_name.clone(), source_watermarks, &columns)?;
    // TODO(yuhao): allow multiple watermark on source.
    assert!(watermark_descs.len() <= 1);

    bind_sql_column_constraints(
        session,
        source_name.clone(),
        &mut columns,
        // TODO(st1page): pass the ref
        sql_columns_defs.to_vec(),
        &pk_col_ids,
    )?;
    check_format_encode(&with_properties, row_id_index, &columns).await?;

    let definition = handler_args.normalized_sql.clone();

    let associated_table_id = if is_create_source {
        None
    } else {
        Some(TableId::placeholder())
    };
    let source = SourceCatalog {
        id: TableId::placeholder().table_id,
        name: source_name,
        columns,
        pk_col_ids,
        append_only: row_id_index.is_some(),
        owner: session.user_id(),
        info: source_info,
        row_id_index,
        with_properties,
        watermark_descs,
        associated_table_id,
        definition,
        connection_id: connector_conn_ref,
        created_at_epoch: None,
        initialized_at_epoch: None,
        version: INITIAL_SOURCE_VERSION_ID,
        created_at_cluster_version: None,
        initialized_at_cluster_version: None,
        rate_limit: source_rate_limit,
    };
    Ok((source, database_id, schema_id))
}

pub async fn handle_create_source(
    mut handler_args: HandlerArgs,
    stmt: CreateSourceStatement,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let overwrite_options = OverwriteOptions::new(&mut handler_args);

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        stmt.source_name.clone(),
        StatementType::CREATE_SOURCE,
        stmt.if_not_exists,
    )? {
        return Ok(resp);
    }

    if handler_args.with_options.is_empty() {
        return Err(RwError::from(InvalidInputSyntax(
            "missing WITH clause".to_owned(),
        )));
    }

    let format_encode = stmt.format_encode.into_v2_with_warning();
    let with_properties = bind_connector_props(&handler_args, &format_encode, true)?;

    let create_source_type = CreateSourceType::for_newly_created(&session, &*with_properties);
    let (columns_from_resolve_source, source_info) = bind_columns_from_source(
        &session,
        &format_encode,
        Either::Left(&with_properties),
        create_source_type,
    )
    .await?;
    let mut col_id_gen = ColumnIdGenerator::new_initial();

    let (source_catalog, database_id, schema_id) = bind_create_source_or_table_with_connector(
        handler_args.clone(),
        stmt.source_name,
        format_encode,
        with_properties,
        &stmt.columns,
        stmt.constraints,
        stmt.wildcard_idx,
        stmt.source_watermarks,
        columns_from_resolve_source,
        source_info,
        stmt.include_column_options,
        &mut col_id_gen,
        create_source_type,
        overwrite_options.source_rate_limit,
        SqlColumnStrategy::FollowChecked,
    )
    .await?;

    // If it is a temporary source, put it into SessionImpl.
    if stmt.temporary {
        if session.get_temporary_source(&source_catalog.name).is_some() {
            return Err(CatalogError::Duplicated("source", source_catalog.name.clone()).into());
        }
        session.create_temporary_source(source_catalog);
        return Ok(PgResponse::empty_result(StatementType::CREATE_SOURCE));
    }

    let source = source_catalog.to_prost(schema_id, database_id);

    let catalog_writer = session.catalog_writer()?;

    if create_source_type.is_shared() {
        let graph = generate_stream_graph_for_source(handler_args, source_catalog)?;
        catalog_writer.create_source(source, Some(graph)).await?;
    } else {
        // For other sources we don't create a streaming job
        catalog_writer.create_source(source, None).await?;
    }

    Ok(PgResponse::empty_result(StatementType::CREATE_SOURCE))
}

pub(super) fn generate_stream_graph_for_source(
    handler_args: HandlerArgs,
    source_catalog: SourceCatalog,
) -> Result<PbStreamFragmentGraph> {
    let context = OptimizerContext::from_handler_args(handler_args);
    let source_node = LogicalSource::with_catalog(
        Rc::new(source_catalog),
        SourceNodeKind::CreateSharedSource,
        context.into(),
        None,
    )?;

    let stream_plan = source_node.to_stream(&mut ToStreamContext::new(false))?;
    let graph = build_graph(stream_plan)?;
    Ok(graph)
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, ROWID_PREFIX};
    use risingwave_common::types::{DataType, StructType};

    use crate::catalog::root_catalog::SchemaPath;
    use crate::catalog::source_catalog::SourceCatalog;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    const GET_COLUMN_FROM_CATALOG: fn(&Arc<SourceCatalog>) -> HashMap<&str, DataType> =
        |catalog: &Arc<SourceCatalog>| -> HashMap<&str, DataType> {
            catalog
                .columns
                .iter()
                .map(|col| (col.name(), col.data_type().clone()))
                .collect::<HashMap<&str, DataType>>()
        };

    #[tokio::test]
    async fn test_create_source_handler() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (connector = 'kinesis')
    FORMAT PLAIN ENCODE PROTOBUF (message = '.test.TestRecord', schema.location = 'file://{}')"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        let columns = GET_COLUMN_FROM_CATALOG(source);

        let city_type = StructType::new(vec![
            ("address", DataType::Varchar),
            ("zipcode", DataType::Varchar),
        ])
        .into();
        let expected_columns = maplit::hashmap! {
            ROWID_PREFIX => DataType::Serial,
            "id" => DataType::Int32,
            "zipcode" => DataType::Int64,
            "rate" => DataType::Float32,
            "country" => StructType::new(
                vec![("address", DataType::Varchar),("city", city_type),("zipcode", DataType::Varchar)],
            ).into(),
        };
        assert_eq!(columns, expected_columns);
    }

    #[tokio::test]
    async fn test_duplicate_props_options() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE SOURCE t
    WITH (
        connector = 'kinesis',
        aws.region='user_test_topic',
        endpoint='172.10.1.1:9090,172.10.1.2:9090',
        aws.credentials.access_key_id = 'your_access_key_1',
        aws.credentials.secret_access_key = 'your_secret_key_1'
    )
    FORMAT PLAIN ENCODE PROTOBUF (
        message = '.test.TestRecord',
        aws.credentials.access_key_id = 'your_access_key_2',
        aws.credentials.secret_access_key = 'your_secret_key_2',
        schema.location = 'file://{}',
    )"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        // AwsAuth params exist in options.
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_2"
        );
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_2"
        );

        // AwsAuth params exist in props.
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_1"
        );
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_1"
        );

        // Options are not merged into props.
        assert!(!source.with_properties.contains_key("schema.location"));
    }

    #[tokio::test]
    async fn test_multi_table_cdc_create_source_handler() {
        let sql =
            "CREATE SOURCE t2 WITH (connector = 'mysql-cdc') FORMAT PLAIN ENCODE JSON".to_owned();
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();

        frontend
            .run_sql_with_session(session.clone(), sql)
            .await
            .unwrap();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t2")
            .unwrap();
        assert_eq!(source.name, "t2");

        let columns = source
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<Vec<(&str, DataType)>>();

        expect_test::expect![[r#"
            [
                (
                    "payload",
                    Jsonb,
                ),
                (
                    "_rw_offset",
                    Varchar,
                ),
                (
                    "_rw_table_name",
                    Varchar,
                ),
                (
                    "_row_id",
                    Serial,
                ),
            ]
        "#]]
        .assert_debug_eq(&columns);
    }

    #[tokio::test]
    async fn test_source_addition_columns() {
        // test derive include column for format plain
        let sql =
            "CREATE SOURCE s (v1 int) include key as _rw_kafka_key with (connector = 'kafka') format plain encode json".to_owned();
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let (source, _) = catalog_reader
            .get_source_by_name(
                DEFAULT_DATABASE_NAME,
                SchemaPath::Name(DEFAULT_SCHEMA_NAME),
                "s",
            )
            .unwrap();
        assert_eq!(source.name, "s");

        let columns = source
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<Vec<(&str, DataType)>>();

        expect_test::expect![[r#"
            [
                (
                    "v1",
                    Int32,
                ),
                (
                    "_rw_kafka_key",
                    Bytea,
                ),
                (
                    "_rw_kafka_timestamp",
                    Timestamptz,
                ),
                (
                    "_rw_kafka_partition",
                    Varchar,
                ),
                (
                    "_rw_kafka_offset",
                    Varchar,
                ),
                (
                    "_row_id",
                    Serial,
                ),
            ]
        "#]]
        .assert_debug_eq(&columns);

        let sql =
            "CREATE SOURCE s3 (v1 int) include timestamp 'header1' as header_col with (connector = 'kafka') format plain encode json".to_owned();
        match frontend.run_sql(sql).await {
            Err(e) => {
                assert_eq!(
                    e.to_string(),
                    "Protocol error: Only header column can have inner field, but got \"timestamp\""
                )
            }
            _ => unreachable!(),
        }
    }
}
