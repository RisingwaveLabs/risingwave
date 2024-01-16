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

use anyhow::{anyhow, Context};
use arrow_schema::Fields;
use bytes::Bytes;
use itertools::Itertools;
use pgwire::pg_response::StatementType;
use risingwave_common::catalog::FunctionId;
use risingwave_common::types::DataType;
use risingwave_expr::expr::get_or_create_wasm_runtime;
use risingwave_object_store::object::{build_remote_object_store, ObjectStoreConfig};
use risingwave_pb::catalog::function::{Kind, ScalarFunction, TableFunction};
use risingwave_pb::catalog::Function;
use risingwave_sqlparser::ast::{
    CreateFunctionBody, FunctionDefinition, ObjectName, OperateFunctionArg,
};
use risingwave_storage::monitor::ObjectStoreMetrics;
use risingwave_udf::ArrowFlightUdfClient;

use super::*;
use crate::catalog::CatalogError;
use crate::{bind_data_type, Binder};

pub async fn handle_create_function(
    handler_args: HandlerArgs,
    or_replace: bool,
    temporary: bool,
    name: ObjectName,
    args: Option<Vec<OperateFunctionArg>>,
    returns: Option<CreateFunctionReturns>,
    params: CreateFunctionBody,
) -> Result<RwPgResponse> {
    if or_replace {
        bail_not_implemented!("CREATE OR REPLACE FUNCTION");
    }
    if temporary {
        bail_not_implemented!("CREATE TEMPORARY FUNCTION");
    }
    // e.g., `language [ python / java / ...etc]`
    let language = match params.language {
        Some(lang) => {
            let lang = lang.real_value().to_lowercase();
            match &*lang {
                "python" | "java" | "wasm" => lang,
                _ => {
                    return Err(ErrorCode::InvalidParameterValue(format!(
                        "language {} is not supported",
                        lang
                    ))
                    .into())
                }
            }
        }
        // Empty language is acceptable since we only require the external server implements the
        // correct protocol.
        None => "".to_string(),
    };
    let return_type;
    let kind = match returns {
        Some(CreateFunctionReturns::Value(data_type)) => {
            return_type = bind_data_type(&data_type)?;
            Kind::Scalar(ScalarFunction {})
        }
        Some(CreateFunctionReturns::Table(columns)) => {
            if columns.len() == 1 {
                // return type is the original type for single column
                return_type = bind_data_type(&columns[0].data_type)?;
            } else {
                // return type is a struct for multiple columns
                let it = columns
                    .into_iter()
                    .map(|c| bind_data_type(&c.data_type).map(|ty| (ty, c.name.real_value())));
                let (datatypes, names) = itertools::process_results(it, |it| it.unzip())?;
                return_type = DataType::new_struct(datatypes, names);
            }
            Kind::Table(TableFunction {})
        }
        None => {
            return Err(ErrorCode::InvalidParameterValue(
                "return type must be specified".to_string(),
            )
            .into())
        }
    };

    let Some(using) = params.using else {
        return Err(ErrorCode::InvalidParameterValue("USING must be specified".to_string()).into());
    };

    let mut arg_types = vec![];
    for arg in args.unwrap_or_default() {
        arg_types.push(bind_data_type(&arg.data_type)?);
    }

    // resolve database and schema id
    let session = &handler_args.session;
    let db_name = session.database();
    let (schema_name, function_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    // check if the function exists in the catalog
    if (session.env().catalog_reader().read_guard())
        .get_schema_by_id(&database_id, &schema_id)?
        .get_function_by_name_args(&function_name, &arg_types)
        .is_some()
    {
        let name = format!(
            "{function_name}({})",
            arg_types.iter().map(|t| t.to_string()).join(",")
        );
        return Err(CatalogError::Duplicated("function", name).into());
    }

    let link;
    let identifier;

    match language.as_str() {
        "python" | "java" | "" => {
            let CreateFunctionUsing::Link(l) = using else {
                return Err(ErrorCode::InvalidParameterValue(
                    "USING LINK must be specified".to_string(),
                )
                .into());
            };
            let Some(FunctionDefinition::SingleQuotedDef(id)) = params.as_ else {
                return Err(
                    ErrorCode::InvalidParameterValue("AS must be specified".to_string()).into(),
                );
            };
            identifier = id;
            link = l;

            // check UDF server
            {
                let client = ArrowFlightUdfClient::connect(&link)
                    .await
                    .map_err(|e| anyhow!(e))?;
                /// A helper function to create a unnamed field from data type.
                fn to_field(data_type: arrow_schema::DataType) -> arrow_schema::Field {
                    arrow_schema::Field::new("", data_type, true)
                }
                let args = arrow_schema::Schema::new(
                    arg_types
                        .iter()
                        .map::<Result<_>, _>(|t| Ok(to_field(t.try_into()?)))
                        .try_collect::<_, Fields, _>()?,
                );
                let returns = arrow_schema::Schema::new(match kind {
                    Kind::Scalar(_) => vec![to_field(return_type.clone().try_into()?)],
                    Kind::Table(_) => vec![
                        arrow_schema::Field::new("row_index", arrow_schema::DataType::Int32, true),
                        to_field(return_type.clone().try_into()?),
                    ],
                    _ => unreachable!(),
                });
                client
                    .check(&identifier, &args, &returns)
                    .await
                    .context("failed to check UDF signature")?;
            }
        }
        "wasm" => {
            identifier = wasm_identifier(
                &function_name,
                &arg_types,
                &return_type,
                matches!(kind, Kind::Table(_)),
            );

            link = match using {
                CreateFunctionUsing::Link(link) => {
                    let runtime = get_or_create_wasm_runtime(&link).await?;
                    check_wasm_function(&runtime, &identifier)?;
                    link
                }
                CreateFunctionUsing::Base64(encoded) => {
                    // decode wasm binary from base64
                    use base64::prelude::{Engine, BASE64_STANDARD};
                    let wasm_binary = BASE64_STANDARD
                        .decode(encoded)
                        .context("invalid base64 encoding")?;

                    let runtime = arrow_udf_wasm::Runtime::new(&wasm_binary)?;
                    check_wasm_function(&runtime, &identifier)?;

                    let system_params = session.env().meta_client().get_system_params().await?;
                    let object_name = format!("{:?}.wasm", md5::compute(&wasm_binary));
                    upload_wasm_binary(
                        system_params.wasm_storage_url(),
                        &object_name,
                        wasm_binary.into(),
                    )
                    .await?;

                    format!("{}/{}", system_params.wasm_storage_url(), object_name)
                }
            };
        }
        _ => unreachable!("invalid language: {language}"),
    };

    let function = Function {
        id: FunctionId::placeholder().0,
        schema_id,
        database_id,
        name: function_name,
        kind: Some(kind),
        arg_types: arg_types.into_iter().map(|t| t.into()).collect(),
        return_type: Some(return_type.into()),
        language,
        identifier,
        body: None,
        link,
        owner: session.user_id(),
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.create_function(function).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_FUNCTION))
}

/// Upload wasm binary to object store.
async fn upload_wasm_binary(
    wasm_storage_url: &str,
    object_name: &str,
    wasm_binary: Bytes,
) -> Result<()> {
    // Note: it will panic if the url is invalid. We did a validation on meta startup.
    let object_store = build_remote_object_store(
        wasm_storage_url,
        Arc::new(ObjectStoreMetrics::unused()),
        "Wasm Engine",
        ObjectStoreConfig::default(),
    )
    .await;
    object_store
        .upload(object_name, wasm_binary)
        .await
        .context("failed to upload wasm binary to object store")?;
    Ok(())
}

/// Check if the function exists in the wasm binary.
fn check_wasm_function(runtime: &arrow_udf_wasm::Runtime, identifier: &str) -> Result<()> {
    if !runtime.functions().contains(&identifier) {
        return Err(ErrorCode::InvalidParameterValue(format!(
            "function not found in wasm binary: \"{}\"\nHINT: available functions:\n  {}",
            identifier,
            runtime.functions().join("\n  ")
        ))
        .into());
    }
    Ok(())
}

/// Generate the function identifier in wasm binary.
fn wasm_identifier(name: &str, args: &[DataType], ret: &DataType, table_function: bool) -> String {
    format!(
        "{}({}){}{}",
        name,
        args.iter().map(datatype_name).join(","),
        if table_function { "->>" } else { "->" },
        datatype_name(ret)
    )
}

/// Convert a data type to string used in identifier.
fn datatype_name(ty: &DataType) -> String {
    match ty {
        DataType::Boolean => "boolean".to_string(),
        DataType::Int16 => "int2".to_string(),
        DataType::Int32 => "int4".to_string(),
        DataType::Int64 => "int8".to_string(),
        DataType::Float32 => "float4".to_string(),
        DataType::Float64 => "float8".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Time => "time".to_string(),
        DataType::Timestamp => "timestamp".to_string(),
        DataType::Timestamptz => "timestamptz".to_string(),
        DataType::Interval => "interval".to_string(),
        DataType::Decimal => "decimal".to_string(),
        DataType::Jsonb => "json".to_string(),
        DataType::Serial => "serial".to_string(),
        DataType::Int256 => "int256".to_string(),
        DataType::Bytea => "bytea".to_string(),
        DataType::Varchar => "varchar".to_string(),
        DataType::List(inner) => format!("{}[]", datatype_name(inner)),
        DataType::Struct(s) => format!(
            "struct<{}>",
            s.iter()
                .map(|(name, ty)| format!("{}:{}", name, datatype_name(ty)))
                .join(",")
        ),
    }
}
