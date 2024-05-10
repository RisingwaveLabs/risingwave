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

use anyhow::Context;
use arrow_schema::Fields;
use bytes::Bytes;
use itertools::Itertools;
use pgwire::pg_response::StatementType;
use risingwave_common::array::arrow::{ToArrow, UdfArrowConvert};
use risingwave_common::catalog::FunctionId;
use risingwave_common::types::DataType;
use risingwave_expr::expr::{get_or_create_flight_client, get_or_create_wasm_runtime};
use risingwave_pb::catalog::function::{Kind, ScalarFunction, TableFunction};
use risingwave_pb::catalog::Function;
use risingwave_sqlparser::ast::{CreateFunctionBody, ObjectName, OperateFunctionArg};

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
    with_options: CreateFunctionWithOptions,
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
                "python" | "java" | "wasm" | "rust" | "javascript" => lang,
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

    let rt = match params.runtime {
        Some(runtime) => {
            if language.as_str() == "javascript" {
                runtime.to_string()
            } else {
                return Err(ErrorCode::InvalidParameterValue(
                    "runtime is only supported for javascript".to_string(),
                )
                .into());
            }
        }
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

    let mut arg_names = vec![];
    let mut arg_types = vec![];
    for arg in args.unwrap_or_default() {
        arg_names.push(arg.name.map_or("".to_string(), |n| n.real_value()));
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

    let identifier;
    let mut link = None;
    let mut body = None;
    let mut compressed_binary = None;
    let mut function_type = None;
    let mut runtime = None;

    match language.as_str() {
        "python" if params.using.is_none() => {
            identifier = function_name.to_string();
            body = Some(
                params
                    .as_
                    .ok_or_else(|| ErrorCode::InvalidParameterValue("AS must be specified".into()))?
                    .into_string(),
            );
        }
        "python" | "java" | "" => {
            let Some(CreateFunctionUsing::Link(l)) = params.using else {
                return Err(ErrorCode::InvalidParameterValue(
                    "USING LINK must be specified".to_string(),
                )
                .into());
            };
            let Some(as_) = params.as_ else {
                return Err(
                    ErrorCode::InvalidParameterValue("AS must be specified".to_string()).into(),
                );
            };
            identifier = as_.into_string();

            // check UDF server
            {
                let client = get_or_create_flight_client(&l)?;
                let convert = UdfArrowConvert {
                    legacy: client.protocol_version() == 1,
                };
                // A helper function to create a unnamed field from data type.
                let to_field = |data_type| convert.to_arrow_field("", data_type);
                let args = arrow_schema::Schema::new(
                    arg_types
                        .iter()
                        .map(to_field)
                        .try_collect::<_, Fields, _>()?,
                );
                let returns = arrow_schema::Schema::new(match kind {
                    Kind::Scalar(_) => vec![to_field(&return_type)?],
                    Kind::Table(_) => vec![
                        arrow_schema::Field::new("row", arrow_schema::DataType::Int32, true),
                        to_field(&return_type)?,
                    ],
                    _ => unreachable!(),
                });
                let function = client
                    .get(&identifier)
                    .await
                    .context("failed to check UDF signature")?;
                if !data_types_match(&function.args, &args) {
                    return Err(ErrorCode::InvalidParameterValue(format!(
                        "argument type mismatch, expect: {:?}, actual: {:?}",
                        args, function.args,
                    ))
                    .into());
                }
                if !data_types_match(&function.returns, &returns) {
                    return Err(ErrorCode::InvalidParameterValue(format!(
                        "return type mismatch, expect: {:?}, actual: {:?}",
                        returns, function.returns,
                    ))
                    .into());
                }
            }
            link = Some(l);
        }
        "javascript" if rt.as_str() != "deno" => {
            identifier = function_name.to_string();
            body = Some(
                params
                    .as_
                    .ok_or_else(|| ErrorCode::InvalidParameterValue("AS must be specified".into()))?
                    .into_string(),
            );
            runtime = Some("quickjs".to_string());
        }
        "javascript" if rt.as_str() == "deno" => {
            identifier = function_name.to_string();
            match (params.using, params.as_) {
                (None, None) => {
                    return Err(ErrorCode::InvalidParameterValue(
                        "Either USING or AS must be specified".into(),
                    )
                    .into())
                }
                (None, Some(_as)) => body = Some(_as.into_string()),
                (Some(CreateFunctionUsing::Link(link)), None) => {
                    let bytes = download_code_from_link(&link).await?;
                    compressed_binary = Some(zstd::stream::encode_all(bytes.as_slice(), 0)?);
                }
                (Some(CreateFunctionUsing::Base64(encoded)), None) => {
                    use base64::prelude::{Engine, BASE64_STANDARD};
                    let bytes = BASE64_STANDARD
                        .decode(encoded)
                        .context("invalid base64 encoding")?;
                    compressed_binary = Some(zstd::stream::encode_all(bytes.as_slice(), 0)?);
                }
                (Some(_), Some(_)) => {
                    return Err(ErrorCode::InvalidParameterValue(
                        "Both USING and AS cannot be specified".into(),
                    )
                    .into())
                }
            };

            function_type = match params.function_type {
                Some(CreateFunctionType::Sync) => Some("sync".to_string()),
                Some(CreateFunctionType::Async) => Some("async".to_string()),
                Some(CreateFunctionType::Generator) => Some("generator".to_string()),
                Some(CreateFunctionType::AsyncGenerator) => Some("async_generator".to_string()),
                None => None,
            };

            runtime = Some("deno".to_string());
        }
        "rust" => {
            if params.using.is_some() {
                return Err(ErrorCode::InvalidParameterValue(
                    "USING is not supported for rust function".to_string(),
                )
                .into());
            }
            let identifier_v1 = wasm_identifier_v1(
                &function_name,
                &arg_types,
                &return_type,
                matches!(kind, Kind::Table(_)),
            );
            // if the function returns a struct, users need to add `#[function]` macro by themselves.
            // otherwise, we add it automatically. the code should start with `fn ...`.
            let function_macro = if return_type.is_struct() {
                String::new()
            } else {
                format!("#[function(\"{}\")]", identifier_v1)
            };
            let script = params
                .as_
                .ok_or_else(|| ErrorCode::InvalidParameterValue("AS must be specified".into()))?
                .into_string();
            let script = format!(
                "use arrow_udf::{{function, types::*}};\n{}\n{}",
                function_macro, script
            );
            body = Some(script.clone());

            let wasm_binary = tokio::task::spawn_blocking(move || {
                let mut opts = arrow_udf_wasm::build::BuildOpts::default();
                opts.arrow_udf_version = Some("0.3".to_string());
                opts.script = script;
                // use a fixed tempdir to reuse the build cache
                opts.tempdir = Some(std::env::temp_dir().join("risingwave-rust-udf"));

                arrow_udf_wasm::build::build_with(&opts)
            })
            .await?
            .context("failed to build rust function")?;

            let runtime = get_or_create_wasm_runtime(&wasm_binary)?;
            identifier = find_wasm_identifier_v2(&runtime, &identifier_v1)?;

            compressed_binary = Some(zstd::stream::encode_all(wasm_binary.as_slice(), 0)?);
        }
        "wasm" => {
            let Some(using) = params.using else {
                return Err(ErrorCode::InvalidParameterValue(
                    "USING must be specified".to_string(),
                )
                .into());
            };
            let wasm_binary = match using {
                CreateFunctionUsing::Link(link) => download_binary_from_link(&link).await?,
                CreateFunctionUsing::Base64(encoded) => {
                    // decode wasm binary from base64
                    use base64::prelude::{Engine, BASE64_STANDARD};
                    BASE64_STANDARD
                        .decode(encoded)
                        .context("invalid base64 encoding")?
                        .into()
                }
            };
            let runtime = get_or_create_wasm_runtime(&wasm_binary)?;
            if runtime.abi_version().0 <= 2 {
                return Err(ErrorCode::InvalidParameterValue(
                    "legacy arrow-udf is no longer supported. please update arrow-udf to 0.3+"
                        .to_string(),
                )
                .into());
            }
            let identifier_v1 = wasm_identifier_v1(
                &function_name,
                &arg_types,
                &return_type,
                matches!(kind, Kind::Table(_)),
            );
            identifier = find_wasm_identifier_v2(&runtime, &identifier_v1)?;

            compressed_binary = Some(zstd::stream::encode_all(wasm_binary.as_ref(), 0)?);
        }
        _ => unreachable!("invalid language: {language}"),
    };

    let function = Function {
        id: FunctionId::placeholder().0,
        schema_id,
        database_id,
        name: function_name,
        kind: Some(kind),
        arg_names,
        arg_types: arg_types.into_iter().map(|t| t.into()).collect(),
        return_type: Some(return_type.into()),
        language,
        identifier: Some(identifier),
        link,
        body,
        compressed_binary,
        owner: session.user_id(),
        always_retry_on_network_error: with_options
            .always_retry_on_network_error
            .unwrap_or_default(),
        runtime,
        function_type,
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.create_function(function).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_FUNCTION))
}

/// Download wasm binary from a link.
#[allow(clippy::unused_async)]
async fn download_binary_from_link(link: &str) -> Result<Bytes> {
    // currently only local file system is supported
    if let Some(path) = link.strip_prefix("fs://") {
        let content =
            std::fs::read(path).context("failed to read wasm binary from local file system")?;
        Ok(content.into())
    } else {
        Err(ErrorCode::InvalidParameterValue("only 'fs://' is supported".to_string()).into())
    }
}

/// Convert a v0.1 function identifier to v0.2 format.
///
/// In arrow-udf v0.1 format, struct type is inline in the identifier. e.g.
///
/// ```text
/// keyvalue(varchar,varchar)->struct<key:varchar,value:varchar>
/// ```
///
/// However, since arrow-udf v0.2, struct type is no longer inline.
/// The above identifier is divided into a function and a type.
///
/// ```text
/// keyvalue(varchar,varchar)->struct KeyValue
/// KeyValue=key:varchar,value:varchar
/// ```
///
/// For compatibility, we should call `find_wasm_identifier_v2` to
/// convert v0.1 identifiers to v0.2 format before looking up the function.
fn find_wasm_identifier_v2(
    runtime: &arrow_udf_wasm::Runtime,
    inlined_signature: &str,
) -> Result<String> {
    // Inline types in function signature.
    //
    // # Example
    //
    // ```text
    // types = { "KeyValue": "key:varchar,value:varchar" }
    // input = "keyvalue(varchar, varchar) -> struct KeyValue"
    // output = "keyvalue(varchar, varchar) -> struct<key:varchar,value:varchar>"
    // ```
    let inline_types = |s: &str| -> String {
        let mut inlined = s.to_string();
        // iteratively replace `struct Xxx` with `struct<...>` until no replacement is made.
        loop {
            let replaced = inlined.clone();
            for (k, v) in runtime.types() {
                inlined = inlined.replace(&format!("struct {k}"), &format!("struct<{v}>"));
            }
            if replaced == inlined {
                return inlined;
            }
        }
    };
    // Function signature in arrow-udf is case sensitive.
    // However, SQL identifiers are usually case insensitive and stored in lowercase.
    // So we should convert the signature to lowercase before comparison.
    let identifier = runtime
        .functions()
        .find(|f| inline_types(f).to_lowercase() == inlined_signature)
        .ok_or_else(|| {
            ErrorCode::InvalidParameterValue(format!(
                "function not found in wasm binary: \"{}\"\nHINT: available functions:\n  {}\navailable types:\n  {}",
                inlined_signature,
                runtime.functions().join("\n  "),
                runtime.types().map(|(k, v)| format!("{k}: {v}")).join("\n  "),
            ))
        })?;
    Ok(identifier.into())
}

/// Download wasm binary from a link.
#[allow(clippy::unused_async)]
async fn download_code_from_link(link: &str) -> Result<Vec<u8>> {
    // currently only local file system is supported
    if let Some(path) = link.strip_prefix("fs://") {
        let content =
            std::fs::read(path).context("failed to read the code from local file system")?;
        Ok(content)
    } else {
        Err(ErrorCode::InvalidParameterValue("only 'fs://' is supported".to_string()).into())
    }
}

/// Generate a function identifier in v0.1 format from the function signature.
fn wasm_identifier_v1(
    name: &str,
    args: &[DataType],
    ret: &DataType,
    table_function: bool,
) -> String {
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
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Date => "date32".to_string(),
        DataType::Time => "time64".to_string(),
        DataType::Timestamp => "timestamp".to_string(),
        DataType::Timestamptz => "timestamptz".to_string(),
        DataType::Interval => "interval".to_string(),
        DataType::Decimal => "decimal".to_string(),
        DataType::Jsonb => "json".to_string(),
        DataType::Serial => "serial".to_string(),
        DataType::Int256 => "int256".to_string(),
        DataType::Bytea => "binary".to_string(),
        DataType::Varchar => "string".to_string(),
        DataType::List(inner) => format!("{}[]", datatype_name(inner)),
        DataType::Struct(s) => format!(
            "struct<{}>",
            s.iter()
                .map(|(name, ty)| format!("{}:{}", name, datatype_name(ty)))
                .join(",")
        ),
    }
}

/// Check if two list of data types match, ignoring field names.
fn data_types_match(a: &arrow_schema::Schema, b: &arrow_schema::Schema) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    #[allow(clippy::disallowed_methods)]
    a.fields()
        .iter()
        .zip(b.fields())
        .all(|(a, b)| a.data_type().equals_datatype(b.data_type()))
}
