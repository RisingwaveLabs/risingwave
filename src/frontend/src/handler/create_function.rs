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

use anyhow::Context;
use either::Either;
use risingwave_common::catalog::FunctionId;
use risingwave_common::types::StructType;
use risingwave_expr::sig::{CreateOptions, UdfKind};
use risingwave_pb::catalog::function::{Kind, ScalarFunction, TableFunction};
use risingwave_pb::catalog::Function;

use super::*;
use crate::{bind_data_type, Binder};

pub async fn handle_create_function(
    handler_args: HandlerArgs,
    or_replace: bool,
    temporary: bool,
    if_not_exists: bool,
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
        None => "".to_owned(),
    };

    let runtime = match params.runtime {
        Some(_) => {
            return Err(ErrorCode::InvalidParameterValue(
                "runtime selection is currently not supported".to_owned(),
            )
            .into());
        }
        None => None,
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
                    .map(|c| bind_data_type(&c.data_type).map(|ty| (c.name.real_value(), ty)));
                let fields = it.try_collect::<_, Vec<_>, _>()?;
                return_type = StructType::new(fields).into();
            }
            Kind::Table(TableFunction {})
        }
        None => {
            return Err(ErrorCode::InvalidParameterValue(
                "return type must be specified".to_owned(),
            )
            .into())
        }
    };

    let mut arg_names = vec![];
    let mut arg_types = vec![];
    for arg in args.unwrap_or_default() {
        arg_names.push(arg.name.map_or("".to_owned(), |n| n.real_value()));
        arg_types.push(bind_data_type(&arg.data_type)?);
    }

    // resolve database and schema id
    let session = &handler_args.session;
    let db_name = &session.database();
    let (schema_name, function_name) =
        Binder::resolve_schema_qualified_name(db_name, name.clone())?;
    let (database_id, schema_id) = session.get_database_and_schema_id_for_create(schema_name)?;

    // check if the function exists in the catalog
    if let Either::Right(resp) = session.check_function_name_duplicated(
        StatementType::CREATE_FUNCTION,
        name,
        &arg_types,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    let link = match &params.using {
        Some(CreateFunctionUsing::Link(l)) => Some(l.as_str()),
        _ => None,
    };
    let base64_decoded = match &params.using {
        Some(CreateFunctionUsing::Base64(encoded)) => {
            use base64::prelude::{Engine, BASE64_STANDARD};
            let bytes = BASE64_STANDARD
                .decode(encoded)
                .context("invalid base64 encoding")?;
            Some(bytes)
        }
        _ => None,
    };

    let create_fn =
        risingwave_expr::sig::find_udf_impl(&language, runtime.as_deref(), link)?.create_fn;
    let output = create_fn(CreateOptions {
        kind: match kind {
            Kind::Scalar(_) => UdfKind::Scalar,
            Kind::Table(_) => UdfKind::Table,
            Kind::Aggregate(_) => unreachable!(),
        },
        name: &function_name,
        arg_names: &arg_names,
        arg_types: &arg_types,
        return_type: &return_type,
        as_: params.as_.as_ref().map(|s| s.as_str()),
        using_link: link,
        using_base64_decoded: base64_decoded.as_deref(),
    })?;

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
        runtime,
        identifier: Some(output.name_in_runtime),
        link: link.map(|s| s.to_owned()),
        body: output.body,
        compressed_binary: output.compressed_binary,
        owner: session.user_id(),
        always_retry_on_network_error: with_options
            .always_retry_on_network_error
            .unwrap_or_default(),
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.create_function(function).await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_FUNCTION))
}
