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

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::is_system_schema;
use risingwave_sqlparser::ast::{DropMode, ObjectName};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::error::{ErrorCode, Result};
use crate::handler::HandlerArgs;

pub async fn handle_drop_schema(
    handler_args: HandlerArgs,
    schema_name: ObjectName,
    if_exist: bool,
    mode: Option<DropMode>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let catalog_reader = session.env().catalog_reader();
    let schema_name = Binder::resolve_schema_name(schema_name)?;

    if is_system_schema(&schema_name) {
        return Err(ErrorCode::ProtocolError(format!(
            "cannot drop schema {} because it is required by the database system",
            schema_name
        ))
        .into());
    }

    let schema = {
        let reader = catalog_reader.read_guard();
        match reader.get_schema_by_name(&session.database(), &schema_name) {
            Ok(schema) => schema.clone(),
            Err(err) => {
                // If `if_exist` is true, not return error.
                return if if_exist {
                    Ok(PgResponse::builder(StatementType::DROP_SCHEMA)
                        .notice(format!(
                            "schema \"{}\" does not exist, skipping",
                            schema_name
                        ))
                        .into())
                } else {
                    Err(err.into())
                };
            }
        }
    };

    // Note: we don't check if the schema is empty here when drop mode is cascade.
    // The check is done in meta `ensure_schema_empty`.
    let cascade = matches!(mode, Some(DropMode::Cascade));

    session.check_privilege_for_drop_alter_db_schema(&schema)?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer.drop_schema(schema.id(), cascade).await?;
    Ok(PgResponse::empty_result(StatementType::DROP_SCHEMA))
}
