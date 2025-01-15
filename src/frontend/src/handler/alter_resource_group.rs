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

use pgwire::pg_response::StatementType;
use risingwave_common::bail;
use risingwave_sqlparser::ast::{ObjectName, SetVariableValue, SetVariableValueSingle, Value};

use super::{HandlerArgs, RwPgResponse};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::table_catalog::TableType;
use crate::error::{ErrorCode, Result};
use crate::Binder;

pub async fn handle_alter_resource_group(
    handler_args: HandlerArgs,
    obj_name: ObjectName,
    resource_group: Option<SetVariableValue>,
    stmt_type: StatementType,
    deferred: bool,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(&db_name, obj_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;
    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);

    let table_id = {
        let reader = session.env().catalog_reader().read_guard();

        match stmt_type {
            StatementType::ALTER_MATERIALIZED_VIEW => {
                let (table, schema_name) =
                    reader.get_created_table_by_name(&db_name, schema_path, &real_table_name)?;

                match (table.table_type(), stmt_type) {
                    (TableType::MaterializedView, StatementType::ALTER_MATERIALIZED_VIEW) => {}
                    _ => {
                        return Err(ErrorCode::InvalidInputSyntax(format!(
                            "cannot alter resource group of {} {} by {}",
                            table.table_type().to_prost().as_str_name(),
                            table.name(),
                            stmt_type,
                        ))
                        .into());
                    }
                }

                session.check_privilege_for_drop_alter(schema_name, &**table)?;
                table.id.table_id()
            }
            _ => bail!(
                "invalid statement type for alter resource group: {:?}",
                stmt_type
            ),
        }
    };

    let resource_group = match resource_group {
        None => None,
        Some(SetVariableValue::Single(SetVariableValueSingle::Ident(ident))) => {
            Some(ident.real_value())
        }
        Some(SetVariableValue::Single(SetVariableValueSingle::Literal(
            Value::SingleQuotedString(v),
        ))) => Some(v),
        _ => {
            return Err(ErrorCode::InvalidInputSyntax(
                "target parallelism must be a valid number or adaptive".to_owned(),
            )
            .into());
        }
    };

    let mut builder = RwPgResponse::builder(stmt_type);

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .alter_resource_group(table_id, resource_group, deferred)
        .await?;

    if deferred {
        builder = builder.notice("DEFERRED is used, please ensure that automatic parallelism control is enabled on the meta, otherwise, the alter will not take effect.".to_owned());
    }

    Ok(builder.into())
}
