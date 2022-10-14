// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{DropMode, ObjectName};

use super::RwPgResponse;
use crate::binder::Binder;
use crate::session::OptimizerContext;

pub async fn handle_drop_database(
    context: OptimizerContext,
    database_name: ObjectName,
    if_exists: bool,
    mode: Option<DropMode>,
) -> Result<RwPgResponse> {
    let session = context.session_ctx;
    let catalog_reader = session.env().catalog_reader();
    let database_name = Binder::resolve_database_name(database_name)?;
    if session.database() == database_name {
        return Err(ErrorCode::InternalError(
            "cannot drop the currently open database".to_string(),
        )
        .into());
    }
    if mode.is_some() {
        return Err(ErrorCode::BindError("Drop database not support drop mode".to_string()).into());
    }
    let database = {
        let reader = catalog_reader.read_guard();
        match reader.get_database_by_name(&database_name) {
            Ok(db) => db.clone(),
            Err(err) => {
                // Unable to find this database. If `if_exists` is true,
                // we can just return success.
                return if if_exists {
                    Ok(PgResponse::empty_result_with_notice(
                        StatementType::DROP_DATABASE,
                        format!("database {} does not exist, skipping", database_name),
                    ))
                } else {
                    Err(err)
                };
            }
        }
    };

    if session.user_id() != database.owner() {
        return Err(ErrorCode::PermissionDenied("Do not have the privilege".to_string()).into());
    }

    let catalog_writer = session.env().catalog_writer();
    catalog_writer.drop_database(database.id()).await?;
    Ok(PgResponse::empty_result(StatementType::DROP_DATABASE))
}

#[cfg(test)]
mod tests {
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_drop_database() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        frontend.run_sql("CREATE DATABASE database").await.unwrap();

        frontend.run_sql("CREATE SCHEMA schema").await.unwrap();

        frontend.run_sql("DROP SCHEMA public").await.unwrap();

        frontend.run_sql("CREATE USER user WITH NOSUPERUSER NOCREATEDB PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'").await.unwrap();
        let user_id = {
            let user_reader = session.env().user_info_reader();
            user_reader
                .read_guard()
                .get_user_by_name("user")
                .unwrap()
                .id
        };
        let res = frontend
            .run_user_sql(
                "DROP DATABASE database",
                "dev".to_string(),
                "user".to_string(),
                user_id,
            )
            .await;
        assert!(res.is_err());

        frontend.run_sql("DROP DATABASE database").await.unwrap();

        let database = catalog_reader
            .read_guard()
            .get_database_by_name("database")
            .ok()
            .cloned();
        assert!(database.is_none());
    }
}
