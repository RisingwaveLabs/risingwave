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
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_sqlparser::ast::ObjectName;

use super::RwPgResponse;
use crate::binder::Binder;
use crate::catalog::CatalogError;
use crate::error::ErrorCode::PermissionDenied;
use crate::error::Result;
use crate::handler::HandlerArgs;

pub async fn handle_create_database(
    handler_args: HandlerArgs,
    database_name: ObjectName,
    if_not_exist: bool,
    owner: Option<ObjectName>,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let database_name = Binder::resolve_database_name(database_name)?;

    {
        let user_reader = session.env().user_info_reader();
        let reader = user_reader.read_guard();
        if let Some(info) = reader.get_user_by_name(&session.user_name()) {
            if !info.can_create_db && !info.is_super {
                return Err(PermissionDenied("Do not have the privilege".to_owned()).into());
            }
        } else {
            return Err(PermissionDenied("Session user is invalid".to_owned()).into());
        }
    }

    {
        let catalog_reader = session.env().catalog_reader();
        let reader = catalog_reader.read_guard();
        if reader.get_database_by_name(&database_name).is_ok() {
            // If `if_not_exist` is true, not return error.
            return if if_not_exist {
                Ok(PgResponse::builder(StatementType::CREATE_DATABASE)
                    .notice(format!("database \"{}\" exists, skipping", database_name))
                    .into())
            } else {
                Err(CatalogError::Duplicated("database", database_name).into())
            };
        }
    }

    let database_owner = if let Some(owner) = owner {
        let owner = Binder::resolve_user_name(owner)?;
        session
            .env()
            .user_info_reader()
            .read_guard()
            .get_user_by_name(&owner)
            .map(|u| u.id)
            .ok_or_else(|| CatalogError::NotFound("user", owner.clone()))?
    } else {
        session.user_id()
    };

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        // TODO: add support for create database with resource_group
        .create_database(&database_name, database_owner, DEFAULT_RESOURCE_GROUP)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_DATABASE))
}

#[cfg(test)]
mod tests {
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_database() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        frontend.run_sql("CREATE DATABASE database").await.unwrap();
        {
            let reader = catalog_reader.read_guard();
            assert!(reader.get_database_by_name("database").is_ok());
        }

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
                "CREATE DATABASE database2",
                "dev".to_owned(),
                "user".to_owned(),
                user_id,
            )
            .await;
        assert!(res.is_err());

        frontend.run_sql("CREATE USER user2 WITH NOSUPERUSER CREATEDB PASSWORD 'md5827ccb0eea8a706c4c34a16891f84e7b'").await.unwrap();
        let user_id = {
            let user_reader = session.env().user_info_reader();
            user_reader
                .read_guard()
                .get_user_by_name("user2")
                .unwrap()
                .id
        };
        frontend
            .run_user_sql(
                "CREATE DATABASE database2",
                "dev".to_owned(),
                "user2".to_owned(),
                user_id,
            )
            .await
            .unwrap();
        {
            let reader = catalog_reader.read_guard();
            assert!(reader.get_database_by_name("database2").is_ok());
        }
    }
}
