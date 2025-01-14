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

use either::Either;
use pgwire::pg_response::StatementType;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc};
use risingwave_pb::ddl_service::TableJobType;
use risingwave_sqlparser::ast::{ColumnDef, ObjectName, OnConflict, Query, Statement};

use super::{HandlerArgs, RwPgResponse};
use crate::binder::BoundStatement;
use crate::error::{ErrorCode, Result};
use crate::handler::create_table::{gen_create_table_plan_without_source, ColumnIdGenerator};
use crate::handler::query::handle_query;
use crate::{build_graph, Binder, OptimizerContext};
pub async fn handle_create_as(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    if_not_exists: bool,
    query: Box<Query>,
    column_defs: Vec<ColumnDef>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    ast_engine: risingwave_sqlparser::ast::Engine,
) -> Result<RwPgResponse> {
    if column_defs.iter().any(|column| column.data_type.is_some()) {
        return Err(ErrorCode::InvalidInputSyntax(
            "Should not specify data type in CREATE TABLE AS".into(),
        )
        .into());
    }
    let engine = match ast_engine {
        risingwave_sqlparser::ast::Engine::Hummock => risingwave_common::catalog::Engine::Hummock,
        risingwave_sqlparser::ast::Engine::Iceberg => risingwave_common::catalog::Engine::Iceberg,
    };

    let session = handler_args.session.clone();

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        table_name.clone(),
        StatementType::CREATE_TABLE,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    let mut col_id_gen = ColumnIdGenerator::new_initial();

    // Generate catalog descs from query
    let mut columns: Vec<_> = {
        let mut binder = Binder::new(&session);
        let bound = binder.bind(Statement::Query(query.clone()))?;
        if let BoundStatement::Query(query) = bound {
            // Create ColumnCatelog by Field
            query
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    col_id_gen.generate(field).map(|id| ColumnCatalog {
                        column_desc: ColumnDesc::from_field_with_column_id(field, id.get_id()),
                        is_hidden: false,
                    })
                })
                .try_collect()?
        } else {
            unreachable!()
        }
    };

    if column_defs.len() > columns.len() {
        return Err(ErrorCode::InvalidInputSyntax(
            "too many column names were specified".to_owned(),
        )
        .into());
    }

    // Override column name if it specified in creaet statement.
    column_defs.iter().enumerate().for_each(|(idx, column)| {
        columns[idx].column_desc.name = column.name.real_value();
    });

    let (graph, source, table) = {
        let context = OptimizerContext::from_handler_args(handler_args.clone());
        let (_, secret_refs, connection_refs) = context.with_options().clone().into_parts();
        if !secret_refs.is_empty() || !connection_refs.is_empty() {
            return Err(crate::error::ErrorCode::InvalidParameterValue(
                "Secret reference and Connection reference are not allowed in options for CREATE TABLE AS".to_owned(),
            )
            .into());
        }
        let (plan, table) = gen_create_table_plan_without_source(
            context,
            table_name.clone(),
            columns,
            vec![],
            vec![],
            "".to_owned(), // TODO: support `SHOW CREATE TABLE` for `CREATE TABLE AS`
            vec![],        // No watermark should be defined in for `CREATE TABLE AS`
            append_only,
            on_conflict,
            with_version_column,
            Some(col_id_gen.into_version()),
            None,
            engine,
        )?;
        let graph = build_graph(plan)?;

        (graph, None, table)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .create_table(source, table, graph, TableJobType::Unspecified)
        .await?;

    // Generate insert
    let insert = Statement::Insert {
        table_name,
        columns: vec![],
        source: query,
        returning: vec![],
    };

    handle_query(handler_args, insert, vec![]).await
}
