// Copyright 2023 Singularity Data
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
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{ColumnDef, ObjectName, Statement};
use risingwave_sqlparser::parser::Parser;

use super::create_table::{gen_create_table_plan, VersionedTableColumnIdGenerator};
use super::{HandlerArgs, RwPgResponse};
use crate::binder::Relation;
use crate::{build_graph, Binder, OptimizerContext, TableCatalog};

#[allow(clippy::unused_async)]
pub async fn handle_add_column(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    new_column: ColumnDef,
) -> Result<RwPgResponse> {
    let session = handler_args.session;

    let original_catalog = {
        let relation = Binder::new(&session).bind_relation_by_name(table_name.clone(), None)?;
        match relation {
            Relation::BaseTable(table) if table.table_catalog.is_table() => table.table_catalog,
            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "\"{table_name}\" is not a table or cannot be altered"
            )))?,
        }
    };

    // Do not allow altering a table with a connector.
    if original_catalog.associated_source_id().is_some() {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "cannot alter table \"{}\" because it has a connector",
            table_name
        )))?
    }

    // Retrieve the original table definition and parse it to AST.
    let [mut definition]: [_; 1] = Parser::parse_sql(&original_catalog.definition)
        .context("unable to parse original table definition")?
        .try_into()
        .unwrap();
    let Statement::CreateTable { columns, .. } = &mut definition else {
        panic!("unexpected statement: {:?}", definition);
    };

    // Duplicated names can actually be checked by `StreamMaterialize`. We do here for better error
    // reporting.
    let new_column_name = new_column.name.real_value();
    if columns
        .iter()
        .any(|c| c.name.real_value() == new_column_name)
    {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{}\" of table \"{}\" already exists",
            new_column_name, table_name
        )))?
    }
    // Add the new column to the table definition.
    columns.push(new_column);

    // Create handler args as if we're creating a new table with the altered definition.
    let handler_args = HandlerArgs::new(session.clone(), &definition, "")?;
    let col_id_gen = VersionedTableColumnIdGenerator::for_alter_table(&original_catalog);
    let Statement::CreateTable { columns, constraints, .. } = definition else {
        panic!("unexpected statement type: {:?}", definition);
    };

    let (graph, source, table) = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let (plan, source, table) =
            gen_create_table_plan(context, table_name, columns, constraints, col_id_gen)?;

        // TODO: avoid this backward conversion.
        if TableCatalog::from(&table).pk_column_ids() != original_catalog.pk_column_ids() {
            Err(ErrorCode::InvalidInputSyntax(
                "alter primary key of table is not supported".to_owned(),
            ))?
        }

        let graph = build_graph(plan);

        (graph, source, table)
    };

    // TODO: for test purpose only, we drop the original table and create a new one. This is wrong
    // and really dangerous in production.
    #[cfg(debug_assertions)]
    {
        let catalog_writer = session.env().catalog_writer();

        catalog_writer
            .drop_table(None, original_catalog.id())
            .await?;
        catalog_writer.create_table(source, table, graph).await?;

        Ok(PgResponse::empty_result_with_notice(
            StatementType::ALTER_TABLE,
            "The `ALTER TABLE` feature is incomplete and not data is preserved! This feature is not available in production.".to_owned(),
        ))
    }

    #[cfg(not(debug_assertions))]
    {
        let (_, _, _) = (graph, source, table);
        Err(ErrorCode::NotImplemented(
            "ADD COLUMN".to_owned(),
            6903.into(),
        ))?
    }
}
