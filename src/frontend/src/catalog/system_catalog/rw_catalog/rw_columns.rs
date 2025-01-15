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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;
use crate::expr::{ExprDisplay, ExprImpl};
use crate::user::has_access_to_object;
use crate::user::user_catalog::UserCatalog;

#[derive(Fields)]
#[primary_key(relation_id, name)]
struct RwColumn {
    relation_id: i32,
    // belonged relation id
    name: String,
    // column name
    position: i32,
    // 1-indexed position
    is_hidden: bool,
    is_primary_key: bool,
    is_distribution_key: bool,
    is_generated: bool,
    generation_expression: Option<String>,
    data_type: String,
    type_oid: i32,
    type_len: i16,
    udt_type: String,
}

#[system_catalog(table, "rw_catalog.rw_columns")]
fn read_rw_columns(reader: &SysCatalogReaderImpl) -> Result<Vec<RwColumn>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let user_reader = reader.user_info_reader.read_guard();
    let current_user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .expect("user not found");
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;

    Ok(schemas
        .flat_map(|s| read_rw_columns_in_schema(current_user, s))
        .collect())
}

fn read_rw_columns_in_schema(current_user: &UserCatalog, schema: &SchemaCatalog) -> Vec<RwColumn> {
    let view_rows = schema
        .iter_view()
        .filter(|v| {
            v.is_system_view() || has_access_to_object(current_user, &schema.name, v.id, v.owner)
        })
        .flat_map(|view| {
            view.columns
                .iter()
                .enumerate()
                .map(|(index, column)| RwColumn {
                    relation_id: view.id as i32,
                    name: column.name.clone(),
                    position: index as i32 + 1,
                    is_hidden: false,
                    is_primary_key: false,
                    is_distribution_key: false,
                    is_generated: false,
                    generation_expression: None,
                    data_type: column.data_type().to_string(),
                    type_oid: column.data_type().to_oid(),
                    type_len: column.data_type().type_len(),
                    udt_type: column.data_type().pg_name().into(),
                })
        });

    let sink_rows = schema
        .iter_sink()
        .filter(|s| has_access_to_object(current_user, &schema.name, s.id.sink_id, s.owner.user_id))
        .flat_map(|sink| {
            sink.full_columns()
                .iter()
                .enumerate()
                .map(|(index, column)| RwColumn {
                    relation_id: sink.id.sink_id as i32,
                    name: column.name().into(),
                    position: index as i32 + 1,
                    is_hidden: column.is_hidden,
                    is_primary_key: sink.downstream_pk.contains(&index),
                    is_distribution_key: sink.distribution_key.contains(&index),
                    is_generated: false,
                    generation_expression: None,
                    data_type: column.data_type().to_string(),
                    type_oid: column.data_type().to_oid(),
                    type_len: column.data_type().type_len(),
                    udt_type: column.data_type().pg_name().into(),
                })
        });

    let catalog_rows = schema.iter_system_tables().flat_map(|table| {
        table
            .columns
            .iter()
            .enumerate()
            .map(move |(index, column)| RwColumn {
                relation_id: table.id.table_id as i32,
                name: column.name().into(),
                position: index as i32 + 1,
                is_hidden: column.is_hidden,
                is_primary_key: table.pk.contains(&index),
                is_distribution_key: false,
                is_generated: false,
                generation_expression: None,
                data_type: column.data_type().to_string(),
                type_oid: column.data_type().to_oid(),
                type_len: column.data_type().type_len(),
                udt_type: column.data_type().pg_name().into(),
            })
    });

    let table_rows = schema
        .iter_table_mv_indices()
        .filter(|t| has_access_to_object(current_user, &schema.name, t.id.table_id, t.owner))
        .flat_map(|table| {
            let schema = table.column_schema();
            table
                .columns
                .iter()
                .enumerate()
                .map(move |(index, column)| RwColumn {
                    relation_id: table.id.table_id as i32,
                    name: column.name().into(),
                    position: index as i32 + 1,
                    is_hidden: column.is_hidden,
                    is_primary_key: table.pk().iter().any(|idx| idx.column_index == index),
                    is_distribution_key: table.distribution_key.contains(&index),
                    is_generated: column.is_generated(),
                    generation_expression: column.generated_expr().map(|expr_node| {
                        let expr = ExprImpl::from_expr_proto(expr_node).unwrap();
                        let expr_display = ExprDisplay {
                            expr: &expr,
                            input_schema: &schema,
                        };
                        expr_display.to_string()
                    }),
                    data_type: column.data_type().to_string(),
                    type_oid: column.data_type().to_oid(),
                    type_len: column.data_type().type_len(),
                    udt_type: column.data_type().pg_name().into(),
                })
        });

    let schema_rows = schema
        .iter_source()
        .filter(|s| has_access_to_object(current_user, &schema.name, s.id, s.owner))
        .flat_map(|source| {
            source
                .columns
                .iter()
                .enumerate()
                .map(move |(index, column)| RwColumn {
                    relation_id: source.id as i32,
                    name: column.name().into(),
                    position: index as i32 + 1,
                    is_hidden: column.is_hidden,
                    is_primary_key: source.pk_col_ids.contains(&column.column_id()),
                    is_distribution_key: false,
                    is_generated: false,
                    generation_expression: None,
                    data_type: column.data_type().to_string(),
                    type_oid: column.data_type().to_oid(),
                    type_len: column.data_type().type_len(),
                    udt_type: column.data_type().pg_name().into(),
                })
        });

    view_rows
        .chain(sink_rows)
        .chain(catalog_rows)
        .chain(table_rows)
        .chain(schema_rows)
        .collect()
}
