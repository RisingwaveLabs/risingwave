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

use std::fmt::Display;

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc};
use risingwave_common::types::Fields;
use risingwave_sqlparser::ast::{display_comma_separated, ObjectName};

use super::show::ShowColumnRow;
use super::{fields_to_descriptors, RwPgResponse};
use crate::binder::{Binder, Relation};
use crate::catalog::CatalogError;
use crate::error::Result;
use crate::handler::{HandlerArgs, RwPgResponseBuilderExt};

pub fn handle_describe(handler_args: HandlerArgs, object_name: ObjectName) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let mut binder = Binder::new_for_system(&session);

    Binder::validate_cross_db_reference(&session.database(), &object_name)?;
    let not_found_err =
        CatalogError::NotFound("table, source, sink or view", object_name.to_string());

    // Vec<ColumnCatalog>, Vec<ColumnDesc>, Vec<ColumnDesc>, Vec<Arc<IndexCatalog>>, String, Option<String>
    let (columns, pk_columns, dist_columns, indices, relname, description) = if let Ok(relation) =
        binder.bind_relation_by_name(object_name.clone(), None, None, false)
    {
        match relation {
            Relation::Source(s) => {
                let pk_column_catalogs = s
                    .catalog
                    .pk_col_ids
                    .iter()
                    .map(|&column_id| {
                        s.catalog
                            .columns
                            .iter()
                            .filter(|x| x.column_id() == column_id)
                            .map(|x| x.column_desc.clone())
                            .exactly_one()
                            .unwrap()
                    })
                    .collect_vec();
                (
                    s.catalog.columns,
                    pk_column_catalogs,
                    vec![],
                    vec![],
                    s.catalog.name,
                    None, // Description
                )
            }
            Relation::BaseTable(t) => {
                let pk_column_catalogs = t
                    .table_catalog
                    .pk()
                    .iter()
                    .map(|x| t.table_catalog.columns[x.column_index].column_desc.clone())
                    .collect_vec();
                let dist_columns = t
                    .table_catalog
                    .distribution_key()
                    .iter()
                    .map(|idx| t.table_catalog.columns[*idx].column_desc.clone())
                    .collect_vec();
                (
                    t.table_catalog.columns.clone(),
                    pk_column_catalogs,
                    dist_columns,
                    t.table_indexes,
                    t.table_catalog.name.clone(),
                    t.table_catalog.description.clone(),
                )
            }
            Relation::SystemTable(t) => {
                let pk_column_catalogs = t
                    .sys_table_catalog
                    .pk
                    .iter()
                    .map(|idx| t.sys_table_catalog.columns[*idx].column_desc.clone())
                    .collect_vec();
                (
                    t.sys_table_catalog.columns.clone(),
                    pk_column_catalogs,
                    vec![],
                    vec![],
                    t.sys_table_catalog.name.clone(),
                    None, // Description
                )
            }
            Relation::Share(_) => {
                if let Ok(view) = binder.bind_view_by_name(object_name.clone()) {
                    let columns = view
                        .view_catalog
                        .columns
                        .iter()
                        .enumerate()
                        .map(|(idx, field)| ColumnCatalog {
                            column_desc: ColumnDesc::from_field_with_column_id(field, idx as _),
                            is_hidden: false,
                        })
                        .collect();
                    (
                        columns,
                        vec![],
                        vec![],
                        vec![],
                        view.view_catalog.name.clone(),
                        None,
                    )
                } else {
                    return Err(not_found_err.into());
                }
            }
            _ => {
                return Err(not_found_err.into());
            }
        }
    } else if let Ok(sink) = binder.bind_sink_by_name(object_name.clone()) {
        let columns = sink.sink_catalog.full_columns().to_vec();
        let pk_columns = sink
            .sink_catalog
            .downstream_pk_indices()
            .into_iter()
            .map(|idx| columns[idx].column_desc.clone())
            .collect_vec();
        let dist_columns = sink
            .sink_catalog
            .distribution_key
            .iter()
            .map(|idx| columns[*idx].column_desc.clone())
            .collect_vec();
        (
            columns,
            pk_columns,
            dist_columns,
            vec![],
            sink.sink_catalog.name.clone(),
            None,
        )
    } else {
        return Err(not_found_err.into());
    };

    // Convert all column descs to rows
    let mut rows = columns
        .into_iter()
        .flat_map(ShowColumnRow::from_catalog)
        .collect_vec();

    fn concat<T>(display_elems: impl IntoIterator<Item = T>) -> String
    where
        T: Display,
    {
        format!(
            "{}",
            display_comma_separated(&display_elems.into_iter().collect::<Vec<_>>())
        )
    }

    // Convert primary key to rows
    if !pk_columns.is_empty() {
        rows.push(ShowColumnRow {
            name: "primary key".into(),
            r#type: concat(pk_columns.iter().map(|x| &x.name)),
            is_hidden: None,
            description: None,
        });
    }

    // Convert distribution keys to rows
    if !dist_columns.is_empty() {
        rows.push(ShowColumnRow {
            name: "distribution key".into(),
            r#type: concat(dist_columns.iter().map(|x| &x.name)),
            is_hidden: None,
            description: None,
        });
    }

    // Convert all indexes to rows
    rows.extend(indices.iter().map(|index| {
        let index_display = index.display();

        ShowColumnRow {
            name: index.name.clone(),
            r#type: if index_display.include_columns.is_empty() {
                format!(
                    "index({}) distributed by({})",
                    display_comma_separated(&index_display.index_columns_with_ordering),
                    display_comma_separated(&index_display.distributed_by_columns),
                )
            } else {
                format!(
                    "index({}) include({}) distributed by({})",
                    display_comma_separated(&index_display.index_columns_with_ordering),
                    display_comma_separated(&index_display.include_columns),
                    display_comma_separated(&index_display.distributed_by_columns),
                )
            },
            is_hidden: None,
            // TODO: index description
            description: None,
        }
    }));

    rows.push(ShowColumnRow {
        name: "table description".into(),
        r#type: relname,
        is_hidden: None,
        description: description.map(Into::into),
    });

    // TODO: table name and description as title of response
    // TODO: recover the original user statement
    Ok(PgResponse::builder(StatementType::DESCRIBE)
        .rows(rows)
        .into())
}

pub fn infer_describe() -> Vec<PgFieldDescriptor> {
    fields_to_descriptors(ShowColumnRow::fields())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Index;

    use futures_async_stream::for_await;

    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_describe_handler() {
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend
            .run_sql("create table t (v1 int, v2 int, v3 int primary key, v4 int);")
            .await
            .unwrap();

        frontend
            .run_sql("create index idx1 on t (v1 DESC, v2);")
            .await
            .unwrap();

        let sql = "describe t";
        let mut pg_response = frontend.run_sql(sql).await.unwrap();

        let mut columns = HashMap::new();
        #[for_await]
        for row_set in pg_response.values_stream() {
            let row_set = row_set.unwrap();
            for row in row_set {
                columns.insert(
                    std::str::from_utf8(row.index(0).as_ref().unwrap())
                        .unwrap()
                        .to_owned(),
                    std::str::from_utf8(row.index(1).as_ref().unwrap())
                        .unwrap()
                        .to_owned(),
                );
            }
        }

        let expected_columns: HashMap<String, String> = maplit::hashmap! {
            "v1".into() => "integer".into(),
            "v2".into() => "integer".into(),
            "v3".into() => "integer".into(),
            "v4".into() => "integer".into(),
            "primary key".into() => "v3".into(),
            "distribution key".into() => "v3".into(),
            "_rw_timestamp".into() => "timestamp with time zone".into(),
            "idx1".into() => "index(v1 DESC, v2 ASC, v3 ASC) include(v4) distributed by(v1)".into(),
            "table description".into() => "t".into(),
        };

        assert_eq!(columns, expected_columns);
    }
}
