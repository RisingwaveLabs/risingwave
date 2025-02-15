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

use std::sync::Arc;

use either::Either;
use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{debug_assert_column_ids_distinct, is_system_schema, Field};
use risingwave_common::session_config::USER_NAME_WILD_CARD;
use risingwave_connector::WithPropertiesExt;
use risingwave_sqlparser::ast::{AsOf, Statement, TableAlias};
use risingwave_sqlparser::parser::Parser;
use thiserror_ext::AsReport;

use super::BoundShare;
use crate::binder::relation::BoundShareInput;
use crate::binder::{Binder, Relation};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::system_catalog::SystemTableCatalog;
use crate::catalog::table_catalog::{TableCatalog, TableType};
use crate::catalog::view_catalog::ViewCatalog;
use crate::catalog::{CatalogError, IndexCatalog, TableId};
use crate::error::{ErrorCode, Result, RwError};

#[derive(Debug, Clone)]
pub struct BoundBaseTable {
    pub table_id: TableId,
    pub table_catalog: Arc<TableCatalog>,
    pub table_indexes: Vec<Arc<IndexCatalog>>,
    pub as_of: Option<AsOf>,
}

#[derive(Debug, Clone)]
pub struct BoundSystemTable {
    pub table_id: TableId,
    pub sys_table_catalog: Arc<SystemTableCatalog>,
}

#[derive(Debug, Clone)]
pub struct BoundSource {
    pub catalog: SourceCatalog,
    pub as_of: Option<AsOf>,
}

impl BoundSource {
    pub fn is_shareable_cdc_connector(&self) -> bool {
        self.catalog.with_properties.is_shareable_cdc_connector()
    }

    pub fn is_shared(&self) -> bool {
        self.catalog.info.is_shared()
    }
}

impl Binder {
    /// Binds table or source, or logical view according to what we get from the catalog.
    pub fn bind_relation_by_name_inner(
        &mut self,
        db_name: Option<&str>,
        schema_name: Option<&str>,
        table_name: &str,
        alias: Option<TableAlias>,
        as_of: Option<AsOf>,
    ) -> Result<Relation> {
        // define some helper functions converting catalog to bound relation
        let resolve_sys_table_relation = |sys_table_catalog: &Arc<SystemTableCatalog>| {
            let table = BoundSystemTable {
                table_id: sys_table_catalog.id(),
                sys_table_catalog: sys_table_catalog.clone(),
            };
            (
                Relation::SystemTable(Box::new(table)),
                sys_table_catalog
                    .columns
                    .iter()
                    .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
                    .collect_vec(),
            )
        };

        // start to bind
        let (ret, columns) = {
            match schema_name {
                Some(schema_name) => {
                    let db_name = db_name.unwrap_or(&self.db_name);
                    let schema_path = SchemaPath::Name(schema_name);
                    if is_system_schema(schema_name) {
                        if let Ok(sys_table_catalog) =
                            self.catalog
                                .get_sys_table_by_name(db_name, schema_name, table_name)
                        {
                            resolve_sys_table_relation(sys_table_catalog)
                        } else if let Ok((view_catalog, _)) =
                            self.catalog
                                .get_view_by_name(db_name, schema_path, table_name)
                        {
                            self.resolve_view_relation(&view_catalog.clone())?
                        } else {
                            bail_not_implemented!(
                                issue = 1695,
                                r###"{}.{} is not supported, please use `SHOW` commands for now.
`SHOW TABLES`,
`SHOW MATERIALIZED VIEWS`,
`DESCRIBE <table>`,
`SHOW COLUMNS FROM [table]`
"###,
                                schema_name,
                                table_name
                            );
                        }
                    } else if let Some(source_catalog) =
                        self.temporary_source_manager.get_source(table_name)
                    // don't care about the database and schema
                    {
                        self.resolve_source_relation(&source_catalog.clone(), as_of)
                    } else if let Ok((table_catalog, schema_name)) = self
                        .catalog
                        .get_created_table_by_name(db_name, schema_path, table_name)
                    {
                        self.resolve_table_relation(table_catalog.clone(), schema_name, as_of)?
                    } else if let Ok((source_catalog, _)) =
                        self.catalog
                            .get_source_by_name(db_name, schema_path, table_name)
                    {
                        self.resolve_source_relation(&source_catalog.clone(), as_of)
                    } else if let Ok((view_catalog, _)) =
                        self.catalog
                            .get_view_by_name(db_name, schema_path, table_name)
                    {
                        self.resolve_view_relation(&view_catalog.clone())?
                    } else {
                        return Err(CatalogError::NotFound(
                            "table or source",
                            table_name.to_owned(),
                        )
                        .into());
                    }
                }
                None => (|| {
                    let user_name = &self.auth_context.user_name;

                    for path in self.search_path.path() {
                        if is_system_schema(path)
                            && let Ok(sys_table_catalog) =
                                self.catalog
                                    .get_sys_table_by_name(&self.db_name, path, table_name)
                        {
                            return Ok(resolve_sys_table_relation(sys_table_catalog));
                        } else {
                            let schema_name = if path == USER_NAME_WILD_CARD {
                                user_name
                            } else {
                                path
                            };

                            if let Ok(schema) =
                                self.catalog.get_schema_by_name(&self.db_name, schema_name)
                            {
                                if let Some(source_catalog) =
                                    self.temporary_source_manager.get_source(table_name)
                                // don't care about the database and schema
                                {
                                    return Ok(self
                                        .resolve_source_relation(&source_catalog.clone(), as_of));
                                } else if let Some(table_catalog) =
                                    schema.get_created_table_by_name(table_name)
                                {
                                    return self.resolve_table_relation(
                                        table_catalog.clone(),
                                        &schema_name.clone(),
                                        as_of,
                                    );
                                } else if let Some(source_catalog) =
                                    schema.get_source_by_name(table_name)
                                {
                                    return Ok(self
                                        .resolve_source_relation(&source_catalog.clone(), as_of));
                                } else if let Some(view_catalog) =
                                    schema.get_view_by_name(table_name)
                                {
                                    return self.resolve_view_relation(&view_catalog.clone());
                                }
                            }
                        }
                    }

                    Err(CatalogError::NotFound("table or source", table_name.to_owned()).into())
                })()?,
            }
        };

        self.bind_table_to_context(columns, table_name.to_owned(), alias)?;
        Ok(ret)
    }

    fn resolve_table_relation(
        &mut self,
        table_catalog: Arc<TableCatalog>,
        schema_name: &str,
        as_of: Option<AsOf>,
    ) -> Result<(Relation, Vec<(bool, Field)>)> {
        let table_id = table_catalog.id();
        let columns = table_catalog
            .columns
            .iter()
            .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
            .collect_vec();
        self.included_relations.insert(table_id);
        let table_indexes = self.resolve_table_indexes(schema_name, table_id)?;

        let table = BoundBaseTable {
            table_id,
            table_catalog,
            table_indexes,
            as_of,
        };

        Ok::<_, RwError>((Relation::BaseTable(Box::new(table)), columns))
    }

    fn resolve_source_relation(
        &mut self,
        source_catalog: &SourceCatalog,
        as_of: Option<AsOf>,
    ) -> (Relation, Vec<(bool, Field)>) {
        debug_assert_column_ids_distinct(&source_catalog.columns);
        self.included_relations.insert(source_catalog.id.into());
        (
            Relation::Source(Box::new(BoundSource {
                catalog: source_catalog.clone(),
                as_of,
            })),
            source_catalog
                .columns
                .iter()
                .map(|c| (c.is_hidden, Field::from(&c.column_desc)))
                .collect_vec(),
        )
    }

    fn resolve_view_relation(
        &mut self,
        view_catalog: &ViewCatalog,
    ) -> Result<(Relation, Vec<(bool, Field)>)> {
        let ast = Parser::parse_sql(&view_catalog.sql)
            .expect("a view's sql should be parsed successfully");
        let Statement::Query(query) = ast
            .into_iter()
            .exactly_one()
            .expect("a view should contain only one statement")
        else {
            unreachable!("a view should contain a query statement");
        };
        let query = self.bind_query(*query).map_err(|e| {
            ErrorCode::BindError(format!(
                "failed to bind view {}, sql: {}\nerror: {}",
                view_catalog.name,
                view_catalog.sql,
                e.as_report()
            ))
        })?;

        let columns = view_catalog.columns.clone();

        if !itertools::equal(
            query.schema().fields().iter().map(|f| &f.data_type),
            view_catalog.columns.iter().map(|f| &f.data_type),
        ) {
            return Err(ErrorCode::BindError(format!(
                "failed to bind view {}. The SQL's schema is different from catalog's schema sql: {}, bound schema: {:?}, catalog schema: {:?}",
                view_catalog.name, view_catalog.sql, query.schema(), columns
            )).into());
        }

        let share_id = match self.shared_views.get(&view_catalog.id) {
            Some(share_id) => *share_id,
            None => {
                let share_id = self.next_share_id();
                self.shared_views.insert(view_catalog.id, share_id);
                self.included_relations.insert(view_catalog.id.into());
                share_id
            }
        };
        let input = Either::Left(query);
        Ok((
            Relation::Share(Box::new(BoundShare {
                share_id,
                input: BoundShareInput::Query(input),
            })),
            columns.iter().map(|c| (false, c.clone())).collect_vec(),
        ))
    }

    fn resolve_table_indexes(
        &self,
        schema_name: &str,
        table_id: TableId,
    ) -> Result<Vec<Arc<IndexCatalog>>> {
        Ok(self
            .catalog
            .get_schema_by_name(&self.db_name, schema_name)?
            .get_indexes_by_table_id(&table_id))
    }

    pub(crate) fn bind_table(
        &mut self,
        schema_name: Option<&str>,
        table_name: &str,
        alias: Option<TableAlias>,
    ) -> Result<BoundBaseTable> {
        let db_name = &self.db_name;
        let schema_path = self.bind_schema_path(schema_name);
        let (table_catalog, schema_name) =
            self.catalog
                .get_created_table_by_name(db_name, schema_path, table_name)?;
        let table_catalog = table_catalog.clone();

        let table_id = table_catalog.id();
        let table_indexes = self.resolve_table_indexes(schema_name, table_id)?;

        let columns = table_catalog.columns.clone();

        self.bind_table_to_context(
            columns
                .iter()
                .map(|c| (c.is_hidden, (&c.column_desc).into())),
            table_name.to_owned(),
            alias,
        )?;

        Ok(BoundBaseTable {
            table_id,
            table_catalog,
            table_indexes,
            as_of: None,
        })
    }

    pub(crate) fn resolve_dml_table<'a>(
        &'a self,
        schema_name: Option<&str>,
        table_name: &str,
        is_insert: bool,
    ) -> Result<&'a TableCatalog> {
        let db_name = &self.db_name;
        let schema_path = self.bind_schema_path(schema_name);

        let (table, _schema_name) =
            self.catalog
                .get_created_table_by_name(db_name, schema_path, table_name)?;

        match table.table_type() {
            TableType::Table => {}
            TableType::Index => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot change index \"{table_name}\""
                ))
                .into())
            }
            TableType::MaterializedView => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot change materialized view \"{table_name}\""
                ))
                .into())
            }
            TableType::Internal => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "cannot change internal table \"{table_name}\""
                ))
                .into())
            }
        }

        if table.append_only && !is_insert {
            return Err(ErrorCode::BindError(
                "append-only table does not support update or delete".to_owned(),
            )
            .into());
        }

        Ok(table)
    }
}
