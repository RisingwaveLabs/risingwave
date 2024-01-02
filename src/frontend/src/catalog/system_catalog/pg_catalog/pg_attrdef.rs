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

use std::sync::LazyLock;

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::{infer_dummy_view_sql, BuiltinView, SystemCatalogColumnsDef};

pub const PG_ATTRDEF_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Int32, "adrelid"),
    (DataType::Int16, "adnum"),
    // The column default value, use pg_get_expr(adbin, adrelid) to convert it to an SQL
    // expression. We don't have `pg_node_tree` type yet, so we use `text` instead.
    (DataType::Varchar, "adbin"),
];

/// The catalog `pg_attrdef` stores column default values. The main information about columns is
/// stored in `pg_attribute`. Only columns for which a default value has been explicitly set will
/// have an entry here. Ref: [`https://www.postgresql.org/docs/current/catalog-pg-attrdef.html`]
pub static PG_ATTRDEF: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_attrdef",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: PG_ATTRDEF_COLUMNS,
    sql: infer_dummy_view_sql(PG_ATTRDEF_COLUMNS),
});
