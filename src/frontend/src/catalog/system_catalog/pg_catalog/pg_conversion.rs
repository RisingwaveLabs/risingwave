// Copyright 2023 RisingWave Labs
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

use risingwave_common::types::DataType;

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

/// The catalog `pg_conversion` describes encoding conversion functions.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-conversion.html`]
pub const PG_CONVERSION_TABLE_NAME: &str = "pg_conversion";
pub const PG_CONVERSION_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Varchar, "conname"),
    (DataType::Int32, "connamespace"),
    (DataType::Int32, "conowner"),
    (DataType::Int16, "conforencoding"),
    (DataType::Int16, "contoencoding"),
    (DataType::Int32, "conproc"),
    (DataType::Boolean, "condefault"),
];
