#![allow(dead_code)]
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode;
use thiserror::Error;

pub(crate) mod catalog_service;
pub(crate) mod column_catalog;
pub(crate) mod database_catalog;
mod schema_catalog;
pub(crate) mod table_catalog;

pub(crate) type DatabaseId = u64;
pub(crate) type SchemaId = u64;
pub(crate) type TableId = risingwave_common::catalog::TableId;
pub(crate) type ColumnId = u64;

pub use catalog_service::CatalogConnector;

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),
    #[error("duplicated {0}: {1}")]
    Duplicated(&'static str, String),
}

impl From<CatalogError> for RwError {
    fn from(e: CatalogError) -> Self {
        ErrorCode::CatalogError(Box::new(e)).into()
    }
}
