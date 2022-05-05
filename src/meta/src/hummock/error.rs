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

use risingwave_common::error::{ErrorCode, RwError, ToErrorStr};
use risingwave_hummock_sdk::HummockContextId;
use thiserror::Error;

use crate::storage::meta_store;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid hummock context {0}")]
    InvalidContext(HummockContextId),
    #[error(transparent)]
    MetaStoreError(anyhow::Error),
    #[error("internal error: {0}")]
    InternalError(String),
}

impl Error {
    pub fn retryable(&self) -> bool {
        match self {
            Error::InvalidContext(_) => false,
            Error::MetaStoreError(_) => true,
            Error::InternalError(_) => false,
        }
    }
}

impl From<meta_store::Error> for Error {
    fn from(error: meta_store::Error) -> Self {
        match error {
            meta_store::Error::ItemNotFound(err) => Error::InternalError(err),
            meta_store::Error::TransactionAbort() => {
                // TODO: need more concrete error from meta store.
                Error::InvalidContext(0)
            }
            meta_store::Error::Internal(err) => Error::MetaStoreError(err),
        }
    }
}

impl From<Error> for ErrorCode {
    fn from(error: Error) -> Self {
        match error {
            Error::InvalidContext(err) => {
                ErrorCode::InternalError(format!("invalid hummock context {}", err))
            }
            Error::MetaStoreError(err) => ErrorCode::MetaError(err.to_error_str()),
            Error::InternalError(err) => ErrorCode::InternalError(err),
        }
    }
}

impl From<Error> for risingwave_common::error::RwError {
    fn from(error: Error) -> Self {
        ErrorCode::from(error).into()
    }
}

// TODO: as a workaround before refactoring `MetadataModel` error
impl From<risingwave_common::error::RwError> for Error {
    fn from(error: RwError) -> Self {
        match error.inner() {
            ErrorCode::InternalError(err) => Error::InternalError(err.to_owned()),
            ErrorCode::ItemNotFound(err) => Error::InternalError(err.to_owned()),
            _ => {
                panic!("convertion not supported");
            }
        }
    }
}
