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

use std::error::Error;
use std::sync::Arc;

#[easy_ext::ext(ToTonicStatus)]
impl<T> T
where
    T: ?Sized + std::error::Error,
{
    /// Convert the error to [`tonic::Status`] with the given [`tonic::Code`].
    ///
    /// The source chain is preserved by pairing with [`TonicStatusWrapper`].
    // TODO(error-handling): disallow constructing `tonic::Status` directly with `new` by clippy.
    pub fn to_status(&self, code: tonic::Code) -> tonic::Status {
        // Embed the whole error (`self`) and its source chain into the details field.
        // At the same time, set the message field to the error message of `self` (without source chain).
        // The redundancy of the current error's message is intentional in case the client ignores the `details` field.
        let source = serde_error::Error::new(self);
        let details = bincode::serialize(&source).unwrap_or_default();

        let mut status = tonic::Status::with_details(code, self.to_string(), details.into());
        // Set the source of `tonic::Status`, though it's not likely to be used.
        // This is only available before serializing to the wire. That's why we need to manually embed it
        // into the `details` field.
        status.set_source(Arc::new(source));
        status
    }
}

/// A wrapper of [`tonic::Status`] that provides better error message and extracts
/// the source chain from the `details` field.
#[derive(Debug)]
pub struct TonicStatusWrapper(tonic::Status);

impl TonicStatusWrapper {
    /// Create a new [`TonicStatusWrapper`] from the given [`tonic::Status`] and extract
    /// the source chain from its `details` field.
    pub fn new(mut status: tonic::Status) -> Self {
        if status.source().is_none() {
            if let Ok(e) = bincode::deserialize::<serde_error::Error>(status.details()) {
                status.set_source(Arc::new(e));
            }
        }
        Self(status)
    }

    /// Returns the reference to the inner [`tonic::Status`].
    pub fn inner(&self) -> &tonic::Status {
        &self.0
    }

    /// Consumes `self` and returns the inner [`tonic::Status`].
    pub fn into_inner(self) -> tonic::Status {
        self.0
    }
}

impl From<tonic::Status> for TonicStatusWrapper {
    fn from(status: tonic::Status) -> Self {
        Self::new(status)
    }
}

impl std::fmt::Display for TonicStatusWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gRPC error ({}): {}", self.0.code(), self.0.message())
    }
}

impl std::error::Error for TonicStatusWrapper {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        // Delegate to `self.0` as if we're transparent.
        self.0.source()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ui() {
        #[derive(thiserror::Error, Debug)]
        #[error("{message}")]
        struct MyError {
            message: &'static str,
            source: Option<Box<MyError>>,
        }

        let original = MyError {
            message: "outer",
            source: Some(Box::new(MyError {
                message: "inner",
                source: None,
            })),
        };

        let server_status = original.to_status(tonic::Code::Internal);
        let body = server_status.to_http();
        let client_status = tonic::Status::from_header_map(body.headers()).unwrap();

        let wrapper = TonicStatusWrapper::new(client_status);
        assert_eq!(wrapper.to_string(), "gRPC error (Internal error): outer");

        let source = wrapper.source().unwrap();
        assert!(source.is::<serde_error::Error>());
        assert_eq!(source.to_string(), "outer");
        assert_eq!(source.source().unwrap().to_string(), "inner");
    }
}
