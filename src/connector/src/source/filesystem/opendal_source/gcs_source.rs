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

use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Gcs;
use opendal::Operator;
use serde::Deserialize;

use super::opendal_enumerator::{EngineType, OpenDALSplitEnumerator};

impl OpenDALSplitEnumerator {
    /// create opendal gcs engine.
    pub fn new_gcs_source(bucket: String) -> anyhow::Result<Self> {
        // Create gcs backend builder.
        let mut builder = Gcs::default();

        builder.bucket(&bucket);

        // if credential env is set, use it. Otherwise, ADC will be used.
        let cred = std::env::var("GOOGLE_APPLICATION_CREDENTIALS");
        if let Ok(cred) = cred {
            builder.credential(&cred);
        }
        let op: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(Self {
            op,
            engine_type: EngineType::Gcs,
        })
    }
}

use crate::source::filesystem::FsSplit;
use crate::source::SourceProperties;

pub const GCS_CONNECTOR: &str = "s3";

#[derive(Clone, Debug, Deserialize)]
pub struct GCSProperties {
    #[serde(rename = "gcs.bucket_name")]
    pub bucket_name: String,
}

impl SourceProperties for GCSProperties {
    type Split = FsSplit;
    type SplitEnumerator = OpenDALSplitEnumerator;
    type SplitReader = OpenDALSplitEnumerator;

    const SOURCE_NAME: &'static str = GCS_CONNECTOR;
}
