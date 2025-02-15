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

use std::marker::PhantomData;

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use opendal::{Metakey, Operator};
use risingwave_common::types::Timestamptz;

use super::OpendalSource;
use crate::error::ConnectorResult;
use crate::source::filesystem::file_common::CompressionFormat;
use crate::source::filesystem::{FsPageItem, OpendalFsSplit};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

#[derive(Debug, Clone)]
pub struct OpendalEnumerator<Src: OpendalSource> {
    pub op: Operator,
    // prefix is used to reduce the number of objects to be listed
    pub(crate) prefix: Option<String>,
    pub(crate) matcher: Option<glob::Pattern>,
    pub(crate) marker: PhantomData<Src>,
    pub(crate) compression_format: CompressionFormat,
}

#[async_trait]
impl<Src: OpendalSource> SplitEnumerator for OpendalEnumerator<Src> {
    type Properties = Src::Properties;
    type Split = OpendalFsSplit<Src>;

    async fn new(
        properties: Src::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<Self> {
        Src::new_enumerator(properties)
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<OpendalFsSplit<Src>>> {
        let empty_split: OpendalFsSplit<Src> = OpendalFsSplit::empty_split();
        let prefix = self.prefix.as_deref().unwrap_or("/");

        let mut lister = self.op.lister(prefix).await?;
        // fetch one item as validation, no need to get all
        match lister.try_next().await {
            Ok(_) => return Ok(vec![empty_split]),
            Err(e) => {
                return Err(anyhow!(e)
                    .context("fail to create source, please check your config.")
                    .into())
            }
        }
    }
}

impl<Src: OpendalSource> OpendalEnumerator<Src> {
    pub async fn list(&self) -> ConnectorResult<ObjectMetadataIter> {
        let prefix = self.prefix.as_deref().unwrap_or("/");
        let list_prefix = match prefix.ends_with("/") {
            true => prefix,
            false => "/",
        };
        let object_lister = self
            .op
            .lister_with(list_prefix)
            .recursive(true)
            .metakey(Metakey::ContentLength | Metakey::LastModified)
            .await?;
        let stream = stream::unfold(object_lister, |mut object_lister| async move {
            match object_lister.next().await {
                Some(Ok(object)) => {
                    let name = object.path().to_owned();
                    let om = object.metadata();

                    let t = match om.last_modified() {
                        Some(t) => t,
                        None => DateTime::<Utc>::from_timestamp(0, 0).unwrap_or_default(),
                    };
                    let timestamp = Timestamptz::from(t);
                    let size = om.content_length() as i64;
                    let metadata = FsPageItem {
                        name,
                        size,
                        timestamp,
                    };
                    Some((Ok(metadata), object_lister))
                }
                Some(Err(err)) => Some((Err(err.into()), object_lister)),
                None => {
                    tracing::info!("list object completed.");
                    None
                }
            }
        });

        Ok(stream.boxed())
    }

    pub fn get_matcher(&self) -> &Option<glob::Pattern> {
        &self.matcher
    }

    pub fn get_prefix(&self) -> &str {
        self.prefix.as_deref().unwrap_or("/")
    }
}
pub type ObjectMetadataIter = BoxStream<'static, ConnectorResult<FsPageItem>>;
