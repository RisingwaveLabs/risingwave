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

use crate::CtlContext;

pub async fn pause(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    meta_client.pause().await?;

    println!("Done.");

    Ok(())
}

pub async fn resume(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;

    meta_client.resume().await?;

    println!("Done.");

    Ok(())
}
