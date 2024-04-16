// Copyright 2024 RisingWave Labs
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

//! This is for arrow dependency named `arrow-xxx` such as `arrow-array` in the cargo workspace.
//!
//! This should the default arrow version to be used in our system.
//!
//! The corresponding version of arrow is currently used by `udf` and `iceberg` sink.

#![allow(unused_imports)]
pub use arrow_impl::{
    to_record_batch_with_schema, ToArrowArrayConvert, ToArrowArrayWithTypeConvert,
    ToArrowTypeConvert,
};
use {arrow_array, arrow_buffer, arrow_cast, arrow_schema};

#[expect(clippy::duplicate_mod)]
#[path = "./arrow_impl.rs"]
mod arrow_impl;
