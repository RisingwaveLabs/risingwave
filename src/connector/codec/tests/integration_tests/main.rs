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

//! Data driven testing for converting Avro/Protobuf Schema to RisingWave Schema, and then converting Avro/Protobuf data into RisingWave data.
//!
//! The expected results can be automatically updated. To run and update the tests:
//! ```bash
//! UPDATE_EXPECT=1 cargo test -p risingwave_connector_codec
//! ```
//! Or use Rust Analyzer. Refer to <https://github.com/rust-analyzer/expect-test>.
//!
//! ## Why not directly test the uppermost layer `AvroParserConfig` and `AvroAccessBuilder`?
//!
//! Because their interface are not clean enough, and have complex logic like schema registry.
//! We might need to separate logic to make them cleaner and then we can use it directly for testing.
//!
//! ## If we reimplement a similar logic here, what are we testing?
//!
//! Basically unit tests of `avro_schema_to_column_descs`, `convert_to_datum`, i.e., the type mapping.
//!
//! It makes some sense, as the data parsing logic is generally quite simple (one-liner), and the most
//! complex and error-prone part is the type mapping.
//!
//! ## Why test schema mapping and data mapping together?
//!
//! Because the expected data type for data mapping comes from the schema mapping.

mod avro;
mod json;
mod protobuf;

pub mod utils;
