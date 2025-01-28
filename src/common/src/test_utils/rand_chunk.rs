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

use crate::array::{
    BoolArray, DataChunk, DateArray, DecimalArray, F32Array, F64Array, I16Array, I32Array,
    I64Array, Int256Array, IntervalArray, SerialArray, TimeArray, TimestampArray,
    TimestampNanosecondArray, TimestamptzArray, Utf8Array,
};
use crate::test_utils::rand_array::seed_rand_array_ref;
use crate::types::DataType;
use crate::util::schema_check;

pub fn gen_chunk(data_types: &[DataType], size: usize, seed: u64, null_ratio: f64) -> DataChunk {
    let mut columns = vec![];

    for d in data_types {
        columns.push(match d {
            DataType::Boolean => seed_rand_array_ref::<BoolArray>(size, seed, null_ratio),
            DataType::Int16 => seed_rand_array_ref::<I16Array>(size, seed, null_ratio),
            DataType::Int32 => seed_rand_array_ref::<I32Array>(size, seed, null_ratio),
            DataType::Int64 => seed_rand_array_ref::<I64Array>(size, seed, null_ratio),
            DataType::Float32 => seed_rand_array_ref::<F32Array>(size, seed, null_ratio),
            DataType::Float64 => seed_rand_array_ref::<F64Array>(size, seed, null_ratio),
            DataType::Decimal => seed_rand_array_ref::<DecimalArray>(size, seed, null_ratio),
            DataType::Date => seed_rand_array_ref::<DateArray>(size, seed, null_ratio),
            DataType::Varchar => seed_rand_array_ref::<Utf8Array>(size, seed, null_ratio),
            DataType::Time => seed_rand_array_ref::<TimeArray>(size, seed, null_ratio),
            DataType::Serial => seed_rand_array_ref::<SerialArray>(size, seed, null_ratio),
            DataType::Timestamp => seed_rand_array_ref::<TimestampArray>(size, seed, null_ratio),
            DataType::TimestampNanosecond => {
                seed_rand_array_ref::<TimestampNanosecondArray>(size, seed, null_ratio)
            }
            DataType::Timestamptz => {
                seed_rand_array_ref::<TimestamptzArray>(size, seed, null_ratio)
            }
            DataType::Interval => seed_rand_array_ref::<IntervalArray>(size, seed, null_ratio),
            DataType::Int256 => seed_rand_array_ref::<Int256Array>(size, seed, null_ratio),
            DataType::Struct(_)
            | DataType::Bytea
            | DataType::Jsonb
            | DataType::List(_)
            | DataType::Map(_) => {
                todo!()
            }
        });
    }
    schema_check::schema_check(data_types, &columns).unwrap();
    DataChunk::new(columns, size)
}
