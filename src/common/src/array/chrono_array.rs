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

use super::{PrimitiveArray, PrimitiveArrayBuilder, TimestampNanosecond};
use crate::types::{Date, Time, Timestamp, Timestamptz};

pub type DateArray = PrimitiveArray<Date>;
pub type TimeArray = PrimitiveArray<Time>;
pub type TimestampArray = PrimitiveArray<Timestamp>;
pub type TimestampNanosecondArray = PrimitiveArray<TimestampNanosecond>;
pub type TimestamptzArray = PrimitiveArray<Timestamptz>;

pub type DateArrayBuilder = PrimitiveArrayBuilder<Date>;
pub type TimeArrayBuilder = PrimitiveArrayBuilder<Time>;
pub type TimestampArrayBuilder = PrimitiveArrayBuilder<Timestamp>;
pub type TimestampNanosecondArrayBuilder = PrimitiveArrayBuilder<TimestampNanosecond>;
pub type TimestamptzArrayBuilder = PrimitiveArrayBuilder<Timestamptz>;

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::array::{Array, ArrayBuilder};
    use crate::util::iter_util::ZipEqFast;

    #[test]
    fn test_date_builder() {
        let v = (0..1000)
            .map(Date::with_days_since_ce)
            .map(|x| x.ok())
            .collect_vec();
        let mut builder = DateArrayBuilder::new(0);
        for i in &v {
            builder.append(*i);
        }
        let a = builder.finish();
        let res = v.iter().zip_eq_fast(a.iter()).all(|(a, b)| *a == b);
        assert!(res)
    }

    #[test]
    fn test_date_array_to_protobuf() {
        let input = vec![
            Date::with_days_since_ce(12345).ok(),
            None,
            Date::with_days_since_ce(67890).ok(),
        ];

        let array = DateArray::from_iter(&input);
        let buffers = array.to_protobuf().values;

        assert_eq!(buffers.len(), 1);

        let output_buffer = input.iter().fold(Vec::new(), |mut v, d| match d {
            Some(d) => {
                d.to_protobuf(&mut v).unwrap();
                v
            }
            None => v,
        });

        assert_eq!(buffers[0].get_body(), &output_buffer);
    }
}
