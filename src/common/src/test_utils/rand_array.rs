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

//! Contains helper methods for generating random arrays in tests.
//!
//! Use [`seed_rand_array`] to generate an random array.

use std::sync::Arc;

use chrono::Datelike;
use rand::distributions::Standard;
use rand::prelude::Distribution;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::array::{Array, ArrayBuilder, ArrayRef, ListValue, MapValue, StructValue};
use crate::types::{
    DataType, Date, Decimal, Int256, Interval, JsonbVal, MapType, NativeType, Scalar, Serial, Time,
    Timestamp, TimestampNanosecond, Timestamptz,
};

pub trait RandValue {
    fn rand_value<R: Rng>(rand: &mut R) -> Self;
}

impl<T> RandValue for T
where
    T: NativeType,
    Standard: Distribution<T>,
{
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        rand.gen()
    }
}

impl RandValue for Box<str> {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let len = rand.gen_range(1..=10);
        // `rand.gen:::<char>` create a random Unicode scalar value.
        (0..len)
            .map(|_| rand.gen::<char>())
            .collect::<String>()
            .into_boxed_str()
    }
}

impl RandValue for Box<[u8]> {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let len = rand.gen_range(1..=10);
        (0..len)
            .map(|_| rand.gen::<char>())
            .collect::<String>()
            .into_bytes()
            .into()
    }
}

impl RandValue for Decimal {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        Decimal::try_from((rand.gen::<u32>() as f64) + 0.1f64).unwrap()
    }
}

impl RandValue for Interval {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let months = rand.gen_range(0..100);
        let days = rand.gen_range(0..200);
        let usecs = rand.gen_range(0..100_000);
        Interval::from_month_day_usec(months, days, usecs)
    }
}

impl RandValue for Date {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let max_day = chrono::NaiveDate::MAX.num_days_from_ce();
        let min_day = chrono::NaiveDate::MIN.num_days_from_ce();
        let days = rand.gen_range(min_day..=max_day);
        Date::with_days_since_ce(days).unwrap()
    }
}

impl RandValue for Time {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let hour = rand.gen_range(0..24);
        let min = rand.gen_range(0..60);
        let sec = rand.gen_range(0..60);
        let nano = rand.gen_range(0..1_000_000_000);
        Time::from_hms_nano_uncheck(hour, min, sec, nano)
    }
}

impl RandValue for Timestamp {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        Timestamp::new(Date::rand_value(rand).0.and_time(Time::rand_value(rand).0))
    }
}

impl RandValue for TimestampNanosecond {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        TimestampNanosecond::new(Date::rand_value(rand).0.and_time(Time::rand_value(rand).0))
    }
}

impl RandValue for Timestamptz {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        Timestamptz::from_micros(rand.gen())
    }
}

impl RandValue for bool {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        rand.gen::<bool>()
    }
}

impl RandValue for Serial {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        // TODO(peng), serial should be in format of RowId
        i64::rand_value(rand).into()
    }
}

impl RandValue for Int256 {
    fn rand_value<R: Rng>(rand: &mut R) -> Self {
        let mut bytes = [0u8; 32];
        rand.fill_bytes(&mut bytes);
        Int256::from_ne_bytes(bytes)
    }
}

impl RandValue for JsonbVal {
    fn rand_value<R: rand::Rng>(_rand: &mut R) -> Self {
        JsonbVal::null()
    }
}

impl RandValue for StructValue {
    fn rand_value<R: rand::Rng>(_rand: &mut R) -> Self {
        StructValue::new(vec![])
    }
}

impl RandValue for ListValue {
    fn rand_value<R: rand::Rng>(rand: &mut R) -> Self {
        ListValue::from_iter([rand.gen::<i16>()])
    }
}

impl RandValue for MapValue {
    fn rand_value<R: Rng>(_rand: &mut R) -> Self {
        // dummy value
        MapValue::from_entries(ListValue::empty(&DataType::Struct(
            MapType::struct_type_for_map(DataType::Varchar, DataType::Varchar),
        )))
    }
}

pub fn rand_array<A, R>(rand: &mut R, size: usize, null_ratio: f64) -> A
where
    A: Array,
    R: Rng,
    A::OwnedItem: RandValue,
{
    let mut builder = A::Builder::new(size);
    for _ in 0..size {
        let is_null = rand.gen_bool(null_ratio);
        if is_null {
            builder.append_null();
        } else {
            let value = A::OwnedItem::rand_value(rand);
            builder.append(Some(value.as_scalar_ref()));
        }
    }

    builder.finish()
}

pub fn seed_rand_array<A>(size: usize, seed: u64, null_ratio: f64) -> A
where
    A: Array,
    A::OwnedItem: RandValue,
{
    let mut rand = SmallRng::seed_from_u64(seed);
    rand_array(&mut rand, size, null_ratio)
}

pub fn seed_rand_array_ref<A>(size: usize, seed: u64, null_ratio: f64) -> ArrayRef
where
    A: Array,
    A::OwnedItem: RandValue,
{
    let array: A = seed_rand_array(size, seed, null_ratio);
    Arc::new(array.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::for_all_variants;

    #[test]
    fn test_create_array() {
        macro_rules! gen_rand_array {
            ($( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
            $(
                {
                    let array = seed_rand_array::<$array>(10, 1024, 0.5);
                    assert_eq!(10, array.len());
                }
            )*
        };
    }

        for_all_variants! { gen_rand_array }
    }
}
