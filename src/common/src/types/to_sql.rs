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

use std::error::Error;

use bytes::BytesMut;
use postgres_types::{to_sql_checked, IsNull, ToSql, Type};
use risingwave_common::types::ScalarRefImpl;

use crate::types::ScalarImpl;

impl ToSql for ScalarImpl {
    to_sql_checked!();

    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            ScalarImpl::Int16(v) => v.to_sql(ty, out),
            ScalarImpl::Int32(v) => v.to_sql(ty, out),
            ScalarImpl::Int64(v) => v.to_sql(ty, out),
            ScalarImpl::Serial(v) => v.to_sql(ty, out),
            ScalarImpl::Float32(v) => v.to_sql(ty, out),
            ScalarImpl::Float64(v) => v.to_sql(ty, out),
            ScalarImpl::Utf8(v) => v.to_sql(ty, out),
            ScalarImpl::Bool(v) => v.to_sql(ty, out),
            ScalarImpl::Decimal(v) => v.to_sql(ty, out),
            ScalarImpl::Interval(v) => v.to_sql(ty, out),
            ScalarImpl::Date(v) => v.to_sql(ty, out),
            ScalarImpl::Timestamp(v) => v.to_sql(ty, out),
            ScalarImpl::Timestamptz(v) => v.to_sql(ty, out),
            ScalarImpl::Time(v) => v.to_sql(ty, out),
            ScalarImpl::Bytea(v) => (&**v).to_sql(ty, out),
            ScalarImpl::Jsonb(v) => v.to_sql(ty, out),
            ScalarImpl::Int256(_)
            | ScalarImpl::Struct(_)
            | ScalarImpl::List(_)
            | ScalarImpl::TimestampNanosecond(_) => {
                bail_not_implemented!("the postgres encoding for {ty} is unsupported")
            }
            ScalarImpl::Map(_) => todo!(),
        }
    }

    // return true to accept all types
    fn accepts(_ty: &Type) -> bool
    where
        Self: Sized,
    {
        true
    }
}

impl ToSql for ScalarRefImpl<'_> {
    to_sql_checked!();

    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            ScalarRefImpl::Int16(v) => v.to_sql(ty, out),
            ScalarRefImpl::Int32(v) => v.to_sql(ty, out),
            ScalarRefImpl::Int64(v) => v.to_sql(ty, out),
            ScalarRefImpl::Serial(v) => v.to_sql(ty, out),
            ScalarRefImpl::Float32(v) => v.to_sql(ty, out),
            ScalarRefImpl::Float64(v) => v.to_sql(ty, out),
            ScalarRefImpl::Utf8(v) => v.to_sql(ty, out),
            ScalarRefImpl::Bool(v) => v.to_sql(ty, out),
            ScalarRefImpl::Decimal(v) => v.to_sql(ty, out),
            ScalarRefImpl::Interval(v) => v.to_sql(ty, out),
            ScalarRefImpl::Date(v) => v.to_sql(ty, out),
            ScalarRefImpl::Timestamp(v) => v.to_sql(ty, out),
            ScalarRefImpl::Timestamptz(v) => v.to_sql(ty, out),
            ScalarRefImpl::Time(v) => v.to_sql(ty, out),
            ScalarRefImpl::Bytea(v) => (&**v).to_sql(ty, out),
            ScalarRefImpl::Jsonb(v) => v.to_sql(ty, out),
            ScalarRefImpl::Int256(_)
            | ScalarRefImpl::Struct(_)
            | ScalarRefImpl::List(_)
            | ScalarRefImpl::TimestampNanosecond(_) => {
                bail_not_implemented!("the postgres encoding for {ty} is unsupported")
            }
            ScalarRefImpl::Map(_) => todo!(),
        }
    }

    // return true to accept all types
    fn accepts(_ty: &Type) -> bool
    where
        Self: Sized,
    {
        true
    }
}
