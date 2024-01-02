// Copyright 2024 RisingWave Labs
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
use postgres_types::{accepts, to_sql_checked, IsNull, ToSql, Type};

use crate::types::{JsonbRef, ScalarRefImpl};

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
            ScalarRefImpl::Bytea(v) => v.to_sql(ty, out),
            ScalarRefImpl::Jsonb(_) // jsonbb::Value doesn't implement ToSql yet
            | ScalarRefImpl::Int256(_)
            | ScalarRefImpl::Struct(_)
            | ScalarRefImpl::List(_) => {
                bail_not_implemented!("the postgres encoding for {ty} is unsupported")
            }
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

impl ToSql for JsonbRef<'_> {
    accepts!(JSONB);

    to_sql_checked!();

    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>>
    where
        Self: Sized,
    {
        let buf = self.value_serialize();
        out.extend(buf);
        Ok(IsNull::No)
    }
}
