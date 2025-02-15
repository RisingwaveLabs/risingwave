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

use std::sync::LazyLock;

use anyhow::anyhow;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{Binary, Bson, DateTime, Document};
use risingwave_common::array::RowRef;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef, JsonbVal, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqDebug;
use thiserror_ext::AsReport;

use super::{Result as SinkResult, RowEncoder, SerTo};
use crate::sink::SinkError;

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);

pub struct BsonEncoder {
    schema: Schema,
    col_indices: Option<Vec<usize>>,
    pk_indices: Vec<usize>,
}

impl BsonEncoder {
    pub fn new(schema: Schema, col_indices: Option<Vec<usize>>, pk_indices: Vec<usize>) -> Self {
        Self {
            schema,
            col_indices,
            pk_indices,
        }
    }

    pub fn construct_pk(&self, row: RowRef<'_>) -> Bson {
        if self.pk_indices.len() == 1 {
            let pk_field = &self.schema.fields[self.pk_indices[0]];
            let pk_datum = row.datum_at(self.pk_indices[0]);
            datum_to_bson(pk_field, pk_datum)
        } else {
            self.pk_indices
                .iter()
                .map(|&idx| {
                    let pk_field = &self.schema.fields[idx];
                    (
                        pk_field.name.clone(),
                        datum_to_bson(pk_field, row.datum_at(idx)),
                    )
                })
                .collect::<Document>()
                .into()
        }
    }
}

impl SerTo<Vec<u8>> for Document {
    fn ser_to(self) -> SinkResult<Vec<u8>> {
        mongodb::bson::to_vec(&self).map_err(|err| {
            SinkError::Mongodb(anyhow!(err).context("cannot serialize Document to Vec<u8>"))
        })
    }
}

impl RowEncoder for BsonEncoder {
    type Output = Document;

    fn encode_cols(
        &self,
        row: impl Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> SinkResult<Self::Output> {
        Ok(col_indices
            .map(|idx| (&self.schema.fields[idx], row.datum_at(idx)))
            .map(|(field, datum)| (field.name.clone(), datum_to_bson(field, datum)))
            .collect())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        self.col_indices.as_ref().map(Vec::as_ref)
    }
}

/// We support converting all types to `MongoDB`. If there is an unmatched type, it will be
/// converted to its string representation. If there is a conversion error, a warning log is printed
/// and a `Bson::Null` is returned
fn datum_to_bson(field: &Field, datum: DatumRef<'_>) -> Bson {
    let scalar_ref = match datum {
        None => {
            return Bson::Null;
        }
        Some(datum) => datum,
    };

    let data_type = field.data_type();

    match (data_type, scalar_ref) {
        (DataType::Int16, ScalarRefImpl::Int16(v)) => Bson::Int32(v as i32),
        (DataType::Int32, ScalarRefImpl::Int32(v)) => Bson::Int32(v),
        (DataType::Int64, ScalarRefImpl::Int64(v)) => Bson::Int64(v),
        (DataType::Int256, ScalarRefImpl::Int256(v)) => Bson::String(v.to_string()),
        (DataType::Float32, ScalarRefImpl::Float32(v)) => Bson::Double(v.into_inner() as f64),
        (DataType::Float64, ScalarRefImpl::Float64(v)) => Bson::Double(v.into_inner()),
        (DataType::Varchar, ScalarRefImpl::Utf8(v)) => Bson::String(v.to_owned()),
        (DataType::Boolean, ScalarRefImpl::Bool(v)) => Bson::Boolean(v),
        (DataType::Decimal, ScalarRefImpl::Decimal(v)) => {
            let decimal_str = v.to_string();
            let converted = decimal_str.parse();
            match converted {
                Ok(v) => Bson::Decimal128(v),
                Err(err) => {
                    if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                        tracing::warn!(
                            suppressed_count,
                            error = %err.as_report(),
                            ?field,
                            "risingwave decimal {} convert to bson decimal128 failed",
                            decimal_str,
                        );
                    }
                    Bson::Null
                }
            }
        }
        (DataType::Interval, ScalarRefImpl::Interval(v)) => Bson::String(v.to_string()),
        (DataType::Date, ScalarRefImpl::Date(v)) => Bson::String(v.to_string()),
        (DataType::Time, ScalarRefImpl::Time(v)) => Bson::String(v.to_string()),
        (DataType::Timestamp, ScalarRefImpl::Timestamp(v)) => {
            Bson::DateTime(DateTime::from_millis(v.0.and_utc().timestamp_millis()))
        }
        (DataType::TimestampNanosecond, ScalarRefImpl::TimestampNanosecond(v)) => {
            Bson::DateTime(DateTime::from_millis(v.0.and_utc().timestamp_millis()))
        }
        (DataType::Timestamptz, ScalarRefImpl::Timestamptz(v)) => {
            Bson::DateTime(DateTime::from_millis(v.timestamp_millis()))
        }
        (DataType::Jsonb, ScalarRefImpl::Jsonb(v)) => {
            let jsonb_val: JsonbVal = v.into();
            match jsonb_val.take().try_into() {
                Ok(doc) => doc,
                Err(err) => {
                    if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                        tracing::warn!(
                            suppressed_count,
                            error = %err.as_report(),
                            ?field,
                            "convert jsonb to mongodb bson failed",
                        );
                    }
                    Bson::Null
                }
            }
        }
        (DataType::Serial, ScalarRefImpl::Serial(v)) => Bson::Int64(v.into_inner()),
        (DataType::Struct(st), ScalarRefImpl::Struct(struct_ref)) => {
            let mut doc = Document::new();
            for (sub_datum_ref, sub_field) in struct_ref.iter_fields_ref().zip_eq_debug(
                st.iter()
                    .map(|(name, dt)| Field::with_name(dt.clone(), name)),
            ) {
                doc.insert(
                    sub_field.name.clone(),
                    datum_to_bson(&sub_field, sub_datum_ref),
                );
            }
            Bson::Document(doc)
        }
        (DataType::List(dt), ScalarRefImpl::List(v)) => {
            let inner_field = Field::unnamed(Box::<DataType>::into_inner(dt));
            v.iter()
                .map(|scalar_ref| datum_to_bson(&inner_field, scalar_ref))
                .collect::<Bson>()
        }
        (DataType::Bytea, ScalarRefImpl::Bytea(v)) => Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: v.into(),
        }),
        // TODO(map): support map
        _ => {
            if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
                tracing::warn!(
                    suppressed_count,
                    ?field,
                    ?scalar_ref,
                    "datum_to_bson: unsupported data type"
                );
            }
            Bson::Null
        }
    }
}
