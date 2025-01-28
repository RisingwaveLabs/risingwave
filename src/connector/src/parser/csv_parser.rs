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

use risingwave_common::cast::str_to_bool;
use risingwave_common::types::{
    Date, Decimal, ScalarImpl, Time, Timestamp, TimestampNanosecond, Timestamptz,
};

use super::unified::{AccessError, AccessResult};
use super::{ByteStreamSourceParser, CsvProperties};
use crate::error::ConnectorResult;
use crate::only_parse_payload;
use crate::parser::{ParserFormat, SourceStreamChunkRowWriter};
use crate::source::{DataType, SourceColumnDesc, SourceContext, SourceContextRef};

macro_rules! parse {
    ($v:ident, $t:ty) => {
        $v.parse::<$t>().map_err(|_| AccessError::TypeError {
            expected: stringify!($t).to_owned(),
            got: "string".to_owned(),
            value: $v.to_string(),
        })
    };
}

/// Parser for CSV format
#[derive(Debug)]
pub struct CsvParser {
    rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
    headers: Option<Vec<String>>,
    delimiter: u8,
}

impl CsvParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        csv_props: CsvProperties,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        let CsvProperties {
            delimiter,
            has_header,
        } = csv_props;

        Ok(Self {
            rw_columns,
            delimiter,
            headers: if has_header { Some(Vec::new()) } else { None },
            source_ctx,
        })
    }

    fn read_row(&self, buf: &[u8]) -> ConnectorResult<Vec<String>> {
        let mut reader_builder = csv::ReaderBuilder::default();
        reader_builder.delimiter(self.delimiter).has_headers(false);
        let record = reader_builder
            .from_reader(buf)
            .records()
            .next()
            .transpose()?;
        Ok(record
            .map(|record| record.iter().map(|field| field.to_owned()).collect())
            .unwrap_or_default())
    }

    #[inline]
    fn parse_string(dtype: &DataType, v: String) -> AccessResult {
        let v = match dtype {
            // mysql use tinyint to represent boolean
            DataType::Boolean => {
                str_to_bool(&v)
                    .map(ScalarImpl::Bool)
                    .map_err(|_| AccessError::TypeError {
                        expected: "boolean".to_owned(),
                        got: "string".to_owned(),
                        value: v,
                    })?
            }
            DataType::Int16 => parse!(v, i16)?.into(),
            DataType::Int32 => parse!(v, i32)?.into(),
            DataType::Int64 => parse!(v, i64)?.into(),
            DataType::Float32 => parse!(v, f32)?.into(),
            DataType::Float64 => parse!(v, f64)?.into(),
            // FIXME: decimal should have more precision than f64
            DataType::Decimal => parse!(v, Decimal)?.into(),
            DataType::Varchar => v.into(),
            DataType::Date => parse!(v, Date)?.into(),
            DataType::Time => parse!(v, Time)?.into(),
            DataType::Timestamp => parse!(v, Timestamp)?.into(),
            DataType::TimestampNanosecond => parse!(v, TimestampNanosecond)?.into(),
            DataType::Timestamptz => parse!(v, Timestamptz)?.into(),
            _ => {
                return Err(AccessError::UnsupportedType {
                    ty: dtype.to_string(),
                })
            }
        };
        Ok(Some(v))
    }

    #[allow(clippy::unused_async)]
    pub async fn parse_inner(
        &mut self,
        payload: Vec<u8>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<()> {
        let mut fields = self.read_row(&payload)?;

        if let Some(headers) = &mut self.headers {
            if headers.is_empty() {
                *headers = fields;
                // The header row does not output a row, so we return early.
                return Ok(());
            }
            writer.do_insert(|desc| {
                if let Some(i) = headers.iter().position(|name| name == &desc.name) {
                    let value = fields.get_mut(i).map(std::mem::take).unwrap_or_default();
                    if value.is_empty() {
                        return Ok(None);
                    }
                    Self::parse_string(&desc.data_type, value)
                } else {
                    Ok(None)
                }
            })?;
        } else {
            fields.reverse();
            writer.do_insert(|desc| {
                if let Some(value) = fields.pop() {
                    if value.is_empty() {
                        return Ok(None);
                    }
                    Self::parse_string(&desc.data_type, value)
                } else {
                    Ok(None)
                }
            })?;
        }

        Ok(())
    }
}

impl ByteStreamSourceParser for CsvParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::Csv
    }

    async fn parse_one<'a>(
        &'a mut self,
        _key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> ConnectorResult<()> {
        only_parse_payload!(self, payload, writer)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::Op;
    use risingwave_common::row::Row;
    use risingwave_common::types::{DataType, ToOwnedDatum};

    use super::*;
    use crate::parser::SourceStreamChunkBuilder;
    use crate::source::SourceCtrlOpts;

    #[tokio::test]
    async fn test_csv_without_headers() {
        let data = vec![
            r#"1,a,2"#,
            r#""15541","a,1,1,",4"#,
            r#"0,"""0",0"#,
            r#"0,0,0,0,0,0,0,0,0,0,0,0,0,"#,
            r#",,,,"#,
        ];
        let descs = vec![
            SourceColumnDesc::simple("a", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("b", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("c", DataType::Int32, 2.into()),
        ];
        let mut parser = CsvParser::new(
            Vec::new(),
            CsvProperties {
                delimiter: b',',
                has_header: false,
            },
            SourceContext::dummy().into(),
        )
        .unwrap();
        let mut builder = SourceStreamChunkBuilder::new(descs, SourceCtrlOpts::for_test());
        for item in data {
            parser
                .parse_inner(item.as_bytes().to_vec(), builder.row_writer())
                .await
                .unwrap();
        }
        builder.finish_current_chunk();
        let chunk = builder.consume_ready_chunks().next().unwrap();
        let mut rows = chunk.rows();
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("a".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(2)))
            );
        }
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(15541)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("a,1,1,".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(4)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("\"0".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("0".into())))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.datum_at(0), None);
            assert_eq!(row.datum_at(1), None);
            assert_eq!(row.datum_at(2), None);
        }
    }
    #[tokio::test]
    async fn test_csv_with_headers() {
        let data = [
            r#"c,b,a"#,
            r#"1,a,2"#,
            r#""15541","a,1,1,",4"#,
            r#"0,"""0",0"#,
            r#"0,0,0,0,0,0,0,0,0,0,0,0,0,"#,
        ];
        let descs = vec![
            SourceColumnDesc::simple("a", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("b", DataType::Varchar, 1.into()),
            SourceColumnDesc::simple("c", DataType::Int32, 2.into()),
        ];
        let mut parser = CsvParser::new(
            Vec::new(),
            CsvProperties {
                delimiter: b',',
                has_header: true,
            },
            SourceContext::dummy().into(),
        )
        .unwrap();
        let mut builder = SourceStreamChunkBuilder::new(descs, SourceCtrlOpts::for_test());
        for item in data {
            let _ = parser
                .parse_inner(item.as_bytes().to_vec(), builder.row_writer())
                .await;
        }
        builder.finish_current_chunk();
        let chunk = builder.consume_ready_chunks().next().unwrap();
        let mut rows = chunk.rows();
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("a".into())))
            );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(2)))
            );
        }
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(15541)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("a,1,1,".into())))
            );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(4)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("\"0".into())))
            );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Utf8("0".into())))
            );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(0)))
            );
        }
    }

    #[test]
    fn test_parse_boolean() {
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "1".to_owned()).unwrap(),
            Some(true.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "t".to_owned()).unwrap(),
            Some(true.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "T".to_owned()).unwrap(),
            Some(true.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "true".to_owned()).unwrap(),
            Some(true.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "TRUE".to_owned()).unwrap(),
            Some(true.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "True".to_owned()).unwrap(),
            Some(true.into())
        );

        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "0".to_owned()).unwrap(),
            Some(false.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "f".to_owned()).unwrap(),
            Some(false.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "F".to_owned()).unwrap(),
            Some(false.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "false".to_owned()).unwrap(),
            Some(false.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "FALSE".to_owned()).unwrap(),
            Some(false.into())
        );
        assert_eq!(
            CsvParser::parse_string(&DataType::Boolean, "False".to_owned()).unwrap(),
            Some(false.into())
        );

        assert!(CsvParser::parse_string(&DataType::Boolean, "2".to_owned()).is_err());
        assert!(CsvParser::parse_string(&DataType::Boolean, "t1".to_owned()).is_err());
        assert!(CsvParser::parse_string(&DataType::Boolean, "f1".to_owned()).is_err());
        assert!(CsvParser::parse_string(&DataType::Boolean, "false1".to_owned()).is_err());
        assert!(CsvParser::parse_string(&DataType::Boolean, "TRUE1".to_owned()).is_err());
    }
}
