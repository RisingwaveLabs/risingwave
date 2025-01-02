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

use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, ToText};

use super::RowEncoder;

/// Encode with [`ToText`]. Only used to encode key.
pub struct TextEncoder {
    pub schema: Schema,
    // the column must contain only one element
    pub col_index: usize,
}

impl TextEncoder {
    pub fn new(schema: Schema, col_index: usize) -> Self {
        Self { schema, col_index }
    }
}

impl RowEncoder for TextEncoder {
    type Output = String;

    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn col_indices(&self) -> Option<&[usize]> {
        Some(std::slice::from_ref(&self.col_index))
    }

    fn encode_cols(
        &self,
        row: impl risingwave_common::row::Row,
        col_indices: impl Iterator<Item = usize>,
    ) -> crate::sink::Result<Self::Output> {
        // It is guaranteed by the caller that col_indices contains only one element
        let mut result = String::new();
        for col_index in col_indices {
            let datum = row.datum_at(col_index);
            let data_type = &self.schema.fields[col_index].data_type;
            if data_type == &DataType::Boolean {
                result = if let Some(scalar_impl) = datum {
                    scalar_impl.into_bool().to_string()
                } else {
                    "NULL".to_owned()
                }
            } else {
                result = datum.to_text_with_type(data_type);
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::catalog::Field;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl;

    use super::*;

    #[test]
    fn test_text_encoder_ser_bool() {
        let schema = Schema::new(vec![Field::with_name(DataType::Boolean, "col1")]);
        let encoder = TextEncoder::new(schema, 0);

        let row = OwnedRow::new(vec![Some(ScalarImpl::Bool(true))]);
        assert_eq!(
            encoder
                .encode_cols(&row, std::iter::once(0))
                .unwrap()
                .as_str(),
            "true"
        );

        let row = OwnedRow::new(vec![Some(ScalarImpl::Bool(false))]);
        assert_eq!(
            encoder
                .encode_cols(&row, std::iter::once(0))
                .unwrap()
                .as_str(),
            "false"
        );

        let row = OwnedRow::new(vec![None]);
        assert_eq!(
            encoder
                .encode_cols(&row, std::iter::once(0))
                .unwrap()
                .as_str(),
            "NULL"
        );
    }
}
