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

use std::iter::TrustedLen;
use std::ops::Index;
use std::slice::Iter;

use bytes::Bytes;

use crate::error::{PsqlError, PsqlResult};

/// A row of data returned from the database by a query.
#[derive(Debug, Clone)]
// NOTE: Since we only support simple query protocol, the values are represented as strings.
pub struct Row(pub Vec<Option<Bytes>>);

impl Row {
    /// Create a row from values.
    pub fn new(row: Vec<Option<Bytes>>) -> Self {
        Self(row)
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the row contains no values. Required by clippy.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the values.
    pub fn values(&self) -> &[Option<Bytes>] {
        &self.0
    }

    pub fn take(self) -> Vec<Option<Bytes>> {
        self.0
    }

    pub fn project(&mut self, indices: &[usize]) -> Row {
        let mut new_row = Vec::with_capacity(indices.len());
        for i in indices {
            new_row.push(self.0[*i].take());
        }
        Row(new_row)
    }
}

impl Index<usize> for Row {
    type Output = Option<Bytes>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

/// <https://www.postgresql.org/docs/current/protocol-overview.html#PROTOCOL-FORMAT-CODES>
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Binary,
    Text,
}

impl Format {
    pub fn from_i16(format_code: i16) -> PsqlResult<Self> {
        match format_code {
            0 => Ok(Format::Text),
            1 => Ok(Format::Binary),
            _ => Err(PsqlError::Uncategorized(
                format!("Unknown format code: {}", format_code).into(),
            )),
        }
    }
}

/// FormatIterator used to generate formats of actual length given the provided format.
/// According Postgres Document: <https://www.postgresql.org/docs/current/protocol-message-formats.html#:~:text=The%20number%20of,number%20of%20parameters>
/// - If the length of provided format is 0, all format will be default format(TEXT).
/// - If the length of provided format is 1, all format will be the same as this only format.
/// - If the length of provided format > 1, provided format should be the actual format.
#[derive(Debug, Clone)]
pub struct FormatIterator<'a, 'b>
where
    'a: 'b,
{
    _formats: &'a [Format],
    format_iter: Iter<'b, Format>,
    actual_len: usize,
    default_format: Format,
}

impl<'a> FormatIterator<'a, '_> {
    pub fn new(provided_formats: &'a [Format], actual_len: usize) -> Result<Self, String> {
        if !provided_formats.is_empty()
            && provided_formats.len() != 1
            && provided_formats.len() != actual_len
        {
            return Err(format!(
                "format codes length {} is not 0, 1 or equal to actual length {}",
                provided_formats.len(),
                actual_len
            ));
        }

        let default_format = provided_formats.first().copied().unwrap_or(Format::Text);

        Ok(Self {
            _formats: provided_formats,
            default_format,
            format_iter: provided_formats.iter(),
            actual_len,
        })
    }
}

impl Iterator for FormatIterator<'_, '_> {
    type Item = Format;

    fn next(&mut self) -> Option<Self::Item> {
        if self.actual_len == 0 {
            return None;
        }

        self.actual_len -= 1;

        Some(
            self.format_iter
                .next()
                .copied()
                .unwrap_or(self.default_format),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.actual_len, Some(self.actual_len))
    }
}

impl ExactSizeIterator for FormatIterator<'_, '_> {}
unsafe impl TrustedLen for FormatIterator<'_, '_> {}
