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

use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;

use super::DataType;
use crate::util::iter_util::{ZipEqDebug, ZipEqFast};

/// A cheaply cloneable struct type.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StructType(Arc<StructTypeInner>);

impl Debug for StructType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StructType")
            .field("field_names", &self.0.field_names)
            .field("field_types", &self.0.field_types)
            .finish()
    }
}

#[derive(Debug, educe::Educe)]
#[educe(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct StructTypeInner {
    /// Details about a struct type. There are 2 cases for a struct:
    /// 1. `field_names.len() == field_types.len()`: it represents a struct with named fields,
    ///     e.g. `STRUCT<i INT, j VARCHAR>`.
    /// 2. `field_names.len() == 0`: it represents a struct with unnamed fields,
    ///     e.g. `ROW(1, 2)`.
    field_names: Box<[String]>,
    field_types: Box<[DataType]>,
    #[educe(PartialEq(ignore))]
    #[educe(Ord(ignore))]
    #[educe(Hash(ignore))]
    unqualified_name: Option<Box<str>>,
}

impl StructType {
    /// Creates a struct type with named fields.
    pub fn new(named_fields: impl IntoIterator<Item = (impl Into<String>, DataType)>) -> Self {
        let iter = named_fields.into_iter();
        let mut field_types = Vec::with_capacity(iter.size_hint().0);
        let mut field_names = Vec::with_capacity(iter.size_hint().0);
        for (name, ty) in iter {
            field_names.push(name.into());
            field_types.push(ty);
        }
        Self(Arc::new(StructTypeInner {
            field_types: field_types.into(),
            field_names: field_names.into(),
            unqualified_name: None,
        }))
    }

    /// Creates a struct type with no fields.
    #[cfg(test)]
    pub fn empty() -> Self {
        Self(Arc::new(StructTypeInner {
            field_types: Box::new([]),
            field_names: Box::new([]),
            unqualified_name: None,
        }))
    }

    /// Creates a struct type with unnamed fields.
    pub fn unnamed(fields: Vec<DataType>) -> Self {
        Self(Arc::new(StructTypeInner {
            field_types: fields.into(),
            field_names: Box::new([]),
            unqualified_name: None,
        }))
    }

    /// Returns the number of fields.
    pub fn len(&self) -> usize {
        self.0.field_types.len()
    }

    /// Returns `true` if there are no fields.
    pub fn is_empty(&self) -> bool {
        self.0.field_types.is_empty()
    }

    /// Gets an iterator over the names of the fields.
    ///
    /// If the struct field is unnamed, the iterator returns **no names**.
    pub fn names(&self) -> impl ExactSizeIterator<Item = &str> {
        self.0.field_names.iter().map(|s| s.as_str())
    }

    /// Gets an iterator over the types of the fields.
    pub fn types(&self) -> impl ExactSizeIterator<Item = &DataType> {
        self.0.field_types.iter()
    }

    /// Gets an iterator over the fields.
    ///
    /// If the struct field is unnamed, the iterator returns **empty strings**.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &DataType)> {
        self.0
            .field_names
            .iter()
            .map(|s| s.as_str())
            .chain(std::iter::repeat("").take(self.0.field_types.len() - self.0.field_names.len()))
            .zip_eq_debug(self.0.field_types.iter())
    }

    /// Compares the datatype with another, ignoring nested field names and metadata.
    pub fn equals_datatype(&self, other: &StructType) -> bool {
        if self.0.field_types.len() != other.0.field_types.len() {
            return false;
        }
        (self.0.field_types.iter())
            .zip_eq_fast(other.0.field_types.iter())
            .all(|(a, b)| a.equals_datatype(b))
    }
}

impl Display for StructType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if self.0.field_names.is_empty() {
            write!(f, "record")
        } else {
            write!(
                f,
                "struct<{}>",
                (self.0.field_types.iter())
                    .zip_eq_fast(self.0.field_names.iter())
                    .map(|(d, s)| format!("{} {}", s, d))
                    .join(", ")
            )
        }
    }
}

impl FromStr for StructType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "record" {
            return Ok(StructType::unnamed(Vec::new()));
        }
        if !(s.starts_with("struct<") && s.ends_with('>')) {
            return Err(anyhow!("expect struct<...>"));
        };
        let mut field_types = Vec::new();
        let mut field_names = Vec::new();
        for field in s[7..s.len() - 1].split(',') {
            let field = field.trim();
            let mut iter = field.split_whitespace();
            let field_name = iter.next().unwrap();
            let field_type = iter.next().unwrap();
            field_names.push(field_name.to_owned());
            field_types.push(DataType::from_str(field_type)?);
        }
        Ok(Self(Arc::new(StructTypeInner {
            field_types: field_types.into(),
            field_names: field_names.into(),
            unqualified_name: None,
        })))
    }
}
