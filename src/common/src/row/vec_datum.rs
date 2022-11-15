// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::hash::{BuildHasher, Hasher};
use std::{cmp, ops};

use itertools::Itertools;

use crate::array::RowRef;
use crate::collection::estimate_size::EstimateSize;
use crate::hash::HashCode;
use crate::types::{hash_datum, DataType, Datum};
use crate::util::ordered::OrderedRowSerde;
use crate::util::value_encoding;
use crate::util::value_encoding::{deserialize_datum, serialize_datum};

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Row(pub Vec<Datum>);

impl ops::Index<usize> for Row {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl ops::IndexMut<usize> for Row {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

// TODO: remove this due to implicit allocation
impl From<RowRef<'_>> for Row {
    fn from(row_ref: RowRef<'_>) -> Self {
        row_ref.to_owned_row()
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.0.len() != other.0.len() {
            return None;
        }
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Row {
    pub fn new(values: Vec<Datum>) -> Self {
        Self(values)
    }

    pub fn empty<'a>() -> &'a Self {
        static EMPTY_ROW: Row = Row(Vec::new());
        &EMPTY_ROW
    }

    /// Compare two rows' key
    pub fn cmp_by_key(
        row1: impl AsRef<Self>,
        key1: &[usize],
        row2: impl AsRef<Self>,
        key2: &[usize],
    ) -> cmp::Ordering {
        assert_eq!(key1.len(), key2.len());
        let pk_len = key1.len();
        for i in 0..pk_len {
            let datum1 = &row1.as_ref()[key1[i]];
            let datum2 = &row2.as_ref()[key2[i]];
            if datum1 > datum2 {
                return cmp::Ordering::Greater;
            }
            if datum1 < datum2 {
                return cmp::Ordering::Less;
            }
        }
        cmp::Ordering::Equal
    }

    /// Serialize the row into value encoding bytes.
    /// WARNING: If you want to serialize to a memcomparable format, use
    /// [`crate::util::ordered::OrderedRow`]
    ///
    /// All values are nullable. Each value will have 1 extra byte to indicate whether it is null.
    pub fn serialize(&self, value_indices: &Option<Vec<usize>>) -> Vec<u8> {
        let mut result = vec![];
        // value_indices is None means serializing each `Datum` in sequence, otherwise only
        // columns of given value_indices will be serialized.
        match value_indices {
            Some(value_indices) => {
                for value_idx in value_indices {
                    serialize_datum(&self.0[*value_idx], &mut result);
                }
            }
            None => {
                for cell in &self.0 {
                    serialize_datum(cell, &mut result);
                }
            }
        }

        result
    }

    /// Serialize part of the row into memcomparable bytes.
    pub fn extract_memcomparable_by_indices(
        &self,
        serializer: &OrderedRowSerde,
        key_indices: &[usize],
    ) -> Vec<u8> {
        let mut bytes = vec![];
        serializer.serialize_datums(self.datums_by_indices(key_indices), &mut bytes);
        bytes
    }

    /// Return number of cells in the row.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn push(&mut self, value: Datum) {
        self.0.push(value);
    }

    pub fn values(&self) -> impl Iterator<Item = &Datum> {
        self.0.iter()
    }

    pub fn concat(&self, values: impl IntoIterator<Item = Datum>) -> Row {
        Row::new(self.values().cloned().chain(values).collect())
    }

    /// Hash row data all in one
    pub fn hash_row<H>(&self, hash_builder: &H) -> HashCode
    where
        H: BuildHasher,
    {
        let mut hasher = hash_builder.build_hasher();
        for datum in &self.0 {
            hash_datum(datum, &mut hasher);
        }
        HashCode(hasher.finish())
    }

    /// Compute hash value of a row on corresponding indices.
    pub fn hash_by_indices<H>(&self, hash_indices: &[usize], hash_builder: &H) -> HashCode
    where
        H: BuildHasher,
    {
        let mut hasher = hash_builder.build_hasher();
        for idx in hash_indices {
            hash_datum(&self.0[*idx], &mut hasher);
        }
        HashCode(hasher.finish())
    }

    /// Get an owned `Row` by the given `indices` from current row.
    ///
    /// Use `datum_refs_by_indices` if possible instead to avoid allocating owned datums.
    pub fn by_indices(&self, indices: &[usize]) -> Row {
        Row(indices.iter().map(|&idx| self.0[idx].clone()).collect_vec())
    }

    /// Get a reference to the datums in the row by the given `indices`.
    pub fn datums_by_indices<'a>(&'a self, indices: &'a [usize]) -> impl Iterator<Item = &Datum> {
        indices.iter().map(|&idx| &self.0[idx])
    }
}

impl EstimateSize for Row {
    fn estimated_heap_size(&self) -> usize {
        // FIXME(bugen): this is not accurate now as the heap size of some `Scalar` is not counted.
        self.0.capacity() * std::mem::size_of::<Datum>()
    }
}

/// Deserializer of the `Row`.
#[derive(Clone, Debug)]
pub struct RowDeserializer {
    data_types: Vec<DataType>,
}

impl RowDeserializer {
    /// Creates a new `RowDeserializer` with row schema.
    pub fn new(data_types: Vec<DataType>) -> Self {
        RowDeserializer { data_types }
    }

    /// Deserialize the row from value encoding bytes.
    pub fn deserialize(&self, mut data: impl bytes::Buf) -> value_encoding::Result<Row> {
        let mut values = Vec::with_capacity(self.data_types.len());
        for typ in &self.data_types {
            values.push(deserialize_datum(&mut data, typ)?);
        }
        Ok(Row(values))
    }

    pub fn data_types(&self) -> &[DataType] {
        &self.data_types
    }
}
