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

mod compacted_row;
mod vec_datum;

use std::hash::{BuildHasher, Hasher};

pub use compacted_row::CompactedRow;
pub use vec_datum::{Row, RowDeserializer};

use crate::array::RowRef;
use crate::hash::HashCode;
use crate::types::{hash_datum_ref, to_datum_ref, Datum, DatumRef, ToOwnedDatum};
use crate::util::value_encoding;

pub trait Row2: Sized + std::fmt::Debug + PartialEq + Eq {
    type DatumIter<'a>: Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_>;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn datums(&self) -> Self::DatumIter<'_>;

    fn to_owned_row(&self) -> Row {
        Row(self.datums().map(|d| d.to_owned_datum()).collect())
    }

    fn into_owned_row(self) -> Row {
        self.to_owned_row()
    }

    fn value_serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        for datum in self.datums() {
            value_encoding::serialize_datum_ref(&datum, &mut buf);
        }
        buf
    }

    fn hash<H: BuildHasher>(&self, hash_builder: H) -> HashCode {
        let mut hasher = hash_builder.build_hasher();
        for datum in self.datums() {
            hash_datum_ref(datum, &mut hasher);
        }
        HashCode(hasher.finish())
    }
}

const fn assert_row<R: Row2>(r: R) -> R {
    r
}

pub trait RowExt: Row2 {
    fn chain<R: Row2>(self, other: R) -> Chain<Self, R>
    where
        Self: Sized,
    {
        assert_row(Chain {
            r1: self,
            r2: other,
        })
    }

    fn project(self, indices: &[usize]) -> Project<'_, Self>
    where
        Self: Sized,
    {
        assert_row(Project { row: self, indices })
    }
}

impl<R: Row2> RowExt for R {}

#[derive(Debug)]
pub struct Chain<R1, R2> {
    r1: R1,
    r2: R2,
}

impl<R1: Row2, R2: Row2> PartialEq for Chain<R1, R2> {
    fn eq(&self, other: &Self) -> bool {
        self.datums().eq(other.datums())
    }
}
impl<R1: Row2, R2: Row2> Eq for Chain<R1, R2> {}

impl<R1: Row2, R2: Row2> Row2 for Chain<R1, R2> {
    type DatumIter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        R1: 'a,
        R2: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        if index < self.r1.len() {
            self.r1.datum_at(index)
        } else {
            self.r2.datum_at(index - self.r1.len())
        }
    }

    fn len(&self) -> usize {
        self.r1.len() + self.r2.len()
    }

    fn is_empty(&self) -> bool {
        self.r1.is_empty() && self.r2.is_empty()
    }

    fn datums(&self) -> Self::DatumIter<'_> {
        self.r1.datums().chain(self.r2.datums())
    }
}

#[derive(Debug)]
pub struct Project<'i, R> {
    row: R,
    indices: &'i [usize],
}

impl<'i, R: Row2> PartialEq for Project<'i, R> {
    fn eq(&self, other: &Self) -> bool {
        self.datums().eq(other.datums())
    }
}
impl<'i, R: Row2> Eq for Project<'i, R> {}

impl<'i, R: Row2> Row2 for Project<'i, R> {
    type DatumIter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        R: 'a,
        'i: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        self.row.datum_at(self.indices[index])
    }

    fn len(&self) -> usize {
        self.indices.len()
    }

    fn datums(&self) -> Self::DatumIter<'_> {
        self.indices.iter().map(|&i| self.row.datum_at(i))
    }
}

macro_rules! deref_forward_row {
    () => {
        fn datum_at(&self, index: usize) -> DatumRef<'_> {
            (**self).datum_at(index)
        }

        fn len(&self) -> usize {
            (**self).len()
        }

        fn is_empty(&self) -> bool {
            (**self).is_empty()
        }

        fn datums(&self) -> Self::DatumIter<'_> {
            (**self).datums()
        }

        fn to_owned_row(&self) -> Row {
            (**self).to_owned_row()
        }
    };
}

impl<R: Row2> Row2 for &R {
    type DatumIter<'a> = R::DatumIter<'a>
    where
        Self: 'a;

    deref_forward_row!();
}

impl<R: Row2> Row2 for Box<R> {
    type DatumIter<'a> = R::DatumIter<'a>
    where
        Self: 'a;

    deref_forward_row!();

    fn into_owned_row(self) -> Row {
        (*self).into_owned_row()
    }
}

impl Row2 for &[Datum] {
    type DatumIter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        to_datum_ref(&self[index])
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn datums(&self) -> Self::DatumIter<'_> {
        Iterator::map(self.as_ref().iter(), to_datum_ref)
    }
}

impl Row2 for &[DatumRef<'_>] {
    type DatumIter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        self[index]
    }

    fn len(&self) -> usize {
        <[DatumRef<'_>]>::len(self)
    }

    fn datums(&self) -> Self::DatumIter<'_> {
        self.iter().copied()
    }
}

impl Row2 for Row {
    type DatumIter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        to_datum_ref(&self[index])
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn datums(&self) -> Self::DatumIter<'_> {
        Iterator::map(self.0.iter(), to_datum_ref)
    }

    fn to_owned_row(&self) -> Row {
        self.clone()
    }

    fn into_owned_row(self) -> Row {
        self
    }
}

impl Row2 for RowRef<'_> {
    type DatumIter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        RowRef::value_at(self, index)
    }

    fn len(&self) -> usize {
        RowRef::size(self)
    }

    fn datums(&self) -> Self::DatumIter<'_> {
        RowRef::values(self)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Empty;

impl Row2 for Empty {
    type DatumIter<'a> = impl Iterator<Item = DatumRef<'a>>
    where
        Self: 'a;

    fn datum_at(&self, index: usize) -> DatumRef<'_> {
        [][index]
    }

    fn len(&self) -> usize {
        0
    }

    fn datums(&self) -> Self::DatumIter<'_> {
        std::iter::empty()
    }
}
