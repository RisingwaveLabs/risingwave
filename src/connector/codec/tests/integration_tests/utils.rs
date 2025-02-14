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

pub use expect_test::{expect, Expect};
pub use itertools::Itertools;
pub use risingwave_common::catalog::ColumnDesc;
use risingwave_common::types::{
    DataType, Datum, DatumCow, DatumRef, ScalarImpl, ScalarRefImpl, ToDatumRef,
};
use risingwave_pb::plan_common::AdditionalColumn;

/// More concise display for `DataType`, to use in tests.
pub struct DataTypeTestDisplay<'a>(pub &'a DataType);

impl std::fmt::Debug for DataTypeTestDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            DataType::Struct(s) => {
                if s.len() == 1 {
                    // avoid multiline display for single field struct
                    let (name, ty) = s.iter().next().unwrap();
                    return write!(f, "Struct {{ {}: {:?} }}", name, &DataTypeTestDisplay(ty));
                }

                let mut f = f.debug_struct("Struct");
                for (name, ty) in s.iter() {
                    f.field(name, &DataTypeTestDisplay(ty));
                }
                f.finish()?;
                Ok(())
            }
            DataType::List(t) => {
                if t.is_struct() {
                    f.debug_tuple("List")
                        .field(&DataTypeTestDisplay(t))
                        .finish()
                } else {
                    write!(f, "List({:?})", &DataTypeTestDisplay(t))
                }
            }
            DataType::Map(m) => {
                write!(
                    f,
                    "Map({:?},{:?})",
                    &DataTypeTestDisplay(m.key()),
                    &DataTypeTestDisplay(m.value())
                )
            }
            _ => {
                // do not use alternative display for simple types
                write!(f, "{:?}", self.0)
            }
        }
    }
}

/// More concise display for `ScalarRefImpl`, to use in tests.
pub struct ScalarRefImplTestDisplay<'a>(pub ScalarRefImpl<'a>);

impl std::fmt::Debug for ScalarRefImplTestDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            ScalarRefImpl::Struct(s) => {
                if s.iter_fields_ref().len() == 1 {
                    // avoid multiline display for single field struct
                    let field = s.iter_fields_ref().next().unwrap();
                    return write!(f, "StructValue({:#?})", &DatumRefTestDisplay(field));
                }

                let mut f = f.debug_tuple("StructValue");
                for field in s.iter_fields_ref() {
                    f.field(&DatumRefTestDisplay(field));
                }
                f.finish()?;
                Ok(())
            }
            ScalarRefImpl::List(l) => f
                .debug_list()
                .entries(l.iter().map(DatumRefTestDisplay))
                .finish(),
            ScalarRefImpl::Map(m) => f
                .debug_list()
                .entries(m.inner().iter().map(DatumRefTestDisplay))
                .finish(),
            ScalarRefImpl::Jsonb(j) => {
                let compact_str = format!("{}", j);
                if compact_str.len() > 50 {
                    write!(f, "Jsonb({:#?})", jsonbb::ValueRef::from(j))
                } else {
                    write!(f, "Jsonb({:#})", j)
                }
            }
            _ => {
                // do not use alternative display for simple types
                write!(f, "{:?}", self.0)
            }
        }
    }
}

/// More concise display for `ScalarImpl`, to use in tests.
pub struct ScalarImplTestDisplay<'a>(pub &'a ScalarImpl);

impl std::fmt::Debug for ScalarImplTestDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        ScalarRefImplTestDisplay(self.0.as_scalar_ref_impl()).fmt(f)
    }
}

/// More concise display for `DatumRef`, to use in tests.
pub struct DatumRefTestDisplay<'a>(pub DatumRef<'a>);

impl std::fmt::Debug for DatumRefTestDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(scalar) => ScalarRefImplTestDisplay(scalar).fmt(f),
            None => write!(f, "null"),
        }
    }
}

/// More concise display for `Datum`, to use in tests.
pub struct DatumTestDisplay<'a>(pub &'a Datum);

impl std::fmt::Debug for DatumTestDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        DatumRefTestDisplay(self.0.to_datum_ref()).fmt(f)
    }
}

/// More concise display for `DatumCow`, to use in tests.
pub struct DatumCowTestDisplay<'a>(pub &'a DatumCow<'a>);

impl std::fmt::Debug for DatumCowTestDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            DatumCow::Borrowed(datum_ref) => {
                // don't use debug_tuple to avoid extra newline
                write!(f, "Borrowed(")?;
                DatumRefTestDisplay(*datum_ref).fmt(f)?;
                write!(f, ")")?;
            }
            DatumCow::Owned(datum) => {
                write!(f, "Owned(")?;
                DatumTestDisplay(datum).fmt(f)?;
                write!(f, ")")?;
            }
        }
        Ok(())
    }
}

/// More concise display for `ColumnDesc`, to use in tests.
pub struct ColumnDescTestDisplay<'a>(pub &'a ColumnDesc);

impl std::fmt::Debug for ColumnDescTestDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ColumnDesc {
            data_type,
            column_id,
            name,
            generated_or_default_column,
            description,
            additional_column: AdditionalColumn { column_type },
            version: _,
            system_column: _,
        } = &self.0;

        write!(
            f,
            "{name}(#{column_id}): {:#?}",
            DataTypeTestDisplay(data_type)
        )?;

        if let Some(generated_or_default_column) = generated_or_default_column {
            write!(
                f,
                ", generated_or_default_column: {:?}",
                generated_or_default_column
            )?;
        }
        if let Some(description) = description {
            write!(f, ", description: {:?}", description)?;
        }
        if let Some(column_type) = column_type {
            write!(f, ", additional_column: {:?}", column_type)?;
        }
        Ok(())
    }
}
