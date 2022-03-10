use risingwave_pb::plan::ColumnDesc as ProstColumnDesc;

use crate::types::DataType;

/// Column ID is the unique identifier of a column in a table. Different from table ID,
/// column ID is not globally unique.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ColumnId(i32);

impl ColumnId {
    pub const fn new(column_id: i32) -> Self {
        Self(column_id)
    }
}

impl ColumnId {
    pub fn get_id(&self) -> i32 {
        self.0
    }
}

impl From<i32> for ColumnId {
    fn from(column_id: i32) -> Self {
        Self::new(column_id)
    }
}

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct ColumnDesc {
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub name: String, // for debugging
}

impl ColumnDesc {
    pub fn unnamed(column_id: ColumnId, data_type: DataType) -> ColumnDesc {
        ColumnDesc {
            data_type,
            column_id,
            name: String::new(),
        }
    }
}

impl From<ProstColumnDesc> for ColumnDesc {
    fn from(prost: ProstColumnDesc) -> Self {
        let ProstColumnDesc {
            column_type,
            column_id,
            name,
            type_name: _,
            field_descs: _,
        } = prost;

        Self {
            data_type: DataType::from(&column_type.unwrap()),
            column_id: ColumnId::new(column_id),
            name,
        }
    }
}
