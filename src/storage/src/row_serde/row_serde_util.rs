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

use std::sync::Arc;

use bytes::Buf;
use memcomparable::from_slice;
use risingwave_common::array::Row;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, VirtualNode, VIRTUAL_NODE_SIZE};
use risingwave_common::util::ordered::{OrderedRowDeserializer, OrderedRowSerializer};
use risingwave_common::util::value_encoding::deserialize_datum;

use super::ColumnDescMapping;

pub fn serialize_pk(pk: &Row, serializer: &OrderedRowSerializer) -> Vec<u8> {
    let mut result = vec![];
    serializer.serialize(pk, &mut result);
    result
}

pub fn serialize_pk_with_vnode(
    pk: &Row,
    serializer: &OrderedRowSerializer,
    vnode: VirtualNode,
) -> Vec<u8> {
    let pk_bytes = serialize_pk(pk, serializer);
    [&vnode.to_be_bytes(), pk_bytes.as_slice()].concat()
}

// NOTE: Only for debug purpose now
pub fn deserialize_pk_with_vnode(
    key: &[u8],
    deserializer: &OrderedRowDeserializer,
) -> Result<(VirtualNode, Row)> {
    let vnode = VirtualNode::from_be_bytes(key[0..VIRTUAL_NODE_SIZE].try_into().unwrap());
    let pk = deserializer.deserialize(&key[VIRTUAL_NODE_SIZE..])?;
    Ok((vnode, pk))
}

pub fn deserialize_column_id(bytes: &[u8]) -> Result<ColumnId> {
    let column_id = from_slice::<u32>(bytes)? ^ (1 << 31);
    Ok((column_id as i32).into())
}

pub fn parse_raw_key_to_vnode_and_key(raw_key: &[u8]) -> (VirtualNode, &[u8]) {
    let (vnode_bytes, key_bytes) = raw_key.split_at(VIRTUAL_NODE_SIZE);
    let vnode = VirtualNode::from_be_bytes(vnode_bytes.try_into().unwrap());
    (vnode, key_bytes)
}

/// used for batch table deserialize
// TODO(eric): remove usages of this. Use `RowDeserializer` instead
pub fn batch_deserialize(
    column_mapping: Arc<ColumnDescMapping>,
    value: impl AsRef<[u8]>,
) -> Result<Row> {
    let mut origin_row = streaming_deserialize(&column_mapping.all_data_types, value.as_ref())?;

    let mut output_row = Vec::with_capacity(column_mapping.output_index.len());
    for col_idx in &column_mapping.output_index {
        output_row.push(origin_row.0[*col_idx].take());
    }
    Ok(Row(output_row))
}

// TODO(eric): remove me along with batch_deserialize
fn streaming_deserialize(data_types: &[DataType], mut row: impl Buf) -> Result<Row> {
    // value encoding
    let mut values = Vec::with_capacity(data_types.len());
    for ty in data_types {
        values.push(deserialize_datum(&mut row, ty)?);
    }

    Ok(Row(values))
}
