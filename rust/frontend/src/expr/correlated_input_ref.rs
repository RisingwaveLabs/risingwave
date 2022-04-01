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

use core::fmt;

use risingwave_common::types::DataType;

use super::Expr;
use crate::expr::ExprType;

#[derive(Clone, Eq, PartialEq, Hash)]
/// A reference to a column outside the subquery.
/// 
/// `depth` is the number of of nesting levels of the subquery relative to the refered relation, and should be non-zero. 
/// 
/// `index` is the index in the refered relation.
pub struct CorrelatedInputRef {
    index: usize,
    data_type: DataType,
    depth: usize,
}

impl CorrelatedInputRef {
    pub fn new(index: usize, data_type: DataType, depth: usize) -> Self {
        CorrelatedInputRef {
            index,
            data_type,
            depth,
        }
    }

    pub fn get_expr_type(&self) -> ExprType {
        ExprType::InputRef
    }

    /// Get a reference to the input ref's index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Get a reference to the input ref's data type.
    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    pub fn depth(&self) -> usize {
        self.depth
    }
}

impl Expr for CorrelatedInputRef {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn to_protobuf(&self) -> risingwave_pb::expr::ExprNode {
        use risingwave_pb::expr::expr_node::*;
        use risingwave_pb::expr::*;
        ExprNode {
            expr_type: self.get_expr_type().into(),
            return_type: Some(self.return_type().to_protobuf()),
            rex_node: Some(RexNode::InputRef(InputRefExpr {
                column_idx: self.index() as i32,
            })),
        }
    }
}

impl fmt::Debug for CorrelatedInputRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CorrelatedInputRef")
                .field("index", &self.index)
                .field("depth", &self.depth)
                .finish()
    }
}
