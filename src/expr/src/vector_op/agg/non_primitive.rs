// Copyright 2023 RisingWave Labs
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

use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::types::num256::Int256;
use risingwave_common::types::DataType;

use crate::vector_op::agg::aggregator::Aggregator;

#[derive(Clone)]
pub struct NonPrimitiveSum {
    return_type: DataType,
    result: Int256,
}

impl NonPrimitiveSum {
    pub fn new(return_type: DataType) -> Self {
        Self {
            return_type,
            result: Int256::default(),
        }
    }
}

#[async_trait::async_trait]
impl Aggregator for NonPrimitiveSum {
    fn return_type(&self) -> DataType {
        todo!()
    }

    async fn update_single(&mut self, input: &DataChunk, row_id: usize) -> crate::Result<()> {
        todo!()
    }

    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> crate::Result<()> {
        todo!()
    }

    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> crate::Result<()> {
        todo!()
    }
    // fn return_type(&self) -> DataType {
    //     self.return_type.clone()
    // }
    //
    // async fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
    //     if let (_, true) = input.row_at(row_id) {
    //         self.result += 1;
    //     }
    //     Ok(())
    // }
    //
    // async fn update_multi(
    //     &mut self,
    //     input: &DataChunk,
    //     start_row_id: usize,
    //     end_row_id: usize,
    // ) -> Result<()> {
    //     if let Some(visibility) = input.visibility() {
    //         for row_id in start_row_id..end_row_id {
    //             if visibility.is_set(row_id) {
    //                 self.result += 1;
    //             }
    //         }
    //     } else {
    //         self.result += end_row_id - start_row_id;
    //     }
    //     Ok(())
    // }
    //
    // fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
    //     let res = std::mem::replace(&mut self.result, 0) as i64;
    //     match builder {
    //         ArrayBuilderImpl::Int64(b) => {
    //             b.append(Some(res));
    //             Ok(())
    //         }
    //         _ => bail!("Unexpected builder for count(*)."),
    //     }
    // }
}
