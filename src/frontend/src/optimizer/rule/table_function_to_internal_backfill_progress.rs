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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqDebug;

use super::{BoxedRule, Rule};
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::expr::{Expr, TableFunctionType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::LogicalTableFunction;
use crate::optimizer::PlanRef;
use crate::TableCatalog;

/// Transform a special `TableFunction` (with `FILE_SCAN` table function type) into a `LogicalFileScan`
pub struct TableFunctionToInternalBackfillProgressRule {}
impl Rule for TableFunctionToInternalBackfillProgressRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function.function_type
            != TableFunctionType::InternalBackfillProgress
        {
            return None;
        }
        let DataType::Struct(st) = logical_table_function.table_function().return_type() else {
            unreachable!()
        };
        let reader = plan.ctx().session_ctx().env().catalog_reader().read_guard();
        // TODO(kwannoel): Make sure it reads from source, snapshot backfill tables as well.
        let backfilling_table_names = get_backfilling_tables(reader);
        todo!()
    }
}

fn get_backfilling_tables(reader: CatalogReadGuard) -> Vec<TableCatalog> {
    todo!()
    // reader
    //     .get_creating_tables()
}

impl TableFunctionToInternalBackfillProgressRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToInternalBackfillProgressRule {})
    }
}
