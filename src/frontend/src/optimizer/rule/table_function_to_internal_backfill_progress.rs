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

use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{internal_table_name_to_parts, Field, Schema, StreamJobStatus};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_expr::aggregate::AggType;

use super::{ApplyResult, BoxedRule, FallibleRule};
use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::table_catalog::TableType;
use crate::expr::{AggCall, ExprImpl, ExprType, FunctionCall, InputRef, Literal, OrderBy, TableFunctionType};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalAgg, LogicalJoin, LogicalProject, LogicalScan, LogicalTableFunction, LogicalUnion, LogicalValues};
use crate::optimizer::PlanRef;
use crate::utils::{Condition, GroupBy};
use crate::TableCatalog;
pub use risingwave_pb::expr::agg_call::PbKind as PbAggKind;
use risingwave_pb::plan_common::JoinType;

/// Transform a special `TableFunction` (with `FILE_SCAN` table function type) into a `LogicalFileScan`
pub struct TableFunctionToInternalBackfillProgressRule {}
impl FallibleRule for TableFunctionToInternalBackfillProgressRule {
    fn apply(&self, plan: PlanRef) -> ApplyResult {
        let logical_table_function: &LogicalTableFunction = plan.as_logical_table_function()?;
        if logical_table_function.table_function.function_type
            != TableFunctionType::InternalBackfillProgress
        {
            return ApplyResult::NotApplicable;
        }

        let fields = vec![
            Field::new("job_id", DataType::Int32),
            Field::new("row_count", DataType::Int64),
        ];

        let reader = plan.ctx().session_ctx().env().catalog_reader().read_guard();
        // TODO(kwannoel): Make sure it reads from source tables as well.
        let backfilling_tables = get_backfilling_tables(reader);

        // No backfill in progress, just return empty values.
        if backfilling_tables.is_empty() {
            let plan = LogicalValues::new(vec![], Schema::new(fields), plan.ctx().clone());
            return ApplyResult::Ok(plan.into());
        }

        let mut counts = Vec::with_capacity(backfilling_tables.len());
        let mut backfilling_job_ids = vec![];
        for table in backfilling_tables {
            let Some(job_id) = table.job_id else {
                return ApplyResult::Err(
                    anyhow!("`job_id` column not found in backfill table").into(),
                );
            };
            backfilling_job_ids.push(job_id);
            let Some(row_count_column_index) =
                table.columns.iter().position(|c| c.name() == "row_count")
            else {
                return ApplyResult::Err(
                    anyhow!("`row_count` column not found in backfill table").into(),
                );
            };
            let scan = LogicalScan::create(
                table.name.clone(),
                table,
                vec![],
                plan.ctx(),
                None,
                Default::default(),
            );
            let project = {
                let job_id_expr = ExprImpl::Literal(Box::new(Literal::new(
                    Some(ScalarImpl::Int32(job_id.table_id as i32)),
                    DataType::Int32,
                )));
                let row_count_expr = ExprImpl::InputRef(Box::new(InputRef {
                    index: row_count_column_index,
                    data_type: DataType::Int64,
                }));
                LogicalProject::new(
                    scan.into(),
                    vec![job_id_expr, row_count_expr],
                )
            };
            counts.push(project.into());
        }
        let union = LogicalUnion::new(true, counts);
        let select_exprs = {
            let job_id = ExprImpl::InputRef(Box::new(InputRef {
                index: 0,
                data_type: DataType::Int32,
            }));
            let sum_agg = ExprImpl::AggCall(Box::new(AggCall::new(
                AggType::Builtin(PbAggKind::Sum),
                vec![
                    ExprImpl::InputRef(Box::new(InputRef {
                        index: 1,
                        data_type: DataType::Int64,
                    })),
                ],
                false,
                OrderBy::any(),
                Condition::true_cond(),
                vec![],
            )?));
            vec![
                job_id,
                sum_agg,
            ]
        };
        let group_key = GroupBy::GroupKey(vec![ExprImpl::InputRef(Box::new(InputRef {
            index: 0,
            data_type: DataType::Int32,
        }))]);
        let (agg, _rewritten_select_exprs, _rewritten_having_exprs) =
            LogicalAgg::create(select_exprs, group_key, None, union.into())?;
        let current_counts = LogicalProject::new(
            agg.into(),
            vec![
                ExprImpl::InputRef(Box::new(InputRef {
                    index: 0,
                    data_type: DataType::Int32,
                })),
                ExprImpl::InputRef(Box::new(InputRef {
                    index: 1,
                    data_type: DataType::Decimal,
                })).cast_explicit(DataType::Decimal)?,
            ],
        );

        let total_counts = {
            let catalog = plan.ctx().session_ctx().env().catalog_reader().read_guard();
            let mut total_counts = vec![];
            for job_id in backfilling_job_ids {
                let total_key_count = if let Some(stats) = catalog.table_stats().table_stats.get(&(job_id.table_id)) {
                    stats.total_key_count
                } else {
                    return ApplyResult::Err(
                        anyhow!("Table stats not found for table_id: {}", job_id.table_id).into(),
                    );
                };
                let job_id_expr = ExprImpl::Literal(Box::new(Literal::new(
                    Some(ScalarImpl::Int32(job_id.table_id as i32)),
                    DataType::Int32,
                )));
                let total_key_count_expr = ExprImpl::Literal(Box::new(Literal::new(
                    Some(ScalarImpl::Int64(total_key_count as i64)),
                    DataType::Int64,
                )));
                let total_count = LogicalValues::new(
                    vec![vec![job_id_expr, total_key_count_expr]],
                    Schema::new(vec![
                        Field::new("job_id", DataType::Int32),
                        Field::new("total_key_count", DataType::Int64),
                    ]),
                    plan.ctx().clone(),
                );
                total_counts.push(total_count.into());
            }
            LogicalUnion::new(true, total_counts)
        };

        let join = {
            let conjunctions = vec![
                ExprImpl::FunctionCall(Box::new(FunctionCall::new(
                    ExprType::Equal,
                    vec![
                        ExprImpl::InputRef(Box::new(InputRef {
                            index: 0,
                            data_type: DataType::Int32,
                        })),
                        ExprImpl::InputRef(Box::new(InputRef {
                            index: 2,
                            data_type: DataType::Int32,
                        })),
                    ],
                )?)),
            ];
            let condition = Condition { conjunctions };
            LogicalJoin::new(
                current_counts.into(),
                total_counts.into(),
                JoinType::Inner,
                condition,
            )
        };

        let project = {
            let op1 = ExprImpl::InputRef(Box::new(InputRef {
                index: 1,
                data_type: DataType::Int64,
            })).cast_implicit(DataType::Decimal)?;
            let op2 = ExprImpl::InputRef(Box::new(InputRef {
                index: 3,
                data_type: DataType::Int64,
            })).cast_implicit(DataType::Decimal)?;
            let div_expr = ExprImpl::FunctionCall(Box::new(FunctionCall::new(
                ExprType::Divide,
                vec![op1, op2],
            )?));
            LogicalProject::new(
                join.into(),
                vec![
                    ExprImpl::InputRef(Box::new(InputRef {
                        index: 0,
                        data_type: DataType::Int32,
                    })),
                    div_expr,
                ],
            )
        };

        ApplyResult::Ok(project.into())
    }
}

fn get_backfilling_tables(reader: CatalogReadGuard) -> Vec<Arc<TableCatalog>> {
    reader
        .iter_tables()
        .filter(|table| {
            let name = &table.name;
            println!("table_name: {:?}", name);
            println!("vnode count: {:?}", table.vnode_count);
            match internal_table_name_to_parts(name) {
                None => false,
                Some((_job_name, _fragment_id, executor_type, _table_id)) => {
                    let is_backfill = executor_type == "streamscan";
                    println!("is_backfill: {:?}", is_backfill);
                    let is_creating = table.stream_job_status == StreamJobStatus::Creating;
                    println!("is_creating: {:?}", is_creating);
                    let is_internal = table.table_type == TableType::Internal;
                    println!("is_internal: {:?}", is_internal);
                    is_backfill && is_creating && is_internal
                }
            }
        })
        .cloned()
        .collect_vec()
}

impl TableFunctionToInternalBackfillProgressRule {
    pub fn create() -> BoxedRule {
        Box::new(TableFunctionToInternalBackfillProgressRule {})
    }
}
