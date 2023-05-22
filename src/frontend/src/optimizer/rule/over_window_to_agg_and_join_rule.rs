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

use itertools::Itertools;
use risingwave_expr::function::window::WindowFuncKind;
use risingwave_pb::expr::expr_node::Type;
use risingwave_pb::plan_common::JoinType;

use super::Rule;
use crate::expr::{AggCall, ExprImpl, FunctionCall, InputRef, OrderBy};
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalJoin, LogicalProject, LogicalShare, PlanTreeNodeUnary,
};
use crate::utils::Condition;
use crate::PlanRef;
pub struct OverWindowToAggAndJoinRule;

impl OverWindowToAggAndJoinRule {
    pub fn create() -> Box<dyn Rule> {
        Box::new(OverWindowToAggAndJoinRule)
    }
}

impl Rule for OverWindowToAggAndJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let over_window = plan.as_logical_over_window()?;
        if over_window.window_functions().len() != 1 {
            return None;
        }
        let window = &over_window.window_functions()[0];
        if !window.order_by.is_empty()
            || !window.frame.bounds.start_is_unbounded()
            || !window.frame.bounds.end_is_unbounded()
        {
            return None;
        }
        let group_exprs: Vec<ExprImpl> = window
            .partition_by
            .iter()
            .map(|x| x.clone().into())
            .collect_vec();
        let mut select_exprs = group_exprs.clone();
        if let WindowFuncKind::Aggregate(kind) = window.kind {
            let agg_call = AggCall::new(
                kind,
                window.args.iter().map(|x| x.clone().into()).collect_vec(),
                false,
                OrderBy::any(),
                Condition::true_cond(),
            );
            if agg_call.is_err() {
                return None;
            }
            select_exprs.push(agg_call.unwrap().into());
        } else {
            return None;
        }

        let input_len = over_window.input().schema().len();
        let mut out_fields = (0..input_len).collect_vec();
        out_fields.push(input_len + group_exprs.len());
        let common_input = LogicalShare::create(over_window.input());
        let agg = LogicalAgg::create(select_exprs, group_exprs, None, common_input.clone());
        if agg.is_err() {
            return None;
        }
        let mut on_clause = Condition::true_cond();
        window.partition_by.iter().enumerate().for_each(|(idx, x)| {
            on_clause = on_clause.clone().and(Condition::with_expr(
                FunctionCall::new(
                    Type::Equal,
                    vec![
                        x.clone().into(),
                        InputRef::new(idx + input_len, x.data_type.clone()).into(),
                    ],
                )
                .unwrap()
                .into(),
            ))
        });
        Some(
            LogicalProject::with_out_col_idx(
                LogicalJoin::new(common_input, agg.unwrap().0, JoinType::Inner, on_clause).into(),
                out_fields.into_iter(),
            )
            .into(),
        )
    }
}
