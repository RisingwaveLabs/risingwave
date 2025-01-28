//  Copyright 2025 RisingWave Labs
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

use crate::optimizer::plan_node::{BatchIcebergScan, PlanAggCall};
use crate::optimizer::rule::{BoxedRule, Rule};
use crate::PlanRef;

pub struct BatchIcebergCountStar {}

impl Rule for BatchIcebergCountStar {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_batch_simple_agg()?;
        if agg.core.group_key.is_empty()
            && agg.agg_calls().len() == 1
            && agg.agg_calls()[0].eq(&PlanAggCall::count_star())
        {
            let batch_iceberg = agg.core.input.as_batch_iceberg_scan()?;
            return Some(
                BatchIcebergScan::new_count_star_with_batch_iceberg_scan(batch_iceberg).into(),
            );
        }
        None
    }
}

impl BatchIcebergCountStar {
    pub fn create() -> BoxedRule {
        Box::new(BatchIcebergCountStar {})
    }
}
