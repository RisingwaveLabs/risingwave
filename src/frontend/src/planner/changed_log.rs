// Copyright 2024 RisingWave Labs
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
use crate::binder::Relation;
use crate::error::Result;
use crate::optimizer::plan_node::LogicalChangedLog;
use crate::{PlanRef, Planner};

impl Planner {
    pub(super) fn plan_changed_log(&mut self, relation: Relation) -> Result<PlanRef> {
        let root = self.plan_relation(relation)?;
        let plan = LogicalChangedLog::create(root);
        Ok(plan)
    }
}
