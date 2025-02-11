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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

use itertools::Itertools;

use super::ApplyResult;
use crate::error::Result;
use crate::optimizer::plan_node::PlanTreeNode;
use crate::optimizer::rule::BoxedRule;
use crate::optimizer::PlanRef;
#[cfg(debug_assertions)]
use crate::Explain;

/// Traverse order of [`HeuristicOptimizer`]
pub enum ApplyOrder {
    TopDown,
    BottomUp,
}

// TODO: we should have a builder of HeuristicOptimizer here
/// A rule-based heuristic optimizer, which traverses every plan nodes and tries to
/// apply each rule on them.
pub struct HeuristicOptimizer<'a> {
    apply_order: &'a ApplyOrder,
    rules: &'a [BoxedRule],
    stats: Stats,
}

impl<'a> HeuristicOptimizer<'a> {
    pub fn new(apply_order: &'a ApplyOrder, rules: &'a [BoxedRule]) -> Self {
        Self {
            apply_order,
            rules,
            stats: Stats::new(),
        }
    }

    fn optimize_node(&mut self, mut plan: PlanRef) -> Result<PlanRef> {
        for rule in self.rules {
            match rule.apply(plan.clone()) {
                ApplyResult::Ok(applied) => {
                    #[cfg(debug_assertions)]
                    Self::check_equivalent_plan(rule.description(), &plan, &applied);

                    plan = applied;
                    self.stats.count_rule(rule);
                }
                ApplyResult::NotApplicable => {}
                ApplyResult::Err(error) => return Err(error),
            }
        }
        Ok(plan)
    }

    fn optimize_inputs(&mut self, plan: PlanRef) -> Result<PlanRef> {
        let pre_applied = self.stats.total_applied();
        let inputs: Vec<_> = plan
            .inputs()
            .into_iter()
            .map(|sub_tree| self.optimize(sub_tree))
            .try_collect()?;

        Ok(if pre_applied != self.stats.total_applied() {
            plan.clone_with_inputs(&inputs)
        } else {
            plan
        })
    }

    pub fn optimize(&mut self, mut plan: PlanRef) -> Result<PlanRef> {
        match self.apply_order {
            ApplyOrder::TopDown => {
                plan = self.optimize_node(plan)?;
                self.optimize_inputs(plan)
            }
            ApplyOrder::BottomUp => {
                plan = self.optimize_inputs(plan)?;
                self.optimize_node(plan)
            }
        }
    }

    pub fn get_stats(&self) -> &Stats {
        &self.stats
    }

    #[cfg(debug_assertions)]
    pub fn check_equivalent_plan(rule_desc: &str, input_plan: &PlanRef, output_plan: &PlanRef) {
        if !input_plan.schema().type_eq(output_plan.schema()) {
            panic!("{} fails to generate equivalent schema.\nInput schema: {:?}\nInput plan: \n{}\nOutput schema: {:?}\nOutput plan: \n{}\nSQL: {}",
                   rule_desc,
                   input_plan.schema(),
                   input_plan.explain_to_string(),
                   output_plan.schema(),
                   output_plan.explain_to_string(),
                   output_plan.ctx().sql());
        }
    }
}

pub struct Stats {
    total_applied: usize,
    rule_counter: HashMap<String, u32>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            rule_counter: HashMap::new(),
            total_applied: 0,
        }
    }

    pub fn count_rule(&mut self, rule: &BoxedRule) {
        self.total_applied += 1;
        match self.rule_counter.entry(rule.description().to_owned()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() += 1;
            }
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
        }
    }

    pub fn has_applied_rule(&self) -> bool {
        self.total_applied != 0
    }

    pub fn total_applied(&self) -> usize {
        self.total_applied
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (rule, count) in &self.rule_counter {
            writeln!(f, "apply {} {} time(s)", rule, count)?;
        }
        Ok(())
    }
}
