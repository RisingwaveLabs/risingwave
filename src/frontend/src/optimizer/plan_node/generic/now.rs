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

use educe::Educe;
use enum_as_inner::EnumAsInner;
use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, Interval, Timestamptz};

use super::{DistillUnit, GenericPlanNode};
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::FunctionalDependencySet;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Now {
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,

    pub mode: Mode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumAsInner)]
pub enum Mode {
    /// Emit current timestamp on startup, update it on barrier.
    UpdateCurrent,
    /// Generate a series of timestamps starting from `start_timestamp` with `interval`.
    /// Keep generating new timestamps on barrier.
    GenerateSeries {
        start_timestamp: Timestamptz,
        interval: Interval,
    },
}

impl GenericPlanNode for Now {
    fn functional_dependency(&self) -> crate::optimizer::property::FunctionalDependencySet {
        FunctionalDependencySet::new(1) // only one column and no dependency
    }

    fn schema(&self) -> risingwave_common::catalog::Schema {
        Schema::new(vec![Field {
            data_type: DataType::Timestamptz,
            name: String::from(if self.mode.is_update_current() {
                "now"
            } else {
                "ts"
            }),
        }])
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        match self.mode {
            Mode::UpdateCurrent => Some(vec![]),
            Mode::GenerateSeries { .. } => Some(vec![0]),
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }
}

impl Now {
    pub fn update_current(ctx: OptimizerContextRef) -> Self {
        Self::new_inner(ctx, Mode::UpdateCurrent)
    }

    pub fn generate_series(
        ctx: OptimizerContextRef,
        start_timestamp: Timestamptz,
        interval: Interval,
    ) -> Self {
        Self::new_inner(
            ctx,
            Mode::GenerateSeries {
                start_timestamp,
                interval,
            },
        )
    }

    fn new_inner(ctx: OptimizerContextRef, mode: Mode) -> Self {
        Self { ctx, mode }
    }
}

impl DistillUnit for Now {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        childless_record(name, vec![("mode", Pretty::debug(&self.mode))])
    }
}
