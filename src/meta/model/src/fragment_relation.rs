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

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::actor_dispatcher::DispatcherType;
use crate::{FragmentId, I32Array};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "fragment_relation")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub source_fragment_id: FragmentId,
    #[sea_orm(primary_key)]
    pub target_fragment_id: FragmentId,
    pub dispatcher_type: DispatcherType,
    pub dist_key_indices: I32Array,
    pub output_indices: I32Array,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::fragment::Entity",
        from = "Column::SourceFragmentId",
        to = "super::fragment::Column::FragmentId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    SourceFragment,
    #[sea_orm(
        belongs_to = "super::fragment::Entity",
        from = "Column::TargetFragmentId",
        to = "super::fragment::Column::FragmentId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    TargetFragment,
}

impl ActiveModelBehavior for ActiveModel {}
