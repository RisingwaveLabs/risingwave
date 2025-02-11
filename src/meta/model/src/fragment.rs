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

use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{FragmentId, I32Array, ObjectId, StreamNode};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "fragment")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub fragment_id: FragmentId,
    pub job_id: ObjectId,
    pub fragment_type_mask: i32,
    pub distribution_type: DistributionType,
    /// Note: the `StreamNode` is different from the final plan node used by actors.
    /// Specifically, `Merge` nodes' `upstream_actor_id` will be filled. (See `compose_fragment`)
    pub stream_node: StreamNode,
    pub state_table_ids: I32Array,
    // pub upstream_fragment_id: I32Array,
    pub vnode_count: i32,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum DistributionType {
    #[sea_orm(string_value = "SINGLE")]
    Single,
    #[sea_orm(string_value = "HASH")]
    Hash,
}

impl From<DistributionType> for PbFragmentDistributionType {
    fn from(val: DistributionType) -> Self {
        match val {
            DistributionType::Single => PbFragmentDistributionType::Single,
            DistributionType::Hash => PbFragmentDistributionType::Hash,
        }
    }
}

impl From<PbFragmentDistributionType> for DistributionType {
    fn from(val: PbFragmentDistributionType) -> Self {
        match val {
            PbFragmentDistributionType::Unspecified => unreachable!(),
            PbFragmentDistributionType::Single => DistributionType::Single,
            PbFragmentDistributionType::Hash => DistributionType::Hash,
        }
    }
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::actor::Entity")]
    Actor,
    #[sea_orm(has_many = "super::actor_dispatcher::Entity")]
    ActorDispatcher,
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::JobId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
}

impl Related<super::actor::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Actor.def()
    }
}

impl Related<super::actor_dispatcher::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ActorDispatcher.def()
    }
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
