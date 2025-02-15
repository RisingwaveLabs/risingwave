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

use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    ActorId, ActorUpstreamActors, ConnectorSplits, ExprContext, FragmentId, VnodeBitmap, WorkerId,
};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum ActorStatus {
    #[sea_orm(string_value = "INACTIVE")]
    Inactive,
    #[sea_orm(string_value = "RUNNING")]
    Running,
}

impl From<PbActorState> for ActorStatus {
    fn from(val: PbActorState) -> Self {
        match val {
            PbActorState::Unspecified => unreachable!(),
            PbActorState::Inactive => ActorStatus::Inactive,
            PbActorState::Running => ActorStatus::Running,
        }
    }
}

impl From<ActorStatus> for PbActorState {
    fn from(val: ActorStatus) -> Self {
        match val {
            ActorStatus::Inactive => PbActorState::Inactive,
            ActorStatus::Running => PbActorState::Running,
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "actor")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub actor_id: ActorId,
    pub fragment_id: FragmentId,
    pub status: ActorStatus,
    pub splits: Option<ConnectorSplits>,
    pub worker_id: WorkerId,
    #[deprecated]
    pub upstream_actor_ids: ActorUpstreamActors,
    pub vnode_bitmap: Option<VnodeBitmap>,
    pub expr_context: ExprContext,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::actor_dispatcher::Entity")]
    ActorDispatcher,
    #[sea_orm(
        belongs_to = "super::fragment::Entity",
        from = "Column::FragmentId",
        to = "super::fragment::Column::FragmentId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Fragment,
}

impl Related<super::actor_dispatcher::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ActorDispatcher.def()
    }
}

impl Related<super::fragment::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fragment.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
