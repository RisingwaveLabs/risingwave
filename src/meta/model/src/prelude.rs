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

pub use {Source as SourceModel, Table as TableModel};

pub use super::actor::Entity as Actor;
pub use super::actor_dispatcher::Entity as ActorDispatcher;
pub use super::catalog_version::Entity as CatalogVersion;
pub use super::cluster::Entity as Cluster;
pub use super::compaction_config::Entity as CompactionConfig;
pub use super::compaction_status::Entity as CompactionStatus;
pub use super::compaction_task::Entity as CompactionTask;
pub use super::connection::Entity as Connection;
pub use super::database::Entity as Database;
pub use super::fragment::Entity as Fragment;
pub use super::fragment_relation::Entity as FragmentRelation;
pub use super::function::Entity as Function;
pub use super::hummock_pinned_snapshot::Entity as HummockPinnedSnapshot;
pub use super::hummock_pinned_version::Entity as HummockPinnedVersion;
pub use super::hummock_sequence::Entity as HummockSequence;
pub use super::hummock_version_delta::Entity as HummockVersionDelta;
pub use super::hummock_version_stats::Entity as HummockVersionStats;
pub use super::index::Entity as Index;
pub use super::object::Entity as Object;
pub use super::object_dependency::Entity as ObjectDependency;
pub use super::schema::Entity as Schema;
pub use super::secret::Entity as Secret;
pub use super::session_parameter::Entity as SessionParameter;
pub use super::sink::Entity as Sink;
pub use super::source::Entity as Source;
pub use super::streaming_job::Entity as StreamingJob;
pub use super::subscription::Entity as Subscription;
pub use super::system_parameter::Entity as SystemParameter;
pub use super::table::Entity as Table;
pub use super::user::Entity as User;
pub use super::user_privilege::Entity as UserPrivilege;
pub use super::view::Entity as View;
pub use super::worker::Entity as Worker;
pub use super::worker_property::Entity as WorkerProperty;
