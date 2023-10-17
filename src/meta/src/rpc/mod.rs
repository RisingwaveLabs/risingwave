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

pub mod cloud_provider;
pub mod ddl_controller;
pub mod election;
pub mod intercept;
pub mod metrics;
// pub mod server;
// pub mod service;

#[derive(Debug)]
pub enum MetaStoreBackend {
    Etcd {
        endpoints: Vec<String>,
        credentials: Option<(String, String)>,
    },
    Mem,
}

pub type ElectionClientRef = std::sync::Arc<dyn ElectionClient>;

pub use election::etcd::EtcdElectionClient;
pub use election::{ElectionClient, ElectionMember};
// pub use service::cluster_service::ClusterServiceImpl;
// pub use service::ddl_service::DdlServiceImpl;
// pub use service::heartbeat_service::HeartbeatServiceImpl;
// pub use service::hummock_service::HummockServiceImpl;
// pub use service::notification_service::NotificationServiceImpl;
// pub use service::stream_service::StreamServiceImpl;
