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

use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::user::grant_privilege::Object;

use crate::catalog::system_catalog::{get_acl_items, SysCatalogReaderImpl};
use crate::error::Result;
use crate::user::has_access_to_object;

#[derive(Fields)]
struct RwSubscription {
    #[primary_key]
    id: i32,
    name: String,
    schema_id: i32,
    owner: i32,
    definition: String,
    acl: Vec<String>,
    initialized_at: Option<Timestamptz>,
    created_at: Option<Timestamptz>,
    initialized_at_cluster_version: Option<String>,
    created_at_cluster_version: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_subscriptions")]
fn read_rw_sinks_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwSubscription>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;
    let user_reader = reader.user_info_reader.read_guard();
    let current_user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .expect("user not found");
    let users = user_reader.get_all_users();
    let username_map = user_reader.get_user_name_map();

    Ok(schemas
        .flat_map(|schema| {
            schema
                .iter_subscription()
                .filter(|s| {
                    has_access_to_object(
                        current_user,
                        &schema.name,
                        s.id.subscription_id,
                        s.owner.user_id,
                    )
                })
                .map(|subscription| RwSubscription {
                    id: subscription.id.subscription_id as i32,
                    name: subscription.name.clone(),
                    schema_id: schema.id() as i32,
                    owner: subscription.owner.user_id as i32,
                    definition: subscription.definition.clone(),
                    acl: get_acl_items(
                        &Object::SubscriptionId(subscription.id.subscription_id),
                        false,
                        &users,
                        username_map,
                    ),
                    initialized_at: subscription
                        .initialized_at_epoch
                        .map(|e| e.as_timestamptz()),
                    created_at: subscription.created_at_epoch.map(|e| e.as_timestamptz()),
                    initialized_at_cluster_version: subscription
                        .initialized_at_cluster_version
                        .clone(),
                    created_at_cluster_version: subscription.created_at_cluster_version.clone(),
                })
        })
        .collect())
}
