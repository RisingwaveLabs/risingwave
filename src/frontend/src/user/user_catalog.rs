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

use risingwave_common::acl::{AclMode, AclModeSet};
use risingwave_pb::user::grant_privilege::{Action, Object as GrantObject, Object};
use risingwave_pb::user::{PbAuthInfo, PbGrantPrivilege, PbUserInfo};

use crate::catalog::{DatabaseId, SchemaId};
use crate::user::UserId;

/// `UserCatalog` is responsible for managing user's information.
#[derive(Clone, Debug)]
pub struct UserCatalog {
    pub id: UserId,
    pub name: String,
    pub is_super: bool,
    pub can_create_db: bool,
    pub can_create_user: bool,
    pub can_login: bool,
    pub auth_info: Option<PbAuthInfo>,
    pub grant_privileges: Vec<PbGrantPrivilege>,

    // User owned acl mode set, group by object id.
    // TODO: merge it after we fully migrate to sql-backend.
    pub database_acls: HashMap<DatabaseId, AclModeSet>,
    pub schema_acls: HashMap<SchemaId, AclModeSet>,
    pub object_acls: HashMap<u32, AclModeSet>,
}

impl From<PbUserInfo> for UserCatalog {
    fn from(user: PbUserInfo) -> Self {
        let mut user_catalog = Self {
            id: user.id,
            name: user.name,
            is_super: user.is_super,
            can_create_db: user.can_create_db,
            can_create_user: user.can_create_user,
            can_login: user.can_login,
            auth_info: user.auth_info,
            grant_privileges: user.grant_privileges,
            database_acls: Default::default(),
            schema_acls: Default::default(),
            object_acls: Default::default(),
        };
        user_catalog.refresh_acl_modes();

        user_catalog
    }
}

impl UserCatalog {
    pub fn to_prost(&self) -> PbUserInfo {
        PbUserInfo {
            id: self.id,
            name: self.name.clone(),
            is_super: self.is_super,
            can_create_db: self.can_create_db,
            can_create_user: self.can_create_user,
            can_login: self.can_login,
            auth_info: self.auth_info.clone(),
            grant_privileges: self.grant_privileges.clone(),
        }
    }

    fn get_acl_entry(&mut self, object: GrantObject) -> Entry<'_, u32, AclModeSet> {
        match object {
            Object::DatabaseId(id) => self.database_acls.entry(id),
            Object::SchemaId(id) => self.schema_acls.entry(id),
            Object::TableId(id) => self.object_acls.entry(id),
            Object::SourceId(id) => self.object_acls.entry(id),
            Object::SinkId(id) => self.object_acls.entry(id),
            Object::ViewId(id) => self.object_acls.entry(id),
            Object::FunctionId(_) => {
                unreachable!("grant privilege on function is not supported yet.")
            }
            _ => unreachable!(""),
        }
    }

    fn get_acl(&self, object: &GrantObject) -> Option<&AclModeSet> {
        match object {
            Object::DatabaseId(id) => self.database_acls.get(id),
            Object::SchemaId(id) => self.schema_acls.get(id),
            Object::TableId(id) => self.object_acls.get(id),
            Object::SourceId(id) => self.object_acls.get(id),
            Object::SinkId(id) => self.object_acls.get(id),
            Object::ViewId(id) => self.object_acls.get(id),
            Object::FunctionId(_) => {
                unreachable!("grant privilege on function is not supported yet.")
            }
            _ => unreachable!("unexpected object type."),
        }
    }

    fn refresh_acl_modes(&mut self) {
        self.database_acls.clear();
        self.schema_acls.clear();
        self.object_acls.clear();
        let privileges = self.grant_privileges.clone();
        for privilege in privileges {
            let entry = self
                .get_acl_entry(privilege.object.unwrap())
                .or_insert(AclModeSet::empty());
            for awo in privilege.action_with_opts {
                entry
                    .modes
                    .insert::<AclMode>(awo.get_action().unwrap().into());
            }
        }
    }

    // Only for test, used in `MockUserInfoWriter`.
    pub fn extend_privileges(&mut self, privileges: Vec<PbGrantPrivilege>) {
        self.grant_privileges.extend(privileges);
        self.refresh_acl_modes();
    }

    // Only for test, used in `MockUserInfoWriter`.
    pub fn revoke_privileges(
        &mut self,
        privileges: Vec<PbGrantPrivilege>,
        revoke_grant_option: bool,
    ) {
        self.grant_privileges.iter_mut().for_each(|p| {
            for rp in &privileges {
                if rp.object != p.object {
                    continue;
                }
                if revoke_grant_option {
                    for ao in &mut p.action_with_opts {
                        if rp
                            .action_with_opts
                            .iter()
                            .any(|rao| rao.action == ao.action)
                        {
                            ao.with_grant_option = false;
                        }
                    }
                } else {
                    p.action_with_opts.retain(|po| {
                        rp.action_with_opts
                            .iter()
                            .all(|rao| rao.action != po.action)
                    });
                }
            }
        });
        self.grant_privileges
            .retain(|p| !p.action_with_opts.is_empty());
        self.refresh_acl_modes();
    }

    pub fn check_privilege(&self, object: &GrantObject, mode: AclMode) -> bool {
        self.get_acl(object)
            .map_or(false, |acl_set| acl_set.has_mode(mode))
    }

    pub fn check_privilege_with_grant_option(
        &self,
        object: &GrantObject,
        actions: &Vec<(Action, bool)>,
    ) -> bool {
        if self.is_super {
            return true;
        }
        let mut action_map: HashMap<_, _> = actions.iter().map(|action| (action, false)).collect();

        for privilege in &self.grant_privileges {
            if privilege.get_object().unwrap() != object {
                continue;
            }
            for awo in &privilege.action_with_opts {
                let action = awo.get_action().unwrap();
                let with_grant_option = awo.with_grant_option;

                for (&key, found) in &mut action_map {
                    let (required_action, required_grant_option) = *key;
                    if action == required_action && (!required_grant_option | with_grant_option) {
                        *found = true;
                    }
                }
            }
        }
        action_map.values().all(|&found| found)
    }

    pub fn check_object_visibility(&self, obj_id: u32) -> bool {
        if self.is_super {
            return true;
        }

        // `Select` and `Execute` are the minimum required privileges for object visibility.
        // `Execute` is required for functions.
        self.object_acls.get(&obj_id).map_or(false, |acl_set| {
            acl_set.has_mode(AclMode::Select) || acl_set.has_mode(AclMode::Execute)
        })
    }
}
