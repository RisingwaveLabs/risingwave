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

pub mod information_schema;
pub mod pg_catalog;
pub mod rw_catalog;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use futures::stream::BoxStream;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::acl::AclMode;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, Field, SysCatalogReader, TableDesc, TableId, DEFAULT_SUPER_USER_ID,
    MAX_SYS_CATALOG_NUM, SYS_CATALOG_START_ID,
};
use risingwave_common::error::BoxedError;
use risingwave_common::session_config::SessionConfig;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::types::DataType;
use risingwave_pb::meta::list_streaming_job_states_response::StreamingJobState;
use risingwave_pb::meta::table_parallelism::{PbFixedParallelism, PbParallelism};
use risingwave_pb::user::grant_privilege::Object as GrantObject;

use crate::catalog::catalog_service::CatalogReader;
use crate::catalog::view_catalog::ViewCatalog;
use crate::meta_client::FrontendMetaClient;
use crate::session::AuthContext;
use crate::user::user_catalog::UserCatalog;
use crate::user::user_privilege::available_prost_privilege;
use crate::user::user_service::UserInfoReader;
use crate::user::UserId;

#[derive(Clone, Debug, PartialEq)]
pub struct SystemTableCatalog {
    pub id: TableId,

    pub name: String,

    // All columns in this table.
    pub columns: Vec<ColumnCatalog>,

    /// Primary key columns indices.
    pub pk: Vec<usize>,

    // owner of table, should always be default super user, keep it for compatibility.
    pub owner: u32,

    /// description of table, set by `comment on`.
    pub description: Option<String>,
}

impl SystemTableCatalog {
    /// Get a reference to the system catalog's table id.
    pub fn id(&self) -> TableId {
        self.id
    }

    pub fn with_id(mut self, id: TableId) -> Self {
        self.id = id;
        self
    }

    /// Get a reference to the system catalog's columns.
    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    /// Get a [`TableDesc`] of the system table.
    pub fn table_desc(&self) -> TableDesc {
        TableDesc {
            table_id: self.id,
            columns: self.columns.iter().map(|c| c.column_desc.clone()).collect(),
            stream_key: self.pk.clone(),
            ..Default::default()
        }
    }

    /// Get a reference to the system catalog's name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }
}

pub struct SysCatalogReaderImpl {
    // Read catalog info: database/schema/source/table.
    catalog_reader: CatalogReader,
    // Read user info.
    user_info_reader: UserInfoReader,
    // Read from meta.
    meta_client: Arc<dyn FrontendMetaClient>,
    // Read auth context.
    auth_context: Arc<AuthContext>,
    // Read config.
    config: Arc<RwLock<SessionConfig>>,
    // Read system params.
    system_params: SystemParamsReaderRef,
}

impl SysCatalogReaderImpl {
    pub fn new(
        catalog_reader: CatalogReader,
        user_info_reader: UserInfoReader,
        meta_client: Arc<dyn FrontendMetaClient>,
        auth_context: Arc<AuthContext>,
        config: Arc<RwLock<SessionConfig>>,
        system_params: SystemParamsReaderRef,
    ) -> Self {
        Self {
            catalog_reader,
            user_info_reader,
            meta_client,
            auth_context,
            config,
            system_params,
        }
    }
}

pub struct BuiltinTable {
    name: &'static str,
    schema: &'static str,
    columns: Vec<SystemCatalogColumnsDef<'static>>,
    pk: &'static [usize],
    function: for<'a> fn(&'a SysCatalogReaderImpl) -> BoxStream<'a, Result<DataChunk, BoxedError>>,
}

pub struct BuiltinView {
    name: &'static str,
    schema: &'static str,
    columns: Vec<SystemCatalogColumnsDef<'static>>,
    sql: String,
}

pub enum BuiltinCatalog {
    Table(BuiltinTable),
    View(BuiltinView),
}

impl BuiltinCatalog {
    fn full_name(&self) -> String {
        match self {
            BuiltinCatalog::Table(t) => format!("{}.{}", t.schema, t.name),
            BuiltinCatalog::View(t) => format!("{}.{}", t.schema, t.name),
        }
    }
}

impl From<&BuiltinTable> for SystemTableCatalog {
    fn from(val: &BuiltinTable) -> Self {
        SystemTableCatalog {
            id: TableId::placeholder(),
            name: val.name.to_owned(),
            columns: val
                .columns
                .iter()
                .enumerate()
                .map(|(idx, (name, ty))| ColumnCatalog {
                    column_desc: ColumnDesc::new_atomic(ty.clone(), name, idx as i32),
                    is_hidden: false,
                })
                .collect(),
            pk: val.pk.to_vec(),
            owner: DEFAULT_SUPER_USER_ID,
            description: None,
        }
    }
}

impl From<&BuiltinView> for ViewCatalog {
    fn from(val: &BuiltinView) -> Self {
        ViewCatalog {
            id: 0,
            name: val.name.to_owned(),
            columns: val
                .columns
                .iter()
                .map(|(name, ty)| Field::with_name(ty.clone(), name.to_string()))
                .collect(),
            sql: val.sql.clone(),
            owner: DEFAULT_SUPER_USER_ID,
            properties: Default::default(),
        }
    }
}

// TODO: support struct column and type name when necessary.
pub(super) type SystemCatalogColumnsDef<'a> = (&'a str, DataType);

/// `infer_dummy_view_sql` returns a dummy SQL statement for a view with the given columns that
/// returns no rows. For example, with columns `a` and `b`, it returns `SELECT NULL::integer AS a,
/// NULL::varchar AS b WHERE 1 != 1`.
// FIXME(noel): Tracked by <https://github.com/risingwavelabs/risingwave/issues/3431#issuecomment-1164160988>
#[inline(always)]
pub fn infer_dummy_view_sql(columns: &[SystemCatalogColumnsDef<'_>]) -> String {
    format!(
        "SELECT {} WHERE 1 != 1",
        columns
            .iter()
            .map(|(name, ty)| format!("NULL::{} AS {}", ty, name))
            .join(", ")
    )
}

fn extract_parallelism_from_table_state(state: &StreamingJobState) -> String {
    match state
        .parallelism
        .as_ref()
        .and_then(|parallelism| parallelism.parallelism.as_ref())
    {
        Some(PbParallelism::Auto(_)) | Some(PbParallelism::Adaptive(_)) => "adaptive".to_owned(),
        Some(PbParallelism::Fixed(PbFixedParallelism { parallelism })) => {
            format!("fixed({parallelism})")
        }
        Some(PbParallelism::Custom(_)) => "custom".to_owned(),
        None => "unknown".to_owned(),
    }
}

/// get acl items of `object` in string, ignore public.
fn get_acl_items(
    object: &GrantObject,
    for_dml_table: bool,
    users: &Vec<UserCatalog>,
    username_map: &HashMap<UserId, String>,
) -> Vec<String> {
    let mut res = vec![];
    let super_privilege = available_prost_privilege(*object, for_dml_table);
    for user in users {
        let privileges = if user.is_super {
            vec![&super_privilege]
        } else {
            user.grant_privileges
                .iter()
                .filter(|&privilege| privilege.object.as_ref().unwrap() == object)
                .collect_vec()
        };
        if privileges.is_empty() {
            continue;
        };
        let mut grantor_map = HashMap::new();
        privileges.iter().for_each(|&privilege| {
            privilege.action_with_opts.iter().for_each(|ao| {
                grantor_map
                    .entry(ao.granted_by)
                    .or_insert_with(Vec::new)
                    .push((ao.get_action().unwrap(), ao.with_grant_option));
            })
        });
        for (granted_by, actions) in grantor_map {
            let mut aclitem = String::new();
            aclitem.push_str(&user.name);
            aclitem.push('=');
            for (action, option) in actions {
                aclitem.push_str(&AclMode::from(action).to_string());
                if option {
                    aclitem.push('*');
                }
            }
            aclitem.push('/');
            // should be able to query grantor's name
            aclitem.push_str(username_map.get(&granted_by).unwrap());
            res.push(aclitem);
        }
    }
    res
}

pub struct SystemCatalog {
    // table id = index + SYS_CATALOG_START_ID
    catalogs: Vec<BuiltinCatalog>,
}

pub fn get_sys_tables_in_schema(schema_name: &str) -> Vec<Arc<SystemTableCatalog>> {
    SYS_CATALOGS
        .catalogs
        .iter()
        .enumerate()
        .filter_map(|(idx, c)| match c {
            BuiltinCatalog::Table(t) if t.schema == schema_name => Some(Arc::new(
                SystemTableCatalog::from(t)
                    .with_id((idx as u32 + SYS_CATALOG_START_ID as u32).into()),
            )),
            _ => None,
        })
        .collect()
}

pub fn get_sys_views_in_schema(schema_name: &str) -> Vec<Arc<ViewCatalog>> {
    SYS_CATALOGS
        .catalogs
        .iter()
        .enumerate()
        .filter_map(|(idx, c)| match c {
            BuiltinCatalog::View(v) if v.schema == schema_name => Some(Arc::new(
                ViewCatalog::from(v).with_id(idx as u32 + SYS_CATALOG_START_ID as u32),
            )),
            _ => None,
        })
        .collect()
}

pub fn is_system_catalog(oid: u32) -> bool {
    oid >= SYS_CATALOG_START_ID as u32
}

/// The global registry of all builtin catalogs.
pub static SYS_CATALOGS: LazyLock<SystemCatalog> = LazyLock::new(|| {
    tracing::info!("found {} catalogs", SYS_CATALOGS_SLICE.len());
    assert!(SYS_CATALOGS_SLICE.len() <= MAX_SYS_CATALOG_NUM as usize);
    let catalogs = SYS_CATALOGS_SLICE
        .iter()
        .map(|f| f())
        .sorted_by_key(|c| c.full_name())
        .collect();
    SystemCatalog { catalogs }
});

#[linkme::distributed_slice]
pub static SYS_CATALOGS_SLICE: [fn() -> BuiltinCatalog];

impl SysCatalogReader for SysCatalogReaderImpl {
    fn read_table(&self, table_id: TableId) -> BoxStream<'_, Result<DataChunk, BoxedError>> {
        let table_name = SYS_CATALOGS
            .catalogs
            .get((table_id.table_id - SYS_CATALOG_START_ID as u32) as usize)
            .unwrap();
        match table_name {
            BuiltinCatalog::Table(t) => (t.function)(self),
            BuiltinCatalog::View(_) => panic!("read_table should not be called on a view"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::system_catalog::SYS_CATALOGS;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_builtin_view_definition() {
        let frontend = LocalFrontend::new(Default::default()).await;
        let sqls = SYS_CATALOGS.catalogs.iter().filter_map(|c| match c {
            super::BuiltinCatalog::View(v) => Some(v.sql.clone()),
            _ => None,
        });
        for sql in sqls {
            frontend.query_formatted_result(sql).await;
        }
    }
}
