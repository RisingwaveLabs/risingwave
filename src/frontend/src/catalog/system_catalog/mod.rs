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

pub mod information_schema;
pub mod pg_catalog;
pub mod rw_catalog;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, Field, SysCatalogReader, TableDesc, TableId, DEFAULT_SUPER_USER_ID,
    NON_RESERVED_SYS_CATALOG_ID,
};
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_pb::user::UserInfo;

use crate::catalog::catalog_service::CatalogReader;
use crate::catalog::system_catalog::information_schema::*;
use crate::catalog::system_catalog::pg_catalog::*;
use crate::catalog::system_catalog::rw_catalog::*;
use crate::catalog::view_catalog::ViewCatalog;
use crate::meta_client::FrontendMetaClient;
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::session::AuthContext;
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
    // Read cluster info.
    worker_node_manager: WorkerNodeManagerRef,
    // Read from meta.
    meta_client: Arc<dyn FrontendMetaClient>,
    auth_context: Arc<AuthContext>,
}

impl SysCatalogReaderImpl {
    pub fn new(
        catalog_reader: CatalogReader,
        user_info_reader: UserInfoReader,
        worker_node_manager: WorkerNodeManagerRef,
        meta_client: Arc<dyn FrontendMetaClient>,
        auth_context: Arc<AuthContext>,
    ) -> Self {
        Self {
            catalog_reader,
            user_info_reader,
            worker_node_manager,
            meta_client,
            auth_context,
        }
    }
}

pub struct BuiltinTable {
    name: &'static str,
    schema: &'static str,
    columns: &'static [SystemCatalogColumnsDef<'static>],
    pk: &'static [usize],
}

pub struct BuiltinView {
    name: &'static str,
    schema: &'static str,
    columns: &'static [SystemCatalogColumnsDef<'static>],
    sql: String,
}

#[allow(dead_code)]
pub enum BuiltinCatalog {
    Table(&'static BuiltinTable),
    View(&'static BuiltinView),
}

impl BuiltinCatalog {
    fn name(&self) -> &'static str {
        match self {
            BuiltinCatalog::Table(t) => t.name,
            BuiltinCatalog::View(v) => v.name,
        }
    }
}

impl From<&BuiltinTable> for SystemTableCatalog {
    fn from(val: &BuiltinTable) -> Self {
        SystemTableCatalog {
            id: TableId::placeholder(),
            name: val.name.to_string(),
            columns: val
                .columns
                .iter()
                .enumerate()
                .map(|(idx, c)| ColumnCatalog {
                    column_desc: ColumnDesc::new_atomic(c.0.clone(), c.1, idx as i32),
                    is_hidden: false,
                })
                .collect(),
            pk: val.pk.to_vec(),
            owner: DEFAULT_SUPER_USER_ID,
        }
    }
}

impl From<&BuiltinView> for ViewCatalog {
    fn from(val: &BuiltinView) -> Self {
        ViewCatalog {
            id: 0,
            name: val.name.to_string(),
            columns: val
                .columns
                .iter()
                .map(|c| Field::with_name(c.0.clone(), c.1.to_string()))
                .collect(),
            sql: val.sql.to_string(),
            owner: DEFAULT_SUPER_USER_ID,
            properties: Default::default(),
        }
    }
}

// TODO: support struct column and type name when necessary.
pub(super) type SystemCatalogColumnsDef<'a> = (DataType, &'a str);

/// get acl items of `object` in string, ignore public.
fn get_acl_items(
    object: &Object,
    users: &Vec<UserInfo>,
    username_map: &HashMap<UserId, String>,
) -> String {
    let mut res = String::from("{");
    let mut empty_flag = true;
    let super_privilege = available_prost_privilege(object.clone());
    for user in users {
        let privileges = if user.get_is_super() {
            vec![&super_privilege]
        } else {
            user.get_grant_privileges()
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
                grantor_map.entry(ao.granted_by).or_insert_with(Vec::new);
                grantor_map
                    .get_mut(&ao.granted_by)
                    .unwrap()
                    .push((ao.action, ao.with_grant_option));
            })
        });
        for key in grantor_map.keys() {
            if empty_flag {
                empty_flag = false;
            } else {
                res.push(',');
            }
            res.push_str(user.get_name());
            res.push('=');
            grantor_map
                .get(key)
                .unwrap()
                .iter()
                .for_each(|(action, option)| {
                    let str = match Action::from_i32(*action).unwrap() {
                        Action::Select => "r",
                        Action::Insert => "a",
                        Action::Update => "w",
                        Action::Delete => "d",
                        Action::Create => "C",
                        Action::Connect => "c",
                        _ => unreachable!(),
                    };
                    res.push_str(str);
                    if *option {
                        res.push('*');
                    }
                });
            res.push('/');
            // should be able to query grantor's name
            res.push_str(username_map.get(key).as_ref().unwrap());
        }
    }
    res.push('}');
    res
}

pub struct SystemCatalog {
    table_by_schema_name: HashMap<&'static str, Vec<Arc<SystemTableCatalog>>>,
    table_name_by_id: HashMap<TableId, &'static str>,
    #[allow(dead_code)]
    view_by_schema_name: HashMap<&'static str, Vec<Arc<ViewCatalog>>>,
}

pub fn get_sys_tables_in_schema(schema_name: &str) -> Option<Vec<Arc<SystemTableCatalog>>> {
    SYS_CATALOGS
        .table_by_schema_name
        .get(schema_name)
        .map(Clone::clone)
}

#[allow(dead_code)]
pub fn get_sys_views_in_schema(schema_name: &str) -> Option<Vec<Arc<ViewCatalog>>> {
    SYS_CATALOGS
        .view_by_schema_name
        .get(schema_name)
        .map(Clone::clone)
}

macro_rules! prepare_sys_catalog {
    ($( { $builtin_catalog:expr $(, $func:ident $($await:ident)?)? } ),* $(,)?) => {
        pub(crate) static SYS_CATALOGS: LazyLock<SystemCatalog> = LazyLock::new(|| {
            let mut table_by_schema_name = HashMap::new();
            let mut table_name_by_id = HashMap::new();
            let mut view_by_schema_name = HashMap::new();
            $(
                let id = (${index()} + 1) as u32;
                match $builtin_catalog {
                    BuiltinCatalog::Table(table) => {
                        let sys_table: SystemTableCatalog = table.into();
                        table_by_schema_name.entry(table.schema).or_insert(vec![]).push(Arc::new(sys_table.with_id(id.into())));
                        table_name_by_id.insert(id.into(), table.name);
                    },
                    BuiltinCatalog::View(view) => {
                        let sys_view: ViewCatalog = view.into();
                        view_by_schema_name.entry(view.schema).or_insert(vec![]).push(Arc::new(sys_view.with_id(id)));
                    },
                }
            )*
            assert!(table_name_by_id.len() < NON_RESERVED_SYS_CATALOG_ID as usize, "too many system catalogs");

            SystemCatalog {
                table_by_schema_name,
                table_name_by_id,
                view_by_schema_name,
            }
        });

        #[async_trait]
        impl SysCatalogReader for SysCatalogReaderImpl {
            async fn read_table(&self, table_id: &TableId) -> Result<Vec<OwnedRow>> {
                let table_name = SYS_CATALOGS.table_name_by_id.get(table_id).unwrap();
                $(
                    if $builtin_catalog.name() == *table_name {
                        $(
                            let rows = self.$func();
                            $(let rows = rows.$await;)?
                            return rows;
                        )?
                    }
                )*
                unreachable!()
            }
        }
    }
}

// `prepare_sys_catalog!` macro is used to generate all builtin system catalogs.
prepare_sys_catalog! {
    { BuiltinCatalog::Table(&PG_TYPE), read_types },
    { BuiltinCatalog::Table(&PG_NAMESPACE), read_namespace },
    { BuiltinCatalog::Table(&PG_CAST), read_cast },
    { BuiltinCatalog::Table(&PG_MATVIEWS), read_mviews_info await },
    { BuiltinCatalog::Table(&PG_USER), read_user_info },
    { BuiltinCatalog::Table(&PG_CLASS), read_class_info },
    { BuiltinCatalog::Table(&PG_INDEX), read_index_info },
    { BuiltinCatalog::Table(&PG_OPCLASS), read_opclass_info },
    { BuiltinCatalog::Table(&PG_COLLATION), read_collation_info },
    { BuiltinCatalog::Table(&PG_AM), read_am_info },
    { BuiltinCatalog::Table(&PG_OPERATOR), read_operator_info },
    { BuiltinCatalog::Table(&PG_VIEWS), read_views_info },
    { BuiltinCatalog::Table(&PG_ATTRIBUTE), read_pg_attribute },
    { BuiltinCatalog::Table(&PG_DATABASE), read_database_info },
    { BuiltinCatalog::Table(&PG_DESCRIPTION), read_description_info },
    { BuiltinCatalog::Table(&PG_SETTINGS), read_settings_info },
    { BuiltinCatalog::Table(&PG_KEYWORDS), read_keywords_info },
    { BuiltinCatalog::Table(&PG_ATTRDEF), read_attrdef_info },
    { BuiltinCatalog::Table(&PG_ROLES), read_roles_info },
    { BuiltinCatalog::Table(&PG_SHDESCRIPTION), read_shdescription_info },
    { BuiltinCatalog::Table(&PG_TABLESPACE), read_tablespace_info },
    { BuiltinCatalog::Table(&PG_STAT_ACTIVITY), read_stat_activity },
    { BuiltinCatalog::Table(&PG_ENUM), read_enum_info },
    { BuiltinCatalog::Table(&PG_CONVERSION), read_conversion_info },
    { BuiltinCatalog::Table(&PG_INDEXES), read_indexes_info },
    { BuiltinCatalog::Table(&PG_INHERITS), read_inherits_info },
    { BuiltinCatalog::Table(&PG_CONSTRAINT), read_constraint_info },
    { BuiltinCatalog::Table(&PG_TABLES), read_pg_tables_info },
    { BuiltinCatalog::Table(&PG_PROC), read_pg_proc_info },
    { BuiltinCatalog::Table(&PG_SHADOW), read_user_info_shadow },
    { BuiltinCatalog::Table(&PG_LOCKS), read_locks_info },
    { BuiltinCatalog::Table(&INFORMATION_SCHEMA_COLUMNS), read_columns_info },
    { BuiltinCatalog::Table(&INFORMATION_SCHEMA_TABLES), read_tables_info },
    { BuiltinCatalog::Table(&RW_DATABASES), read_rw_database_info },
    { BuiltinCatalog::Table(&RW_SCHEMAS), read_rw_schema_info },
    { BuiltinCatalog::Table(&RW_USERS), read_rw_user_info },
    { BuiltinCatalog::Table(&RW_TABLES), read_rw_table_info },
    { BuiltinCatalog::Table(&RW_MATERIALIZED_VIEWS), read_rw_mview_info },
    { BuiltinCatalog::Table(&RW_INDEXES), read_rw_indexes_info },
    { BuiltinCatalog::Table(&RW_SOURCES), read_rw_sources_info },
    { BuiltinCatalog::Table(&RW_SINKS), read_rw_sinks_info },
    { BuiltinCatalog::Table(&RW_CONNECTIONS), read_rw_connections_info },
    { BuiltinCatalog::Table(&RW_FUNCTIONS), read_rw_functions_info },
    { BuiltinCatalog::Table(&RW_VIEWS), read_rw_views_info },
    { BuiltinCatalog::Table(&RW_WORKER_NODES), read_rw_worker_nodes_info },
    { BuiltinCatalog::Table(&RW_PARALLEL_UNITS), read_rw_parallel_units_info },
    { BuiltinCatalog::Table(&RW_TABLE_FRAGMENTS), read_rw_table_fragments_info await },
    { BuiltinCatalog::Table(&RW_FRAGMENTS), read_rw_fragment_distributions_info await },
    { BuiltinCatalog::Table(&RW_ACTORS), read_rw_actor_states_info await },
    { BuiltinCatalog::Table(&RW_META_SNAPSHOT), read_meta_snapshot await },
    { BuiltinCatalog::Table(&RW_DDL_PROGRESS), read_ddl_progress await },
    { BuiltinCatalog::Table(&RW_TABLE_STATS), read_table_stats },
    { BuiltinCatalog::Table(&RW_RELATION_INFO), read_relation_info await },
}
