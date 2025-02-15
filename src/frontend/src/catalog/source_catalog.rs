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

use anyhow::Context as _;
use itertools::Itertools as _;
use risingwave_common::catalog::{ColumnCatalog, SourceVersionId};
use risingwave_common::util::epoch::Epoch;
use risingwave_connector::{WithOptionsSecResolved, WithPropertiesExt};
use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use risingwave_pb::catalog::{PbSource, StreamSourceInfo, WatermarkDesc};
use risingwave_sqlparser::ast;
use risingwave_sqlparser::parser::Parser;
use thiserror_ext::AsReport as _;

use super::purify::try_purify_table_source_create_sql_ast;
use super::{ColumnId, ConnectionId, DatabaseId, OwnedByUserCatalog, SchemaId, SourceId};
use crate::catalog::TableId;
use crate::error::Result;
use crate::session::current::notice_to_user;
use crate::user::UserId;

/// This struct `SourceCatalog` is used in frontend.
/// Compared with `PbSource`, it only maintains information used during optimization.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SourceCatalog {
    pub id: SourceId,
    pub name: String,
    pub schema_id: SchemaId,
    pub database_id: DatabaseId,
    pub columns: Vec<ColumnCatalog>,
    pub pk_col_ids: Vec<ColumnId>,
    pub append_only: bool,
    pub owner: UserId,
    pub info: StreamSourceInfo,
    pub row_id_index: Option<usize>,
    pub with_properties: WithOptionsSecResolved,
    pub watermark_descs: Vec<WatermarkDesc>,
    pub associated_table_id: Option<TableId>,
    pub definition: String,
    pub connection_id: Option<ConnectionId>,
    pub created_at_epoch: Option<Epoch>,
    pub initialized_at_epoch: Option<Epoch>,
    pub version: SourceVersionId,
    pub created_at_cluster_version: Option<String>,
    pub initialized_at_cluster_version: Option<String>,
    pub rate_limit: Option<u32>,
}

impl SourceCatalog {
    /// Returns the SQL definition when the source was created.
    pub fn create_sql(&self) -> String {
        self.definition.clone()
    }

    /// Returns the parsed SQL definition when the source was created.
    ///
    /// Returns error if it's invalid.
    pub fn create_sql_ast(&self) -> Result<ast::Statement> {
        Ok(Parser::parse_sql(&self.definition)
            .context("unable to parse definition sql")?
            .into_iter()
            .exactly_one()
            .context("expecting exactly one statement in definition")?)
    }

    pub fn to_prost(&self) -> PbSource {
        let (with_properties, secret_refs) = self.with_properties.clone().into_parts();
        PbSource {
            id: self.id,
            schema_id: self.schema_id,
            database_id: self.database_id,
            name: self.name.clone(),
            row_id_index: self.row_id_index.map(|idx| idx as _),
            columns: self.columns.iter().map(|c| c.to_protobuf()).collect(),
            pk_column_ids: self.pk_col_ids.iter().map(Into::into).collect(),
            with_properties,
            owner: self.owner,
            info: Some(self.info.clone()),
            watermark_descs: self.watermark_descs.clone(),
            definition: self.definition.clone(),
            connection_id: self.connection_id,
            initialized_at_epoch: self.initialized_at_epoch.map(|x| x.0),
            created_at_epoch: self.created_at_epoch.map(|x| x.0),
            optional_associated_table_id: self
                .associated_table_id
                .map(|id| OptionalAssociatedTableId::AssociatedTableId(id.table_id)),
            version: self.version,
            created_at_cluster_version: self.created_at_cluster_version.clone(),
            initialized_at_cluster_version: self.initialized_at_cluster_version.clone(),
            secret_refs,
            rate_limit: self.rate_limit,
        }
    }

    /// Get a reference to the source catalog's version.
    pub fn version(&self) -> SourceVersionId {
        self.version
    }

    pub fn connector_name(&self) -> String {
        self.with_properties
            .get_connector()
            .expect("connector name is missing")
    }
}

impl SourceCatalog {
    /// Returns the SQL definition when the source was created, purified with best effort.
    pub fn create_sql_purified(&self) -> String {
        self.create_sql_ast_purified()
            .map(|stmt| stmt.to_string())
            .unwrap_or_else(|_| self.create_sql())
    }

    /// Returns the parsed SQL definition when the source was created, purified with best effort.
    ///
    /// Returns error if it's invalid.
    pub fn create_sql_ast_purified(&self) -> Result<ast::Statement> {
        match try_purify_table_source_create_sql_ast(
            self.create_sql_ast()?,
            &self.columns,
            self.row_id_index,
            &self.pk_col_ids,
        ) {
            Ok(stmt) => return Ok(stmt),
            Err(e) => notice_to_user(format!(
                "error occurred while purifying definition for source \"{}\", \
                     results may be inaccurate: {}",
                self.name,
                e.as_report()
            )),
        }

        self.create_sql_ast()
    }

    /// Fills the `definition` field with the purified SQL definition.
    ///
    /// There's no need to call this method for correctness because we automatically purify the
    /// SQL definition at the time of querying. However, this helps to maintain more accurate
    /// `definition` field in the catalog when directly inspected for debugging purposes.
    pub fn fill_purified_create_sql(&mut self) {
        self.definition = self.create_sql_purified();
    }
}

impl From<&PbSource> for SourceCatalog {
    fn from(prost: &PbSource) -> Self {
        let id = prost.id;
        let name = prost.name.clone();
        let database_id = prost.database_id;
        let schema_id = prost.schema_id;
        let prost_columns = prost.columns.clone();
        let pk_col_ids = prost
            .pk_column_ids
            .clone()
            .into_iter()
            .map(Into::into)
            .collect();
        let connector_props_with_secrets =
            WithOptionsSecResolved::new(prost.with_properties.clone(), prost.secret_refs.clone());
        let columns = prost_columns.into_iter().map(ColumnCatalog::from).collect();
        let row_id_index = prost.row_id_index.map(|idx| idx as _);

        let append_only = row_id_index.is_some();
        let owner = prost.owner;
        let watermark_descs = prost.get_watermark_descs().clone();

        let associated_table_id = prost.optional_associated_table_id.map(|id| match id {
            OptionalAssociatedTableId::AssociatedTableId(id) => id,
        });
        let version = prost.version;

        let connection_id = prost.connection_id;
        let rate_limit = prost.rate_limit;

        Self {
            id,
            name,
            schema_id,
            database_id,
            columns,
            pk_col_ids,
            append_only,
            owner,
            info: prost.info.clone().unwrap(),
            row_id_index,
            with_properties: connector_props_with_secrets,
            watermark_descs,
            associated_table_id: associated_table_id.map(|x| x.into()),
            definition: prost.definition.clone(),
            connection_id,
            created_at_epoch: prost.created_at_epoch.map(Epoch::from),
            initialized_at_epoch: prost.initialized_at_epoch.map(Epoch::from),
            version,
            created_at_cluster_version: prost.created_at_cluster_version.clone(),
            initialized_at_cluster_version: prost.initialized_at_cluster_version.clone(),
            rate_limit,
        }
    }
}

impl OwnedByUserCatalog for SourceCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}
