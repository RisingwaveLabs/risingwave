// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::error::Result;
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::{Source as ProstSource, Table as ProstTable, TableSourceInfo};
use risingwave_pb::plan::ColumnCatalog;
use risingwave_sqlparser::ast::{ColumnDef, ObjectName};

use super::create_source::make_prost_source;
use crate::binder::expr::bind_data_type;
use crate::catalog::{check_valid_column_name, row_id_column_desc};
use crate::optimizer::plan_node::StreamSource;
use crate::optimizer::property::{Distribution, Order};
use crate::optimizer::{PlanRef, PlanRoot};
use crate::session::{OptimizerContext, OptimizerContextRef, SessionImpl};

/// Binds the column schemas declared in CREATE statement into `ColumnCatalog`.
pub fn bind_sql_columns(columns: Vec<ColumnDef>) -> Result<Vec<ColumnCatalog>> {
    let column_descs = {
        let mut column_descs = Vec::with_capacity(columns.len() + 1);
        // Put the hidden row id column in the first column. This is used for PK.
        column_descs.push(row_id_column_desc());
        // Then user columns.
        for (i, column) in columns.into_iter().enumerate() {
            check_valid_column_name(&column.name.value)?;
            column_descs.push(ColumnDesc {
                data_type: bind_data_type(&column.data_type)?,
                column_id: ColumnId::new((i + 1) as i32),
                name: column.name.value,
                field_descs: vec![],
                type_name: "".to_string(),
            });
        }
        column_descs
    };

    let columns_catalog = column_descs
        .into_iter()
        .enumerate()
        .map(|(i, c)| ColumnCatalog {
            column_desc: c.to_protobuf().into(),
            is_hidden: i == 0, // the row id column is hidden
        })
        .collect_vec();
    Ok(columns_catalog)
}

pub(crate) fn gen_create_table_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
) -> Result<(PlanRef, ProstSource, ProstTable)> {
    let source = make_prost_source(
        session,
        table_name,
        Info::TableSource(TableSourceInfo {
            columns: bind_sql_columns(columns)?,
        }),
    )?;
    let (plan, table) = gen_materialized_source_plan(context, source.clone())?;
    Ok((plan, source, table))
}

/// Generate a stream plan with `StreamSource` + `StreamMaterialize`, it ressembles a
/// `CREATE MATERIALIZED VIEW AS SELECT * FROM <source>`.
pub(crate) fn gen_materialized_source_plan(
    context: OptimizerContextRef,
    source: ProstSource,
) -> Result<(PlanRef, ProstTable)> {
    let materialize = {
        // Manually assemble the materialization plan for the table.
        let source_node: PlanRef = StreamSource::create(
            context,
            vec![0], // row id column as pk
            source.clone(),
        )
        .into();
        let mut required_cols = FixedBitSet::with_capacity(source_node.schema().len());
        required_cols.toggle_range(..);
        required_cols.toggle(0);

        PlanRoot::new(
            source_node,
            Distribution::HashShard(vec![0]),
            Order::any().clone(),
            required_cols,
        )
        .gen_create_mv_plan(source.name.clone())?
    };
    let table = materialize
        .table()
        .to_prost(source.schema_id, source.database_id);

    Ok((materialize.into(), table))
}

pub async fn handle_create_table(
    context: OptimizerContext,
    table_name: ObjectName,
    columns: Vec<ColumnDef>,
) -> Result<PgResponse> {
    let session = context.session_ctx.clone();

    let (plan, source, table) = {
        let (plan, source, table) =
            gen_create_table_plan(&session, context.into(), table_name.clone(), columns)?;
        let plan = plan.to_stream_prost();

        (plan, source, table)
    };

    log::trace!(
        "name={}, plan=\n{}",
        table_name,
        serde_json::to_string_pretty(&plan).unwrap()
    );

    let catalog_writer = session.env().catalog_writer();
    catalog_writer
        .create_materialized_source(source, table, plan)
        .await?;

    Ok(PgResponse::empty_result(StatementType::CREATE_TABLE))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use risingwave_common::types::DataType;

    use crate::catalog::gen_row_id_column_name;
    use crate::test_utils::LocalFrontend;

    #[tokio::test]
    async fn test_create_table_handler() {
        let sql = "create table t (v1 smallint, v2 int, v3 bigint, v4 float, v5 double);";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader();

        // Check source exists.
        let source = catalog_reader
            .read_guard()
            .get_source_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap()
            .clone();
        assert_eq!(source.name, "t");

        // Check table exists.
        let table = catalog_reader
            .read_guard()
            .get_table_by_name(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap()
            .clone();
        assert_eq!(table.name(), "t");

        let columns = table
            .columns()
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let row_id_col_name = gen_row_id_column_name(0);
        let expected_columns = maplit::hashmap! {
            row_id_col_name.as_str() => DataType::Int64,
            "v1" => DataType::Int16,
            "v2" => DataType::Int32,
            "v3" => DataType::Int64,
            "v4" => DataType::Float64,
            "v5" => DataType::Float64,
        };

        assert_eq!(columns, expected_columns);
    }
}
