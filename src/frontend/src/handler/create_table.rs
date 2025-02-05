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

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use clap::ValueEnum;
use either::Either;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pgwire::pg_response::{PgResponse, StatementType};
use prost::Message as _;
use risingwave_common::catalog::{
    CdcTableDesc, ColumnCatalog, ColumnDesc, ConflictBehavior, Engine, FieldLike, TableId,
    TableVersionId, DEFAULT_SCHEMA_NAME, INITIAL_TABLE_VERSION_ID, RISINGWAVE_ICEBERG_ROW_ID,
    ROWID_PREFIX,
};
use risingwave_common::config::MetaBackend;
use risingwave_common::license::Feature;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_common::{bail, bail_not_implemented};
use risingwave_connector::jvm_runtime::JVM;
use risingwave_connector::sink::decouple_checkpoint_log_sink::COMMIT_CHECKPOINT_INTERVAL;
use risingwave_connector::source::cdc::build_cdc_table_id;
use risingwave_connector::source::cdc::external::{
    ExternalTableConfig, ExternalTableImpl, DATABASE_NAME_KEY, SCHEMA_NAME_KEY, TABLE_NAME_KEY,
};
use risingwave_connector::{source, WithOptionsSecResolved};
use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use risingwave_pb::catalog::{PbSource, PbTable, PbWebhookSourceInfo, Table, WatermarkDesc};
use risingwave_pb::ddl_service::TableJobType;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::{
    AdditionalColumn, ColumnDescVersion, DefaultColumnDesc, GeneratedColumnDesc,
};
use risingwave_pb::secret::secret_ref::PbRefAsType;
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_sqlparser::ast::{
    CdcTableInfo, ColumnDef, ColumnOption, CompatibleFormatEncode, CreateSink, CreateSinkStatement,
    CreateSourceStatement, DataType as AstDataType, ExplainOptions, Format, FormatEncodeOptions,
    Ident, ObjectName, OnConflict, SecretRefAsType, SourceWatermark, Statement, TableConstraint,
    WebhookSourceInfo, WithProperties,
};
use risingwave_sqlparser::parser::{IncludeOption, Parser};
use thiserror_ext::AsReport;

use super::create_source::{bind_columns_from_source, CreateSourceType, SqlColumnStrategy};
use super::{create_sink, create_source, RwPgResponse};
use crate::binder::{bind_data_type, bind_struct_field, Clause, SecureCompareContext};
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::table_catalog::{TableVersion, ICEBERG_SINK_PREFIX, ICEBERG_SOURCE_PREFIX};
use crate::catalog::{check_valid_column_name, ColumnId, DatabaseId, SchemaId};
use crate::error::{bail_bind_error, ErrorCode, Result, RwError};
use crate::expr::{Expr, ExprImpl, ExprRewriter};
use crate::handler::create_source::{
    bind_connector_props, bind_create_source_or_table_with_connector, bind_source_watermark,
    handle_addition_columns, UPSTREAM_SOURCE_KEY,
};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::generic::{CdcScanOptions, SourceNodeKind};
use crate::optimizer::plan_node::{LogicalCdcScan, LogicalSource};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{OptimizerContext, OptimizerContextRef, PlanRef, PlanRoot};
use crate::session::SessionImpl;
use crate::stream_fragmenter::build_graph;
use crate::utils::OverwriteOptions;
use crate::{Binder, TableCatalog, WithOptions};

/// Column ID generator for a new table or a new version of an existing table to alter.
#[derive(Debug)]
pub struct ColumnIdGenerator {
    /// Existing column names and their IDs and data types.
    ///
    /// This is used for aligning column IDs between versions (`ALTER`s). If a column already
    /// exists and the data type matches, its ID is reused. Otherwise, a new ID is generated.
    ///
    /// For a new table, this is empty.
    pub existing: HashMap<String, (ColumnId, DataType)>,

    /// The next column ID to generate, used for new columns that do not exist in `existing`.
    pub next_column_id: ColumnId,

    /// The version ID of the table to be created or altered.
    ///
    /// For a new table, this is 0. For altering an existing table, this is the **next** version ID
    /// of the `version_id` field in the original table catalog.
    pub version_id: TableVersionId,
}

impl ColumnIdGenerator {
    /// Creates a new [`ColumnIdGenerator`] for altering an existing table.
    pub fn new_alter(original: &TableCatalog) -> Self {
        let existing = original
            .columns()
            .iter()
            .map(|col| {
                (
                    col.name().to_owned(),
                    (col.column_id(), col.data_type().clone()),
                )
            })
            .collect();

        let version = original.version().expect("version field not set");

        Self {
            existing,
            next_column_id: version.next_column_id,
            version_id: version.version_id + 1,
        }
    }

    /// Creates a new [`ColumnIdGenerator`] for a new table.
    pub fn new_initial() -> Self {
        Self {
            existing: HashMap::new(),
            next_column_id: ColumnId::first_user_column(),
            version_id: INITIAL_TABLE_VERSION_ID,
        }
    }

    /// Generates a new [`ColumnId`] for a column with the given field.
    ///
    /// Returns an error if the data type of the column has been changed.
    pub fn generate(&mut self, field: impl FieldLike) -> Result<ColumnId> {
        if let Some((id, original_type)) = self.existing.get(field.name()) {
            // Intentionally not using `datatype_equals` here because we want nested types to be
            // exactly the same, **NOT** ignoring field names as they may be referenced in expressions
            // of generated columns or downstream jobs.
            // TODO: support compatible changes on types, typically for `STRUCT` types.
            //       https://github.com/risingwavelabs/risingwave/issues/19755
            if original_type == field.data_type() {
                Ok(*id)
            } else {
                bail_not_implemented!(
                    "The data type of column \"{}\" has been changed from \"{}\" to \"{}\". \
                     This is currently not supported, even if it could be a compatible change in external systems.",
                    field.name(),
                    original_type,
                    field.data_type()
                );
            }
        } else {
            let id = self.next_column_id;
            self.next_column_id = self.next_column_id.next();
            Ok(id)
        }
    }

    /// Consume this generator and return a [`TableVersion`] for the table to be created or altered.
    pub fn into_version(self) -> TableVersion {
        TableVersion {
            version_id: self.version_id,
            next_column_id: self.next_column_id,
        }
    }
}

fn ensure_column_options_supported(c: &ColumnDef) -> Result<()> {
    for option_def in &c.options {
        match option_def.option {
            ColumnOption::GeneratedColumns(_) => {}
            ColumnOption::DefaultValue(_) => {}
            ColumnOption::DefaultValueInternal { .. } => {}
            ColumnOption::Unique { is_primary: true } => {}
            _ => bail_not_implemented!("column constraints \"{}\"", option_def),
        }
    }
    Ok(())
}

/// Binds the column schemas declared in CREATE statement into `ColumnDesc`.
/// If a column is marked as `primary key`, its `ColumnId` is also returned.
/// This primary key is not combined with table constraints yet.
pub fn bind_sql_columns(column_defs: &[ColumnDef]) -> Result<Vec<ColumnCatalog>> {
    let mut columns = Vec::with_capacity(column_defs.len());

    for column in column_defs {
        ensure_column_options_supported(column)?;
        // Destruct to make sure all fields are properly handled rather than ignored.
        // Do NOT use `..` to ignore fields you do not want to deal with.
        // Reject them with a clear NotImplemented error.
        let ColumnDef {
            name,
            data_type,
            collation,
            ..
        } = column;

        let data_type = data_type
            .clone()
            .ok_or_else(|| ErrorCode::InvalidInputSyntax("data type is not specified".into()))?;
        if let Some(collation) = collation {
            // PostgreSQL will limit the datatypes that collate can work on.
            // https://www.postgresql.org/docs/16/collation.html#COLLATION-CONCEPTS
            //   > The built-in collatable data types are `text`, `varchar`, and `char`.
            //
            // But we don't support real collation, we simply ignore it here.
            if !["C", "POSIX"].contains(&collation.real_value().as_str()) {
                bail_not_implemented!(
                    "Collate collation other than `C` or `POSIX` is not implemented"
                );
            }

            match data_type {
                AstDataType::Text | AstDataType::Varchar | AstDataType::Char(_) => {}
                _ => {
                    return Err(ErrorCode::NotSupported(
                        format!("{} is not a collatable data type", data_type),
                        "The only built-in collatable data types are `varchar`, please check your type".into(),
                    ).into());
                }
            }
        }

        check_valid_column_name(&name.real_value())?;

        let field_descs: Vec<ColumnDesc> = if let AstDataType::Struct(fields) = &data_type {
            fields
                .iter()
                .map(bind_struct_field)
                .collect::<Result<Vec<_>>>()?
        } else {
            vec![]
        };
        columns.push(ColumnCatalog {
            column_desc: ColumnDesc {
                data_type: bind_data_type(&data_type)?,
                column_id: ColumnId::placeholder(),
                name: name.real_value(),
                field_descs,
                type_name: "".to_owned(),
                generated_or_default_column: None,
                description: None,
                additional_column: AdditionalColumn { column_type: None },
                version: ColumnDescVersion::Pr13707,
                system_column: None,
            },
            is_hidden: false,
        });
    }

    Ok(columns)
}

fn check_generated_column_constraints(
    column_name: &String,
    column_id: ColumnId,
    expr: &ExprImpl,
    column_catalogs: &[ColumnCatalog],
    generated_column_names: &[String],
    pk_column_ids: &[ColumnId],
) -> Result<()> {
    let input_refs = expr.collect_input_refs(column_catalogs.len());
    for idx in input_refs.ones() {
        let referred_generated_column = &column_catalogs[idx].column_desc.name;
        if generated_column_names
            .iter()
            .any(|c| c == referred_generated_column)
        {
            return Err(ErrorCode::BindError(format!(
                "Generated can not reference another generated column. \
                But here generated column \"{}\" referenced another generated column \"{}\"",
                column_name, referred_generated_column
            ))
            .into());
        }
    }

    if pk_column_ids.contains(&column_id) && expr.is_impure() {
        return Err(ErrorCode::BindError(format!(
            "Generated columns with impure expressions should not be part of the primary key. \
            Here column \"{}\" is defined as part of the primary key.",
            column_name
        ))
        .into());
    }

    Ok(())
}

/// Binds constraints that can be only specified in column definitions,
/// currently generated columns and default columns.
pub fn bind_sql_column_constraints(
    session: &SessionImpl,
    table_name: String,
    column_catalogs: &mut [ColumnCatalog],
    columns: Vec<ColumnDef>,
    pk_column_ids: &[ColumnId],
) -> Result<()> {
    let generated_column_names = {
        let mut names = vec![];
        for column in &columns {
            for option_def in &column.options {
                if let ColumnOption::GeneratedColumns(_) = option_def.option {
                    names.push(column.name.real_value());
                    break;
                }
            }
        }
        names
    };

    let mut binder = Binder::new_for_ddl(session);
    binder.bind_columns_to_context(table_name.clone(), column_catalogs)?;

    for column in columns {
        let Some(idx) = column_catalogs
            .iter()
            .position(|c| c.name() == column.name.real_value())
        else {
            // It's possible that we don't follow the user defined columns in SQL but take the
            // ones resolved from the source, thus missing some columns. Simply ignore them.
            continue;
        };

        for option_def in column.options {
            match option_def.option {
                ColumnOption::GeneratedColumns(expr) => {
                    binder.set_clause(Some(Clause::GeneratedColumn));

                    let expr_impl = binder.bind_expr(expr).with_context(|| {
                        format!(
                            "fail to bind expression in generated column \"{}\"",
                            column.name.real_value()
                        )
                    })?;

                    check_generated_column_constraints(
                        &column.name.real_value(),
                        column_catalogs[idx].column_id(),
                        &expr_impl,
                        column_catalogs,
                        &generated_column_names,
                        pk_column_ids,
                    )?;

                    column_catalogs[idx].column_desc.generated_or_default_column = Some(
                        GeneratedOrDefaultColumn::GeneratedColumn(GeneratedColumnDesc {
                            expr: Some(expr_impl.to_expr_proto()),
                        }),
                    );
                    binder.set_clause(None);
                }
                ColumnOption::DefaultValue(expr) => {
                    let expr_impl = binder
                        .bind_expr(expr)?
                        .cast_assign(column_catalogs[idx].data_type().clone())?;

                    // Rewrite expressions to evaluate a snapshot value, used for missing values in the case of
                    // schema change.
                    //
                    // TODO: Currently we don't support impure expressions other than `now()` (like `random()`),
                    // so the rewritten expression should almost always be pure and we directly call `fold_const`
                    // here. Actually we do not require purity of the expression here since we're only to get a
                    // snapshot value.
                    let rewritten_expr_impl = session
                        .pinned_snapshot()
                        .inline_now_proc_time()
                        .rewrite_expr(expr_impl.clone());

                    if let Some(snapshot_value) = rewritten_expr_impl.try_fold_const() {
                        let snapshot_value = snapshot_value?;

                        column_catalogs[idx].column_desc.generated_or_default_column =
                            Some(GeneratedOrDefaultColumn::DefaultColumn(DefaultColumnDesc {
                                snapshot_value: Some(snapshot_value.to_protobuf()),
                                expr: Some(expr_impl.to_expr_proto()),
                                // persist the original expression
                            }));
                    } else {
                        return Err(ErrorCode::BindError(format!(
                            "Default expression used in column `{}` cannot be evaluated. \
                            Use generated columns instead if you mean to reference other columns.",
                            column.name
                        ))
                        .into());
                    }
                }
                ColumnOption::DefaultValueInternal { persisted, expr: _ } => {
                    // When a `DEFAULT INTERNAL` is used internally for schema change, the persisted value
                    // should already be set during purifcation. So if we encounter an empty value here, it
                    // means the user has specified it explicitly in the SQL statement, typically by
                    // directly copying the result of `SHOW CREATE TABLE` and executing it.
                    if persisted.is_empty() {
                        bail_bind_error!(
                            "DEFAULT INTERNAL is only used for internal purposes, \
                             please specify a concrete default value"
                        );
                    }

                    let desc = DefaultColumnDesc::decode(&*persisted)
                        .expect("failed to decode persisted `DefaultColumnDesc`");

                    column_catalogs[idx].column_desc.generated_or_default_column =
                        Some(GeneratedOrDefaultColumn::DefaultColumn(desc));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Currently we only support Primary key table constraint, so just return pk names if it exists
pub fn bind_table_constraints(table_constraints: &[TableConstraint]) -> Result<Vec<String>> {
    let mut pk_column_names = vec![];

    for constraint in table_constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary: true,
            } => {
                if !pk_column_names.is_empty() {
                    return Err(multiple_pk_definition_err());
                }
                pk_column_names = columns.iter().map(|c| c.real_value()).collect_vec();
            }
            _ => bail_not_implemented!("table constraint \"{}\"", constraint),
        }
    }
    Ok(pk_column_names)
}

pub fn bind_sql_pk_names(
    columns_defs: &[ColumnDef],
    pk_names_from_table_constraints: Vec<String>,
) -> Result<Vec<String>> {
    let mut pk_column_names = pk_names_from_table_constraints;

    for column in columns_defs {
        for option_def in &column.options {
            if let ColumnOption::Unique { is_primary: true } = option_def.option {
                if !pk_column_names.is_empty() {
                    return Err(multiple_pk_definition_err());
                }
                pk_column_names.push(column.name.real_value());
            };
        }
    }

    Ok(pk_column_names)
}

fn multiple_pk_definition_err() -> RwError {
    ErrorCode::BindError("multiple primary keys are not allowed".into()).into()
}

/// Binds primary keys defined in SQL.
///
/// It returns the columns together with `pk_column_ids`, and an optional row id column index if
/// added.
pub fn bind_pk_and_row_id_on_relation(
    mut columns: Vec<ColumnCatalog>,
    pk_names: Vec<String>,
    must_need_pk: bool,
) -> Result<(Vec<ColumnCatalog>, Vec<ColumnId>, Option<usize>)> {
    for c in &columns {
        assert!(c.column_id() != ColumnId::placeholder());
    }

    // Mapping from column name to column id.
    let name_to_id = columns
        .iter()
        .map(|c| (c.name(), c.column_id()))
        .collect::<HashMap<_, _>>();

    let mut pk_column_ids: Vec<_> = pk_names
        .iter()
        .map(|name| {
            name_to_id.get(name.as_str()).copied().ok_or_else(|| {
                ErrorCode::BindError(format!("column \"{name}\" named in key does not exist"))
            })
        })
        .try_collect()?;

    // Add `_row_id` column if `pk_column_ids` is empty and must_need_pk
    let need_row_id = pk_column_ids.is_empty() && must_need_pk;

    let row_id_index = need_row_id.then(|| {
        let column = ColumnCatalog::row_id_column();
        let index = columns.len();
        pk_column_ids = vec![column.column_id()];
        columns.push(column);
        index
    });

    if let Some(col) = columns.iter().map(|c| c.name()).duplicates().next() {
        Err(ErrorCode::InvalidInputSyntax(format!(
            "column \"{col}\" specified more than once"
        )))?;
    }

    Ok((columns, pk_column_ids, row_id_index))
}

/// `gen_create_table_plan_with_source` generates the plan for creating a table with an external
/// stream source.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn gen_create_table_plan_with_source(
    mut handler_args: HandlerArgs,
    explain_options: ExplainOptions,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    format_encode: FormatEncodeOptions,
    source_watermarks: Vec<SourceWatermark>,
    mut col_id_gen: ColumnIdGenerator,
    include_column_options: IncludeOption,
    props: CreateTableProps,
    sql_column_strategy: SqlColumnStrategy,
) -> Result<(PlanRef, Option<PbSource>, PbTable)> {
    if props.append_only
        && format_encode.format != Format::Plain
        && format_encode.format != Format::Native
    {
        return Err(ErrorCode::BindError(format!(
            "Append only table does not support format {}.",
            format_encode.format
        ))
        .into());
    }

    let session = &handler_args.session;
    let with_properties = bind_connector_props(&handler_args, &format_encode, false)?;

    let db_name: &str = &session.database();
    let (schema_name, _) = Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;

    // TODO: omit this step if `sql_column_strategy` is `Follow`.
    let (columns_from_resolve_source, source_info) = bind_columns_from_source(
        session,
        &format_encode,
        Either::Left(&with_properties),
        CreateSourceType::Table,
    )
    .await?;

    let overwrite_options = OverwriteOptions::new(&mut handler_args);
    let rate_limit = overwrite_options.source_rate_limit;
    let (source_catalog, database_id, schema_id) = bind_create_source_or_table_with_connector(
        handler_args.clone(),
        table_name,
        format_encode,
        with_properties,
        &column_defs,
        constraints,
        wildcard_idx,
        source_watermarks,
        columns_from_resolve_source,
        source_info,
        include_column_options,
        &mut col_id_gen,
        CreateSourceType::Table,
        rate_limit,
        sql_column_strategy,
    )
    .await?;

    let pb_source = source_catalog.to_prost(schema_id, database_id);

    let context = OptimizerContext::new(handler_args, explain_options);

    let (plan, table) = gen_table_plan_with_source(
        context.into(),
        schema_name,
        source_catalog,
        col_id_gen.into_version(),
        props,
    )?;

    Ok((plan, Some(pb_source), table))
}

/// `gen_create_table_plan` generates the plan for creating a table without an external stream
/// source.
#[allow(clippy::too_many_arguments)]
pub(crate) fn gen_create_table_plan(
    context: OptimizerContext,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    mut col_id_gen: ColumnIdGenerator,
    source_watermarks: Vec<SourceWatermark>,
    props: CreateTableProps,
) -> Result<(PlanRef, PbTable)> {
    let mut columns = bind_sql_columns(&column_defs)?;
    for c in &mut columns {
        c.column_desc.column_id = col_id_gen.generate(&*c)?;
    }

    let (_, secret_refs, connection_refs) = context.with_options().clone().into_parts();
    if !secret_refs.is_empty() || !connection_refs.is_empty() {
        return Err(crate::error::ErrorCode::InvalidParameterValue("Secret reference and Connection reference are not allowed in options when creating table without external source".to_owned()).into());
    }

    gen_create_table_plan_without_source(
        context,
        table_name,
        columns,
        column_defs,
        constraints,
        source_watermarks,
        col_id_gen.into_version(),
        props,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn gen_create_table_plan_without_source(
    context: OptimizerContext,
    table_name: ObjectName,
    columns: Vec<ColumnCatalog>,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    source_watermarks: Vec<SourceWatermark>,
    version: TableVersion,
    props: CreateTableProps,
) -> Result<(PlanRef, PbTable)> {
    // XXX: Why not bind outside?
    let pk_names = bind_sql_pk_names(&column_defs, bind_table_constraints(&constraints)?)?;
    let (mut columns, pk_column_ids, row_id_index) =
        bind_pk_and_row_id_on_relation(columns, pk_names, true)?;

    let watermark_descs: Vec<WatermarkDesc> = bind_source_watermark(
        context.session_ctx(),
        table_name.real_value(),
        source_watermarks,
        &columns,
    )?;

    bind_sql_column_constraints(
        context.session_ctx(),
        table_name.real_value(),
        &mut columns,
        column_defs,
        &pk_column_ids,
    )?;
    let session = context.session_ctx().clone();

    let db_name = &session.database();
    let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, table_name)?;

    let info = CreateTableInfo {
        columns,
        pk_column_ids,
        row_id_index,
        watermark_descs,
        source_catalog: None,
        version,
    };

    gen_table_plan_inner(context.into(), schema_name, table_name, info, props)
}

fn gen_table_plan_with_source(
    context: OptimizerContextRef,
    schema_name: Option<String>,
    source_catalog: SourceCatalog,
    version: TableVersion,
    props: CreateTableProps,
) -> Result<(PlanRef, PbTable)> {
    let table_name = source_catalog.name.clone();

    let info = CreateTableInfo {
        columns: source_catalog.columns.clone(),
        pk_column_ids: source_catalog.pk_col_ids.clone(),
        row_id_index: source_catalog.row_id_index,
        watermark_descs: source_catalog.watermark_descs.clone(),
        source_catalog: Some(source_catalog),
        version,
    };

    gen_table_plan_inner(context, schema_name, table_name, info, props)
}

/// On-conflict behavior either from user input or existing table catalog.
#[derive(Clone, Copy)]
pub enum EitherOnConflict {
    Ast(Option<OnConflict>),
    Resolved(ConflictBehavior),
}

impl From<Option<OnConflict>> for EitherOnConflict {
    fn from(v: Option<OnConflict>) -> Self {
        Self::Ast(v)
    }
}

impl From<ConflictBehavior> for EitherOnConflict {
    fn from(v: ConflictBehavior) -> Self {
        Self::Resolved(v)
    }
}

impl EitherOnConflict {
    /// Resolves the conflict behavior based on the given information.
    pub fn to_behavior(self, append_only: bool, row_id_as_pk: bool) -> Result<ConflictBehavior> {
        let conflict_behavior = match self {
            EitherOnConflict::Ast(on_conflict) => {
                if append_only {
                    if row_id_as_pk {
                        // Primary key will be generated, no conflict check needed.
                        ConflictBehavior::NoCheck
                    } else {
                        // User defined PK on append-only table, enforce `DO NOTHING`.
                        if let Some(on_conflict) = on_conflict
                            && on_conflict != OnConflict::Nothing
                        {
                            return Err(ErrorCode::InvalidInputSyntax(
                                "When PRIMARY KEY constraint applied to an APPEND ONLY table, \
                                     the ON CONFLICT behavior must be DO NOTHING."
                                    .to_owned(),
                            )
                            .into());
                        }
                        ConflictBehavior::IgnoreConflict
                    }
                } else {
                    // Default to `UPDATE FULL` for non-append-only tables.
                    match on_conflict.unwrap_or(OnConflict::UpdateFull) {
                        OnConflict::UpdateFull => ConflictBehavior::Overwrite,
                        OnConflict::Nothing => ConflictBehavior::IgnoreConflict,
                        OnConflict::UpdateIfNotNull => ConflictBehavior::DoUpdateIfNotNull,
                    }
                }
            }
            EitherOnConflict::Resolved(b) => b,
        };

        Ok(conflict_behavior)
    }
}

/// Arguments of the functions that generate a table plan, part 1.
///
/// Compared to [`CreateTableProps`], this struct contains fields that need some work of binding
/// or resolving based on the user input.
pub struct CreateTableInfo {
    pub columns: Vec<ColumnCatalog>,
    pub pk_column_ids: Vec<ColumnId>,
    pub row_id_index: Option<usize>,
    pub watermark_descs: Vec<WatermarkDesc>,
    pub source_catalog: Option<SourceCatalog>,
    pub version: TableVersion,
}

/// Arguments of the functions that generate a table plan, part 2.
///
/// Compared to [`CreateTableInfo`], this struct contains fields that can be (relatively) simply
/// obtained from the input or the context.
pub struct CreateTableProps {
    pub definition: String,
    pub append_only: bool,
    pub on_conflict: EitherOnConflict,
    pub with_version_column: Option<String>,
    pub webhook_info: Option<PbWebhookSourceInfo>,
    pub engine: Engine,
}

#[allow(clippy::too_many_arguments)]
fn gen_table_plan_inner(
    context: OptimizerContextRef,
    schema_name: Option<String>,
    table_name: String,
    info: CreateTableInfo,
    props: CreateTableProps,
) -> Result<(PlanRef, PbTable)> {
    let CreateTableInfo {
        ref columns,
        row_id_index,
        ref watermark_descs,
        ref source_catalog,
        ..
    } = info;
    let CreateTableProps { append_only, .. } = props;

    let (database_id, schema_id) = context
        .session_ctx()
        .get_database_and_schema_id_for_create(schema_name)?;

    let session = context.session_ctx().clone();
    let retention_seconds = context.with_options().retention_seconds();

    let source_node: PlanRef = LogicalSource::new(
        source_catalog.clone().map(Rc::new),
        columns.clone(),
        row_id_index,
        SourceNodeKind::CreateTable,
        context.clone(),
        None,
    )?
    .into();

    let required_cols = FixedBitSet::with_capacity(columns.len());
    let plan_root = PlanRoot::new_with_logical_plan(
        source_node,
        RequiredDist::Any,
        Order::any(),
        required_cols,
        vec![],
    );

    if !append_only && !watermark_descs.is_empty() {
        return Err(ErrorCode::NotSupported(
            "Defining watermarks on table requires the table to be append only.".to_owned(),
            "Use the key words `APPEND ONLY`".to_owned(),
        )
        .into());
    }

    if !append_only && retention_seconds.is_some() {
        return Err(ErrorCode::NotSupported(
            "Defining retention seconds on table requires the table to be append only.".to_owned(),
            "Use the key words `APPEND ONLY`".to_owned(),
        )
        .into());
    }

    let materialize = plan_root.gen_table_plan(context, table_name, info, props)?;

    let mut table = materialize.table().to_prost(schema_id, database_id);

    table.owner = session.user_id();
    Ok((materialize.into(), table))
}

/// Generate stream plan for cdc table based on shared source.
/// In replace workflow, the `table_id` is the id of the table to be replaced
/// in create table workflow, the `table_id` is a placeholder will be filled in the Meta
#[allow(clippy::too_many_arguments)]
pub(crate) fn gen_create_table_plan_for_cdc_table(
    context: OptimizerContextRef,
    source: Arc<SourceCatalog>,
    external_table_name: String,
    column_defs: Vec<ColumnDef>,
    mut columns: Vec<ColumnCatalog>,
    pk_names: Vec<String>,
    cdc_with_options: WithOptionsSecResolved,
    mut col_id_gen: ColumnIdGenerator,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    include_column_options: IncludeOption,
    table_name: ObjectName,
    resolved_table_name: String, // table name without schema prefix
    database_id: DatabaseId,
    schema_id: SchemaId,
    table_id: TableId,
    engine: Engine,
) -> Result<(PlanRef, PbTable)> {
    let session = context.session_ctx().clone();

    // append additional columns to the end
    handle_addition_columns(
        None,
        &cdc_with_options,
        include_column_options,
        &mut columns,
        true,
    )?;

    for c in &mut columns {
        c.column_desc.column_id = col_id_gen.generate(&*c)?;
    }

    let (mut columns, pk_column_ids, _row_id_index) =
        bind_pk_and_row_id_on_relation(columns, pk_names, true)?;

    // NOTES: In auto schema change, default value is not provided in column definition.
    bind_sql_column_constraints(
        context.session_ctx(),
        table_name.real_value(),
        &mut columns,
        column_defs,
        &pk_column_ids,
    )?;

    let definition = context.normalized_sql().to_owned();

    let pk_column_indices = {
        let mut id_to_idx = HashMap::new();
        columns.iter().enumerate().for_each(|(idx, c)| {
            id_to_idx.insert(c.column_id(), idx);
        });
        // pk column id must exist in table columns.
        pk_column_ids
            .iter()
            .map(|c| id_to_idx.get(c).copied().unwrap())
            .collect_vec()
    };
    let table_pk = pk_column_indices
        .iter()
        .map(|idx| ColumnOrder::new(*idx, OrderType::ascending()))
        .collect();

    let (options, secret_refs) = cdc_with_options.into_parts();

    let non_generated_column_descs = columns
        .iter()
        .filter(|&c| (!c.is_generated()))
        .map(|c| c.column_desc.clone())
        .collect_vec();
    let non_generated_column_num = non_generated_column_descs.len();

    let cdc_table_desc = CdcTableDesc {
        table_id,
        source_id: source.id.into(), // id of cdc source streaming job
        external_table_name: external_table_name.clone(),
        pk: table_pk,
        columns: non_generated_column_descs,
        stream_key: pk_column_indices,
        connect_properties: options,
        secret_refs,
    };

    tracing::debug!(?cdc_table_desc, "create cdc table");

    let options = CdcScanOptions::from_with_options(context.with_options())?;

    let logical_scan = LogicalCdcScan::create(
        external_table_name.clone(),
        Rc::new(cdc_table_desc),
        context.clone(),
        options,
    );

    let scan_node: PlanRef = logical_scan.into();
    let required_cols = FixedBitSet::with_capacity(non_generated_column_num);
    let plan_root = PlanRoot::new_with_logical_plan(
        scan_node,
        RequiredDist::Any,
        Order::any(),
        required_cols,
        vec![],
    );

    let cdc_table_id = build_cdc_table_id(source.id, &external_table_name);
    let materialize = plan_root.gen_table_plan(
        context,
        resolved_table_name,
        CreateTableInfo {
            columns,
            pk_column_ids,
            row_id_index: None,
            watermark_descs: vec![],
            source_catalog: Some((*source).clone()),
            version: col_id_gen.into_version(),
        },
        CreateTableProps {
            definition,
            append_only: false,
            on_conflict: on_conflict.into(),
            with_version_column,
            webhook_info: None,
            engine,
        },
    )?;

    let mut table = materialize.table().to_prost(schema_id, database_id);
    table.owner = session.user_id();
    table.cdc_table_id = Some(cdc_table_id);
    table.dependent_relations = vec![source.id];

    Ok((materialize.into(), table))
}

fn derive_with_options_for_cdc_table(
    source_with_properties: &WithOptionsSecResolved,
    external_table_name: String,
) -> Result<WithOptionsSecResolved> {
    use source::cdc::{MYSQL_CDC_CONNECTOR, POSTGRES_CDC_CONNECTOR, SQL_SERVER_CDC_CONNECTOR};
    // we should remove the prefix from `full_table_name`
    let mut with_options = source_with_properties.clone();
    if let Some(connector) = source_with_properties.get(UPSTREAM_SOURCE_KEY) {
        match connector.as_str() {
            MYSQL_CDC_CONNECTOR => {
                // MySQL doesn't allow '.' in database name and table name, so we can split the
                // external table name by '.' to get the table name
                let (db_name, table_name) = external_table_name.split_once('.').ok_or_else(|| {
                    anyhow!("The upstream table name must contain database name prefix, e.g. 'database.table'")
                })?;
                with_options.insert(DATABASE_NAME_KEY.into(), db_name.into());
                with_options.insert(TABLE_NAME_KEY.into(), table_name.into());
            }
            POSTGRES_CDC_CONNECTOR => {
                let (schema_name, table_name) = external_table_name
                    .split_once('.')
                    .ok_or_else(|| anyhow!("The upstream table name must contain schema name prefix, e.g. 'public.table'"))?;

                // insert 'schema.name' into connect properties
                with_options.insert(SCHEMA_NAME_KEY.into(), schema_name.into());
                with_options.insert(TABLE_NAME_KEY.into(), table_name.into());
            }
            SQL_SERVER_CDC_CONNECTOR => {
                // SQL Server external table name is in 'databaseName.schemaName.tableName' pattern,
                // we remove the database name prefix and split the schema name and table name
                let schema_table_name = external_table_name
                    .split_once('.')
                    .ok_or_else(|| {
                        anyhow!("The upstream table name must be in 'database.schema.table' format")
                    })?
                    .1;

                let (schema_name, table_name) =
                    schema_table_name.split_once('.').ok_or_else(|| {
                        anyhow!("The table name must contain schema name prefix, e.g. 'dbo.table'")
                    })?;

                // insert 'schema.name' into connect properties
                with_options.insert(SCHEMA_NAME_KEY.into(), schema_name.into());
                with_options.insert(TABLE_NAME_KEY.into(), table_name.into());
            }
            _ => {
                return Err(RwError::from(anyhow!(
                    "connector {} is not supported for cdc table",
                    connector
                )));
            }
        };
    }
    Ok(with_options)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_create_table_plan(
    handler_args: HandlerArgs,
    explain_options: ExplainOptions,
    format_encode: Option<FormatEncodeOptions>,
    cdc_table_info: Option<CdcTableInfo>,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    include_column_options: IncludeOption,
    webhook_info: Option<WebhookSourceInfo>,
    engine: Engine,
) -> Result<(PlanRef, Option<PbSource>, PbTable, TableJobType)> {
    let col_id_gen = ColumnIdGenerator::new_initial();
    let format_encode = check_create_table_with_source(
        &handler_args.with_options,
        format_encode,
        &include_column_options,
        &cdc_table_info,
    )?;
    let webhook_info = webhook_info
        .map(|info| bind_webhook_info(&handler_args.session, &column_defs, info))
        .transpose()?;

    let props = CreateTableProps {
        definition: handler_args.normalized_sql.clone(),
        append_only,
        on_conflict: on_conflict.into(),
        with_version_column: with_version_column.clone(),
        webhook_info,
        engine,
    };

    let ((plan, source, table), job_type) = match (format_encode, cdc_table_info.as_ref()) {
        (Some(format_encode), None) => (
            gen_create_table_plan_with_source(
                handler_args,
                explain_options,
                table_name.clone(),
                column_defs,
                wildcard_idx,
                constraints,
                format_encode,
                source_watermarks,
                col_id_gen,
                include_column_options,
                props,
                SqlColumnStrategy::Reject,
            )
            .await?,
            TableJobType::General,
        ),
        (None, None) => {
            let context = OptimizerContext::new(handler_args, explain_options);
            let (plan, table) = gen_create_table_plan(
                context,
                table_name.clone(),
                column_defs,
                constraints,
                col_id_gen,
                source_watermarks,
                props,
            )?;

            ((plan, None, table), TableJobType::General)
        }

        (None, Some(cdc_table)) => {
            sanity_check_for_cdc_table(
                append_only,
                &column_defs,
                &wildcard_idx,
                &constraints,
                &source_watermarks,
            )?;

            let session = &handler_args.session;
            let db_name = &session.database();
            let user_name = &session.user_name();
            let search_path = session.config().search_path();
            let (schema_name, resolved_table_name) =
                Binder::resolve_schema_qualified_name(db_name, table_name.clone())?;
            let (database_id, schema_id) =
                session.get_database_and_schema_id_for_create(schema_name.clone())?;

            // cdc table cannot be append-only
            let (format_encode, source_name) =
                Binder::resolve_schema_qualified_name(db_name, cdc_table.source_name.clone())?;

            let source = {
                let catalog_reader = session.env().catalog_reader().read_guard();
                let schema_path =
                    SchemaPath::new(format_encode.as_deref(), &search_path, user_name);

                let (source, _) = catalog_reader.get_source_by_name(
                    db_name,
                    schema_path,
                    source_name.as_str(),
                )?;
                source.clone()
            };
            let cdc_with_options: WithOptionsSecResolved = derive_with_options_for_cdc_table(
                &source.with_properties,
                cdc_table.external_table_name.clone(),
            )?;

            let (columns, pk_names) = match wildcard_idx {
                Some(_) => bind_cdc_table_schema_externally(cdc_with_options.clone()).await?,
                None => {
                    for column_def in &column_defs {
                        for option_def in &column_def.options {
                            if let ColumnOption::DefaultValue(_)
                            | ColumnOption::DefaultValueInternal { .. } = option_def.option
                            {
                                return Err(ErrorCode::NotSupported(
                                            "Default value for columns defined on the table created from a CDC source".into(),
                                            "Remove the default value expression in the column definitions".into(),
                                        )
                                            .into());
                            }
                        }
                    }

                    let (mut columns, pk_names) =
                        bind_cdc_table_schema(&column_defs, &constraints)?;
                    // read default value definition from external db
                    let (options, secret_refs) = cdc_with_options.clone().into_parts();
                    let config = ExternalTableConfig::try_from_btreemap(options, secret_refs)
                        .context("failed to extract external table config")?;

                    let table: ExternalTableImpl = ExternalTableImpl::connect(config)
                        .await
                        .context("failed to auto derive table schema")?;
                    let external_columns: HashMap<&str, &ColumnDesc> = table
                        .column_descs()
                        .iter()
                        .map(|column_desc| (column_desc.name.as_str(), column_desc))
                        .collect();

                    for col in &mut columns {
                        // Keep the default column aligned with external table.
                        // Note the generated columns have not been initialized yet. If a column is not found here, it should be a generated column.
                        if let Some(external_column_desc) = external_columns.get(col.name()) {
                            col.column_desc.generated_or_default_column =
                                external_column_desc.generated_or_default_column.clone();
                        }
                    }
                    (columns, pk_names)
                }
            };

            let context: OptimizerContextRef =
                OptimizerContext::new(handler_args, explain_options).into();
            let (plan, table) = gen_create_table_plan_for_cdc_table(
                context,
                source,
                cdc_table.external_table_name.clone(),
                column_defs,
                columns,
                pk_names,
                cdc_with_options,
                col_id_gen,
                on_conflict,
                with_version_column,
                include_column_options,
                table_name,
                resolved_table_name,
                database_id,
                schema_id,
                TableId::placeholder(),
                engine,
            )?;

            ((plan, None, table), TableJobType::SharedCdcSource)
        }
        (Some(_), Some(_)) => {
            return Err(ErrorCode::NotSupported(
                "Data format and encoding format doesn't apply to table created from a CDC source"
                    .into(),
                "Remove the FORMAT and ENCODE specification".into(),
            )
            .into())
        }
    };
    Ok((plan, source, table, job_type))
}

fn sanity_check_for_cdc_table(
    append_only: bool,
    column_defs: &Vec<ColumnDef>,
    wildcard_idx: &Option<usize>,
    constraints: &Vec<TableConstraint>,
    source_watermarks: &Vec<SourceWatermark>,
) -> Result<()> {
    // wildcard cannot be used with column definitions
    if wildcard_idx.is_some() && !column_defs.is_empty() {
        return Err(ErrorCode::NotSupported(
            "wildcard(*) and column definitions cannot be used together".to_owned(),
            "Remove the wildcard or column definitions".to_owned(),
        )
        .into());
    }

    // cdc table must have primary key constraint or primary key column
    if !wildcard_idx.is_some()
        && !constraints.iter().any(|c| {
            matches!(
                c,
                TableConstraint::Unique {
                    is_primary: true,
                    ..
                }
            )
        })
        && !column_defs.iter().any(|col| {
            col.options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::Unique { is_primary: true }))
        })
    {
        return Err(ErrorCode::NotSupported(
            "CDC table without primary key constraint is not supported".to_owned(),
            "Please define a primary key".to_owned(),
        )
        .into());
    }
    if append_only {
        return Err(ErrorCode::NotSupported(
            "append only modifier on the table created from a CDC source".into(),
            "Remove the APPEND ONLY clause".into(),
        )
        .into());
    }

    if !source_watermarks.is_empty() {
        return Err(ErrorCode::NotSupported(
            "watermark defined on the table created from a CDC source".into(),
            "Remove the Watermark definitions".into(),
        )
        .into());
    }

    Ok(())
}

/// Derive schema for cdc table when create a new Table or alter an existing Table
async fn bind_cdc_table_schema_externally(
    cdc_with_options: WithOptionsSecResolved,
) -> Result<(Vec<ColumnCatalog>, Vec<String>)> {
    // read cdc table schema from external db or parsing the schema from SQL definitions
    Feature::CdcTableSchemaMap.check_available().map_err(
        |err: risingwave_common::license::FeatureNotAvailable| {
            ErrorCode::NotSupported(
                err.to_report_string(),
                "Please define the schema manually".to_owned(),
            )
        },
    )?;
    let (options, secret_refs) = cdc_with_options.into_parts();
    let config = ExternalTableConfig::try_from_btreemap(options, secret_refs)
        .context("failed to extract external table config")?;

    let table = ExternalTableImpl::connect(config)
        .await
        .context("failed to auto derive table schema")?;
    Ok((
        table
            .column_descs()
            .iter()
            .cloned()
            .map(|column_desc| ColumnCatalog {
                column_desc,
                is_hidden: false,
            })
            .collect(),
        table.pk_names().clone(),
    ))
}

/// Derive schema for cdc table when create a new Table or alter an existing Table
fn bind_cdc_table_schema(
    column_defs: &Vec<ColumnDef>,
    constraints: &Vec<TableConstraint>,
) -> Result<(Vec<ColumnCatalog>, Vec<String>)> {
    let columns = bind_sql_columns(column_defs)?;
    let pk_names = bind_sql_pk_names(column_defs, bind_table_constraints(constraints)?)?;
    Ok((columns, pk_names))
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_create_table(
    handler_args: HandlerArgs,
    table_name: ObjectName,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    if_not_exists: bool,
    format_encode: Option<FormatEncodeOptions>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    cdc_table_info: Option<CdcTableInfo>,
    include_column_options: IncludeOption,
    webhook_info: Option<WebhookSourceInfo>,
    ast_engine: risingwave_sqlparser::ast::Engine,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();

    if append_only {
        session.notice_to_user("APPEND ONLY TABLE is currently an experimental feature.");
    }

    session.check_cluster_limits().await?;

    let engine = match ast_engine {
        risingwave_sqlparser::ast::Engine::Hummock => Engine::Hummock,
        risingwave_sqlparser::ast::Engine::Iceberg => Engine::Iceberg,
    };

    if let Either::Right(resp) = session.check_relation_name_duplicated(
        table_name.clone(),
        StatementType::CREATE_TABLE,
        if_not_exists,
    )? {
        return Ok(resp);
    }

    let (graph, source, table, job_type) = {
        let (plan, source, table, job_type) = handle_create_table_plan(
            handler_args.clone(),
            ExplainOptions::default(),
            format_encode,
            cdc_table_info,
            table_name.clone(),
            column_defs.clone(),
            wildcard_idx,
            constraints.clone(),
            source_watermarks,
            append_only,
            on_conflict,
            with_version_column,
            include_column_options,
            webhook_info,
            engine,
        )
        .await?;

        let graph = build_graph(plan)?;

        (graph, source, table, job_type)
    };

    tracing::trace!(
        "name={}, graph=\n{}",
        table_name,
        serde_json::to_string_pretty(&graph).unwrap()
    );

    // Handle engine
    match engine {
        Engine::Hummock => {
            let catalog_writer = session.catalog_writer()?;
            catalog_writer
                .create_table(source, table, graph, job_type)
                .await?;
        }
        Engine::Iceberg => {
            create_iceberg_engine_table(
                session,
                handler_args,
                source,
                table,
                graph,
                job_type,
                column_defs,
                constraints,
                table_name,
            )
            .await?;
        }
    }

    Ok(PgResponse::empty_result(StatementType::CREATE_TABLE))
}

#[allow(clippy::too_many_arguments)]
pub async fn create_iceberg_engine_table(
    session: Arc<SessionImpl>,
    handler_args: HandlerArgs,
    mut source: Option<PbSource>,
    table: PbTable,
    graph: StreamFragmentGraph,
    job_type: TableJobType,
    column_defs: Vec<ColumnDef>,
    constraints: Vec<TableConstraint>,
    table_name: ObjectName,
) -> Result<()> {
    // 1. fetch iceberg engine options from the meta node.
    // 2. create a hummock table
    // 3. create an iceberg sink
    // 4. create an iceberg source

    let meta_client = session.env().meta_client();
    let system_params = meta_client.get_system_params().await?;
    let state_store_endpoint = system_params.state_store().to_owned();
    let data_directory = system_params.data_directory().to_owned();
    let meta_store_endpoint = meta_client.get_meta_store_endpoint().await?;

    let (s3_region, s3_bucket, s3_endpoint, s3_ak, s3_sk) = match state_store_endpoint {
        s3 if s3.starts_with("hummock+s3://") => {
            let s3_region = if let Ok(s3_region) = std::env::var("AWS_REGION") {
                s3_region
            } else {
                bail!("To create an iceberg engine table with s3 backend, AWS_REGION needed to be set");
            };
            (
                s3_region,
                s3.strip_prefix("hummock+s3://").unwrap().to_owned(),
                None,
                None,
                None,
            )
        }
        minio if minio.starts_with("hummock+minio://") => {
            let server = minio.strip_prefix("hummock+minio://").unwrap();
            let (access_key_id, rest) = server.split_once(':').unwrap();
            let (secret_access_key, mut rest) = rest.split_once('@').unwrap();
            let endpoint_prefix = if let Some(rest_stripped) = rest.strip_prefix("https://") {
                rest = rest_stripped;
                "https://"
            } else if let Some(rest_stripped) = rest.strip_prefix("http://") {
                rest = rest_stripped;
                "http://"
            } else {
                "http://"
            };
            let (address, bucket) = rest.split_once('/').unwrap();
            (
                "us-east-1".to_owned(),
                bucket.to_owned(),
                Some(format!("{}{}", endpoint_prefix, address)),
                Some(access_key_id.to_owned()),
                Some(secret_access_key.to_owned()),
            )
        }
        _ => {
            bail!(
                "iceberg engine can't operate with this state store endpoint: {}",
                state_store_endpoint
            );
        }
    };

    let meta_store_endpoint = url::Url::parse(&meta_store_endpoint).map_err(|_| {
        ErrorCode::InternalError("failed to parse the meta store endpoint".to_owned())
    })?;
    let meta_store_backend = meta_store_endpoint.scheme().to_owned();
    let meta_store_user = meta_store_endpoint.username().to_owned();
    let meta_store_password = meta_store_endpoint
        .password()
        .ok_or_else(|| {
            ErrorCode::InternalError("failed to parse password from meta store endpoint".to_owned())
        })?
        .to_owned();
    let meta_store_host = meta_store_endpoint
        .host_str()
        .ok_or_else(|| {
            ErrorCode::InternalError("failed to parse host from meta store endpoint".to_owned())
        })?
        .to_owned();
    let meta_store_port = meta_store_endpoint.port().ok_or_else(|| {
        ErrorCode::InternalError("failed to parse port from meta store endpoint".to_owned())
    })?;
    let meta_store_database = meta_store_endpoint
        .path()
        .trim_start_matches('/')
        .to_owned();

    let Ok(meta_backend) = MetaBackend::from_str(&meta_store_backend, true) else {
        bail!("failed to parse meta backend: {}", meta_store_backend);
    };

    let rw_db_name = session
        .env()
        .catalog_reader()
        .read_guard()
        .get_database_by_id(&table.database_id)?
        .name()
        .to_owned();
    let rw_schema_name = session
        .env()
        .catalog_reader()
        .read_guard()
        .get_schema_by_id(&table.database_id, &table.schema_id)?
        .name()
        .clone();
    let iceberg_catalog_name = rw_db_name.clone();
    let iceberg_database_name = rw_schema_name.clone();
    let iceberg_table_name = table_name.0.last().unwrap().real_value();

    // Iceberg sinks require a primary key, if none is provided, we will use the _row_id column
    // Fetch primary key from columns
    let mut pks = column_defs
        .into_iter()
        .filter(|c| {
            c.options
                .iter()
                .any(|o| matches!(o.option, ColumnOption::Unique { is_primary: true }))
        })
        .map(|c| c.name.to_string())
        .collect::<Vec<String>>();

    // Fetch primary key from constraints
    if pks.is_empty() {
        pks = constraints
            .into_iter()
            .filter(|c| {
                matches!(
                    c,
                    TableConstraint::Unique {
                        is_primary: true,
                        ..
                    }
                )
            })
            .flat_map(|c| match c {
                TableConstraint::Unique { columns, .. } => columns
                    .into_iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>(),
                _ => vec![],
            })
            .collect::<Vec<String>>();
    }

    // For the table without primary key. We will use `_row_id` as primary key
    let sink_from = if pks.is_empty() {
        pks = vec![RISINGWAVE_ICEBERG_ROW_ID.to_owned()];
        let [stmt]: [_; 1] = Parser::parse_sql(&format!(
            "select {} as {}, * from {}",
            ROWID_PREFIX, RISINGWAVE_ICEBERG_ROW_ID, table_name
        ))
        .context("unable to parse query")?
        .try_into()
        .unwrap();

        let Statement::Query(query) = &stmt else {
            panic!("unexpected statement: {:?}", stmt);
        };
        CreateSink::AsQuery(query.clone())
    } else {
        CreateSink::From(table_name.clone())
    };

    let with_properties = WithProperties(vec![]);
    let mut sink_name = table_name.clone();
    *sink_name.0.last_mut().unwrap() = Ident::from(
        (ICEBERG_SINK_PREFIX.to_owned() + &sink_name.0.last().unwrap().real_value()).as_str(),
    );
    let create_sink_stmt = CreateSinkStatement {
        if_not_exists: false,
        sink_name,
        with_properties,
        sink_from,
        columns: vec![],
        emit_mode: None,
        sink_schema: None,
        into_table_name: None,
    };

    let catalog_uri = match meta_backend {
        MetaBackend::Postgres => {
            format!(
                "jdbc:postgresql://{}:{}/{}",
                meta_store_host.clone(),
                meta_store_port.clone(),
                meta_store_database.clone()
            )
        }
        MetaBackend::Mysql => {
            format!(
                "jdbc:mysql://{}:{}/{}",
                meta_store_host.clone(),
                meta_store_port.clone(),
                meta_store_database.clone()
            )
        }
        MetaBackend::Sqlite | MetaBackend::Sql | MetaBackend::Mem => {
            bail!(
                "Unsupported meta backend for iceberg engine table: {}",
                meta_store_backend
            );
        }
    };

    let warehouse_path = format!(
        "s3://{}/{}/iceberg/{}",
        s3_bucket, data_directory, iceberg_catalog_name
    );

    let mut sink_handler_args = handler_args.clone();
    let mut with = BTreeMap::new();
    with.insert("connector".to_owned(), "iceberg".to_owned());

    with.insert("primary_key".to_owned(), pks.join(","));
    with.insert("type".to_owned(), "upsert".to_owned());
    with.insert("catalog.type".to_owned(), "jdbc".to_owned());
    with.insert("warehouse.path".to_owned(), warehouse_path.clone());
    if let Some(s3_endpoint) = s3_endpoint.clone() {
        with.insert("s3.endpoint".to_owned(), s3_endpoint);
    }
    if let Some(s3_ak) = s3_ak.clone() {
        with.insert("s3.access.key".to_owned(), s3_ak.clone());
    }
    if let Some(s3_sk) = s3_sk.clone() {
        with.insert("s3.secret.key".to_owned(), s3_sk.clone());
    }
    with.insert("s3.region".to_owned(), s3_region.clone());
    with.insert("catalog.uri".to_owned(), catalog_uri.clone());
    with.insert("catalog.jdbc.user".to_owned(), meta_store_user.clone());
    with.insert(
        "catalog.jdbc.password".to_owned(),
        meta_store_password.clone(),
    );
    with.insert("catalog.name".to_owned(), iceberg_catalog_name.clone());
    with.insert("database.name".to_owned(), iceberg_database_name.clone());
    with.insert("table.name".to_owned(), iceberg_table_name.clone());
    let commit_checkpoint_interval = handler_args
        .with_options
        .get(COMMIT_CHECKPOINT_INTERVAL)
        .map(|v| v.to_owned())
        .unwrap_or_else(|| "60".to_owned());
    let commit_checkpoint_interval = commit_checkpoint_interval.parse::<u32>().map_err(|_| {
        ErrorCode::InvalidInputSyntax(format!(
            "commit_checkpoint_interval must be a positive integer: {}",
            commit_checkpoint_interval
        ))
    })?;

    if commit_checkpoint_interval == 0 {
        bail!("commit_checkpoint_interval must be a positive integer: 0");
    }

    // remove commit_checkpoint_interval from source options, otherwise it will be considered as an unknown field.
    source
        .as_mut()
        .map(|x| x.with_properties.remove(COMMIT_CHECKPOINT_INTERVAL));

    let sink_decouple = session.config().sink_decouple();
    if matches!(sink_decouple, SinkDecouple::Disable) && commit_checkpoint_interval > 1 {
        bail!("config conflict: `commit_checkpoint_interval` larger than 1 means that sink decouple must be enabled, but session config sink_decouple is disabled")
    }

    with.insert(
        COMMIT_CHECKPOINT_INTERVAL.to_owned(),
        commit_checkpoint_interval.to_string(),
    );
    with.insert("create_table_if_not_exists".to_owned(), "true".to_owned());
    with.insert("enable_config_load".to_owned(), "true".to_owned());
    sink_handler_args.with_options = WithOptions::new_with_options(with);

    let mut source_name = table_name.clone();
    *source_name.0.last_mut().unwrap() = Ident::from(
        (ICEBERG_SOURCE_PREFIX.to_owned() + &source_name.0.last().unwrap().real_value()).as_str(),
    );
    let create_source_stmt = CreateSourceStatement {
        temporary: false,
        if_not_exists: false,
        columns: vec![],
        source_name,
        wildcard_idx: None,
        constraints: vec![],
        with_properties: WithProperties(vec![]),
        format_encode: CompatibleFormatEncode::V2(FormatEncodeOptions::none()),
        source_watermarks: vec![],
        include_column_options: vec![],
    };

    let mut source_handler_args = handler_args.clone();
    let mut with = BTreeMap::new();
    with.insert("connector".to_owned(), "iceberg".to_owned());
    with.insert("catalog.type".to_owned(), "jdbc".to_owned());
    with.insert("warehouse.path".to_owned(), warehouse_path.clone());
    if let Some(s3_endpoint) = s3_endpoint {
        with.insert("s3.endpoint".to_owned(), s3_endpoint.clone());
    }
    if let Some(s3_ak) = s3_ak.clone() {
        with.insert("s3.access.key".to_owned(), s3_ak.clone());
    }
    if let Some(s3_sk) = s3_sk.clone() {
        with.insert("s3.secret.key".to_owned(), s3_sk.clone());
    }
    with.insert("s3.region".to_owned(), s3_region.clone());
    with.insert("catalog.uri".to_owned(), catalog_uri.clone());
    with.insert("catalog.jdbc.user".to_owned(), meta_store_user.clone());
    with.insert(
        "catalog.jdbc.password".to_owned(),
        meta_store_password.clone(),
    );
    with.insert("catalog.name".to_owned(), iceberg_catalog_name.clone());
    with.insert("database.name".to_owned(), iceberg_database_name.clone());
    with.insert("table.name".to_owned(), iceberg_table_name.clone());
    with.insert("enable_config_load".to_owned(), "true".to_owned());
    source_handler_args.with_options = WithOptions::new_with_options(with);

    // before we create the table, ensure the JVM is initialized as we use jdbc catalog right now.
    // If JVM isn't initialized successfully, current not atomic ddl will result in a partially created iceberg engine table.
    let _ = JVM.get_or_init()?;

    let catalog_writer = session.catalog_writer()?;
    // TODO(iceberg): make iceberg engine table creation ddl atomic
    catalog_writer
        .create_table(source, table, graph, job_type)
        .await?;
    create_sink::handle_create_sink(sink_handler_args, create_sink_stmt).await?;
    create_source::handle_create_source(source_handler_args, create_source_stmt).await?;

    Ok(())
}

pub fn check_create_table_with_source(
    with_options: &WithOptions,
    format_encode: Option<FormatEncodeOptions>,
    include_column_options: &IncludeOption,
    cdc_table_info: &Option<CdcTableInfo>,
) -> Result<Option<FormatEncodeOptions>> {
    // skip check for cdc table
    if cdc_table_info.is_some() {
        return Ok(format_encode);
    }
    let defined_source = with_options.is_source_connector();

    if !include_column_options.is_empty() && !defined_source {
        return Err(ErrorCode::InvalidInputSyntax(
            "INCLUDE should be used with a connector".to_owned(),
        )
        .into());
    }
    if defined_source {
        format_encode.as_ref().ok_or_else(|| {
            ErrorCode::InvalidInputSyntax("Please specify a source schema using FORMAT".to_owned())
        })?;
    }
    Ok(format_encode)
}

#[allow(clippy::too_many_arguments)]
pub async fn generate_stream_graph_for_replace_table(
    _session: &Arc<SessionImpl>,
    table_name: ObjectName,
    original_catalog: &Arc<TableCatalog>,
    format_encode: Option<FormatEncodeOptions>,
    handler_args: HandlerArgs,
    col_id_gen: ColumnIdGenerator,
    column_defs: Vec<ColumnDef>,
    wildcard_idx: Option<usize>,
    constraints: Vec<TableConstraint>,
    source_watermarks: Vec<SourceWatermark>,
    append_only: bool,
    on_conflict: Option<OnConflict>,
    with_version_column: Option<String>,
    cdc_table_info: Option<CdcTableInfo>,
    include_column_options: IncludeOption,
    engine: Engine,
    sql_column_strategy: SqlColumnStrategy,
) -> Result<(StreamFragmentGraph, Table, Option<PbSource>, TableJobType)> {
    use risingwave_pb::catalog::table::OptionalAssociatedSourceId;

    let props = CreateTableProps {
        definition: handler_args.normalized_sql.clone(),
        append_only,
        on_conflict: on_conflict.into(),
        with_version_column: with_version_column.clone(),
        webhook_info: original_catalog.webhook_info.clone(),
        engine,
    };

    let ((plan, mut source, table), job_type) = match (format_encode, cdc_table_info.as_ref()) {
        (Some(format_encode), None) => (
            gen_create_table_plan_with_source(
                handler_args,
                ExplainOptions::default(),
                table_name,
                column_defs,
                wildcard_idx,
                constraints,
                format_encode,
                source_watermarks,
                col_id_gen,
                include_column_options,
                props,
                sql_column_strategy,
            )
            .await?,
            TableJobType::General,
        ),
        (None, None) => {
            let context = OptimizerContext::from_handler_args(handler_args);
            let (plan, table) = gen_create_table_plan(
                context,
                table_name,
                column_defs,
                constraints,
                col_id_gen,
                source_watermarks,
                props,
            )?;
            ((plan, None, table), TableJobType::General)
        }
        (None, Some(cdc_table)) => {
            let session = &handler_args.session;
            let (source, resolved_table_name, database_id, schema_id) =
                get_source_and_resolved_table_name(session, cdc_table.clone(), table_name.clone())?;

            let cdc_with_options = derive_with_options_for_cdc_table(
                &source.with_properties,
                cdc_table.external_table_name.clone(),
            )?;

            let (columns, pk_names) = bind_cdc_table_schema(&column_defs, &constraints)?;

            let context: OptimizerContextRef =
                OptimizerContext::new(handler_args, ExplainOptions::default()).into();
            let (plan, table) = gen_create_table_plan_for_cdc_table(
                context,
                source,
                cdc_table.external_table_name.clone(),
                column_defs,
                columns,
                pk_names,
                cdc_with_options,
                col_id_gen,
                on_conflict,
                with_version_column,
                IncludeOption::default(),
                table_name,
                resolved_table_name,
                database_id,
                schema_id,
                original_catalog.id(),
                engine,
            )?;

            ((plan, None, table), TableJobType::SharedCdcSource)
        }
        (Some(_), Some(_)) => {
            return Err(ErrorCode::NotSupported(
                "Data format and encoding format doesn't apply to table created from a CDC source"
                    .into(),
                "Remove the FORMAT and ENCODE specification".into(),
            )
            .into());
        }
    };

    // TODO: avoid this backward conversion.
    if TableCatalog::from(&table).pk_column_ids() != original_catalog.pk_column_ids() {
        Err(ErrorCode::InvalidInputSyntax(
            "alter primary key of table is not supported".to_owned(),
        ))?
    }

    let graph = build_graph(plan)?;

    // Fill the original table ID.
    let mut table = Table {
        id: original_catalog.id().table_id(),
        ..table
    };
    if let Some(source_id) = original_catalog.associated_source_id() {
        table.optional_associated_source_id = Some(OptionalAssociatedSourceId::AssociatedSourceId(
            source_id.table_id,
        ));
        source.as_mut().unwrap().id = source_id.table_id;
        source.as_mut().unwrap().optional_associated_table_id =
            Some(OptionalAssociatedTableId::AssociatedTableId(table.id))
    }

    Ok((graph, table, source, job_type))
}

fn get_source_and_resolved_table_name(
    session: &Arc<SessionImpl>,
    cdc_table: CdcTableInfo,
    table_name: ObjectName,
) -> Result<(Arc<SourceCatalog>, String, DatabaseId, SchemaId)> {
    let db_name = &session.database();
    let (schema_name, resolved_table_name) =
        Binder::resolve_schema_qualified_name(db_name, table_name)?;
    let (database_id, schema_id) =
        session.get_database_and_schema_id_for_create(schema_name.clone())?;

    let (format_encode, source_name) =
        Binder::resolve_schema_qualified_name(db_name, cdc_table.source_name.clone())?;

    let source = {
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_name = format_encode.unwrap_or(DEFAULT_SCHEMA_NAME.to_owned());
        let (source, _) = catalog_reader.get_source_by_name(
            db_name,
            SchemaPath::Name(schema_name.as_str()),
            source_name.as_str(),
        )?;
        source.clone()
    };

    Ok((source, resolved_table_name, database_id, schema_id))
}

// validate the webhook_info and also bind the webhook_info to protobuf
fn bind_webhook_info(
    session: &Arc<SessionImpl>,
    columns_defs: &[ColumnDef],
    webhook_info: WebhookSourceInfo,
) -> Result<PbWebhookSourceInfo> {
    // validate columns
    if columns_defs.len() != 1 || columns_defs[0].data_type.as_ref().unwrap() != &AstDataType::Jsonb
    {
        return Err(ErrorCode::InvalidInputSyntax(
            "Table with webhook source should have exactly one JSONB column".to_owned(),
        )
        .into());
    }

    let WebhookSourceInfo {
        secret_ref,
        signature_expr,
    } = webhook_info;

    // validate secret_ref
    let db_name = &session.database();
    let (schema_name, secret_name) =
        Binder::resolve_schema_qualified_name(db_name, secret_ref.secret_name.clone())?;
    let secret_catalog = session.get_secret_by_name(schema_name, &secret_name)?;
    let pb_secret_ref = PbSecretRef {
        secret_id: secret_catalog.id.secret_id(),
        ref_as: match secret_ref.ref_as {
            SecretRefAsType::Text => PbRefAsType::Text,
            SecretRefAsType::File => PbRefAsType::File,
        }
        .into(),
    };

    let secure_compare_context = SecureCompareContext {
        column_name: columns_defs[0].name.real_value(),
        secret_name,
    };
    let mut binder = Binder::new_for_ddl_with_secure_compare(session, secure_compare_context);
    let expr = binder.bind_expr(signature_expr.clone())?;

    // validate expr, ensuring it is SECURE_COMPARE()
    if expr.as_function_call().is_none()
        || expr.as_function_call().unwrap().func_type()
            != crate::optimizer::plan_node::generic::ExprType::SecureCompare
    {
        return Err(ErrorCode::InvalidInputSyntax(
            "The signature verification function must be SECURE_COMPARE()".to_owned(),
        )
        .into());
    }

    let pb_webhook_info = PbWebhookSourceInfo {
        secret_ref: Some(pb_secret_ref),
        signature_expr: Some(expr.to_expr_proto()),
    };

    Ok(pb_webhook_info)
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{
        Field, DEFAULT_DATABASE_NAME, ROWID_PREFIX, RW_TIMESTAMP_COLUMN_NAME,
    };
    use risingwave_common::types::{DataType, StructType};

    use super::*;
    use crate::test_utils::{create_proto_file, LocalFrontend, PROTO_FILE_DATA};

    struct BrandNewColumn(&'static str);
    use BrandNewColumn as B;

    impl FieldLike for BrandNewColumn {
        fn name(&self) -> &str {
            self.0
        }

        fn data_type(&self) -> &DataType {
            unreachable!("for brand new columns, data type will not be accessed")
        }
    }

    #[test]
    fn test_col_id_gen_initial() {
        let mut gen = ColumnIdGenerator::new_initial();
        assert_eq!(gen.generate(B("v1")).unwrap(), ColumnId::new(1));
        assert_eq!(gen.generate(B("v2")).unwrap(), ColumnId::new(2));
    }

    #[test]
    fn test_col_id_gen_alter() {
        let mut gen = ColumnIdGenerator::new_alter(&TableCatalog {
            columns: vec![
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field::with_name(DataType::Float32, "f32"),
                        1,
                    ),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field::with_name(DataType::Float64, "f64"),
                        2,
                    ),
                    is_hidden: false,
                },
                ColumnCatalog {
                    column_desc: ColumnDesc::from_field_with_column_id(
                        &Field::with_name(
                            StructType::new([("f1", DataType::Int32)]).into(),
                            "nested",
                        ),
                        3,
                    ),
                    is_hidden: false,
                },
            ],
            version: Some(TableVersion::new_initial_for_test(ColumnId::new(3))),
            ..Default::default()
        });

        assert_eq!(gen.generate(B("v1")).unwrap(), ColumnId::new(4));
        assert_eq!(gen.generate(B("v2")).unwrap(), ColumnId::new(5));
        assert_eq!(
            gen.generate(Field::new("f32", DataType::Float32)).unwrap(),
            ColumnId::new(1)
        );

        // mismatched data type
        gen.generate(Field::new("f64", DataType::Float32))
            .unwrap_err();

        // mismatched data type
        // we require the nested data type to be exactly the same
        gen.generate(Field::new(
            "nested",
            StructType::new([("f1", DataType::Int32), ("f2", DataType::Int64)]).into(),
        ))
        .unwrap_err();

        assert_eq!(gen.generate(B("v3")).unwrap(), ColumnId::new(6));
    }

    #[tokio::test]
    async fn test_create_table_handler() {
        let sql =
            "create table t (v1 smallint, v2 struct<v3 bigint, v4 float, v5 double>) append only;";
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check table exists.
        let (table, _) = catalog_reader
            .get_created_table_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(table.name(), "t");

        let columns = table
            .columns
            .iter()
            .map(|col| (col.name(), col.data_type().clone()))
            .collect::<HashMap<&str, DataType>>();

        let expected_columns = maplit::hashmap! {
            ROWID_PREFIX => DataType::Serial,
            "v1" => DataType::Int16,
            "v2" => StructType::new(
                vec![("v3", DataType::Int64),("v4", DataType::Float64),("v5", DataType::Float64)],
            ).into(),
            RW_TIMESTAMP_COLUMN_NAME => DataType::Timestamptz,
        };

        assert_eq!(columns, expected_columns);
    }

    #[test]
    fn test_bind_primary_key() {
        // Note: Column ID 0 is reserved for row ID column.

        for (sql, expected) in [
            ("create table t (v1 int, v2 int)", Ok(&[0] as &[_])),
            ("create table t (v1 int primary key, v2 int)", Ok(&[1])),
            ("create table t (v1 int, v2 int primary key)", Ok(&[2])),
            (
                "create table t (v1 int primary key, v2 int primary key)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key primary key, v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, v2 int, primary key (v1))",
                Ok(&[1]),
            ),
            (
                "create table t (v1 int, primary key (v2), v2 int)",
                Ok(&[2]),
            ),
            (
                "create table t (primary key (v2, v1), v1 int, v2 int)",
                Ok(&[2, 1]),
            ),
            (
                "create table t (v1 int, primary key (v1), v2 int, primary key (v1))",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int primary key, primary key (v1), v2 int)",
                Err("multiple primary keys are not allowed"),
            ),
            (
                "create table t (v1 int, primary key (V3), v2 int)",
                Err("column \"v3\" named in key does not exist"),
            ),
        ] {
            let mut ast = risingwave_sqlparser::parser::Parser::parse_sql(sql).unwrap();
            let risingwave_sqlparser::ast::Statement::CreateTable {
                columns: column_defs,
                constraints,
                ..
            } = ast.remove(0)
            else {
                panic!("test case should be create table")
            };
            let actual: Result<_> = (|| {
                let mut columns = bind_sql_columns(&column_defs)?;
                let mut col_id_gen = ColumnIdGenerator::new_initial();
                for c in &mut columns {
                    c.column_desc.column_id = col_id_gen.generate(&*c).unwrap();
                }

                let pk_names =
                    bind_sql_pk_names(&column_defs, bind_table_constraints(&constraints)?)?;
                let (_, pk_column_ids, _) =
                    bind_pk_and_row_id_on_relation(columns, pk_names, true)?;
                Ok(pk_column_ids)
            })();
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(
                    expected.iter().copied().map(ColumnId::new).collect_vec(),
                    actual,
                    "sql: {sql}"
                ),
                (Ok(_), Err(actual)) => panic!("sql: {sql}\nunexpected error: {actual:?}"),
                (Err(_), Ok(actual)) => panic!("sql: {sql}\nexpects error but got: {actual:?}"),
                (Err(expected), Err(actual)) => assert!(
                    actual.to_string().contains(expected),
                    "sql: {sql}\nexpected: {expected:?}\nactual: {actual:?}"
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_duplicate_props_options() {
        let proto_file = create_proto_file(PROTO_FILE_DATA);
        let sql = format!(
            r#"CREATE TABLE t
    WITH (
        connector = 'kinesis',
        aws.region='user_test_topic',
        endpoint='172.10.1.1:9090,172.10.1.2:9090',
        aws.credentials.access_key_id = 'your_access_key_1',
        aws.credentials.secret_access_key = 'your_secret_key_1'
    )
    FORMAT PLAIN ENCODE PROTOBUF (
        message = '.test.TestRecord',
        aws.credentials.access_key_id = 'your_access_key_2',
        aws.credentials.secret_access_key = 'your_secret_key_2',
        schema.location = 'file://{}',
    )"#,
            proto_file.path().to_str().unwrap()
        );
        let frontend = LocalFrontend::new(Default::default()).await;
        frontend.run_sql(sql).await.unwrap();

        let session = frontend.session_ref();
        let catalog_reader = session.env().catalog_reader().read_guard();
        let schema_path = SchemaPath::Name(DEFAULT_SCHEMA_NAME);

        // Check source exists.
        let (source, _) = catalog_reader
            .get_source_by_name(DEFAULT_DATABASE_NAME, schema_path, "t")
            .unwrap();
        assert_eq!(source.name, "t");

        // AwsAuth params exist in options.
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_2"
        );
        assert_eq!(
            source
                .info
                .format_encode_options
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_2"
        );

        // AwsAuth params exist in props.
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.access_key_id")
                .unwrap(),
            "your_access_key_1"
        );
        assert_eq!(
            source
                .with_properties
                .get("aws.credentials.secret_access_key")
                .unwrap(),
            "your_secret_key_1"
        );

        // Options are not merged into props.
        assert!(!source.with_properties.contains_key("schema.location"));
    }
}
