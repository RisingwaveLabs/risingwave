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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::FunctionId;
use risingwave_common::session_config::{SearchPath, SessionConfig};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_sqlparser::ast::{Expr as AstExpr, SelectItem, SetExpr, Statement};

use crate::error::Result;

mod bind_context;
mod bind_param;
mod create;
mod create_view;
mod declare_cursor;
mod delete;
mod expr;
pub mod fetch_cursor;
mod for_system;
mod insert;
mod query;
mod relation;
mod select;
mod set_expr;
mod statement;
mod struct_field;
mod update;
mod values;

pub use bind_context::{BindContext, Clause, LateralBindContext};
pub use create_view::BoundCreateView;
pub use delete::BoundDelete;
pub use expr::bind_data_type;
pub use insert::BoundInsert;
use pgwire::pg_server::{Session, SessionId};
pub use query::BoundQuery;
pub use relation::{
    BoundBackCteRef, BoundBaseTable, BoundJoin, BoundShare, BoundShareInput, BoundSource,
    BoundSystemTable, BoundWatermark, BoundWindowTableFunction, Relation,
    ResolveQualifiedNameError, WindowTableFunctionKind,
};
pub use select::{BoundDistinct, BoundSelect};
pub use set_expr::*;
pub use statement::BoundStatement;
pub use update::{BoundUpdate, UpdateProject};
pub use values::BoundValues;

use crate::catalog::catalog_service::CatalogReadGuard;
use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::{CatalogResult, TableId, ViewId};
use crate::error::ErrorCode;
use crate::expr::ExprImpl;
use crate::session::{AuthContext, SessionImpl, TemporarySourceManager};

pub type ShareId = usize;

/// The type of binding statement.
enum BindFor {
    /// Binding MV/SINK
    Stream,
    /// Binding a batch query
    Batch,
    /// Binding a DDL (e.g. CREATE TABLE/SOURCE)
    Ddl,
    /// Binding a system query (e.g. SHOW)
    System,
}

/// `Binder` binds the identifiers in AST to columns in relations
pub struct Binder {
    // TODO: maybe we can only lock the database, but not the whole catalog.
    catalog: CatalogReadGuard,
    db_name: String,
    session_id: SessionId,
    context: BindContext,
    auth_context: Arc<AuthContext>,
    /// A stack holding contexts of outer queries when binding a subquery.
    /// It also holds all of the lateral contexts for each respective
    /// subquery.
    ///
    /// See [`Binder::bind_subquery_expr`] for details.
    upper_subquery_contexts: Vec<(BindContext, Vec<LateralBindContext>)>,

    /// A stack holding contexts of left-lateral `TableFactor`s.
    ///
    /// We need a separate stack as `CorrelatedInputRef` depth is
    /// determined by the upper subquery context depth, not the lateral context stack depth.
    lateral_contexts: Vec<LateralBindContext>,

    next_subquery_id: usize,
    next_values_id: usize,
    /// The `ShareId` is used to identify the share relation which could be a CTE, a source, a view
    /// and so on.
    next_share_id: ShareId,

    session_config: Arc<RwLock<SessionConfig>>,

    search_path: SearchPath,
    /// The type of binding statement.
    bind_for: BindFor,

    /// `ShareId`s identifying shared views.
    shared_views: HashMap<ViewId, ShareId>,

    /// The included relations while binding a query.
    included_relations: HashSet<TableId>,

    /// The included user-defined functions while binding a query.
    included_udfs: HashSet<FunctionId>,

    param_types: ParameterTypes,

    /// The sql udf context that will be used during binding phase
    udf_context: UdfContext,

    /// The temporary sources that will be used during binding phase
    temporary_source_manager: TemporarySourceManager,

    /// Information for `secure_compare` function. It's ONLY available when binding the
    /// `VALIDATE` clause of Webhook source i.e. `VALIDATE SECRET ... AS SECURE_COMPARE(...)`.
    secure_compare_context: Option<SecureCompareContext>,
}

// There's one more hidden name, `HEADERS`, which is a reserved identifier for HTTP headers. Its type is `JSONB`.
#[derive(Default, Clone, Debug)]
pub struct SecureCompareContext {
    /// The column name to store the whole payload in `JSONB`, but during validation it will be used as `bytea`
    pub column_name: String,
    /// The secret (usually a token provided by the webhook source user) to validate the calls
    pub secret_name: String,
}

#[derive(Clone, Debug, Default)]
pub struct UdfContext {
    /// The mapping from `sql udf parameters` to a bound `ExprImpl` generated from `ast expressions`
    /// Note: The expressions are constructed during runtime, correspond to the actual users' input
    udf_param_context: HashMap<String, ExprImpl>,

    /// The global counter that records the calling stack depth
    /// of the current binding sql udf chain
    udf_global_counter: u32,
}

impl UdfContext {
    pub fn new() -> Self {
        Self {
            udf_param_context: HashMap::new(),
            udf_global_counter: 0,
        }
    }

    pub fn global_count(&self) -> u32 {
        self.udf_global_counter
    }

    pub fn incr_global_count(&mut self) {
        self.udf_global_counter += 1;
    }

    pub fn decr_global_count(&mut self) {
        self.udf_global_counter -= 1;
    }

    pub fn _is_empty(&self) -> bool {
        self.udf_param_context.is_empty()
    }

    pub fn update_context(&mut self, context: HashMap<String, ExprImpl>) {
        self.udf_param_context = context;
    }

    pub fn _clear(&mut self) {
        self.udf_global_counter = 0;
        self.udf_param_context.clear();
    }

    pub fn get_expr(&self, name: &str) -> Option<&ExprImpl> {
        self.udf_param_context.get(name)
    }

    pub fn get_context(&self) -> HashMap<String, ExprImpl> {
        self.udf_param_context.clone()
    }

    /// A common utility function to extract sql udf
    /// expression out from the input `ast`
    pub fn extract_udf_expression(ast: Vec<Statement>) -> Result<AstExpr> {
        if ast.len() != 1 {
            return Err(ErrorCode::InvalidInputSyntax(
                "the query for sql udf should contain only one statement".to_owned(),
            )
            .into());
        }

        // Extract the expression out
        let Statement::Query(query) = ast[0].clone() else {
            return Err(ErrorCode::InvalidInputSyntax(
                "invalid function definition, please recheck the syntax".to_owned(),
            )
            .into());
        };

        let SetExpr::Select(select) = query.body else {
            return Err(ErrorCode::InvalidInputSyntax(
                "missing `select` body for sql udf expression, please recheck the syntax"
                    .to_owned(),
            )
            .into());
        };

        if select.projection.len() != 1 {
            return Err(ErrorCode::InvalidInputSyntax(
                "`projection` should contain only one `SelectItem`".to_owned(),
            )
            .into());
        }

        let SelectItem::UnnamedExpr(expr) = select.projection[0].clone() else {
            return Err(ErrorCode::InvalidInputSyntax(
                "expect `UnnamedExpr` for `projection`".to_owned(),
            )
            .into());
        };

        Ok(expr)
    }
}

/// `ParameterTypes` is used to record the types of the parameters during binding prepared stataments.
/// It works by following the rules:
/// 1. At the beginning, it contains the user specified parameters type.
/// 2. When the binder encounters a parameter, it will record it as unknown(call `record_new_param`)
///    if it didn't exist in `ParameterTypes`.
/// 3. When the binder encounters a cast on parameter, if it's a unknown type, the cast function
///    will record the target type as infer type for that parameter(call `record_infer_type`). If the
///    parameter has been inferred, the cast function will act as a normal cast.
/// 4. After bind finished:
///     (a) parameter not in `ParameterTypes` means that the user didn't specify it and it didn't
///    occur in the query. `export` will return error if there is a kind of
///    parameter. This rule is compatible with PostgreSQL
///     (b) parameter is None means that it's a unknown type. The user didn't specify it
///    and we can't infer it in the query. We will treat it as VARCHAR type finally. This rule is
///    compatible with PostgreSQL.
///     (c) parameter is Some means that it's a known type.
#[derive(Clone, Debug)]
pub struct ParameterTypes(Arc<RwLock<HashMap<u64, Option<DataType>>>>);

impl ParameterTypes {
    pub fn new(specified_param_types: Vec<Option<DataType>>) -> Self {
        let map = specified_param_types
            .into_iter()
            .enumerate()
            .map(|(index, data_type)| ((index + 1) as u64, data_type))
            .collect::<HashMap<u64, Option<DataType>>>();
        Self(Arc::new(RwLock::new(map)))
    }

    pub fn has_infer(&self, index: u64) -> bool {
        self.0.read().get(&index).unwrap().is_some()
    }

    pub fn read_type(&self, index: u64) -> Option<DataType> {
        self.0.read().get(&index).unwrap().clone()
    }

    pub fn record_new_param(&mut self, index: u64) {
        self.0.write().entry(index).or_insert(None);
    }

    pub fn record_infer_type(&mut self, index: u64, data_type: DataType) {
        assert!(
            !self.has_infer(index),
            "The parameter has been inferred, should not be inferred again."
        );
        self.0.write().get_mut(&index).unwrap().replace(data_type);
    }

    pub fn export(&self) -> Result<Vec<DataType>> {
        let types = self
            .0
            .read()
            .clone()
            .into_iter()
            .sorted_by_key(|(index, _)| *index)
            .collect::<Vec<_>>();

        // Check if all the parameters have been inferred.
        for ((index, _), expect_index) in types.iter().zip_eq_debug(1_u64..=types.len() as u64) {
            if *index != expect_index {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "Cannot infer the type of the parameter {}.",
                    expect_index
                ))
                .into());
            }
        }

        Ok(types
            .into_iter()
            .map(|(_, data_type)| data_type.unwrap_or(DataType::Varchar))
            .collect::<Vec<_>>())
    }
}

impl Binder {
    fn new_inner(
        session: &SessionImpl,
        bind_for: BindFor,
        param_types: Vec<Option<DataType>>,
    ) -> Binder {
        Binder {
            catalog: session.env().catalog_reader().read_guard(),
            db_name: session.database(),
            session_id: session.id(),
            context: BindContext::new(),
            auth_context: session.auth_context(),
            upper_subquery_contexts: vec![],
            lateral_contexts: vec![],
            next_subquery_id: 0,
            next_values_id: 0,
            next_share_id: 0,
            session_config: session.shared_config(),
            search_path: session.config().search_path(),
            bind_for,
            shared_views: HashMap::new(),
            included_relations: HashSet::new(),
            included_udfs: HashSet::new(),
            param_types: ParameterTypes::new(param_types),
            udf_context: UdfContext::new(),
            temporary_source_manager: session.temporary_source_manager(),
            secure_compare_context: None,
        }
    }

    pub fn new(session: &SessionImpl) -> Binder {
        Self::new_inner(session, BindFor::Batch, vec![])
    }

    pub fn new_with_param_types(
        session: &SessionImpl,
        param_types: Vec<Option<DataType>>,
    ) -> Binder {
        Self::new_inner(session, BindFor::Batch, param_types)
    }

    pub fn new_for_stream(session: &SessionImpl) -> Binder {
        Self::new_inner(session, BindFor::Stream, vec![])
    }

    pub fn new_for_ddl(session: &SessionImpl) -> Binder {
        Self::new_inner(session, BindFor::Ddl, vec![])
    }

    pub fn new_for_ddl_with_secure_compare(
        session: &SessionImpl,
        ctx: SecureCompareContext,
    ) -> Binder {
        let mut binder = Self::new_inner(session, BindFor::Ddl, vec![]);
        binder.secure_compare_context = Some(ctx);
        binder
    }

    pub fn new_for_system(session: &SessionImpl) -> Binder {
        Self::new_inner(session, BindFor::System, vec![])
    }

    pub fn new_for_stream_with_param_types(
        session: &SessionImpl,
        param_types: Vec<Option<DataType>>,
    ) -> Binder {
        Self::new_inner(session, BindFor::Stream, param_types)
    }

    fn is_for_stream(&self) -> bool {
        matches!(self.bind_for, BindFor::Stream)
    }

    #[allow(dead_code)]
    fn is_for_batch(&self) -> bool {
        matches!(self.bind_for, BindFor::Batch)
    }

    fn is_for_ddl(&self) -> bool {
        matches!(self.bind_for, BindFor::Ddl)
    }

    /// Bind a [`Statement`].
    pub fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt)
    }

    pub fn export_param_types(&self) -> Result<Vec<DataType>> {
        self.param_types.export()
    }

    /// Get included relations in the query after binding. This is used for resolving relation
    /// dependencies. Note that it only contains referenced relations discovered during binding.
    /// After the plan is built, the referenced relations may be changed. We cannot rely on the
    /// collection result of plan, because we still need to record the dependencies that have been
    /// optimised away.
    pub fn included_relations(&self) -> &HashSet<TableId> {
        &self.included_relations
    }

    /// Get included user-defined functions in the query after binding.
    pub fn included_udfs(&self) -> &HashSet<FunctionId> {
        &self.included_udfs
    }

    fn push_context(&mut self) {
        let new_context = std::mem::take(&mut self.context);
        self.context
            .cte_to_relation
            .clone_from(&new_context.cte_to_relation);
        let new_lateral_contexts = std::mem::take(&mut self.lateral_contexts);
        self.upper_subquery_contexts
            .push((new_context, new_lateral_contexts));
    }

    fn pop_context(&mut self) -> Result<()> {
        let (old_context, old_lateral_contexts) = self
            .upper_subquery_contexts
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("Popping non-existent context".to_owned()))?;
        self.context = old_context;
        self.lateral_contexts = old_lateral_contexts;
        Ok(())
    }

    fn push_lateral_context(&mut self) {
        let new_context = std::mem::take(&mut self.context);
        self.context
            .cte_to_relation
            .clone_from(&new_context.cte_to_relation);
        self.lateral_contexts.push(LateralBindContext {
            is_visible: false,
            context: new_context,
        });
    }

    fn pop_and_merge_lateral_context(&mut self) -> Result<()> {
        let mut old_context = self
            .lateral_contexts
            .pop()
            .ok_or_else(|| ErrorCode::InternalError("Popping non-existent context".to_owned()))?
            .context;
        old_context.merge_context(self.context.clone())?;
        self.context = old_context;
        Ok(())
    }

    fn try_mark_lateral_as_visible(&mut self) {
        if let Some(mut ctx) = self.lateral_contexts.pop() {
            ctx.is_visible = true;
            self.lateral_contexts.push(ctx);
        }
    }

    fn try_mark_lateral_as_invisible(&mut self) {
        if let Some(mut ctx) = self.lateral_contexts.pop() {
            ctx.is_visible = false;
            self.lateral_contexts.push(ctx);
        }
    }

    fn next_subquery_id(&mut self) -> usize {
        let id = self.next_subquery_id;
        self.next_subquery_id += 1;
        id
    }

    fn next_values_id(&mut self) -> usize {
        let id = self.next_values_id;
        self.next_values_id += 1;
        id
    }

    fn next_share_id(&mut self) -> ShareId {
        let id = self.next_share_id;
        self.next_share_id += 1;
        id
    }

    fn first_valid_schema(&self) -> CatalogResult<&SchemaCatalog> {
        self.catalog.first_valid_schema(
            &self.db_name,
            &self.search_path,
            &self.auth_context.user_name,
        )
    }

    fn bind_schema_path<'a>(&'a self, schema_name: Option<&'a str>) -> SchemaPath<'a> {
        SchemaPath::new(schema_name, &self.search_path, &self.auth_context.user_name)
    }

    pub fn set_clause(&mut self, clause: Option<Clause>) {
        self.context.clause = clause;
    }

    pub fn udf_context_mut(&mut self) -> &mut UdfContext {
        &mut self.udf_context
    }
}

/// The column name stored in [`BindContext`] for a column without an alias.
pub const UNNAMED_COLUMN: &str = "?column?";
/// The table name stored in [`BindContext`] for a subquery without an alias.
const UNNAMED_SUBQUERY: &str = "?subquery?";
/// The table name stored in [`BindContext`] for a column group.
const COLUMN_GROUP_PREFIX: &str = "?column_group_id?";

#[cfg(test)]
pub mod test_utils {
    use risingwave_common::types::DataType;

    use super::Binder;
    use crate::session::SessionImpl;

    #[cfg(test)]
    pub fn mock_binder() -> Binder {
        Binder::new(&SessionImpl::mock())
    }

    #[cfg(test)]
    pub fn mock_binder_with_param_types(param_types: Vec<Option<DataType>>) -> Binder {
        Binder::new_with_param_types(&SessionImpl::mock(), param_types)
    }
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use super::test_utils::*;

    #[tokio::test]
    async fn test_rcte() {
        let stmt = risingwave_sqlparser::parser::Parser::parse_sql(
            "WITH RECURSIVE t1 AS (SELECT 1 AS a UNION ALL SELECT a + 1 FROM t1 WHERE a < 10) SELECT * FROM t1",
        ).unwrap().into_iter().next().unwrap();
        let mut binder = mock_binder();
        let bound = binder.bind(stmt).unwrap();

        let expected = expect![[r#"
            Query(
                BoundQuery {
                    body: Select(
                        BoundSelect {
                            distinct: All,
                            select_items: [
                                InputRef(
                                    InputRef {
                                        index: 0,
                                        data_type: Int32,
                                    },
                                ),
                            ],
                            aliases: [
                                Some(
                                    "a",
                                ),
                            ],
                            from: Some(
                                Share(
                                    BoundShare {
                                        share_id: 0,
                                        input: Query(
                                            Right(
                                                RecursiveUnion {
                                                    all: true,
                                                    base: Select(
                                                        BoundSelect {
                                                            distinct: All,
                                                            select_items: [
                                                                Literal(
                                                                    Literal {
                                                                        data: Some(
                                                                            Int32(
                                                                                1,
                                                                            ),
                                                                        ),
                                                                        data_type: Some(
                                                                            Int32,
                                                                        ),
                                                                    },
                                                                ),
                                                            ],
                                                            aliases: [
                                                                Some(
                                                                    "a",
                                                                ),
                                                            ],
                                                            from: None,
                                                            where_clause: None,
                                                            group_by: GroupKey(
                                                                [],
                                                            ),
                                                            having: None,
                                                            schema: Schema {
                                                                fields: [
                                                                    a:Int32,
                                                                ],
                                                            },
                                                        },
                                                    ),
                                                    recursive: Select(
                                                        BoundSelect {
                                                            distinct: All,
                                                            select_items: [
                                                                FunctionCall(
                                                                    FunctionCall {
                                                                        func_type: Add,
                                                                        return_type: Int32,
                                                                        inputs: [
                                                                            InputRef(
                                                                                InputRef {
                                                                                    index: 0,
                                                                                    data_type: Int32,
                                                                                },
                                                                            ),
                                                                            Literal(
                                                                                Literal {
                                                                                    data: Some(
                                                                                        Int32(
                                                                                            1,
                                                                                        ),
                                                                                    ),
                                                                                    data_type: Some(
                                                                                        Int32,
                                                                                    ),
                                                                                },
                                                                            ),
                                                                        ],
                                                                    },
                                                                ),
                                                            ],
                                                            aliases: [
                                                                None,
                                                            ],
                                                            from: Some(
                                                                BackCteRef(
                                                                    BoundBackCteRef {
                                                                        share_id: 0,
                                                                        base: Select(
                                                                            BoundSelect {
                                                                                distinct: All,
                                                                                select_items: [
                                                                                    Literal(
                                                                                        Literal {
                                                                                            data: Some(
                                                                                                Int32(
                                                                                                    1,
                                                                                                ),
                                                                                            ),
                                                                                            data_type: Some(
                                                                                                Int32,
                                                                                            ),
                                                                                        },
                                                                                    ),
                                                                                ],
                                                                                aliases: [
                                                                                    Some(
                                                                                        "a",
                                                                                    ),
                                                                                ],
                                                                                from: None,
                                                                                where_clause: None,
                                                                                group_by: GroupKey(
                                                                                    [],
                                                                                ),
                                                                                having: None,
                                                                                schema: Schema {
                                                                                    fields: [
                                                                                        a:Int32,
                                                                                    ],
                                                                                },
                                                                            },
                                                                        ),
                                                                    },
                                                                ),
                                                            ),
                                                            where_clause: Some(
                                                                FunctionCall(
                                                                    FunctionCall {
                                                                        func_type: LessThan,
                                                                        return_type: Boolean,
                                                                        inputs: [
                                                                            InputRef(
                                                                                InputRef {
                                                                                    index: 0,
                                                                                    data_type: Int32,
                                                                                },
                                                                            ),
                                                                            Literal(
                                                                                Literal {
                                                                                    data: Some(
                                                                                        Int32(
                                                                                            10,
                                                                                        ),
                                                                                    ),
                                                                                    data_type: Some(
                                                                                        Int32,
                                                                                    ),
                                                                                },
                                                                            ),
                                                                        ],
                                                                    },
                                                                ),
                                                            ),
                                                            group_by: GroupKey(
                                                                [],
                                                            ),
                                                            having: None,
                                                            schema: Schema {
                                                                fields: [
                                                                    ?column?:Int32,
                                                                ],
                                                            },
                                                        },
                                                    ),
                                                    schema: Schema {
                                                        fields: [
                                                            a:Int32,
                                                        ],
                                                    },
                                                },
                                            ),
                                        ),
                                    },
                                ),
                            ),
                            where_clause: None,
                            group_by: GroupKey(
                                [],
                            ),
                            having: None,
                            schema: Schema {
                                fields: [
                                    a:Int32,
                                ],
                            },
                        },
                    ),
                    order: [],
                    limit: None,
                    offset: None,
                    with_ties: false,
                    extra_order_exprs: [],
                },
            )"#]];

        expected.assert_eq(&format!("{:#?}", bound));
    }

    #[tokio::test]
    async fn test_bind_approx_percentile() {
        let stmt = risingwave_sqlparser::parser::Parser::parse_sql(
            "SELECT approx_percentile(0.5, 0.01) WITHIN GROUP (ORDER BY generate_series) FROM generate_series(1, 100)",
        ).unwrap().into_iter().next().unwrap();
        let parse_expected = expect![[r#"
            Query(
                Query {
                    with: None,
                    body: Select(
                        Select {
                            distinct: All,
                            projection: [
                                UnnamedExpr(
                                    Function(
                                        Function {
                                            scalar_as_agg: false,
                                            name: ObjectName(
                                                [
                                                    Ident {
                                                        value: "approx_percentile",
                                                        quote_style: None,
                                                    },
                                                ],
                                            ),
                                            arg_list: FunctionArgList {
                                                distinct: false,
                                                args: [
                                                    Unnamed(
                                                        Expr(
                                                            Value(
                                                                Number(
                                                                    "0.5",
                                                                ),
                                                            ),
                                                        ),
                                                    ),
                                                    Unnamed(
                                                        Expr(
                                                            Value(
                                                                Number(
                                                                    "0.01",
                                                                ),
                                                            ),
                                                        ),
                                                    ),
                                                ],
                                                variadic: false,
                                                order_by: [],
                                                ignore_nulls: false,
                                            },
                                            within_group: Some(
                                                OrderByExpr {
                                                    expr: Identifier(
                                                        Ident {
                                                            value: "generate_series",
                                                            quote_style: None,
                                                        },
                                                    ),
                                                    asc: None,
                                                    nulls_first: None,
                                                },
                                            ),
                                            filter: None,
                                            over: None,
                                        },
                                    ),
                                ),
                            ],
                            from: [
                                TableWithJoins {
                                    relation: TableFunction {
                                        name: ObjectName(
                                            [
                                                Ident {
                                                    value: "generate_series",
                                                    quote_style: None,
                                                },
                                            ],
                                        ),
                                        alias: None,
                                        args: [
                                            Unnamed(
                                                Expr(
                                                    Value(
                                                        Number(
                                                            "1",
                                                        ),
                                                    ),
                                                ),
                                            ),
                                            Unnamed(
                                                Expr(
                                                    Value(
                                                        Number(
                                                            "100",
                                                        ),
                                                    ),
                                                ),
                                            ),
                                        ],
                                        with_ordinality: false,
                                    },
                                    joins: [],
                                },
                            ],
                            lateral_views: [],
                            selection: None,
                            group_by: [],
                            having: None,
                        },
                    ),
                    order_by: [],
                    limit: None,
                    offset: None,
                    fetch: None,
                },
            )"#]];
        parse_expected.assert_eq(&format!("{:#?}", stmt));

        let mut binder = mock_binder();
        let bound = binder.bind(stmt).unwrap();

        let expected = expect![[r#"
            Query(
                BoundQuery {
                    body: Select(
                        BoundSelect {
                            distinct: All,
                            select_items: [
                                AggCall(
                                    AggCall {
                                        agg_type: Builtin(
                                            ApproxPercentile,
                                        ),
                                        return_type: Float64,
                                        args: [
                                            FunctionCall(
                                                FunctionCall {
                                                    func_type: Cast,
                                                    return_type: Float64,
                                                    inputs: [
                                                        InputRef(
                                                            InputRef {
                                                                index: 0,
                                                                data_type: Int32,
                                                            },
                                                        ),
                                                    ],
                                                },
                                            ),
                                        ],
                                        filter: Condition {
                                            conjunctions: [],
                                        },
                                        distinct: false,
                                        order_by: OrderBy {
                                            sort_exprs: [
                                                OrderByExpr {
                                                    expr: InputRef(
                                                        InputRef {
                                                            index: 0,
                                                            data_type: Int32,
                                                        },
                                                    ),
                                                    order_type: OrderType {
                                                        direction: Ascending,
                                                        nulls_are: Largest,
                                                    },
                                                },
                                            ],
                                        },
                                        direct_args: [
                                            Literal {
                                                data: Some(
                                                    Float64(
                                                        OrderedFloat(
                                                            0.5,
                                                        ),
                                                    ),
                                                ),
                                                data_type: Some(
                                                    Float64,
                                                ),
                                            },
                                            Literal {
                                                data: Some(
                                                    Float64(
                                                        OrderedFloat(
                                                            0.01,
                                                        ),
                                                    ),
                                                ),
                                                data_type: Some(
                                                    Float64,
                                                ),
                                            },
                                        ],
                                    },
                                ),
                            ],
                            aliases: [
                                Some(
                                    "approx_percentile",
                                ),
                            ],
                            from: Some(
                                TableFunction {
                                    expr: TableFunction(
                                        FunctionCall {
                                            function_type: GenerateSeries,
                                            return_type: Int32,
                                            args: [
                                                Literal(
                                                    Literal {
                                                        data: Some(
                                                            Int32(
                                                                1,
                                                            ),
                                                        ),
                                                        data_type: Some(
                                                            Int32,
                                                        ),
                                                    },
                                                ),
                                                Literal(
                                                    Literal {
                                                        data: Some(
                                                            Int32(
                                                                100,
                                                            ),
                                                        ),
                                                        data_type: Some(
                                                            Int32,
                                                        ),
                                                    },
                                                ),
                                            ],
                                        },
                                    ),
                                    with_ordinality: false,
                                },
                            ),
                            where_clause: None,
                            group_by: GroupKey(
                                [],
                            ),
                            having: None,
                            schema: Schema {
                                fields: [
                                    approx_percentile:Float64,
                                ],
                            },
                        },
                    ),
                    order: [],
                    limit: None,
                    offset: None,
                    with_ties: false,
                    extra_order_exprs: [],
                },
            )"#]];

        expected.assert_eq(&format!("{:#?}", bound));
    }
}
