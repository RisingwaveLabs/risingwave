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
use std::fmt;

use fixedbitset::FixedBitSet;
use pretty_xmlish::{Pretty, StrAssocArr};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::util::iter_util::ZipEqFast;

use super::{GenericPlanNode, GenericPlanRef};
use crate::expr::{
    assert_input_ref, Expr, ExprDisplay, ExprImpl, ExprRewriter, ExprType, ExprVisitor,
    FunctionCall, InputRef,
};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt};

fn check_expr_type(expr: &ExprImpl) -> std::result::Result<(), &'static str> {
    if expr.has_subquery() {
        return Err("subquery");
    }
    if expr.has_agg_call() {
        return Err("aggregate function");
    }
    if expr.has_table_function() {
        return Err("table function");
    }
    if expr.has_window_function() {
        return Err("window function");
    }
    Ok(())
}

/// [`Project`] computes a set of expressions from its input relation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(clippy::manual_non_exhaustive)]
pub struct Project<PlanRef> {
    pub exprs: Vec<ExprImpl>,
    /// Mapping from expr index to field name. May not contain all exprs.
    pub field_names: BTreeMap<usize, String>,
    pub input: PlanRef,
    // we need some check when construct the `Project::new`
    _private: (),
}

impl<PlanRef> Project<PlanRef> {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.exprs = self
            .exprs
            .iter()
            .map(|e| r.rewrite_expr(e.clone()))
            .collect();
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.exprs.iter().for_each(|e| v.visit_expr(e));
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Project<PlanRef> {
    fn schema(&self) -> Schema {
        let o2i = self.o2i_col_mapping();
        let exprs = &self.exprs;
        let input_schema = self.input.schema();
        let ctx = self.ctx();
        let fields = exprs
            .iter()
            .enumerate()
            .map(|(i, expr)| {
                // Get field info from o2i.
                let name = match o2i.try_map(i) {
                    Some(input_idx) => {
                        if let Some(name) = self.field_names.get(&i) {
                            name.clone()
                        } else {
                            input_schema.fields()[input_idx].name.clone()
                        }
                    }
                    None => match expr {
                        ExprImpl::InputRef(_) | ExprImpl::Literal(_) => {
                            format!("{:?}", ExprDisplay { expr, input_schema })
                        }
                        _ => {
                            if let Some(name) = self.field_names.get(&i) {
                                name.clone()
                            } else {
                                format!("$expr{}", ctx.next_expr_display_id())
                            }
                        }
                    },
                };
                Field::with_name(expr.return_type(), name)
            })
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let i2o = self.i2o_col_mapping();
        self.input
            .stream_key()?
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let i2o = self.i2o_col_mapping();
        i2o.rewrite_functional_dependency_set(self.input.functional_dependency().clone())
    }
}

impl<PlanRef: GenericPlanRef> Project<PlanRef> {
    pub fn new(exprs: Vec<ExprImpl>, input: PlanRef) -> Self {
        for expr in &exprs {
            assert_input_ref!(expr, input.schema().fields().len());
            check_expr_type(expr)
                .map_err(|expr| format!("{expr} should not in Project operator"))
                .unwrap();
        }
        Project {
            exprs,
            field_names: Default::default(),
            input,
            _private: (),
        }
    }

    /// Creates a `Project` which select some columns from the input.
    ///
    /// `mapping` should maps from `(0..input_fields.len())` to a consecutive range starting from 0.
    ///
    /// This is useful in column pruning when we want to add a project to ensure the output schema
    /// is correct.
    pub fn with_mapping(input: PlanRef, mapping: ColIndexMapping) -> Self {
        if mapping.target_size() == 0 {
            // The mapping is empty, so the parent actually doesn't need the output of the input.
            // This can happen when the parent node only selects constant expressions.
            return Self::new(vec![], input);
        };
        let mut input_refs = vec![None; mapping.target_size()];
        for (src, tar) in mapping.mapping_pairs() {
            assert_eq!(input_refs[tar], None);
            input_refs[tar] = Some(src);
        }
        let input_schema = input.schema();
        let exprs: Vec<ExprImpl> = input_refs
            .into_iter()
            .map(|i| i.unwrap())
            .map(|i| InputRef::new(i, input_schema.fields()[i].data_type()).into())
            .collect();

        Self::new(exprs, input)
    }

    /// Creates a `Project` which select some columns from the input.
    pub fn with_out_fields(input: PlanRef, out_fields: &FixedBitSet) -> Self {
        Self::with_out_col_idx(input, out_fields.ones())
    }

    /// Creates a `Project` which select some columns from the input.
    pub fn with_out_col_idx(input: PlanRef, out_fields: impl Iterator<Item = usize>) -> Self {
        let input_schema = input.schema();
        let exprs = out_fields
            .map(|index| InputRef::new(index, input_schema[index].data_type()).into())
            .collect();
        Self::new(exprs, input)
    }

    /// Creates a `Project` with an additional `_vnode` column at the end of the schema.
    pub fn with_vnode_col(input: PlanRef, dist_key: &[usize]) -> Self {
        let input_fields = input.schema().fields();
        let mut new_exprs: Vec<_> = input_fields
            .iter()
            .enumerate()
            .map(|(idx, field)| InputRef::new(idx, field.data_type.clone()).into())
            .collect();
        new_exprs.push(
            FunctionCall::new(
                ExprType::Vnode,
                dist_key
                    .iter()
                    .map(|idx| InputRef::new(*idx, input_fields[*idx].data_type()).into())
                    .collect(),
            )
            .expect("Vnode function call should be valid here")
            .into(),
        );
        let vnode_expr_idx = new_exprs.len() - 1;

        let mut new = Self::new(new_exprs, input);
        new.field_names.insert(vnode_expr_idx, "_vnode".to_owned());
        new
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        (self.exprs, self.input)
    }

    pub fn fields_pretty<'a>(&self, schema: &Schema) -> StrAssocArr<'a> {
        let f = |t| Pretty::debug(&t);
        let e = Pretty::Array(self.exprs_for_display(schema).iter().map(f).collect());
        vec![("exprs", e)]
    }

    fn exprs_for_display<'a>(&'a self, schema: &Schema) -> Vec<AliasedExpr<'a>> {
        self.exprs
            .iter()
            .zip_eq_fast(schema.fields().iter())
            .map(|(expr, field)| AliasedExpr {
                expr: ExprDisplay {
                    expr,
                    input_schema: self.input.schema(),
                },
                alias: {
                    match expr {
                        ExprImpl::InputRef(_) | ExprImpl::Literal(_) => None,
                        _ => Some(field.name.clone()),
                    }
                },
            })
            .collect()
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let exprs = &self.exprs;
        let input_len = self.input.schema().len();
        let mut map = vec![None; exprs.len()];
        for (i, expr) in exprs.iter().enumerate() {
            if let ExprImpl::InputRef(input) = expr {
                map[i] = Some(input.index())
            }
        }
        ColIndexMapping::new(map, input_len)
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        let exprs = &self.exprs;
        let input_len = self.input.schema().len();
        let mut map = vec![None; input_len];
        for (i, expr) in exprs.iter().enumerate() {
            if let ExprImpl::InputRef(input) = expr {
                map[input.index()] = Some(i)
            }
        }
        ColIndexMapping::new(map, exprs.len())
    }

    pub fn is_all_inputref(&self) -> bool {
        self.exprs
            .iter()
            .all(|expr| matches!(expr, ExprImpl::InputRef(_)))
    }

    pub fn is_identity(&self) -> bool {
        self.exprs.len() == self.input.schema().len()
        && self
            .exprs
            .iter()
            .zip_eq_fast(self.input.schema().fields())
            .enumerate()
            .all(|(i, (expr, field))| {
                matches!(expr, ExprImpl::InputRef(input_ref) if **input_ref == InputRef::new(i, field.data_type()))
            })
    }

    pub fn try_as_projection(&self) -> Option<Vec<usize>> {
        self.exprs
            .iter()
            .map(|expr| match expr {
                ExprImpl::InputRef(input_ref) => Some(input_ref.index),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
    }

    pub(crate) fn likely_produces_noop_updates(&self) -> bool {
        struct HasJsonbAccess {
            has: bool,
        }

        impl ExprVisitor for HasJsonbAccess {
            fn visit_function_call(&mut self, func_call: &FunctionCall) {
                if matches!(
                    func_call.func_type(),
                    ExprType::JsonbAccess
                        | ExprType::JsonbAccessStr
                        | ExprType::JsonbExtractPath
                        | ExprType::JsonbExtractPathVariadic
                        | ExprType::JsonbExtractPathText
                        | ExprType::JsonbExtractPathTextVariadic
                        | ExprType::JsonbPathExists
                        | ExprType::JsonbPathMatch
                        | ExprType::JsonbPathQueryArray
                        | ExprType::JsonbPathQueryFirst
                ) {
                    self.has = true;
                }
            }
        }

        self.exprs.iter().any(|expr| {
            // When there's a jsonb access in the `Project`, it's very likely that the query is
            // extracting some fields from a jsonb payload column. In this case, a change from the
            // input jsonb payload may not change the output of the `Project`.
            let mut visitor = HasJsonbAccess { has: false };
            visitor.visit_expr(expr);
            visitor.has
        })
    }
}

/// Construct a `Project` and dedup expressions.
/// expressions
#[derive(Default)]
pub struct ProjectBuilder {
    exprs: Vec<ExprImpl>,
    exprs_index: HashMap<ExprImpl, usize>,
}

impl ProjectBuilder {
    /// add an expression to the `LogicalProject` and return the column index of the project's
    /// output
    pub fn add_expr(&mut self, expr: &ExprImpl) -> std::result::Result<usize, &'static str> {
        check_expr_type(expr)?;
        if let Some(idx) = self.exprs_index.get(expr) {
            Ok(*idx)
        } else {
            let index = self.exprs.len();
            self.exprs.push(expr.clone());
            self.exprs_index.insert(expr.clone(), index);
            Ok(index)
        }
    }

    pub fn get_expr(&self, index: usize) -> Option<&ExprImpl> {
        self.exprs.get(index)
    }

    pub fn expr_index(&self, expr: &ExprImpl) -> Option<usize> {
        check_expr_type(expr).ok()?;
        self.exprs_index.get(expr).copied()
    }

    /// build the `LogicalProject` from `LogicalProjectBuilder`
    pub fn build<PlanRef: GenericPlanRef>(self, input: PlanRef) -> Project<PlanRef> {
        Project::new(self.exprs, input)
    }

    pub fn exprs_len(&self) -> usize {
        self.exprs.len()
    }
}

/// Auxiliary struct for displaying `expr AS alias`
pub struct AliasedExpr<'a> {
    pub expr: ExprDisplay<'a>,
    pub alias: Option<String>,
}

impl fmt::Debug for AliasedExpr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.alias {
            Some(alias) => write!(f, "{:?} as {}", self.expr, alias),
            None => write!(f, "{:?}", self.expr),
        }
    }
}
