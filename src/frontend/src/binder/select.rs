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

use std::fmt::Debug;

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_expr::expr::AggKind;
use risingwave_sqlparser::ast::{Expr, Select, SelectItem};

use super::bind_context::{Clause, ColumnBinding};
use super::UNNAMED_COLUMN;
use crate::binder::{Binder, Relation};
use crate::catalog::check_valid_column_name;
use crate::expr::{Expr as _, ExprImpl, ExprType, ExprVisitor, GetFieldDesc, InputRef};

#[derive(Debug)]
pub struct BoundSelect {
    pub distinct: bool,
    pub select_items: Vec<ExprImpl>,
    pub aliases: Vec<Option<String>>,
    pub from: Option<Relation>,
    pub where_clause: Option<ExprImpl>,
    pub group_by: Vec<ExprImpl>,
    pub schema: Schema,
}

impl BoundSelect {
    /// The names returned by this [`BoundSelect`].
    pub fn names(&self) -> Vec<String> {
        self.aliases
            .iter()
            .cloned()
            .map(|alias| alias.unwrap_or_else(|| UNNAMED_COLUMN.to_string()))
            .collect()
    }

    /// The types returned by this [`BoundSelect`].
    pub fn data_types(&self) -> Vec<DataType> {
        self.select_items
            .iter()
            .map(|item| item.return_type())
            .collect()
    }

    pub fn is_correlated(&self) -> bool {
        self.select_items
            .iter()
            .chain(self.group_by.iter())
            .chain(self.where_clause.iter())
            .any(|expr| expr.has_correlated_input_ref())
    }
}

impl Binder {
    pub(super) fn bind_select(&mut self, select: Select) -> Result<BoundSelect> {
        // Bind FROM clause.
        let from = self.bind_vec_table_with_joins(select.from)?;

        // Bind WHERE clause.
        self.context.clause = Some(Clause::Where);
        let selection = select
            .selection
            .map(|expr| self.bind_expr(expr))
            .transpose()?;
        self.context.clause = None;

        if let Some(selection) = &selection {
            let return_type = selection.return_type();
            if return_type != DataType::Boolean {
                return Err(ErrorCode::InternalError(format!(
                    "argument of WHERE must be boolean, not type {:?}",
                    return_type
                ))
                .into());
            }
        }

        // Bind GROUP BY clause.
        let group_by = select
            .group_by
            .into_iter()
            .map(|expr| self.bind_expr(expr))
            .try_collect()?;

        // Bind SELECT clause.
        let (select_items, aliases) = self.bind_project(select.projection)?;

        // Store field from `ExprImpl` to support binding `field_desc` in `subquery`.
        let fields = select_items
            .iter()
            .zip_eq(aliases.iter())
            .map(|(s, a)| {
                let name = a.clone().unwrap_or_else(|| UNNAMED_COLUMN.to_string());
                self.expr_to_field(s, name)
            })
            .collect::<Result<Vec<Field>>>()?;

        Ok(BoundSelect {
            distinct: select.distinct,
            select_items,
            aliases,
            from,
            where_clause: selection,
            group_by,
            schema: Schema { fields },
        })
    }

    pub fn bind_project(
        &mut self,
        select_items: Vec<SelectItem>,
    ) -> Result<(Vec<ExprImpl>, Vec<Option<String>>)> {
        let mut select_list = vec![];
        let mut aliases = vec![];
        for item in select_items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let (select_expr, alias) = match &expr.clone() {
                        Expr::Identifier(ident) => {
                            (self.bind_expr(expr)?, Some(ident.value.clone()))
                        }
                        Expr::CompoundIdentifier(idents) => (
                            self.bind_expr(expr)?,
                            idents.last().map(|ident| ident.value.clone()),
                        ),
                        Expr::FieldIdentifier(field_expr, idents) => (
                            self.bind_single_field_column(*field_expr.clone(), idents)?,
                            idents.last().map(|ident| ident.value.clone()),
                        ),
                        _ => (self.bind_expr(expr)?, None),
                    };
                    select_list.push(select_expr);
                    aliases.push(alias);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    check_valid_column_name(&alias.value)?;

                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                    aliases.push(Some(alias.value));
                }
                SelectItem::QualifiedWildcard(obj_name) => {
                    let table_name = &obj_name.0.last().unwrap().value;
                    let (begin, end) = self.context.range_of.get(table_name).ok_or_else(|| {
                        ErrorCode::ItemNotFound(format!("relation \"{}\"", table_name))
                    })?;
                    let (exprs, names) =
                        Self::iter_bound_columns(self.context.columns[*begin..*end].iter());
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
                SelectItem::ExprQualifiedWildcard(expr, idents) => {
                    let (exprs, names) = self.bind_wildcard_field_column(expr, &idents.0)?;
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
                SelectItem::Wildcard => {
                    let (exprs, names) = Self::iter_bound_columns(
                        self.context.columns[..].iter().filter(|c| !c.is_hidden),
                    );
                    select_list.extend(exprs);
                    aliases.extend(names);
                }
            }
        }
        Ok((select_list, aliases))
    }

    pub fn iter_bound_columns<'a>(
        column_binding: impl Iterator<Item = &'a ColumnBinding>,
    ) -> (Vec<ExprImpl>, Vec<Option<String>>) {
        column_binding
            .map(|c| {
                (
                    InputRef::new(c.index, c.desc.data_type.clone()).into(),
                    Some(c.desc.name.clone()),
                )
            })
            .unzip()
    }

    /// Only struct return type need to transfer to Field.
    /// Now we accept `InputRef`, Field `FunctionCall` and Min and Max `AggCall` to return struct.
    /// Use `GetFieldDesc Visitor` to find `field_desc`.
    pub fn expr_to_field(&self, item: &ExprImpl, name: String) -> Result<Field> {
        let err = Err(ErrorCode::BindError(format!(
            "Not support this expr type to return struct data_type {:?}",
            item
        ))
        .into());
        let is_field_select = {
            if let DataType::Struct { .. } = item.return_type() {
                match item {
                    ExprImpl::InputRef(_) => true,
                    ExprImpl::FunctionCall(function) => match function.get_expr_type() {
                        ExprType::Field => true,
                        _ => {
                            return err;
                        }
                    },
                    ExprImpl::AggCall(agg) => match agg.agg_kind() {
                        AggKind::Max | AggKind::Min => true,
                        _ => {
                            return err;
                        }
                    },
                    _ => false,
                }
            } else {
                false
            }
        };
        if is_field_select {
            let mut visitor = GetFieldDesc::new(self.context.columns.clone());
            visitor.visit_expr(item);
            match visitor.collect() {
                None => {
                    return Err(ErrorCode::BindError(format!(
                        "Not find field_descs for struct return expr {:?}",
                        item
                    ))
                    .into());
                }
                Some(column) => Ok(Field::with_struct(
                    item.return_type(),
                    name,
                    column.field_descs.iter().map(|f| f.into()).collect_vec(),
                    column.type_name,
                )),
            }
        } else {
            Ok(Field::with_name(item.return_type(), name))
        }
    }
}
