// Copyright 2023 Singularity Data
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

use rand::prelude::SliceRandom;
use rand::Rng;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Ident, ObjectName, TableAlias, TableFactor, TableWithJoins};

use crate::sql_gen::{Column, SqlGenerator, SqlGeneratorContext};
use crate::{BinaryOperator, Expr, Join, JoinConstraint, JoinOperator, Table};

fn create_join_on_clause(left: String, right: String) -> Expr {
    let left = Box::new(Expr::Identifier(Ident::new(left)));
    let right = Box::new(Expr::Identifier(Ident::new(right)));
    Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    }
}

impl<'a, R: Rng> SqlGenerator<'a, R> {
    /// A relation specified in the FROM clause.
    pub(crate) fn gen_from_relation(&mut self) -> (TableWithJoins, Vec<Table>) {
        let range = if self.can_recurse() { 10 } else { 9 };
        match self.rng.gen_range(0..=range) {
            0..=7 => self.gen_simple_table(),
            8..=8 => self.gen_time_window_func(),
            // TODO: Enable after resolving: <https://github.com/risingwavelabs/risingwave/issues/2771>.
            9..=9 => self.gen_join_clause(),
            10..=10 => self.gen_table_subquery(),
            _ => unreachable!(),
        }
    }

    fn gen_simple_table(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (relation, _, table) = self.gen_simple_table_factor();

        (
            TableWithJoins {
                relation,
                joins: vec![],
            },
            table,
        )
    }

    fn gen_simple_table_factor(&mut self) -> (TableFactor, Vec<Column>, Vec<Table>) {
        let alias = self.gen_table_name_with_prefix("t");
        let mut table = self.tables.choose(&mut self.rng).unwrap().clone();
        let table_factor = TableFactor::Table {
            name: ObjectName(vec![Ident::new(&table.name)]),
            alias: Some(TableAlias {
                name: alias.as_str().into(),
                columns: vec![],
            }),
        };
        table.name = alias; // Rename the table.
        let columns = table.get_qualified_columns();
        (table_factor, columns, vec![table])
    }

    fn gen_table_factor(&mut self) -> (TableFactor, Vec<Column>, Vec<Table>) {
        let current_context = self.new_local_context();
        let factor = self.gen_table_factor_inner();
        self.restore_context(current_context);
        factor
    }

    /// Generates a table factor, and provides bound columns.
    /// Generated column names should be qualified by table name.
    fn gen_table_factor_inner(&mut self) -> (TableFactor, Vec<Column>, Vec<Table>) {
        // TODO: TableFactor::Derived, TableFactor::TableFunction, TableFactor::NestedJoin
        self.gen_simple_table_factor()
    }

    fn gen_join_constraint(&mut self, available_join_on_columns: Vec<(&Column, &Column)>) -> JoinConstraint {
        match self.rng.gen_range(0..=5) {
            0..=3 => {
                let i = self.rng.gen_range(0..available_join_on_columns.len());
                let (left_column, right_column) = available_join_on_columns[i];
                let join_on_expr = match self.rng.gen_bool(0.9) {
                    true => create_join_on_clause(left_column.name.clone(), right_column.name.clone()),
                    false => self.gen_simple_scalar(&DataType::Boolean),
                };
                JoinConstraint::On(join_on_expr)
            },
            4 => JoinConstraint::Natural,
            _ => JoinConstraint::None,
        }
    }

    fn gen_join_clause(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (left_factor, left_columns, mut left_table) = self.gen_table_factor();
        let (right_factor, right_columns, mut right_table) = self.gen_table_factor();

        let mut available_join_on_columns = vec![];
        for left_column in &left_columns {
            for right_column in &right_columns {
                if left_column.data_type == right_column.data_type {
                    available_join_on_columns.push((left_column, right_column))
                }
            }
        }
        if available_join_on_columns.is_empty() {
            return self.gen_simple_table();
        }
        let join_constraint = self.gen_join_constraint(available_join_on_columns);

        // NOTE: INNER JOIN works fine, usually does not encounter `StreamNestedLoopJoin` much.
        // If many failures due to `StreamNestedLoopJoin`, try disable the others.
        let join_operator = match self.rng.gen_range(0..=4) {
            0 => JoinOperator::Inner(join_constraint),
            1 => JoinOperator::LeftOuter(join_constraint),
            2 => JoinOperator::RightOuter(join_constraint),
            3 => JoinOperator::FullOuter(join_constraint),
            _ => JoinOperator::CrossJoin,
        };

        let right_factor_with_join = Join {
            relation: right_factor,
            join_operator,
        };
        left_table.append(&mut right_table);
        (
            TableWithJoins {
                relation: left_factor,
                joins: vec![right_factor_with_join],
            },
            left_table,
        )
    }

    fn gen_table_subquery(&mut self) -> (TableWithJoins, Vec<Table>) {
        let (subquery, columns) = self.gen_local_query();
        let alias = self.gen_table_name_with_prefix("sq");
        let table = Table {
            name: alias.clone(),
            columns,
        };
        let relation = TableWithJoins {
            relation: TableFactor::Derived {
                lateral: false,
                subquery: Box::new(subquery),
                alias: Some(TableAlias {
                    name: Ident::new(alias),
                    columns: vec![],
                }),
            },
            joins: vec![],
        };

        (relation, vec![table])
    }
}
