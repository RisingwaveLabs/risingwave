use risingwave_common::error::RwError;

use crate::expr::{ExprRewriter, ExprImpl, Literal, Expr};

pub(crate) struct ConstEvalRewriter {
    pub(crate) error: Option<RwError>,
}
impl ExprRewriter for ConstEvalRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        if self.error.is_some() {
            return expr;
        }
        if expr.is_const() { //} && expr.count_nows() == 0 {
            let data_type = expr.return_type();
            match expr.eval_row_const() {
                Ok(datum) => Literal::new(datum, data_type).into(),
                Err(e) => {
                    self.error = Some(e);
                    expr
                }
            }
        } else {
            match expr {
                ExprImpl::InputRef(inner) => self.rewrite_input_ref(*inner),
                ExprImpl::Literal(inner) => self.rewrite_literal(*inner),
                ExprImpl::FunctionCall(inner) => self.rewrite_function_call(*inner),
                ExprImpl::AggCall(inner) => self.rewrite_agg_call(*inner),
                ExprImpl::Subquery(inner) => self.rewrite_subquery(*inner),
                ExprImpl::CorrelatedInputRef(inner) => {
                    self.rewrite_correlated_input_ref(*inner)
                }
                ExprImpl::TableFunction(inner) => self.rewrite_table_function(*inner),
                ExprImpl::WindowFunction(inner) => self.rewrite_window_function(*inner),
                ExprImpl::UserDefinedFunction(inner) => {
                    self.rewrite_user_defined_function(*inner)
                }
            }
        }
    }
}
