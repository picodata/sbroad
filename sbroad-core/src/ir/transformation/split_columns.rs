//! Split the tuples to columns in the expression trees of the plan.
//!
//! The transformation splits the tuples in the boolean operators into
//! the AND-ed chain of the single-column tuples.
//!
//! For example:
//! ```sql
//!    select a from t where (a, 2) = (1, b)
//! ```
//! is transformed to:
//! ```sql
//!   select a from t where (a) = (1) and (2) = (b)
//! ```

use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::Bool;
use crate::ir::Plan;
use crate::otm::child_span;
use sbroad_proc::otm_child_span;

fn call_expr_tree_split_columns(
    plan: &mut Plan,
    top_id: usize,
) -> Result<usize, QueryPlannerError> {
    plan.expr_tree_replace_bool(
        top_id,
        &call_split_bool,
        &[
            Bool::Eq,
            Bool::Gt,
            Bool::GtEq,
            Bool::Lt,
            Bool::LtEq,
            Bool::NotEq,
        ],
    )
}

fn call_split_bool(plan: &mut Plan, top_id: usize) -> Result<usize, QueryPlannerError> {
    plan.split_bool(top_id)
}

impl Plan {
    /// Replace left and right tuples in the boolean operator with the chain
    /// of the AND-ed operators constructed from the tuple's columns.
    ///
    /// # Errors
    /// - If the operator is not a boolean operator.
    /// - If left and right tuples have different number of columns.
    /// - If the plan is invalid for some unknown reason.
    fn split_bool(&mut self, expr_id: usize) -> Result<usize, QueryPlannerError> {
        let expr = self.get_expression_node(expr_id)?;
        let (left_id, right_id, op) = match expr {
            Expression::Bool {
                left, op, right, ..
            } => (*left, *right, op.clone()),
            _ => {
                return Err(QueryPlannerError::CustomError(format!(
                    "Node is not a boolean expression: {:?}",
                    expr
                )));
            }
        };
        let left_expr = self.get_expression_node(left_id)?;
        let right_expr = self.get_expression_node(right_id)?;
        if let (
            Expression::Row {
                list: left_list, ..
            },
            Expression::Row {
                list: right_list, ..
            },
        ) = (left_expr, right_expr)
        {
            if left_list.len() != right_list.len() {
                return Err(QueryPlannerError::CustomError(format!(
                    "Left and right rows have different number of columns: {:?}, {:?}",
                    left_expr, right_expr
                )));
            }
            let pairs = left_list
                .iter()
                .zip(right_list.iter())
                .map(|(l, r)| (*l, *r))
                .collect::<Vec<_>>();
            if let Some((first, other)) = pairs.split_first() {
                let left_col_id = first.0;
                let right_col_id = first.1;
                let left_row_id = self.nodes.add_row(vec![left_col_id], None);
                let right_row_id = self.nodes.add_row(vec![right_col_id], None);
                let mut top_id = self.add_cond(left_row_id, op.clone(), right_row_id)?;

                for (left_col_id, right_col_id) in other {
                    let left_col_id = *left_col_id;
                    let right_col_id = *right_col_id;
                    let left_row_id = self.nodes.add_row(vec![left_col_id], None);
                    let right_row_id = self.nodes.add_row(vec![right_col_id], None);
                    let new_top_id = self.add_cond(left_row_id, op.clone(), right_row_id)?;
                    top_id = self.concat_and(top_id, new_top_id)?;
                }

                return Ok(top_id);
            }
        }
        Ok(expr_id)
    }

    /// Split columns in all the boolean operators of the plan.
    ///
    /// # Errors
    /// - If the plan tree is invalid (doesn't contain correct nodes where we expect it to).
    #[otm_child_span("plan.transformation.split_columns")]
    pub fn split_columns(&mut self) -> Result<(), QueryPlannerError> {
        self.transform_expr_trees(&call_expr_tree_split_columns)
    }
}

#[cfg(test)]
mod tests;
