//! Replace all boolean "IN": operators with a chian of equalities,
//! combined by "OR" operator.
//!
//! For example, the following query:
//! ```sql
//! SELECT * FROM t WHERE a IN (1, 2, 3)
//! ```
//! would be converted to:
//! ```sql
//! SELECT * FROM t WHERE (a = 1) or (a = 2) or (a = 3)
//! ```

use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::Bool;
use crate::ir::Plan;

/// Replace IN operator with the chain of the OR-ed equalities in the expression tree.
fn call_expr_tree_replace_in(plan: &mut Plan, top_id: usize) -> Result<usize, QueryPlannerError> {
    plan.expr_tree_replace_bool(top_id, &call_from_in, &[Bool::In])
}

fn call_from_in(plan: &mut Plan, top_id: usize) -> Result<usize, QueryPlannerError> {
    plan.from_in(top_id)
}

impl Plan {
    /// Convert the IN operator to the chain of the OR-ed equalities.
    fn from_in(&mut self, expr_id: usize) -> Result<usize, QueryPlannerError> {
        let expr = self.get_expression_node(expr_id)?;
        let (left_id, right_id) = match expr {
            Expression::Bool {
                left,
                op: Bool::In,
                right,
                ..
            } => (*left, *right),
            _ => {
                return Err(QueryPlannerError::CustomError(format!(
                    "Node is not a boolean IN expression: {:?}",
                    expr
                )));
            }
        };

        // To not apply current transformation to motion and sub-query nodes.
        if self.get_motion_from_row(right_id)?.is_some()
            || self.get_sub_query_from_row_node(right_id)?.is_some()
        {
            return Ok(expr_id);
        }

        let right_columns = self.get_expression_node(right_id)?.extract_row_list()?;
        if let Some((first_id, other)) = right_columns.split_first() {
            let new_left_id = self.expr_clone(left_id)?;

            let first_expr = self.get_expression_node(*first_id)?;
            let mut top_id = if first_expr.is_row() {
                self.add_cond(new_left_id, Bool::Eq, *first_id)?
            } else {
                let new_row_id = self.nodes.add_row(vec![*first_id], None);
                self.add_cond(new_left_id, Bool::Eq, new_row_id)?
            };

            for right_id in other {
                let right_expr = self.get_expression_node(*right_id)?;
                let new_right_id = if right_expr.is_row() {
                    self.add_cond(new_left_id, Bool::Eq, *right_id)?
                } else {
                    let new_row_id = self.nodes.add_row(vec![*right_id], None);
                    self.add_cond(new_left_id, Bool::Eq, new_row_id)?
                };
                top_id = self.concat_or(top_id, new_right_id)?;
            }

            return Ok(top_id);
        }
        Ok(expr_id)
    }

    /// Replace all IN operators with the OR-ed chain of equalities.
    ///
    /// # Errors
    /// - If the plan tree is invalid (doesn't contain correct nodes where we expect it to).
    pub fn replace_in_operator(&mut self) -> Result<(), QueryPlannerError> {
        self.transform_expr_trees(&call_expr_tree_replace_in)
    }
}

#[cfg(test)]
mod tests;
