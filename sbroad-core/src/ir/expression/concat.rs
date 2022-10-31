use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::{Node, Plan};

impl Plan {
    /// Add concatenation expression to the IR plan.
    ///
    /// # Errors
    /// - Left or right child nodes are not of the expression type.
    pub fn add_concat(
        &mut self,
        left_id: usize,
        right_id: usize,
    ) -> Result<usize, QueryPlannerError> {
        // Check that both children are of expression type.
        for child_id in &[left_id, right_id] {
            self.get_expression_node(*child_id)?;
        }
        let concat_id = self.nodes.push(Node::Expression(Expression::Concat {
            left: left_id,
            right: right_id,
        }));
        Ok(concat_id)
    }
}