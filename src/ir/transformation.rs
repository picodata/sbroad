//! Plan tree transformation module.
//!
//! Contains rule-based transformations.

pub mod bool_in;
pub mod dnf;
pub mod equality_propagation;
pub mod redistribution;

use crate::errors::QueryPlannerError;
use crate::ir::operator::{Bool, Relational};
use crate::ir::Plan;
use traversal::DftPost;

impl Plan {
    /// Concatenate trivalents (boolean or NULL expressions) to the AND node.
    ///
    /// # Errors
    /// - If left or right child is not a trivalent.
    pub fn concat_and(
        &mut self,
        left_expr_id: usize,
        right_expr_id: usize,
    ) -> Result<usize, QueryPlannerError> {
        if !self.is_trivalent(left_expr_id)? {
            return Err(QueryPlannerError::CustomError(format!(
                "Left expression is not a boolean expression or NULL: {:?}",
                self.get_expression_node(left_expr_id)?
            )));
        }
        if !self.is_trivalent(right_expr_id)? {
            return Err(QueryPlannerError::CustomError(format!(
                "Right expression is not a boolean expression or NULL: {:?}",
                self.get_expression_node(right_expr_id)?
            )));
        }
        self.add_cond(left_expr_id, Bool::And, right_expr_id)
    }

    /// Concatenate trivalents (boolean or NULL expressions) to the OR node.
    ///
    /// # Errors
    /// - If left or right child is not a trivalent.
    pub fn concat_or(
        &mut self,
        left_expr_id: usize,
        right_expr_id: usize,
    ) -> Result<usize, QueryPlannerError> {
        if !self.is_trivalent(left_expr_id)? {
            return Err(QueryPlannerError::CustomError(format!(
                "Left expression is not a boolean expression or NULL: {:?}",
                self.get_expression_node(left_expr_id)?
            )));
        }
        if !self.is_trivalent(right_expr_id)? {
            return Err(QueryPlannerError::CustomError(format!(
                "Right expression is not a boolean expression or NULL: {:?}",
                self.get_expression_node(right_expr_id)?
            )));
        }
        self.add_cond(left_expr_id, Bool::Or, right_expr_id)
    }

    /// Transform all expression subtrees in the plan
    /// for selection, projection and inner join nodes
    /// with the passed function.
    ///
    /// # Errors
    /// - If failed to get the plan top node.
    /// - If relational iterator returns non-relational node.
    /// - If failed to transform the expression subtree.
    pub fn transform_expr_trees(
        &mut self,
        f: &dyn Fn(&mut Plan, usize) -> Result<usize, QueryPlannerError>,
    ) -> Result<(), QueryPlannerError> {
        let mut nodes: Vec<usize> = Vec::new();
        let top_id = self.get_top()?;
        let ir_tree = DftPost::new(&top_id, |node| self.nodes.rel_iter(node));
        for (_, id) in ir_tree {
            nodes.push(*id);
        }
        for id in &nodes {
            let rel = self.get_relation_node(*id)?;
            let new_tree_id = match rel {
                Relational::Projection {
                    output: tree_id, ..
                }
                | Relational::Selection {
                    filter: tree_id, ..
                }
                | Relational::InnerJoin {
                    condition: tree_id, ..
                } => {
                    let expr_id = *tree_id;
                    f(self, expr_id)?
                }
                _ => continue,
            };
            let rel = self.get_mut_relation_node(*id)?;
            match rel {
                Relational::Selection {
                    filter: tree_id, ..
                }
                | Relational::Projection {
                    output: tree_id, ..
                }
                | Relational::InnerJoin {
                    condition: tree_id, ..
                } => {
                    *tree_id = new_tree_id;
                }
                _ => continue,
            }
        }
        Ok(())
    }
}
