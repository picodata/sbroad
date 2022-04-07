//! Plan tree transformation module.
//!
//! Contains rule-based transformations.

pub mod bool_in;
pub mod dnf;
pub mod equality_propagation;
pub mod merge_tuples;
pub mod redistribution;
pub mod split_columns;

use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational};
use crate::ir::Plan;
use std::collections::HashMap;
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

    /// Replace boolean operators in an expression subtree with the
    /// new boolean expressions produced by the user defined function.
    ///
    /// # Errors
    ///  If expression subtree iterator returns a non-expression node.
    pub fn expr_tree_replace_bool(
        &mut self,
        top_id: usize,
        f: &dyn Fn(&mut Plan, usize) -> Result<usize, QueryPlannerError>,
        ops: &[Bool],
    ) -> Result<usize, QueryPlannerError> {
        let mut map: HashMap<usize, usize> = HashMap::new();
        let mut nodes: Vec<usize> = Vec::new();
        let subtree = DftPost::new(&top_id, |node| self.nodes.expr_iter(node, false));
        for (_, id) in subtree {
            nodes.push(*id);
        }
        for id in &nodes {
            let expr = self.get_expression_node(*id)?;
            if let Expression::Bool { op, .. } = expr {
                if ops.contains(op) || ops.is_empty() {
                    let new_tree_id = f(self, *id)?;
                    map.insert(*id, new_tree_id);
                }
            }
        }
        let mut new_top_id = top_id;
        for id in &nodes {
            let expr = self.get_mut_expression_node(*id)?;
            // For all expressions in the subtree try to replace their children
            // with the new nodes from the map.
            match expr {
                Expression::Alias { child, .. } => {
                    if let Some(new_id) = map.get(child) {
                        *child = *new_id;
                    }
                }
                Expression::Bool { left, right, .. } => {
                    if let Some(new_id) = map.get(left) {
                        *left = *new_id;
                    }
                    if let Some(new_id) = map.get(right) {
                        *right = *new_id;
                    }
                }
                Expression::Row { list, .. } => {
                    for id in list {
                        if let Some(new_id) = map.get(id) {
                            *id = *new_id;
                        }
                    }
                }
                _ => {}
            }
        }
        // Check if the top node is an new node itself.
        if let Some(new_id) = map.get(&top_id) {
            new_top_id = *new_id;
        }
        Ok(new_top_id)
    }
}

#[cfg(test)]
mod helpers;
