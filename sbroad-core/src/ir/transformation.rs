//! Plan tree transformation module.
//!
//! Contains rule-based transformations.

pub mod bool_in;
pub mod dnf;
pub mod equality_propagation;
pub mod merge_tuples;
pub mod redistribution;
pub mod split_columns;

use crate::errors::{Entity, SbroadError};
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational};
use crate::ir::Plan;
use std::collections::HashMap;

use super::tree::traversal::{PostOrder, EXPR_CAPACITY};

impl Plan {
    /// Concatenates trivalents (boolean or NULL expressions) to the AND node.
    ///
    /// # Errors
    /// - If the left or right child is not a trivalent.
    pub fn concat_and(
        &mut self,
        left_expr_id: usize,
        right_expr_id: usize,
    ) -> Result<usize, SbroadError> {
        if !self.is_trivalent(left_expr_id)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format!(
                    "Left expression is not a boolean expression or NULL: {:?}",
                    self.get_expression_node(left_expr_id)?
                )),
            ));
        }
        if !self.is_trivalent(right_expr_id)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format!(
                    "Right expression is not a boolean expression or NULL: {:?}",
                    self.get_expression_node(right_expr_id)?
                )),
            ));
        }
        self.add_cond(left_expr_id, Bool::And, right_expr_id)
    }

    /// Concatenates trivalents (boolean or NULL expressions) to the OR node.
    ///
    /// # Errors
    /// - If the left or right child is not a trivalent.
    pub fn concat_or(
        &mut self,
        left_expr_id: usize,
        right_expr_id: usize,
    ) -> Result<usize, SbroadError> {
        if !self.is_trivalent(left_expr_id)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format!(
                    "left expression is not a boolean expression or NULL: {:?}",
                    self.get_expression_node(left_expr_id)?
                )),
            ));
        }
        if !self.is_trivalent(right_expr_id)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format!(
                    "right expression is not a boolean expression or NULL: {:?}",
                    self.get_expression_node(right_expr_id)?
                )),
            ));
        }
        self.add_cond(left_expr_id, Bool::Or, right_expr_id)
    }

    /// Transforms all expression subtrees in the plan
    /// for selection and inner join nodes using the
    /// defined function.
    ///
    /// # Errors
    /// - If failed to get the plan top node.
    /// - If relational iterator returns non-relational node.
    /// - If failed to transform the expression subtree.
    pub fn transform_expr_trees(
        &mut self,
        f: &dyn Fn(&mut Plan, usize) -> Result<usize, SbroadError>,
    ) -> Result<(), SbroadError> {
        let top_id = self.get_top()?;
        let mut ir_tree = PostOrder::with_capacity(|node| self.nodes.rel_iter(node), EXPR_CAPACITY);
        ir_tree.populate_nodes(top_id);
        let nodes = ir_tree.take_nodes();
        for (_, id) in &nodes {
            let rel = self.get_relation_node(*id)?;
            let (new_tree_id, old_tree_id) = match rel {
                Relational::Selection {
                    filter: tree_id, ..
                }
                | Relational::InnerJoin {
                    condition: tree_id, ..
                } => {
                    let expr_id = *tree_id;
                    (f(self, expr_id)?, expr_id)
                }
                _ => continue,
            };
            if old_tree_id != new_tree_id {
                self.undo.add(old_tree_id, new_tree_id);
            }
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

    /// Replaces boolean operators in an expression subtree with the
    /// new boolean expressions produced by the user defined function.
    ///
    /// # Errors
    ///  If expression subtree iterator returns a non-expression node.
    pub fn expr_tree_replace_bool(
        &mut self,
        top_id: usize,
        f: &dyn Fn(&mut Plan, usize) -> Result<usize, SbroadError>,
        ops: &[Bool],
    ) -> Result<usize, SbroadError> {
        let mut map: HashMap<usize, usize> = HashMap::new();
        let mut subtree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, false), EXPR_CAPACITY);
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();
        for (_, id) in &nodes {
            let expr = self.get_expression_node(*id)?;
            if let Expression::Bool { op, .. } = expr {
                if ops.contains(op) || ops.is_empty() {
                    let new_tree_id = f(self, *id)?;
                    map.insert(*id, new_tree_id);
                }
            }
        }
        let mut new_top_id = top_id;
        for (_, id) in &nodes {
            let expr = self.get_mut_expression_node(*id)?;
            // For all expressions in the subtree tries to replace their children
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
        // Checks if the top node is a new node.
        if let Some(new_id) = map.get(&top_id) {
            new_top_id = *new_id;
        }
        Ok(new_top_id)
    }
}

#[cfg(test)]
pub mod helpers;
