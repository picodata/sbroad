//! Disjunctive normal form (DNF) transformation.
//!
//! DNF transformation is a subtask of the derivation of the distribution keys in filter trees.
//! The idea is to generate wide "AND" expression chains that can be later converted to the
//! tuples. So we increase the chance that the current tuple contains a distribution key that
//! can be later used to optimize motion policy and filter the amount of segments to evaluate on
//! the local query.
//!
//! The idea behind generating DNF is to use an IR expression tree, build "AND" chains from its top
//! to bottom and then unite the chains into one "OR" expression. An important aspect: we clone the
//! chain in every "OR" node, so "OR" is treated as a "fork" node.
//!
//! For example:
//! ```
//! ((a = 1) and (b = 2) or (a = 3)) and (c = 4)
//! ```
//! can be converted to a tree:
//! ```
//!                         ┌─────┐
//!                         │ AND │
//!                         └┬───┬┘
//!                          │   │
//!                          │   │
//!               ┌──────┐   │   │   ┌──────┐
//!               │  OR  │◄──┘   └───┤c = 4 │
//!               └──┬─┬─┘           └──────┘
//!                  │ │
//!                  │ │
//!         ┌─────┐  │ │  ┌─────┐
//!         │ AND │◄─┘ └─►│a = 3│
//!         └─┬──┬┘       └─────┘
//!           │  │
//!           │  │
//! ┌─────┐   │  │   ┌─────┐
//! │a = 1│◄──┘  └──►│b = 2│
//! └─────┘          └─────┘
//! ```
//!
//! Here is the list of all the chains ("paths") from the top to the bottom:
//! ```
//! 1. (c = 4) and (a = 1) and (b = 2)
//! 2. (c = 4) and (a = 3)
//! ```
//!
//! To build the DNF we unite the chains with the "OR" node:
//! ```
//! ((c = 4) and (a = 1) and (b = 2)) or ((c = 4) and (a = 3))
//! ```
//!
//! The corresponding tree:
//! ```
//!                         ┌────┐
//!                         │ OR │
//!                         └┬──┬┘
//!                          │  │
//!                          │  │
//!                ┌─────┐   │  │           ┌─────┐
//!                │ AND │◄──┘  └──────────►│ AND │
//!                └─┬─┬─┘                  └┬───┬┘
//!                  │ │                     │   │
//!                  │ │                     │   │
//!         ┌─────┐  │ │  ┌─────┐   ┌─────┐  │   │  ┌─────┐
//!         │ AND │◄─┘ └─►│c = 4│   │a = 3│◄─┘   └─►│c = 4│
//!         └─┬──┬┘       └─────┘   └─────┘         └─────┘
//!           │  │
//!           │  │
//! ┌─────┐   │  │   ┌─────┐
//! │a = 1│◄──┘  └──►│b = 2│
//! └─────┘          └─────┘
//! ```

use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational};
use crate::ir::Plan;
use std::collections::VecDeque;
use traversal::DftPost;

/// A chain of the boolean expressions concatenated by AND.
#[derive(Clone, Debug)]
struct Chain {
    nodes: VecDeque<usize>,
}

impl Chain {
    fn new() -> Self {
        Chain {
            nodes: VecDeque::new(),
        }
    }

    /// Append a new node to the chain. Keep AND and OR nodes in the back,
    /// while other nodes in the front of the chain double-ended queue.
    fn push(&mut self, expr_id: usize, plan: &Plan) -> Result<(), QueryPlannerError> {
        let expr = plan.get_expression_node(expr_id)?;
        if let Expression::Bool { op, .. } = expr {
            if let Bool::And | Bool::Or = *op {
                self.nodes.push_back(expr_id);
                return Ok(());
            }
        }
        self.nodes.push_front(expr_id);
        Ok(())
    }

    /// Pop AND and OR nodes (we append them to the back).
    fn pop_back(&mut self, plan: &Plan) -> Result<Option<usize>, QueryPlannerError> {
        if let Some(expr_id) = self.nodes.back() {
            let expr = plan.get_expression_node(*expr_id)?;
            if let Expression::Bool { op, .. } = expr {
                if let Bool::And | Bool::Or = *op {
                    return Ok(self.nodes.pop_back());
                }
            }
        }
        Ok(None)
    }

    /// Pop boolean nodes other than AND and OR (we keep then in the front).
    fn pop_front(&mut self) -> Option<usize> {
        self.nodes.pop_front()
    }

    /// Convert a chain to a new expression tree
    /// (all boolean expressions are cloned to the new nodes in arena).
    fn as_plan(&mut self, plan: &mut Plan) -> Result<usize, QueryPlannerError> {
        let mut top_id: Option<usize> = None;
        while let Some(expr_id) = self.pop_front() {
            let left_id: usize = if let Some(id) = top_id {
                id
            } else {
                let new_expr_id = plan.expr_clone(expr_id)?;
                top_id = Some(new_expr_id);
                continue;
            };

            let right_id = plan.expr_clone(expr_id)?;
            top_id = Some(plan.concat_and(left_id, right_id)?);
        }
        let new_top_id =
            top_id.ok_or_else(|| QueryPlannerError::CustomError("Empty chain".into()))?;
        Ok(new_top_id)
    }
}

impl Plan {
    fn concat_and(
        &mut self,
        left_expr_id: usize,
        right_expr_id: usize,
    ) -> Result<usize, QueryPlannerError> {
        let left_expr = self.get_expression_node(left_expr_id)?;
        if !left_expr.is_bool(self)? && !left_expr.is_null(self)? {
            return Err(QueryPlannerError::CustomError(format!(
                "Left expression is not a boolean expression or NULL: {:?}",
                left_expr
            )));
        }
        let right_expr = self.get_expression_node(right_expr_id)?;
        if !right_expr.is_bool(self)? && !right_expr.is_null(self)? {
            return Err(QueryPlannerError::CustomError(format!(
                "Right expression is not a boolean expression or NULL: {:?}",
                right_expr
            )));
        }
        self.add_cond(left_expr_id, Bool::And, right_expr_id)
    }

    fn concat_or(
        &mut self,
        left_expr_id: usize,
        right_expr_id: usize,
    ) -> Result<usize, QueryPlannerError> {
        let left_expr = self.get_expression_node(left_expr_id)?;
        if !left_expr.is_bool(self)? && !left_expr.is_null(self)? {
            return Err(QueryPlannerError::CustomError(format!(
                "Left expression is not a boolean expression or NULL: {:?}",
                left_expr
            )));
        }
        let right_expr = self.get_expression_node(right_expr_id)?;
        if !right_expr.is_bool(self)? && !right_expr.is_null(self)? {
            return Err(QueryPlannerError::CustomError(format!(
                "Right expression is not a boolean expression or NULL: {:?}",
                right_expr
            )));
        }
        self.add_cond(left_expr_id, Bool::Or, right_expr_id)
    }

    /// Convert an expression tree of boolean nodes to a disjunctive normal form (DNF).
    ///
    /// # Errors
    /// - If the expression tree is not a boolean expression.
    /// - Failed to append node to the AND chain.
    /// - Failed to convert the AND chain to a new expression tree.
    /// - Failed to concatenate the AND expression trees to the OR tree.
    pub fn expr_tree_to_dnf(&mut self, top_id: usize) -> Result<usize, QueryPlannerError> {
        let mut result: VecDeque<Chain> = VecDeque::new();
        let mut stack: Vec<Chain> = Vec::new();

        let mut top_chain = Chain::new();
        top_chain.push(top_id, self)?;
        stack.push(top_chain);

        while let Some(mut chain) = stack.pop() {
            let expr_id = if let Some(expr_id) = chain.pop_back(self)? {
                expr_id
            } else {
                result.push_back(chain);
                continue;
            };
            let expr = self.get_expression_node(expr_id)?;
            if let Expression::Bool {
                op, left, right, ..
            } = expr
            {
                match *op {
                    Bool::And => {
                        chain.push(*right, self)?;
                        chain.push(*left, self)?;
                    }
                    Bool::Or => {
                        let mut new_chain = chain.clone();
                        new_chain.push(*right, self)?;
                        stack.push(new_chain);
                        chain.push(*left, self)?;
                    }
                    _ => {
                        return Err(QueryPlannerError::CustomError(
                            "Chain returned unexpected boolean operator".into(),
                        ))
                    }
                }
            }
            stack.push(chain);
        }

        let mut new_top_id: Option<usize> = None;
        while let Some(mut chain) = result.pop_front() {
            let ir_chain_top = chain.as_plan(self)?;
            if let Some(top_id) = new_top_id {
                new_top_id = Some(self.concat_or(top_id, ir_chain_top)?);
            } else {
                new_top_id = Some(ir_chain_top);
            }
        }

        if let Some(top_id) = new_top_id {
            Ok(top_id)
        } else {
            Err(QueryPlannerError::CustomError(
                "Chain returned no expressions".into(),
            ))
        }
    }

    /// Convert an expression tree of boolean nodes to a conjunctive
    /// normal form (CNF) for the whole plan.
    ///
    /// # Errors
    /// - If the plan does not contain the top.
    /// - If the plan doesn't contain relational operators where expected.
    /// - If failed to convert an expression tree to a CNF.
    pub fn set_dnf(&mut self) -> Result<(), QueryPlannerError> {
        let mut nodes: Vec<usize> = Vec::new();
        let top_id = self.get_top()?;
        let subtree = DftPost::new(&top_id, |node| self.nodes.rel_iter(node));
        for (_, id) in subtree {
            nodes.push(*id);
        }
        for id in &nodes {
            let rel = self.get_relation_node(*id)?;
            let new_tree_id = match rel {
                Relational::Selection {
                    filter: tree_id, ..
                }
                | Relational::Projection {
                    output: tree_id, ..
                }
                | Relational::InnerJoin {
                    condition: tree_id, ..
                } => {
                    let expr_id = *tree_id;
                    self.expr_tree_to_dnf(expr_id)?
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

#[cfg(test)]
mod tests;
