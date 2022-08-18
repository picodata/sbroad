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
//! ```text
//! ((a = 1) and (b = 2) or (a = 3)) and (c = 4)
//! ```
//! can be converted to a tree:
//! ```text
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
//! ```text
//! 1. (c = 4) and (a = 1) and (b = 2)
//! 2. (c = 4) and (a = 3)
//! ```
//!
//! To build the DNF we unite the chains with the "OR" node:
//! ```text
//! ((c = 4) and (a = 1) and (b = 2)) or ((c = 4) and (a = 3))
//! ```
//!
//! The corresponding tree:
//! ```text
//!
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
use crate::ir::operator::Bool;
use crate::ir::{Node, Plan};
use std::collections::VecDeque;

/// A chain of the trivalents (boolean or NULL expressions) concatenated by AND.
#[derive(Clone, Debug)]
pub struct Chain {
    nodes: VecDeque<usize>,
}

impl Chain {
    fn with_capacity(capacity: usize) -> Self {
        Chain {
            nodes: VecDeque::with_capacity(capacity),
        }
    }

    /// Append a new node to the chain. Keep AND and OR nodes in the back,
    /// while other nodes in the front of the chain double-ended queue.
    fn push(&mut self, expr_id: usize, plan: &Plan) -> Result<(), QueryPlannerError> {
        let expr = plan.get_expression_node(expr_id)?;
        if let Expression::Bool {
            op: Bool::And | Bool::Or,
            ..
        } = expr
        {
            self.nodes.push_back(expr_id);
            return Ok(());
        }
        self.nodes.push_front(expr_id);
        Ok(())
    }

    /// Pop AND and OR nodes (we append them to the back).
    fn pop_back(&mut self, plan: &Plan) -> Result<Option<usize>, QueryPlannerError> {
        if let Some(expr_id) = self.nodes.back() {
            let expr = plan.get_expression_node(*expr_id)?;
            if let Expression::Bool {
                op: Bool::And | Bool::Or,
                ..
            } = expr
            {
                return Ok(self.nodes.pop_back());
            }
        }
        Ok(None)
    }

    /// Pop trivalent nodes other than AND and OR (we keep then in the front).
    fn pop_front(&mut self) -> Option<usize> {
        self.nodes.pop_front()
    }

    /// Convert a chain to a new expression tree (reuse trivalent expressions).
    fn as_plan(&mut self, plan: &mut Plan) -> Result<usize, QueryPlannerError> {
        let mut top_id: Option<usize> = None;
        while let Some(expr_id) = self.pop_front() {
            match top_id {
                None => {
                    top_id = Some(expr_id);
                }
                Some(left_id) => {
                    let right_id = expr_id;
                    top_id = Some(plan.concat_and(left_id, right_id)?);
                }
            }
        }
        let new_top_id =
            top_id.ok_or_else(|| QueryPlannerError::CustomError("Empty chain".into()))?;
        Ok(new_top_id)
    }

    /// Return a mutable reference to the chain nodes.
    pub fn get_mut_nodes(&mut self) -> &mut VecDeque<usize> {
        &mut self.nodes
    }
}

fn call_expr_tree_to_dnf(plan: &mut Plan, top_id: usize) -> Result<usize, QueryPlannerError> {
    plan.expr_tree_to_dnf(top_id)
}

impl Plan {
    /// Get the DNF "AND" chains from the expression tree.
    ///
    /// # Errors
    /// - If the expression tree is not a trivalent expression.
    /// - Failed to append node to the AND chain.
    pub fn get_dnf_chains(&self, top_id: usize) -> Result<VecDeque<Chain>, QueryPlannerError> {
        let capacity: usize = self.nodes.arena.iter().fold(0_usize, |acc, node| {
            acc + match node {
                Node::Expression(Expression::Bool {
                    op: Bool::And | Bool::Or,
                    ..
                }) => 1,
                _ => 0,
            }
        });
        let mut result: VecDeque<Chain> = VecDeque::with_capacity(capacity);
        let mut stack: Vec<Chain> = Vec::with_capacity(capacity);

        let mut top_chain = Chain::with_capacity(capacity);
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

        Ok(result)
    }

    /// Convert an expression tree of trivalent nodes to a disjunctive normal form (DNF).
    ///
    /// # Errors
    /// - Failed to retrieve DNF chains.
    /// - Failed to convert the AND chain to a new expression tree.
    /// - Failed to concatenate the AND expression trees to the OR tree.
    pub fn expr_tree_to_dnf(&mut self, top_id: usize) -> Result<usize, QueryPlannerError> {
        let mut result = self.get_dnf_chains(top_id)?;

        let mut new_top_id: Option<usize> = None;
        while let Some(mut chain) = result.pop_front() {
            let ir_chain_top = chain.as_plan(self)?;
            new_top_id = match new_top_id {
                Some(top_id) => Some(self.concat_or(top_id, ir_chain_top)?),
                None => Some(ir_chain_top),
            }
        }

        new_top_id
            .ok_or_else(|| QueryPlannerError::CustomError("Chain returned no expressions".into()))
    }

    /// Convert an expression tree of trivalent nodes to a conjunctive
    /// normal form (CNF) for the whole plan.
    ///
    /// # Errors
    /// - If the plan does not contain the top.
    /// - If the plan doesn't contain relational operators where expected.
    /// - If failed to convert an expression tree to a CNF.
    pub fn set_dnf(&mut self) -> Result<(), QueryPlannerError> {
        self.transform_expr_trees(&call_expr_tree_to_dnf)
    }
}

#[cfg(test)]
mod tests;
