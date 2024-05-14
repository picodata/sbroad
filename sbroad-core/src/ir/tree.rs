//! IR tree traversal module.

use super::{Nodes, Plan};
use crate::ir::expression::Expression;
use std::cell::RefCell;

trait TreeIterator<'nodes> {
    fn get_current(&self) -> usize;
    fn get_child(&self) -> &RefCell<usize>;
    fn get_nodes(&self) -> &'nodes Nodes;

    fn handle_trim(&mut self, expr: &'nodes Expression) -> Option<&'nodes usize> {
        let Expression::Trim {
            pattern, target, ..
        } = expr
        else {
            panic!("Trim expected")
        };
        let child_step = *self.get_child().borrow();
        match child_step {
            0 => {
                *self.get_child().borrow_mut() += 1;
                match pattern {
                    Some(_) => pattern.as_ref(),
                    None => Some(target),
                }
            }
            1 => {
                *self.get_child().borrow_mut() += 1;
                match pattern {
                    Some(_) => Some(target),
                    None => None,
                }
            }
            _ => None,
        }
    }

    fn handle_left_right_children(&mut self, expr: &'nodes Expression) -> Option<&'nodes usize> {
        let (Expression::Bool { left, right, .. }
        | Expression::Arithmetic { left, right, .. }
        | Expression::Concat { left, right, .. }) = expr
        else {
            panic!("Expected expression with left and right children")
        };
        let child_step = *self.get_child().borrow();
        if child_step == 0 {
            *self.get_child().borrow_mut() += 1;
            return Some(left);
        } else if child_step == 1 {
            *self.get_child().borrow_mut() += 1;
            return Some(right);
        }
        None
    }

    fn handle_single_child(&mut self, expr: &'nodes Expression) -> Option<&'nodes usize> {
        let (Expression::Alias { child, .. }
        | Expression::ExprInParentheses { child }
        | Expression::Cast { child, .. }
        | Expression::Unary { child, .. }) = expr
        else {
            panic!("Expected expression with single child")
        };
        let step = *self.get_child().borrow();
        *self.get_child().borrow_mut() += 1;
        if step == 0 {
            return Some(child);
        }
        None
    }

    fn handle_case_iter(&mut self, expr: &'nodes Expression) -> Option<&'nodes usize> {
        let Expression::Case {
            search_expr,
            when_blocks,
            else_expr,
        } = expr
        else {
            panic!("Case expression expected");
        };
        let mut child_step = *self.get_child().borrow();
        *self.get_child().borrow_mut() += 1;
        if let Some(search_expr) = search_expr {
            if child_step == 0 {
                return Some(search_expr);
            }
            child_step -= 1;
        }

        let when_blocks_index = child_step / 2;
        let index_reminder = child_step % 2;
        return if when_blocks_index < when_blocks.len() {
            let (cond_expr, res_expr) = when_blocks
                .get(when_blocks_index)
                .expect("When block must have been found.");
            return match index_reminder {
                0 => Some(cond_expr),
                1 => Some(res_expr),
                _ => unreachable!("Impossible reminder"),
            };
        } else if when_blocks_index == when_blocks.len() && index_reminder == 0 {
            if let Some(else_expr) = else_expr {
                Some(else_expr)
            } else {
                None
            }
        } else {
            None
        };
    }
}

trait PlanTreeIterator<'plan>: TreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan;
}

/// A snapshot describes the version of the plan
/// subtree to iterate over.
#[derive(Debug, Clone)]
pub enum Snapshot {
    Latest,
    Oldest,
}

pub mod and;
pub mod expression;
pub mod relation;
pub mod subtree;
pub mod traversal;

#[cfg(test)]
mod tests;
