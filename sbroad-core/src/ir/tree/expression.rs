use std::cell::RefCell;

use super::TreeIterator;
use crate::ir::expression::Expression;
use crate::ir::{Node, Nodes};

trait ExpressionTreeIterator<'nodes>: TreeIterator<'nodes> {
    fn get_make_row_leaf(&self) -> bool;
}

/// Expression node's children iterator.
///
/// The iterator returns the next child for expression
/// nodes. It is required to use `traversal` crate.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct ExpressionIterator<'n> {
    current: usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
    make_row_leaf: bool,
}

pub struct AggregateIterator<'p> {
    inner: ExpressionIterator<'p>,
    must_stop: bool,
}

impl<'n> Nodes {
    #[must_use]
    pub fn expr_iter(&'n self, current: usize, make_row_leaf: bool) -> ExpressionIterator<'n> {
        ExpressionIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
            make_row_leaf,
        }
    }

    #[must_use]
    pub fn aggregate_iter(&'n self, current: usize, make_row_leaf: bool) -> AggregateIterator<'n> {
        let must_stop = if let Some(Node::Expression(Expression::StableFunction { name, .. })) =
            self.arena.get(current)
        {
            Expression::is_aggregate_name(name)
        } else {
            false
        };
        AggregateIterator {
            inner: ExpressionIterator {
                current,
                child: RefCell::new(0),
                nodes: self,
                make_row_leaf,
            },
            must_stop,
        }
    }
}

impl<'nodes> TreeIterator<'nodes> for ExpressionIterator<'nodes> {
    fn get_current(&self) -> usize {
        self.current
    }

    fn get_child(&self) -> &RefCell<usize> {
        &self.child
    }

    fn get_nodes(&self) -> &'nodes Nodes {
        self.nodes
    }
}

impl<'nodes> ExpressionTreeIterator<'nodes> for ExpressionIterator<'nodes> {
    fn get_make_row_leaf(&self) -> bool {
        self.make_row_leaf
    }
}

impl<'n> Iterator for ExpressionIterator<'n> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        expression_next(self).copied()
    }
}

impl<'n> Iterator for AggregateIterator<'n> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.must_stop {
            return None;
        }
        expression_next(&mut self.inner).copied()
    }
}

#[allow(clippy::too_many_lines)]
fn expression_next<'nodes>(
    iter: &mut impl ExpressionTreeIterator<'nodes>,
) -> Option<&'nodes usize> {
    match iter.get_nodes().arena.get(iter.get_current()) {
        Some(Node::Expression(
            Expression::Alias { child, .. }
            | Expression::ExprInParentheses { child, .. }
            | Expression::Cast { child, .. }
            | Expression::Unary { child, .. },
        )) => {
            let child_step = *iter.get_child().borrow();
            if child_step == 0 {
                *iter.get_child().borrow_mut() += 1;
                return Some(child);
            }
            None
        }
        Some(Node::Expression(Expression::Case {
            search_expr,
            when_blocks,
            else_expr,
        })) => {
            let mut child_step = *iter.get_child().borrow();
            *iter.get_child().borrow_mut() += 1;
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
        Some(Node::Expression(
            Expression::Bool { left, right, .. }
            | Expression::Arithmetic { left, right, .. }
            | Expression::Concat { left, right },
        )) => {
            let child_step = *iter.get_child().borrow();
            if child_step == 0 {
                *iter.get_child().borrow_mut() += 1;
                return Some(left);
            } else if child_step == 1 {
                *iter.get_child().borrow_mut() += 1;
                return Some(right);
            }
            None
        }
        Some(Node::Expression(Expression::Trim {
            pattern, target, ..
        })) => {
            let child_step = *iter.get_child().borrow();
            match child_step {
                0 => {
                    *iter.get_child().borrow_mut() += 1;
                    match pattern {
                        Some(_) => pattern.as_ref(),
                        None => Some(target),
                    }
                }
                1 => {
                    *iter.get_child().borrow_mut() += 1;
                    match pattern {
                        Some(_) => Some(target),
                        None => None,
                    }
                }
                _ => None,
            }
        }
        Some(Node::Expression(Expression::Row { list, .. })) => {
            let child_step = *iter.get_child().borrow();
            let mut is_leaf = false;

            // Check on the first step, if the row contains only leaf nodes.
            if child_step == 0 {
                is_leaf = true;
                for col in list {
                    if !matches!(
                        iter.get_nodes().arena.get(*col),
                        Some(Node::Expression(
                            Expression::Reference { .. } | Expression::Constant { .. }
                        ))
                    ) {
                        is_leaf = false;
                        break;
                    }
                }
            }

            // If the row contains only leaf nodes (or we don't want to go deeper
            // into the row tree for some reasons), skip traversal.
            if !is_leaf || !iter.get_make_row_leaf() {
                match list.get(child_step) {
                    None => return None,
                    Some(child) => {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(child);
                    }
                }
            }

            None
        }
        Some(Node::Expression(Expression::StableFunction { children, .. })) => {
            let child_step = *iter.get_child().borrow();
            match children.get(child_step) {
                None => None,
                Some(child) => {
                    *iter.get_child().borrow_mut() += 1;
                    Some(child)
                }
            }
        }
        Some(
            Node::Expression(
                Expression::Constant { .. }
                | Expression::Reference { .. }
                | Expression::CountAsterisk,
            )
            | Node::Relational(_)
            | Node::Parameter
            | Node::Ddl(_)
            | Node::Acl(_)
            | Node::Block(_),
        )
        | None => None,
    }
}
