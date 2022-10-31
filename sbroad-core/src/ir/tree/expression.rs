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
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
    make_row_leaf: bool,
}

impl<'n> Nodes {
    #[must_use]
    pub fn expr_iter(&'n self, current: &'n usize, make_row_leaf: bool) -> ExpressionIterator<'n> {
        ExpressionIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
            make_row_leaf,
        }
    }
}

impl<'nodes> TreeIterator<'nodes> for ExpressionIterator<'nodes> {
    fn get_current(&self) -> &'nodes usize {
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
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        expression_next(self)
    }
}

fn expression_next<'nodes>(
    iter: &mut impl ExpressionTreeIterator<'nodes>,
) -> Option<&'nodes usize> {
    match iter.get_nodes().arena.get(*iter.get_current()) {
        Some(Node::Expression(
            Expression::Alias { child, .. }
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
        Some(Node::Expression(
            Expression::Bool { left, right, .. } | Expression::Concat { left, right },
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
            Node::Expression(Expression::Constant { .. } | Expression::Reference { .. })
            | Node::Relational(_)
            | Node::Parameter,
        )
        | None => None,
    }
}
