use super::expression::Expression;
use super::{Node, Nodes};
use std::cell::RefCell;

/// Expression node's children iterator.
///
/// Iterator returns the next child for expression
/// nodes. It is required to use `traversal` crate.
#[derive(Debug)]
pub struct ExpressionIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
}

impl<'n> Nodes {
    #[must_use]
    pub fn expr_iter(&'n self, current: &'n usize) -> ExpressionIterator<'n> {
        ExpressionIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }
}

impl<'n> Iterator for ExpressionIterator<'n> {
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self.nodes.arena.get(*self.current) {
            Some(Node::Expression(Expression::Alias { child, .. })) => {
                let child_step = *self.child.borrow();
                if child_step == 0 {
                    *self.child.borrow_mut() += 1;
                    return Some(child);
                }
                None
            }
            Some(Node::Expression(Expression::Bool { left, right, .. })) => {
                let child_step = *self.child.borrow();
                if child_step == 0 {
                    *self.child.borrow_mut() += 1;
                    return Some(left);
                } else if child_step == 1 {
                    *self.child.borrow_mut() += 1;
                    return Some(right);
                }
                None
            }
            Some(Node::Expression(Expression::Row { list, .. })) => {
                let child_step = *self.child.borrow();
                match list.get(child_step) {
                    None => None,
                    Some(child) => {
                        *self.child.borrow_mut() += 1;
                        Some(child)
                    }
                }
            }
            None
            | Some(
                Node::Expression(Expression::Constant { .. } | Expression::Reference { .. })
                | Node::Relational(_),
            ) => None,
        }
    }
}

#[cfg(test)]
mod tests;
