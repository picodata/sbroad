use super::expression::Expression;
use super::operator::Bool;
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

/// Children iterator for "and" node chains.
/// 
/// Iterator returns the next child for Bool::And nodes.
#[derive(Debug)]
pub struct AndChainIterator<'n> {
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

    #[must_use]
    pub fn and_iter(&'n self, current: &'n usize) -> AndChainIterator<'n> {
        AndChainIterator {
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

impl<'n> Iterator for AndChainIterator<'n> {
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(Node::Expression(Expression::Bool {left, op, right, .. })) = self.nodes.arena.get(*self.current) {
            if *op != Bool::And {
                return None;
            }
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
        else {
            None
        }
    }
}

#[cfg(test)]
mod tests;
