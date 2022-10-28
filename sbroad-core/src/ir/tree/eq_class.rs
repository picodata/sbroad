use std::cell::RefCell;

use super::TreeIterator;
use crate::ir::expression::Expression;
use crate::ir::operator::Bool;
use crate::ir::{Node, Nodes};

trait EqClassTreeIterator<'nodes>: TreeIterator<'nodes> {}

/// Children iterator for "and"-ed equivalent expressions.
///
/// The iterator returns the next child for the chained `Bool::And`
/// and `Bool::Eq` nodes.
#[derive(Debug)]
pub struct EqClassIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
}

impl<'nodes> TreeIterator<'nodes> for EqClassIterator<'nodes> {
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

impl<'nodes> EqClassTreeIterator<'nodes> for EqClassIterator<'nodes> {}

impl<'n> Nodes {
    #[must_use]
    pub fn eq_iter(&'n self, current: &'n usize) -> EqClassIterator<'n> {
        EqClassIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }
}

impl<'n> Iterator for EqClassIterator<'n> {
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        eq_class_next(self)
    }
}

fn eq_class_next<'nodes>(iter: &mut impl EqClassTreeIterator<'nodes>) -> Option<&'nodes usize> {
    if let Some(Node::Expression(Expression::Bool {
        left, op, right, ..
    })) = iter.get_nodes().arena.get(*iter.get_current())
    {
        if (*op != Bool::And) && (*op != Bool::Eq) {
            return None;
        }
        let child_step = *iter.get_child().borrow();
        if child_step == 0 {
            *iter.get_child().borrow_mut() += 1;
            return Some(left);
        } else if child_step == 1 {
            *iter.get_child().borrow_mut() += 1;
            return Some(right);
        }
        None
    } else {
        None
    }
}
