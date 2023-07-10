use std::cell::RefCell;

use super::TreeIterator;
use crate::ir::operator::Relational;
use crate::ir::{Node, Nodes};

trait RelationalTreeIterator<'nodes>: TreeIterator<'nodes> {}

/// Relational node's child iterator.
///
/// The iterator returns the next relational node in the plan tree.
#[derive(Debug)]
pub struct RelationalIterator<'n> {
    current: usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
}

impl<'n> Nodes {
    #[must_use]
    pub fn rel_iter(&'n self, current: usize) -> RelationalIterator<'n> {
        RelationalIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }
}

impl<'nodes> TreeIterator<'nodes> for RelationalIterator<'nodes> {
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

impl<'nodes> RelationalTreeIterator<'nodes> for RelationalIterator<'nodes> {}

impl<'n> Iterator for RelationalIterator<'n> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        relational_next(self).copied()
    }
}

fn relational_next<'nodes>(
    iter: &mut impl RelationalTreeIterator<'nodes>,
) -> Option<&'nodes usize> {
    match iter.get_nodes().arena.get(iter.get_current()) {
        Some(Node::Relational(
            Relational::Except { children, .. }
            | Relational::Join { children, .. }
            | Relational::Insert { children, .. }
            | Relational::Motion { children, .. }
            | Relational::Projection { children, .. }
            | Relational::ScanSubQuery { children, .. }
            | Relational::Selection { children, .. }
            | Relational::Having { children, .. }
            | Relational::UnionAll { children, .. }
            | Relational::Values { children, .. }
            | Relational::ValuesRow { children, .. },
        )) => {
            let step = *iter.get_child().borrow();
            if step < children.len() {
                *iter.get_child().borrow_mut() += 1;
                return children.get(step);
            }
            None
        }
        Some(Node::Relational(Relational::GroupBy { children, .. })) => {
            let step = *iter.get_child().borrow();
            if step == 0 {
                *iter.get_child().borrow_mut() += 1;
                return children.get(step);
            }
            None
        }
        Some(
            Node::Relational(Relational::ScanRelation { .. })
            | Node::Expression(_)
            | Node::Parameter
            | Node::Ddl(_),
        )
        | None => None,
    }
}
