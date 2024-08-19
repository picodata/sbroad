use std::cell::RefCell;

use super::TreeIterator;
use crate::ir::node::relational::Relational;
use crate::ir::node::{ArenaType, GroupBy, Limit, NodeId, OrderBy, ScanCte};
use crate::ir::{Node, Nodes};

trait RelationalTreeIterator<'nodes>: TreeIterator<'nodes> {}

/// Relational node's child iterator.
///
/// The iterator returns the next relational node in the plan tree.
#[derive(Debug)]
pub struct RelationalIterator<'n> {
    current: NodeId,
    child: RefCell<usize>,
    nodes: &'n Nodes,
}

impl<'n> Nodes {
    #[must_use]
    pub fn rel_iter(&'n self, current: NodeId) -> RelationalIterator<'n> {
        RelationalIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }

    #[must_use]
    pub fn empty_rel_iter(&'n self) -> RelationalIterator<'n> {
        RelationalIterator {
            current: self.next_id(ArenaType::Arena64),
            child: RefCell::new(0),
            nodes: self,
        }
    }
}

impl<'nodes> TreeIterator<'nodes> for RelationalIterator<'nodes> {
    fn get_current(&self) -> NodeId {
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
    type Item = NodeId;

    fn next(&mut self) -> Option<Self::Item> {
        relational_next(self)
    }
}

fn relational_next<'nodes>(iter: &mut impl RelationalTreeIterator<'nodes>) -> Option<NodeId> {
    match iter.get_nodes().get(iter.get_current()) {
        Some(Node::Relational(
            node @ (Relational::Except { .. }
            | Relational::Join { .. }
            | Relational::Insert { .. }
            | Relational::Intersect { .. }
            | Relational::Delete { .. }
            | Relational::Motion { .. }
            | Relational::Projection { .. }
            | Relational::ScanSubQuery { .. }
            | Relational::Selection { .. }
            | Relational::Having { .. }
            | Relational::Union { .. }
            | Relational::UnionAll { .. }
            | Relational::Update { .. }
            | Relational::Values { .. }
            | Relational::ValuesRow { .. }),
        )) => {
            let step = *iter.get_child().borrow();
            let children = node.children();
            if step < children.len() {
                *iter.get_child().borrow_mut() += 1;
                return children.get(step).copied();
            }
            None
        }
        Some(Node::Relational(Relational::GroupBy(GroupBy { children, .. }))) => {
            let step = *iter.get_child().borrow();
            if step == 0 {
                *iter.get_child().borrow_mut() += 1;
                return children.get(step).copied();
            }
            None
        }
        Some(Node::Relational(
            Relational::OrderBy(OrderBy { child, .. })
            | Relational::ScanCte(ScanCte { child, .. })
            | Relational::Limit(Limit { child, .. }),
        )) => {
            let step = *iter.get_child().borrow();
            if step == 0 {
                *iter.get_child().borrow_mut() += 1;
                return Some(*child);
            }
            None
        }
        Some(
            Node::Relational(Relational::ScanRelation { .. })
            | Node::Expression(_)
            | Node::Parameter(_)
            | Node::Invalid(_)
            | Node::Ddl(_)
            | Node::Acl(_)
            | Node::Block(_),
        )
        | None => None,
    }
}
