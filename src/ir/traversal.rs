use super::expression::Expression;
use super::operator::Relational;
use super::{Node, Plan};
use std::cell::RefCell;

/// Plan node iterator over its branches.
///
/// For example, inner join returns at first a left child, then
/// the right one. But a relation scan doesn't have any children
/// and stop iteration at the moment.
///
/// We need this iterator to traverse plan tree with `traversal` crate.
#[derive(Debug)]
pub struct BranchIterator<'n> {
    node: &'n Node,
    step: RefCell<usize>,
    plan: &'n Plan,
}

#[allow(dead_code)]
impl<'n> BranchIterator<'n> {
    /// Constructor for a new branch iterator instance.
    #[must_use]
    pub fn new(node: &'n Node, plan: &'n Plan) -> Self {
        BranchIterator {
            node,
            step: RefCell::new(0),
            plan,
        }
    }
}

impl<'n> Iterator for BranchIterator<'n> {
    type Item = &'n Node;

    fn next(&mut self) -> Option<Self::Item> {
        let get_next_child = |children: &Vec<usize>| -> Option<&Node> {
            let current_step = *self.step.borrow();
            let child = children.get(current_step);
            child.and_then(|pos| {
                let node = self.plan.nodes.arena.get(*pos);
                *self.step.borrow_mut() += 1;
                node
            })
        };

        match self.node {
            Node::Expression(expr) => match expr {
                Expression::Alias { child, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.arena.get(*child);
                    }
                    None
                }
                Expression::Bool { left, right, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.arena.get(*left);
                    } else if current_step == 1 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.arena.get(*right);
                    }
                    None
                }
                Expression::Constant { .. } | Expression::Reference { .. } => None,
                Expression::Row { list, .. } => {
                    let current_step = *self.step.borrow();
                    if let Some(node) = list.get(current_step) {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.arena.get(*node);
                    }
                    None
                }
            },
            Node::Relational(rel) => match rel {
                Relational::InnerJoin {
                    children,
                    condition,
                    ..
                } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 || current_step == 1 {
                        return get_next_child(children);
                    } else if current_step == 2 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.arena.get(*condition);
                    }
                    None
                }
                Relational::ScanRelation { .. } => None,
                Relational::ScanSubQuery { children, .. }
                | Relational::Motion { children, .. }
                | Relational::Selection { children, .. }
                | Relational::Projection { children, .. } => get_next_child(children),
                Relational::UnionAll { children, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 || current_step == 1 {
                        return get_next_child(children);
                    }
                    None
                }
            },
        }
    }
}
