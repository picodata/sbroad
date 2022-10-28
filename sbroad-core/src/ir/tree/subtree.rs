use std::cell::RefCell;
use std::cmp::Ordering;

use super::{PlanTreeIterator, TreeIterator};
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::{Node, Nodes, Plan};

trait SubtreePlanIterator<'plan>: PlanTreeIterator<'plan> {}

/// Expression and relational nodes iterator.
#[derive(Debug)]
pub struct SubtreeIterator<'plan> {
    current: &'plan usize,
    child: RefCell<usize>,
    plan: &'plan Plan,
}

impl<'nodes> TreeIterator<'nodes> for SubtreeIterator<'nodes> {
    fn get_current(&self) -> &'nodes usize {
        self.current
    }

    fn get_child(&self) -> &RefCell<usize> {
        &self.child
    }

    fn get_nodes(&self) -> &'nodes Nodes {
        &self.plan.nodes
    }
}

impl<'plan> PlanTreeIterator<'plan> for SubtreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan {
        self.plan
    }
}

impl<'plan> SubtreePlanIterator<'plan> for SubtreeIterator<'plan> {}

impl<'plan> Iterator for SubtreeIterator<'plan> {
    type Item = &'plan usize;

    fn next(&mut self) -> Option<Self::Item> {
        subtree_next(self)
    }
}

impl<'plan> Plan {
    #[must_use]
    pub fn subtree_iter(&'plan self, current: &'plan usize) -> SubtreeIterator<'plan> {
        SubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
        }
    }
}

#[allow(clippy::too_many_lines)]
fn subtree_next<'plan>(iter: &mut impl SubtreePlanIterator<'plan>) -> Option<&'plan usize> {
    if let Some(child) = iter.get_nodes().arena.get(*iter.get_current()) {
        return match child {
            Node::Parameter => None,
            Node::Expression(exp) => match exp {
                Expression::Alias { child, .. }
                | Expression::Cast { child, .. }
                | Expression::Unary { child, .. } => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;
                    if step == 0 {
                        return Some(child);
                    }
                    None
                }
                Expression::Bool { left, right, .. } | Expression::Concat { left, right } => {
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
                Expression::Row { list, .. }
                | Expression::StableFunction { children: list, .. } => {
                    let child_step = *iter.get_child().borrow();
                    return match list.get(child_step) {
                        None => None,
                        Some(child) => {
                            *iter.get_child().borrow_mut() += 1;
                            Some(child)
                        }
                    };
                }
                Expression::Constant { .. } => None,
                Expression::Reference { .. } => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;

                        // At first we need to detect the place where the reference is used:
                        // for selection filter or a join condition, we need to check whether
                        // the reference points to an **additional** sub-query and then traverse
                        // into it. Otherwise, stop traversal.
                        let parent_id = match exp.get_parent() {
                            Ok(parent_id) => parent_id,
                            Err(_) => return None,
                        };
                        if let Ok(rel_id) = iter
                            .get_plan()
                            .get_relational_from_reference_node(*iter.get_current())
                        {
                            match iter.get_plan().get_relation_node(*rel_id) {
                                Ok(rel_node) if rel_node.is_subquery() => {
                                    // Check if the sub-query is an additional one.
                                    let parent = iter.get_plan().get_relation_node(parent_id);
                                    let mut is_additional = false;
                                    if let Ok(Relational::Selection { children, .. }) = parent {
                                        if children.iter().skip(1).any(|&c| c == *rel_id) {
                                            is_additional = true;
                                        }
                                    }
                                    if let Ok(Relational::InnerJoin { children, .. }) = parent {
                                        if children.iter().skip(2).any(|&c| c == *rel_id) {
                                            is_additional = true;
                                        }
                                    }
                                    if is_additional {
                                        return Some(rel_id);
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                    None
                }
            },

            Node::Relational(r) => match r {
                Relational::InnerJoin {
                    children,
                    condition,
                    ..
                } => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    match step.cmp(&2) {
                        Ordering::Less => {
                            return children.get(step);
                        }
                        Ordering::Equal => {
                            return Some(condition);
                        }
                        Ordering::Greater => None,
                    }
                }

                Relational::Except { children, .. }
                | Relational::Insert { children, .. }
                | Relational::Motion { children, .. }
                | Relational::ScanSubQuery { children, .. }
                | Relational::UnionAll { children, .. } => {
                    let step = *iter.get_child().borrow();
                    if step < children.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return children.get(step);
                    }
                    None
                }
                Relational::Values {
                    output, children, ..
                }
                | Relational::Projection {
                    output, children, ..
                } => {
                    let step = *iter.get_child().borrow();
                    *iter.get_child().borrow_mut() += 1;
                    if step == 0 {
                        return Some(output);
                    }
                    if step <= children.len() {
                        return children.get(step - 1);
                    }
                    None
                }
                Relational::Selection {
                    children, filter, ..
                } => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    match step.cmp(&1) {
                        Ordering::Less => {
                            return children.get(step);
                        }
                        Ordering::Equal => {
                            return Some(filter);
                        }
                        Ordering::Greater => None,
                    }
                }
                Relational::ValuesRow { data, .. } => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    if step == 0 {
                        return Some(data);
                    }
                    None
                }
                Relational::ScanRelation { .. } => None,
            },
        };
    }
    None
}
