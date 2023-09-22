use std::cell::RefCell;
use std::cmp::Ordering;

use super::{PlanTreeIterator, Snapshot, TreeIterator};
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::{Node, Nodes, Plan};

trait SubtreePlanIterator<'plan>: PlanTreeIterator<'plan> {
    fn need_output(&self) -> bool;
    fn need_motion_subtree(&self) -> bool;
}

/// Expression and relational nodes iterator.
#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub struct SubtreeIterator<'plan> {
    current: usize,
    child: RefCell<usize>,
    plan: &'plan Plan,
}

impl<'nodes> TreeIterator<'nodes> for SubtreeIterator<'nodes> {
    fn get_current(&self) -> usize {
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

impl<'plan> SubtreePlanIterator<'plan> for SubtreeIterator<'plan> {
    fn need_output(&self) -> bool {
        false
    }

    fn need_motion_subtree(&self) -> bool {
        true
    }
}

impl<'plan> Iterator for SubtreeIterator<'plan> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        subtree_next(self, &Snapshot::Latest).copied()
    }
}

impl<'plan> Plan {
    #[must_use]
    pub fn subtree_iter(&'plan self, current: usize) -> SubtreeIterator<'plan> {
        SubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
        }
    }
}

/// Expression and relational nodes flashback iterator.
/// It uses the UNDO transformation log to go back to the
/// original state of some subtrees in the plan (selections
/// at the moment).
#[derive(Debug)]
pub struct FlashbackSubtreeIterator<'plan> {
    current: usize,
    child: RefCell<usize>,
    plan: &'plan Plan,
}

impl<'nodes> TreeIterator<'nodes> for FlashbackSubtreeIterator<'nodes> {
    fn get_current(&self) -> usize {
        self.current
    }

    fn get_child(&self) -> &RefCell<usize> {
        &self.child
    }

    fn get_nodes(&self) -> &'nodes Nodes {
        &self.plan.nodes
    }
}

impl<'plan> PlanTreeIterator<'plan> for FlashbackSubtreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan {
        self.plan
    }
}

impl<'plan> SubtreePlanIterator<'plan> for FlashbackSubtreeIterator<'plan> {
    fn need_output(&self) -> bool {
        false
    }

    fn need_motion_subtree(&self) -> bool {
        true
    }
}

impl<'plan> Iterator for FlashbackSubtreeIterator<'plan> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        subtree_next(self, &Snapshot::Oldest).copied()
    }
}

impl<'plan> Plan {
    #[must_use]
    pub fn flashback_subtree_iter(&'plan self, current: usize) -> FlashbackSubtreeIterator<'plan> {
        FlashbackSubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
        }
    }
}

/// An iterator used while copying and execution plan subtree.
#[derive(Debug)]
pub struct ExecPlanSubtreeIterator<'plan> {
    current: usize,
    child: RefCell<usize>,
    plan: &'plan Plan,
}

impl<'nodes> TreeIterator<'nodes> for ExecPlanSubtreeIterator<'nodes> {
    fn get_current(&self) -> usize {
        self.current
    }

    fn get_child(&self) -> &RefCell<usize> {
        &self.child
    }

    fn get_nodes(&self) -> &'nodes Nodes {
        &self.plan.nodes
    }
}

impl<'plan> PlanTreeIterator<'plan> for ExecPlanSubtreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan {
        self.plan
    }
}

impl<'plan> SubtreePlanIterator<'plan> for ExecPlanSubtreeIterator<'plan> {
    fn need_output(&self) -> bool {
        true
    }

    fn need_motion_subtree(&self) -> bool {
        false
    }
}

impl<'plan> Iterator for ExecPlanSubtreeIterator<'plan> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        subtree_next(self, &Snapshot::Oldest).copied()
    }
}

impl<'plan> Plan {
    #[must_use]
    pub fn exec_plan_subtree_iter(&'plan self, current: usize) -> ExecPlanSubtreeIterator<'plan> {
        ExecPlanSubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
        }
    }
}

#[allow(clippy::too_many_lines)]
fn subtree_next<'plan>(
    iter: &mut impl SubtreePlanIterator<'plan>,
    snapshot: &Snapshot,
) -> Option<&'plan usize> {
    if let Some(child) = iter.get_nodes().arena.get(iter.get_current()) {
        return match child {
            Node::Parameter | Node::Ddl(..) | Node::Acl(..) => None,
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
                Expression::Bool { left, right, .. }
                | Expression::Arithmetic { left, right, .. }
                | Expression::Concat { left, right } => {
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
                Expression::Constant { .. } | Expression::CountAsterisk => None,
                Expression::Reference { .. } => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;

                        // At first we need to detect the place where the reference is used:
                        // for selection filter or a join condition, we need to check whether
                        // the reference points to an **additional** sub-query and then traverse
                        // into it. Otherwise, stop traversal.
                        let Ok(parent_id) = exp.get_parent() else {
                            return None
                        };
                        if let Ok(rel_id) = iter
                            .get_plan()
                            .get_relational_from_reference_node(iter.get_current())
                        {
                            match iter.get_plan().get_relation_node(*rel_id) {
                                Ok(rel_node) if rel_node.is_subquery() || rel_node.is_motion() => {
                                    // Check if the sub-query is an additional one.
                                    let parent = iter.get_plan().get_relation_node(parent_id);
                                    let mut is_additional = false;
                                    if let Ok(
                                        Relational::Selection { children, .. }
                                        | Relational::Having { children, .. },
                                    ) = parent
                                    {
                                        if children.iter().skip(1).any(|&c| c == *rel_id) {
                                            is_additional = true;
                                        }
                                    }
                                    if let Ok(Relational::Join { children, .. }) = parent {
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
                Relational::Join {
                    children,
                    condition,
                    output,
                    ..
                } => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    match step.cmp(&2) {
                        Ordering::Less => {
                            return children.get(step);
                        }
                        Ordering::Equal => match snapshot {
                            Snapshot::Latest => Some(condition),
                            Snapshot::Oldest => {
                                return Some(
                                    iter.get_plan()
                                        .undo
                                        .get_oldest(condition)
                                        .map_or_else(|| condition, |id| id),
                                );
                            }
                        },
                        Ordering::Greater => {
                            if step == 3 && iter.need_output() {
                                return Some(output);
                            }
                            None
                        }
                    }
                }

                Relational::Except {
                    children, output, ..
                }
                | Relational::Insert {
                    children, output, ..
                }
                | Relational::Delete {
                    children, output, ..
                }
                | Relational::ScanSubQuery {
                    children, output, ..
                }
                | Relational::UnionAll {
                    children, output, ..
                } => {
                    let step = *iter.get_child().borrow();
                    if step < children.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return children.get(step);
                    }
                    if iter.need_output() && step == children.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(output);
                    }
                    None
                }
                Relational::GroupBy {
                    children,
                    output,
                    gr_cols,
                    ..
                } => {
                    let step = *iter.get_child().borrow();
                    if step == 0 {
                        *iter.get_child().borrow_mut() += 1;
                        return children.get(step);
                    }
                    let col_idx = step - 1;
                    if col_idx < gr_cols.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return gr_cols.get(col_idx);
                    }
                    if iter.need_output() && col_idx == gr_cols.len() {
                        *iter.get_child().borrow_mut() += 1;
                        return Some(output);
                    }
                    None
                }
                Relational::Motion {
                    children,
                    output,
                    policy,
                    ..
                } => {
                    if policy.is_local() || iter.need_motion_subtree() {
                        let step = *iter.get_child().borrow();
                        if step < children.len() {
                            *iter.get_child().borrow_mut() += 1;
                            return children.get(step);
                        }
                        if iter.need_output() && step == children.len() {
                            *iter.get_child().borrow_mut() += 1;
                            return Some(output);
                        }
                    } else {
                        let step = *iter.get_child().borrow();
                        if iter.need_output() && step == 0 {
                            *iter.get_child().borrow_mut() += 1;
                            return Some(output);
                        }
                    }
                    None
                }
                Relational::Values {
                    output, children, ..
                }
                | Relational::Update {
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
                    children,
                    filter,
                    output,
                    ..
                }
                | Relational::Having {
                    children,
                    filter,
                    output,
                } => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    match step.cmp(&1) {
                        Ordering::Less => {
                            return children.get(step);
                        }
                        Ordering::Equal => match snapshot {
                            Snapshot::Latest => Some(filter),
                            Snapshot::Oldest => {
                                return Some(
                                    iter.get_plan()
                                        .undo
                                        .get_oldest(filter)
                                        .map_or_else(|| filter, |id| id),
                                );
                            }
                        },
                        Ordering::Greater => {
                            if step == 2 && iter.need_output() {
                                return Some(output);
                            }
                            None
                        }
                    }
                }
                Relational::ValuesRow { data, output, .. } => {
                    let step = *iter.get_child().borrow();

                    *iter.get_child().borrow_mut() += 1;
                    if step == 0 {
                        return Some(data);
                    }
                    if iter.need_output() && step == 1 {
                        return Some(output);
                    }
                    None
                }
                Relational::ScanRelation { output, .. } => {
                    if iter.need_output() {
                        let step = *iter.get_child().borrow();

                        *iter.get_child().borrow_mut() += 1;
                        if step == 0 {
                            return Some(output);
                        }
                    }
                    None
                }
            },
        };
    }
    None
}
