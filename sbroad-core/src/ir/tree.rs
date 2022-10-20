//! IR tree traversal module.

use std::cell::RefCell;
use std::cmp::Ordering;

use super::expression::Expression;
use super::operator::{Bool, Relational};
use super::{Node, Nodes, Plan};

/// Relational node's child iterator.
///
/// The iterator returns the next relational node in the plan tree.
#[derive(Debug)]
pub struct RelationalIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
}

/// Expression node's children iterator.
///
/// The iterator returns the next child for expression
/// nodes. It is required to use `traversal` crate.
#[derive(Debug)]
pub struct ExpressionIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
    make_row_leaf: bool,
}

/// Expression and relational nodes iterator.
#[derive(Debug)]
pub struct SubtreeIterator<'p> {
    current: &'p usize,
    child: RefCell<usize>,
    plan: &'p Plan,
}

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

/// Children iterator for "and"-ed expression chains.
///
/// The iterator returns the next child for the chained `Bool::And` nodes.
#[derive(Debug)]
pub struct AndIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
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

    #[must_use]
    pub fn eq_iter(&'n self, current: &'n usize) -> EqClassIterator<'n> {
        EqClassIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }

    #[must_use]
    pub fn and_iter(&'n self, current: &'n usize) -> AndIterator<'n> {
        AndIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }

    #[must_use]
    pub fn rel_iter(&'n self, current: &'n usize) -> RelationalIterator<'n> {
        RelationalIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }
}

impl<'p> Plan {
    #[must_use]
    pub fn subtree_iter(&'p self, current: &'p usize) -> SubtreeIterator<'p> {
        SubtreeIterator {
            current,
            child: RefCell::new(0),
            plan: self,
        }
    }
}

impl<'n> Iterator for ExpressionIterator<'n> {
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self.nodes.arena.get(*self.current) {
            Some(Node::Expression(
                Expression::Alias { child, .. }
                | Expression::Cast { child, .. }
                | Expression::Unary { child, .. },
            )) => {
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
                let mut is_leaf = false;

                // Check on the first step, if the row contains only leaf nodes.
                if child_step == 0 {
                    is_leaf = true;
                    for col in list {
                        if !matches!(
                            self.nodes.arena.get(*col),
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
                if !is_leaf || !self.make_row_leaf {
                    match list.get(child_step) {
                        None => return None,
                        Some(child) => {
                            *self.child.borrow_mut() += 1;
                            return Some(child);
                        }
                    }
                }

                None
            }
            Some(Node::Expression(Expression::StableFunction { children, .. })) => {
                let child_step = *self.child.borrow();
                match children.get(child_step) {
                    None => None,
                    Some(child) => {
                        *self.child.borrow_mut() += 1;
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
}

impl<'n> Iterator for EqClassIterator<'n> {
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(Node::Expression(Expression::Bool {
            left, op, right, ..
        })) = self.nodes.arena.get(*self.current)
        {
            if (*op != Bool::And) && (*op != Bool::Eq) {
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
        } else {
            None
        }
    }
}

impl<'n> Iterator for AndIterator<'n> {
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.nodes.arena.get(*self.current);
        if let Some(Node::Expression(Expression::Bool {
            left, op, right, ..
        })) = node
        {
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
        } else {
            None
        }
    }
}

impl<'n> Iterator for RelationalIterator<'n> {
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self.nodes.arena.get(*self.current) {
            Some(Node::Relational(
                Relational::Except { children, .. }
                | Relational::InnerJoin { children, .. }
                | Relational::Insert { children, .. }
                | Relational::Motion { children, .. }
                | Relational::Projection { children, .. }
                | Relational::ScanSubQuery { children, .. }
                | Relational::Selection { children, .. }
                | Relational::UnionAll { children, .. }
                | Relational::Values { children, .. }
                | Relational::ValuesRow { children, .. },
            )) => {
                let step = *self.child.borrow();
                if step < children.len() {
                    *self.child.borrow_mut() += 1;
                    return children.get(step);
                }
                None
            }
            Some(
                Node::Relational(Relational::ScanRelation { .. })
                | Node::Expression(_)
                | Node::Parameter,
            )
            | None => None,
        }
    }
}

impl<'p> Iterator for SubtreeIterator<'p> {
    type Item = &'p usize;

    #[allow(clippy::too_many_lines)]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(child) = self.plan.nodes.arena.get(*self.current) {
            return match child {
                Node::Parameter => None,
                Node::Expression(exp) => match exp {
                    Expression::Alias { child, .. }
                    | Expression::Cast { child, .. }
                    | Expression::Unary { child, .. } => {
                        let step = *self.child.borrow();
                        *self.child.borrow_mut() += 1;
                        if step == 0 {
                            return Some(child);
                        }
                        None
                    }
                    Expression::Bool { left, right, .. } => {
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
                    Expression::Row { list, .. }
                    | Expression::StableFunction { children: list, .. } => {
                        let child_step = *self.child.borrow();
                        return match list.get(child_step) {
                            None => None,
                            Some(child) => {
                                *self.child.borrow_mut() += 1;
                                Some(child)
                            }
                        };
                    }
                    Expression::Constant { .. } => None,
                    Expression::Reference { .. } => {
                        let step = *self.child.borrow();
                        if step == 0 {
                            *self.child.borrow_mut() += 1;

                            // At first we need to detect the place where the reference is used:
                            // for selection filter or a join condition, we need to check whether
                            // the reference points to an **additional** sub-query and then traverse
                            // into it. Otherwise, stop traversal.
                            let parent_id = match exp.get_parent() {
                                Ok(parent_id) => parent_id,
                                Err(_) => return None,
                            };
                            if let Ok(rel_id) =
                                self.plan.get_relational_from_reference_node(*self.current)
                            {
                                match self.plan.get_relation_node(*rel_id) {
                                    Ok(rel_node) if rel_node.is_subquery() => {
                                        // Check if the sub-query is an additional one.
                                        let parent = self.plan.get_relation_node(parent_id);
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
                        let step = *self.child.borrow();

                        *self.child.borrow_mut() += 1;
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
                        let step = *self.child.borrow();
                        if step < children.len() {
                            *self.child.borrow_mut() += 1;
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
                        let step = *self.child.borrow();
                        *self.child.borrow_mut() += 1;
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
                        let step = *self.child.borrow();

                        *self.child.borrow_mut() += 1;
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
                        let step = *self.child.borrow();

                        *self.child.borrow_mut() += 1;
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
}

#[cfg(test)]
mod tests;
