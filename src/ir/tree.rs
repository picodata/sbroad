use std::cell::RefCell;
use std::cmp::Ordering;

use super::expression::Expression;
use super::operator::{Bool, Relational};
use super::{Node, Nodes};

/// Relational node's child iterator.
///
/// Iterator returns next relational node in the plan tree.
#[derive(Debug)]
pub struct RelationalIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
}

/// Expression node's children iterator.
///
/// Iterator returns the next child for expression
/// nodes. It is required to use `traversal` crate.
#[derive(Debug)]
pub struct ExpressionIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
    make_row_leaf: bool,
}

#[derive(Debug)]
pub struct SubtreeIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
}

/// Children iterator for "and"-ed equivalent expressions.
///
/// Iterator returns the next child for the chained `Bool::And`
/// and `Bool::Eq` nodes.
#[derive(Debug)]
pub struct EqClassIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
}

/// Children iterator for "and"-ed expression chains.
///
/// Iterator returns the next child for the chained `Bool::And` nodes.
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

    #[must_use]
    pub fn subtree_iter(&'n self, current: &'n usize) -> SubtreeIterator<'n> {
        SubtreeIterator {
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
                let mut is_leaf = true;
                for col in list {
                    if let Some(Node::Expression(
                        Expression::Reference { .. } | Expression::Constant { .. },
                    )) = self.nodes.arena.get(*col)
                    {
                    } else {
                        is_leaf = false;
                        break;
                    }
                }
                if !is_leaf || !self.make_row_leaf {
                    let child_step = *self.child.borrow();
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
            Some(
                Node::Expression(Expression::Constant { .. } | Expression::Reference { .. })
                | Node::Relational(_),
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
                Relational::InnerJoin { children, .. }
                | Relational::Motion { children, .. }
                | Relational::Projection { children, .. }
                | Relational::ScanSubQuery { children, .. }
                | Relational::Selection { children, .. }
                | Relational::UnionAll { children, .. },
            )) => {
                let step = *self.child.borrow();
                if step < children.len() {
                    *self.child.borrow_mut() += 1;
                    return children.get(step);
                }
                None
            }
            Some(Node::Relational(Relational::ScanRelation { .. }) | Node::Expression(_))
            | None => None,
        }
    }
}

impl<'n> Iterator for SubtreeIterator<'n> {
    type Item = &'n usize;

    #[allow(clippy::too_many_lines)]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(child) = self.nodes.arena.get(*self.current) {
            return match child {
                Node::Expression(exp) => match exp {
                    Expression::Alias { child, .. } => {
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
                    Expression::Row { list, .. } => {
                        let child_step = *self.child.borrow();
                        return match list.get(child_step) {
                            None => None,
                            Some(child) => {
                                *self.child.borrow_mut() += 1;
                                Some(child)
                            }
                        };
                    }
                    Expression::Constant { .. } | Expression::Reference { .. } => None,
                },

                Node::Relational(r) => match r {
                    Relational::InnerJoin {
                        children,
                        condition,
                        ..
                    } => {
                        let step = *self.child.borrow();

                        *self.child.borrow_mut() += 1;
                        match step.cmp(&children.len()) {
                            Ordering::Less => {
                                return children.get(step);
                            }
                            Ordering::Equal => {
                                return Some(condition);
                            }
                            Ordering::Greater => None,
                        }
                    }

                    Relational::Motion { children, .. }
                    | Relational::ScanSubQuery { children, .. }
                    | Relational::UnionAll { children, .. } => {
                        let step = *self.child.borrow();
                        if step < children.len() {
                            *self.child.borrow_mut() += 1;
                            return children.get(step);
                        }
                        None
                    }
                    Relational::Projection {
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
                        match step.cmp(&children.len()) {
                            Ordering::Less => {
                                return children.get(step);
                            }
                            Ordering::Equal => {
                                return Some(filter);
                            }
                            Ordering::Greater => None,
                        }
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
