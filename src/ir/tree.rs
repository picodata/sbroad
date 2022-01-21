use super::expression::Expression;
use super::operator::{Bool, Relational};
use super::{Node, Nodes};
use std::cell::RefCell;

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
    pub fn eq_iter(&'n self, current: &'n usize) -> EqClassIterator<'n> {
        EqClassIterator {
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
                if !is_leaf {
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

#[cfg(test)]
mod tests;
