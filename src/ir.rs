//! Intermediate representation.
//!
//! Contains the logical plan tree and helpers.

pub mod expression;
pub mod operator;
pub mod relation;
pub mod value;

use crate::errors::QueryPlannerError;
use expression::{Branch, Distribution, Expression};
use operator::Relational;
use relation::Table;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;

/// Plan tree node.
///
/// There are two kinds of node variants: expressions and relational
/// operators. Both of them can easily refer each other in the
/// tree as they are stored in the same node arena. The reasons
/// to separate them are:
///
/// - they should be treated with quite different logic
/// - we don't want to have a single huge enum
///
/// Enum was chosen as we don't want to mess with dynamic
/// dispatching and its performance penalties.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Node {
    Expression(Expression),
    Relational(Relational),
}

/// Suggested distribution by the child relational node.
/// A wrapper for `Expression::suggest_distribution()`;
/// used by relational nodes when they calculate their
/// distribution.
///
/// # Errors
/// Returns `QueryPlannerError`:
/// - parent node's output is not a valid tuple
/// - child node is not relational
/// - child's output is not a valid tuple
fn child_dist(
    output: usize,
    child: usize,
    branch: &Branch,
    plan: &Plan,
) -> Result<Distribution, QueryPlannerError> {
    // Get current output tuple column list
    let aliases: &Vec<usize> =
        if let Node::Expression(Expression::Row { list, .. }) = plan.get_node(output)? {
            Ok(list)
        } else {
            Err(QueryPlannerError::InvalidRow)
        }?;

    // Distribution suggested by the child.
    if let Node::Relational(child_node) = plan.get_node(child)? {
        if let Node::Expression(child_row) = plan.get_node(child_node.output())? {
            Ok(child_row.suggest_distribution(branch, aliases, plan)?)
        } else {
            Err(QueryPlannerError::InvalidRow)
        }
    } else {
        Err(QueryPlannerError::InvalidPlan)
    }
}

/// Set output tuple distribution for the node.
///
/// # Errors
/// Returns `QueryPlannerError`:
/// - when node position doesn't exist in the plan node arena
/// - for nodes, that don't produce tuples (all expressions except `Row`)
/// - for relational nodes with invalid output or children
pub fn set_distribution(pointer: usize, plan: &mut Plan) -> Result<(), QueryPlannerError> {
    match plan.get_node(pointer)? {
        Node::Relational(relational) => {
            match relational {
                Relational::ScanRelation {
                    relation: table_name,
                    ..
                } => {
                    if let Some(relations) = &plan.relations {
                        if let Some(rel) = relations.get(table_name) {
                            // Update output tuple distribution to the relation's one.
                            match rel {
                                Table::Segment { key, .. } | Table::VirtualSegment { key, .. } => {
                                    let rel_tuple = relational.output();
                                    let node = plan
                                        .nodes
                                        .get_mut(rel_tuple)
                                        .ok_or(QueryPlannerError::ValueOutOfRange)?;
                                    if let Node::Expression(Expression::Row {
                                        ref mut distribution,
                                        ..
                                    }) = node
                                    {
                                        *distribution =
                                            Some(Distribution::Segment { key: key.clone() });
                                        return Ok(());
                                    }
                                    return Err(QueryPlannerError::InvalidRow);
                                }
                                Table::Virtual { .. } => {
                                    let rel_tuple = relational.output();
                                    let node = plan
                                        .nodes
                                        .get_mut(rel_tuple)
                                        .ok_or(QueryPlannerError::ValueOutOfRange)?;
                                    if let Node::Expression(Expression::Row {
                                        ref mut distribution,
                                        ..
                                    }) = node
                                    {
                                        *distribution = Some(Distribution::Random);
                                        return Ok(());
                                    }
                                    return Err(QueryPlannerError::InvalidRow);
                                }
                            }
                        }
                        return Err(QueryPlannerError::InvalidRelation);
                    }
                    Err(QueryPlannerError::EmptyPlanRelations)
                }
                Relational::Projection { child, output, .. }
                | Relational::Selection { child, output, .. }
                | Relational::ScanSubQuery { child, output, .. } => {
                    let dist = child_dist(*output, *child, &Branch::Left, plan)?;
                    let rel_tuple = relational.output();
                    let node = plan
                        .nodes
                        .get_mut(rel_tuple)
                        .ok_or(QueryPlannerError::ValueOutOfRange)?;

                    if let Node::Expression(Expression::Row {
                        ref mut distribution,
                        ..
                    }) = node
                    {
                        *distribution = Some(dist);
                        return Ok(());
                    }
                    Err(QueryPlannerError::InvalidNode)
                }
                Relational::UnionAll {
                    left,
                    right,
                    output,
                    ..
                } => {
                    let left_dist = child_dist(*output, *left, &Branch::Both, plan)?;

                    let right_dist = child_dist(*output, *right, &Branch::Both, plan)?;

                    let rel_tuple = relational.output();
                    let node = plan
                        .nodes
                        .get_mut(rel_tuple)
                        .ok_or(QueryPlannerError::ValueOutOfRange)?;
                    if let Node::Expression(Expression::Row {
                        ref mut distribution,
                        ..
                    }) = node
                    {
                        *distribution = Some(Distribution::new_union(left_dist, right_dist)?);
                        return Ok(());
                    }
                    Err(QueryPlannerError::InvalidRow)
                }
                // TODO: implement it!
                Relational::InnerJoin { .. } | Relational::Motion { .. } => {
                    Err(QueryPlannerError::QueryNotImplemented)
                }
            }
        }
        // TODO: how should we implement it for the `Row`?
        Node::Expression(_) => Err(QueryPlannerError::QueryNotImplemented),
    }
}

/// Plan node "allocator".
///
/// Inserts an element to the array and returns its position,
/// that is treated as a pointer.
pub fn vec_alloc<T>(v: &mut Vec<T>, item: T) -> usize {
    let idx = v.len();
    v.push(item);
    idx
}

/// Logical plan tree structure.
///
/// We don't want to mess with the borrow checker and RefCell/Rc,
/// so all nodes are stored in the single arena ("nodes" array).
/// The positions in the array act like pointers, so it is possible
/// only to add nodes to the plan, but never remove them.
///
/// Relations are stored in a hash-map, with a table name acting as a
/// key to guarantee its uniqueness across the plan. The map is marked
/// optional because plans without relations do exist (`select 1`).
///
/// Slice is a plan subtree under Motion node, that can be executed
/// on a single db instance without data distribution problems (we add
/// Motions to resolve them). Them we traverse the plan tree and collect
/// Motions level by level in a bottom-up manner to the "slices" array
/// of arrays. All the slices on the same level can be executed in parallel.
/// In fact, "slices" is a prepared set of commands for the executor.
///
/// The plan top is marked as optional for tree creation convenience.
/// We build the plan tree in a bottom-up manner, so the top would
/// be added last. The plan without a top should be treated as invalid.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Plan {
    nodes: Vec<Node>,
    relations: Option<HashMap<String, Table>>,
    slices: Option<Vec<Vec<usize>>>,
    top: Option<usize>,
}

#[allow(dead_code)]
impl Plan {
    /// Add relation to the plan.
    ///
    /// If relation already exists, do nothing.
    pub fn add_rel(&mut self, table: Table) {
        match &mut self.relations {
            None => {
                let mut map = HashMap::new();
                map.insert(String::from(table.name()), table);
                self.relations = Some(map);
            }
            Some(relations) => {
                relations.entry(String::from(table.name())).or_insert(table);
            }
        }
    }

    /// Check that plan tree is valid.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the plan tree check fails.
    pub fn check(&self) -> Result<(), QueryPlannerError> {
        match self.top {
            None => return Err(QueryPlannerError::InvalidPlan),
            Some(top) => {
                if self.nodes.get(top).is_none() {
                    return Err(QueryPlannerError::ValueOutOfRange);
                }
            }
        }

        //TODO: additional consistency checks

        Ok(())
    }

    /// Constructor for an empty plan structure.
    #[must_use]
    pub fn empty() -> Self {
        Plan {
            nodes: Vec::new(),
            relations: None,
            slices: None,
            top: None,
        }
    }

    /// Get a node by its pointer (position in the node arena).
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the node with requested index
    /// doesn't exist.
    pub fn get_node(&self, pos: usize) -> Result<&Node, QueryPlannerError> {
        match self.nodes.get(pos) {
            None => Err(QueryPlannerError::ValueOutOfRange),
            Some(node) => Ok(node),
        }
    }

    /// Construct a plan from the YAML file.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the YAML plan is invalid.
    pub fn from_yaml(s: &str) -> Result<Self, QueryPlannerError> {
        let plan: Plan = match serde_yaml::from_str(s) {
            Ok(p) => p,
            Err(_) => return Err(QueryPlannerError::Serialization),
        };
        plan.check()?;
        Ok(plan)
    }
}

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
        match self.node {
            Node::Expression(expr) => match expr {
                Expression::Alias { child, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*child);
                    }
                    None
                }
                Expression::Bool { left, right, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*left);
                    } else if current_step == 1 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*right);
                    }
                    None
                }
                Expression::Constant { .. } | Expression::Reference { .. } => None,
                Expression::Row { list, .. } => {
                    let current_step = *self.step.borrow();
                    if let Some(node) = list.get(current_step) {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*node);
                    }
                    None
                }
            },
            Node::Relational(rel) => match rel {
                Relational::InnerJoin {
                    left,
                    right,
                    condition,
                    ..
                } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*left);
                    } else if current_step == 1 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*right);
                    } else if current_step == 2 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*condition);
                    }
                    None
                }
                Relational::ScanRelation { .. } => None,
                Relational::ScanSubQuery { child, .. }
                | Relational::Motion { child, .. }
                | Relational::Selection { child, .. }
                | Relational::Projection { child, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*child);
                    }
                    None
                }
                Relational::UnionAll { left, right, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*left);
                    } else if current_step == 1 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*right);
                    }
                    None
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::relation::*;
    use pretty_assertions::assert_eq;
    use std::fs;
    use std::path::Path;

    #[test]
    fn plan_no_top() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("plan_no_top.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            QueryPlannerError::InvalidPlan,
            Plan::from_yaml(&s).unwrap_err()
        );
    }

    #[test]
    fn plan_oor_top() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("plan_oor_top.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            QueryPlannerError::ValueOutOfRange,
            Plan::from_yaml(&s).unwrap_err()
        );
    }

    #[test]
    fn get_node() {
        let mut plan = Plan::empty();

        let t = Table::new_seg("t", vec![Column::new("a", Type::Boolean)], &["a"]).unwrap();
        plan.add_rel(t);

        let scan = Relational::new_scan("t", &mut plan).unwrap();
        let scan_id = vec_alloc(&mut plan.nodes, Node::Relational(scan));

        if let Node::Relational(Relational::ScanRelation { relation, .. }) =
            plan.get_node(scan_id).unwrap()
        {
            assert_eq!(relation, "t");
        } else {
            panic!("Unexpected node returned!")
        }
    }

    #[test]
    fn get_node_oor() {
        let plan = Plan::empty();
        assert_eq!(
            QueryPlannerError::ValueOutOfRange,
            plan.get_node(42).unwrap_err()
        );
    }

    //TODO: add relation test
}
