//! Intermediate representation.
//!
//! Contains the logical plan tree and helpers.

pub mod distribution;
pub mod expression;
pub mod operator;
pub mod relation;
pub mod traversal;
pub mod value;

use crate::errors::QueryPlannerError;
use expression::Expression;
use operator::Relational;
use relation::Table;
use serde::{Deserialize, Serialize};
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

    /// Returns the next node position
    #[must_use]
    pub fn next_node_id(&self) -> usize {
        self.nodes.len()
    }

    /// Build {logical id: position} map for relational nodes
    #[must_use]
    pub fn relational_id_map(&self) -> HashMap<usize, usize> {
        let mut map: HashMap<usize, usize> = HashMap::new();
        for (pos, node) in self.nodes.iter().enumerate() {
            if let Node::Relational(relational) = node {
                map.insert(relational.logical_id(), pos);
            }
        }
        map
    }
}

#[cfg(test)]
mod tests;
