//! Intermediate representation.
//!
//! Contains the logical plan tree and helpers.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use expression::Expression;
use operator::Relational;
use relation::Table;

use crate::errors::QueryPlannerError;
use crate::ir::value::Value;

pub mod distribution;
pub mod expression;
pub mod helpers;
pub mod operator;
pub mod relation;
pub mod transformation;
pub mod tree;
pub mod value;

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
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Node {
    Expression(Expression),
    Relational(Relational),
}

/// Plan nodes storage.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Nodes {
    /// We don't want to mess with the borrow checker and RefCell/Rc,
    /// so all nodes are stored in the single arena ("nodes" array).
    /// The positions in the array act like pointers, so it is possible
    /// only to add nodes to the plan, but never remove them.
    arena: Vec<Node>,
}

impl Nodes {
    /// Add new node to arena.
    ///
    /// Inserts a new node to the arena and returns its position,
    /// that is treated as a pointer.
    pub fn push(&mut self, node: Node) -> usize {
        let position = self.arena.len();
        self.arena.push(node);
        position
    }

    /// Returns the next node position
    #[must_use]
    pub fn next_id(&self) -> usize {
        self.arena.len()
    }
}

/// Logical plan tree structure.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Plan {
    /// Append only arena for the plan nodes.
    pub(crate) nodes: Nodes,
    /// Relations are stored in a hash-map, with a table name acting as a
    /// key to guarantee its uniqueness across the plan. The map is marked
    /// optional because plans without relations do exist (`select 1`).
    pub(crate) relations: Option<HashMap<String, Table>>,
    /// A special map to translate logical node ids to the positions in the
    /// arena.
    relational_map: Option<HashMap<usize, usize>>,
    /// Slice is a plan subtree under Motion node, that can be executed
    /// on a single db instance without data distribution problems (we add
    /// Motions to resolve them). Them we traverse the plan tree and collect
    /// Motions level by level in a bottom-up manner to the "slices" array
    /// of arrays. All the slices on the same level can be executed in parallel.
    /// In fact, "slices" is a prepared set of commands for the executor.
    slices: Option<Vec<Vec<usize>>>,
    /// The plan top is marked as optional for tree creation convenience.
    /// We build the plan tree in a bottom-up manner, so the top would
    /// be added last. The plan without a top should be treated as invalid.
    top: Option<usize>,
}

impl Default for Plan {
    fn default() -> Self {
        Self::new()
    }
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
                if self.nodes.arena.get(top).is_none() {
                    return Err(QueryPlannerError::ValueOutOfRange);
                }
            }
        }

        //TODO: additional consistency checks

        Ok(())
    }

    /// Constructor for an empty plan structure.
    #[must_use]
    pub fn new() -> Self {
        Plan {
            nodes: Nodes { arena: Vec::new() },
            relations: None,
            slices: None,
            top: None,
            relational_map: None,
        }
    }

    /// Get a node by its pointer (position in the node arena).
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the node with requested index
    /// doesn't exist.
    pub fn get_node(&self, pos: usize) -> Result<&Node, QueryPlannerError> {
        match self.nodes.arena.get(pos) {
            None => Err(QueryPlannerError::ValueOutOfRange),
            Some(node) => Ok(node),
        }
    }

    /// Get a top node of the plan tree.
    ///
    /// # Errors
    /// - top node is None (i.e. invalid plan)
    pub fn get_top(&self) -> Result<usize, QueryPlannerError> {
        self.top.ok_or(QueryPlannerError::InvalidPlan)
    }

    /// Get plan slices.
    #[must_use]
    pub fn get_slices(&self) -> Option<Vec<Vec<usize>>> {
        self.slices.clone()
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

    /// Build {logical id: position} map for relational nodes
    pub fn build_relational_map(&mut self) {
        if self.relational_map.is_none() {
            let mut map: HashMap<usize, usize> = HashMap::new();
            for (pos, node) in self.nodes.arena.iter().enumerate() {
                if let Node::Relational(relational) = node {
                    map.insert(relational.logical_id(), pos);
                }
            }
            self.relational_map = Some(map);
        }
    }

    /// Get relational node and produce a new row without aliases from its output (row with aliases).
    ///
    /// # Errors
    /// - node is not relational
    /// - node's output is not a row of aliases
    pub fn get_row_from_rel_node(&mut self, node: usize) -> Result<usize, QueryPlannerError> {
        if let Node::Relational(rel) = self.get_node(node)? {
            if let Node::Expression(Expression::Row { list, .. }) = self.get_node(rel.output())? {
                let mut cols: Vec<usize> = Vec::new();
                for alias in list {
                    if let Node::Expression(Expression::Alias { child, .. }) =
                        self.get_node(*alias)?
                    {
                        cols.push(*child);
                    } else {
                        return Err(QueryPlannerError::InvalidNode);
                    }
                }
                return Ok(self.nodes.add_row(cols, None));
            }
        }
        Err(QueryPlannerError::InvalidRelation)
    }

    #[must_use]
    pub fn next_id(&self) -> usize {
        self.nodes.next_id()
    }

    /// Add constant value to the plan.
    pub fn add_const(&mut self, v: Value) -> usize {
        self.nodes.add_const(v)
    }

    /// Add condition note to the plan.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the condition node can't append'.
    pub fn add_cond(
        &mut self,
        left: usize,
        op: operator::Bool,
        right: usize,
    ) -> Result<usize, QueryPlannerError> {
        self.nodes.add_bool(left, op, right)
    }

    /// Set top node of plan
    /// # Errors
    /// - top node doesn't exist in the plan.
    pub fn set_top(&mut self, top: usize) -> Result<(), QueryPlannerError> {
        self.get_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get relation type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    pub fn get_relation_node(&self, node_id: usize) -> Result<&Relational, QueryPlannerError> {
        match self.get_node(node_id)? {
            Node::Relational(rel) => Ok(rel),
            Node::Expression(_) => Err(QueryPlannerError::CustomError(
                "Node isn't relational".into(),
            )),
        }
    }

    /// Get expression type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not expression type
    pub fn get_expression_node(&self, node_id: usize) -> Result<&Expression, QueryPlannerError> {
        match self.get_node(node_id)? {
            Node::Expression(exp) => Ok(exp),
            Node::Relational(_) => Err(QueryPlannerError::CustomError(
                "Node isn't expression".into(),
            )),
        }
    }

    /// Gets linked node from relational map
    ///
    /// # Errors
    /// - relation map wasn't init
    /// - relation node not found for current index
    pub fn get_map_relational_value(&self, key: usize) -> Result<usize, QueryPlannerError> {
        let result = *self
            .relational_map
            .as_ref()
            .ok_or_else(|| {
                QueryPlannerError::CustomError("Relation map doesn't initialize".into())
            })?
            .get(&key)
            .ok_or_else(|| {
                QueryPlannerError::CustomError("Node not found in relational map".into())
            })?;

        Ok(result)
    }

    /// Get relation node by index in internal table map
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not relation type
    pub fn get_mapping_relation_node(
        &self,
        node_id: usize,
    ) -> Result<&Relational, QueryPlannerError> {
        let rel_id = self.get_map_relational_value(node_id)?;

        self.get_relation_node(rel_id)
    }

    /// Get alias string for `Reference` node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not `Reference`
    /// - invalid references between nodes
    pub fn get_alias_from_reference_node(
        &self,
        node: &Expression,
    ) -> Result<String, QueryPlannerError> {
        if let Expression::Reference {
            targets,
            position,
            parent,
        } = node
        {
            let ref_node = self.get_mapping_relation_node(*parent)?;

            if let Some(list_of_column_nodes) = ref_node.children() {
                let child_ids = targets.clone().ok_or_else(|| {
                    QueryPlannerError::CustomError("Node refs to scan node, not alias".into())
                })?;
                let column_index_in_list = child_ids.get(0).ok_or_else(|| {
                    QueryPlannerError::CustomError("Invalid child index in target".into())
                })?;
                let col_idx_in_rel =
                    list_of_column_nodes
                        .get(*column_index_in_list)
                        .ok_or_else(|| {
                            QueryPlannerError::CustomError(format!(
                                "Not found column node with index {}",
                                column_index_in_list
                            ))
                        })?;

                let column_rel_node = self.get_relation_node(*col_idx_in_rel)?;
                let column_expr_node = self.get_expression_node(column_rel_node.output())?;

                let col_alias_idx = *column_expr_node
                    .extract_row_list()?
                    .get(*position)
                    .ok_or_else(|| {
                        QueryPlannerError::CustomError("Invalid position in row list".into())
                    })?;

                let name = self.get_expression_node(col_alias_idx)?.get_alias_name()?;

                return Ok(name);
            }
        }

        Err(QueryPlannerError::CustomError(
            "Node isn't reference type".into(),
        ))
    }

    /// Set slices of the plan.
    pub fn set_slices(&mut self, slices: Option<Vec<Vec<usize>>>) {
        self.slices = slices;
    }
}

#[cfg(test)]
mod tests;
