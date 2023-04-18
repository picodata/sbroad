//! Intermediate representation (IR) module.
//!
//! Contains the logical plan tree and helpers.

use base64ct::{Base64, Encoding};
use serde::{Deserialize, Serialize};

use std::slice::Iter;

use expression::Expression;
use operator::{Arithmetic, Relational};
use relation::Table;

use crate::errors::{Action, Entity, SbroadError};
use crate::ir::undo::TransformationLog;

use self::parameters::Parameters;
use self::relation::Relations;

pub mod aggregates;
pub mod distribution;
pub mod expression;
pub mod function;
pub mod helpers;
pub mod operator;
pub mod parameters;
pub mod relation;
pub mod transformation;
pub mod tree;
pub mod undo;
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Node {
    Expression(Expression),
    Relational(Relational),
    Parameter,
}

/// Plan nodes storage.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Nodes {
    /// We don't want to mess with the borrow checker and RefCell/Rc,
    /// so all nodes are stored in the single arena ("nodes" array).
    /// The positions in the array act like pointers, so it is possible
    /// only to add nodes to the plan, but never remove them.
    arena: Vec<Node>,
}

impl Nodes {
    /// Get the amount of relational nodes in the plan.
    #[must_use]
    pub fn relation_node_amount(&self) -> usize {
        self.arena
            .iter()
            .filter(|node| matches!(node, Node::Relational(_)))
            .count()
    }

    /// Get the amount of expression nodes in the plan.
    #[must_use]
    pub fn expression_node_amount(&self) -> usize {
        self.arena
            .iter()
            .filter(|node| matches!(node, Node::Expression(_)))
            .count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.arena.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.arena.len()
    }

    pub fn iter(&self) -> Iter<'_, Node> {
        self.arena.iter()
    }

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

    /// Replace a node in arena with another one.
    ///
    /// # Errors
    /// - The node with the given position doesn't exist.
    pub fn replace(&mut self, id: usize, node: Node) -> Result<Node, SbroadError> {
        if id >= self.arena.len() {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "can't replace node with id {id} as it is out of arena bounds"
            )));
        }
        let old_node = std::mem::replace(&mut self.arena[id], node);
        Ok(old_node)
    }

    pub fn reserve(&mut self, capacity: usize) {
        self.arena.reserve(capacity);
    }

    pub fn shrink_to_fit(&mut self) {
        self.arena.shrink_to_fit();
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Slice {
    slice: Vec<usize>,
}

impl From<Vec<usize>> for Slice {
    fn from(vec: Vec<usize>) -> Self {
        Self { slice: vec }
    }
}

impl Slice {
    #[must_use]
    pub fn position(&self, index: usize) -> Option<&usize> {
        self.slice.get(index)
    }

    #[must_use]
    pub fn positions(&self) -> &[usize] {
        &self.slice
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Slices {
    pub slices: Vec<Slice>,
}

impl From<Vec<Slice>> for Slices {
    fn from(vec: Vec<Slice>) -> Self {
        Self { slices: vec }
    }
}

impl From<Vec<Vec<usize>>> for Slices {
    fn from(vec: Vec<Vec<usize>>) -> Self {
        Self {
            slices: vec.into_iter().map(Slice::from).collect(),
        }
    }
}

impl Slices {
    #[must_use]
    pub fn slice(&self, index: usize) -> Option<&Slice> {
        self.slices.get(index)
    }

    #[must_use]
    pub fn slices(&self) -> &[Slice] {
        self.slices.as_ref()
    }

    #[must_use]
    pub fn empty() -> Self {
        Self { slices: vec![] }
    }
}

/// Logical plan tree structure.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Plan {
    /// Append only arena for the plan nodes.
    pub(crate) nodes: Nodes,
    /// Relations are stored in a hash-map, with a table name acting as a
    /// key to guarantee its uniqueness across the plan.
    pub(crate) relations: Relations,
    /// Slice is a plan subtree under Motion node, that can be executed
    /// on a single db instance without data distribution problems (we add
    /// Motions to resolve them). Them we traverse the plan tree and collect
    /// Motions level by level in a bottom-up manner to the "slices" array
    /// of arrays. All the slices on the same level can be executed in parallel.
    /// In fact, "slices" is a prepared set of commands for the executor.
    pub(crate) slices: Slices,
    /// The plan top is marked as optional for tree creation convenience.
    /// We build the plan tree in a bottom-up manner, so the top would
    /// be added last. The plan without a top should be treated as invalid.
    top: Option<usize>,
    /// The flag is enabled if user wants to get a query plan only.
    /// In this case we don't need to execute query.
    is_explain: bool,
    /// The undo log keeps the history of the plan transformations. It can
    /// be used to revert the plan subtree to some previous snapshot if needed.
    pub(crate) undo: TransformationLog,
    /// Maps parameter to the corresponding constant node.
    pub(crate) constants: Parameters,
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
        self.relations.insert(table);
    }

    /// Check that plan tree is valid.
    ///
    /// # Errors
    /// Returns `SbroadError` when the plan tree check fails.
    pub fn check(&self) -> Result<(), SbroadError> {
        match self.top {
            None => {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some("plan tree top is None".into()),
                ))
            }
            Some(top) => {
                if self.nodes.arena.get(top).is_none() {
                    return Err(SbroadError::NotFound(
                        Entity::Node,
                        format!("from arena with index {top}"),
                    ));
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
            relations: Relations::new(),
            slices: Slices { slices: vec![] },
            top: None,
            is_explain: false,
            undo: TransformationLog::new(),
            constants: Parameters::new(),
        }
    }

    /// Check if the plan arena is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.arena.is_empty()
    }

    /// Get a node by its pointer (position in the node arena).
    ///
    /// # Errors
    /// Returns `SbroadError` when the node with requested index
    /// doesn't exist.
    pub fn get_node(&self, id: usize) -> Result<&Node, SbroadError> {
        match self.nodes.arena.get(id) {
            None => Err(SbroadError::NotFound(
                Entity::Node,
                format!("from arena with index {id}"),
            )),
            Some(node) => Ok(node),
        }
    }

    /// Get a mutable node by its pointer (position in the node arena).
    ///
    /// # Errors
    /// Returns `SbroadError` when the node with requested index
    /// doesn't exist.
    pub fn get_mut_node(&mut self, id: usize) -> Result<&mut Node, SbroadError> {
        match self.nodes.arena.get_mut(id) {
            None => Err(SbroadError::NotFound(
                Entity::Node,
                format!("(mutable) from arena with index {id}"),
            )),
            Some(node) => Ok(node),
        }
    }

    /// Get a top node of the plan tree.
    ///
    /// # Errors
    /// - top node is None (i.e. invalid plan)
    pub fn get_top(&self) -> Result<usize, SbroadError> {
        self.top
            .ok_or_else(|| SbroadError::Invalid(Entity::Plan, Some("plan tree top is None".into())))
    }

    /// Clone plan slices.
    #[must_use]
    pub fn clone_slices(&self) -> Slices {
        self.slices.clone()
    }

    /// Get relation in the plan by its name.
    #[must_use]
    pub fn get_relation(&self, name: &str) -> Option<&Table> {
        self.relations.get(name)
    }

    /// Construct a plan from the YAML file.
    ///
    /// # Errors
    /// Returns `SbroadError` when the YAML plan is invalid.
    pub fn from_yaml(s: &str) -> Result<Self, SbroadError> {
        let plan: Plan = match serde_yaml::from_str(s) {
            Ok(p) => p,
            Err(e) => return Err(SbroadError::Invalid(Entity::Plan, Some(e.to_string()))),
        };
        plan.check()?;
        Ok(plan)
    }

    /// Helper function for writing tests with yaml
    ///
    /// # Errors
    /// Returns `SbroadError` when serde failed to serialize the plan.
    pub fn to_yaml(&self) -> Result<String, SbroadError> {
        let s = match serde_yaml::to_string(self) {
            Ok(s) => s,
            Err(e) => return Err(SbroadError::Invalid(Entity::Plan, Some(e.to_string()))),
        };
        Ok(s)
    }

    /// Get relational node and produce a new row without aliases from its output (row with aliases).
    ///
    /// # Errors
    /// - node is not relational
    /// - node's output is not a row of aliases
    pub fn get_row_from_rel_node(&mut self, node: usize) -> Result<usize, SbroadError> {
        if let Node::Relational(rel) = self.get_node(node)? {
            if let Node::Expression(Expression::Row { list, .. }) = self.get_node(rel.output())? {
                let mut cols: Vec<usize> = Vec::with_capacity(list.len());
                for alias in list {
                    if let Node::Expression(Expression::Alias { child, .. }) =
                        self.get_node(*alias)?
                    {
                        cols.push(*child);
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some("node's output is not a row of aliases".into()),
                        ));
                    }
                }
                return Ok(self.nodes.add_row(cols, None));
            }
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some("node is not Relational type".into()),
        ))
    }

    #[must_use]
    pub fn next_id(&self) -> usize {
        self.nodes.next_id()
    }

    /// Add condition node to the plan.
    ///
    /// # Errors
    /// Returns `SbroadError` when the condition node can't append'.
    pub fn add_cond(
        &mut self,
        left: usize,
        op: operator::Bool,
        right: usize,
    ) -> Result<usize, SbroadError> {
        self.nodes.add_bool(left, op, right)
    }

    /// Add rithmetic node to the plan.
    ///
    /// # Errors
    /// Returns `SbroadError` when the condition node can't append'.
    pub fn add_arithmetic_to_plan(
        &mut self,
        left: usize,
        op: Arithmetic,
        right: usize,
        with_parentheses: bool,
    ) -> Result<usize, SbroadError> {
        self.nodes
            .add_arithmetic_node(left, op, right, with_parentheses)
    }

    /// Add unary operator node to the plan.
    ///
    /// # Errors
    /// - Child node is invalid
    pub fn add_unary(&mut self, op: operator::Unary, child: usize) -> Result<usize, SbroadError> {
        self.nodes.add_unary_bool(op, child)
    }

    /// Marks plan as query explain
    pub fn mark_as_explain(&mut self) {
        self.is_explain = true;
    }

    /// Checks that plan is explain query
    #[must_use]
    pub fn is_expain(&self) -> bool {
        self.is_explain
    }

    /// Set top node of plan
    /// # Errors
    /// - top node doesn't exist in the plan.
    pub fn set_top(&mut self, top: usize) -> Result<(), SbroadError> {
        self.get_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get relation type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not a relational type
    pub fn get_relation_node(&self, node_id: usize) -> Result<&Relational, SbroadError> {
        match self.get_node(node_id)? {
            Node::Relational(rel) => Ok(rel),
            Node::Expression(_) | Node::Parameter => Err(SbroadError::Invalid(
                Entity::Node,
                Some("node is not Relational type".into()),
            )),
        }
    }

    /// Get mutable relation type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not a relational type
    pub fn get_mut_relation_node(
        &mut self,
        node_id: usize,
    ) -> Result<&mut Relational, SbroadError> {
        match self.get_mut_node(node_id)? {
            Node::Relational(rel) => Ok(rel),
            Node::Expression(_) | Node::Parameter => Err(SbroadError::Invalid(
                Entity::Node,
                Some("Node is not relational".into()),
            )),
        }
    }

    /// Get expression type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not expression type
    pub fn get_expression_node(&self, node_id: usize) -> Result<&Expression, SbroadError> {
        match self.get_node(node_id)? {
            Node::Expression(exp) => Ok(exp),
            Node::Parameter => {
                let node = self.constants.get(node_id);
                if let Some(Node::Expression(exp)) = node {
                    Ok(exp)
                } else {
                    Err(SbroadError::Invalid(
                        Entity::Node,
                        Some("parameter node does not refer to an expression".into()),
                    ))
                }
            }
            Node::Relational(_) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("node is not Expression type".into()),
            )),
        }
    }

    /// Get mutable expression type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not expression type
    pub fn get_mut_expression_node(
        &mut self,
        node_id: usize,
    ) -> Result<&mut Expression, SbroadError> {
        match self.get_mut_node(node_id)? {
            Node::Expression(exp) => Ok(exp),
            Node::Relational(_) | Node::Parameter => Err(SbroadError::Invalid(
                Entity::Node,
                Some("node is not expression type".into()),
            )),
        }
    }

    /// Gets list of `Row` children ids
    ///
    /// # Errors
    /// - supplied id does not correspond to `Row` node
    pub fn get_row_list(&self, row_id: usize) -> Result<&[usize], SbroadError> {
        self.get_expression_node(row_id)?.get_row_list()
    }

    /// Gets `GroupBy` column by idx
    ///
    /// # Errors
    /// - supplied index is out of range
    /// - node is not `GroupBy`
    pub fn get_groupby_col(&self, groupby_id: usize, col_idx: usize) -> Result<usize, SbroadError> {
        let node = self.get_relation_node(groupby_id)?;
        if let Relational::GroupBy { gr_cols, .. } = node {
            let col_id = gr_cols.get(col_idx).ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(format!(
                    "groupby column index out of range. Node: {node:?}"
                ))
            })?;
            return Ok(*col_id);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("Expected GroupBy node. Got: {node:?}")),
        ))
    }

    /// Get alias string for `Reference` node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not `Reference`
    /// - invalid references between nodes
    pub fn get_alias_from_reference_node(&self, node: &Expression) -> Result<&str, SbroadError> {
        if let Expression::Reference {
            targets,
            position,
            parent,
        } = node
        {
            let ref_node = if let Some(parent) = parent {
                self.get_relation_node(*parent)?
            } else {
                return Err(SbroadError::UnexpectedNumberOfValues(
                    "Reference node has no parent".into(),
                ));
            };

            // In a case of insert we don't inspect children output tuple
            // but rather use target relation columns.
            if let Relational::Insert { ref relation, .. } = ref_node {
                let rel = self
                    .relations
                    .get(relation)
                    .ok_or_else(|| SbroadError::NotFound(Entity::Table, relation.to_string()))?;
                let col_name = rel
                    .columns
                    .get(*position)
                    .ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Table,
                            "{relation}'s column {position}".into(),
                        )
                    })?
                    .name
                    .as_str();
                return Ok(col_name);
            }

            if let Some(list_of_column_nodes) = ref_node.children() {
                let child_ids = targets.as_ref().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Target,
                        Some("node refs to scan node, not alias".into()),
                    )
                })?;
                let column_index_in_list = child_ids.first().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues("Target has no children".into())
                })?;
                let col_idx_in_rel =
                    list_of_column_nodes
                        .get(*column_index_in_list)
                        .ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Node,
                                format!("type Column with index {column_index_in_list}"),
                            )
                        })?;

                let column_rel_node = self.get_relation_node(*col_idx_in_rel)?;
                let column_expr_node = self.get_expression_node(column_rel_node.output())?;

                let col_alias_idx =
                    column_expr_node
                        .get_row_list()?
                        .get(*position)
                        .ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Column,
                                format!("at position {position} in row list"),
                            )
                        })?;

                let col_alias_node = self.get_expression_node(*col_alias_idx)?;
                match col_alias_node {
                    Expression::Alias { name, .. } => return Ok(name),
                    _ => {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some("expected alias node".into()),
                        ))
                    }
                }
            }

            return Err(SbroadError::FailedTo(
                Action::Get,
                None,
                "a referred relational node".into(),
            ));
        }

        Err(SbroadError::Invalid(
            Entity::Node,
            Some("node is not of a reference type".into()),
        ))
    }

    /// Set slices of the plan.
    pub fn set_slices(&mut self, slices: Vec<Vec<usize>>) {
        self.slices = slices.into();
    }

    /// # Errors
    /// - serialization error (to binary)
    pub fn pattern_id(&self) -> Result<String, SbroadError> {
        let mut bytes: Vec<u8> = bincode::serialize(&self.nodes).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                None,
                format!("plan nodes to binary: {e:?}"),
            )
        })?;
        let mut relation_bytes: Vec<u8> = bincode::serialize(&self.relations).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                None,
                format!("plan relations to binary: {e:?}"),
            )
        })?;
        let mut slice_bytes: Vec<u8> = bincode::serialize(&self.slices).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                None,
                format!("plan slices to binary: {e:?}"),
            )
        })?;
        let mut top_bytes: Vec<u8> = bincode::serialize(&self.top).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                None,
                format!("plan top to binary: {e:?}"),
            )
        })?;
        bytes.append(&mut relation_bytes);
        bytes.append(&mut slice_bytes);
        bytes.append(&mut top_bytes);

        let hash = Base64::encode_string(blake3::hash(&bytes).to_hex().as_bytes());
        Ok(hash)
    }
}

pub mod api;
mod explain;
#[cfg(test)]
mod tests;
