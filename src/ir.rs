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
use distribution::Distribution;
use expression::Expression;
use operator::Relational;
use relation::Table;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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

    /// Calculate and set tuple distribution.
    ///
    /// As the references in the `Row` expression contain only logical ID of the parent relational nodes,
    /// we need at first traverse all the plan nodes and build a "logical id - array position" map with
    /// `relational_id_map()` function and pass its reference to this function.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when current expression is not a `Row` or contains broken references.
    pub fn set_distribution<S: ::std::hash::BuildHasher + Default>(
        &mut self,
        row_node: usize,
        id_map: &HashMap<usize, usize, S>,
    ) -> Result<(), QueryPlannerError> {
        let mut child_set: HashSet<usize> = HashSet::new();
        let mut child_pos_map: HashMap<(usize, usize), usize> = HashMap::new();
        let mut table_set: HashSet<String> = HashSet::new();
        let mut table_pos_map: HashMap<usize, usize> = HashMap::default();
        let mut parent_node: Option<usize> = None;

        if let Node::Expression(Expression::Row { list, .. }) = self.get_node(row_node)? {
            // Gather information about children nodes, that are pointed by the row references.
            for (pos, alias_node) in list.iter().enumerate() {
                if let Node::Expression(Expression::Alias { child, .. }) =
                    self.get_node(*alias_node)?
                {
                    if let Node::Expression(Expression::Reference {
                        targets,
                        position,
                        parent,
                        ..
                    }) = self.get_node(*child)?
                    {
                        // Get the relational node, containing this row
                        parent_node =
                            Some(*id_map.get(parent).ok_or(QueryPlannerError::InvalidNode)?);
                        if let Node::Relational(relational_op) =
                            self.get_node(parent_node.ok_or(QueryPlannerError::InvalidNode)?)?
                        {
                            if let Some(children) = relational_op.children() {
                                // References in the branch node.
                                let child_pos_list: &Vec<usize> = targets
                                    .as_ref()
                                    .ok_or(QueryPlannerError::InvalidReference)?;
                                for target in child_pos_list {
                                    let child_node: usize = *children
                                        .get(*target)
                                        .ok_or(QueryPlannerError::ValueOutOfRange)?;
                                    child_set.insert(child_node);
                                    child_pos_map.insert((child_node, *position), pos);
                                }
                            } else {
                                // References in the leaf (relation scan) node.
                                if targets.is_some() {
                                    return Err(QueryPlannerError::InvalidReference);
                                }
                                if let Relational::ScanRelation { relation, .. } = relational_op {
                                    table_set.insert(relation.clone());
                                    table_pos_map.insert(*position, pos);
                                } else {
                                    return Err(QueryPlannerError::InvalidReference);
                                }
                            }
                        } else {
                            return Err(QueryPlannerError::InvalidNode);
                        }
                    }
                }
            }
        }

        match child_set.len() {
            0 => {
                // Scan
                self.set_scan_dist(&table_set, &table_pos_map, row_node)?;
            }
            1 => {
                // Single child
                let child: usize = *child_set
                    .iter()
                    .next()
                    .ok_or(QueryPlannerError::InvalidNode)?;
                let suggested_dist = self.dist_from_child(child, &child_pos_map)?;
                if let Node::Expression(Expression::Row {
                    ref mut distribution,
                    ..
                }) = self
                    .nodes
                    .get_mut(row_node)
                    .ok_or(QueryPlannerError::InvalidRow)?
                {
                    *distribution = Some(suggested_dist);
                }
            }
            2 => {
                // Union, join
                self.set_two_children_node_dist(
                    &child_set,
                    &child_pos_map,
                    &parent_node,
                    row_node,
                )?;
            }
            _ => return Err(QueryPlannerError::InvalidReference),
        }
        Ok(())
    }

    // Private methods

    fn dist_from_child(
        &self,
        child_rel_node: usize,
        child_pos_map: &HashMap<(usize, usize), usize>,
    ) -> Result<Distribution, QueryPlannerError> {
        if let Node::Relational(relational_op) = self.get_node(child_rel_node)? {
            if let Node::Expression(Expression::Row {
                distribution: child_dist,
                ..
            }) = self.get_node(relational_op.output())?
            {
                match child_dist {
                    None => return Err(QueryPlannerError::UninitializedDistribution),
                    Some(Distribution::Random) => return Ok(Distribution::Random),
                    Some(Distribution::Replicated) => return Ok(Distribution::Replicated),
                    Some(Distribution::Coordinator) => return Ok(Distribution::Coordinator),
                    Some(Distribution::Segment { key }) => {
                        let mut new_key: Vec<usize> = Vec::new();
                        let all_found = key.iter().all(|pos| {
                            child_pos_map
                                .get(&(child_rel_node, *pos))
                                .map_or(false, |v| {
                                    new_key.push(*v);
                                    true
                                })
                        });
                        if all_found {
                            return Ok(Distribution::Segment { key: new_key });
                        }
                        return Ok(Distribution::Random);
                    }
                }
            }
        }
        Err(QueryPlannerError::InvalidRow)
    }

    fn set_scan_dist(
        &mut self,
        table_set: &HashSet<String>,
        table_pos_map: &HashMap<usize, usize>,
        row_node: usize,
    ) -> Result<(), QueryPlannerError> {
        if table_set.len() != 1 {
            return Err(QueryPlannerError::InvalidNode);
        }
        if let Some(relations) = &self.relations {
            let table_name: &str = table_set
                .iter()
                .next()
                .ok_or(QueryPlannerError::InvalidNode)?;
            let table: &Table = relations
                .get(table_name)
                .ok_or(QueryPlannerError::InvalidRelation)?;
            match table {
                Table::Segment { key, .. } | Table::VirtualSegment { key, .. } => {
                    let mut new_key: Vec<usize> = Vec::new();
                    let all_found = key.iter().all(|pos| {
                        table_pos_map.get(pos).map_or(false, |v| {
                            new_key.push(*v);
                            true
                        })
                    });
                    if all_found {
                        if let Node::Expression(Expression::Row {
                            ref mut distribution,
                            ..
                        }) = self
                            .nodes
                            .get_mut(row_node)
                            .ok_or(QueryPlannerError::InvalidRow)?
                        {
                            *distribution = Some(Distribution::Segment { key: new_key });
                        }
                    }
                }
                Table::Virtual { .. } => {
                    if let Node::Expression(Expression::Row {
                        ref mut distribution,
                        ..
                    }) = self
                        .nodes
                        .get_mut(row_node)
                        .ok_or(QueryPlannerError::InvalidRow)?
                    {
                        *distribution = Some(Distribution::Random);
                    }
                }
            }
            Ok(())
        } else {
            Err(QueryPlannerError::InvalidPlan)
        }
    }

    fn set_two_children_node_dist(
        &mut self,
        child_set: &HashSet<usize>,
        child_pos_map: &HashMap<(usize, usize), usize>,
        parent_node: &Option<usize>,
        row_node: usize,
    ) -> Result<(), QueryPlannerError> {
        let mut child_set_iter = child_set.iter();
        let left_child = *child_set_iter
            .next()
            .ok_or(QueryPlannerError::InvalidNode)?;
        let right_child = *child_set_iter
            .next()
            .ok_or(QueryPlannerError::InvalidNode)?;

        let is_union_all: bool = matches!(
            self.get_node(parent_node.ok_or(QueryPlannerError::InvalidNode)?)?,
            Node::Relational(Relational::UnionAll { .. })
        );

        if is_union_all {
            let left_dist = self.dist_from_child(left_child, child_pos_map)?;
            let right_dist = self.dist_from_child(right_child, child_pos_map)?;
            if let Node::Expression(Expression::Row {
                ref mut distribution,
                ..
            }) = self
                .nodes
                .get_mut(row_node)
                .ok_or(QueryPlannerError::InvalidRow)?
            {
                *distribution = Some(Distribution::new_union(left_dist, right_dist)?);
            }
        } else {
            // TODO: implement join
            return Err(QueryPlannerError::InvalidNode);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
