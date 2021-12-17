//! Intermediate representation.
//!
//! Contains the logical plan tree and helpers.

pub mod distribution;
pub mod expression;
pub mod operator;
pub mod relation;
pub mod value;

use crate::errors::QueryPlannerError;
use distribution::Distribution;
use expression::Expression;
use operator::Relational;
use relation::Table;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
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

    /// Create a new tuple from the children nodes output, containing only
    /// a specified list of column names. If the column list is empty then
    /// just copy all the columns to a new tuple.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - relation node contains invalid `Row` in the output
    /// - targets and children are inconsistent
    /// - column names don't exits
    pub fn add_row(
        &mut self,
        rel_node_id: usize,
        children: &[usize],
        targets: &[usize],
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        // We can pass two target children nodes only in a case
        // of `UnionAll`. Even for a `NaturalJoin` we work with
        // each child independently. In fact, we need only the
        // first child in a `UnionAll` operator to get correct
        // column names for a new tuple (second child aliases
        // would be shadowed). But each reference should point
        // to both children to give us additional information
        // during transformations.
        if (targets.len() > 2) || targets.is_empty() {
            return Err(QueryPlannerError::InvalidRow);
        }

        if let Some(max) = targets.iter().max() {
            if *max >= children.len() {
                return Err(QueryPlannerError::InvalidRow);
            }
        }

        let target_child: usize = if let Some(target) = targets.get(0) {
            *target
        } else {
            return Err(QueryPlannerError::InvalidRow);
        };
        let child_node: usize = if let Some(child) = children.get(target_child) {
            *child
        } else {
            return Err(QueryPlannerError::InvalidRow);
        };

        if col_names.is_empty() {
            let child_row_list: Vec<usize> =
                if let Node::Relational(relational_op) = self.get_node(child_node)? {
                    if let Node::Expression(Expression::Row { list, .. }) =
                        self.get_node(relational_op.output())?
                    {
                        list.clone()
                    } else {
                        return Err(QueryPlannerError::InvalidRow);
                    }
                } else {
                    return Err(QueryPlannerError::InvalidNode);
                };

            let mut aliases: Vec<usize> = Vec::new();

            for (pos, alias_node) in child_row_list.iter().enumerate() {
                let name: String = if let Node::Expression(Expression::Alias { ref name, .. }) =
                    self.get_node(*alias_node)?
                {
                    String::from(name)
                } else {
                    return Err(QueryPlannerError::InvalidRow);
                };
                let new_targets: Vec<usize> = targets.iter().copied().collect();
                // Create new references and aliases. Save them to the plan nodes arena.
                let r_id = vec_alloc(
                    &mut self.nodes,
                    Node::Expression(Expression::new_ref(rel_node_id, Some(new_targets), pos)),
                );
                let a_id = vec_alloc(
                    &mut self.nodes,
                    Node::Expression(Expression::new_alias(&name, r_id)),
                );
                aliases.push(a_id);
            }

            let row_node = vec_alloc(
                &mut self.nodes,
                Node::Expression(Expression::new_row(aliases, None)),
            );
            return Ok(row_node);
        }

        let map = if let Node::Relational(relational_op) = self.get_node(child_node)? {
            relational_op.output_alias_position_map(self)?
        } else {
            return Err(QueryPlannerError::InvalidNode);
        };

        let mut aliases: Vec<usize> = Vec::new();

        let all_found = col_names.iter().all(|col| {
            map.get(*col).map_or(false, |pos| {
                let new_targets: Vec<usize> = targets.iter().copied().collect();
                // Create new references and aliases. Save them to the plan nodes arena.
                let r_id = vec_alloc(
                    &mut self.nodes,
                    Node::Expression(Expression::new_ref(rel_node_id, Some(new_targets), *pos)),
                );
                let a_id = vec_alloc(
                    &mut self.nodes,
                    Node::Expression(Expression::new_alias(col, r_id)),
                );
                aliases.push(a_id);
                true
            })
        });

        if all_found {
            let row_node = vec_alloc(
                &mut self.nodes,
                Node::Expression(Expression::new_row(aliases, None)),
            );
            return Ok(row_node);
        }
        Err(QueryPlannerError::InvalidRow)
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
        let get_next_child = |children: &Vec<usize>| -> Option<&Node> {
            let current_step = *self.step.borrow();
            let child = children.get(current_step);
            child.and_then(|pos| {
                let node = self.plan.nodes.get(*pos);
                *self.step.borrow_mut() += 1;
                node
            })
        };

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
                    children,
                    condition,
                    ..
                } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 || current_step == 1 {
                        return get_next_child(children);
                    } else if current_step == 2 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*condition);
                    }
                    None
                }
                Relational::ScanRelation { .. } => None,
                Relational::ScanSubQuery { children, .. }
                | Relational::Motion { children, .. }
                | Relational::Selection { children, .. }
                | Relational::Projection { children, .. } => get_next_child(children),
                Relational::UnionAll { children, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 || current_step == 1 {
                        return get_next_child(children);
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
