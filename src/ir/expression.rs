//! Expressions are the building blocks of the tuple.
//!
//! They provide us information about:
//! - what input tuple's columns where used to build our tuple
//! - the order of the columns (and we can get their types as well)
//! - distribution of the data in the tuple

use super::operator;
use super::operator::Relational;
use super::relation::Table;
use super::value::Value;
use super::{Node, Plan};
use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Tuple data chunk distribution policy in the cluster.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Distribution {
    /// Only coordinator contains all the data.
    ///
    /// Example: insertion to the virtual table on coordinator.
    Coordinator,
    /// Each segment contains a random portion of data.
    ///
    /// Example: "erased" distribution key by projection operator.
    Random,
    /// Each segment contains a full copy of data.
    ///
    /// Example: constant expressions.
    Replicated,
    /// Each segment contains a portion of data,
    /// determined by the distribution key rule.
    ///
    /// Example: segmented table.
    Segment {
        /// A list of column positions in the output tuple,
        /// that for a distribution key.
        key: Vec<usize>,
    },
}

impl Distribution {
    /// Calculate a new distribution for the `UnionAll` output tuple.
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - distribution conflict that should be resolved by adding a `Motion` node
    pub fn new_union(
        left: Distribution,
        right: Distribution,
    ) -> Result<Distribution, QueryPlannerError> {
        match left {
            Distribution::Coordinator => match right {
                Distribution::Coordinator => Ok(left),
                _ => Err(QueryPlannerError::RequireMotion),
            },
            Distribution::Random => match right {
                Distribution::Coordinator => Err(QueryPlannerError::RequireMotion),
                _ => Ok(left),
            },
            Distribution::Replicated => match right {
                Distribution::Coordinator => Err(QueryPlannerError::RequireMotion),
                _ => Ok(right),
            },
            Distribution::Segment {
                key: ref left_key, ..
            } => match right {
                Distribution::Random => Ok(Distribution::Random),
                Distribution::Replicated => Ok(left),
                Distribution::Segment {
                    key: ref right_key, ..
                } => {
                    if left_key.iter().zip(right_key.iter()).all(|(l, r)| l == r) {
                        Ok(left)
                    } else {
                        Ok(Distribution::Random)
                    }
                }
                Distribution::Coordinator => Err(QueryPlannerError::RequireMotion),
            },
        }
    }
}

/// Tuple tree build blocks.
///
/// Tuple describes a single portion of data moved among the cluster.
/// It consists of the ordered, strictly typed expressions with names
/// (columns) and additional information about data distribution policy.
///
/// Tuple is a tree with a `Row` top (level 0) and a list of the named
/// `Alias` columns (level 1). This convention is used among the code
/// and should not be changed. Thanks to this fact we always know the
/// name of any column in the tuple that should simplify AST
/// deserialization.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Expression {
    /// Expression name.
    ///
    /// Example: `42 as a`.
    Alias {
        /// Alias name.
        name: String,
        /// Child expression node index in the plan node arena.
        child: usize,
    },
    /// Binary expression returning boolean result.
    ///
    /// Example: `a > 42`, `b in (select c from ...)`.
    Bool {
        /// Left branch expression node index in the plan node arena.
        left: usize,
        /// Boolean operator.
        op: operator::Bool,
        /// Right branch expression node index in the plan node arena.
        right: usize,
    },
    /// Constant expressions.
    ///
    // Example: `42`.
    Constant {
        /// Contained value (boolean, number, string or null)
        value: Value,
    },
    /// Reference to the position in the incoming tuple(s).
    /// Uses a relative pointer as a coordinate system:
    /// - relational node (containing this reference)
    /// - target(s) in the relational nodes list of children
    /// - column position in the child(ren) output tuple
    Reference {
        /// Targets in the relational node children list.
        /// - Leaf nodes (relation scans): None.
        /// - Union nodes: two elements (left and right).
        /// - Other: single element.
        targets: Option<Vec<usize>>,
        /// Expression position in the input tuple.
        position: usize,
        /// Relational node ID, that contains current reference
        parent: usize,
    },
    /// Top of the tuple tree with a list of aliases.
    ///
    ///  Example: (a, b, 1).
    Row {
        /// A list of the alias expression node indexes in the plan node arena.
        list: Vec<usize>,
        /// Resulting data distribution of the tuple. Should be filled as a part
        /// of the last "add Motion" transformation.
        distribution: Option<Distribution>,
    },
}

fn dist_suggested_by_child(
    child: usize,
    plan: &Plan,
    child_pos_map: &HashMap<(usize, usize), usize>,
) -> Result<Distribution, QueryPlannerError> {
    if let Node::Relational(relational_op) = plan.get_node(child)? {
        if let Node::Expression(Expression::Row {
            distribution: child_dist,
            ..
        }) = plan.get_node(relational_op.output())?
        {
            match child_dist {
                None => return Err(QueryPlannerError::UninitializedDistribution),
                Some(Distribution::Random) => return Ok(Distribution::Random),
                Some(Distribution::Replicated) => return Ok(Distribution::Replicated),
                Some(Distribution::Coordinator) => return Ok(Distribution::Coordinator),
                Some(Distribution::Segment { key }) => {
                    let mut new_key: Vec<usize> = Vec::new();
                    let all_found = key.iter().all(|pos| {
                        child_pos_map.get(&(child, *pos)).map_or(false, |v| {
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

fn set_scan_tuple_distribution(
    plan: &mut Plan,
    table_set: &HashSet<String>,
    table_pos_map: &HashMap<usize, usize>,
    row_node: usize,
) -> Result<(), QueryPlannerError> {
    if table_set.len() != 1 {
        return Err(QueryPlannerError::InvalidNode);
    }
    if let Some(relations) = &plan.relations {
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
                    }) = plan
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
                }) = plan
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

fn set_double_children_node_tuple_distribution(
    plan: &mut Plan,
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
        plan.get_node(parent_node.ok_or(QueryPlannerError::InvalidNode)?)?,
        Node::Relational(Relational::UnionAll { .. })
    );

    if is_union_all {
        let left_dist = dist_suggested_by_child(left_child, plan, child_pos_map)?;
        let right_dist = dist_suggested_by_child(right_child, plan, child_pos_map)?;
        if let Node::Expression(Expression::Row {
            ref mut distribution,
            ..
        }) = plan
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

/// Calculate and set tuple distribution.
///
/// As the references in the `Row` expression contain only logical ID of the parent relational nodes,
/// we need at first traverse all the plan nodes and build a "logical id - array position" map with
/// `relational_id_map()` function and pass its reference to this function.
///
/// # Errors
/// Returns `QueryPlannerError` when current expression is not a `Row` or contains broken references.
pub fn set_distribution<S: ::std::hash::BuildHasher + Default>(
    row_node: usize,
    id_map: &HashMap<usize, usize, S>,
    plan: &mut Plan,
) -> Result<(), QueryPlannerError> {
    let mut child_set: HashSet<usize> = HashSet::new();
    let mut child_pos_map: HashMap<(usize, usize), usize> = HashMap::new();
    let mut table_set: HashSet<String> = HashSet::new();
    let mut table_pos_map: HashMap<usize, usize> = HashMap::default();
    let mut parent_node: Option<usize> = None;

    if let Node::Expression(Expression::Row { list, .. }) = plan.get_node(row_node)? {
        // Gather information about children nodes, that are pointed by the row references.
        for (pos, alias_node) in list.iter().enumerate() {
            if let Node::Expression(Expression::Alias { child, .. }) = plan.get_node(*alias_node)? {
                if let Node::Expression(Expression::Reference {
                    targets,
                    position,
                    parent,
                    ..
                }) = plan.get_node(*child)?
                {
                    // Get the relational node, containing this row
                    parent_node = Some(*id_map.get(parent).ok_or(QueryPlannerError::InvalidNode)?);
                    if let Node::Relational(relational_op) =
                        plan.get_node(parent_node.ok_or(QueryPlannerError::InvalidNode)?)?
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
            set_scan_tuple_distribution(plan, &table_set, &table_pos_map, row_node)?;
        }
        1 => {
            // Single child
            let child: usize = *child_set
                .iter()
                .next()
                .ok_or(QueryPlannerError::InvalidNode)?;
            let suggested_dist = dist_suggested_by_child(child, plan, &child_pos_map)?;
            if let Node::Expression(Expression::Row {
                ref mut distribution,
                ..
            }) = plan
                .nodes
                .get_mut(row_node)
                .ok_or(QueryPlannerError::InvalidRow)?
            {
                *distribution = Some(suggested_dist);
            }
        }
        2 => {
            // Union, join
            set_double_children_node_tuple_distribution(
                plan,
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

#[allow(dead_code)]
impl Expression {
    /// Get current row distribution.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the function is called on expression
    /// other than `Row` or a node doesn't know its distribution yet.
    pub fn distribution(&self) -> Result<&Distribution, QueryPlannerError> {
        if let Expression::Row { distribution, .. } = self {
            let dist = match distribution {
                Some(d) => d,
                None => return Err(QueryPlannerError::UninitializedDistribution),
            };
            return Ok(dist);
        }
        Err(QueryPlannerError::InvalidRow)
    }

    /// Alias expression constructor.
    #[must_use]
    pub fn new_alias(name: &str, child: usize) -> Self {
        Expression::Alias {
            name: String::from(name),
            child,
        }
    }

    /// Constant expression constructor.
    #[must_use]
    pub fn new_const(value: Value) -> Self {
        Expression::Constant { value }
    }

    /// Reference expression constructor.
    #[must_use]
    pub fn new_ref(parent: usize, targets: Option<Vec<usize>>, position: usize) -> Self {
        Expression::Reference {
            parent,
            targets,
            position,
        }
    }

    // TODO: check that doesn't contain top-level aliases with the same names
    /// Row expression constructor.
    #[must_use]
    pub fn new_row(list: Vec<usize>, distribution: Option<Distribution>) -> Self {
        Expression::Row { list, distribution }
    }

    /// Boolean expression constructor.
    #[must_use]
    pub fn new_bool(left: usize, op: operator::Bool, right: usize) -> Self {
        Expression::Bool { left, op, right }
    }
}

#[cfg(test)]
mod tests;
