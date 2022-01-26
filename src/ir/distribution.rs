use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::collection;
use crate::errors::QueryPlannerError;

use super::expression::Expression;
use super::operator::Relational;
use super::relation::Table;
use super::{Node, Plan};

/// Tuple columns, that determinate its segment distribution.
///
/// If table T1 is segmented by columns (a, b) and produces
/// tuples with columns (a, b, c), it means that for any T1 tuple
/// on a segment S1: f(a, b) = S1 and (a, b) is a segmentation key.
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct Key {
    /// A list of column positions in the tuple that form a
    /// segmentation key.
    pub positions: Vec<usize>,
}

impl Key {
    #[must_use]
    pub fn new(positions: Vec<usize>) -> Self {
        Key { positions }
    }
}

/// Tuple distribution in the cluster.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Distribution {
    /// A tuple can be located on any data node
    Any,
    /// Tuple distribution is set by the distribution key.
    /// Example: tuples from the segmented table.
    Segment {
        /// A set of distribution keys (we can have multiple keys after join)
        keys: HashSet<Key>,
    },
}

impl Distribution {
    /// Calculate a new distribution for the `UnionAll` output tuple.
    fn union(left: &Distribution, right: &Distribution) -> Distribution {
        match left {
            Distribution::Any => Distribution::Any,
            Distribution::Segment {
                keys: ref keys_left,
                ..
            } => match right {
                Distribution::Any => Distribution::Any,
                Distribution::Segment {
                    keys: ref keys_right,
                    ..
                } => {
                    let mut keys: HashSet<Key> = HashSet::new();
                    for key in keys_left.intersection(keys_right) {
                        keys.insert(Key::new(key.positions.clone()));
                    }
                    if keys.is_empty() {
                        Distribution::Any
                    } else {
                        Distribution::Segment { keys }
                    }
                }
            },
        }
    }

    /// Calculate a new distribution for the tuple combined from the two different tuples.
    fn join(left: &Distribution, right: Distribution) -> Distribution {
        match left {
            Distribution::Any => right,
            Distribution::Segment {
                keys: ref keys_left,
                ..
            } => match right {
                Distribution::Any => Distribution::Any,
                Distribution::Segment {
                    keys: ref keys_right,
                    ..
                } => {
                    let mut keys: HashSet<Key> = HashSet::new();
                    for key in keys_left.union(keys_right) {
                        keys.insert(Key::new(key.positions.clone()));
                    }
                    if keys.is_empty() {
                        Distribution::Any
                    } else {
                        Distribution::Segment { keys }
                    }
                }
            },
        }
    }
}

impl Plan {
    /// Calculate and set tuple distribution.
    ///
    /// As the references in the `Row` expression contain only logical ID of the parent relational nodes,
    /// we need at first traverse all the plan nodes and build a "logical id - array position" map with
    /// `relational_id_map()` function and pass its reference to this function.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when current expression is not a `Row` or contains broken references.
    pub fn set_distribution(&mut self, row_node: usize) -> Result<(), QueryPlannerError> {
        let mut child_set: HashSet<usize> = HashSet::new();
        let mut child_pos_map: HashMap<(usize, usize), usize> = HashMap::new();
        let mut table_set: HashSet<String> = HashSet::new();
        let mut table_pos_map: HashMap<usize, usize> = HashMap::default();
        let mut parent_node: Option<usize> = None;

        let mut populate_maps = |pos: usize, node_id: usize| -> Result<(), QueryPlannerError> {
            if let Node::Expression(Expression::Reference {
                targets,
                position,
                parent,
                ..
            }) = self.get_node(node_id)?
            {
                // Get a relational node, containing current row.
                parent_node = Some(*id_map.get(parent).ok_or(QueryPlannerError::InvalidNode)?);
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
            Ok(())
        };

        if let Node::Expression(Expression::Row { list, .. }) = self.get_node(row_node)? {
            // Gather information about children nodes, that are pointed by the row references.
            for (pos, node) in list.iter().enumerate() {
                if let Node::Expression(Expression::Alias { child, .. }) = self.get_node(*node)? {
                    populate_maps(pos, *child)?;
                } else {
                    populate_maps(pos, *node)?;
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
                    .arena
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
                    Some(Distribution::Any) => return Ok(Distribution::Any),
                    Some(Distribution::Segment { keys }) => {
                        let mut new_keys: HashSet<Key> = HashSet::new();
                        for key in keys {
                            let mut new_key: Key = Key::new(Vec::new());
                            let all_found = key.positions.iter().all(|pos| {
                                child_pos_map
                                    .get(&(child_rel_node, *pos))
                                    .map_or(false, |v| {
                                        new_key.positions.push(*v);
                                        true
                                    })
                            });

                            if all_found {
                                new_keys.insert(new_key);
                            }
                        }

                        if new_keys.is_empty() {
                            return Ok(Distribution::Any);
                        }
                        return Ok(Distribution::Segment { keys: new_keys });
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
                    let mut new_key: Key = Key::new(Vec::new());
                    let all_found = key.positions.iter().all(|pos| {
                        table_pos_map.get(pos).map_or(false, |v| {
                            new_key.positions.push(*v);
                            true
                        })
                    });
                    if all_found {
                        if let Node::Expression(Expression::Row {
                            ref mut distribution,
                            ..
                        }) = self
                            .nodes
                            .arena
                            .get_mut(row_node)
                            .ok_or(QueryPlannerError::InvalidRow)?
                        {
                            *distribution = Some(Distribution::Segment {
                                keys: collection! { new_key },
                            });
                        }
                    }
                }
                Table::Virtual { .. } => {
                    if let Node::Expression(Expression::Row {
                        ref mut distribution,
                        ..
                    }) = self
                        .nodes
                        .arena
                        .get_mut(row_node)
                        .ok_or(QueryPlannerError::InvalidRow)?
                    {
                        *distribution = Some(Distribution::Any);
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

        let is_join: bool = matches!(
            self.get_node(parent_node.ok_or(QueryPlannerError::InvalidNode)?)?,
            Node::Relational(Relational::InnerJoin { .. })
        );

        let left_dist = self.dist_from_child(left_child, child_pos_map)?;
        let right_dist = self.dist_from_child(right_child, child_pos_map)?;

        if is_union_all {
            if let Node::Expression(Expression::Row {
                ref mut distribution,
                ..
            }) = self
                .nodes
                .arena
                .get_mut(row_node)
                .ok_or(QueryPlannerError::InvalidRow)?
            {
                *distribution = Some(Distribution::union(&left_dist, &right_dist));
            }
        } else if is_join {
            if let Node::Expression(Expression::Row {
                ref mut distribution,
                ..
            }) = self
                .nodes
                .arena
                .get_mut(row_node)
                .ok_or(QueryPlannerError::InvalidRow)?
            {
                *distribution = Some(Distribution::join(&left_dist, right_dist));
            }
        } else {
            return Err(QueryPlannerError::InvalidNode);
        }
        Ok(())
    }

    /// Get distribution of the row node.
    ///
    /// # Errors
    /// - node is invalid
    /// - node is not a row
    pub fn get_distribution(&self, row_id: usize) -> Result<&Distribution, QueryPlannerError> {
        if let Node::Expression(expr) = self.get_node(row_id)? {
            return expr.distribution();
        }
        Err(QueryPlannerError::InvalidNode)
    }
}

#[cfg(test)]
mod tests;
