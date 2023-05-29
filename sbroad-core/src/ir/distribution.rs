//! Tuple distribution module.

use ahash::{AHashMap, RandomState};
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::collection;
use crate::errors::{Action, Entity, SbroadError};
use crate::ir::helpers::RepeatableState;
use crate::ir::transformation::redistribution::{MotionKey, Target};

use super::expression::Expression;
use super::operator::Relational;
use super::relation::Table;
use super::{Node, Plan};

/// Tuple columns that determinate its segment distribution.
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

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct KeySet(HashSet<Key, RepeatableState>);

impl TryFrom<&MotionKey> for KeySet {
    type Error = SbroadError;

    fn try_from(value: &MotionKey) -> Result<Self, Self::Error> {
        let mut positions: Vec<usize> = Vec::with_capacity(value.targets.len());
        for t in &value.targets {
            match t {
                Target::Reference(pos) => positions.push(*pos),
                Target::Value(v) => {
                    return Err(SbroadError::FailedTo(
                        Action::Create,
                        Some(Entity::DistributionKey),
                        format!("found value target in motion key: {v}"),
                    ))
                }
            }
        }
        let keys: HashSet<_, RepeatableState> = collection! { Key::new(positions) };
        Ok(keys.into())
    }
}

impl KeySet {
    pub fn iter(&self) -> impl Iterator<Item = &Key> {
        self.0.iter()
    }

    #[must_use]
    pub fn intersection(&self, other: &Self) -> Self {
        KeySet(self.0.intersection(&other.0).cloned().collect())
    }

    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        KeySet(self.0.union(&other.0).cloned().collect())
    }
}

impl From<HashSet<Key, RepeatableState>> for KeySet {
    fn from(keys: HashSet<Key, RepeatableState>) -> Self {
        Self(keys)
    }
}

/// Tuple distribution in the cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Distribution {
    /// A tuple can be located on any data node.
    /// Example: projection removes the segment key columns.
    Any,
    /// A tuple is located on all data nodes and on coordinator
    /// (constants).
    Replicated,
    /// Tuple distribution is set by the distribution key.
    /// Example: tuples from the segmented table.
    Segment {
        /// A set of distribution keys (we can have multiple keys after join)
        keys: KeySet,
    },
    /// A tuple is located exactly only on one node
    Single,
}

impl Distribution {
    /// Calculate a new distribution for the `Except` and `UnionAll` output tuple.
    /// Single
    fn union_except(
        left: &Distribution,
        right: &Distribution,
    ) -> Result<Distribution, SbroadError> {
        let dist = match (left, right) {
            (Distribution::Single, _) | (_, Distribution::Single) => {
                 return Err(SbroadError::Invalid(
                     Entity::Distribution,
                     Some(format!("union/except child has unexpected distribution Single. Left: {left:?}, right: {right:?}"))))   
            }
            (Distribution::Any, _) | (_, Distribution::Any) => Distribution::Any,
            (Distribution::Replicated, _) | (_, Distribution::Replicated) => {
                Distribution::Replicated
            }
            (
                Distribution::Segment {
                    keys: keys_left, ..
                },
                Distribution::Segment {
                    keys: keys_right, ..
                },
            ) => {
                let mut keys: HashSet<Key, RepeatableState> = HashSet::with_hasher(RepeatableState);
                for key in keys_left.intersection(keys_right).iter() {
                    keys.insert(Key::new(key.positions.clone()));
                }
                if keys.is_empty() {
                    Distribution::Any
                } else {
                    Distribution::Segment { keys: keys.into() }
                }
            }
        };
        Ok(dist)
    }

    /// Calculate a new distribution for the tuple combined from two different tuples.
    fn join(left: &Distribution, right: &Distribution) -> Result<Distribution, SbroadError> {
        let dist = match (left, right) {
            (Distribution::Single, _) | (_, Distribution::Single) => {
                return Err(SbroadError::Invalid(
                    Entity::Distribution,
                    Some(format!("join child has unexpected distribution Single. Left: {left:?}, right: {right:?}"))))
            }
            (Distribution::Any, Distribution::Any | Distribution::Replicated)
            | (Distribution::Replicated, Distribution::Any) => Distribution::Any,
            (Distribution::Replicated, Distribution::Replicated) => Distribution::Replicated,
            (Distribution::Any | Distribution::Replicated, Distribution::Segment { .. }) => {
                right.clone()
            }
            (Distribution::Segment { .. }, Distribution::Any | Distribution::Replicated) => {
                left.clone()
            }
            (
                Distribution::Segment {
                    keys: ref keys_left,
                    ..
                },
                Distribution::Segment {
                    keys: ref keys_right,
                    ..
                },
            ) => {
                let mut keys: HashSet<Key, RepeatableState> = HashSet::with_hasher(RepeatableState);
                for key in keys_left.union(keys_right).iter() {
                    keys.insert(Key::new(key.positions.clone()));
                }
                if keys.is_empty() {
                    Distribution::Any
                } else {
                    Distribution::Segment { keys: keys.into() }
                }
            }
        };
        Ok(dist)
    }

    #[must_use]
    pub fn is_unknown(&self) -> bool {
        if let Distribution::Any = self {
            return true;
        }

        false
    }
}

enum ReferredNodes {
    None,
    Single(usize),
    Pair(usize, usize),
    Multiple(Vec<usize>),
}

impl ReferredNodes {
    fn new() -> Self {
        ReferredNodes::None
    }

    fn append(&mut self, node: usize) {
        match self {
            ReferredNodes::None => *self = ReferredNodes::Single(node),
            ReferredNodes::Single(n) => {
                if *n != node {
                    *self = ReferredNodes::Pair(*n, node);
                }
            }
            ReferredNodes::Pair(n1, n2) => {
                if *n1 != node && *n2 != node {
                    *self = ReferredNodes::Multiple(vec![*n1, *n2, node]);
                }
            }
            ReferredNodes::Multiple(ref mut nodes) => {
                if !nodes.contains(&node) {
                    nodes.push(node);
                }
            }
        }
    }

    fn reserve(&mut self, capacity: usize) {
        if let ReferredNodes::Multiple(ref mut nodes) = self {
            nodes.reserve(capacity);
        }
    }
}

impl Iterator for ReferredNodes {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ReferredNodes::None => None,
            ReferredNodes::Single(n) => {
                let node = *n;
                *self = ReferredNodes::None;
                Some(node)
            }
            ReferredNodes::Pair(n1, n2) => {
                let node = *n1;
                *self = ReferredNodes::Single(*n2);
                Some(node)
            }
            ReferredNodes::Multiple(ref mut nodes) => {
                let node = nodes.pop();
                if nodes.is_empty() {
                    *self = ReferredNodes::None;
                }
                node
            }
        }
    }
}

/// Helper structure to get the column position
/// in the child node.
#[derive(Debug, Eq, Hash, PartialEq)]
struct ChildColumnReference {
    /// Child node id.
    node_id: usize,
    /// Column position in the child node.
    column_position: usize,
}

impl From<(usize, usize)> for ChildColumnReference {
    fn from((node_id, column_position): (usize, usize)) -> Self {
        ChildColumnReference {
            node_id,
            column_position,
        }
    }
}

type ParentColumnPosition = usize;

impl Plan {
    /// Calculate and set tuple distribution.
    ///
    /// # Errors
    /// Returns `SbroadError` when current expression is not a `Row` or contains broken references.
    #[allow(clippy::too_many_lines)]
    pub fn set_distribution(&mut self, row_id: usize) -> Result<(), SbroadError> {
        let row_children = self.get_expression_node(row_id)?.get_row_list()?;

        // There are two kinds of rows: constructed from aliases (projections)
        // and constructed from any other expressions (selection filters, join conditions, etc).
        // This closure returns the proper child id for the row.
        let row_child_id = |col_id: usize| -> Result<usize, SbroadError> {
            match self.get_expression_node(col_id) {
                Ok(Expression::Alias { child, .. }) => Ok(*child),
                Ok(_) => Ok(col_id),
                Err(e) => Err(e),
            }
        };

        let mut parent_node = None;
        let mut contains_expr = false;
        for id in row_children.iter() {
            let child_id = row_child_id(*id)?;
            match self.get_expression_node(child_id)? {
                Expression::Reference { parent, .. } => {
                    parent_node = *parent;
                    break;
                }
                Expression::Constant { .. } => {}
                _ => {
                    contains_expr = true;
                    break;
                }
            }
        }

        // if node's output has non-const expression, we can't
        // make any assumptions about its distribution.
        // e.g select a + b from t
        // Here Projection must have Distribution::Any
        if contains_expr {
            self.set_dist(row_id, Distribution::Any)?;
            return Ok(());
        }

        let parent_id: usize = if let Some(parent_id) = parent_node {
            parent_id
        } else {
            // There are no references in the row, so it is a row of constants
            // with replicated distribution.
            self.set_const_dist(row_id)?;
            return Ok(());
        };
        let parent = self.get_relation_node(parent_id)?;

        // References in the branch node.
        if let Some(parent_children) = parent.children() {
            // Set of the referred relational nodes in the row.
            let mut ref_nodes = ReferredNodes::new();
            // Child node columns referred by the parent node columns in its output tuple.
            // We'll need it later to deal with the `Segment` distribution.
            let mut ref_map: AHashMap<ChildColumnReference, ParentColumnPosition> = AHashMap::new();
            for (parent_column_pos, id) in row_children.iter().enumerate() {
                let child_id = row_child_id(*id)?;
                if let Expression::Reference {
                    targets, position, ..
                } = self.get_expression_node(child_id)?
                {
                    // As the row is located in the branch relational node, the targets should be non-empty.
                    let targets = targets.as_ref().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Reference targets are empty".to_string(),
                        )
                    })?;
                    ref_map.reserve(targets.len());
                    ref_nodes.reserve(targets.len());
                    for target in targets {
                        let referred_id = *parent_children.get(*target).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Expression,
                                "reference points to invalid column".to_string(),
                            )
                        })?;
                        ref_nodes.append(referred_id);
                        ref_map.insert((referred_id, *position).into(), parent_column_pos);
                    }
                }
            }
            // The parent node is VALUES.
            if let Relational::Values { .. } = parent {
                let mut dist = Distribution::Replicated;
                for child_id in ref_nodes {
                    let right_dist = self.dist_from_child(child_id, &ref_map)?;
                    dist = Distribution::union_except(&dist, &right_dist)?;
                }
                let output = self.get_mut_expression_node(row_id)?;
                if let Expression::Row {
                    ref mut distribution,
                    ..
                } = output
                {
                    if distribution.is_none() {
                        *distribution = Some(dist);
                    }
                }
                return Ok(());
            }

            match ref_nodes {
                ReferredNodes::None => {
                    // We should never get here as we have already handled the case
                    // when there are no references in the row (a row of constants).
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("the row contains no references".to_string()),
                    ));
                }
                ReferredNodes::Single(child_id) => {
                    let suggested_dist = self.dist_from_child(child_id, &ref_map)?;
                    let output = self.get_mut_expression_node(row_id)?;
                    if let Expression::Row {
                        ref mut distribution,
                        ..
                    } = output
                    {
                        if distribution.is_none() {
                            *distribution = Some(suggested_dist);
                        }
                    }
                }
                ReferredNodes::Pair(n1, n2) => {
                    // Union, join
                    self.set_two_children_node_dist(&ref_map, n1, n2, parent_id, row_id)?;
                }
                ReferredNodes::Multiple(_) => {
                    return Err(SbroadError::DuplicatedValue(
                        "Row contains multiple references to the same node (and in is not VALUES)"
                            .to_string(),
                    ));
                }
            }
        } else {
            // References in the leaf (relation scan) node.
            let mut table_map: HashMap<usize, usize, RandomState> =
                HashMap::with_capacity_and_hasher(row_children.len(), RandomState::new());
            for (pos, id) in row_children.iter().enumerate() {
                let child_id = row_child_id(*id)?;
                if let Expression::Reference {
                    targets, position, ..
                } = self.get_expression_node(child_id)?
                {
                    if targets.is_some() {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some("References to the children targets in the leaf (relation scan) node are not supported".to_string()),
                        ));
                    }
                    table_map.insert(*position, pos);
                }
            }
            // We don't handle a case with the ValueRow (distribution would be set to Replicated in Value node).
            // So, we should care only about relation scan nodes.
            let table_name: String = if let Relational::ScanRelation { relation, .. } = parent {
                relation.clone()
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some("the parent node is not a relation scan node".to_string()),
                ));
            };
            self.set_scan_dist(&table_name, &table_map, row_id)?;
        }
        Ok(())
    }

    // Private methods

    fn dist_from_child(
        &self,
        child_rel_node: usize,
        child_pos_map: &AHashMap<ChildColumnReference, ParentColumnPosition>,
    ) -> Result<Distribution, SbroadError> {
        if let Node::Relational(relational_op) = self.get_node(child_rel_node)? {
            let node = self.get_node(relational_op.output())?;
            if let Node::Expression(Expression::Row {
                distribution: child_dist,
                ..
            }) = node
            {
                match child_dist {
                    None => {
                        return Err(SbroadError::Invalid(
                            Entity::Distribution,
                            Some("distribution is uninitialized".to_string()),
                        ))
                    }
                    Some(Distribution::Single) => return Ok(Distribution::Single),
                    Some(Distribution::Any) => return Ok(Distribution::Any),
                    Some(Distribution::Replicated) => return Ok(Distribution::Replicated),
                    Some(Distribution::Segment { keys }) => {
                        let mut new_keys: HashSet<Key, RepeatableState> =
                            HashSet::with_hasher(RepeatableState);
                        for key in keys.iter() {
                            let mut new_key: Key = Key::new(Vec::new());
                            let all_found = key.positions.iter().all(|pos| {
                                child_pos_map.get(&(child_rel_node, *pos).into()).map_or(
                                    false,
                                    |v| {
                                        new_key.positions.push(*v);
                                        true
                                    },
                                )
                            });

                            if all_found {
                                new_keys.insert(new_key);
                            }
                        }

                        if new_keys.is_empty() {
                            return Ok(Distribution::Any);
                        }
                        return Ok(Distribution::Segment {
                            keys: new_keys.into(),
                        });
                    }
                }
            }
        }
        Err(SbroadError::Invalid(Entity::Relational, None))
    }

    /// Sets row distribution to replicated.
    ///
    /// # Errors
    /// - Node is not of a row type.
    pub fn set_const_dist(&mut self, row_id: usize) -> Result<(), SbroadError> {
        self.set_dist(row_id, Distribution::Replicated)
    }

    /// Sets the `Distribution` of row to given one
    ///
    /// # Errors
    /// - supplied node is `Row`
    pub fn set_dist(&mut self, row_id: usize, dist: Distribution) -> Result<(), SbroadError> {
        if let Expression::Row {
            ref mut distribution,
            ..
        } = self.get_mut_expression_node(row_id)?
        {
            if distribution.is_none() {
                *distribution = Some(dist);
            }
            return Ok(());
        }
        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("the node is not a row type".to_string()),
        ))
    }

    fn set_scan_dist(
        &mut self,
        table_name: &str,
        table_pos_map: &HashMap<usize, usize, RandomState>,
        row_id: usize,
    ) -> Result<(), SbroadError> {
        let table: &Table = self.relations.get(table_name).ok_or_else(|| {
            SbroadError::NotFound(Entity::Table, format!("{table_name} among plan relations"))
        })?;
        let mut new_key: Key = Key::new(Vec::new());
        let all_found = table.key.positions.iter().all(|pos| {
            table_pos_map.get(pos).map_or(false, |v| {
                new_key.positions.push(*v);
                true
            })
        });
        if all_found {
            if let Expression::Row {
                ref mut distribution,
                ..
            } = self.get_mut_expression_node(row_id)?
            {
                let keys: HashSet<Key, RepeatableState> = collection! { new_key };
                *distribution = Some(Distribution::Segment { keys: keys.into() });
            }
        }
        Ok(())
    }

    fn set_two_children_node_dist(
        &mut self,
        child_pos_map: &AHashMap<ChildColumnReference, ParentColumnPosition>,
        left_id: usize,
        right_id: usize,
        parent_id: usize,
        row_id: usize,
    ) -> Result<(), SbroadError> {
        let left_dist = self.dist_from_child(left_id, child_pos_map)?;
        let right_dist = self.dist_from_child(right_id, child_pos_map)?;

        let parent = self.get_relation_node(parent_id)?;
        let new_dist = match parent {
            Relational::Except { .. } | Relational::UnionAll { .. } => {
                Distribution::union_except(&left_dist, &right_dist)?
            }
            Relational::Join { .. } => Distribution::join(&left_dist, &right_dist)?,
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Relational,
                    Some("expected Except, UnionAll or InnerJoin".to_string()),
                ))
            }
        };
        let expr = self.get_mut_expression_node(row_id)?;
        if let Expression::Row {
            ref mut distribution,
            ..
        } = expr
        {
            *distribution = Some(new_dist);
        } else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("expected Row".to_string()),
            ));
        };

        Ok(())
    }

    /// Gets distribution of the output row.
    ///
    /// # Errors
    /// - Node is not of a row type.
    pub fn get_distribution(&self, row_id: usize) -> Result<&Distribution, SbroadError> {
        match self.get_node(row_id)? {
            Node::Expression(expr) => expr.distribution(),
            Node::Relational(_) => Err(SbroadError::Invalid(
                Entity::Distribution,
                Some(
                    "Failed to get distribution for a relational node (try its row output tuple)."
                        .to_string(),
                ),
            )),
            Node::Parameter => Err(SbroadError::Invalid(
                Entity::Distribution,
                Some("Failed to get distribution for a parameter node.".to_string()),
            )),
        }
    }

    /// Gets distribution of the row node or initializes it if not set.
    ///
    /// # Errors
    /// - node is invalid
    /// - node is not a row
    /// - row contains broken references
    pub fn get_or_init_distribution(
        &mut self,
        row_id: usize,
    ) -> Result<&Distribution, SbroadError> {
        if let Err(SbroadError::Invalid(Entity::Distribution, _)) = self.get_distribution(row_id) {
            self.set_distribution(row_id)?;
        }
        self.get_distribution(row_id)
    }
}

#[cfg(test)]
mod tests;
