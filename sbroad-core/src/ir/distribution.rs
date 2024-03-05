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
use super::relation::{Column, ColumnPositions};
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

    pub(crate) fn with_columns(
        columns: &[Column],
        pos_map: &ColumnPositions,
        sharding_key: &[&str],
    ) -> Result<Self, SbroadError> {
        let shard_positions = sharding_key
            .iter()
            .map(|name| match pos_map.get(name) {
                Some(pos) => {
                    // Check that the column type is scalar.
                    // Compound types are not supported as sharding keys.
                    let column = &columns.get(pos).ok_or_else(|| {
                        SbroadError::FailedTo(
                            Action::Create,
                            Some(Entity::Column),
                            format!("column {name} not found at position {pos}"),
                        )
                    })?;
                    if !column.r#type.is_scalar() {
                        return Err(SbroadError::Invalid(
                            Entity::Column,
                            Some(format!("column {name} at position {pos} is not scalar",)),
                        ));
                    }
                    Ok(pos)
                }
                None => Err(SbroadError::Invalid(Entity::ShardingKey, None)),
            })
            .collect::<Result<Vec<usize>, _>>()?;
        Ok(Key::new(shard_positions))
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
                    ));
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

/// Tuple distribution (location in cluster) in the cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Distribution {
    /// The output of relational operator with this distribution
    /// can be located on several storages (maybe zero or one).
    /// Example: projection removes the segment key columns.
    Any,
    /// The output of relational operator with this distribution
    /// can be located on several storages (maybe zero or one).
    /// But if the data is present on the node, it is located
    /// as if it is a sharded by any of the keys in the keyset.
    ///
    /// Example: tuples from the segmented table.
    Segment {
        /// A set of distribution keys (we can have multiple keys after join)
        keys: KeySet,
    },
    /// A subtree with relational operator that has this distribution is guaranteed
    /// to be executed on a single node.
    Single,
    /// If subtree which top has `Global` distribution is executed on several nodes,
    /// then on each node output table will be exactly the same table.
    ///
    /// Example: scan of global tables, motion with policy full.
    Global,
}

impl Distribution {
    fn union(left: &Distribution, right: &Distribution) -> Result<Distribution, SbroadError> {
        let dist = match (left, right) {
            (
                Distribution::Global | Distribution::Any,
                Distribution::Any | Distribution::Segment { .. },
            )
            | (
                Distribution::Any | Distribution::Segment { .. },
                Distribution::Global | Distribution::Any,
            ) => Distribution::Any,
            (Distribution::Global, Distribution::Global) => Distribution::Global,
            (Distribution::Single, _) | (_, Distribution::Single) => {
                return Err(SbroadError::Invalid(
                    Entity::Distribution,
                    Some(format!("union child has unexpected distribution Single. Left: {left:?}, right: {right:?}"))));
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

    /// Calculate a new distribution for the `Except` and `UnionAll` output tuple.
    /// Single
    fn except(left: &Distribution, right: &Distribution) -> Result<Distribution, SbroadError> {
        let dist = match (left, right) {
            (Distribution::Global, _) => right.clone(),
            (_, Distribution::Global) => left.clone(),
            (Distribution::Single, _) | (_, Distribution::Single) => {
                return Err(SbroadError::Invalid(
                    Entity::Distribution,
                    Some(format!("union/except child has unexpected distribution Single. Left: {left:?}, right: {right:?}"))));
            }
            (Distribution::Any, _) | (_, Distribution::Any) => Distribution::Any,
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
            (Distribution::Any, Distribution::Any) => Distribution::Any,
            (Distribution::Single, Distribution::Global | Distribution::Single)
            | (Distribution::Global, Distribution::Single) => Distribution::Single,
            (Distribution::Single, _) | (_, Distribution::Single) => {
                return Err(SbroadError::Invalid(
                    Entity::Distribution,
                    Some(format!("join child has unexpected distribution Single. Left: {left:?}, right: {right:?}"))));
            }
            (Distribution::Global, Distribution::Global) => {
                // this case is handled by `dist_from_subqueries``
                Distribution::Global
            }
            (Distribution::Global, _) | (Distribution::Any, Distribution::Segment { .. }) => {
                right.clone()
            }
            (_, Distribution::Global) | (Distribution::Segment { .. }, Distribution::Any) => {
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

struct ReferenceInfo {
    referred_children: ReferredNodes,
    child_column_to_parent_col: AHashMap<ChildColumnReference, ParentColumnPosition>,
}

impl ReferenceInfo {
    pub fn new(row_id: usize, ir: &Plan, parent_children: &[usize]) -> Result<Self, SbroadError> {
        let mut ref_nodes = ReferredNodes::new();
        let mut ref_map: AHashMap<ChildColumnReference, ParentColumnPosition> = AHashMap::new();
        for (parent_column_pos, id) in ir.get_row_list(row_id)?.iter().enumerate() {
            let child_id = ir.get_child_under_alias(*id)?;
            if let Expression::Reference {
                targets, position, ..
            } = ir.get_expression_node(child_id)?
            {
                // As the row is located in the branch relational node, the targets should be non-empty.
                let targets = targets.as_ref().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues("Reference targets are empty".to_string())
                })?;
                ref_map.reserve(targets.len());
                ref_nodes.reserve(targets.len());
                for target in targets {
                    let referred_id = parent_children.get(*target).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Expression,
                            "reference points to invalid column".to_string(),
                        )
                    })?;
                    ref_nodes.append(*referred_id);
                    ref_map.insert((*referred_id, *position).into(), parent_column_pos);
                }
            }
        }

        Ok(ReferenceInfo {
            referred_children: ref_nodes,
            child_column_to_parent_col: ref_map,
        })
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
    /// Sets distribution for output tuple of projection.
    ///
    /// # Errors
    /// - node is not projection
    /// - invalid projection node (e.g. no children)
    /// - failed to get child distribution
    pub fn set_projection_distribution(&mut self, proj_id: usize) -> Result<(), SbroadError> {
        if !matches!(
            self.get_relation_node(proj_id)?,
            Relational::Projection { .. }
        ) {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("expected projection on id: {proj_id}")),
            ));
        };

        let output_id = self.get_relational_output(proj_id)?;
        let child_id = self.get_relational_child(proj_id, 0)?;

        if let Distribution::Global = self.get_rel_distribution(child_id)? {
            self.set_dist(output_id, Distribution::Global)?;
            return Ok(());
        }

        let mut only_compound_exprs = true;
        for id in self.get_row_list(output_id)? {
            let child_id = self.get_child_under_alias(*id)?;
            if let Expression::Reference { .. } = self.get_expression_node(child_id)? {
                only_compound_exprs = false;
                break;
            }
        }
        if only_compound_exprs {
            // The projection looks like this: `select 1, a + b, 10 * b`
            // i.e no bare references like in `select a, b, c`
            self.set_dist(output_id, Distribution::Any)?;
            return Ok(());
        }

        // Projection has some bare references: `select a + b, b, c ..`
        // It may have Segment distribution, to check that we need a mapping
        // between referenced columns in child and column position in projection's
        // output.
        let children = self.get_relational_children(proj_id)?.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Node,
                Some(format!("projection node ({proj_id}) has no children")),
            )
        })?;
        let ref_info = ReferenceInfo::new(output_id, self, children)?;
        if let ReferredNodes::Single(child_id) = ref_info.referred_children {
            let child_dist =
                self.dist_from_child(child_id, &ref_info.child_column_to_parent_col)?;
            self.set_dist(output_id, child_dist)?;
        } else {
            return Err(SbroadError::Invalid(Entity::ReferredNodes, None));
        }

        Ok(())
    }

    /// Calculate and set tuple distribution.
    ///
    /// # Errors
    /// Returns `SbroadError` when current expression is not a `Row` or contains broken references.
    #[allow(clippy::too_many_lines)]
    pub fn set_distribution(&mut self, row_id: usize) -> Result<(), SbroadError> {
        let row_children = self.get_expression_node(row_id)?.get_row_list()?;

        let mut parent_node = None;
        for id in row_children {
            let child_id = self.get_child_under_alias(*id)?;
            if let Expression::Reference { parent, .. } = self.get_expression_node(child_id)? {
                parent_node = *parent;
                break;
            }
        }

        let Some(parent_id) = parent_node else {
            self.set_dist(row_id, Distribution::Any)?;
            return Ok(());
        };
        let parent = self.get_relation_node(parent_id)?;

        // References in the branch node.
        if let Some(children) = parent.children() {
            // Set of the referred relational nodes in the row.
            let ref_info = ReferenceInfo::new(row_id, self, children)?;

            match ref_info.referred_children {
                ReferredNodes::None => {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("the row contains no references".to_string()),
                    ));
                }
                ReferredNodes::Single(child_id) => {
                    let suggested_dist =
                        self.dist_from_child(child_id, &ref_info.child_column_to_parent_col)?;
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
                    self.set_two_children_node_dist(
                        &ref_info.child_column_to_parent_col,
                        n1,
                        n2,
                        parent_id,
                        row_id,
                    )?;
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
            let tbl_name = self.get_scan_relation(parent_id)?;
            let tbl = self.get_relation_or_error(tbl_name)?;
            if tbl.is_global() {
                self.set_dist(row_id, Distribution::Global)?;
                return Ok(());
            }
            let mut table_map: HashMap<usize, usize, RandomState> =
                HashMap::with_capacity_and_hasher(row_children.len(), RandomState::new());
            for (pos, id) in row_children.iter().enumerate() {
                let child_id = self.get_child_under_alias(*id)?;
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
            let sk = tbl.get_sk()?;
            let mut new_key: Key = Key::new(Vec::with_capacity(sk.len()));
            let all_found = sk.iter().all(|pos| {
                table_map.get(pos).map_or(false, |v| {
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
        }
        Ok(())
    }

    /// Join, Having or Selection
    /// may have references that refer only to one
    /// child (or two in case of Join),
    /// but their final distribution also depends
    /// on subqueries. Currently this dependency
    /// arises only when non-sq children (required children)
    /// have `Distribution::Global`.
    ///
    /// This method checks whether node is Join, Selection or Having,
    /// and if all required children have Global distribution, it computes
    /// the correct distribution in case there are any subqueries. Otherwise,
    /// it returns `None`.
    ///
    /// # Errors
    /// - node is not relational
    /// - incorrect number of children for node
    /// - missing Motion(Full) for sq with Any distribution
    pub(crate) fn dist_from_subqueries(
        &self,
        node_id: usize,
    ) -> Result<Option<Distribution>, SbroadError> {
        let node = self.get_relation_node(node_id)?;

        if !matches!(
            node,
            Relational::Join { .. } | Relational::Having { .. } | Relational::Selection { .. }
        ) {
            return Ok(None);
        }

        let required_children_len = self.get_required_children_len(node_id)?;
        // check all required children have Global distribution
        for child_idx in 0..required_children_len {
            let child_id = self.get_relational_child(node_id, child_idx)?;
            let child_dist = self.get_rel_distribution(child_id)?;
            if !matches!(child_dist, Distribution::Global) {
                return Ok(None);
            }
        }

        let children_len = node.children().map_or(0, <[usize]>::len);
        let mut suggested_dist = Some(Distribution::Global);
        for sq_idx in required_children_len..children_len {
            let sq_id = self.get_relational_child(node_id, sq_idx)?;
            match self.get_rel_distribution(sq_id)? {
                Distribution::Segment { .. } => {
                    suggested_dist = Some(Distribution::Any);
                }
                Distribution::Any { .. } => {
                    // Earier when resolving conflicts for subqueries we must have
                    // inserted Motion(Full) for subquery with Any
                    // distribution.
                    return Err(SbroadError::Invalid(
                        Entity::Distribution,
                        Some(format!(
                            "expected Motion(Full) for subquery child ({sq_id})"
                        )),
                    ));
                }
                Distribution::Single | Distribution::Global => {}
            }
        }

        Ok(suggested_dist)
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
                        ));
                    }
                    Some(Distribution::Single) => return Ok(Distribution::Single),
                    Some(Distribution::Any) => return Ok(Distribution::Any),
                    Some(Distribution::Global) => return Ok(Distribution::Global),
                    Some(Distribution::Segment { keys }) => {
                        let mut new_keys: HashSet<Key, RepeatableState> =
                            HashSet::with_hasher(RepeatableState);
                        for key in keys.iter() {
                            let mut new_key: Key =
                                Key::new(Vec::with_capacity(key.positions.len()));
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

                        // Parent's operator output does not contain some
                        // sharding columns. For example:
                        // ```sql
                        // select b from t
                        // ```
                        //
                        // Where `t` is sharded by `a`.
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
            Relational::Except { .. } => Distribution::except(&left_dist, &right_dist)?,
            Relational::UnionAll { .. } => Distribution::union(&left_dist, &right_dist)?,
            Relational::Join { .. } => Distribution::join(&left_dist, &right_dist)?,
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Relational,
                    Some("expected Except, UnionAll or InnerJoin".to_string()),
                ));
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
            Node::Ddl(_) => Err(SbroadError::Invalid(
                Entity::Distribution,
                Some("Failed to get distribution for a DDL node.".to_string()),
            )),
            Node::Acl(_) => Err(SbroadError::Invalid(
                Entity::Distribution,
                Some("Failed to get distribution for a ACL node.".to_string()),
            )),
            Node::Block(_) => Err(SbroadError::Invalid(
                Entity::Distribution,
                Some("Failed to get distribution for a code block node.".to_string()),
            )),
        }
    }

    /// Gets distribution of the relational node.
    ///
    /// # Errors
    /// - Node is not realtional
    /// - Node is not of a row type.
    pub fn get_rel_distribution(&self, rel_id: usize) -> Result<&Distribution, SbroadError> {
        self.get_distribution(self.get_relation_node(rel_id)?.output())
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
