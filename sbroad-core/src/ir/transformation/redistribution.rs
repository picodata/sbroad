//! Resolve distribution conflicts and insert motion nodes to IR.

use ahash::{AHashSet, RandomState};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{hash_map::Entry, HashMap, HashSet};

use crate::errors::{Action, Entity, SbroadError};
use crate::ir::distribution::{Distribution, Key, KeySet};
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, JoinKind, Relational, Unary};

use crate::ir::transformation::redistribution::eq_cols::EqualityCols;
use crate::ir::tree::traversal::{BreadthFirst, PostOrder, EXPR_CAPACITY, REL_CAPACITY};
use crate::ir::value::Value;
use crate::ir::{Node, Plan};
use crate::otm::child_span;
use sbroad_proc::otm_child_span;

pub(crate) mod delete;
pub(crate) mod eq_cols;
pub(crate) mod groupby;
pub(crate) mod insert;

pub(crate) enum JoinChild {
    Inner,
    Outer,
}

/// Redistribution key targets (columns or values of the key).
#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub enum Target {
    /// A position of the existing column in the tuple.
    Reference(usize),
    /// A value that should be used as a part of the
    /// redistribution key. We need it in a case of
    /// `insert into t1 (b) ...` when a column `a` is
    /// absent and should be generated from the default
    /// value.
    Value(Value),
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub struct MotionKey {
    pub targets: Vec<Target>,
}

impl MotionKey {
    fn new() -> Self {
        MotionKey { targets: vec![] }
    }
}

impl From<Key> for MotionKey {
    fn from(key: Key) -> Self {
        MotionKey {
            targets: key.positions.into_iter().map(Target::Reference).collect(),
        }
    }
}

impl From<&Key> for MotionKey {
    fn from(key: &Key) -> Self {
        let positions: &[usize] = &key.positions;
        MotionKey {
            targets: positions
                .iter()
                .map(|pos| Target::Reference(*pos))
                .collect(),
        }
    }
}

/// Determinate what portion of data to move between data nodes in cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum MotionPolicy {
    /// Nothing to move.
    None,
    /// Move all data.
    Full,
    /// Move only a segment of data according to the motion key.
    Segment(MotionKey),
    /// Materialize a virtual table on the data node.
    Local,
    /// Materialize a virtual table on the data node and calculate
    /// tuple buckets.
    LocalSegment(MotionKey),
}

impl MotionPolicy {
    #[must_use]
    pub fn is_local(&self) -> bool {
        matches!(
            self,
            MotionPolicy::Local | MotionPolicy::LocalSegment(_) | MotionPolicy::None
        )
    }
}

pub type ColumnPosition = usize;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum MotionOpcode {
    PrimaryKey(Vec<ColumnPosition>),
    Projection(Vec<ColumnPosition>),
}

/// Helper struct that unwraps `Expression::Bool` fields.
struct BoolOp {
    left: usize,
    op: Bool,
    right: usize,
}

impl BoolOp {
    fn from_expr(plan: &Plan, expr_id: usize) -> Result<Self, SbroadError> {
        if let Expression::Bool {
            left, op, right, ..
        } = plan.get_expression_node(expr_id)?
        {
            Ok(BoolOp {
                left: *left,
                op: op.clone(),
                right: *right,
            })
        } else {
            Err(SbroadError::Invalid(Entity::Operator, None))
        }
    }
}

pub type Program = Vec<MotionOpcode>;
type ChildId = usize;
type DataTransformation = (MotionPolicy, Program);

/// Helper struct to store motion policy for every child of
/// relational node with `parent_id`.
#[derive(Debug)]
struct Strategy {
    parent_id: usize,
    children_policy: HashMap<ChildId, DataTransformation>,
}

impl Strategy {
    fn new(parent_id: usize) -> Self {
        Self {
            parent_id,
            children_policy: HashMap::new(),
        }
    }

    /// Add motion policy for child node.
    /// Update policy in case `child_id` key is already in the `children_policy` map.
    fn add_child(&mut self, child_id: usize, policy: MotionPolicy, program: Program) {
        self.children_policy.insert(child_id, (policy, program));
    }
}

/// Choose a policy for the inner join child (a policy with the largest motion of data wins).
fn join_policy_for_or(
    left_policy: &MotionPolicy,
    right_policy: &MotionPolicy,
) -> Result<MotionPolicy, SbroadError> {
    match (left_policy, right_policy) {
        (MotionPolicy::LocalSegment(_) | MotionPolicy::Local, _)
        | (_, MotionPolicy::LocalSegment(_) | MotionPolicy::Local) => Err(SbroadError::Invalid(
            Entity::Motion,
            Some("LocalSegment motion is not supported for joins".to_string()),
        )),
        (MotionPolicy::Full, _) | (_, MotionPolicy::Full) => Ok(MotionPolicy::Full),
        (MotionPolicy::None, _) => Ok(right_policy.clone()),
        (_, MotionPolicy::None) => Ok(left_policy.clone()),
        (MotionPolicy::Segment(key_left), MotionPolicy::Segment(key_right)) => {
            if key_left == key_right {
                Ok(left_policy.clone())
            } else {
                Ok(MotionPolicy::Full)
            }
        }
    }
}

/// Choose a policy for the inner join child (a policy with the smallest motion of data wins).
fn join_policy_for_and(
    left_policy: &MotionPolicy,
    right_policy: &MotionPolicy,
) -> Result<MotionPolicy, SbroadError> {
    match (left_policy, right_policy) {
        (MotionPolicy::LocalSegment(_) | MotionPolicy::Local, _)
        | (_, MotionPolicy::LocalSegment(_) | MotionPolicy::Local) => Err(SbroadError::Invalid(
            Entity::Motion,
            Some("LocalSegment motion is not supported for joins".to_string()),
        )),
        (MotionPolicy::Full, _) => Ok(right_policy.clone()),
        (_, MotionPolicy::Full) => Ok(left_policy.clone()),
        (MotionPolicy::None, _) | (_, MotionPolicy::None) => Ok(MotionPolicy::None),
        (MotionPolicy::Segment(key_left), MotionPolicy::Segment(key_right)) => {
            if key_left == key_right {
                Ok(left_policy.clone())
            } else {
                Ok(MotionPolicy::Full)
            }
        }
    }
}

impl Plan {
    /// Get a list of relational nodes in a DFS post order.
    ///
    /// # Errors
    /// - plan doesn't contain the top node
    fn get_relational_nodes_dfs_post(&self) -> Result<Vec<usize>, SbroadError> {
        let top = self.get_top()?;
        let mut post_tree =
            PostOrder::with_capacity(|node| self.nodes.rel_iter(node), REL_CAPACITY);
        let nodes: Vec<usize> = post_tree.iter(top).map(|(_, id)| id).collect();
        Ok(nodes)
    }

    /// Get boolean expressions with both row children in the sub-tree.
    /// It's a helper function for resolving subquery conflicts.
    /// E.g. boolean `In` operator will have both row children where
    /// right `Row` is a transformed subquery.
    ///
    /// # Errors
    /// - some of the expression nodes are invalid
    pub(crate) fn get_bool_nodes_with_row_children(
        &self,
        top: usize,
    ) -> Result<Vec<usize>, SbroadError> {
        let mut nodes: Vec<usize> = Vec::new();

        let mut post_tree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, false), EXPR_CAPACITY);
        for (_, id) in post_tree.iter(top) {
            // Append only booleans with row children.
            if let Node::Expression(Expression::Bool { left, right, .. }) = self.get_node(id)? {
                let left_is_row = matches!(
                    self.get_node(*left)?,
                    Node::Expression(Expression::Row { .. })
                );
                let right_is_row = matches!(
                    self.get_node(*right)?,
                    Node::Expression(Expression::Row { .. })
                );
                if left_is_row && right_is_row {
                    nodes.push(id);
                }
            }
        }
        Ok(nodes)
    }

    /// Get unary expressions with both row children in the sub-tree.
    /// It's a helper function for resolving subquery conflicts.
    /// E.g. unary `Exists` operator will have `Row` child that
    /// is a transformed subquery.
    ///
    /// # Errors
    /// - some of the expression nodes are invalid
    pub(crate) fn get_unary_nodes_with_row_children(
        &self,
        top: usize,
    ) -> Result<Vec<usize>, SbroadError> {
        let mut nodes: Vec<usize> = Vec::new();

        let mut post_tree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, false), EXPR_CAPACITY);
        for (_, id) in post_tree.iter(top) {
            // Append only unaries with row children.
            if let Node::Expression(Expression::Unary { child, .. }) = self.get_node(id)? {
                let child_is_row = matches!(
                    self.get_node(*child)?,
                    Node::Expression(Expression::Row { .. })
                );
                if child_is_row {
                    nodes.push(id);
                }
            }
        }
        Ok(nodes)
    }

    /// Get a single sub-query from the row node.
    ///
    /// # Errors
    /// - Row node is not of a row type
    /// There are more than one sub-queries in the row node.
    pub fn get_sub_query_from_row_node(&self, row_id: usize) -> Result<Option<usize>, SbroadError> {
        let rel_ids = self.get_relational_nodes_from_row(row_id)?;
        self.get_sub_query_among_rel_nodes(&rel_ids)
    }

    /// Get a single sub-query from among the list of relational nodes.
    ///
    /// # Errors
    /// - Some of the nodes are not relational
    /// - There are more than one sub-query
    pub fn get_sub_query_among_rel_nodes(
        &self,
        rel_nodes: &HashSet<usize, RandomState>,
    ) -> Result<Option<usize>, SbroadError> {
        let mut sq_set: HashSet<usize, RandomState> = HashSet::with_hasher(RandomState::new());
        for rel_id in rel_nodes {
            if let Node::Relational(Relational::ScanSubQuery { .. }) = self.get_node(*rel_id)? {
                sq_set.insert(*rel_id);
            }
        }
        match sq_set.len().cmp(&1) {
            Ordering::Equal | Ordering::Greater => sq_set.iter().next().map_or_else(
                || {
                    Err(SbroadError::UnexpectedNumberOfValues(format!(
                        "Failed to get the first sub-query node from the list of relational nodes: {rel_nodes:?}."
                    )))
                },
                |sq_id| Ok(Some(*sq_id)),
            ),
            Ordering::Less => Ok(None),
        }
    }

    /// Extract a motion node id from the row node
    ///
    /// # Errors
    /// - Row node is not of a row type
    /// - There are more than one motion nodes in the row node
    pub fn get_motion_from_row(&self, node_id: usize) -> Result<Option<usize>, SbroadError> {
        let rel_nodes = self.get_relational_nodes_from_row(node_id)?;
        self.get_motion_among_rel_nodes(&rel_nodes)
    }

    /// Extract motion node id from row node
    ///
    /// # Errors
    /// - Some of the nodes are not relational
    pub fn get_motion_among_rel_nodes(
        &self,
        rel_nodes: &HashSet<usize, RandomState>,
    ) -> Result<Option<usize>, SbroadError> {
        let mut motion_set: HashSet<usize> = HashSet::new();

        for child in rel_nodes {
            if self.get_relation_node(*child)?.is_motion() {
                motion_set.insert(*child);
            }
        }

        match motion_set.len().cmp(&1) {
            Ordering::Equal | Ordering::Greater => {
                let motion_id = motion_set.iter().next().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(
                        "failed to get the first Motion node from the set.".into(),
                    )
                })?;
                Ok(Some(*motion_id))
            }
            Ordering::Less => Ok(None),
        }
    }

    /// Choose a `MotionPolicy` strategy for the inner row.
    ///
    /// # Errors
    /// - nodes are not rows
    /// - uninitialized distribution for some row
    fn choose_strategy_for_bool_op_inner_sq(
        &self,
        outer_id: usize,
        inner_id: usize,
        op: &Bool,
    ) -> Result<MotionPolicy, SbroadError> {
        let outer_dist = self.get_distribution(outer_id)?;
        let inner_dist = self.get_distribution(inner_id)?;
        if Bool::Eq == *op || Bool::In == *op {
            if let Distribution::Segment {
                keys: ref keys_outer,
            } = outer_dist
            {
                // If the inner and outer tuples are segmented by the same key we don't need motions.
                if let Distribution::Segment {
                    keys: ref keys_inner,
                } = inner_dist
                {
                    if keys_outer.intersection(keys_inner).iter().next().is_some() {
                        return Ok(MotionPolicy::None);
                    }
                }
                // Redistribute the inner tuples using the first key from the outer tuple.
                return keys_outer.iter().next().map_or_else(
                    || {
                        Err(SbroadError::UnexpectedNumberOfValues(String::from(
                            "Failed to get the first distribution key from the outer row.",
                        )))
                    },
                    |key| Ok(MotionPolicy::Segment(key.into())),
                );
            }
        }
        Ok(MotionPolicy::Full)
    }

    /// Create Motions from the strategy map.
    fn create_motion_nodes(&mut self, mut strategy: Strategy) -> Result<(), SbroadError> {
        let parent_id = strategy.parent_id;
        let children: Vec<usize> =
            if let Some(children) = self.get_relational_children(parent_id)? {
                children.to_vec()
            } else {
                return Err(SbroadError::FailedTo(
                    Action::Add,
                    Some(Entity::Motion),
                    "trying to add motions under the leaf relational node".into(),
                ));
            };

        // Check that all children we need to add motions exist in the current relational node.
        let children_set: HashSet<usize> = children.iter().copied().collect();
        if let false = strategy
            .children_policy
            .iter()
            .all(|(node, _)| children_set.get(node).is_some())
        {
            return Err(SbroadError::FailedTo(
                Action::Add,
                Some(Entity::Motion),
                "trying to add motions for non-existing children in relational node".into(),
            ));
        }

        // Add motions.
        let mut children_with_motions: Vec<usize> = Vec::new();
        for child in children {
            if let Some((policy, ref mut program)) = strategy.children_policy.get_mut(&child) {
                let program = std::mem::take(program);
                if let MotionPolicy::None = policy {
                    children_with_motions.push(child);
                } else {
                    children_with_motions.push(self.add_motion(child, policy, program)?);
                }
            } else {
                children_with_motions.push(child);
            }
        }
        self.set_relational_children(parent_id, children_with_motions)?;
        Ok(())
    }

    /// Get `Relational::SubQuery` node that is referenced by passed `row_id`.
    /// Only returns `SubQuery` that is an additional child of passed `rel_id` node.
    fn get_additional_sq(
        &self,
        rel_id: usize,
        row_id: usize,
    ) -> Result<Option<usize>, SbroadError> {
        if self.get_expression_node(row_id)?.is_row() {
            if let Some(sq_id) = self.get_sub_query_from_row_node(row_id)? {
                if self.is_additional_child_of_rel(rel_id, sq_id)? {
                    return Ok(Some(sq_id));
                }
            }
        }
        Ok(None)
    }

    /// Get `SubQuery`s from passed boolean `op_id` node (e.g. `In`).
    fn get_sq_node_strategies_for_bool_op(
        &self,
        rel_id: usize,
        op_id: usize,
    ) -> Result<Vec<(usize, MotionPolicy)>, SbroadError> {
        let mut strategies: Vec<(usize, MotionPolicy)> = Vec::new();
        let bool_op = BoolOp::from_expr(self, op_id)?;
        let left = self.get_additional_sq(rel_id, bool_op.left)?;
        let right = self.get_additional_sq(rel_id, bool_op.right)?;
        match left {
            Some(left_sq) => {
                match right {
                    Some(right_sq) => {
                        // Both sides are sub-queries and require a full copy.
                        strategies.push((left_sq, MotionPolicy::Full));
                        strategies.push((right_sq, MotionPolicy::Full));
                    }
                    None => {
                        // Left side is sub-query, right is an outer tuple.
                        strategies.push((
                            left_sq,
                            self.choose_strategy_for_bool_op_inner_sq(
                                bool_op.right,
                                self.get_relational_output(left_sq)?,
                                &bool_op.op,
                            )?,
                        ));
                    }
                }
            }
            None => {
                if let Some(right_sq) = right {
                    // Left side is an outer tuple, right is sub-query.
                    strategies.push((
                        right_sq,
                        self.choose_strategy_for_bool_op_inner_sq(
                            bool_op.left,
                            self.get_relational_output(right_sq)?,
                            &bool_op.op,
                        )?,
                    ));
                }
            }
        }
        Ok(strategies)
    }

    /// Get `SubQuery`s from passed unary `op_id` node (e.g. `Exists`).
    fn get_sq_node_strategy_for_unary_op(
        &self,
        rel_id: usize,
        op_id: usize,
    ) -> Result<Option<(usize, MotionPolicy)>, SbroadError> {
        let unary_op_expr = self.get_expression_node(op_id)?;
        let Expression::Unary { child, op } = unary_op_expr else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format!("Expected Unary expression, got {unary_op_expr:?}")),
            ));
        };

        if let Unary::Exists | Unary::NotExists = op {
            let child_sq = self.get_additional_sq(rel_id, *child)?;
            if let Some(child_sq) = child_sq {
                return Ok(Some((child_sq, MotionPolicy::Full)));
            }
        }

        Ok(None)
    }

    /// Resolve sub-query conflicts with motion policies.
    fn resolve_sub_query_conflicts(
        &mut self,
        select_id: usize,
        filter_id: usize,
    ) -> Result<Strategy, SbroadError> {
        let mut strategy = Strategy::new(select_id);

        let bool_nodes = self.get_bool_nodes_with_row_children(filter_id)?;
        for bool_node in &bool_nodes {
            let bool_op = BoolOp::from_expr(self, *bool_node)?;
            self.set_distribution(bool_op.left)?;
            self.set_distribution(bool_op.right)?;
        }
        for bool_node in &bool_nodes {
            let strategies = self.get_sq_node_strategies_for_bool_op(select_id, *bool_node)?;
            for (id, policy) in strategies {
                strategy.add_child(id, policy, Program::new());
            }
        }

        let unary_nodes = self.get_unary_nodes_with_row_children(filter_id)?;
        for unary_node in &unary_nodes {
            let unary_strategy = self.get_sq_node_strategy_for_unary_op(select_id, *unary_node)?;
            if let Some((id, policy)) = unary_strategy {
                strategy.add_child(id, policy, Program::new());
            }
        }

        Ok(strategy)
    }

    /// Get the children of a join node (outer, inner and sub-queries).
    ///
    /// # Errors
    /// - If the node is not a join node.
    /// - Join node has no children.
    fn get_join_children(&self, join_id: usize) -> Result<&[usize], SbroadError> {
        let join = self.get_relation_node(join_id)?;
        if let Relational::Join { .. } = join {
        } else {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some("Join node is not an inner join.".into()),
            ));
        }
        let children = join.children().ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues("Join node has no children.".into())
        })?;
        Ok(children)
    }

    /// Detect join child from the position map corresponding to the distribution key.
    fn get_join_child_by_key(
        &self,
        key: &Key,
        row_map: &HashMap<usize, usize>,
        join_children: &[usize],
    ) -> Result<usize, SbroadError> {
        let mut children_set: HashSet<usize> = HashSet::new();
        for pos in &key.positions {
            let column_id = *row_map.get(pos).ok_or_else(|| {
                SbroadError::NotFound(Entity::Column, format!("{pos} in row map {row_map:?}"))
            })?;
            if let Expression::Reference { targets, .. } = self.get_expression_node(column_id)? {
                if let Some(targets) = targets {
                    for target in targets {
                        let child_id = *join_children.get(*target).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Target,
                                format!("{target} in join children {join_children:?}"),
                            )
                        })?;
                        children_set.insert(child_id);
                    }
                }
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("Row column is not a reference.".into()),
                ));
            }
        }
        if children_set.len() > 1 {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Distribution key in the join condition has more than one child.".into(),
            ));
        }
        children_set.iter().next().copied().ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues(
                "Distribution key in the join condition has no children.".into(),
            )
        })
    }

    /// Builds a row column map, where every column in the row is mapped by its position.
    ///
    /// # Errors
    /// - If the node is not a row node.
    fn build_row_map(&self, row_id: usize) -> Result<HashMap<usize, usize>, SbroadError> {
        let columns = self.get_expression_node(row_id)?.get_row_list()?;
        let mut map: HashMap<usize, usize> = HashMap::new();
        for (pos, col) in columns.iter().enumerate() {
            map.insert(pos, *col);
        }
        Ok(map)
    }

    /// Split the distribution keys from the row of the join
    /// condition into inner and outer keys.
    /// Returns a tuple of (outer keys, inner keys).
    ///
    /// # Errors
    /// - Join node does not have any children.
    /// - Distribution keys do not refer to inner or outer children of the join.
    fn split_join_keys_to_inner_and_outer(
        &self,
        join_id: usize,
        keys: &[Key],
        row_map: &HashMap<usize, usize>,
    ) -> Result<(Vec<Key>, Vec<Key>), SbroadError> {
        let mut outer_keys: Vec<Key> = Vec::new();
        let mut inner_keys: Vec<Key> = Vec::new();

        let children = self.get_join_children(join_id)?;
        let outer_child = *children.first().ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues("Join node has no children.".into())
        })?;
        let inner_child = *children.get(1).ok_or_else(|| {
            SbroadError::NotFound(Entity::Node, "that is Join node inner child".into())
        })?;

        for key in keys {
            let child = self.get_join_child_by_key(key, row_map, children)?;
            if child == outer_child {
                outer_keys.push(key.clone());
            } else if child == inner_child {
                inner_keys.push(key.clone());
            } else {
                // It can be only a sub-query, but we have already processed it.
                return Err(SbroadError::Invalid(
                    Entity::DistributionKey,
                    Some(
                        "distribution key doesn't correspond to inner or outer join children."
                            .into(),
                    ),
                ));
            }
        }
        Ok((outer_keys, inner_keys))
    }

    /// The join condition row can contain arbitrary expressions inside (constants,
    /// arithmetic operations, references to inner and outer children, etc.).
    /// This function extracts only the positions of the references to the inner child.
    fn get_inner_positions_from_condition_row(
        &self,
        row_map: &HashMap<usize, usize>,
    ) -> Result<AHashSet<usize>, SbroadError> {
        let mut inner_positions: AHashSet<usize> = AHashSet::with_capacity(row_map.len());
        for (pos, col) in row_map {
            let expression = self.get_expression_node(*col)?;
            if let Expression::Reference {
                targets: Some(targets),
                ..
            } = expression
            {
                // Inner child of the join node is always the second one.
                if targets == &[1; 1] {
                    inner_positions.insert(*pos);
                }
            }
        }
        Ok(inner_positions)
    }

    /// Take the positions of the columns in the join condition row
    /// and return the positions of the columns in the inner child row.
    fn get_referred_inner_child_column_positions(
        &self,
        column_positions: &[usize],
        condition_row_map: &HashMap<usize, usize>,
    ) -> Result<Vec<usize>, SbroadError> {
        let mut referred_column_positions: Vec<usize> = Vec::with_capacity(column_positions.len());
        for pos in column_positions {
            let column_id = *condition_row_map.get(pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Column,
                    format!("{pos} in row map {condition_row_map:?}"),
                )
            })?;
            if let Expression::Reference {
                targets, position, ..
            } = self.get_expression_node(column_id)?
            {
                if let Some(targets) = targets {
                    // Inner child of the join node is always the second one.
                    if targets == &[1; 1] {
                        referred_column_positions.push(*position);
                    }
                }
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("Row column is not a reference.".into()),
                ));
            }
        }
        Ok(referred_column_positions)
    }

    /// This is a helper function used to determine the policy of the inner child motion node in the join.
    /// For this purpose we inspect the join equality condition expression. It has the left and the right
    /// sides, both of which can either point to the inner and outer children of the join.
    /// If one of the sides has a `Segment` distribution and its keys point to the outer join child, then
    /// we try to calculate the possible motion policy of the inner child (it can be `Segment` or `Full`).
    /// To deal with it we also need the inner row map that maps the positions of the columns in the condition
    /// row to the `Reference` ids in that row. These references point to the inner child row.
    fn get_inner_policy_by_outer_segment(
        &self,
        outer_keys: &[Key],
        inner_row_map: &HashMap<usize, usize>,
    ) -> Result<MotionPolicy, SbroadError> {
        let inner_position_map = self.get_inner_positions_from_condition_row(inner_row_map)?;
        for outer_key in outer_keys {
            if outer_key
                .positions
                .iter()
                .all(|pos| inner_position_map.contains(pos))
            {
                let mut matched_inner_positions: Vec<usize> =
                    Vec::with_capacity(outer_key.positions.len());
                for pos in &outer_key.positions {
                    let pos = match inner_position_map.get(pos) {
                        Some(pos) => *pos,
                        None => continue,
                    };
                    matched_inner_positions.push(pos);
                }
                let inner_child_positions = self.get_referred_inner_child_column_positions(
                    &matched_inner_positions,
                    inner_row_map,
                )?;
                let inner_key = Key::new(inner_child_positions);
                return Ok(MotionPolicy::Segment(inner_key.into()));
            }
        }
        Ok(MotionPolicy::Full)
    }

    /// Derive the motion policy in the boolean node with equality operator.
    ///
    /// # Errors
    /// - Left or right row IDs are invalid.
    /// - Failed to get row distribution.
    /// - Failed to split distribution keys in the row to inner and outer keys.
    fn join_policy_for_eq(
        &self,
        join_id: usize,
        left_row_id: usize,
        right_row_id: usize,
    ) -> Result<MotionPolicy, SbroadError> {
        let left_dist = self.get_distribution(left_row_id)?;
        let right_dist = self.get_distribution(right_row_id)?;
        let row_map_left = self.build_row_map(left_row_id)?;
        let row_map_right = self.build_row_map(right_row_id)?;

        match (left_dist, right_dist) {
            (
                Distribution::Segment {
                    keys: keys_left_set,
                },
                Distribution::Segment {
                    keys: keys_right_set,
                },
            ) => {
                let keys_left = keys_left_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let keys_right = keys_right_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let (left_outer_keys, left_inner_keys) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_left, &row_map_left)?;
                let (right_outer_keys, right_inner_keys) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_right, &row_map_right)?;

                // Can we join locally?
                let pairs = &[
                    (&left_outer_keys, &right_inner_keys),
                    (&right_outer_keys, &left_inner_keys),
                ];
                // We don't care about O(n^2) here, because the number of keys is usually small.
                for (outer_keys, inner_keys) in pairs {
                    for outer_key in *outer_keys {
                        for inner_key in *inner_keys {
                            if outer_key == inner_key {
                                return Ok(MotionPolicy::None);
                            }
                        }
                    }
                }
                // Example:
                //
                // t1 (a, b, c): key(c, b)
                // t2 (d, e): key(d)
                // select * from t1 join t2 on (t1.b, t1.c) = (t2.d, t2.e);
                // (t1.b, t1.c): segment[1, 0]
                // (t2.d, t2.e): segment[0]
                //
                // We take the first left outer key - (t1.b, t1.c) with positions 1 and 0 - and
                // check if on the right side of equality operator we have references to the inner
                // child at these positions. In our case, we have t2.e at positions 1 and t2.d at
                // 0 that refers to the inner child t2. So, we should reshard the inner child t2 by
                // (t2.e, t2.d). The motion node would have a distribution segment[0, 1] as t2.e is
                // at position 0 and t2.d is at position 1 in the inner child t2.

                if let MotionPolicy::Segment(inner_key) =
                    self.get_inner_policy_by_outer_segment(&left_outer_keys, &row_map_right)?
                {
                    return Ok(MotionPolicy::Segment(inner_key));
                }
                self.get_inner_policy_by_outer_segment(&right_outer_keys, &row_map_left)
            }
            (Distribution::Segment { keys: keys_set }, _) => {
                let keys_left = keys_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let (left_outer_keys, _) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_left, &row_map_left)?;
                self.get_inner_policy_by_outer_segment(&left_outer_keys, &row_map_right)
            }
            (_, Distribution::Segment { keys: keys_set }) => {
                let keys_right = keys_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let (right_outer_keys, _) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_right, &row_map_right)?;
                self.get_inner_policy_by_outer_segment(&right_outer_keys, &row_map_left)
            }
            _ => Ok(MotionPolicy::Full),
        }
    }

    /// Derive the motion policy for the inner child and sub-queries in the join node.
    ///
    /// # Errors
    /// - Failed to set row distribution in the join condition tree.
    #[allow(clippy::too_many_lines)]
    fn resolve_join_conflicts(
        &mut self,
        rel_id: usize,
        cond_id: usize,
        join_kind: &JoinKind,
    ) -> Result<(), SbroadError> {
        // If one of the children has Distribution::Single, then we can't compute Distribution of
        // Rows in condition, because in case of Single it depends on join condition, and computing
        // distribution of Row in condition makes no sense, so we handle the single distribution separately
        if let Some(strategy) =
            self.calculate_strategy_for_single_distribution(rel_id, cond_id, join_kind)?
        {
            self.create_motion_nodes(strategy)?;
            let nodes = self.get_bool_nodes_with_row_children(cond_id)?;
            for node in &nodes {
                let bool_op = BoolOp::from_expr(self, *node)?;
                self.set_distribution(bool_op.left)?;
                self.set_distribution(bool_op.right)?;
            }
            return Ok(());
        }

        // First, we need to set the motion policy for each boolean expression in the join condition.
        {
            let nodes = self.get_bool_nodes_with_row_children(cond_id)?;
            for node in &nodes {
                let bool_op = BoolOp::from_expr(self, *node)?;
                self.set_distribution(bool_op.left)?;
                self.set_distribution(bool_op.right)?;
            }
        }

        // Init the strategy (motion policy map) for all the join children except the outer child.
        let join_children = self.get_join_children(rel_id)?;
        let mut strategy = Strategy::new(rel_id);
        if let Some((_, children)) = join_children.split_first() {
            for child_id in children {
                strategy.add_child(*child_id, MotionPolicy::Full, Program::new());
            }
        } else {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Join node doesn't have any children.".into(),
            ));
        }

        // Let's improve the full motion policy for the join children (sub-queries and the inner child).
        let inner_child = *join_children.get(1).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                "that is Join node inner child with index 1.".into(),
            )
        })?;
        let mut inner_map: HashMap<usize, MotionPolicy> = HashMap::new();
        let mut new_inner_policy = MotionPolicy::Full;
        let mut expr_tree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, true), EXPR_CAPACITY);
        for (_, node_id) in expr_tree.iter(cond_id) {
            let expr = self.get_expression_node(node_id)?;
            let bool_op = if let Expression::Bool { .. } = expr {
                BoolOp::from_expr(self, node_id)?
            } else {
                continue;
            };

            // Try to improve full motion policy in the sub-queries.
            // We don't influence the inner child here, so the inner map is empty
            // for the current node id.
            // `get_sq_node_strategies_for_bool_op` will be triggered only in case `node_id` is a
            // boolean operator with both `Row` children.
            // Note, that we don't have to call `get_sq_node_strategy_for_unary_op` here, because
            // the only strategy it can return is `Motion::Full` for its child and all subqueries
            // are covered with `Motion::Full` by default.
            let sq_strategies = self.get_sq_node_strategies_for_bool_op(rel_id, node_id)?;
            let sq_strategies_len = sq_strategies.len();
            for (id, policy) in sq_strategies {
                strategy.add_child(id, policy, Program::new());
            }
            if sq_strategies_len > 0 {
                continue;
            }

            // Ok, we don't have any sub-queries.
            // Lets try to improve the motion policy for the inner join child.
            let left_expr = self.get_expression_node(bool_op.left)?;
            let right_expr = self.get_expression_node(bool_op.right)?;
            new_inner_policy = match (left_expr, right_expr) {
                (Expression::Arithmetic { .. }, _) | (_, Expression::Arithmetic { .. }) => {
                    MotionPolicy::Full
                }
                (
                    Expression::Bool { .. } | Expression::Unary { .. },
                    Expression::Bool { .. } | Expression::Unary { .. },
                ) => {
                    let left_policy = inner_map
                        .get(&bool_op.left)
                        .cloned()
                        .unwrap_or(MotionPolicy::Full);
                    let right_policy = inner_map
                        .get(&bool_op.right)
                        .cloned()
                        .unwrap_or(MotionPolicy::Full);
                    match bool_op.op {
                        Bool::And => join_policy_for_and(&left_policy, &right_policy)?,
                        Bool::Or => join_policy_for_or(&left_policy, &right_policy)?,
                        _ => {
                            return Err(SbroadError::Unsupported(
                                Entity::Operator,
                                Some("unsupported boolean operation, expected And or Or".into()),
                            ))
                        }
                    }
                }
                (Expression::Row { .. }, Expression::Row { .. }) => {
                    match bool_op.op {
                        Bool::Eq | Bool::In => {
                            self.join_policy_for_eq(rel_id, bool_op.left, bool_op.right)?
                        }
                        Bool::Gt
                        | Bool::GtEq
                        | Bool::Lt
                        | Bool::LtEq
                        | Bool::NotEq
                        | Bool::NotIn => MotionPolicy::Full,
                        Bool::And | Bool::Or => {
                            // "a and 1" or "a or 1" expressions make no sense.
                            return Err(SbroadError::Unsupported(
                                Entity::Operator,
                                Some("unsupported boolean operation And or Or".into()),
                            ));
                        }
                    }
                }
                (
                    Expression::Constant { .. },
                    Expression::Bool { .. } | Expression::Unary { .. },
                ) => inner_map
                    .get(&bool_op.right)
                    .cloned()
                    .unwrap_or(MotionPolicy::Full),
                (
                    Expression::Bool { .. } | Expression::Unary { .. },
                    Expression::Constant { .. },
                ) => inner_map
                    .get(&bool_op.left)
                    .cloned()
                    .unwrap_or(MotionPolicy::Full),
                _ => {
                    return Err(SbroadError::Unsupported(
                        Entity::Operator,
                        Some("unsupported boolean operation".into()),
                    ))
                }
            };
            inner_map.insert(node_id, new_inner_policy.clone());
        }
        strategy.add_child(inner_child, new_inner_policy, Program::new());
        self.create_motion_nodes(strategy)?;
        Ok(())
    }

    /// Helper function to compute motion policies for outer and inner children of join
    /// when one child has Segment distribution, and the other one has Single distribution
    fn compute_policies_for_segment_vs_single(
        condition_eq_cols: Option<&EqualityCols>,
        keys: &KeySet,
        segmented_child: &JoinChild,
        join_kind: &JoinKind,
    ) -> (MotionPolicy, MotionPolicy) {
        let (mut outer_policy, mut inner_policy) = match segmented_child {
            JoinChild::Outer => (MotionPolicy::None, MotionPolicy::Full),
            JoinChild::Inner => (MotionPolicy::Full, MotionPolicy::None),
        };
        if let Some(eq_cols) = condition_eq_cols {
            for key in keys.iter() {
                if key.positions.len() == eq_cols.len()
                    && key.positions.iter().all(|pos| -> bool {
                        eq_cols.iter().any(|(i, o)| {
                            *pos == match segmented_child {
                                JoinChild::Outer => *o,
                                JoinChild::Inner => *i,
                            }
                        })
                    })
                {
                    let policy_to_update = match segmented_child {
                        JoinChild::Outer => &mut inner_policy,
                        JoinChild::Inner => &mut outer_policy,
                    };
                    *policy_to_update = MotionPolicy::Segment(MotionKey {
                        targets: eq_cols
                            .iter()
                            .map(|(i_col, o_col)| -> Target {
                                let pos = match segmented_child {
                                    JoinChild::Outer => *i_col,
                                    JoinChild::Inner => *o_col,
                                };
                                Target::Reference(pos)
                            })
                            .collect::<Vec<Target>>(),
                    });
                    break;
                }
            }
        } else if let JoinKind::LeftOuter = join_kind {
            // if we can't perform repartition join (no equality columns),
            // and left join is performed, we can't broadcast left (outer) table.
            // in this case we broadcast the inner table and rehash outer table
            outer_policy = MotionPolicy::Segment(MotionKey {
                // we can choose any distribution columns here
                targets: vec![Target::Reference(0)],
            });
            inner_policy = MotionPolicy::Full;
        }
        (outer_policy, inner_policy)
    }

    /// Create strategy if at least one of the join children
    /// has `Distribution::Single`.
    ///
    /// Distributed join can be done either by broadcasting
    /// or repartitioning. Broadcasting is joining two tables
    /// when one table is sharded across several nodes and the
    /// copy of the second table is sent (broadcasted) to each node.
    /// Repartitioning is when the second table is sent by parts (not
    /// whole table is transferred).
    ///
    /// ```text
    /// select * from o join i on o.a = i.b
    /// ```
    /// In this example repartition join can be done:
    /// we can segment one of the tables by equality column and send
    /// it to nodes where other table resides.
    ///
    /// Because repartitioning involves less data motion, we should
    /// use it over broadcasting. But repartition join is possible only
    /// if join condition is good enough. For most of the join conditions
    /// we (sbroad) can do only broadcast join:
    ///```text
    /// select * from o join i on o.a < i.b
    ///```
    ///
    /// This function attempts to find by which columns inner and outer
    /// join children should be distributed in order to do repartition
    /// join.
    ///
    /// Then it looks on distributions of inner and outer. And depending
    /// on distribution of both children creates a strategy map.
    #[allow(clippy::too_many_lines)]
    fn calculate_strategy_for_single_distribution(
        &mut self,
        join_id: usize,
        condition_id: usize,
        join_kind: &JoinKind,
    ) -> Result<Option<Strategy>, SbroadError> {
        let (outer_id, inner_id) = if let Some(children) = self.get_relational_children(join_id)? {
            (
                *children.first().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format!(
                        "join {join_id} has no children!"
                    ))
                })?,
                *children.get(1).ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format!("join {join_id} has one child!"))
                })?,
            )
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("join {join_id} has no children!")),
            ));
        };
        let outer_dist = self.get_distribution(self.get_relational_output(outer_id)?)?;
        let inner_dist = self.get_distribution(self.get_relational_output(inner_id)?)?;

        if !matches!(
            (outer_dist, inner_dist),
            (Distribution::Single, _) | (_, Distribution::Single)
        ) {
            return Ok(None);
        }

        let mut strategy = Strategy::new(join_id);
        let eq_cols = EqualityCols::from_join_condition(self, join_id, inner_id, condition_id)?;
        let (outer_policy, inner_policy) = match (outer_dist, inner_dist) {
            (Distribution::Single, Distribution::Single) => {
                if let Some(eq_cols) = eq_cols {
                    let mut inner_targets: Vec<Target> = Vec::with_capacity(eq_cols.len());
                    let mut outer_targets: Vec<Target> = Vec::with_capacity(eq_cols.len());
                    for (i_col, o_col) in eq_cols.iter() {
                        inner_targets.push(Target::Reference(*i_col));
                        outer_targets.push(Target::Reference(*o_col));
                    }
                    let inner_policy = MotionPolicy::Segment(MotionKey {
                        targets: inner_targets,
                    });
                    let outer_policy = MotionPolicy::Segment(MotionKey {
                        targets: outer_targets,
                    });
                    (outer_policy, inner_policy)
                } else {
                    // Note: it is important that outer policy is Segment
                    // in case it is left join, we can't broadcast the left table
                    // for more details: https://git.picodata.io/picodata/picodata/sbroad/-/issues/248
                    let outer_policy = MotionPolicy::Segment(MotionKey {
                        // we can choose any distribution columns here
                        targets: vec![Target::Reference(0)],
                    });
                    let inner_policy = MotionPolicy::Full;
                    (outer_policy, inner_policy)
                }
            }
            (Distribution::Single, Distribution::Segment { keys }) => {
                Self::compute_policies_for_segment_vs_single(
                    eq_cols.as_ref(),
                    keys,
                    &JoinChild::Inner,
                    join_kind,
                )
            }
            (Distribution::Segment { keys }, Distribution::Single) => {
                Self::compute_policies_for_segment_vs_single(
                    eq_cols.as_ref(),
                    keys,
                    &JoinChild::Outer,
                    join_kind,
                )
            }
            (Distribution::Replicated | Distribution::Any, Distribution::Single) => {
                (MotionPolicy::None, MotionPolicy::Full)
            }
            (Distribution::Single, Distribution::Replicated | Distribution::Any) => {
                if let JoinKind::LeftOuter = join_kind {
                    // outer table can't be safely broadcasted in case of LeftJoin see
                    // https://git.picodata.io/picodata/picodata/sbroad/-/issues/248
                    (
                        MotionPolicy::Segment(MotionKey {
                            // we can choose any distribution columns here
                            targets: vec![Target::Reference(0)],
                        }),
                        MotionPolicy::Full,
                    )
                } else {
                    (MotionPolicy::Full, MotionPolicy::None)
                }
            }
            // above we checked that at least one child has Distribution::Single
            (_, _) => return Err(SbroadError::Invalid(Entity::Distribution, None)),
        };
        strategy.add_child(outer_id, outer_policy, Program::new());
        strategy.add_child(inner_id, inner_policy, Program::new());

        let subqueries = self
            .get_relational_children(join_id)?
            .ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(format!("join {join_id} has no children!"))
            })?
            .split_at(2)
            .1;
        for sq in subqueries {
            // todo: improve subqueries motions
            strategy.add_child(*sq, MotionPolicy::Full, Program::new());
        }
        Ok(Some(strategy))
    }

    fn resolve_delete_conflicts(&mut self, rel_id: usize) -> Result<Strategy, SbroadError> {
        let mut map = Strategy::new(rel_id);
        let child_id = self.delete_child_id(rel_id)?;
        let space = self.delete_table(rel_id)?;
        let pk_len = space.primary_key.positions.len();
        if pk_len == 0 {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "empty primary key for space {}",
                space.name()
            )));
        }
        // We expect that the columns in the child projection of the DELETE operator
        let pk_pos: Vec<usize> = (0..pk_len).collect();

        // Mark primary keys in the motion's virtual table.
        let program = vec![MotionOpcode::PrimaryKey(pk_pos)];

        // Delete node alway produce a local segment policy
        // (i.e. materialization without bucket calculation).
        map.add_child(child_id, MotionPolicy::Local, program);
        Ok(map)
    }

    fn resolve_insert_conflicts(&mut self, rel_id: usize) -> Result<Strategy, SbroadError> {
        let mut map = Strategy::new(rel_id);
        let motion_key = self.insert_child_motion_key(rel_id)?;
        let child_id = self.insert_child_id(rel_id)?;
        let child_output_id = self.get_relation_node(child_id)?.output();
        let child_dist = self.get_distribution(child_output_id)?;

        // Check that we can make a local segment motion.
        if let Distribution::Segment { keys } = child_dist {
            for key in keys.iter() {
                let insert_mkey = MotionKey::from(key);
                if insert_mkey == motion_key {
                    map.add_child(
                        child_id,
                        MotionPolicy::LocalSegment(motion_key),
                        Program::new(),
                    );
                    return Ok(map);
                }
            }
        }
        if let Relational::Values { .. } = self.get_relation_node(child_id)? {
            if let Distribution::Replicated = child_dist {
                map.add_child(
                    child_id,
                    MotionPolicy::LocalSegment(motion_key),
                    Program::new(),
                );
                return Ok(map);
            }
        }

        map.add_child(child_id, MotionPolicy::Segment(motion_key), Program::new());

        Ok(map)
    }

    #[allow(clippy::too_many_lines)]
    fn resolve_except_conflicts(&mut self, rel_id: usize) -> Result<Strategy, SbroadError> {
        let mut map = Strategy::new(rel_id);
        match self.get_relation_node(rel_id)? {
            Relational::Except { children, .. } => {
                if let (Some(left), Some(right), None) =
                    (children.first(), children.get(1), children.get(2))
                {
                    let left_output_id = self.get_relation_node(*left)?.output();
                    let right_output_id = self.get_relation_node(*right)?.output();
                    let left_output_row =
                        self.get_expression_node(left_output_id)?.get_row_list()?;
                    let right_output_row =
                        self.get_expression_node(right_output_id)?.get_row_list()?;
                    if left_output_row.len() != right_output_row.len() {
                        return Err(SbroadError::UnexpectedNumberOfValues(format!(
                            "Except node children have different row lengths: left {}, right {}",
                            left_output_row.len(),
                            right_output_row.len()
                        )));
                    }
                    let left_dist = self.get_distribution(left_output_id)?;
                    let right_dist = self.get_distribution(right_output_id)?;
                    match left_dist {
                        Distribution::Segment {
                            keys: left_keys, ..
                        } => {
                            if let Distribution::Segment {
                                keys: right_keys, ..
                            } = right_dist
                            {
                                // Distribution key sets have common keys, no need for the data motion.
                                if right_keys.intersection(left_keys).iter().next().is_some() {
                                    return Ok(map);
                                }
                            }
                            let key = left_keys.iter().next().ok_or_else(|| SbroadError::Invalid(
                                Entity::Distribution,
                                Some("left child's segment distribution is invalid: no keys found in the set".into())
                            ))?;
                            map.add_child(
                                *right,
                                MotionPolicy::Segment(key.into()),
                                Program::new(),
                            );
                        }
                        Distribution::Single => {
                            match right_dist {
                                Distribution::Segment { keys: right_keys } => {
                                    map.add_child(
                                        *left,
                                        MotionPolicy::Segment(MotionKey::from(
                                            right_keys.iter().next().ok_or_else(|| {
                                                SbroadError::Invalid(
                                                    Entity::Distribution,
                                                    Some(format!(
                                                        "{} {} {right}",
                                                        "Segment distribution with no keys.",
                                                        "Except right child:"
                                                    )),
                                                )
                                            })?,
                                        )),
                                        Program::new(),
                                    );
                                    map.add_child(*right, MotionPolicy::None, Program::new());
                                }
                                Distribution::Single => {
                                    // we could redistribute both children by any combination of columns,
                                    // first column is used for simplicity
                                    map.add_child(
                                        *left,
                                        MotionPolicy::Segment(MotionKey {
                                            targets: vec![Target::Reference(0)],
                                        }),
                                        Program::new(),
                                    );
                                    map.add_child(
                                        *right,
                                        MotionPolicy::Segment(MotionKey {
                                            targets: vec![Target::Reference(0)],
                                        }),
                                        Program::new(),
                                    );
                                }
                                _ => {
                                    // right child must to be broadcasted to each node
                                    map.add_child(
                                        *left,
                                        MotionPolicy::Segment(MotionKey {
                                            targets: vec![Target::Reference(0)], // any combination of columns would suffice
                                        }),
                                        Program::new(),
                                    );
                                    map.add_child(*right, MotionPolicy::Full, Program::new());
                                }
                            }
                        }
                        _ => {
                            map.add_child(*right, MotionPolicy::Full, Program::new());
                        }
                    }
                    return Ok(map);
                }
                Err(SbroadError::UnexpectedNumberOfValues(
                    "Except node doesn't have exactly two children.".into(),
                ))
            }
            _ => Err(SbroadError::Invalid(
                Entity::Relational,
                Some("expected Except node".into()),
            )),
        }
    }

    fn resolve_union_conflicts(&mut self, rel_id: usize) -> Result<Strategy, SbroadError> {
        let mut map = Strategy::new(rel_id);
        match self.get_relation_node(rel_id)? {
            Relational::UnionAll { children, .. } => {
                if let (Some(left), Some(right), None) =
                    (children.first(), children.get(1), children.get(2))
                {
                    let left_output_id = self.get_relation_node(*left)?.output();
                    let right_output_id = self.get_relation_node(*right)?.output();
                    let left_output_row =
                        self.get_expression_node(left_output_id)?.get_row_list()?;
                    let right_output_row =
                        self.get_expression_node(right_output_id)?.get_row_list()?;
                    if left_output_row.len() != right_output_row.len() {
                        return Err(SbroadError::UnexpectedNumberOfValues(format!(
                            "Except node children have different row lengths: left {}, right {}",
                            left_output_row.len(),
                            right_output_row.len()
                        )));
                    }
                    let left_dist = self.get_distribution(left_output_id)?;
                    let right_dist = self.get_distribution(right_output_id)?;
                    if let Distribution::Single = left_dist {
                        map.add_child(
                            *left,
                            MotionPolicy::Segment(MotionKey {
                                targets: vec![Target::Reference(0)],
                            }),
                            Program::new(),
                        );
                    }
                    if let Distribution::Single = right_dist {
                        map.add_child(
                            *right,
                            MotionPolicy::Segment(MotionKey {
                                targets: vec![Target::Reference(0)],
                            }),
                            Program::new(),
                        );
                    }
                    return Ok(map);
                }
                Err(SbroadError::UnexpectedNumberOfValues(
                    "UnionAll node doesn't have exactly two children.".into(),
                ))
            }
            _ => Err(SbroadError::Invalid(
                Entity::Relational,
                Some("expected UnionAll node".into()),
            )),
        }
    }

    /// Add motion nodes to the plan tree.
    ///
    /// # Errors
    /// - failed to get relational nodes (plan is invalid?)
    /// - failed to resolve distribution conflicts
    /// - failed to set distribution
    #[otm_child_span("plan.transformation.add_motions")]
    pub fn add_motions(&mut self) -> Result<(), SbroadError> {
        let nodes = self.get_relational_nodes_dfs_post()?;
        for id in &nodes {
            match self.get_relation_node(*id)?.clone() {
                // At the moment our grammar and IR constructors
                // don't allow projection and values row with
                // sub queries.
                Relational::ScanRelation { output, .. }
                | Relational::ScanSubQuery { output, .. }
                | Relational::Values { output, .. }
                | Relational::GroupBy { output, .. }
                | Relational::Having { output, .. }
                | Relational::ValuesRow { output, .. } => {
                    self.set_distribution(output)?;
                }
                Relational::Projection {
                    output: proj_output_id,
                    ..
                } => {
                    if !self.add_two_stage_aggregation(*id)? {
                        self.set_distribution(proj_output_id)?;
                    }
                }
                Relational::Motion { .. } => {
                    // We can apply this transformation only once,
                    // i.e. to the plan without any motion nodes.
                    return Err(SbroadError::DuplicatedValue(String::from(
                        "IR already has Motion nodes.",
                    )));
                }
                Relational::Selection { output, filter, .. } => {
                    self.set_distribution(output)?;
                    let strategy = self.resolve_sub_query_conflicts(*id, filter)?;
                    self.create_motion_nodes(strategy)?;
                }
                Relational::Join {
                    output,
                    condition,
                    kind,
                    ..
                } => {
                    self.resolve_join_conflicts(*id, condition, &kind)?;
                    self.set_distribution(output)?;
                }
                Relational::Delete { .. } => {
                    let strategy = self.resolve_delete_conflicts(*id)?;
                    self.create_motion_nodes(strategy)?;
                }
                Relational::Insert { .. } => {
                    // Insert output tuple already has relation's distribution.
                    let strategy = self.resolve_insert_conflicts(*id)?;
                    self.create_motion_nodes(strategy)?;
                }
                Relational::Except { output, .. } => {
                    let strategy = self.resolve_except_conflicts(*id)?;
                    self.create_motion_nodes(strategy)?;
                    self.set_distribution(output)?;
                }
                Relational::UnionAll { output, .. } => {
                    let strategy = self.resolve_union_conflicts(*id)?;
                    self.create_motion_nodes(strategy)?;
                    self.set_distribution(output)?;
                }
            }
        }

        // Gather motions (revert levels in bft)
        let mut motions: Vec<Vec<usize>> = Vec::new();
        let top = self.get_top()?;
        let mut bft_tree = BreadthFirst::with_capacity(
            |node| self.nodes.rel_iter(node),
            REL_CAPACITY,
            REL_CAPACITY,
        );
        let mut map: HashMap<usize, usize> = HashMap::new();
        let mut max_level: usize = 0;
        for (level, id) in bft_tree.iter(top) {
            if let Node::Relational(Relational::Motion { .. }) = self.get_node(id)? {
                let key: usize = match map.entry(level) {
                    Entry::Occupied(o) => *o.into_mut(),
                    Entry::Vacant(v) => {
                        let old_level = max_level;
                        v.insert(old_level);
                        max_level += 1;
                        old_level
                    }
                };
                match motions.get_mut(key) {
                    Some(list) => list.push(id),
                    None => motions.push(vec![id]),
                }
            }
        }
        if !motions.is_empty() {
            self.set_slices(motions.into_iter().rev().collect());
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod tests;
