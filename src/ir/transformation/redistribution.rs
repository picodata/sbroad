//! Resolve distribution conflicts and insert motion nodes to IR.

use std::cmp::Ordering;
use std::collections::{hash_map::Entry, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use traversal::{Bft, DftPost};

use crate::errors::QueryPlannerError;
use crate::ir::distribution::{Distribution, Key};
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational};
use crate::ir::relation::Column;
use crate::ir::value::Value;
use crate::ir::{Node, Plan};

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
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum MotionPolicy {
    /// Move all data.
    Full,
    /// Move only a segment of data according to the motion key.
    Segment(MotionKey),
    /// No need to move data.
    Local,
}

/// Determine what portion of data to generate during motion.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum DataGeneration {
    /// Nothing to generate.
    None,
    /// Generate a sharding column (`bucket_id` in terms of Tarantool).
    ShardingColumn,
}

struct BoolOp {
    left: usize,
    op: Bool,
    right: usize,
}

impl BoolOp {
    fn from_expr(plan: &Plan, expr_id: usize) -> Result<Self, QueryPlannerError> {
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
            Err(QueryPlannerError::InvalidBool)
        }
    }
}

type Strategy = HashMap<usize, (MotionPolicy, DataGeneration)>;

/// Choose a policy for the inner join child (a policy with the largest motion of data wins).
fn join_policy_for_or(left_policy: &MotionPolicy, right_policy: &MotionPolicy) -> MotionPolicy {
    match (left_policy, right_policy) {
        (MotionPolicy::Full, _) | (_, MotionPolicy::Full) => MotionPolicy::Full,
        (MotionPolicy::Local, _) => right_policy.clone(),
        (_, MotionPolicy::Local) => left_policy.clone(),
        (MotionPolicy::Segment(key_left), MotionPolicy::Segment(key_right)) => {
            if key_left == key_right {
                left_policy.clone()
            } else {
                MotionPolicy::Full
            }
        }
    }
}

/// Choose a policy for the inner join child (a policy with the smallest motion of data wins).
fn join_policy_for_and(left_policy: &MotionPolicy, right_policy: &MotionPolicy) -> MotionPolicy {
    match (left_policy, right_policy) {
        (MotionPolicy::Full, _) => right_policy.clone(),
        (_, MotionPolicy::Full) => left_policy.clone(),
        (MotionPolicy::Local, _) | (_, MotionPolicy::Local) => MotionPolicy::Local,
        (MotionPolicy::Segment(key_left), MotionPolicy::Segment(key_right)) => {
            if key_left == key_right {
                left_policy.clone()
            } else {
                MotionPolicy::Full
            }
        }
    }
}

impl Plan {
    /// Get a list of relational nodes in a DFS post order.
    ///
    /// # Errors
    /// - plan doesn't contain the top node
    fn get_relational_nodes_dfs_post(&self) -> Result<Vec<usize>, QueryPlannerError> {
        let top = self.get_top()?;
        let post_tree = DftPost::new(&top, |node| self.nodes.rel_iter(node));
        let nodes: Vec<usize> = post_tree.map(|(_, id)| *id).collect();
        Ok(nodes)
    }

    /// Get boolean expressions with row children in the sub-tree.
    ///
    /// # Errors
    /// - some of the expression nodes are invalid
    pub(crate) fn get_bool_nodes_with_row_children(
        &self,
        top: usize,
    ) -> Result<Vec<usize>, QueryPlannerError> {
        let mut nodes: Vec<usize> = Vec::new();

        let post_tree = DftPost::new(&top, |node| self.nodes.expr_iter(node, false));
        for (_, node) in post_tree {
            // Append only booleans with row children.
            if let Node::Expression(Expression::Bool { left, right, .. }) = self.get_node(*node)? {
                let left_is_row = matches!(
                    self.get_node(*left)?,
                    Node::Expression(Expression::Row { .. })
                );
                let right_is_row = matches!(
                    self.get_node(*right)?,
                    Node::Expression(Expression::Row { .. })
                );
                if left_is_row && right_is_row {
                    nodes.push(*node);
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
    pub fn get_sub_query_from_row_node(
        &self,
        row_id: usize,
    ) -> Result<Option<usize>, QueryPlannerError> {
        let rel_ids = self.get_relational_from_row_nodes(row_id)?;
        self.get_sub_query_among_rel_nodes(&rel_ids)
    }

    /// Get a single sub-query from among the list of relational nodes.
    ///
    /// # Errors
    /// - Some of the nodes are not relational
    /// - There are more than one sub-query
    pub fn get_sub_query_among_rel_nodes(
        &self,
        rel_nodes: &HashSet<usize>,
    ) -> Result<Option<usize>, QueryPlannerError> {
        let mut sq_set: HashSet<usize> = HashSet::new();
        for rel_id in rel_nodes {
            if let Node::Relational(Relational::ScanSubQuery { .. }) = self.get_node(*rel_id)? {
                sq_set.insert(*rel_id);
            }
        }
        match sq_set.len().cmp(&1) {
            Ordering::Equal => sq_set.iter().next().map_or_else(
                || {
                    Err(QueryPlannerError::CustomError(format!(
                        "Failed to get the first sub-query node from the list of relational nodes: {:?}.",
                        rel_nodes
                    )))
                },
                |sq_id| Ok(Some(*sq_id)),
            ),
            Ordering::Less => Ok(None),
            Ordering::Greater => Err(QueryPlannerError::CustomError(format!(
                "Found multiple sub-queries in a list of relational nodes: {:?}.",
                rel_nodes
            ))),
        }
    }

    /// Extract a motion node id from the row node
    ///
    /// # Errors
    /// - Row node is not of a row type
    /// - There are more than one motion nodes in the row node
    pub fn get_motion_from_row(&self, node_id: usize) -> Result<Option<usize>, QueryPlannerError> {
        let rel_nodes = self.get_relational_from_row_nodes(node_id)?;
        self.get_motion_among_rel_nodes(&rel_nodes)
    }

    /// Extract motion node id from row node
    ///
    /// # Errors
    /// - Some of the nodes are not relational
    /// - There are more than one motion node
    pub fn get_motion_among_rel_nodes(
        &self,
        rel_nodes: &HashSet<usize>,
    ) -> Result<Option<usize>, QueryPlannerError> {
        let mut motion_set: HashSet<usize> = HashSet::new();

        for child in rel_nodes {
            if self.get_relation_node(*child)?.is_motion() {
                motion_set.insert(*child);
            }
        }

        match motion_set.len().cmp(&1) {
            Ordering::Equal => {
                let motion_id = motion_set.iter().next().ok_or_else(|| {
                    QueryPlannerError::CustomError(
                        "Failed to get the first motion node from the set.".into(),
                    )
                })?;
                Ok(Some(*motion_id))
            }
            Ordering::Less => Ok(None),
            Ordering::Greater => Err(QueryPlannerError::CustomError(
                "Node must contain only a single motion".into(),
            )),
        }
    }

    /// Choose a `MotionPolicy` strategy for the inner row.
    ///
    /// # Errors
    /// - nodes are not rows
    /// - uninitialized distribution for some row
    fn choose_strategy_for_inner_sq(
        &self,
        outer_id: usize,
        inner_id: usize,
        op: &Bool,
    ) -> Result<MotionPolicy, QueryPlannerError> {
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
                    if keys_outer.intersection(keys_inner).next().is_some() {
                        return Ok(MotionPolicy::Local);
                    }
                }
                // Redistribute the inner tuples using the first key from the outer tuple.
                return keys_outer.iter().next().map_or_else(
                    || {
                        Err(QueryPlannerError::CustomError(String::from(
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
    fn create_motion_nodes(
        &mut self,
        rel_id: usize,
        strategy: &Strategy,
    ) -> Result<(), QueryPlannerError> {
        let children: Vec<usize> = if let Some(children) = self.get_relational_children(rel_id)? {
            children.to_vec()
        } else {
            return Err(QueryPlannerError::CustomError(String::from(
                "Trying to add motions under the leaf relational node.",
            )));
        };

        // Check that all children we need to add motions exist in the current relational node.
        let children_set: HashSet<usize> = children.iter().copied().collect();
        if let false = strategy
            .iter()
            .all(|(node, _)| children_set.get(node).is_some())
        {
            return Err(QueryPlannerError::CustomError(String::from(
                "Trying to add motions for non-existing children in relational node.",
            )));
        }

        // Add motions.
        let mut children_with_motions: Vec<usize> = Vec::new();
        for child in children {
            if let Some((policy, data_gen)) = strategy.get(&child) {
                if let MotionPolicy::Local = policy {
                    children_with_motions.push(child);
                } else {
                    children_with_motions.push(self.add_motion(child, policy, data_gen)?);
                }
            } else {
                children_with_motions.push(child);
            }
        }
        self.set_relational_children(rel_id, children_with_motions)?;
        Ok(())
    }

    fn get_additional_sq(
        &self,
        rel_id: usize,
        row_id: usize,
    ) -> Result<Option<usize>, QueryPlannerError> {
        if self.get_expression_node(row_id)?.is_row() {
            if let Some(sq_id) = self.get_sub_query_from_row_node(row_id)? {
                if self.is_additional_child_of_rel(rel_id, sq_id)? {
                    return Ok(Some(sq_id));
                }
            }
        }
        Ok(None)
    }

    fn get_sq_node_strategies(
        &self,
        rel_id: usize,
        node_id: usize,
    ) -> Result<Vec<(usize, MotionPolicy)>, QueryPlannerError> {
        let mut strategies: Vec<(usize, MotionPolicy)> = Vec::new();
        let bool_op = BoolOp::from_expr(self, node_id)?;
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
                            self.choose_strategy_for_inner_sq(
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
                        self.choose_strategy_for_inner_sq(
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

    /// Resolve sub-query conflicts with motion policies.
    fn resolve_sub_query_conflicts(
        &mut self,
        rel_id: usize,
        expr_id: usize,
    ) -> Result<Strategy, QueryPlannerError> {
        let nodes = self.get_bool_nodes_with_row_children(expr_id)?;
        for node in &nodes {
            let bool_op = BoolOp::from_expr(self, *node)?;
            self.set_distribution(bool_op.left)?;
            self.set_distribution(bool_op.right)?;
        }

        let mut strategy: Strategy = HashMap::new();
        for node in &nodes {
            let strategies = self.get_sq_node_strategies(rel_id, *node)?;
            for (id, policy) in strategies {
                strategy.insert(id, (policy, DataGeneration::None));
            }
        }
        Ok(strategy)
    }

    /// Get the children of a join node (outer, inner and sub-queries).
    ///
    /// # Errors
    /// - If the node is not a join node.
    /// - Join node has no children.
    fn get_join_children(&self, join_id: usize) -> Result<&[usize], QueryPlannerError> {
        let join = self.get_relation_node(join_id)?;
        if let Relational::InnerJoin { .. } = join {
        } else {
            return Err(QueryPlannerError::CustomError(
                "Join node is not an inner join.".into(),
            ));
        }
        let children = join.children().ok_or_else(|| {
            QueryPlannerError::CustomError("Join node doesn't have any children.".into())
        })?;
        Ok(children)
    }

    /// Detect join child from the position map corresponding to the distribution key.
    fn get_join_child_by_key(
        &self,
        key: &Key,
        row_map: &HashMap<usize, usize>,
        join_children: &[usize],
    ) -> Result<usize, QueryPlannerError> {
        let mut children_set: HashSet<usize> = HashSet::new();
        for pos in &key.positions {
            let column_id = *row_map.get(pos).ok_or_else(|| {
                QueryPlannerError::CustomError(format!(
                    "Column {} not found in row map {:?}.",
                    pos, row_map
                ))
            })?;
            if let Expression::Reference { targets, .. } = self.get_expression_node(column_id)? {
                if let Some(targets) = targets {
                    for target in targets {
                        let child_id = *join_children.get(*target).ok_or_else(|| {
                            QueryPlannerError::CustomError(format!(
                                "Target {} not found in join children {:?}.",
                                target, join_children
                            ))
                        })?;
                        children_set.insert(child_id);
                    }
                }
            } else {
                return Err(QueryPlannerError::CustomError(
                    "Row column is not a reference.".into(),
                ));
            }
        }
        if children_set.len() > 1 {
            return Err(QueryPlannerError::CustomError(
                "Distribution key in the join condition has more than one child.".into(),
            ));
        }
        children_set.iter().next().copied().ok_or_else(|| {
            QueryPlannerError::CustomError(
                "Distribution key in the join condition has no children.".into(),
            )
        })
    }

    /// Builds a row column map, where every column in the row is mapped by its position.
    ///
    /// # Errors
    /// - If the node is not a row node.
    fn build_row_map(&self, row_id: usize) -> Result<HashMap<usize, usize>, QueryPlannerError> {
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
    ) -> Result<(Vec<Key>, Vec<Key>), QueryPlannerError> {
        let mut outer_keys: Vec<Key> = Vec::new();
        let mut inner_keys: Vec<Key> = Vec::new();

        let children = self.get_join_children(join_id)?;
        let outer_child = *children.get(0).ok_or_else(|| {
            QueryPlannerError::CustomError("Join node doesn't have an outer child.".into())
        })?;
        let inner_child = *children.get(1).ok_or_else(|| {
            QueryPlannerError::CustomError("Join node doesn't have an inner child.".into())
        })?;

        for key in keys {
            let child = self.get_join_child_by_key(key, row_map, children)?;
            if child == outer_child {
                outer_keys.push(key.clone());
            } else if child == inner_child {
                inner_keys.push(key.clone());
            } else {
                // It can be only a sub-query, but we have already processed it.
                return Err(QueryPlannerError::CustomError(
                    "Distribution key doesn't correspond  to inner or outer join children.".into(),
                ));
            }
        }
        Ok((outer_keys, inner_keys))
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
    ) -> Result<MotionPolicy, QueryPlannerError> {
        let left_dist = self.get_distribution(left_row_id)?.clone();
        let right_dist = self.get_distribution(right_row_id)?.clone();

        let get_policy_for_one_side_segment = |row_map: &HashMap<usize, usize>,
                                               keys_set: &HashSet<Key>|
         -> Result<MotionPolicy, QueryPlannerError> {
            let keys = keys_set.iter().map(Clone::clone).collect::<Vec<_>>();
            let (outer_keys, _) =
                self.split_join_keys_to_inner_and_outer(join_id, &keys, row_map)?;
            if let Some(outer_key) = outer_keys.get(0) {
                Ok(MotionPolicy::Segment(outer_key.into()))
            } else {
                Ok(MotionPolicy::Full)
            }
        };

        match (left_dist, right_dist) {
            (
                Distribution::Segment {
                    keys: keys_left_set,
                },
                Distribution::Segment {
                    keys: keys_right_set,
                },
            ) => {
                let mut first_outer_key = None;
                let keys_left = keys_left_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let row_map_left = self.build_row_map(left_row_id)?;
                let keys_right = keys_right_set.iter().map(Clone::clone).collect::<Vec<_>>();
                let row_map_right = self.build_row_map(right_row_id)?;
                let (left_outer_keys, left_inner_keys) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_left, &row_map_left)?;
                let (right_outer_keys, right_inner_keys) =
                    self.split_join_keys_to_inner_and_outer(join_id, &keys_right, &row_map_right)?;
                let pairs = &[
                    (left_outer_keys, right_inner_keys),
                    (right_outer_keys, left_inner_keys),
                ];
                for (outer_keys, inner_keys) in pairs {
                    for outer_key in outer_keys {
                        if first_outer_key.is_none() {
                            first_outer_key = Some(outer_key.clone());
                        }
                        for inner_key in inner_keys {
                            if outer_key == inner_key {
                                return Ok(MotionPolicy::Local);
                            }
                        }
                    }
                }
                if let Some(outer_key) = first_outer_key {
                    // Choose the first of the outer keys for the segment motion policy.
                    Ok(MotionPolicy::Segment(outer_key.into()))
                } else {
                    Ok(MotionPolicy::Full)
                }
            }
            (Distribution::Segment { keys: keys_set }, _) => {
                let row_map = self.build_row_map(left_row_id)?;
                get_policy_for_one_side_segment(&row_map, &keys_set)
            }
            (_, Distribution::Segment { keys: keys_set }) => {
                let row_map = self.build_row_map(right_row_id)?;
                get_policy_for_one_side_segment(&row_map, &keys_set)
            }
            _ => Ok(MotionPolicy::Full),
        }
    }

    /// Derive the motion policy for the inner child and sub-queries in the join node.
    ///
    /// # Errors
    /// - Failed to set row distribution in the join condition tree.
    fn resolve_join_conflicts(
        &mut self,
        rel_id: usize,
        expr_id: usize,
    ) -> Result<Strategy, QueryPlannerError> {
        // First, we need to set the motion policy for each boolean expression in the join condition.
        let nodes = self.get_bool_nodes_with_row_children(expr_id)?;
        for node in &nodes {
            let bool_op = BoolOp::from_expr(self, *node)?;
            self.set_distribution(bool_op.left)?;
            self.set_distribution(bool_op.right)?;
        }

        // Init the strategy (motion policy map) for all the join children except the outer child.
        let join_children = self.get_join_children(rel_id)?;
        let mut strategy: Strategy = HashMap::new();
        if let Some((_, children)) = join_children.split_first() {
            for child_id in children {
                strategy.insert(*child_id, (MotionPolicy::Full, DataGeneration::None));
            }
        } else {
            return Err(QueryPlannerError::CustomError(
                "Join node doesn't have any children.".into(),
            ));
        }

        // Let's improve the full motion policy for the join children (sub-queries and the inner child).
        let inner_child = *join_children.get(1).ok_or_else(|| {
            QueryPlannerError::CustomError("Join node doesn't have an inner child.".into())
        })?;
        let mut inner_map: HashMap<usize, MotionPolicy> = HashMap::new();
        let mut new_inner_policy = MotionPolicy::Full;
        let expr_tree = DftPost::new(&expr_id, |node| self.nodes.expr_iter(node, true));
        for (_, node_id) in expr_tree {
            let expr = self.get_expression_node(*node_id)?;
            let bool_op = if let Expression::Bool { .. } = expr {
                BoolOp::from_expr(self, *node_id)?
            } else {
                continue;
            };

            // Try to improve full motion policy in the sub-queries.
            // We don't influence the inner child here, so the inner map is empty
            // for the current node id.
            let sq_strategies = self.get_sq_node_strategies(rel_id, *node_id)?;
            let sq_strategies_len = sq_strategies.len();
            for (id, policy) in sq_strategies {
                strategy.insert(id, (policy, DataGeneration::None));
            }
            if sq_strategies_len > 0 {
                continue;
            }

            // Ok, we don't have any sub-queries.
            // Lets try to improve the motion policy for the inner join child.
            let left_expr = self.get_expression_node(bool_op.left)?;
            let right_expr = self.get_expression_node(bool_op.right)?;
            new_inner_policy = match (left_expr, right_expr) {
                (Expression::Bool { .. }, Expression::Bool { .. }) => {
                    let left_policy = inner_map
                        .get(&bool_op.left)
                        .cloned()
                        .unwrap_or(MotionPolicy::Full);
                    let right_policy = inner_map
                        .get(&bool_op.right)
                        .cloned()
                        .unwrap_or(MotionPolicy::Full);
                    match bool_op.op {
                        Bool::And => join_policy_for_and(&left_policy, &right_policy),
                        Bool::Or => join_policy_for_or(&left_policy, &right_policy),
                        _ => {
                            return Err(QueryPlannerError::CustomError(
                                "Unsupported boolean operation".into(),
                            ))
                        }
                    }
                }
                (Expression::Row { .. }, Expression::Row { .. }) => {
                    match bool_op.op {
                        Bool::Eq | Bool::In => {
                            self.join_policy_for_eq(rel_id, bool_op.left, bool_op.right)?
                        }
                        Bool::NotEq | Bool::Gt | Bool::GtEq | Bool::Lt | Bool::LtEq => {
                            MotionPolicy::Full
                        }
                        Bool::And | Bool::Or => {
                            // "a and 1" or "a or 1" expressions make no sense.
                            return Err(QueryPlannerError::CustomError(
                                "Unsupported boolean operation".into(),
                            ));
                        }
                    }
                }
                (Expression::Constant { .. }, Expression::Bool { .. }) => inner_map
                    .get(&bool_op.right)
                    .cloned()
                    .unwrap_or(MotionPolicy::Full),
                (Expression::Bool { .. }, Expression::Constant { .. }) => inner_map
                    .get(&bool_op.left)
                    .cloned()
                    .unwrap_or(MotionPolicy::Full),
                _ => {
                    return Err(QueryPlannerError::CustomError(
                        "Unsupported boolean operation".into(),
                    ))
                }
            };
            inner_map.insert(*node_id, new_inner_policy.clone());
        }
        strategy.insert(inner_child, (new_inner_policy, DataGeneration::None));
        Ok(strategy)
    }

    fn resolve_insert_conflicts(&mut self, rel_id: usize) -> Result<Strategy, QueryPlannerError> {
        let mut map: Strategy = HashMap::new();
        match self.get_relation_node(rel_id)? {
            Relational::Insert {
                relation,
                columns,
                children,
                ..
            } => {
                let child: usize = if let (Some(child), None) = (children.get(0), children.get(1)) {
                    *child
                } else {
                    return Err(QueryPlannerError::CustomError(
                        "Insert node doesn't have exactly a single child.".into(),
                    ));
                };
                let child_rel = self.get_relation_node(child)?;
                let child_row = self.get_expression_node(child_rel.output())?;
                let (list, distribution) = if let Expression::Row {
                    list, distribution, ..
                } = child_row
                {
                    (list, distribution)
                } else {
                    return Err(QueryPlannerError::CustomError(
                        "Insert child node has an invalid node instead of the output row".into(),
                    ));
                };
                if list.len() != columns.len() {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Insert node expects {} columns instead of {}",
                        list.len(),
                        columns.len()
                    )));
                }
                let columns_map: HashMap<usize, usize> = columns
                    .iter()
                    .enumerate()
                    .map(|(pos, id)| (*id, pos))
                    .collect::<HashMap<_, _>>();
                let mut motion_key: MotionKey = MotionKey::new();
                let rel = self.get_relation(relation).ok_or_else(|| {
                    QueryPlannerError::CustomError(format!("Relation {} not found", relation))
                })?;
                for pos in &rel.key.positions {
                    if let Some(child_pos) = columns_map.get(pos) {
                        // We can use insert column's position instead of
                        // the position in the child node as their lengths
                        // are the same.
                        motion_key.targets.push(Target::Reference(*child_pos));
                    } else {
                        // Check that the column exists on the requested position.
                        rel.columns.get(*pos).ok_or_else(|| {
                            QueryPlannerError::CustomError(format!(
                                "Column {} not found in relation {}",
                                pos, relation
                            ))
                        })?;
                        // We need a default value for the key column.
                        motion_key
                            .targets
                            .push(Target::Value(Column::default_value()));
                    }
                }
                if distribution.is_none() {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Insert node child {} has no distribution",
                        child
                    )));
                }

                // At the moment we always add a segment motion policy under the
                // insertion node, even if the the data can be transferred locally.
                // The reason is in the sharding column (`bucket_id` field in terms
                // of Tarantool) that should be recalculated for each row. At the
                // moment we can perform calculations only on the coordinator node,
                // so we need to always deliver the data to coordinator's virtual
                // table.
                map.insert(
                    child,
                    (
                        MotionPolicy::Segment(motion_key),
                        DataGeneration::ShardingColumn,
                    ),
                );
            }
            _ => {
                return Err(QueryPlannerError::CustomError(
                    "Expected insert node".into(),
                ))
            }
        }

        // We also need to add a sharding column to the end of the insert
        // column list.
        let sharding_pos =
            if let Relational::Insert { relation, .. } = self.get_relation_node(rel_id)? {
                let rel = self.get_relation(relation).ok_or_else(|| {
                    QueryPlannerError::CustomError(format!("Relation {} not found", relation))
                })?;
                rel.get_sharding_column_position()?
            } else {
                return Err(QueryPlannerError::CustomError(
                    "Expected insert node".into(),
                ));
            };
        if let Relational::Insert {
            ref mut columns, ..
        } = self.get_mut_relation_node(rel_id)?
        {
            columns.push(sharding_pos);
        }

        Ok(map)
    }

    fn resolve_except_conflicts(&mut self, rel_id: usize) -> Result<Strategy, QueryPlannerError> {
        let mut map: Strategy = HashMap::new();
        match self.get_relation_node(rel_id)? {
            Relational::Except { children, .. } => {
                if let (Some(left), Some(right), None) =
                    (children.get(0), children.get(1), children.get(2))
                {
                    let left_output_id = self.get_relation_node(*left)?.output();
                    let right_output_id = self.get_relation_node(*right)?.output();
                    let left_output_row =
                        self.get_expression_node(left_output_id)?.get_row_list()?;
                    let right_output_row =
                        self.get_expression_node(right_output_id)?.get_row_list()?;
                    if left_output_row.len() != right_output_row.len() {
                        return Err(QueryPlannerError::CustomError(format!(
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
                                if right_keys
                                    .intersection(left_keys)
                                    .into_iter()
                                    .next()
                                    .is_some()
                                {
                                    return Ok(map);
                                }
                            }
                            let key = left_keys.iter().next().ok_or_else(|| QueryPlannerError::CustomError(
                                "Left child's segment distribution is invalid: no keys found in the set".into()
                            ))?;
                            map.insert(
                                *right,
                                (MotionPolicy::Segment(key.into()), DataGeneration::None),
                            );
                        }
                        _ => {
                            map.insert(*right, (MotionPolicy::Full, DataGeneration::None));
                        }
                    }
                    return Ok(map);
                }
                Err(QueryPlannerError::CustomError(
                    "Except node doesn't have exactly two children.".into(),
                ))
            }
            _ => Err(QueryPlannerError::CustomError(
                "Expected except node".into(),
            )),
        }
    }

    /// Add motion nodes to the plan tree.
    ///
    /// # Errors
    /// - failed to get relational nodes (plan is invalid?)
    /// - failed to resolve distribution conflicts
    /// - failed to set distribution
    pub fn add_motions(&mut self) -> Result<(), QueryPlannerError> {
        let nodes = self.get_relational_nodes_dfs_post()?;
        for id in &nodes {
            match self.get_relation_node(*id)?.clone() {
                // At the moment our grammar and IR constructors
                // don't allow projection and values row with
                // sub queries.
                Relational::Projection { output, .. }
                | Relational::ScanRelation { output, .. }
                | Relational::ScanSubQuery { output, .. }
                | Relational::UnionAll { output, .. }
                | Relational::Values { output, .. }
                | Relational::ValuesRow { output, .. } => {
                    self.set_distribution(output)?;
                }
                Relational::Motion { .. } => {
                    // We can apply this transformation only once,
                    // i.e. to the plan without any motion nodes.
                    return Err(QueryPlannerError::CustomError(String::from(
                        "IR already has Motion nodes.",
                    )));
                }
                Relational::Selection { output, filter, .. } => {
                    self.set_distribution(output)?;
                    let strategy = self.resolve_sub_query_conflicts(*id, filter)?;
                    self.create_motion_nodes(*id, &strategy)?;
                }
                Relational::InnerJoin {
                    output, condition, ..
                } => {
                    let strategy = self.resolve_join_conflicts(*id, condition)?;
                    self.create_motion_nodes(*id, &strategy)?;
                    self.set_distribution(output)?;
                }
                Relational::Insert { .. } => {
                    // Insert output tuple already has relation's distribution.
                    let strategy = self.resolve_insert_conflicts(*id)?;
                    self.create_motion_nodes(*id, &strategy)?;
                }
                Relational::Except { output, .. } => {
                    let strategy = self.resolve_except_conflicts(*id)?;
                    self.create_motion_nodes(*id, &strategy)?;
                    self.set_distribution(output)?;
                }
            }
        }

        // Gather motions (revert levels in bft)
        let mut motions: Vec<Vec<usize>> = Vec::new();
        let top = self.get_top()?;
        let bft_tree = Bft::new(&top, |node| self.nodes.rel_iter(node));
        let mut map: HashMap<usize, usize> = HashMap::new();
        let mut max_level: usize = 0;
        for (level, node) in bft_tree {
            if let Node::Relational(Relational::Motion { .. }) = self.get_node(*node)? {
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
                    Some(list) => list.push(*node),
                    None => motions.push(vec![*node]),
                }
            }
        }
        if !motions.is_empty() {
            self.set_slices(Some(motions.into_iter().rev().collect()));
        }

        Ok(())
    }
}

#[cfg(test)]
pub mod tests;
