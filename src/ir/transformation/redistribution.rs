use crate::errors::QueryPlannerError;
use crate::ir::distribution::{Distribution, Key};
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational};
use crate::ir::{Node, Plan};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use traversal::{Bft, DftPost};

/// A motion policy determinate what portion of data to move
/// between data nodes.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum MotionPolicy {
    /// Move all data.
    Full,
    /// Move only a segment of data according to the distribution key.
    Segment(Key),
    /// No need to move data.
    Local,
}
#[derive(Debug)]
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
        let mut nodes: Vec<usize> = Vec::new();

        let post_tree = DftPost::new(&top, |node| self.nodes.rel_iter(node));
        for (_, node) in post_tree {
            nodes.push(*node);
        }
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

    /// Get a sub-query from the row node (if it points to sub-query)
    ///
    /// # Errors
    /// - reference points to multiple sub-queries (invalid SQL)
    /// - `relational_map` is not initialized
    pub fn get_sub_query_from_row_node(
        &self,
        row_id: usize,
    ) -> Result<Option<usize>, QueryPlannerError> {
        let mut sq_set: HashSet<usize> = HashSet::new();
        let rel_nodes = self.get_relational_from_row_nodes(row_id)?;
        for rel_id in rel_nodes {
            if let Node::Relational(Relational::ScanSubQuery { .. }) = self.get_node(rel_id)? {
                sq_set.insert(rel_id);
            }
        }
        match sq_set.len().cmp(&1) {
            Ordering::Equal => sq_set.iter().next().map_or_else(
                || {
                    Err(QueryPlannerError::CustomError(String::from(
                        "Failed to get the first sub-query node from the set.",
                    )))
                },
                |sq_id| Ok(Some(*sq_id)),
            ),
            Ordering::Less => Ok(None),
            Ordering::Greater => Err(QueryPlannerError::InvalidSubQuery),
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
        if let Bool::Eq = op {
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
                    |key| Ok(MotionPolicy::Segment(key.clone())),
                );
            }
        }
        Ok(MotionPolicy::Full)
    }

    /// Create Motions from the strategy map.
    fn create_motion_nodes(
        &mut self,
        rel_id: usize,
        strategy: &HashMap<usize, MotionPolicy>,
    ) -> Result<(), QueryPlannerError> {
        let children = if let Some(children) = self.get_relational_children(rel_id)? {
            children
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
        for child in &children {
            if let Some(policy) = strategy.get(child) {
                if let MotionPolicy::Local = policy {
                    children_with_motions.push(*child);
                } else {
                    children_with_motions.push(self.add_motion(*child, policy)?);
                }
            } else {
                children_with_motions.push(*child);
            }
        }
        self.set_relational_children(rel_id, children_with_motions)?;
        Ok(())
    }

    /// Check that the sub-query is an additional child of the parent relational node.
    fn sub_query_is_additional_child(
        &self,
        rel_id: usize,
        sq_id: usize,
    ) -> Result<bool, QueryPlannerError> {
        let children = if let Some(children) = self.get_relational_children(rel_id)? {
            children
        } else {
            return Ok(false);
        };
        match self.get_relation_node(rel_id)? {
            Relational::Selection { .. } | Relational::Projection { .. } => {
                Ok(children.get(0) != Some(&sq_id))
            }
            Relational::InnerJoin { .. } => {
                Ok(children.get(0) != Some(&sq_id) && children.get(1) != Some(&sq_id))
            }
            _ => Err(QueryPlannerError::CustomError(String::from(
                "Trying to check if sub-query is an additional child of the wrong node.",
            ))),
        }
    }

    fn get_additional_sq(
        &self,
        rel_id: usize,
        row_id: usize,
    ) -> Result<Option<usize>, QueryPlannerError> {
        if self.get_expression_node(row_id)?.is_row() {
            if let Some(sq_id) = self.get_sub_query_from_row_node(row_id)? {
                if self.sub_query_is_additional_child(rel_id, sq_id)? {
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
    ) -> Result<HashMap<usize, MotionPolicy>, QueryPlannerError> {
        let nodes = self.get_bool_nodes_with_row_children(expr_id)?;
        for node in &nodes {
            let bool_op = BoolOp::from_expr(self, *node)?;
            self.set_distribution(bool_op.left)?;
            self.set_distribution(bool_op.right)?;
        }

        let mut strategy: HashMap<usize, MotionPolicy> = HashMap::new();
        for node in &nodes {
            let strategies = self.get_sq_node_strategies(rel_id, *node)?;
            for (id, policy) in strategies {
                strategy.insert(id, policy);
            }
        }
        Ok(strategy)
    }

    /// Get the children of a join node (outer, inner and sub-queries).
    ///
    /// # Errors
    /// - If the node is not a join node.
    /// - Join node has no children.
    fn get_join_children(&self, join_id: usize) -> Result<Vec<usize>, QueryPlannerError> {
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
        let columns = self.get_expression_node(row_id)?.extract_row_list()?;
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
            let child = self.get_join_child_by_key(key, row_map, &children)?;
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
                Ok(MotionPolicy::Segment(outer_key.clone()))
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
                    Ok(MotionPolicy::Segment(outer_key))
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
    ) -> Result<HashMap<usize, MotionPolicy>, QueryPlannerError> {
        // First, we need to set the motion policy for each boolean expression in the join condition.
        let nodes = self.get_bool_nodes_with_row_children(expr_id)?;
        for node in &nodes {
            let bool_op = BoolOp::from_expr(self, *node)?;
            self.set_distribution(bool_op.left)?;
            self.set_distribution(bool_op.right)?;
        }

        // Init the strategy (motion policy map) for all the join children except the outer child.
        let join_children = self.get_join_children(rel_id)?;
        let mut strategy: HashMap<usize, MotionPolicy> = HashMap::new();
        if let Some((_, children)) = join_children.split_first() {
            for child_id in children {
                strategy.insert(*child_id, MotionPolicy::Full);
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
                strategy.insert(id, policy);
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
        strategy.insert(inner_child, new_inner_policy);
        Ok(strategy)
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
                // At the moment our grammar and IR constructor
                // don't support projection with sub queries.
                Relational::Projection { output, .. }
                | Relational::ScanRelation { output, .. }
                | Relational::ScanSubQuery { output, .. }
                | Relational::UnionAll { output, .. } => {
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
                    self.set_distribution(output)?;
                    let strategy = self.resolve_join_conflicts(*id, condition)?;
                    self.create_motion_nodes(*id, &strategy)?;
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
mod tests;
