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
    fn choose_strategy_for_inner(
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

    fn resolve_sub_query_conflicts(
        &mut self,
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
            let bool_op = BoolOp::from_expr(self, *node)?;
            let left = self.get_sub_query_from_row_node(bool_op.left)?;
            let right = self.get_sub_query_from_row_node(bool_op.right)?;
            match left {
                Some(left_sq) => {
                    match right {
                        Some(right_sq) => {
                            // Both sides are sub-queries and require a full copy.
                            strategy.insert(left_sq, MotionPolicy::Full);
                            strategy.insert(right_sq, MotionPolicy::Full);
                        }
                        None => {
                            // Left side is sub-query, right is an outer tuple.
                            strategy.insert(
                                left_sq,
                                self.choose_strategy_for_inner(
                                    bool_op.right,
                                    self.get_relational_output(left_sq)?,
                                    &bool_op.op,
                                )?,
                            );
                        }
                    }
                }
                None => {
                    if let Some(right_sq) = right {
                        // Left side is an outer tuple, right is sub-query.
                        strategy.insert(
                            right_sq,
                            self.choose_strategy_for_inner(
                                bool_op.left,
                                self.get_relational_output(right_sq)?,
                                &bool_op.op,
                            )?,
                        );
                    }
                }
            }
        }
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
                    let strategy = self.resolve_sub_query_conflicts(filter)?;
                    self.create_motion_nodes(*id, &strategy)?;
                    self.set_distribution(output)?;
                }
                Relational::InnerJoin { .. } => {
                    // TODO: resolve join conflicts and set the output row distribution
                    return Err(QueryPlannerError::CustomError(String::from(
                        "Inner join conflicts resolution is not implemented yet.",
                    )));
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
