use std::collections::HashSet;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::{Router, Vshard};
use crate::executor::Query;
use crate::ir::distribution::Distribution;
use crate::ir::expression::Expression;
use crate::ir::helpers::RepeatableState;
use crate::ir::operator::{Bool, JoinKind, Relational};
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::tree::traversal::{PostOrderWithFilter, REL_CAPACITY};
use crate::ir::value::Value;
use crate::ir::Node;
use crate::otm::child_span;
use sbroad_proc::otm_child_span;

/// Buckets are used to determine which nodes to send the query to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Buckets {
    // We don't want to keep thousands of buckets in memory
    // so we use a special enum to represent all the buckets
    // in a cluster.
    All,
    // A filtered set of buckets.
    Filtered(HashSet<u64, RepeatableState>),
    // Execute query only on a single node
    Single,
}

impl Buckets {
    /// Get all buckets in the cluster.
    #[must_use]
    pub fn new_all() -> Self {
        Buckets::All
    }

    /// Get no buckets in the cluster (coordinator).
    #[must_use]
    pub fn new_empty() -> Self {
        Buckets::Filtered(HashSet::with_hasher(RepeatableState))
    }

    /// Get a filtered set of buckets.
    #[must_use]
    pub fn new_filtered(buckets: HashSet<u64, RepeatableState>) -> Self {
        Buckets::Filtered(buckets)
    }

    /// Conjuction of two sets of buckets.
    ///
    /// # Errors
    /// - Buckets that can't be conjucted
    pub fn conjuct(&self, buckets: &Buckets) -> Result<Buckets, SbroadError> {
        let buckets = match (self, buckets) {
            // We have Buckets::Single only for motion subtree root, we do not have any leafs
            // or internal nodes that produce Buckets::Single
            (Buckets::Single, _) | (_, Buckets::Single) => {
                return Err(SbroadError::Unsupported(
                    Entity::Buckets,
                    Some("can't conjuct Buckets::Single".into()),
                ))
            }
            (Buckets::All, Buckets::All) => Buckets::All,
            (Buckets::Filtered(b), Buckets::All) | (Buckets::All, Buckets::Filtered(b)) => {
                Buckets::Filtered(b.clone())
            }
            (Buckets::Filtered(a), Buckets::Filtered(b)) => {
                Buckets::Filtered(a.intersection(b).copied().collect())
            }
        };
        Ok(buckets)
    }

    /// Disjunction of two sets of buckets.
    ///
    /// # Errors
    /// - Buckets that can't be disjuncted
    pub fn disjunct(&self, buckets: &Buckets) -> Result<Buckets, SbroadError> {
        let buckets = match (self, buckets) {
            // Buckets::Single is only possible for a motion subtree root, we don't have leafs
            // or any internal nodes that produce Buckets::Single
            (Buckets::Single, _) | (_, Buckets::Single) => {
                return Err(SbroadError::Unsupported(
                    Entity::Buckets,
                    Some("can't disjunct Buckets::Single".into()),
                ))
            }
            (Buckets::All, _) | (_, Buckets::All) => Buckets::All,
            (Buckets::Filtered(a), Buckets::Filtered(b)) => {
                Buckets::Filtered(a.union(b).copied().collect())
            }
        };
        Ok(buckets)
    }
}

impl<'a, T> Query<'a, T>
where
    T: Router + Vshard,
    &'a T: Vshard,
{
    /// Inner logic of `get_expression_tree_buckets` for simple expressions (without OR and AND
    /// operators).
    /// In general it returns `Buckets::All`, but in some cases (e.g. `Eq` and `In` operators) it
    /// will return `Buckets::Filtered` (if such a result is met in SELECT or JOIN filter, it means
    /// that we can execute the query only on some of the replicasets).
    fn get_buckets_from_expr(&self, expr_id: usize) -> Result<Buckets, SbroadError> {
        // Vec of `Buckets` that would be con conjucted later in case the vec is not empty.
        // The only possible case there will be several `Buckets` in the vec is when we have `Eq`.
        // See the logic of its handling below.
        let mut buckets: Vec<Buckets> = Vec::new();
        let ir_plan = self.exec_plan.get_ir_plan();
        let expr = ir_plan.get_expression_node(expr_id)?;

        if let Expression::Bool {
            op: Bool::Eq | Bool::In,
            left,
            right,
            ..
        } = expr
        {
            let pairs = vec![(*left, *right), (*right, *left)];
            for (left_id, right_id) in pairs {
                let left_expr = ir_plan.get_expression_node(left_id)?;

                if left_expr.is_arithmetic() {
                    return Ok(Buckets::new_all());
                }
                if !left_expr.is_row() {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(format!(
                            "left side of equality expression is not a row or arithmetic: {left_expr:?}"
                        )),
                    ));
                }

                let right_expr = ir_plan.get_expression_node(right_id)?;
                if right_expr.is_arithmetic() {
                    return Ok(Buckets::new_all());
                }
                let right_columns = if let Expression::Row { list, .. } = right_expr {
                    list.clone()
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(format!(
                            "right side of equality expression is not a row or arithmetic: {right_expr:?}"
                        )),
                    ));
                };

                // Get the distribution of the left row.
                let left_dist = ir_plan.get_distribution(left_id)?;

                // Gather buckets from the right row.
                if let Distribution::Segment { keys } = left_dist {
                    // If the right side is a row referencing the motion
                    // it means that the corresponding virtual table contains
                    // tuple with the same distribution as the left side (because this motion
                    // was specially added in order to fulfill `Eq` of `In` conditions).
                    if let Some(motion_id) = ir_plan.get_motion_from_row(right_id)? {
                        let virtual_table = self.exec_plan.get_motion_vtable(motion_id)?;
                        let bucket_ids: HashSet<u64, RepeatableState> =
                            virtual_table.get_bucket_index().keys().copied().collect();
                        if !bucket_ids.is_empty() {
                            return Ok(Buckets::new_filtered(bucket_ids));
                        }
                    }

                    // The right side is a regular row. So we have a case of `Eq` operator.
                    // If we have a case of constants on the positions of the left keys,
                    // we can return `Buckets::Filtered`.
                    // E.g. we have query
                    // `SELECT * FROM (SELECT A.a, B.b FROM A JOIN B ON A.a = B.b)
                    //  WHERE (a, b) = (0, 1)`.
                    // Here (a, b) row will have Distribution::Segment(keys = {[a], [b]}).
                    // After handling key "a" we will leave buckets which satisfy `a = 0`.
                    // After handling key "b" we will leave buckets which satisfy `b = 1`.
                    // In the end (when `conjuct` function is called) we will leave buckets
                    // which satisfy `(a, b) = (0, 1)`.
                    for key in keys.iter() {
                        let mut values: Vec<&Value> = Vec::new();
                        for position in &key.positions {
                            let right_column_id =
                                *right_columns.get(*position).ok_or_else(|| {
                                    SbroadError::NotFound(
                                        Entity::Column,
                                        format!("at position {position} for right row"),
                                    )
                                })?;
                            let right_column_expr = ir_plan.get_expression_node(right_column_id)?;
                            if let Expression::Constant { .. } = right_column_expr {
                                values.push(right_column_expr.as_const_value_ref()?);
                            } else {
                                // One of the columns is not a constant. Skip this key.
                                values = Vec::new();
                                break;
                            }
                        }
                        if !values.is_empty() {
                            let bucket = self.coordinator.determine_bucket_id(&values)?;
                            let bucket_set: HashSet<u64, RepeatableState> =
                                vec![bucket].into_iter().collect();
                            buckets.push(Buckets::new_filtered(bucket_set));
                        }
                    }
                }
            }
        }

        if buckets.is_empty() {
            Ok(Buckets::new_all())
        } else {
            Ok::<Result<Buckets, SbroadError>, SbroadError>(
                buckets
                    .into_iter()
                    .fold(Ok(Buckets::new_all()), |a, b| a?.conjuct(&b)),
            )?
        }
    }

    /// Inner logic of `bucket_discovery` for expressions (currently it's called only on SELECTION's
    /// and JOIN's filters).
    /// It splits given expression into DNF chains and calls `get_buckets_from_expr` on each simple
    /// chain subexpression, later conjucting results.
    fn get_expression_tree_buckets(&self, expr_id: usize) -> Result<Buckets, SbroadError> {
        let ir_plan = self.exec_plan.get_ir_plan();
        let chains = ir_plan.get_dnf_chains(expr_id)?;
        let mut result: Vec<Buckets> = Vec::new();
        for mut chain in chains {
            let mut chain_buckets = Buckets::new_all();
            let nodes = chain.get_mut_nodes();
            // Nodes in the chain are in the top-down order (from left to right).
            // We need to pop back the chain to get nodes in the bottom-up order.
            while let Some(node_id) = nodes.pop_back() {
                let node_buckets = self.get_buckets_from_expr(node_id)?;
                chain_buckets = chain_buckets.conjuct(&node_buckets)?;
            }
            result.push(chain_buckets);
        }

        if let Some((first, other)) = result.split_first_mut() {
            for buckets in other {
                *first = first.disjunct(buckets)?;
            }
            return Ok(first.clone());
        }

        Ok(Buckets::All)
    }

    /// Discover required buckets to execute the query subtree.
    ///
    /// # Errors
    /// - Relational iterator returns non-relational nodes.
    /// - Failed to find a virtual table.
    /// - Relational nodes contain invalid children.
    #[allow(clippy::too_many_lines)]
    #[otm_child_span("query.bucket.discovery")]
    pub fn bucket_discovery(&mut self, top_id: usize) -> Result<Buckets, SbroadError> {
        // if top's output has Distribution::Single then the whole subtree must executed only on
        // a single node, no need to traverse the subtree
        let top_output_id = self.exec_plan.get_ir_plan().get_relational_output(top_id)?;
        if let Expression::Row {
            distribution: Some(dist),
            ..
        } = self
            .exec_plan
            .get_ir_plan()
            .get_expression_node(top_output_id)?
        {
            if *dist == Distribution::Single {
                return Ok(Buckets::Single);
            }
        }

        let ir_plan = self.exec_plan.get_ir_plan();
        let filter = |node_id: usize| -> bool {
            if let Ok(Node::Relational(_)) = ir_plan.get_node(node_id) {
                return true;
            }
            false
        };
        // We use a `exec_plan_subtree_iter()` because we need DNF version of the
        // filter/condition expressions to determine buckets.
        let mut tree = PostOrderWithFilter::with_capacity(
            |node| ir_plan.exec_plan_subtree_iter(node),
            REL_CAPACITY,
            Box::new(filter),
        );

        for (_, node_id) in tree.iter(top_id) {
            if self.bucket_map.get(&node_id).is_some() {
                continue;
            }

            let rel = self.exec_plan.get_ir_plan().get_relation_node(node_id)?;
            match rel {
                Relational::ScanRelation { output, .. } => {
                    self.bucket_map.insert(*output, Buckets::new_all());
                }
                Relational::Motion {
                    children,
                    policy,
                    output,
                    ..
                } => match policy {
                    MotionPolicy::Full | MotionPolicy::Local => {
                        self.bucket_map.insert(*output, Buckets::new_all());
                    }
                    MotionPolicy::Segment(_) => {
                        let virtual_table = self.exec_plan.get_motion_vtable(node_id)?;
                        let buckets = virtual_table
                            .get_bucket_index()
                            .keys()
                            .copied()
                            .collect::<HashSet<u64, RepeatableState>>();
                        self.bucket_map
                            .insert(*output, Buckets::new_filtered(buckets));
                    }
                    MotionPolicy::LocalSegment(_) => {
                        if let Ok(virtual_table) = self.exec_plan.get_motion_vtable(node_id) {
                            // In a case of `insert .. values ..` it is possible built a local
                            // segmented virtual table right on the router. So, we can use its
                            // buckets from the index to determine the buckets for the insert.
                            let buckets = virtual_table
                                .get_bucket_index()
                                .keys()
                                .copied()
                                .collect::<HashSet<u64, RepeatableState>>();
                            self.bucket_map
                                .insert(*output, Buckets::new_filtered(buckets));
                        } else {
                            // We'll create and populate a local segmented virtual table on the
                            // storage later. At the moment the best thing we can do is to copy
                            // child's buckets.
                            let child_id = children.first().ok_or_else(|| {
                                SbroadError::UnexpectedNumberOfValues(
                                    "Motion node should have exactly one child".to_string(),
                                )
                            })?;
                            let child_rel =
                                self.exec_plan.get_ir_plan().get_relation_node(*child_id)?;
                            let child_buckets = self
                                .bucket_map
                                .get(&child_rel.output())
                                .ok_or_else(|| {
                                    SbroadError::FailedTo(
                                        Action::Retrieve,
                                        Some(Entity::Buckets),
                                        "of the child from the bucket map.".to_string(),
                                    )
                                })?
                                .clone();
                            self.bucket_map.insert(*output, child_buckets);
                        }
                    }
                    MotionPolicy::None => {
                        return Err(SbroadError::Invalid(
                            Entity::Motion,
                            Some("local motion policy should never appear in the plan".to_string()),
                        ));
                    }
                },
                Relational::Delete {
                    children, output, ..
                }
                | Relational::Insert {
                    children, output, ..
                }
                | Relational::Projection {
                    children, output, ..
                }
                | Relational::GroupBy {
                    children, output, ..
                }
                | Relational::Having {
                    children, output, ..
                }
                | Relational::Update {
                    children, output, ..
                }
                | Relational::ScanSubQuery {
                    children, output, ..
                } => {
                    let child_id = children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Current node should have exactly one child".to_string(),
                        )
                    })?;
                    let child_rel = self.exec_plan.get_ir_plan().get_relation_node(*child_id)?;
                    let child_buckets = self
                        .bucket_map
                        .get(&child_rel.output())
                        .ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Retrieve,
                                Some(Entity::Buckets),
                                "of the child from the bucket map.".to_string(),
                            )
                        })?
                        .clone();
                    self.bucket_map.insert(*output, child_buckets);
                }
                Relational::Except {
                    children, output, ..
                } => {
                    if let (Some(first_id), Some(_), None) =
                        (children.first(), children.get(1), children.get(2))
                    {
                        // We are only interested in the first child (the left one).
                        // The rows from the second child would be transferred to the
                        // first child by the motion or already located in the first
                        // child's bucket. So we don't need to worry about the second
                        // child's buckets here.
                        let first_rel =
                            self.exec_plan.get_ir_plan().get_relation_node(*first_id)?;
                        let first_buckets = self
                            .bucket_map
                            .get(&first_rel.output())
                            .ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Retrieve,
                                    Some(Entity::Buckets),
                                    "of the first except child from the bucket map.".to_string(),
                                )
                            })?
                            .clone();
                        self.bucket_map.insert(*output, first_buckets);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "current node should have exactly two children".to_string(),
                        ));
                    }
                }
                Relational::UnionAll {
                    children, output, ..
                } => {
                    if let (Some(first_id), Some(second_id), None) =
                        (children.first(), children.get(1), children.get(2))
                    {
                        let first_rel =
                            self.exec_plan.get_ir_plan().get_relation_node(*first_id)?;
                        let second_rel =
                            self.exec_plan.get_ir_plan().get_relation_node(*second_id)?;
                        let first_buckets =
                            self.bucket_map.get(&first_rel.output()).ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Retrieve,
                                    Some(Entity::Buckets),
                                    "of the first union all child from the bucket map.".to_string(),
                                )
                            })?;
                        let second_buckets =
                            self.bucket_map.get(&second_rel.output()).ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Retrieve,
                                    Some(Entity::Buckets),
                                    "of the second union all child from the bucket map."
                                        .to_string(),
                                )
                            })?;
                        let buckets = first_buckets.disjunct(second_buckets)?;
                        self.bucket_map.insert(*output, buckets);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "current node should have exactly two children".to_string(),
                        ));
                    }
                }
                Relational::Selection {
                    children,
                    filter,
                    output,
                    ..
                } => {
                    // We need to get the buckets of the child node for the case
                    // when the filter returns no buckets to reduce.
                    let child_id = children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "current node should have exactly one child".to_string(),
                        )
                    })?;
                    let child_rel = self.exec_plan.get_ir_plan().get_relation_node(*child_id)?;
                    let child_buckets = self
                        .bucket_map
                        .get(&child_rel.output())
                        .ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Retrieve,
                                Some(Entity::Buckets),
                                "of the selection child from the bucket map.".to_string(),
                            )
                        })?
                        .clone();
                    let output_id = *output;
                    let filter_id = *filter;
                    let filter_buckets = self.get_expression_tree_buckets(filter_id)?;
                    self.bucket_map
                        .insert(output_id, child_buckets.conjuct(&filter_buckets)?);
                }
                Relational::Join {
                    children,
                    condition,
                    output,
                    kind,
                } => {
                    if let (Some(inner_id), Some(outer_id)) = (children.first(), children.get(1)) {
                        let inner_rel =
                            self.exec_plan.get_ir_plan().get_relation_node(*inner_id)?;
                        let outer_rel =
                            self.exec_plan.get_ir_plan().get_relation_node(*outer_id)?;
                        let inner_buckets = self
                            .bucket_map
                            .get(&inner_rel.output())
                            .ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Retrieve,
                                    Some(Entity::Buckets),
                                    "of the inner child from the bucket map.".to_string(),
                                )
                            })?
                            .clone();
                        let outer_buckets = self
                            .bucket_map
                            .get(&outer_rel.output())
                            .ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Retrieve,
                                    Some(Entity::Buckets),
                                    "of the outer child from the bucket map.".to_string(),
                                )
                            })?
                            .clone();
                        let output_id = *output;
                        let condition_id = *condition;
                        let join_buckets = match kind {
                            JoinKind::Inner => {
                                let filter_buckets =
                                    self.get_expression_tree_buckets(condition_id)?;
                                inner_buckets
                                    .disjunct(&outer_buckets)?
                                    .conjuct(&filter_buckets)?
                            }
                            JoinKind::LeftOuter => inner_buckets.disjunct(&outer_buckets)?,
                        };
                        self.bucket_map.insert(output_id, join_buckets);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "current node should have at least two children".to_string(),
                        ));
                    }
                }
                Relational::Values { output, .. } | Relational::ValuesRow { output, .. } => {
                    // At the moment values rows are located on the coordinator,
                    // so there are no buckets to execute on.
                    self.bucket_map.insert(*output, Buckets::new_empty());
                }
            }
        }

        let top_rel = self.exec_plan.get_ir_plan().get_relation_node(top_id)?;
        let top_buckets = self
            .bucket_map
            .get(&top_rel.output())
            .ok_or_else(|| {
                SbroadError::FailedTo(
                    Action::Retrieve,
                    Some(Entity::Buckets),
                    "of the top relation from the bucket map.".to_string(),
                )
            })?
            .clone();

        Ok(top_buckets)
    }
}

#[cfg(test)]
mod tests;
