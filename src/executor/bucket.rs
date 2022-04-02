use crate::errors::QueryPlannerError;
use crate::executor::engine::Engine;
use crate::executor::Query;
use crate::ir::distribution::Distribution;
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational};
use crate::ir::transformation::redistribution::MotionPolicy;
use std::collections::HashSet;
use traversal::DftPost;

/// Buckets are used to determine which nodes to send the query to.
#[derive(Clone, Debug, PartialEq)]
pub enum Buckets {
    // We don't want to keep thousands of buckets in memory
    // so we use a special enum to represent all the buckets
    // in a cluster.
    All,
    // A filtered set of buckets.
    Filtered(HashSet<u64>),
}

impl Buckets {
    /// Get all buckets in the cluster.
    pub fn new_all() -> Self {
        Buckets::All
    }

    /// Get a filtered set of buckets.
    pub fn new_filtered(buckets: HashSet<u64>) -> Self {
        Buckets::Filtered(buckets)
    }

    /// Disjunction of two sets of buckets.
    pub fn disjunct(&self, buckets: &Buckets) -> Buckets {
        match (self, buckets) {
            (Buckets::All, Buckets::All) => Buckets::All,
            (Buckets::Filtered(b), Buckets::All) | (Buckets::All, Buckets::Filtered(b)) => {
                Buckets::Filtered(b.clone())
            }
            (Buckets::Filtered(a), Buckets::Filtered(b)) => {
                Buckets::Filtered(a.intersection(b).copied().collect())
            }
        }
    }

    /// Conjunction of two sets of buckets.
    pub fn conjunct(&self, buckets: &Buckets) -> Buckets {
        match (self, buckets) {
            (Buckets::All, _) | (_, Buckets::All) => Buckets::All,
            (Buckets::Filtered(a), Buckets::Filtered(b)) => {
                Buckets::Filtered(a.union(b).copied().collect())
            }
        }
    }
}

impl<T> Query<T>
where
    T: Engine,
{
    fn get_buckets_from_expr(&self, expr_id: usize) -> Result<Buckets, QueryPlannerError> {
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
                if !left_expr.is_row() {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Left side of equality expression is not a row: {:?}",
                        left_expr
                    )));
                }
                let right_expr = ir_plan.get_expression_node(right_id)?;
                let right_columns = if let Expression::Row { list, .. } = right_expr {
                    list.clone()
                } else {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Right side of equality expression is not a row: {:?}",
                        right_expr
                    )));
                };

                // Get the distribution of the left row.
                let left_dist = ir_plan.get_distribution(left_id)?;

                // Gather buckets from the right row.
                if let Distribution::Segment { keys } = left_dist {
                    // If the right side is a row referencing to the motion
                    // it means that the corresponding virtual table contains
                    // tuple with the same distribution as the left side.
                    if let Some(motion_id) = ir_plan.get_motion_from_row(right_id)? {
                        let virtual_table = self.exec_plan.get_motion_vtable(motion_id)?;
                        let hashed_keys = virtual_table.get_tuple_distribution().keys();
                        let mut bucket_ids: HashSet<u64> = HashSet::new();
                        for bucket_str in hashed_keys {
                            bucket_ids.insert(self.engine.determine_bucket_id(bucket_str));
                        }
                        if !bucket_ids.is_empty() {
                            buckets.push(Buckets::new_filtered(bucket_ids));
                        }
                    }

                    // The right side is a regular row with constants
                    // on the positions of the left keys (if we are lucky).
                    for key in keys {
                        let mut values: Vec<String> = Vec::new();
                        for position in &key.positions {
                            let right_column_id =
                                *right_columns.get(*position).ok_or_else(|| {
                                    QueryPlannerError::CustomError(format!(
                                        "Right row does not have column at position {}",
                                        position
                                    ))
                                })?;
                            let right_column_expr = ir_plan.get_expression_node(right_column_id)?;
                            if let Expression::Constant { .. } = right_column_expr {
                                values.push(right_column_expr.get_const_value()?.into());
                            } else {
                                // One of the columns is not a constant. Skip this key.
                                values = Vec::new();
                                break;
                            }
                        }
                        if !values.is_empty() {
                            let bucket_str = values.join("");
                            let bucket = self.engine.determine_bucket_id(&bucket_str);
                            buckets.push(Buckets::new_filtered([bucket].into()));
                        }
                    }
                }
            }
        }

        if buckets.is_empty() {
            Ok(Buckets::new_all())
        } else {
            Ok(buckets
                .into_iter()
                .fold(Buckets::new_all(), |a, b| a.disjunct(&b)))
        }
    }

    fn get_expression_tree_buckets(&self, expr_id: usize) -> Result<Buckets, QueryPlannerError> {
        let ir_plan = self.exec_plan.get_ir_plan();
        let chains = ir_plan.get_dnf_chains(expr_id)?;
        let mut result: Vec<Buckets> = Vec::new();
        for mut chain in chains {
            let mut chain_buckets = Buckets::new_all();
            let nodes = chain.get_mut_nodes();
            // Nodes in the chain are in the top-down order (from left tot right).
            // We need to pop back the chain to get nodes in the bottom-up order.
            while let Some(node_id) = nodes.pop_back() {
                let node_buckets = self.get_buckets_from_expr(node_id)?;
                chain_buckets = chain_buckets.disjunct(&node_buckets);
            }
            result.push(chain_buckets);
        }

        if let Some((first, other)) = result.split_first_mut() {
            for buckets in other {
                *first = first.conjunct(buckets);
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
    pub fn bucket_discovery(&mut self, top_id: usize) -> Result<Buckets, QueryPlannerError> {
        let mut nodes: Vec<usize> = Vec::new();
        let ir_plan = self.exec_plan.get_ir_plan();
        let rel_tree = DftPost::new(&top_id, |node| ir_plan.nodes.rel_iter(node));
        for (_, node_id) in rel_tree {
            nodes.push(*node_id);
        }

        for node_id in nodes {
            if self.bucket_map.get(&node_id).is_some() {
                continue;
            }

            let rel = self.exec_plan.get_ir_plan().get_relation_node(node_id)?;
            match rel {
                Relational::ScanRelation { output, .. } => {
                    self.bucket_map.insert(*output, Buckets::new_all());
                }
                Relational::Motion { policy, output, .. } => match policy {
                    MotionPolicy::Full => {
                        self.bucket_map.insert(*output, Buckets::new_all());
                    }
                    MotionPolicy::Segment(_) => {
                        let virtual_table = self.exec_plan.get_motion_vtable(node_id)?;
                        let mut buckets: HashSet<u64> = HashSet::new();
                        for key in virtual_table.get_tuple_distribution().keys() {
                            let bucket = self.engine.determine_bucket_id(key);
                            buckets.insert(bucket);
                        }
                        self.bucket_map
                            .insert(*output, Buckets::new_filtered(buckets));
                    }
                    MotionPolicy::Local => {
                        return Err(QueryPlannerError::CustomError(
                            "Local motion policy should never appear in the plan".to_string(),
                        ));
                    }
                },
                Relational::Projection {
                    children, output, ..
                }
                | Relational::ScanSubQuery {
                    children, output, ..
                } => {
                    let child_id = children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Current node should have exactly one child".to_string(),
                        )
                    })?;
                    let child_rel = self.exec_plan.get_ir_plan().get_relation_node(*child_id)?;
                    let child_buckets = self
                        .bucket_map
                        .get(&child_rel.output())
                        .ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Failed to retrieve buckets of the child from the bucket map."
                                    .to_string(),
                            )
                        })?
                        .clone();
                    self.bucket_map.insert(*output, child_buckets);
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
                        let first_buckets = self.bucket_map.get(&first_rel.output()).ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Failed to retrieve buckets of the first union all child from the bucket map."
                                    .to_string(),
                            )
                        })?;
                        let second_buckets = self.bucket_map.get(&second_rel.output()).ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Failed to retrieve buckets of the second union all child from the bucket map."
                                    .to_string(),
                            )
                        })?;
                        let buckets = first_buckets.conjunct(second_buckets);
                        self.bucket_map.insert(*output, buckets);
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Current node should have exactly two children".to_string(),
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
                        QueryPlannerError::CustomError(
                            "Current node should have exactly one child".to_string(),
                        )
                    })?;
                    let child_rel = self.exec_plan.get_ir_plan().get_relation_node(*child_id)?;
                    let child_buckets = self
                        .bucket_map
                        .get(&child_rel.output())
                        .ok_or_else(|| {
                            QueryPlannerError::CustomError(
                            "Failed to retrieve buckets of the selection child from the bucket map."
                                .to_string(),
                        )
                        })?
                        .clone();
                    let output_id = *output;
                    let filter_id = *filter;
                    let filter_buckets = self.get_expression_tree_buckets(filter_id)?;
                    self.bucket_map
                        .insert(output_id, child_buckets.disjunct(&filter_buckets));
                }
                Relational::InnerJoin {
                    children,
                    condition,
                    output,
                    ..
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
                                QueryPlannerError::CustomError(
                                "Failed to retrieve buckets of the inner child from the bucket map."
                                    .to_string(),
                            )
                            })?
                            .clone();
                        let outer_buckets = self
                            .bucket_map
                            .get(&outer_rel.output())
                            .ok_or_else(|| {
                                QueryPlannerError::CustomError(
                                "Failed to retrieve buckets of the outer child from the bucket map."
                                .to_string(),
                            )
                            })?
                            .clone();
                        let output_id = *output;
                        let condition_id = *condition;
                        let filter_buckets = self.get_expression_tree_buckets(condition_id)?;
                        self.bucket_map.insert(
                            output_id,
                            inner_buckets
                                .conjunct(&outer_buckets)
                                .disjunct(&filter_buckets),
                        );
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Current node should have at least two children".to_string(),
                        ));
                    }
                }
            }
        }

        let top_rel = self.exec_plan.get_ir_plan().get_relation_node(top_id)?;
        let top_buckets = self
            .bucket_map
            .get(&top_rel.output())
            .ok_or_else(|| {
                QueryPlannerError::CustomError(
                    "Failed to retrieve buckets of the top relation from the bucket map."
                        .to_string(),
                )
            })?
            .clone();

        Ok(top_buckets)
    }
}

#[cfg(test)]
mod tests;
