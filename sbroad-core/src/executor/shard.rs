use std::collections::HashSet;

use traversal::DftPost;

use crate::executor::ir::ExecutionPlan;
use crate::{
    errors::{SbroadError, Entity},
    ir::{distribution::Distribution, expression::Expression},
};

impl<'e> ExecutionPlan<'e> {
    /// Get boolean equalities with both row children.
    #[allow(dead_code)]
    fn get_bool_eq_with_rows(&self, top_node_id: usize) -> Vec<usize> {
        let mut nodes: Vec<usize> = Vec::new();
        let ir_plan = self.get_ir_plan();

        let post_tree = DftPost::new(&top_node_id, |node| ir_plan.subtree_iter(node));
        for (_, node_id) in post_tree {
            if ir_plan.is_bool_eq_with_rows(*node_id) {
                nodes.push(*node_id);
            }
        }
        nodes
    }

    /// Extract all sharding key values of motion node.
    ///
    /// # Errors
    /// - node is not `Relation` type
    #[allow(dead_code)]
    fn get_motion_sharding_keys(
        &self,
        top_node_id: usize,
    ) -> Result<HashSet<String>, SbroadError> {
        let mut result = HashSet::new();

        let ir_plan = self.get_ir_plan();
        let post_tree = DftPost::new(&top_node_id, |node| ir_plan.nodes.rel_iter(node));
        for (_, node) in post_tree {
            if ir_plan.get_relation_node(*node)?.is_motion() {
                let vtable = self.get_motion_vtable(*node)?;
                result.extend(&vtable.get_sharding_keys()?);
            }
        }

        Ok(result)
    }

    /// Function evaluates buckets for the sending query.
    ///
    /// # Errors
    /// - getting bucket id error
    #[allow(dead_code)]
    pub fn discovery(
        &mut self,
        node_id: usize,
    ) -> Result<Option<HashSet<String>>, SbroadError> {
        let mut dist_keys: HashSet<String> = HashSet::new();

        let motion_shard_keys = self.get_motion_sharding_keys(node_id)?;
        dist_keys.extend(motion_shard_keys);

        let filter_nodes = self.get_bool_eq_with_rows(node_id);
        let ir_plan = &mut self.get_ir_plan();

        for filter_id in filter_nodes {
            let node = ir_plan.get_expression_node(filter_id)?;
            let pairs = if let Expression::Bool { left, right, .. } = node {
                vec![(*left, *right), (*right, *left)]
            } else {
                continue;
            };

            for (left_id, right_id) in pairs {
                // Get left and right rows.
                let left_expr = ir_plan.get_expression_node(left_id)?;
                if !left_expr.is_row() {
                    // We should never get here.
                    continue;
                }
                let right_expr = ir_plan.get_expression_node(right_id)?;
                let right_columns = if let Expression::Row { list, .. } = right_expr {
                    list
                } else {
                    // We should never get here.
                    continue;
                };

                // Get the distribution of the left row.
                if let Err(SbroadError::Invalid(Entity::Distribution, _)) =
                    ir_plan.get_distribution(left_id)
                {
                    ir_plan.set_distribution(left_id)?;
                }
                let left_dist = ir_plan.get_distribution(left_id)?;
                // Gather right constants corresponding to the left keys.
                if let Distribution::Segment { keys } = left_dist {
                    for key in keys {
                        let positions: Vec<usize> = key.positions;
                        // FIXME: we don't support complex distribution keys (ignore them).
                        if positions.len() > 1 {
                            return Ok(None);
                        }
                        let position = positions.get(0).ok_or_else(|| {
                            SbroadError::NotFound(Entity::DistributionKey, "with index 0".into())
                        })?;
                        let right_column_id = *right_columns.get(*position).ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues("left and right rows have different length.".into())
                        })?;
                        let right_column = ir_plan.get_expression_node(right_column_id)?;
                        if right_column.is_const() {
                            dist_keys.insert(right_column.get_const_value()?.to_string());
                        }
                    }
                }
            }
        }

        if dist_keys.is_empty() {
            return Ok(None);
        }

        Ok(Some(dist_keys))
    }
}

#[cfg(test)]
mod tests;
