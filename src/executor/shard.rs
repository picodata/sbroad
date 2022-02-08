use std::collections::HashSet;

use traversal::DftPost;

use crate::{
    errors::QueryPlannerError,
    ir::{distribution::Distribution, expression::Expression, Plan},
};

impl Plan {
    /// Get boolean equalities with both row children.
    ///
    /// # Errors
    /// - some of the expression nodes are invalid
    #[allow(dead_code)]
    fn get_bool_eq_with_rows(&self, top_node_id: usize) -> Vec<usize> {
        let mut nodes: Vec<usize> = Vec::new();

        let post_tree = DftPost::new(&top_node_id, |node| self.nodes.subtree_iter(node));
        for (_, node_id) in post_tree {
            if self.is_bool_eq_with_rows(*node_id) {
                nodes.push(*node_id);
            }
        }
        nodes
    }

    /// Function evaluates buckets for the sending query.
    ///
    /// # Errors
    /// - getting bucket id error
    #[allow(dead_code)]
    pub fn get_sharding_keys(
        &mut self,
        node_id: usize,
    ) -> Result<Option<HashSet<String>>, QueryPlannerError> {
        let top_node = self.get_relation_node(node_id)?;

        // Determine query sharding keys
        let distribute_query_row = self.get_expression_node(top_node.output())?;
        if distribute_query_row.has_unknown_distribution()? {
            // Fallback to broadcast.
            return Ok(None);
        }

        let mut dist_keys: HashSet<String> = HashSet::new();
        let filter_nodes = self.get_bool_eq_with_rows(node_id);
        for filter_id in filter_nodes {
            let node = self.get_expression_node(filter_id)?;
            let pairs = if let Expression::Bool { left, right, .. } = node {
                vec![(*left, *right), (*right, *left)]
            } else {
                continue;
            };

            for (left_id, right_id) in pairs {
                // Get left and right rows.
                let left_expr = self.get_expression_node(left_id)?;
                if !left_expr.is_row() {
                    // We should never get here.
                    continue;
                }
                let right_expr = self.get_expression_node(right_id)?;
                let right_columns = if let Expression::Row { list, .. } = right_expr {
                    list.clone()
                } else {
                    // We should never get here.
                    continue;
                };

                // Get the distribution of the left row.
                if let Err(QueryPlannerError::UninitializedDistribution) =
                    self.get_distribution(left_id)
                {
                    self.set_distribution(left_id)?;
                }
                let left_dist = self.get_distribution(left_id)?;

                // Gather right constants corresponding to the left keys.
                if let Distribution::Segment { keys } = left_dist {
                    for key in keys {
                        let positions: Vec<usize> = key.positions.clone();
                        // FIXME: we don't support complex distribution keys (ignore them).
                        if positions.len() > 1 {
                            return Ok(None);
                        }
                        let position = positions.get(0).ok_or_else(|| {
                            QueryPlannerError::CustomError("Invalid distribution key".into())
                        })?;
                        let right_column_id = *right_columns.get(*position).ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Left and right rows have different length.".into(),
                            )
                        })?;
                        let right_column = self.get_expression_node(right_column_id)?;
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
