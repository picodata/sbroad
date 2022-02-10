use std::collections::HashSet;

use traversal::DftPost;

use crate::{
    errors::QueryPlannerError,
    ir::{expression::Expression, Plan},
};

impl Plan {
    /// Get`Bool` expression node where children are `Row` and `Const` types
    ///
    /// # Errors
    /// - some of the expression nodes are invalid
    #[allow(dead_code)]
    fn get_eq_bool_nodes_with_row_const_children(&self, top_node_id: usize) -> Vec<usize> {
        let mut nodes: Vec<usize> = Vec::new();

        let post_tree = DftPost::new(&top_node_id, |node| self.nodes.subtree_iter(node));
        for (_, node_id) in post_tree {
            if self.is_bool_node_simple_eq(*node_id) {
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
            // sharding keys wasn't determine and query will be sent to all instance
            return Ok(None);
        }

        let mut shard_keys = HashSet::new();
        let filter_nodes = self.get_eq_bool_nodes_with_row_const_children(node_id);
        for filter_id in filter_nodes {
            let node = self.get_expression_node(filter_id)?;

            if let Expression::Bool { left, right, .. } = node {
                let mut row_id = *left;
                let mut const_id = *right;

                // if left node isn't row we must swap node
                if !self.get_expression_node(row_id)?.is_row() {
                    const_id = *right;
                    row_id = *left;
                }

                // needs for temporary set empty distribution
                // that must set after motion transformation (now doesn't work in this case)
                if let Err(QueryPlannerError::UninitializedDistribution) =
                    self.get_distribution(row_id)
                {
                    self.set_distribution(row_id)?;
                }

                let sharding_info = self.get_distribution(row_id)?;
                if !sharding_info.is_unknown() {
                    // Queries with the complex sharding key aren't support yet (ignore them)
                    if sharding_info.get_segment_keys()?.len() > 1 {
                        return Ok(None);
                    }

                    shard_keys.insert(
                        self.get_expression_node(const_id)?
                            .get_const_value()?
                            .to_string(),
                    );
                }
            }
        }

        if shard_keys.is_empty() {
            return Ok(None);
        }
        // let mut result = HashSet::new();
        // for k in shard_keys {
        //     let hash = str_to_bucket_id(&k, bucket_count);
        //     result.insert(hash);
        // }

        Ok(Some(shard_keys))
    }
}

#[cfg(test)]
mod tests;
