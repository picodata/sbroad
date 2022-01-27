use crate::errors::QueryPlannerError;
use crate::ir::distribution::{Distribution, Key};
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational};
use crate::ir::{Node, Plan};
use std::collections::{HashMap, HashSet};
use traversal::DftPost;

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

    ///
    /// # Errors
    /// - top node is invalid
    fn get_expr_nodes_dfs_post(&self, top: usize) -> Vec<usize> {
        let mut nodes: Vec<usize> = Vec::new();

        let post_tree = DftPost::new(&top, |node| self.nodes.expr_iter(node));
        for (_, node) in post_tree {
            nodes.push(*node);
        }
        nodes
    }

    fn resolve_join_conflicts(
        &mut self,
        rel: usize,
        expr: usize,
        map: &HashMap<usize, usize>,
    ) -> Result<(), QueryPlannerError> {
        // TODO: resolve joins
        Ok(())
    }

    fn resolve_subquery_conflicts(
        &mut self,
        rel: usize,
        expr: usize,
        map: &HashMap<usize, usize>,
    ) -> Result<(), QueryPlannerError> {
        // TODO: resolve sub queries
        Ok(())
    }

    /// Add motion nodes to the plan tree.
    ///
    /// # Errors
    /// - failed to get relational nodes (plan is invalid?)
    /// - failed to resolve distribution conflicts
    /// - failed to set distribution
    pub fn add_motions(&mut self) -> Result<(), QueryPlannerError> {
        let map = self.relational_id_map();

        let nodes = self.get_relational_nodes_dfs_post()?;
        for id in &nodes {
            let rel_op: Relational = match self.get_node(*id)? {
                Node::Expression(_) => return Err(QueryPlannerError::InvalidNode),
                Node::Relational(rel) => rel.clone(),
            };
            match rel_op {
                // At the moment our grammar and IR constructor
                // don't support projection with sub queries.
                Relational::Projection { output, .. }
                | Relational::ScanRelation { output, .. }
                | Relational::ScanSubQuery { output, .. }
                | Relational::UnionAll { output, .. } => {
                    self.set_distribution(output, &map)?;
                }
                Relational::Motion { .. } => {
                    // We can apply this transformation only once,
                    // i.e. to the plan without any motion nodes.
                    return Err(QueryPlannerError::InvalidNode);
                }
                Relational::Selection { output, filter, .. } => {
                    self.resolve_subquery_conflicts(*id, filter, &map)?;
                    self.set_distribution(output, &map)?;
                }
                Relational::InnerJoin {
                    output, condition, ..
                } => {
                    self.resolve_join_conflicts(*id, condition, &map)?;
                    self.set_distribution(output, &map)?;
                }
            }
        }
        Ok(())
    }
}
