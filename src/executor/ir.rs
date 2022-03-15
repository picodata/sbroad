use std::collections::HashMap;

use crate::errors::QueryPlannerError;
use crate::errors::QueryPlannerError::CustomError;
use crate::executor::vtable::VirtualTable;
use crate::ir::operator::Relational;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::Plan;

#[derive(Debug, Clone)]
pub struct ExecutionPlan<'e> {
    plan: &'e Plan,
    vtables: Option<HashMap<usize, VirtualTable>>,
}

impl<'e> From<&'e Plan> for ExecutionPlan<'e> {
    fn from(plan: &'e Plan) -> Self {
        ExecutionPlan {
            plan,
            vtables: None,
        }
    }
}

impl<'e> ExecutionPlan<'e> {
    pub fn get_ir_plan(&self) -> &Plan {
        self.plan
    }

    /// Add materialize motion result to translation map of virtual tables
    ///
    /// # Errors
    /// - invalid motion node
    pub fn add_motion_result(
        &mut self,
        motion_id: usize,
        vtable: VirtualTable,
    ) -> Result<(), QueryPlannerError> {
        let mut motion_result = vtable;
        if let MotionPolicy::Segment(shard_key) = &self.get_motion_policy(motion_id)? {
            motion_result.hashing_tuple_by_shard(shard_key);
        }

        let mut virtual_tables = match &self.vtables {
            None => HashMap::new(),
            Some(v) => v.clone(),
        };

        virtual_tables.insert(motion_id, motion_result);

        self.vtables = Some(virtual_tables);

        Ok(())
    }

    /// Get motion virtual table
    pub fn get_motion_vtable(&self, motion_id: usize) -> Result<VirtualTable, QueryPlannerError> {
        if let Some(vtable) = self.vtables.clone() {
            if let Some(result) = vtable.get(&motion_id) {
                return Ok(result.clone());
            }
        }

        Err(QueryPlannerError::CustomError(format!(
            "Motion node ({}) not found in the virtual table",
            motion_id
        )))
    }

    /// Extract policy from motion node
    ///
    /// # Errors
    /// - node is not `Relation` type
    /// - node is not `Motion` type
    pub fn get_motion_policy(&self, node_id: usize) -> Result<MotionPolicy, QueryPlannerError> {
        if let Relational::Motion { policy, .. } = &self.plan.get_relation_node(node_id)? {
            return Ok(policy.clone());
        }

        Err(QueryPlannerError::CustomError(String::from(
            "Invalid motion",
        )))
    }

    /// Get root from motion sub tree
    ///
    /// # Errors
    /// - node is not valid
    pub fn get_motion_subtree_root(&self, node_id: usize) -> Result<usize, QueryPlannerError> {
        let sq_id = &self.get_motion_child(node_id)?;
        self.get_subquery_child(*sq_id)
    }

    /// Extract a child from the motion node. Motion node must contain only a single child.
    ///
    /// # Errors
    /// - node is not `Relation` type
    /// - node does not contain children
    fn get_motion_child(&self, node_id: usize) -> Result<usize, QueryPlannerError> {
        let node = self.get_ir_plan().get_relation_node(node_id)?;
        if !node.is_motion() {
            return Err(CustomError(format!(
                "Current node ({}) is not motion",
                node_id
            )));
        }

        let children = self.plan.get_relational_children(node_id)?.ok_or_else(|| {
            QueryPlannerError::CustomError("Could not get motion children".to_string())
        })?;

        if children.len() != 1 {
            return Err(CustomError(format!(
                "Motion node ({}) must have once child only (actual {})",
                node_id,
                children.len()
            )));
        }

        let child_id = children.get(0).ok_or_else(|| {
            QueryPlannerError::CustomError("Failed to get the first motion child".to_string())
        })?;

        Ok(*child_id)
    }

    /// Extract subquery child node. Subquery node must contains only one child
    ///
    /// # Errors
    /// - node is not `Relation` type
    /// - node does not contain children
    fn get_subquery_child(&self, node_id: usize) -> Result<usize, QueryPlannerError> {
        let node = self.get_ir_plan().get_relation_node(node_id)?;
        if !node.is_subquery() {
            return Err(CustomError(format!(
                "Current node ({}) is not sub query",
                node_id
            )));
        }

        let children = self.plan.get_relational_children(node_id)?.ok_or_else(|| {
            QueryPlannerError::CustomError("Could not get subquery children".to_string())
        })?;

        if children.len() != 1 {
            return Err(CustomError(format!(
                "Sub query node ({}) must have once child only (actual {})",
                node_id,
                children.len()
            )));
        }

        let child_id = children.get(0).ok_or_else(|| {
            QueryPlannerError::CustomError("Could not find subquery child".to_string())
        })?;

        Ok(*child_id)
    }
}
