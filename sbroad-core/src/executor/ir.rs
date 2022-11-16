use std::collections::HashMap;
use std::rc::Rc;

use ahash::AHashMap;
use traversal::DftPost;

use crate::errors::QueryPlannerError;
use crate::errors::QueryPlannerError::CustomError;
use crate::executor::vtable::VirtualTable;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::{Node, Plan};

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    plan: Plan,
    vtables: Option<HashMap<usize, Rc<VirtualTable>>>,
}

impl From<Plan> for ExecutionPlan {
    fn from(plan: Plan) -> Self {
        ExecutionPlan {
            plan,
            vtables: None,
        }
    }
}

impl ExecutionPlan {
    #[must_use]
    pub fn get_ir_plan(&self) -> &Plan {
        &self.plan
    }

    #[allow(dead_code)]
    pub fn get_mut_ir_plan(&mut self) -> &mut Plan {
        &mut self.plan
    }

    #[must_use]
    pub fn get_vtables(&self) -> Option<&HashMap<usize, Rc<VirtualTable>>> {
        self.vtables.as_ref()
    }

    pub fn get_mut_vtables(&mut self) -> Option<&mut HashMap<usize, Rc<VirtualTable>>> {
        self.vtables.as_mut()
    }

    pub fn set_vtables(&mut self, vtables: HashMap<usize, Rc<VirtualTable>>) {
        self.vtables = Some(vtables);
    }

    /// Get motion virtual table
    ///
    /// # Errors
    /// - Failed to find a virtual table for the motion node.
    pub fn get_motion_vtable(
        &self,
        motion_id: usize,
    ) -> Result<Rc<VirtualTable>, QueryPlannerError> {
        if let Some(vtable) = &self.vtables {
            if let Some(result) = vtable.get(&motion_id) {
                return Ok(Rc::clone(result));
            }
        }

        Err(QueryPlannerError::CustomError(format!(
            "Motion node ({}) doesn't have a corresponding virtual table",
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

    /// Get motion alias name
    ///
    /// # Errors
    /// - node is not valid
    pub fn get_motion_alias(&self, node_id: usize) -> Result<Option<&String>, QueryPlannerError> {
        let sq_id = &self.get_motion_child(node_id)?;
        if let Relational::ScanSubQuery { alias, .. } =
            self.get_ir_plan().get_relation_node(*sq_id)?
        {
            return Ok(alias.as_ref());
        }

        Ok(None)
    }

    /// Get root from motion sub tree
    ///
    /// # Errors
    /// - node is not valid
    pub fn get_motion_subtree_root(&self, node_id: usize) -> Result<usize, QueryPlannerError> {
        let top_id = &self.get_motion_child(node_id)?;
        let rel = self.get_ir_plan().get_relation_node(*top_id)?;
        match rel {
            Relational::ScanSubQuery { .. } => self.get_subquery_child(*top_id),
            Relational::Except { .. }
            | Relational::InnerJoin { .. }
            | Relational::Projection { .. }
            | Relational::ScanRelation { .. }
            | Relational::Selection { .. }
            | Relational::UnionAll { .. }
            | Relational::Values { .. }
            | Relational::ValuesRow { .. } => Ok(*top_id),
            Relational::Motion { .. } | Relational::Insert { .. } => Err(
                QueryPlannerError::CustomError("Invalid motion child node".to_string()),
            ),
        }
    }

    /// Extract a child from the motion node. Motion node must contain only a single child.
    ///
    /// # Errors
    /// - node is not `Relation` type
    /// - node does not contain children
    pub(crate) fn get_motion_child(&self, node_id: usize) -> Result<usize, QueryPlannerError> {
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

        let child_id = children.first().ok_or_else(|| {
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

        let child_id = children.first().ok_or_else(|| {
            QueryPlannerError::CustomError("Could not find subquery child".to_string())
        })?;

        Ok(*child_id)
    }

    /// Build a new execution plan from the subtree of the existing execution plan.
    ///
    /// # Errors
    /// - the original execution plan is invalid
    #[allow(clippy::too_many_lines)]
    pub fn new_from_subtree(&self, top_id: usize) -> Result<Self, QueryPlannerError> {
        let mut map: AHashMap<usize, usize> = AHashMap::new();
        let mut new_vtables: HashMap<usize, Rc<VirtualTable>> = HashMap::new();
        let mut new_plan = Plan::new();
        let ir_plan = self.get_ir_plan();
        let subtree = DftPost::new(&top_id, |node| ir_plan.exec_plan_subtree_iter(node));
        for (_, node_id) in subtree {
            let mut node = ir_plan.get_node(*node_id)?.clone();
            let next_id = new_plan.nodes.next_id();
            match node {
                Node::Relational(ref mut rel) => {
                    if let Relational::Motion { children, .. } = rel {
                        if let Some(vtable) =
                            self.get_vtables().map_or_else(|| None, |v| v.get(node_id))
                        {
                            new_vtables.insert(next_id, Rc::clone(vtable));
                        }
                        *children = Vec::new();
                    }

                    if let Some(children) = rel.mut_children() {
                        for child_id in children {
                            *child_id = *map.get(child_id).ok_or_else(|| {
                                QueryPlannerError::CustomError(format!(
                                    "Failed to build an execution plan subtree: could not find child node id {} in the map",
                                    child_id
                                ))
                            })?;
                        }
                    }

                    let output = rel.output();
                    *rel.mut_output() = *map.get(&output).ok_or_else(|| {
                        QueryPlannerError::CustomError(format!(
                            "Failed to build an execution plan subtree: could not find output node id {} in the map",
                            output
                        ))
                    })?;
                    new_plan.replace_parent_in_subtree(rel.output(), None, Some(next_id))?;

                    if let Relational::Selection {
                        filter: ref mut expr_id,
                        ..
                    }
                    | Relational::InnerJoin {
                        condition: ref mut expr_id,
                        ..
                    } = rel
                    {
                        let oldest_expr_id = ir_plan
                            .undo
                            .get_oldest(expr_id)
                            .map_or_else(|| &*expr_id, |id| id);
                        *expr_id = *map.get(oldest_expr_id).ok_or_else(|| {
                            QueryPlannerError::CustomError(format!(
                                "Failed to build an execution plan subtree: could not find filter/condition node id {} in the map",
                                oldest_expr_id
                            ))
                        })?;
                        new_plan.replace_parent_in_subtree(*expr_id, None, Some(next_id))?;
                    }

                    if let Relational::ScanRelation { relation, .. } = rel {
                        let table = ir_plan.relations.as_ref().and_then(|r| r.get(relation)).ok_or_else(|| {
                            QueryPlannerError::CustomError(format!(
                                "Failed to build an execution plan subtree: could not find relation {} in the original plan",
                                relation
                            ))
                        })?.clone();
                        new_plan.add_rel(table);
                    }
                }
                Node::Expression(ref mut expr) => match expr {
                    Expression::Alias { ref mut child, .. }
                    | Expression::Cast { ref mut child, .. }
                    | Expression::Unary { ref mut child, .. } => {
                        *child = *map.get(child).ok_or_else(|| {
                                QueryPlannerError::CustomError(format!(
                                    "Failed to build an execution plan subtree: could not find child node id {} in the map",
                                    child
                                ))
                            })?;
                    }
                    Expression::Bool {
                        ref mut left,
                        ref mut right,
                        ..
                    }
                    | Expression::Concat {
                        ref mut left,
                        ref mut right,
                        ..
                    } => {
                        *left = *map.get(left).ok_or_else(|| {
                                QueryPlannerError::CustomError(format!(
                                    "Failed to build an execution plan subtree: could not find left child node id {} in the map",
                                    left
                                ))
                            })?;
                        *right = *map.get(right).ok_or_else(|| {
                                QueryPlannerError::CustomError(format!(
                                    "Failed to build an execution plan subtree: could not find right child node id {} in the map",
                                    right
                                ))
                            })?;
                    }
                    Expression::Reference { ref mut parent, .. } => {
                        // The new parent node id MUST be set while processing the relational nodes.
                        *parent = None;
                    }
                    Expression::Row {
                        list: ref mut children,
                        ..
                    }
                    | Expression::StableFunction {
                        ref mut children, ..
                    } => {
                        for child in children {
                            *child = *map.get(child).ok_or_else(|| {
                                    QueryPlannerError::CustomError(format!(
                                        "Failed to build an execution plan subtree: could not find child node id {} in the map",
                                        child
                                    ))
                                })?;
                        }
                    }
                    Expression::Constant { .. } => {}
                },
                Node::Parameter { .. } => {}
            }
            new_plan.nodes.push(node);
            map.insert(*node_id, next_id);
            if top_id == *node_id {
                new_plan.set_top(next_id)?;
            }
        }

        let vtables = if new_vtables.is_empty() {
            None
        } else {
            Some(new_vtables)
        };
        let new_exec_plan = ExecutionPlan {
            plan: new_plan,
            vtables,
        };
        Ok(new_exec_plan)
    }
}
