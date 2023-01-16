use std::collections::HashMap;
use std::rc::Rc;

use ahash::AHashMap;
use serde::{Deserialize, Serialize};

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::vtable::{VirtualTable, VirtualTableMap};
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::relation::SpaceEngine;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::tree::traversal::PostOrder;
use crate::ir::{Node, Plan};

/// Query type (used to parse the returned results).
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub enum QueryType {
    /// SELECT query.
    DQL,
    /// INSERT query.
    DML,
}

/// Connection type to the Tarantool instance.
#[derive(Debug)]
pub enum ConnectionType {
    Read,
    Write,
}

#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ExecutionPlan {
    plan: Plan,
    pub vtables: Option<VirtualTableMap>,
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
        self.vtables.as_ref().map(VirtualTableMap::map)
    }

    pub fn get_mut_vtables(&mut self) -> Option<&mut HashMap<usize, Rc<VirtualTable>>> {
        self.vtables.as_mut().map(VirtualTableMap::mut_map)
    }

    pub fn set_vtables(&mut self, vtables: HashMap<usize, Rc<VirtualTable>>) {
        self.vtables = Some(VirtualTableMap::new(vtables));
    }

    /// Get motion virtual table
    ///
    /// # Errors
    /// - Failed to find a virtual table for the motion node.
    pub fn get_motion_vtable(&self, motion_id: usize) -> Result<Rc<VirtualTable>, SbroadError> {
        if let Some(vtable) = self.get_vtables() {
            if let Some(result) = vtable.get(&motion_id) {
                return Ok(Rc::clone(result));
            }
        }

        Err(SbroadError::NotFound(
            Entity::VirtualTable,
            format!("for Motion node ({motion_id})"),
        ))
    }

    /// Extract policy from motion node
    ///
    /// # Errors
    /// - node is not `Relation` type
    /// - node is not `Motion` type
    pub fn get_motion_policy(&self, node_id: usize) -> Result<MotionPolicy, SbroadError> {
        if let Relational::Motion { policy, .. } = &self.plan.get_relation_node(node_id)? {
            return Ok(policy.clone());
        }

        Err(SbroadError::Invalid(
            Entity::Relational,
            Some("invalid motion".into()),
        ))
    }

    /// Get motion alias name
    ///
    /// # Errors
    /// - node is not valid
    pub fn get_motion_alias(&self, node_id: usize) -> Result<Option<&String>, SbroadError> {
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
    pub fn get_motion_subtree_root(&self, node_id: usize) -> Result<usize, SbroadError> {
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
            Relational::Motion { .. } | Relational::Insert { .. } => Err(SbroadError::Invalid(
                Entity::Relational,
                Some("invalid motion child node".to_string()),
            )),
        }
    }

    /// Extract a child from the motion node. Motion node must contain only a single child.
    ///
    /// # Errors
    /// - node is not `Relation` type
    /// - node does not contain children
    pub(crate) fn get_motion_child(&self, node_id: usize) -> Result<usize, SbroadError> {
        let node = self.get_ir_plan().get_relation_node(node_id)?;
        if !node.is_motion() {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some(format!("current node ({node_id}) is not motion")),
            ));
        }

        let children = self.plan.get_relational_children(node_id)?.ok_or_else(|| {
            SbroadError::NotFound(Entity::Node, format!("that is Motion {node_id} child(ren)"))
        })?;

        if children.len() != 1 {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "Motion node ({}) must have once child only (actual {})",
                node_id,
                children.len()
            )));
        }

        let child_id = children.first().ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues("Motion has no children".to_string())
        })?;

        Ok(*child_id)
    }

    /// Extract subquery child node. Subquery node must contains only one child
    ///
    /// # Errors
    /// - node is not `Relation` type
    /// - node does not contain children
    fn get_subquery_child(&self, node_id: usize) -> Result<usize, SbroadError> {
        let node = self.get_ir_plan().get_relation_node(node_id)?;
        if !node.is_subquery() {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("current node ({node_id}) is not sub query")),
            ));
        }

        let children = self.plan.get_relational_children(node_id)?.ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("that is Subquery {node_id} child(ren)"),
            )
        })?;

        if children.len() != 1 {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "Sub query node ({}) must have once child only (actual {})",
                node_id,
                children.len()
            )));
        }

        let child_id = children.first().ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues("could not find subquery child".to_string())
        })?;

        Ok(*child_id)
    }

    /// Unlink the subtree of the motion node.
    ///
    /// # Errors
    /// - not a motion node
    pub fn unlink_motion_subtree(&mut self, motion_id: usize) -> Result<(), SbroadError> {
        let motion = self.get_mut_ir_plan().get_mut_relation_node(motion_id)?;
        if let Relational::Motion {
            ref mut children, ..
        } = motion
        {
            *children = vec![];
        } else {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some(format!("node ({motion_id}) is not motion")),
            ));
        }
        Ok(())
    }

    /// Build a new execution plan from the subtree of the existing execution plan.
    /// The operation is destructive and the subtree is removed from the original plan.
    ///
    /// # Errors
    /// - the original execution plan is invalid
    #[allow(clippy::too_many_lines)]
    pub fn take_subtree(&mut self, top_id: usize) -> Result<Self, SbroadError> {
        let nodes_capacity = self.get_ir_plan().nodes.len();
        // Translates the original plan's node id to the new sub-plan one.
        let mut translation: AHashMap<usize, usize> = AHashMap::with_capacity(nodes_capacity);
        let vtables_capacity = self.get_vtables().map_or_else(|| 1, HashMap::len);
        let mut new_vtables: HashMap<usize, Rc<VirtualTable>> =
            HashMap::with_capacity(vtables_capacity);
        let mut new_plan = Plan::new();
        new_plan.nodes.reserve(nodes_capacity);
        let mut subtree = PostOrder::with_capacity(
            |node| self.get_ir_plan().exec_plan_subtree_iter(node),
            self.get_ir_plan().next_id(),
        );
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();
        for (_, node_id) in nodes {
            // We have already processed this node (sub-queries in BETWEEN can be referred twice).
            if translation.contains_key(&node_id) {
                continue;
            }

            let dst_node = self.get_mut_ir_plan().get_mut_node(node_id)?;
            // Replace the node with some invalid value.
            // TODO: introduce some new enum variant for this purpose.
            let mut node: Node = std::mem::replace(dst_node, Node::Parameter);
            let ir_plan = self.get_ir_plan();
            let next_id = new_plan.nodes.next_id();
            match node {
                Node::Relational(ref mut rel) => {
                    if let Relational::ValuesRow { data, .. } = rel {
                        *data = *translation.get(data).ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Build,
                                Some(Entity::SubTree),
                                format!("could not find data node id {data} in the map"),
                            )
                        })?;
                    }

                    if let Relational::Motion { children, .. } = rel {
                        if let Some(vtable) =
                            self.get_vtables().map_or_else(|| None, |v| v.get(&node_id))
                        {
                            new_vtables.insert(next_id, Rc::clone(vtable));
                        }
                        *children = Vec::new();
                    }

                    if let Some(children) = rel.mut_children() {
                        for child_id in children {
                            *child_id = *translation.get(child_id).ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Build,
                                    Some(Entity::SubTree),
                                    format!("could not find child node id {child_id} in the map"),
                                )
                            })?;
                        }
                    }

                    let output = rel.output();
                    *rel.mut_output() = *translation.get(&output).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            format!("as output node {output} in relational node {rel:?}"),
                        )
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
                        // We transform selection's filter and join's condition to DNF for a better bucket calculation.
                        // But as a result we can produce an extremely verbose SQL query from such a plan (tarantool's
                        // parser can fail to parse such SQL).

                        // FIXME: UNDO operation can cause problems if we introduce more complicated transformations
                        // for filter/condition (but then the UNDO logic should be changed as well).
                        let undo_expr_id = ir_plan.undo.get_oldest(expr_id).unwrap_or(expr_id);
                        *expr_id = *translation.get(undo_expr_id).ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Build,
                                Some(Entity::SubTree),
                                format!(
                                    "could not find filter/condition node id {} in the map",
                                    undo_expr_id
                                ),
                            )
                        })?;
                        new_plan.replace_parent_in_subtree(*expr_id, None, Some(next_id))?;
                    }

                    if let Relational::ScanRelation { relation, .. }
                    | Relational::Insert { relation, .. } = rel
                    {
                        let table = ir_plan
                            .relations
                            .get(relation)
                            .ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Build,
                                    Some(Entity::SubTree),
                                    format!(
                                        "could not find relation {} in the original plan",
                                        relation
                                    ),
                                )
                            })?
                            .clone();
                        new_plan.add_rel(table);
                    }
                }
                Node::Expression(ref mut expr) => match expr {
                    Expression::Alias { ref mut child, .. }
                    | Expression::Cast { ref mut child, .. }
                    | Expression::Unary { ref mut child, .. } => {
                        *child = *translation.get(child).ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Build,
                                Some(Entity::SubTree),
                                format!("could not find child node id {child} in the map"),
                            )
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
                        *left = *translation.get(left).ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Build,
                                Some(Entity::SubTree),
                                format!("could not find left child node id {left} in the map"),
                            )
                        })?;
                        *right = *translation.get(right).ok_or_else(|| {
                            SbroadError::FailedTo(
                                Action::Build,
                                Some(Entity::SubTree),
                                format!("could not find right child node id {right} in the map"),
                            )
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
                            *child = *translation.get(child).ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Build,
                                    Some(Entity::SubTree),
                                    format!("could not find child node id {child} in the map"),
                                )
                            })?;
                        }
                    }
                    Expression::Constant { .. } => {}
                },
                Node::Parameter { .. } => {}
            }
            new_plan.nodes.push(node);
            translation.insert(node_id, next_id);
            if top_id == node_id {
                new_plan.set_top(next_id)?;
            }
        }

        new_plan.stash_constants()?;
        new_plan.nodes.shrink_to_fit();

        let vtables = if new_vtables.is_empty() {
            None
        } else {
            Some(VirtualTableMap::new(new_vtables))
        };
        let new_exec_plan = ExecutionPlan {
            plan: new_plan,
            vtables,
        };
        Ok(new_exec_plan)
    }

    /// # Errors
    /// - execution plan is invalid
    pub fn query_type(&self) -> Result<QueryType, SbroadError> {
        let top_id = self.get_ir_plan().get_top()?;
        let top = self.get_ir_plan().get_relation_node(top_id)?;
        if top.is_insert() {
            Ok(QueryType::DML)
        } else {
            Ok(QueryType::DQL)
        }
    }

    pub fn vtables_empty(&self) -> bool {
        self.get_vtables().map_or(true, HashMap::is_empty)
    }

    /// Calculates an engine for the virtual tables in the plan.
    /// The main problem is that we can't use different engines
    /// for an SQL statement within transaction.
    ///
    /// # Errors
    /// - execution plan is invalid
    /// - multiple engines are used in the plan
    pub fn vtable_engine(&self) -> Result<SpaceEngine, SbroadError> {
        let query_type = self.query_type()?;
        if query_type == QueryType::DQL {
            return Ok(SpaceEngine::Memtx);
        }

        let mut engine: Option<SpaceEngine> = None;
        for table in self.get_ir_plan().relations.tables.values() {
            let table_engine = table.engine.clone();
            if engine.is_none() {
                engine = Some(table_engine);
            } else if engine != Some(table_engine) {
                return Err(SbroadError::FailedTo(
                    Action::Build,
                    Some(Entity::Plan),
                    format!(
                        "cannot build execution plan for DML query with multiple engines: {:?}",
                        self.get_ir_plan().relations.tables
                    ),
                ));
            }
        }
        Ok(engine.map_or_else(|| SpaceEngine::Memtx, |e| e))
    }

    /// # Errors
    /// - execution plan is invalid
    pub fn connection_type(&self) -> Result<ConnectionType, SbroadError> {
        match self.query_type()? {
            QueryType::DML => Ok(ConnectionType::Write),
            QueryType::DQL => {
                if self.vtables_empty() {
                    Ok(ConnectionType::Read)
                } else {
                    Ok(ConnectionType::Write)
                }
            }
        }
    }
}
