use std::collections::HashMap;
use std::rc::Rc;

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::Vshard;
use crate::executor::vtable::{VirtualTable, VirtualTableMap};
use crate::ir::expression::Expression;
use crate::ir::operator::{OrderByElement, OrderByEntity, Relational};
use crate::ir::relation::SpaceEngine;
use crate::ir::transformation::redistribution::{MotionOpcode, MotionPolicy};
use crate::ir::tree::traversal::PostOrder;
use crate::ir::{ExecuteOptions, Node, Plan};
use crate::otm::child_span;
use sbroad_proc::otm_child_span;

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

impl ConnectionType {
    #[must_use]
    pub fn is_readonly(&self) -> bool {
        match self {
            ConnectionType::Read => true,
            ConnectionType::Write => false,
        }
    }
}

/// Wrapper over `Plan` containing `vtables` map.
#[derive(Debug, Default, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ExecutionPlan {
    plan: Plan,
    /// Virtual tables for `Motion` nodes.
    /// Map of { `Motion` node_id -> it's corresponding data }
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

    #[must_use]
    pub fn get_vtable_max_rows(&self) -> u64 {
        self.plan.options.vtable_max_rows
    }

    #[must_use]
    pub fn get_execute_options(&self) -> &ExecuteOptions {
        &self.plan.options.execute_options
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
            format_smolstr!("for Motion node ({motion_id})"),
        ))
    }

    /// Add materialize motion result to translation map of virtual tables
    ///
    /// # Errors
    /// - invalid motion node
    #[otm_child_span("query.motion.add")]
    pub fn set_motion_vtable(
        &mut self,
        motion_id: usize,
        vtable: VirtualTable,
        runtime: &impl Vshard,
    ) -> Result<(), SbroadError> {
        let mut vtable = vtable;
        let program_len = if let Relational::Motion { program, .. } =
            self.get_ir_plan().get_relation_node(motion_id)?
        {
            program.0.len()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some("invalid motion node".into()),
            ));
        };
        for op_idx in 0..program_len {
            let plan = self.get_ir_plan();
            let opcode = plan.get_motion_opcode(motion_id, op_idx)?;
            match opcode {
                MotionOpcode::RemoveDuplicates => {
                    vtable.remove_duplicates();
                }
                MotionOpcode::ReshardIfNeeded => {
                    // Resharding must be done before applying projection
                    // to the virtual table. Otherwise projection can
                    // remove sharding columns.
                    match plan.get_motion_policy(motion_id)? {
                        MotionPolicy::Segment(shard_key)
                        | MotionPolicy::LocalSegment(shard_key) => {
                            vtable.reshard(shard_key, runtime)?;
                        }
                        MotionPolicy::Full | MotionPolicy::Local | MotionPolicy::None => {}
                    }
                }
                MotionOpcode::PrimaryKey(positions) => {
                    vtable.set_primary_key(positions)?;
                }
                MotionOpcode::RearrangeForShardedUpdate {
                    update_id,
                    old_shard_columns_len,
                    new_shard_columns_positions,
                } => {
                    let update_id = *update_id;

                    if let Some(v) = vtable.rearrange_for_update(
                        runtime,
                        *old_shard_columns_len,
                        new_shard_columns_positions,
                    )? {
                        let plan = self.get_mut_ir_plan();
                        plan.set_update_delete_tuple_len(update_id, v)?;
                    }
                }
                MotionOpcode::AddMissingRowsForLeftJoin { motion_id, .. } => {
                    let motion_id = *motion_id;
                    let Some(vtables) = &mut self.vtables else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "expected at least one virtual table".into(),
                        ));
                    };
                    let Some(from_vtable) = vtables.mut_map().remove(&motion_id) else {
                        return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                            "expected virtual table for motion {motion_id}"
                        )));
                    };
                    let from_vtable = Rc::try_unwrap(from_vtable).map_err(|_| {
                        SbroadError::FailedTo(
                            Action::Borrow,
                            Some(Entity::VirtualTable),
                            SmolStr::default(),
                        )
                    })?;
                    vtable.add_missing_rows(from_vtable)?;
                }
                MotionOpcode::SerializeAsEmptyTable(_) => {}
            }
        }

        let need_init = self.get_vtables().is_none();
        if need_init {
            self.set_vtables(HashMap::new());
        }

        if let Some(vtables) = self.get_mut_vtables() {
            vtables.insert(motion_id, Rc::new(vtable));
        }

        Ok(())
    }

    #[must_use]
    pub fn has_segmented_tables(&self) -> bool {
        self.vtables.as_ref().map_or(false, |vtable_map| {
            vtable_map
                .map()
                .values()
                .any(|t| !t.get_bucket_index().is_empty())
        })
    }

    /// Return true if plan needs to be customized for each storage.
    /// I.e we can't send the same plan to all storages.
    ///
    /// The check is done by iterating over the plan nodes arena,
    /// and checking whether motion node contains serialize as empty
    /// opcode. Be sure there are no dead nodes in the plan arena:
    /// nodes that are not referenced by actual plan tree.
    #[must_use]
    pub fn has_customization_opcodes(&self) -> bool {
        for node in &self.get_ir_plan().nodes {
            if let Node::Relational(Relational::Motion { program, .. }) = node {
                if program
                    .0
                    .iter()
                    .any(|op| matches!(op, MotionOpcode::SerializeAsEmptyTable(_)))
                {
                    return true;
                }
            }
        }
        false
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
    pub fn get_motion_alias(&self, node_id: usize) -> Result<Option<SmolStr>, SbroadError> {
        let child_id = &self.get_motion_child(node_id)?;
        let child_rel = self.get_ir_plan().get_relation_node(*child_id)?;
        match child_rel {
            Relational::ScanSubQuery { alias, .. } => Ok(alias.clone()),
            Relational::ScanCte { alias, .. } => Ok(Some(alias.clone())),
            _ => Ok(None),
        }
    }

    /// Get root from motion sub tree
    ///
    /// # Errors
    /// - node is not valid
    pub fn get_motion_subtree_root(&self, node_id: usize) -> Result<usize, SbroadError> {
        let top_id = &self.get_motion_child(node_id)?;
        let rel = self.get_ir_plan().get_relation_node(*top_id)?;
        match rel {
            Relational::ScanSubQuery { .. } | Relational::ScanCte { .. } => {
                self.get_ir_plan().get_relational_child(*top_id, 0)
            }
            Relational::Except { .. }
            | Relational::GroupBy { .. }
            | Relational::OrderBy { .. }
            | Relational::Intersect { .. }
            | Relational::Join { .. }
            | Relational::Projection { .. }
            | Relational::ScanRelation { .. }
            | Relational::Selection { .. }
            | Relational::Union { .. }
            | Relational::UnionAll { .. }
            | Relational::Update { .. }
            | Relational::Values { .. }
            | Relational::Having { .. }
            | Relational::ValuesRow { .. } => Ok(*top_id),
            Relational::Motion { .. } | Relational::Insert { .. } | Relational::Delete { .. } => {
                Err(SbroadError::Invalid(
                    Entity::Relational,
                    Some("invalid motion child node".to_smolstr()),
                ))
            }
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
                Some(format_smolstr!("current node ({node_id}) is not motion")),
            ));
        }

        let children = self.plan.get_relational_children(node_id)?;

        if children.len() != 1 {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "Motion node ({}) must have a single child only (actual {})",
                node_id,
                children.len()
            )));
        }

        let child_id = children.get(0).expect("Motion has no children");

        Ok(*child_id)
    }

    /// Unlink the subtree of the motion node.
    /// This logic is applied as an optimization: when we dispatch plan subtrees to the storages,
    /// we don't need to dispatch a subtrees of `Motion` nodes as soon as they are already replaced
    /// with vtables. So we replace its `children` field with an empty vec.
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
                Some(format_smolstr!("node ({motion_id}) is not motion")),
            ));
        }
        Ok(())
    }

    /// Build a new execution plan from the subtree of the existing execution plan.
    /// The operation is destructive and the subtree is removed from the original plan.
    ///
    /// # Errors
    /// - the original execution plan is invalid
    ///
    /// # Panics
    /// - Plan is in invalid state
    #[allow(clippy::too_many_lines)]
    pub fn take_subtree(&mut self, top_id: usize) -> Result<Self, SbroadError> {
        // Get the subtree nodes indexes.
        let plan = self.get_ir_plan();
        let mut subtree =
            PostOrder::with_capacity(|node| plan.exec_plan_subtree_iter(node), plan.next_id());
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();

        // We can't replace CTE subtree as it can be reused in other slices of the plan.
        // So, collect all CTE nodes and their subtree nodes (relational and expression)
        // as a set to avoid their removal.
        let cte_scans = nodes
            .iter()
            .map(|(_, id)| *id)
            .filter(|id| {
                matches!(
                    plan.get_node(*id),
                    Ok(Node::Relational(Relational::ScanCte { .. }))
                )
            })
            .collect::<Vec<_>>();

        // Get the capacity of the CTE nodes. We expect that CTE subtree nodes are located
        // in the beginning of the plan arena (while CTE scans can be located anywhere).
        // So, we get the biggest child id of the CTE nodes and add 1 to get the capacity.
        let mut all_cte_nodes_capacity = 0;
        let mut cte_amount = 0;
        for cte_id in &cte_scans {
            let cte_node = plan.get_relation_node(*cte_id)?;
            let Relational::ScanCte { child, .. } = cte_node else {
                unreachable!("Expected CTE scan node.");
            };
            let child_id = *child;
            if all_cte_nodes_capacity < child_id {
                all_cte_nodes_capacity = child_id;
            }
            cte_amount += 1;
        }
        all_cte_nodes_capacity += 1;
        let single_cte_capacity = if cte_amount <= 1 {
            all_cte_nodes_capacity
        } else {
            all_cte_nodes_capacity / cte_amount * 2
        };

        let mut cte_ids: AHashSet<usize> = AHashSet::new();
        let mut is_reserved = false;
        for cte_id in cte_scans {
            if !is_reserved {
                is_reserved = true;
                cte_ids.reserve(all_cte_nodes_capacity);
            }
            let mut cte_subtree = PostOrder::with_capacity(
                |node| plan.exec_plan_subtree_iter(node),
                single_cte_capacity,
            );
            for (_, id) in cte_subtree.iter(cte_id) {
                cte_ids.insert(id);
            }
        }

        // Translates the original plan's node id to the new sub-plan one.
        let mut translation: AHashMap<usize, usize> = AHashMap::with_capacity(nodes.len());
        let vtables_capacity = self.get_vtables().map_or_else(|| 1, HashMap::len);
        // Map of { plan node_id -> virtual table }.
        let mut new_vtables: HashMap<usize, Rc<VirtualTable>> =
            HashMap::with_capacity(vtables_capacity);

        let mut new_plan = Plan::new();
        new_plan.nodes.reserve(nodes.len());
        for (_, node_id) in nodes {
            // We have already processed this node (sub-queries in BETWEEN
            // and CTEs can be referred twice).
            if translation.contains_key(&node_id) {
                continue;
            }

            // Node from original plan that we'll take and replace with mock parameter node.
            let dst_node = self.get_mut_ir_plan().get_mut_node(node_id)?;
            let next_id = new_plan.nodes.next_id();

            // Replace the node with some invalid value.
            // TODO: introduce some new enum variant for this purpose.
            let mut node: Node = if cte_ids.contains(&node_id) {
                dst_node.clone()
            } else {
                std::mem::replace(dst_node, Node::Parameter)
            };
            let ir_plan = self.get_ir_plan();
            match node {
                Node::Relational(ref mut rel) => {
                    match rel {
                        Relational::Selection {
                            filter: ref mut expr_id,
                            ..
                        }
                        | Relational::Having {
                            filter: ref mut expr_id,
                            ..
                        }
                        | Relational::Join {
                            condition: ref mut expr_id,
                            ..
                        } => {
                            // We transform selection's, having's filter and join's condition to DNF for a better bucket calculation.
                            // But as a result we can produce an extremely verbose SQL query from such a plan (tarantool's
                            // parser can fail to parse such SQL).

                            // XXX: UNDO operation can cause problems if we introduce more complicated transformations
                            // for filter/condition (but then the UNDO logic should be changed as well).
                            let undo_expr_id = ir_plan.undo.get_oldest(expr_id).unwrap_or(expr_id);
                            *expr_id = *translation.get(undo_expr_id).unwrap_or_else(|| panic!("Could not find filter/condition node id {undo_expr_id} in the map."));
                            new_plan.replace_parent_in_subtree(*expr_id, None, Some(next_id))?;
                        }
                        Relational::ScanRelation { relation, .. }
                        | Relational::Insert { relation, .. }
                        | Relational::Delete { relation, .. }
                        | Relational::Update { relation, .. } => {
                            let table = ir_plan
                                .relations
                                .get(relation)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "could not find relation {relation} in the original plan"
                                    )
                                })
                                .clone();
                            new_plan.add_rel(table);
                        }
                        Relational::Motion {
                            children, policy, ..
                        } => {
                            if let Some(vtable) =
                                self.get_vtables().map_or_else(|| None, |v| v.get(&node_id))
                            {
                                new_vtables.insert(next_id, Rc::clone(vtable));
                            }
                            // We should not remove the child of a local motion node.
                            // The subtree is needed to complie the SQL on the storage.
                            if !policy.is_local() {
                                *children = Vec::new();
                            }
                        }
                        Relational::GroupBy { gr_cols, .. } => {
                            let mut new_cols: Vec<usize> = Vec::with_capacity(gr_cols.len());
                            for col_id in gr_cols.iter() {
                                let new_col_id = *translation.get(col_id).unwrap_or_else(|| {
                                    panic!("Grouping column {col_id} in translation map.")
                                });
                                new_plan.replace_parent_in_subtree(
                                    new_col_id,
                                    None,
                                    Some(next_id),
                                )?;
                                new_cols.push(new_col_id);
                            }
                            *gr_cols = new_cols;
                        }
                        Relational::OrderBy {
                            order_by_elements, ..
                        } => {
                            let mut new_elements: Vec<OrderByElement> =
                                Vec::with_capacity(order_by_elements.len());
                            for element in order_by_elements.iter() {
                                let new_entity = match element.entity {
                                    OrderByEntity::Expression { expr_id } => {
                                        let new_element_id =
                                            *translation.get(&expr_id).unwrap_or_else(|| {
                                                panic!("ORDER BY element {element:?} not found in translation map.")
                                            });
                                        new_plan.replace_parent_in_subtree(
                                            new_element_id,
                                            None,
                                            Some(next_id),
                                        )?;
                                        OrderByEntity::Expression {
                                            expr_id: new_element_id,
                                        }
                                    }
                                    OrderByEntity::Index { value } => {
                                        OrderByEntity::Index { value }
                                    }
                                };
                                new_elements.push(OrderByElement {
                                    entity: new_entity,
                                    order_type: element.order_type.clone(),
                                });
                            }
                            *order_by_elements = new_elements;
                        }
                        Relational::ValuesRow { data, .. } => {
                            *data = *translation.get(data).unwrap_or_else(|| {
                                panic!("Could not find data node id {data} in the map.")
                            });
                        }
                        Relational::Except { .. }
                        | Relational::Intersect { .. }
                        | Relational::Projection { .. }
                        | Relational::ScanSubQuery { .. }
                        | Relational::ScanCte { .. }
                        | Relational::Union { .. }
                        | Relational::UnionAll { .. }
                        | Relational::Values { .. } => {}
                    }

                    for child_id in rel.mut_children() {
                        *child_id = *translation.get(child_id).unwrap_or_else(|| {
                            panic!("Could not find child node id {child_id} in the map.")
                        });
                    }

                    let output = rel.output();
                    *rel.mut_output() = *translation.get(&output).unwrap_or_else(|| {
                        panic!("Node not found as output node {output} in relational node {rel:?}.")
                    });
                    new_plan.replace_parent_in_subtree(rel.output(), None, Some(next_id))?;
                }
                Node::Expression(ref mut expr) => match expr {
                    Expression::Alias { ref mut child, .. }
                    | Expression::ExprInParentheses { ref mut child }
                    | Expression::Cast { ref mut child, .. }
                    | Expression::Unary { ref mut child, .. } => {
                        *child = *translation.get(child).unwrap_or_else(|| {
                            panic!("Could not find child node id {child} in the map.")
                        });
                    }
                    Expression::Bool {
                        ref mut left,
                        ref mut right,
                        ..
                    }
                    | Expression::Arithmetic {
                        ref mut left,
                        ref mut right,
                        ..
                    }
                    | Expression::Concat {
                        ref mut left,
                        ref mut right,
                        ..
                    } => {
                        *left = *translation.get(left).unwrap_or_else(|| {
                            panic!("Could not find left child node id {left} in the map.")
                        });
                        *right = *translation.get(right).unwrap_or_else(|| {
                            panic!("Could not find right child node id {right} in the map.")
                        });
                    }
                    Expression::Trim {
                        ref mut pattern,
                        ref mut target,
                        ..
                    } => {
                        if let Some(pattern) = pattern {
                            *pattern = *translation.get(pattern).unwrap_or_else(|| {
                                panic!("Could not find pattern node id {pattern} in the map.")
                            });
                        }
                        *target = *translation.get(target).unwrap_or_else(|| {
                            panic!("Could not find target node id {target} in the map.")
                        });
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
                            *child = *translation.get(child).unwrap_or_else(|| {
                                panic!("Could not find child node id {child} in the map.")
                            });
                        }
                    }
                    Expression::Constant { .. } | Expression::CountAsterisk => {}
                    Expression::Case {
                        search_expr,
                        when_blocks,
                        else_expr,
                    } => {
                        if let Some(search_expr) = search_expr {
                            *search_expr = *translation.get(search_expr).unwrap_or_else(|| {
                                panic!("Could not find search expression {search_expr} in the map.")
                            });
                        }
                        for (cond_expr, res_expr) in when_blocks {
                            *cond_expr = *translation.get(cond_expr).unwrap_or_else(|| {
                                panic!(
                                    "Could not find cond WHEN expression {cond_expr} in the map."
                                )
                            });
                            *res_expr = *translation.get(res_expr).unwrap_or_else(|| {
                                panic!("Could not find res THEN expression {res_expr} in the map.")
                            });
                        }
                        if let Some(else_expr) = else_expr {
                            *else_expr = *translation.get(else_expr).unwrap_or_else(|| {
                                panic!("Could not find else expression {else_expr} in the map.")
                            });
                        }
                    }
                },
                Node::Parameter { .. } => {}
                Node::Ddl { .. } | Node::Acl { .. } | Node::Block { .. } => {
                    panic!("Unexpected node in `take_subtree`: {node:?}")
                }
            }
            new_plan.nodes.push(node);
            translation.insert(node_id, next_id);
            if top_id == node_id {
                new_plan.set_top(next_id)?;
            }
        }

        new_plan.stash_constants()?;
        new_plan.options = self.get_ir_plan().options.clone();

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
        if top.is_dml() {
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
            let table_engine = table.engine();
            if engine.is_none() {
                engine = Some(table_engine);
            } else if engine != Some(table_engine) {
                return Err(SbroadError::FailedTo(
                    Action::Build,
                    Some(Entity::Plan),
                    format_smolstr!(
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
