//! Executor module.
//!
//! The executor is located on the coordinator node in the cluster.
//! It collects all the intermediate results of the plan execution
//! in memory and executes the IR plan tree in the bottom-up manner.
//! It goes like this:
//!
//! 1. The executor collects all the motion nodes from the bottom layer.
//!    In theory all the motions in the same layer can be executed in parallel
//!    (this feature is yet to come).
//! 2. For every motion the executor:
//!    - inspects the IR sub-tree and detects the buckets to execute the query for.
//!    - builds a valid SQL query from the IR sub-tree.
//!    - performs map-reduce for that SQL query (we send it to the shards deduced from the buckets).
//!    - builds a virtual table with query results that correspond to the original motion.
//! 3. Moves to the next motion layer in the IR tree.
//! 4. For every motion the executor then:
//!    - links the virtual table results of the motion from the previous layer we depend on.
//!    - inspects the IR sub-tree and detects the buckets to execute the query.
//!    - builds a valid SQL query from the IR sub-tree.
//!    - performs map-reduce for that SQL query.
//!    - builds a virtual table with query results that correspond to the original motion.
//! 5. Repeats step 3 till we are done with motion layers.
//! 6. Executes the final IR top subtree and returns the final result to the user.

use std::any::Any;
use std::collections::HashMap;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::helpers::normalize_name_for_space_api;
use crate::executor::engine::{helpers::materialize_values, Router, TableVersionMap, Vshard};
use crate::executor::ir::ExecutionPlan;
use crate::executor::lru::Cache;
use crate::frontend::Ast;
use crate::ir::operator::Relational;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::otm::{child_span, query_id};
use sbroad_proc::otm_child_span;

pub mod bucket;
pub mod engine;
pub mod hash;
pub mod ir;
pub mod lru;
pub mod protocol;
pub mod result;
pub mod vtable;

impl Plan {
    /// Apply optimization rules to the plan.
    ///
    /// # Errors
    /// - Failed to optimize the plan.
    pub fn optimize(&mut self) -> Result<(), SbroadError> {
        self.replace_in_operator()?;
        self.push_down_not()?;
        self.split_columns()?;
        self.set_dnf()?;
        self.derive_equalities()?;
        self.merge_tuples()?;
        self.add_motions()?;
        Ok(())
    }
}

/// Query to execute.
#[derive(Debug)]
pub struct Query<'a, C>
where
    C: Router + Vshard,
    &'a C: Vshard,
{
    /// Explain flag
    is_explain: bool,
    /// Execution plan
    exec_plan: ExecutionPlan,
    /// Coordinator runtime
    coordinator: &'a C,
    /// Bucket map of view { plan output_id (Expression::Row) -> `Buckets` }.
    /// It's supposed to denote relational nodes' output buckets destination.
    bucket_map: HashMap<usize, Buckets>,
}

impl<'a, C> Query<'a, C>
where
    C: Router + Vshard,
    &'a C: Vshard,
{
    pub fn from_parts(
        is_explain: bool,
        exec_plan: ExecutionPlan,
        coordinator: &'a C,
        bucket_map: HashMap<usize, Buckets>,
    ) -> Self {
        Self {
            is_explain,
            exec_plan,
            coordinator,
            bucket_map,
        }
    }

    /// Create a new query.
    ///
    /// # Errors
    /// - Failed to parse SQL.
    /// - Failed to build AST.
    /// - Failed to build IR plan.
    /// - Failed to apply optimizing transformations to IR plan.
    pub fn new(coordinator: &'a C, sql: &str, params: Vec<Value>) -> Result<Self, SbroadError>
    where
        C::Cache: Cache<String, Plan>,
        C::ParseTree: Ast,
    {
        let key = query_id(sql);
        let ir_cache = coordinator.cache();

        let mut plan = Plan::new();
        let mut cache = ir_cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Create, Some(Entity::Query), format!("{e:?}"))
        })?;
        if let Some(cached_plan) = cache.get(&key)? {
            plan = cached_plan.clone();
        }
        if plan.is_empty() {
            let metadata = &*coordinator.metadata()?;
            plan = C::ParseTree::transform_into_plan(sql, metadata)?;
            if coordinator.provides_versions() {
                let mut table_version_map =
                    TableVersionMap::with_capacity(plan.relations.tables.len());
                for (tbl_name, tbl) in &plan.relations.tables {
                    if tbl.is_system() {
                        continue;
                    }
                    let normalized = normalize_name_for_space_api(tbl_name);
                    let version = coordinator.get_table_version(normalized.as_str())?;
                    table_version_map.insert(normalized, version);
                }
                plan.version_map = table_version_map;
            }
            if !plan.is_ddl()? && !plan.is_acl()? {
                cache.put(key, plan.clone())?;
            }
        }
        if plan.is_block()? {
            plan.bind_params(params)?;
        } else if !plan.is_ddl()? && !plan.is_acl()? {
            plan.bind_params(params)?;
            plan.apply_options()?;
            plan.optimize()?;
        }
        let query = Query {
            is_explain: plan.is_explain(),
            exec_plan: ExecutionPlan::from(plan),
            coordinator,
            bucket_map: HashMap::new(),
        };
        Ok(query)
    }

    /// Get the execution plan of the query.
    #[must_use]
    pub fn get_exec_plan(&self) -> &ExecutionPlan {
        &self.exec_plan
    }

    /// Get the mutable reference to the execution plan of the query.
    #[must_use]
    pub fn get_mut_exec_plan(&mut self) -> &mut ExecutionPlan {
        &mut self.exec_plan
    }

    /// Get the coordinator runtime of the query.
    #[must_use]
    pub fn get_coordinator(&self) -> &C {
        self.coordinator
    }

    /// Dispatch a distributed query from coordinator to the segments.
    ///
    /// # Errors
    /// - Failed to get a motion subtree.
    /// - Failed to discover buckets.
    /// - Failed to materialize motion result and build a virtual table.
    /// - Failed to get plan top.
    #[otm_child_span("query.dispatch")]
    pub fn dispatch(&mut self) -> Result<Box<dyn Any>, SbroadError> {
        if self.is_explain() {
            return self.coordinator.explain_format(self.to_explain()?);
        }
        self.get_mut_exec_plan()
            .get_mut_ir_plan()
            .restore_constants()?;

        let slices = self.exec_plan.get_ir_plan().clone_slices();
        for slice in slices.slices() {
            // TODO: make it work in parallel
            for motion_id in slice.positions() {
                if let Some(vtables_map) = self.exec_plan.get_vtables() {
                    if vtables_map.contains_key(motion_id) {
                        continue;
                    }
                }
                let motion = self.exec_plan.get_ir_plan().get_relation_node(*motion_id)?;

                if let Relational::Motion { policy, .. } = motion {
                    match policy {
                        MotionPolicy::Segment(_) => {
                            // if child is values, then we can materialize it
                            // on the router.
                            if let Some(virtual_table) =
                                materialize_values(&mut self.exec_plan, *motion_id)?
                            {
                                self.exec_plan.set_motion_vtable(
                                    *motion_id,
                                    virtual_table,
                                    &self.coordinator,
                                )?;
                                self.get_mut_exec_plan().unlink_motion_subtree(*motion_id)?;
                            }
                        }
                        // Skip it and dispatch the query to the segments
                        // (materialization would be done on the segments). Note that we
                        // will operate with vtables for LocalSegment motions via calls like
                        // `if let Ok(virtual_table) = self.exec_plan.get_motion_vtable(node_id)`
                        // in order to define whether virtual table was materialized for values.
                        MotionPolicy::LocalSegment(_) => {
                            continue;
                        }
                        // Local policy should be skipped and dispatched to the segments:
                        // materialization would be done there.
                        MotionPolicy::Local => continue,
                        _ => {}
                    }
                }

                let top_id = self.exec_plan.get_motion_subtree_root(*motion_id)?;
                let buckets = self.bucket_discovery(top_id)?;
                let virtual_table = self.coordinator.materialize_motion(
                    &mut self.exec_plan,
                    *motion_id,
                    &buckets,
                )?;
                self.exec_plan
                    .set_motion_vtable(*motion_id, virtual_table, &self.coordinator)?;
            }
        }
        let top_id = self.exec_plan.get_ir_plan().get_top()?;
        let buckets = self.bucket_discovery(top_id)?;
        self.coordinator
            .dispatch(&mut self.exec_plan, top_id, &buckets)
    }

    /// Query explain
    ///
    /// # Errors
    /// - Failed to build explain
    pub fn to_explain(&self) -> Result<String, SbroadError> {
        self.exec_plan.get_ir_plan().as_explain()
    }

    /// Checks that query is explain and have not to be executed
    fn is_explain(&self) -> bool {
        self.is_explain
    }

    /// Checks that query is a statement block.
    ///
    /// # Errors
    /// - plan is invalid
    pub fn is_block(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_block()
    }

    /// Checks that query is DDL.
    ///
    /// # Errors
    /// - Plan is invalid.
    pub fn is_ddl(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_ddl()
    }

    /// Checks that query is ACL.
    ///
    /// # Errors
    /// - Plan is invalid
    pub fn is_acl(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_acl()
    }
}

#[cfg(test)]
mod tests;
