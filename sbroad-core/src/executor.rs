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
use std::rc::Rc;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::{helpers::materialize_values, Router, Vshard};
use crate::executor::ir::ExecutionPlan;
use crate::executor::lru::Cache;
use crate::frontend::Ast;
use crate::ir::operator::Relational;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::otm::{child_span, query_id};
use ahash::AHashMap;
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
    pub(crate) fn optimize(&mut self) -> Result<(), SbroadError> {
        self.replace_in_operator()?;
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
    /// Bucket map
    bucket_map: HashMap<usize, Buckets>,
}

impl<'a, C> Query<'a, C>
where
    C: Router + Vshard,
    &'a C: Vshard,
{
    /// Create a new query.
    ///
    /// # Errors
    /// - Failed to parse SQL.
    /// - Failed to build AST.
    /// - Failed to build IR plan.
    /// - Failed to apply optimizing transformations to IR plan.
    #[otm_child_span("query.new")]
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
            let ast = C::ParseTree::new(sql)?;
            let metadata = &*coordinator.metadata()?;
            plan = ast.resolve_metadata(metadata)?;
            if !plan.is_ddl()? {
                cache.put(key, plan.clone())?;
            }
        }
        if !plan.is_ddl()? {
            plan.bind_params(params)?;
            plan.apply_options()?;
            plan.optimize()?;
        }
        let query = Query {
            is_explain: plan.is_expain(),
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
        let mut already_materialized: AHashMap<usize, usize> =
            AHashMap::with_capacity(slices.slices().len());
        for slice in slices.slices() {
            // TODO: make it work in parallel
            for motion_id in slice.positions() {
                let top_id = self.exec_plan.get_motion_subtree_root(*motion_id)?;

                // Multiple motions can point to the same subtree (BETWEEN operator).
                if let Some(id) = already_materialized.get(&top_id) {
                    let vtable = self.get_exec_plan().get_motion_vtable(*id)?;
                    if let Some(vtables) = self.get_mut_exec_plan().get_mut_vtables() {
                        vtables.insert(*motion_id, Rc::clone(&vtable));
                    }
                    // Unlink the subtree under the motion node
                    // (it has already been materialized and replaced with invalid nodes).
                    // Such a logic is applied in `materialize_motion`, but only for the
                    // first `Motion` (that e.g. pointed to BETWEEN).
                    // We have to apply it again, but for current `motion_id` now.
                    self.get_mut_exec_plan().unlink_motion_subtree(*motion_id)?;
                    continue;
                }

                // Local segment motions should be treated as a special case.
                // 1. If we can materialize it on the router, then we should do it
                //    (the child node is `VALUES` of constants).
                // 2. Otherwise we should skip it and dispatch the query to the segments
                //    (materialization would be done on the segments).
                let motion = self.exec_plan.get_ir_plan().get_relation_node(*motion_id)?;
                if let Relational::Motion {
                    policy: MotionPolicy::LocalSegment(_),
                    ..
                } = motion
                {
                    if let Some(virtual_table) =
                        materialize_values(&mut self.exec_plan, *motion_id)?
                    {
                        self.exec_plan.set_motion_vtable(
                            *motion_id,
                            virtual_table,
                            &self.coordinator,
                        )?;
                        self.get_mut_exec_plan().unlink_motion_subtree(*motion_id)?;
                        already_materialized.insert(top_id, *motion_id);
                    }
                    continue;
                }

                let buckets = self.bucket_discovery(top_id)?;
                let virtual_table = self.coordinator.materialize_motion(
                    &mut self.exec_plan,
                    *motion_id,
                    &buckets,
                )?;
                self.exec_plan
                    .set_motion_vtable(*motion_id, virtual_table, &self.coordinator)?;
                already_materialized.insert(top_id, *motion_id);
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

    /// Checks that query is DDL.
    ///
    /// # Errors
    /// - Plan is invalid.
    pub fn is_ddl(&self) -> Result<bool, SbroadError> {
        self.exec_plan.get_ir_plan().is_ddl()
    }
}

#[cfg(test)]
mod tests;
