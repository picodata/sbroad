use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::engine::Engine;
pub use crate::executor::engine::Metadata;
use crate::executor::ir::ExecutionPlan;
use crate::executor::result::BoxExecuteFormat;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::Plan;
use std::collections::HashMap;

mod bucket;
pub mod engine;
pub(crate) mod ir;
mod result;
mod vtable;

impl Plan {
    /// Apply optimization rules to the plan.
    fn optimize(&mut self) -> Result<(), QueryPlannerError> {
        self.replace_in_operator()?;
        self.split_columns()?;
        self.set_dnf()?;
        // TODO: make it a plan method and rename to "derive_equalities()".
        self.nodes.add_new_equalities()?;
        self.merge_tuples()?;
        self.add_motions()?;
        Ok(())
    }
}

/// Query object for executing
pub struct Query<T>
where
    T: Engine,
{
    /// Execution plan
    exec_plan: ExecutionPlan,
    /// Execution engine
    engine: T,
    /// Bucket map
    bucket_map: HashMap<usize, Buckets>,
}

impl<T> Query<T>
where
    T: Engine,
{
    /// Create a new query.
    ///
    /// # Errors
    /// - Failed to parse SQL.
    /// - Failed to build AST.
    /// - Failed to build IR plan.
    /// - Failed to apply optimizing transformations to IR plan.
    pub fn new(engine: T, sql: &str) -> Result<Self, QueryPlannerError>
    where
        T::Metadata: Metadata,
    {
        let ast = AbstractSyntaxTree::new(sql)?;
        let mut plan = ast.to_ir(engine.metadata())?;
        plan.optimize()?;
        let query = Query {
            exec_plan: ExecutionPlan::from(plan),
            engine,
            bucket_map: HashMap::new(),
        };
        Ok(query)
    }

    /// Execute distributed query.
    ///
    /// # Errors
    /// - Failed to get a motion subtree.
    /// - Failed to discover buckets.
    /// - Failed to materialize motion result and build a virtual table.
    /// - Failed to get plan top.
    pub fn exec(&mut self) -> Result<BoxExecuteFormat, QueryPlannerError> {
        let slices = self.exec_plan.get_ir_plan().get_slices();
        if let Some(slices) = slices {
            for slice in slices {
                for motion_id in slice {
                    // TODO: make it work in parallel
                    let top_id = self.exec_plan.get_motion_subtree_root(motion_id)?;
                    let buckets = self.bucket_discovery(top_id)?;
                    let virtual_table =
                        self.engine
                            .materialize_motion(&mut self.exec_plan, motion_id, &buckets)?;
                    self.exec_plan.add_motion_result(motion_id, virtual_table)?;
                }
            }
        }

        let top_id = self.exec_plan.get_ir_plan().get_top()?;
        let buckets = self.bucket_discovery(top_id)?;
        self.engine.exec(&mut self.exec_plan, top_id, &buckets)
    }
}

#[cfg(test)]
mod tests;
