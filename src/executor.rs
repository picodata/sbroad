use crate::errors::QueryPlannerError;
use crate::executor::engine::Engine;
pub use crate::executor::engine::Metadata;
use crate::executor::ir::ExecutionPlan;
use crate::executor::result::BoxExecuteFormat;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::Plan;

pub mod engine;
pub(crate) mod ir;
mod result;
mod shard;
mod vtable;

/// Query object for executing
#[allow(dead_code)]
pub struct Query<T>
where
    T: Engine,
{
    /// Query IR
    plan: Plan,
    /// Execute engine object
    engine: T,
}

#[allow(dead_code)]
impl<T> Query<T>
where
    T: Engine,
{
    /// Create query object
    ///
    /// # Errors
    /// - query isn't valid or not support yet.
    pub fn new(engine: T, sql: &str) -> Result<Self, QueryPlannerError>
    where
        T::Metadata: Metadata,
    {
        let ast = AbstractSyntaxTree::new(sql)?;
        Ok(Query {
            plan: ast.to_ir(&engine.metadata())?,
            engine,
        })
    }

    /// Execute query in cluster
    ///
    /// # Errors
    /// - query can't be executed in cluster
    /// - invalid bucket id
    pub fn exec(&self) -> Result<BoxExecuteFormat, QueryPlannerError> {
        let mut exec_plan = ExecutionPlan::from(&self.plan);

        let ir_plan = exec_plan.get_ir_plan();
        if let Some(slices) = ir_plan.get_slices() {
            for motion_level in slices {
                for motion_id in motion_level {
                    // TODO: make it work in parallel
                    let vtable = self.engine.materialize_motion(&mut exec_plan, motion_id)?;
                    exec_plan.add_motion_result(motion_id, vtable)?;
                }
            }
        }

        let top = exec_plan.get_ir_plan().get_top()?;

        self.engine.exec(&mut exec_plan, top)
    }

    /// Apply optimize rules
    ///
    /// # Errors
    /// - transformation can't be applied
    pub fn optimize(&mut self) -> Result<(), QueryPlannerError> {
        self.plan.replace_in_operator()?;
        self.plan.split_columns()?;
        self.plan.set_dnf()?;
        // TODO: make it a plan method and rename to "derive_equalities()".
        self.plan.nodes.add_new_equalities()?;
        self.plan.merge_tuples()?;
        self.plan.add_motions()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
