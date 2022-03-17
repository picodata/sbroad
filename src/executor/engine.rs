use crate::errors::QueryPlannerError;
use crate::executor::ir::ExecutionPlan;
use crate::executor::result::BoxExecuteFormat;
use crate::executor::vtable::VirtualTable;

pub mod cartridge;

/// `Metadata` trait is interface for working with metadata storage, that was needed the query execute.
pub trait Metadata {
    fn get_table_segment(
        &self,
        table_name: &str,
    ) -> Result<crate::ir::relation::Table, QueryPlannerError>;

    fn to_name(s: &str) -> String {
        if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
            s.to_string()
        } else if s.to_uppercase() == s {
            s.to_lowercase()
        } else {
            format!("\"{}\"", s)
        }
    }
}

/// `Engine` trait is interface for working with execution engine.
pub trait Engine {
    type Metadata;

    /// Return object of metadata storage
    fn metadata(&self) -> Self::Metadata
    where
        Self: Sized;

    /// Checking that metadata isn't empty
    fn has_metadata(&self) -> bool;

    /// Clear metadata information
    fn clear_metadata(&mut self);

    /// Load metadate information to storage
    fn load_metadata(&mut self) -> Result<(), QueryPlannerError>;

    /// Materialize result motion node to virtual table
    ///
    /// # Errors
    /// - internal executor errors
    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
    ) -> Result<VirtualTable, QueryPlannerError>;

    /// Execute sql query on the all shards in cluster
    ///
    /// # Errors
    /// - internal executor errors
    fn exec(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
    ) -> Result<BoxExecuteFormat, QueryPlannerError>;

    /// Determine shard for query execution by sharding key value
    fn determine_bucket_id(&self, s: &str) -> usize;
}

#[cfg(test)]
pub mod mock;