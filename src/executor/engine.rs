//! Engine module.
//!
//! Traits that define an execution engine interface.

use std::collections::HashMap;

use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::executor::result::BoxExecuteFormat;
use crate::executor::vtable::VirtualTable;
use crate::ir::value::Value as IrValue;

pub mod cartridge;

/// A metadata storage trait of the cluster.
pub trait Metadata {
    /// Get a table by name.
    ///
    /// # Errors
    /// - Failed to get table by name from the metadata.
    fn get_table_segment(
        &self,
        table_name: &str,
    ) -> Result<crate::ir::relation::Table, QueryPlannerError>;

    fn get_exec_waiting_timeout(&self) -> u64;

    #[must_use]
    fn to_name(s: &str) -> String {
        if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
            s.to_string()
        } else if s.to_uppercase() == s {
            s.to_lowercase()
        } else {
            format!("\"{}\"", s)
        }
    }

    /// Provides an `Vec<&str>` with sharding keys or an error
    ///
    /// # Errors
    /// - Metadata does not contains space
    /// - Metadata contains incorrect sharding keys format
    fn get_sharding_key_by_space(&self, space: &str) -> Result<Vec<&str>, QueryPlannerError>;
}

/// An execution engine trait.
pub trait Engine {
    type Metadata;

    /// Return object of metadata storage
    fn metadata(&self) -> &Self::Metadata
    where
        Self: Sized;

    /// Checking that metadata isn't empty
    fn has_metadata(&self) -> bool;

    /// Clear metadata information
    fn clear_metadata(&mut self);

    /// Load metadate information to storage
    ///
    /// # Errors
    /// - Failed to load metadata.
    fn load_metadata(&mut self) -> Result<(), QueryPlannerError>;

    /// Materialize result motion node to virtual table
    ///
    /// # Errors
    /// - internal executor errors
    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
        buckets: &Buckets,
    ) -> Result<VirtualTable, QueryPlannerError>;

    /// Execute sql query on the all shards in cluster
    ///
    /// # Errors
    /// - internal executor errors
    fn exec(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<BoxExecuteFormat, QueryPlannerError>;

    /// Filter lua table values and return in right order
    ///
    /// # Errors
    /// - args does not contains all sharding keys
    /// - internal metadata errors
    fn extract_sharding_keys(
        &self,
        space: String,
        args: HashMap<String, IrValue>,
    ) -> Result<Vec<IrValue>, QueryPlannerError>;

    /// Determine shard for query execution by sharding key value
    fn determine_bucket_id(&self, s: &str) -> u64;
}

#[cfg(test)]
pub mod mock;
