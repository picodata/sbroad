//! Coordinator module.
//!
//! Traits that define an execution engine interface.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::executor::vtable::VirtualTable;
use crate::ir::value::Value;

pub mod cartridge;

/// A metadata storage trait of the cluster.
pub trait CoordinatorMetadata {
    /// Get a table by name.
    ///
    /// # Errors
    /// - Failed to get table by name from the metadata.
    fn get_table_segment(
        &self,
        table_name: &str,
    ) -> Result<crate::ir::relation::Table, QueryPlannerError>;

    fn get_exec_waiting_timeout(&self) -> u64;

    fn get_sharding_column(&self) -> &str;

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

/// Cluster configuration.
pub trait Configuration: Sized {
    type Configuration;

    /// Return a cached cluster configuration from the Rust memory.
    fn cached_config(&self) -> &Self::Configuration;

    /// Clear the cached cluster configuration in the Rust memory.
    fn clear_config(&mut self);

    /// Check if the cached cluster configuration is empty.
    fn is_config_empty(&self) -> bool;

    /// Retrieve cluster configuration from the Lua memory.
    ///
    /// # Errors
    /// - Internal error.
    fn get_config(&self) -> Result<Option<Self::Configuration>, QueryPlannerError>;

    /// Update cached cluster configuration.
    fn update_config(&mut self, metadata: Self::Configuration);
}

/// A coordinator trait.
pub trait Coordinator: Configuration {
    type Cache;
    type ParseTree;

    /// Flush the coordinator's IR cache.
    ///
    /// # Errors
    /// - Invalid capacity (zero).
    fn clear_ir_cache(&self, capacity: usize) -> Result<(), QueryPlannerError>;

    fn ir_cache(&self) -> &RefCell<Self::Cache>
    where
        Self: Sized;

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

    /// Dispatch a sql query to the shards in cluster and get the results.
    ///
    /// # Errors
    /// - internal executor errors
    fn dispatch(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, QueryPlannerError>;

    /// Extract a list of the sharding keys of the columns from the given space.
    ///
    /// # Errors
    /// - Columns are not the part of the sharding key of the space.
    fn extract_sharding_keys<'engine, 'rec>(
        &'engine self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, QueryPlannerError>;

    /// Determine shard for query execution by sharding key value
    fn determine_bucket_id(&self, s: &[&Value]) -> u64;
}

#[cfg(test)]
pub mod mock;
