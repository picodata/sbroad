//! Engine module.
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

pub trait QueryCache<Key, Value> {
    /// Builds a new cache with the given capacity.
    ///
    /// # Errors
    /// - Capacity is not valid (zero).
    fn new(capacity: usize) -> Result<Self, QueryPlannerError>
    where
        Self: Sized;

    /// Returns a value from the cache.
    ///
    /// # Errors
    /// - Internal error (should never happen).
    fn get(&mut self, key: &Key) -> Result<Option<Value>, QueryPlannerError>;

    /// Inserts a key-value pair into the cache.
    ///
    /// # Errors
    /// - Internal error (should never happen).
    fn put(&mut self, key: Key, value: Value) -> Result<(), QueryPlannerError>;
}

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

/// Local storage for uploading metadata.
#[derive(Debug)]
pub struct LocalMetadata {
    /// Cluster schema.
    pub schema: String,
    /// Query execution timeout.
    pub timeout: u64,
    /// Query cache capacity.
    pub capacity: usize,
    /// Sharding column name.
    pub sharding_column: String,
}

/// An execution engine trait.
pub trait Engine {
    type Metadata;
    type QueryCache;
    type ParseTree;

    /// Return object of metadata storage
    fn metadata(&self) -> &Self::Metadata
    where
        Self: Sized;

    /// Clear metadata information
    fn clear_metadata(&mut self);

    /// Check if the cache is empty.
    fn is_metadata_empty(&self) -> bool;

    /// Retrieve cluster metadata.
    ///
    /// # Errors
    /// - Internal error.
    fn get_metadata(&self) -> Result<Option<LocalMetadata>, QueryPlannerError>;

    /// Update cached metadata information.
    ///
    /// # Errors
    /// - Failed to update metadata information (invalid metadata).
    fn update_metadata(&mut self, metadata: LocalMetadata) -> Result<(), QueryPlannerError>;

    /// Flush the query cache.
    ///
    /// # Errors
    /// - Invalid capacity (zero).
    fn clear_query_cache(&self, capacity: usize) -> Result<(), QueryPlannerError>;

    fn query_cache(&self) -> &RefCell<Self::QueryCache>
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

    /// Execute sql query on the all shards in cluster
    ///
    /// # Errors
    /// - internal executor errors
    fn exec(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, QueryPlannerError>;

    /// Filter lua table values and return in right order
    ///
    /// # Errors
    /// - args does not contains all sharding keys
    /// - internal metadata errors
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
