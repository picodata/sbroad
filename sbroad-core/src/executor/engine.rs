//! Coordinator module.
//!
//! Traits that define an execution engine interface.

use crate::cbo::histogram::Scalar;
use crate::cbo::{ColumnStats, TableColumnPair, TableStats};
use std::any::Any;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use crate::errors::SbroadError;
use crate::executor::bucket::Buckets;
use crate::executor::engine::helpers::storage::PreparedStmt;
use crate::executor::ir::ExecutionPlan;
use crate::executor::protocol::SchemaInfo;
use crate::executor::vtable::VirtualTable;
use crate::ir::function::Function;
use crate::ir::relation::Table;
use crate::ir::value::Value;

use super::ir::{ConnectionType, QueryType};
use super::protocol::Binary;

pub mod helpers;
#[cfg(feature = "mock")]
pub mod mock;

/// A metadata trait of the cluster (getters for tables, functions, etc.).
pub trait Metadata: Sized {
    /// Get a table by name that contains:
    /// * list of the columns,
    /// * distribution key of the output tuples (column positions),
    /// * table name.
    ///
    /// # Errors
    /// - Failed to get table by name from the metadata.
    fn table(&self, table_name: &str) -> Result<Table, SbroadError>;

    /// Lookup for a function in the metadata cache.
    ///
    /// # Errors
    /// - Failed to get function by name from the metadata.
    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError>;

    /// Get the wait timeout for the query execution.
    fn waiting_timeout(&self) -> u64;

    /// Get the name of the sharding column (usually it is `bucket_id`).
    fn sharding_column(&self) -> &str;

    /// Provides vector of the sharding key column names or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    /// - Metadata contains incorrect sharding key format
    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, SbroadError>;

    /// Provides vector of the sharding key column positions in a tuple or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError>;
}

pub trait StorageCache {
    /// Put the prepared statement with given key in cache,
    /// remembering its version.
    ///
    /// # Errors
    /// - failed to get schema version for some table
    /// - invalid `schema_info`
    fn put(
        &mut self,
        plan_id: String,
        stmt: PreparedStmt,
        schema_info: &SchemaInfo,
    ) -> Result<(), SbroadError>;

    /// Get the prepared statement from cache.
    /// If the schema version for some table has
    /// been changed, `None` is returned.
    ///
    /// # Errors
    /// - failed to get schema version for some table
    #[allow(clippy::ptr_arg)]
    fn get(&mut self, plan_id: &String) -> Result<Option<&PreparedStmt>, SbroadError>;

    /// Clears the cache.
    ///
    /// # Errors
    /// - internal errors from implementation
    fn clear(&mut self) -> Result<(), SbroadError>;
}

pub type TableVersionMap = HashMap<String, u64>;

pub trait QueryCache {
    type Cache;

    /// Get the cache.
    ///
    /// # Errors
    /// - Failed to get the cache.
    fn cache(&self) -> &RefCell<Self::Cache>
    where
        Self: Sized;

    /// Get the cache capacity.
    ///
    /// # Errors
    /// - Failed to get the cache capacity.
    fn cache_capacity(&self) -> Result<usize, SbroadError>;

    /// Clear the cache.
    ///
    /// # Errors
    /// - Failed to clear the cache.
    fn clear_cache(&self) -> Result<(), SbroadError>;

    /// `true` if cache can provide a schema version
    /// for given table. Only used for picodata,
    /// cartridge does not need this.
    fn provides_versions(&self) -> bool;

    /// Return current schema version of given table.
    ///
    /// Must be called only if `provides_versions` returns
    /// `true`.
    ///
    /// # Errors
    /// - table was not found in system space
    /// - could not access the system space
    fn get_table_version(&self, _: &str) -> Result<u64, SbroadError>;
}

/// A router trait.
pub trait Router: QueryCache {
    type ParseTree;
    type MetadataProvider: Metadata;

    /// Get the metadata provider (tables, functions, etc.).
    ///
    /// # Errors
    /// - Internal error. Under normal conditions we should always return
    ///   metadata successfully.
    fn metadata(&self) -> Result<Ref<Self::MetadataProvider>, SbroadError>;

    /// Setup output format of query explain
    ///
    /// # Errors
    /// - Internal error. Under normal conditions we should always return
    ///   formatted explain successfully.
    fn explain_format(&self, explain: String) -> Result<Box<dyn Any>, SbroadError>;

    /// Extract a list of the sharding key values from a map for the given space.
    ///
    /// # Errors
    /// - Columns are not present in the sharding key of the space.
    fn extract_sharding_key_from_map<'rec>(
        &self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError>;

    /// Extract a list of the sharding key values from a tuple for the given space.
    ///
    /// # Errors
    /// - Internal error in the table (should never happen, but we recheck).
    fn extract_sharding_key_from_tuple<'rec>(
        &self,
        space: String,
        args: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError>;

    /// Dispatch a sql query to the shards in cluster and get the results.
    ///
    /// # Errors
    /// - internal executor errors
    fn dispatch(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError>;

    /// Materialize result motion node to virtual table
    ///
    /// # Errors
    /// - internal executor errors
    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
        buckets: &Buckets,
    ) -> Result<VirtualTable, SbroadError>;
}

/// Struct representing table stats that we get directly from replicaset system table.
/// **Note**: `*_counter` fields are specific for each of the replicasets and should not be merged.
/// Node that will make statistics calculations need only the `rows_number` field.
///
/// **Note**: All the statistics structures with a __Storage__ prefix represent structures that we construct
/// on storages and then send to the router node (C structure may look differently).
#[allow(dead_code)]
pub struct StorageTableStats {
    /// Number of rows in the table.
    rows_number: u64,
    /// Counter of executed INSERT operations.
    ///
    /// We need to store counter in order to understand when to
    /// actualize table statistics.
    insert_counter: u64,
    /// Counter of executed UPDATE operations.
    update_counter: u64,
    /// Counter of executed REMOVE operations.
    remove_counter: u64,
}

/// A `CostBased` statistics trait.
pub trait Statistics {
    /// Get `TableStats` for table by its name from storages.
    ///
    /// # Errors
    /// - Table statistics can not be gathered neither from the cache nor from the storages.
    fn get_table_stats(&self, table_name: String) -> Result<Rc<TableStats>, SbroadError>;

    /// Get upcasted `ColumnStats` for column by its table name and column name from storages.
    ///
    /// # Errors
    /// - Initial column statistics can not be gathered neither from the cache nor from the storages.
    fn get_column_stats(
        &self,
        table_column_pair: TableColumnPair,
    ) -> Result<Rc<Box<dyn Any>>, SbroadError>;

    /// Update `TableStats` cache with given table statistics.
    ///
    /// # Errors
    /// - Table statistics couldn't be mutually borrowed.
    fn update_table_stats(
        &mut self,
        table_name: String,
        table_stats: TableStats,
    ) -> Result<(), SbroadError>;

    /// Update `ColumnStats` cache with given initial column statistics.
    ///
    /// # Errors
    /// - Initial column statistics couldn't be mutually borrowed.
    fn update_column_stats<T: Scalar>(
        &self,
        table_column_pair: TableColumnPair,
        column_stats: ColumnStats<T>,
    ) -> Result<(), SbroadError>;
}

pub trait Vshard {
    /// Execute a query on all the shards in the cluster.
    ///
    /// # Errors
    /// - Execution errors
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
        vtable_max_rows: u64,
    ) -> Result<Box<dyn Any>, SbroadError>;

    /// Execute a query on a some of the shards in the cluster.
    ///
    /// # Errors
    /// - Execution errors
    fn exec_ir_on_some(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError>;

    /// Execute query locally on the current node.
    ///
    /// # Errors
    /// - Execution errors
    fn exec_ir_on_any_node(&self, sub_plan: ExecutionPlan) -> Result<Box<dyn Any>, SbroadError>;

    /// Get the amount of buckets in the cluster.
    fn bucket_count(&self) -> u64;

    /// Get a random bucket from the cluster.
    fn get_random_bucket(&self) -> Buckets;

    /// Determine shard for query execution by sharding key value
    ///
    /// # Errors
    /// - Internal error. Under normal conditions we should always return
    ///   bucket id successfully.
    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError>;
}
