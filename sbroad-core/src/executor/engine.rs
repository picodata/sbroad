//! Coordinator module.
//!
//! Traits that define an execution engine interface.

use crate::cbo::histogram::MostCommonValueWithFrequency;
use crate::cbo::{TableColumnPair, TableStats};
use std::any::Any;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use crate::errors::SbroadError;
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::executor::vtable::VirtualTable;
use crate::ir::function::Function;
use crate::ir::relation::Table;
use crate::ir::value::Value;

use super::ir::{ConnectionType, QueryType};
use super::protocol::Binary;

pub mod helpers;
#[cfg(test)]
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
    /// - Metadata contains incorrect sharding keys format
    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, SbroadError>;

    /// Provides vector of the sharding key column positions in a tuple or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError>;
}

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

    /// Extract a list of the sharding keys from a map for the given space.
    ///
    /// # Errors
    /// - Columns are not present in the sharding key of the space.
    fn extract_sharding_keys_from_map<'rec>(
        &self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError>;

    /// Extract a list of the sharding key values from a tuple for the given space.
    ///
    /// # Errors
    /// - Internal error in the table (should never happen, but we recheck).
    fn extract_sharding_keys_from_tuple<'rec>(
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

/// Enum that represents initial bucket gathered from storages.
/// It copies `Bucket` enum, where all field are represented by value and not by reference.
#[allow(dead_code)]
#[derive(Debug, Clone)]
enum InitialBucket {
    /// Representation of the first histogram bucket with inclusive `from` edge.
    First {
        from: Value,
        to: Value,
        frequency: usize,
    },
    /// Representation of a non-first histogram bucket with non-inclusive `from` edge.
    NonFirst {
        from: Value,
        to: Value,
        frequency: usize,
    },
}

/// Struct that represents initial histogram gathered from storages.
/// It copies `Histogram` structure, where all field are represented by value and not by reference.
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct InitialHistogram {
    most_common: Vec<MostCommonValueWithFrequency>,
    buckets: Vec<InitialBucket>,
    null_fraction: f64,
    distinct_values_fraction: f64,
}

/// Struct that represents initial statistics gathered from storages.
/// It copies `ColumnStats` structure, where all fields are represented by value and not by reference.
///
/// **Note**: `rows_number` field is missing, because during `ColumnStats`
/// structure initialization this information will be passed from `TableStats`.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct InitialColumnStats {
    min_value: Value,
    max_value: Value,
    avg_size: u64,
    histogram: InitialHistogram,
}

/// A `CostBased` statistics trait.
pub trait Statistics {
    /// Get `TableStats` for table by its name from storages.
    ///
    /// # Errors
    /// - Table statistics can not be gathered neither from the cache nor from the storages.
    fn get_table_stats(&self, table_name: String) -> Result<Rc<TableStats>, SbroadError>;

    /// Get `InitialColumnStats` for column by its table name and column name from storages.
    ///
    /// # Errors
    /// - Initial column statistics can not be gathered neither from the cache nor from the storages.
    fn get_initial_column_stats(
        &self,
        table_column_pair: TableColumnPair,
    ) -> Result<Rc<InitialColumnStats>, SbroadError>;

    /// Update `TableStats` cache with given table statistics.
    ///
    /// # Errors
    /// - Table statistics couldn't be mutually borrowed.
    fn update_table_stats_cache(
        &mut self,
        table_name: String,
        table_stats: TableStats,
    ) -> Result<(), SbroadError>;

    /// Update `InitialColumnStats` cache with given initial column statistics.
    ///
    /// # Errors
    /// - Initial column statistics couldn't be mutually borrowed.
    fn update_column_initial_stats_cache(
        &self,
        table_column_pair: TableColumnPair,
        initial_column_stats: InitialColumnStats,
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

    /// Get the amount of buckets in the cluster.
    fn bucket_count(&self) -> u64;

    /// Get a random bucket from the cluster.
    fn get_random_bucket(&self) -> Buckets;

    /// Determine shard for query execution by sharding key value
    ///
    /// # Errors
    /// - Internal error. Under normal conditions we should always return
    ///   bucket id successfully.
    fn determine_bucket_id(&self, s: &[&Value]) -> u64;
}
