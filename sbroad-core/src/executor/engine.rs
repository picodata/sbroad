//! Coordinator module.
//!
//! Traits that define an execution engine interface.

use crate::cbo::histogram::MostCommonValueWithFrequency;
use crate::cbo::{TableColumnPair, TableStats};
use std::any::Any;
use std::cell::{Ref, RefCell};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use crate::errors::{Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::executor::vtable::VirtualTable;
use crate::ir::function::Function;
use crate::ir::relation::Table;
use crate::ir::value::Value;

/// A metadata storage trait of the cluster.
pub trait CoordinatorMetadata {
    /// Get a table by name that contains:
    /// * list of the columns,
    /// * distribution key of the output tuples (column positions),
    /// * table name.
    ///
    /// # Errors
    /// - Failed to get table by name from the metadata.
    fn get_table_segment(&self, table_name: &str) -> Result<Table, SbroadError>;

    /// Lookup for a function in the metadata cache.
    ///
    /// # Errors
    /// - Failed to get function by name from the metadata.
    fn get_function(&self, fn_name: &str) -> Result<&Function, SbroadError>;

    fn get_exec_waiting_timeout(&self) -> u64;

    fn get_sharding_column(&self) -> &str;

    /// Provides vector of the sharding key column names or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    /// - Metadata contains incorrect sharding keys format
    fn get_sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, SbroadError>;

    /// Provides vector of the sharding key column positions in a tuple or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    fn get_sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError>;

    /// Provides amlount of table columns
    ///
    /// # Errors
    /// - Metadata does not contain space
    fn get_fields_amount_by_space(&self, space: &str) -> Result<usize, SbroadError>;
}

/// Cluster configuration.
pub trait Configuration: Sized {
    type Configuration;

    /// Return a cached cluster configuration from the Rust memory.
    ///
    /// # Errors
    /// - Failed to get a configuration from the coordinator runtime.
    fn cached_config(&self) -> Result<Ref<Self::Configuration>, SbroadError>;

    /// Clear the cached cluster configuration in the Rust memory.
    ///
    /// # Errors
    /// - Failed to clear the cached configuration.
    fn clear_config(&self) -> Result<(), SbroadError>;

    /// Check if the cached cluster configuration is empty.
    ///
    /// # Errors
    /// - Failed to get the cached configuration.
    fn is_config_empty(&self) -> Result<bool, SbroadError>;

    /// Retrieve cluster configuration from the Lua memory.
    ///
    /// If the configuration is already cached, return None,
    /// otherwise return Some(config).
    ///
    /// # Errors
    /// - Internal error.
    fn get_config(&self) -> Result<Option<Self::Configuration>, SbroadError>;

    /// Update cached cluster configuration.
    ///
    /// # Errors
    /// - Failed to update the configuration.
    fn update_config(&self, metadata: Self::Configuration) -> Result<(), SbroadError>;
}

/// A coordinator trait.
pub trait Coordinator: Configuration {
    type Cache;
    type ParseTree;

    /// Flush the coordinator's IR cache.
    ///
    /// # Errors
    /// - Invalid capacity (zero).
    fn clear_ir_cache(&self) -> Result<(), SbroadError>;

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
    ) -> Result<VirtualTable, SbroadError>;

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

    /// Setup output format of query explain
    ///
    /// # Errors
    /// - internal executor errors
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

    /// Determine shard for query execution by sharding key value
    fn determine_bucket_id(&self, s: &[&Value]) -> u64;
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

/// A common function for all engines to calculate the sharding key value from a tuple.
///
/// # Errors
/// - The space was not found in the metadata.
/// - The sharding keys are not present in the space.
pub fn sharding_keys_from_tuple<'rec>(
    conf: &impl CoordinatorMetadata,
    space: &str,
    tuple: &'rec [Value],
) -> Result<Vec<&'rec Value>, SbroadError> {
    let quoted_space = normalize_name_from_schema(space);
    let sharding_positions = conf.get_sharding_positions_by_space(&quoted_space)?;
    let mut sharding_tuple = Vec::with_capacity(sharding_positions.len());
    let table_col_amount = conf.get_fields_amount_by_space(&quoted_space)?;
    if table_col_amount == tuple.len() {
        // The tuple contains a "bucket_id" column.
        for position in &sharding_positions {
            let value = tuple.get(*position).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format!("position {position:?} in the tuple {tuple:?}"),
                )
            })?;
            sharding_tuple.push(value);
        }
        Ok(sharding_tuple)
    } else if table_col_amount == tuple.len() + 1 {
        // The tuple doesn't contain the "bucket_id" column.
        let table = conf.get_table_segment(&quoted_space)?;
        let bucket_position = table.get_bucket_id_position()?;

        // If the "bucket_id" splits the sharding key, we need to shift the sharding
        // key positions of the right part by one.
        // For example, we have a table with columns a, bucket_id, b, and the sharding
        // key is (a, b). Then the sharding key positions are (0, 2).
        // If someone gives us a tuple (42, 666) we should tread is as (42, null, 666).
        for position in &sharding_positions {
            let corrected_pos = match position.cmp(&bucket_position) {
                Ordering::Less => *position,
                Ordering::Equal => {
                    return Err(SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format!(
                            r#"the tuple {tuple:?} contains a "bucket_id" position {position} in a sharding key {sharding_positions:?}"#
                        )),
                    ))
                }
                Ordering::Greater => *position - 1,
            };
            let value = tuple.get(corrected_pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format!("position {corrected_pos:?} in the tuple {tuple:?}"),
                )
            })?;
            sharding_tuple.push(value);
        }
        Ok(sharding_tuple)
    } else {
        Err(SbroadError::Invalid(
            Entity::Tuple,
            Some(format!(
                "the tuple {:?} was expected to have {} filed(s), got {}.",
                tuple,
                table_col_amount - 1,
                tuple.len()
            )),
        ))
    }
}

/// A common function for all engines to calculate the sharding key value from a map.
///
/// # Errors
/// - The space was not found in the metadata.
/// - The sharding keys are not present in the space.
pub fn sharding_keys_from_map<'rec, S: ::std::hash::BuildHasher>(
    conf: &impl CoordinatorMetadata,
    space: &str,
    map: &'rec HashMap<String, Value, S>,
) -> Result<Vec<&'rec Value>, SbroadError> {
    let quoted_space = normalize_name_from_schema(space);
    let sharding_key = conf.get_sharding_key_by_space(&quoted_space)?;
    let quoted_map = map
        .iter()
        .map(|(k, _)| (normalize_name_from_schema(k), k.as_str()))
        .collect::<HashMap<String, &str>>();
    let mut tuple = Vec::with_capacity(sharding_key.len());
    for quoted_column in &sharding_key {
        if let Some(column) = quoted_map.get(quoted_column) {
            let value = map.get(*column).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format!("column {column:?} in the map {map:?}"),
                )
            })?;
            tuple.push(value);
        } else {
            return Err(SbroadError::NotFound(
                Entity::ShardingKey,
                format!(
                "(quoted) column {quoted_column:?} in the quoted map {quoted_map:?} (original map: {map:?})"
            )));
        }
    }
    Ok(tuple)
}

#[must_use]
pub fn normalize_name_from_schema(s: &str) -> String {
    format!("\"{s}\"")
}

#[must_use]
pub fn normalize_name_from_sql(s: &str) -> String {
    if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
        return s.to_string();
    }
    format!("\"{}\"", s.to_uppercase())
}

#[cfg(test)]
pub mod mock;
