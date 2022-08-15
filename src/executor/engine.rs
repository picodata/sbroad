//! Coordinator module.
//!
//! Traits that define an execution engine interface.

use std::any::Any;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;

use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::executor::vtable::VirtualTable;
use crate::ir::value::Value;

pub mod cartridge;

/// A metadata storage trait of the cluster.
pub trait CoordinatorMetadata {
    /// Get a table by name that contains:
    /// * list of the columns,
    /// * distribution key of the output tuples (column positions),
    /// * table name.
    ///
    /// # Errors
    /// - Failed to get table by name from the metadata.
    fn get_table_segment(
        &self,
        table_name: &str,
    ) -> Result<crate::ir::relation::Table, QueryPlannerError>;

    fn get_exec_waiting_timeout(&self) -> u64;

    fn get_sharding_column(&self) -> &str;

    /// Provides vector of the sharding key column names or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    /// - Metadata contains incorrect sharding keys format
    fn get_sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, QueryPlannerError>;

    /// Provides vector of the sharding key column positions in a tuple or an error
    ///
    /// # Errors
    /// - Metadata does not contain space
    fn get_sharding_positions_by_space(&self, space: &str)
        -> Result<Vec<usize>, QueryPlannerError>;

    /// Provides amlount of table columns
    ///
    /// # Errors
    /// - Metadata does not contain space
    fn get_fields_amount_by_space(&self, space: &str) -> Result<usize, QueryPlannerError>;
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
    /// If the configuration is already cached, return None,
    /// otherwise return Some(config).
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
    fn clear_ir_cache(&self) -> Result<(), QueryPlannerError>;

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

    /// Extract a list of the sharding keys from a map for the given space.
    ///
    /// # Errors
    /// - Columns are not present in the sharding key of the space.
    fn extract_sharding_keys_from_map<'engine, 'rec>(
        &'engine self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, QueryPlannerError>;

    /// Extract a list of the sharding key values from a tuple for the given space.
    ///
    /// # Errors
    /// - Internal error in the table (should never happen, but we recheck).
    fn extract_sharding_keys_from_tuple<'engine, 'rec>(
        &'engine self,
        space: String,
        args: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, QueryPlannerError>;

    /// Determine shard for query execution by sharding key value
    fn determine_bucket_id(&self, s: &[&Value]) -> u64;
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
) -> Result<Vec<&'rec Value>, QueryPlannerError> {
    let quoted_space = normalize_name_from_schema(space);
    let sharding_positions = conf.get_sharding_positions_by_space(&quoted_space)?;
    let mut sharding_tuple = Vec::with_capacity(sharding_positions.len());
    let table_col_amount = conf.get_fields_amount_by_space(&quoted_space)?;
    if table_col_amount == tuple.len() {
        // The tuple contains a "bucket_id" column.
        for position in &sharding_positions {
            let value = tuple.get(*position).ok_or_else(|| {
                QueryPlannerError::CustomError(format!(
                    "Missing sharding key position {:?} in the tuple {:?}",
                    position, tuple
                ))
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
                    return Err(QueryPlannerError::CustomError(format!(
                        r#"The tuple {:?} contains a "bucket_id" position {} in a sharding key {:?}"#,
                        tuple, position, sharding_positions
                    )))
                }
                Ordering::Greater => *position - 1,
            };
            let value = tuple.get(corrected_pos).ok_or_else(|| {
                QueryPlannerError::CustomError(format!(
                    "Missing sharding key position {:?} in the tuple {:?}",
                    corrected_pos, tuple
                ))
            })?;
            sharding_tuple.push(value);
        }
        Ok(sharding_tuple)
    } else {
        Err(QueryPlannerError::CustomError(format!(
            "The tuple {:?} was expected to have {} filed(s), got {}.",
            tuple,
            table_col_amount - 1,
            tuple.len()
        )))
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
) -> Result<Vec<&'rec Value>, QueryPlannerError> {
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
                QueryPlannerError::CustomError(format!(
                    "Missing sharding key column {:?} in the map {:?}",
                    column, map
                ))
            })?;
            tuple.push(value);
        } else {
            return Err(QueryPlannerError::CustomError(format!(
                "Missing quoted sharding key column {:?} in the quoted map {:?}. Original map: {:?}",
                quoted_column, quoted_map, map
            )));
        }
    }
    Ok(tuple)
}

#[must_use]
pub fn normalize_name_from_schema(s: &str) -> String {
    format!("\"{}\"", s)
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
