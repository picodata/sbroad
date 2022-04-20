use std::vec;

use serde::{Deserialize, Serialize};

use crate::errors::QueryPlannerError;
use crate::ir::distribution::Key;
use crate::ir::relation::Column;
use crate::ir::value::{AsIrVal, Value};

type VTableTuple = Vec<Value>;
type ShardingKey = Vec<Value>;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct ShardingRecord(ShardingKey, usize);

/// Result tuple storage, created by the executor. All tuples
/// have a distribution key.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct VirtualTable {
    /// List of the columns.
    columns: Vec<Column>,
    /// "Raw" tuples (list of values)
    tuples: Vec<VTableTuple>,
    /// Unique table name (we need to generate it ourselves).
    name: Option<String>,
    /// Unique distribution keys of virtual table tuples
    distribution_key: Option<Key>,
}

impl Default for VirtualTable {
    fn default() -> Self {
        Self::new()
    }
}

impl VirtualTable {
    #[must_use]
    pub fn new() -> Self {
        VirtualTable {
            columns: vec![],
            tuples: vec![],
            name: None,
            distribution_key: None,
        }
    }

    /// Add column to virtual table
    pub fn add_column(&mut self, col: Column) {
        self.columns.push(col);
    }

    /// Adds custom values tuple to virtual table
    ///
    /// # Errors
    /// Returns IR `Value` transformation error
    pub fn add_tuple<T>(&mut self, tuple: Vec<T>) -> Result<(), QueryPlannerError>
    where
        T: AsIrVal,
    {
        let mut t = vec![];

        for el in tuple {
            t.push(el.as_ir_value()?);
        }

        self.add_values_tuple(t);
        Ok(())
    }

    /// Add value rows to table
    pub fn add_values_tuple(&mut self, tuple: VTableTuple) {
        self.tuples.push(tuple);
    }

    /// Get vtable tuples list
    #[must_use]
    pub fn get_tuples(&self) -> Vec<VTableTuple> {
        self.tuples.clone()
    }

    /// Get vtable columns list
    #[must_use]
    pub fn get_columns(&self) -> Vec<Column> {
        self.columns.clone()
    }

    /// Get tuples was distributed by sharding keys
    ///
    /// # Errors
    /// - Distribution key not found
    pub fn get_tuple_distribution(&self) -> Result<Vec<Vec<&Value>>, QueryPlannerError> {
        let mut result = vec![];
        for tuple in &self.tuples {
            let mut shard_key_tuple = vec![];
            if let Some(k) = &self.distribution_key {
                for part in &k.positions {
                    if let Some(p) = tuple.get(*part) {
                        shard_key_tuple.push(p);
                    }
                }

                result.push(shard_key_tuple);
            } else {
                return Err(QueryPlannerError::CustomError(
                    "Distribution key not found".into(),
                ));
            }
        }

        Ok(result)
    }

    /// Distribute tuples by sharding key columns
    pub fn set_distribution(&mut self, sharding_key: &Key) {
        self.distribution_key = Some(sharding_key.clone());
    }

    /// Set vtable alias name
    ///
    /// # Errors
    /// - Try to set an empty alias name to the virtual table.
    pub fn set_alias(&mut self, name: &str) -> Result<(), QueryPlannerError> {
        if name.is_empty() {
            return Err(QueryPlannerError::CustomError(
                "Can't set empty alias for virtual table".into(),
            ));
        }

        self.name = Some(String::from(name));
        Ok(())
    }

    /// Get vtable alias name
    #[must_use]
    pub fn get_alias(&self) -> Option<String> {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests;
