use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::errors::QueryPlannerError;
use crate::ir::distribution::Key;
use crate::ir::relation::Column;
use crate::ir::value::{AsIrVal, Value};

type VTableTuple = Vec<Value>;

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
    /// Tuples grouped by the corresponding shards.
    /// Value `HashSet` contains indexes from the `tuples` attribute
    hashing: HashMap<String, HashSet<usize>>,
}

impl VirtualTable {
    #[must_use]
    pub fn new() -> Self {
        VirtualTable {
            columns: vec![],
            tuples: vec![],
            name: None,
            hashing: HashMap::new(),
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
    #[must_use]
    pub fn get_tuple_distribution(&self) -> HashMap<String, HashSet<usize>> {
        self.hashing.clone()
    }

    /// Distribute tuples by sharding key columns
    pub fn hashing_tuple_by_shard(&mut self, sharding_key: &Key) {
        for (tid, tuple) in self.tuples.iter().enumerate() {
            let mut key = String::new();
            for part in &sharding_key.positions {
                if let Some(p) = tuple.get(*part) {
                    key.push_str(p.to_string().as_str());
                }
            }

            match self.hashing.get_mut(&key) {
                None => {
                    self.hashing.insert(key, HashSet::from([tid]));
                }
                Some(item) => {
                    item.insert(tid);
                }
            };
        }
    }

    /// Set vtable alias name
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
    pub fn get_alias(&self) -> Option<String> {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests;
