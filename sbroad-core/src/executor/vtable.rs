use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::vec;

use serde::{Deserialize, Serialize};

use crate::errors::QueryPlannerError;
use crate::ir::relation::Column;
use crate::ir::transformation::redistribution::{MotionKey, Target};
use crate::ir::value::Value;

type ShardingKey = Vec<Value>;
pub type VTableTuple = Vec<Value>;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct VTableIndex {
    value: HashMap<u64, Vec<usize>>,
}

impl VTableIndex {
    fn new() -> Self {
        Self {
            value: HashMap::new(),
        }
    }
}

impl From<HashMap<u64, Vec<usize>>> for VTableIndex {
    fn from(value: HashMap<u64, Vec<usize>>) -> Self {
        Self { value }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct ShardingRecord(ShardingKey, usize);

/// Result tuple storage, created by the executor. All tuples
/// have a distribution key.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct VirtualTable {
    /// List of the columns.
    columns: Vec<Column>,
    /// "Raw" tuples (list of values)
    tuples: Vec<VTableTuple>,
    /// Unique table name (we need to generate it ourselves).
    name: Option<String>,
    /// Unique distribution keys of virtual table tuples
    distribution_key: Option<MotionKey>,
    /// Index groups tuples by the buckets:
    /// the key is a bucket id, the value is a list of positions
    /// in the `tuples` list corresponding to the bucket.
    index: VTableIndex,
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
            index: VTableIndex::new(),
        }
    }

    /// Add column to virtual table
    pub fn add_column(&mut self, col: Column) {
        self.columns.push(col);
    }

    /// Adds a tuple of values to virtual table
    ///
    /// # Errors
    /// Returns IR `Value` transformation error
    pub fn add_tuple(&mut self, tuple: VTableTuple) {
        self.tuples.push(tuple);
    }

    /// Gets a virtual table tuples list
    #[must_use]
    pub fn get_tuples(&self) -> &[VTableTuple] {
        &self.tuples
    }

    /// Gets a mutable virtual table tuples list
    #[must_use]
    pub fn get_mut_tuples(&mut self) -> &mut [VTableTuple] {
        &mut self.tuples
    }

    /// Gets virtual table columns list
    #[must_use]
    pub fn get_columns(&self) -> &[Column] {
        &self.columns
    }

    /// Gets virtual table columns list
    #[must_use]
    pub fn get_mut_columns(&mut self) -> &mut Vec<Column> {
        &mut self.columns
    }

    /// Gets virtual table motion key
    #[must_use]
    pub fn get_moton_key(&self) -> &Option<MotionKey> {
        &self.distribution_key
    }

    /// Sets vtablvirtual tablee motion key
    pub fn set_motion_key(&mut self, sharding_key: &MotionKey) {
        self.distribution_key = Some(sharding_key.clone());
    }

    /// Gets virtual table index
    #[must_use]
    pub fn get_index(&self) -> &HashMap<u64, Vec<usize>> {
        &self.index.value
    }

    /// Set vtable index
    pub fn set_index(&mut self, index: HashMap<u64, Vec<usize>>) {
        self.index = index.into();
    }

    /// Get virtual table tuples' values, participating in the distribution key.
    ///
    /// # Errors
    /// - Failed to find a distribution key.
    pub fn get_tuple_distribution(&self) -> Result<HashSet<Vec<&Value>>, QueryPlannerError> {
        let mut result: HashSet<Vec<&Value>> = HashSet::with_capacity(self.tuples.len());

        for tuple in &self.tuples {
            let mut shard_key_tuple: Vec<&Value> = Vec::new();
            if let Some(k) = &self.distribution_key {
                for target in &k.targets {
                    match target {
                        Target::Reference(pos) => {
                            let part = tuple.get(*pos).ok_or_else(|| {
                                QueryPlannerError::CustomError(format!(
                                    "Failed to find a distribution key column {} in the tuple {:?}.",
                                    pos, tuple
                                ))
                            })?;
                            shard_key_tuple.push(part);
                        }
                        Target::Value(ref val) => {
                            shard_key_tuple.push(val);
                        }
                    }
                }
            } else {
                return Err(QueryPlannerError::CustomError(
                    "Distribution key not found".into(),
                ));
            }
            result.insert(shard_key_tuple);
        }

        Ok(result)
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
    pub fn get_alias(&self) -> Option<&String> {
        self.name.as_ref()
    }

    /// Create a new virtual table from an original one with
    /// a list of tuples corresponding to some exact buckets.
    #[must_use]
    pub fn new_with_buckets(&self, buckets: &[u64]) -> Self {
        let mut result = Self::new();
        result.columns = self.columns.clone();
        result.distribution_key = self.distribution_key.clone();
        result.name = self.name.clone();

        for bucket in buckets {
            if let Some(positions) = self.index.value.get(bucket) {
                for pos in positions {
                    result.tuples.push(self.tuples[*pos].clone());
                }
                result.index.value.insert(*bucket, positions.clone());
            }
        }

        result
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct VirtualTableMap(HashMap<usize, Rc<VirtualTable>>);

impl VirtualTableMap {
    #[must_use]
    pub fn new(map: HashMap<usize, Rc<VirtualTable>>) -> Self {
        Self(map)
    }

    #[must_use]
    pub fn map(&self) -> &HashMap<usize, Rc<VirtualTable>> {
        &self.0
    }

    pub fn mut_map(&mut self) -> &mut HashMap<usize, Rc<VirtualTable>> {
        &mut self.0
    }
}

#[cfg(test)]
mod tests;
