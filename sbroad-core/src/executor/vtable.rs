use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use std::vec;

use serde::{Deserialize, Serialize};

use crate::errors::{Entity, SbroadError};
use crate::executor::{bucket::Buckets, Vshard};
use crate::ir::relation::Column;
use crate::ir::transformation::redistribution::{MotionKey, Target};
use crate::ir::value::Value;

type ShardingKey = Vec<Value>;
pub type VTableTuple = Vec<Value>;

/// Helper struct to group tuples by buckets.
/// key:   bucket id.
/// value: list of positions in the `tuples` list (see `VirtualTable`) corresponding to the bucket.
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
    /// Motion key used to calculate the bucket index and resharding.
    motion_key: Option<MotionKey>,
    /// Index groups tuples by the buckets:
    /// the key is a bucket id, the value is a list of positions
    /// in the `tuples` list corresponding to the bucket.
    bucket_index: VTableIndex,
}

impl Default for VirtualTable {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for VirtualTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for col in &self.columns {
            write!(f, "{col:?}, ")?;
        }
        writeln!(f)?;
        for row in &self.tuples {
            writeln!(f, "{row:?}")?;
        }
        writeln!(f)
    }
}

impl VirtualTable {
    #[must_use]
    pub fn new() -> Self {
        VirtualTable {
            columns: vec![],
            tuples: vec![],
            name: None,
            motion_key: None,
            bucket_index: VTableIndex::new(),
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
    pub fn get_mut_tuples(&mut self) -> &mut Vec<VTableTuple> {
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

    /// Sets virtual table motion key
    pub fn set_motion_key(&mut self, motion_key: &MotionKey) {
        self.motion_key = Some(motion_key.clone());
    }

    /// Gets virtual table's buket index
    #[must_use]
    pub fn get_bucket_index(&self) -> &HashMap<u64, Vec<usize>> {
        &self.bucket_index.value
    }

    /// Gets virtual table mutable bucket index
    #[must_use]
    pub fn get_mut_bucket_index(&mut self) -> &mut HashMap<u64, Vec<usize>> {
        &mut self.bucket_index.value
    }

    /// Set vtable index
    pub fn set_bucket_index(&mut self, index: HashMap<u64, Vec<usize>>) {
        self.bucket_index = index.into();
    }

    /// Get vtable's tuples corresponding to the buckets.
    #[must_use]
    pub fn get_tuples_with_buckets(&self, buckets: &Buckets) -> Vec<&VTableTuple> {
        let tuples: Vec<&VTableTuple> = match buckets {
            Buckets::All | Buckets::Single => self.get_tuples().iter().collect(),
            Buckets::Filtered(bucket_ids) => {
                if self.get_bucket_index().is_empty() {
                    // TODO: Implement selection push-down (join_linker3_test).
                    self.get_tuples().iter().collect()
                } else {
                    bucket_ids
                        .iter()
                        .filter_map(|bucket_id| self.get_bucket_index().get(bucket_id))
                        .flatten()
                        .filter_map(|pos| self.get_tuples().get(*pos))
                        .collect()
                }
            }
        };
        tuples
    }

    /// Get virtual table tuples' values, participating in the distribution key.
    ///
    /// # Errors
    /// - Failed to find a distribution key.
    pub fn get_tuple_distribution(&self) -> Result<HashSet<Vec<&Value>>, SbroadError> {
        let mut result: HashSet<Vec<&Value>> = HashSet::with_capacity(self.tuples.len());

        for tuple in &self.tuples {
            let mut shard_key_tuple: Vec<&Value> = Vec::new();
            if let Some(k) = &self.motion_key {
                for target in &k.targets {
                    match target {
                        Target::Reference(pos) => {
                            let part = tuple.get(*pos).ok_or_else(|| {
                                SbroadError::NotFound(
                                    Entity::DistributionKey,
                                    format!("column {pos} in the tuple {tuple:?}."),
                                )
                            })?;
                            shard_key_tuple.push(part);
                        }
                        Target::Value(ref val) => {
                            shard_key_tuple.push(val);
                        }
                    }
                }
            } else {
                return Err(SbroadError::NotFound(
                    Entity::DistributionKey,
                    "for virtual table".into(),
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
    pub fn set_alias(&mut self, name: &str) -> Result<(), SbroadError> {
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
    ///
    /// The thing is that given `self` vtable may contain (bucket id -> tuples) pair in its index
    /// for some bucket that is not present in given `bucket_ids`.
    /// We won't add tuples corresponding to such pair into new vtable.
    #[must_use]
    pub fn new_with_buckets(&self, bucket_ids: &[u64]) -> Self {
        let mut result = Self::new();
        result.columns = self.columns.clone();
        result.motion_key = self.motion_key.clone();
        result.name = self.name.clone();

        for bucket_id in bucket_ids {
            // If bucket_id is met among those that are present in self.
            if let Some(positions) = self.get_bucket_index().get(bucket_id) {
                let mut new_positions: Vec<usize> = Vec::with_capacity(positions.len());
                for pos in positions {
                    result.tuples.push(self.tuples[*pos].clone());
                    new_positions.push(result.tuples.len() - 1);
                }
                result
                    .get_mut_bucket_index()
                    .insert(*bucket_id, new_positions);
            }
        }

        result
    }

    /// Reshard a virtual table (i.e. build a bucket index).
    ///
    /// # Errors
    /// - Motion key is invalid.
    pub fn reshard(
        &mut self,
        motion_key: &MotionKey,
        runtime: &impl Vshard,
    ) -> Result<(), SbroadError> {
        self.set_motion_key(motion_key);

        let mut index: HashMap<u64, Vec<usize>> = HashMap::new();
        for (pos, tuple) in self.get_tuples().iter().enumerate() {
            let mut shard_key_tuple: Vec<&Value> = Vec::new();
            for target in &motion_key.targets {
                match target {
                    Target::Reference(col_idx) => {
                        let part = tuple.get(*col_idx).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::DistributionKey,
                                format!(
                                "failed to find a distribution key column {pos} in the tuple {tuple:?}."
                            ),
                            )
                        })?;
                        shard_key_tuple.push(part);
                    }
                    Target::Value(ref value) => {
                        shard_key_tuple.push(value);
                    }
                }
            }
            let bucket_id = runtime.determine_bucket_id(&shard_key_tuple)?;
            match index.entry(bucket_id) {
                Entry::Vacant(entry) => {
                    entry.insert(vec![pos]);
                }
                Entry::Occupied(entry) => {
                    entry.into_mut().push(pos);
                }
            }
        }

        self.set_bucket_index(index);
        Ok(())
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
