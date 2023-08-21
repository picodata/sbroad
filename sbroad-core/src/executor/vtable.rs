use ahash::AHashMap;
use std::collections::{hash_map::Entry, HashMap};
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use std::vec;

use serde::{Deserialize, Serialize};

use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::{TupleBuilderCommands, TupleBuilderPattern};
use crate::executor::{bucket::Buckets, Vshard};
use crate::ir::helpers::RepeatableState;
use crate::ir::relation::Column;
use crate::ir::transformation::redistribution::{ColumnPosition, MotionKey, Target};
use crate::ir::value::Value;

type ShardingKey = Vec<Value>;
pub type VTableTuple = Vec<Value>;

/// Helper struct to group tuples by buckets.
/// key:   bucket id.
/// value: list of positions in the `tuples` list (see `VirtualTable`) corresponding to the bucket.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct VTableIndex {
    value: HashMap<u64, Vec<usize>, RepeatableState>,
}

impl VTableIndex {
    fn new() -> Self {
        Self {
            value: HashMap::with_hasher(RepeatableState),
        }
    }
}

impl From<HashMap<u64, Vec<usize>, RepeatableState>> for VTableIndex {
    fn from(value: HashMap<u64, Vec<usize>, RepeatableState>) -> Self {
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
    /// Column positions that form a primary key.
    primary_key: Option<Vec<ColumnPosition>>,
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
            primary_key: None,
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

    /// Gets virtual table's buket index
    #[must_use]
    pub fn get_bucket_index(&self) -> &HashMap<u64, Vec<usize>, RepeatableState> {
        &self.bucket_index.value
    }

    /// Gets virtual table mutable bucket index
    #[must_use]
    pub fn get_mut_bucket_index(&mut self) -> &mut HashMap<u64, Vec<usize>, RepeatableState> {
        &mut self.bucket_index.value
    }

    /// Set vtable index
    pub fn set_bucket_index(&mut self, index: HashMap<u64, Vec<usize>, RepeatableState>) {
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

    /// Create a new virtual table from the original one with
    /// a list of tuples corresponding only to the passed buckets.
    ///
    /// # Errors
    /// - bucket index is corrupted
    pub fn new_with_buckets(&self, bucket_ids: &[u64]) -> Result<Self, SbroadError> {
        let mut result = Self::new();
        result.columns = self.columns.clone();
        result.name = self.name.clone();

        result.primary_key = self.primary_key.clone();
        for bucket_id in bucket_ids {
            // If bucket_id is met among those that are present in self.
            if let Some(pointers) = self.get_bucket_index().get(bucket_id) {
                let mut new_pointers: Vec<usize> = Vec::with_capacity(pointers.len());
                for pointer in pointers {
                    let tuple = self.tuples.get(*pointer).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::VirtualTable,
                            Some(format!(
                                "Tuple with position {pointer} in the bucket index not found"
                            )),
                        )
                    })?;
                    result.tuples.push(tuple.clone());
                    new_pointers.push(result.tuples.len() - 1);
                }
                result
                    .get_mut_bucket_index()
                    .insert(*bucket_id, new_pointers);
            }
        }
        Ok(result)
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
        let mut index = HashMap::with_hasher(RepeatableState);
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

    /// Set primary key in the virtual table.
    ///
    /// # Errors
    /// - primary key refers invalid column positions
    pub fn set_primary_key(&mut self, pk: &[ColumnPosition]) -> Result<(), SbroadError> {
        for pos in pk {
            if pos >= &self.columns.len() {
                return Err(SbroadError::NotFound(
                    Entity::Column,
                    format!(
                        "primary key in the virtual table {:?} contains invalid position {pos}.",
                        self.name
                    ),
                ));
            }
        }
        self.primary_key = Some(pk.to_vec());
        Ok(())
    }

    fn pk_positions_in_projection(
        &self,
        old_to_new_pos_map: &AHashMap<ColumnPosition, ColumnPosition>,
    ) -> Result<Option<Vec<ColumnPosition>>, SbroadError> {
        if let Some(pk) = &self.primary_key {
            let mut new_pk = Vec::with_capacity(pk.len());
            for old_pk_pos in pk {
                match old_to_new_pos_map.get(old_pk_pos) {
                    Some(new_pos) => new_pk.push(*new_pos),
                    None => {
                        return Err(SbroadError::NotFound(
                            Entity::Column,
                            format!(
                                "{} {old_pk_pos} {} {old_to_new_pos_map:?}.",
                                "primary key contains a column position",
                                "that is not present in the projection column map",
                            ),
                        ));
                    }
                }
            }
            return Ok(Some(new_pk));
        }
        Ok(None)
    }

    /// Remove columns from the virtual table other then declared in projection.
    ///
    /// # Errors
    /// - projection refers invalid column positions
    /// - projection removes primary key columns
    pub fn project(&mut self, projection: &[ColumnPosition]) -> Result<(), SbroadError> {
        let mut old_to_new_pos_map = AHashMap::with_capacity(projection.len());
        let mut pattern = TupleBuilderPattern::with_capacity(self.columns.len());
        for (pos, pointer) in projection.iter().enumerate() {
            if pointer >= &self.columns.len() {
                return Err(SbroadError::NotFound(
                    Entity::Column,
                    format!(
                        "projection in the virtual table {:?} contains invalid position {pointer}.",
                        self.name
                    ),
                ));
            }
            if let Some(dup_pos) = old_to_new_pos_map.get(pointer) {
                pattern.push(TupleBuilderCommands::CloneSelfPosition(*dup_pos));
            } else {
                old_to_new_pos_map.insert(*pointer, pos);
                pattern.push(TupleBuilderCommands::TakePosition(*pointer));
            }
        }
        self.primary_key = self.pk_positions_in_projection(&old_to_new_pos_map)?;
        for old_tuple in &mut self.tuples {
            let mut new_tuple = VTableTuple::with_capacity(pattern.len());
            for command in &pattern {
                match command {
                    TupleBuilderCommands::TakePosition(pos) => {
                        let column = old_tuple.get_mut(*pos).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Column,
                                format!("at position {pos} in the tuple"),
                            )
                        })?;
                        new_tuple.push(std::mem::take(column));
                    }
                    TupleBuilderCommands::CloneSelfPosition(pos) => {
                        let column = new_tuple.get(*pos).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Column,
                                format!(
                                    "at position {pos} in the constructing tuple {new_tuple:?}"
                                ),
                            )
                        })?;
                        new_tuple.push(column.clone());
                    }
                    _ => {
                        return Err(SbroadError::Invalid(
                            Entity::Tuple,
                            Some(format!(
                                "tuple builder command {command:?} is not supported"
                            )),
                        ))
                    }
                }
            }
            *old_tuple = new_tuple;
        }
        let mut new_columns = Vec::with_capacity(pattern.len());
        for command in &pattern {
            match command {
                TupleBuilderCommands::TakePosition(pos) => {
                    let column = self.columns.get_mut(*pos).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Column,
                            format!(
                                "at position {pos} in the column metadata of the virtual table"
                            ),
                        )
                    })?;
                    new_columns.push(std::mem::take(column));
                }
                TupleBuilderCommands::CloneSelfPosition(pos) => {
                    let column = new_columns.get(*pos).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Column,
                            format!(
                                "{} {self:?}",
                                "at position {pos} in the new column metadata of the virtual table",
                            ),
                        )
                    })?;
                    new_columns.push(column.clone());
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format!(
                            "tuple builder command {command:?} is not supported"
                        )),
                    ))
                }
            }
        }
        self.columns = new_columns;

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
