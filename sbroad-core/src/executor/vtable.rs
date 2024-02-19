use std::collections::{hash_map::Entry, HashMap};
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use std::vec;

use ahash::AHashSet;
use serde::{Deserialize, Serialize};

use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::{TupleBuilderCommand, TupleBuilderPattern};
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

    pub fn add_entry(&mut self, bucket_id: u64, position: usize) {
        match self.value.entry(bucket_id) {
            Entry::Vacant(entry) => {
                entry.insert(vec![position]);
            }
            Entry::Occupied(entry) => {
                entry.into_mut().push(position);
            }
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
            Buckets::All | Buckets::Any => self.get_tuples().iter().collect(),
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

    /// Get primary key in the virtual table.
    ///
    /// # Errors
    /// - primary key refers invalid column positions
    pub fn get_primary_key(&self) -> Result<&[ColumnPosition], SbroadError> {
        if let Some(cols) = &self.primary_key {
            return Ok(cols);
        }
        Err(SbroadError::Invalid(
            Entity::VirtualTable,
            Some("expected to have primary key!".into()),
        ))
    }

    /// Helper logic of `rearrange_for_update` related to creation of delete tuples.
    /// For more details see `rearrange_for_update`.
    fn create_delete_tuples(
        &mut self,
        runtime: &(impl Vshard + Sized),
        old_shard_columns_len: usize,
    ) -> Result<(Vec<VTableTuple>, VTableIndex), SbroadError> {
        let mut index = VTableIndex::new();
        let delete_tuple_pattern: TupleBuilderPattern = {
            let pk_positions = self.get_primary_key()?;
            let mut res = Vec::with_capacity(old_shard_columns_len + pk_positions.len());
            for pos in pk_positions {
                res.push(TupleBuilderCommand::TakePosition(*pos));
            }
            res
        };
        let mut delete_tuple: VTableTuple = vec![Value::Null; delete_tuple_pattern.len()];
        let mut delete_tuples: Vec<VTableTuple> = Vec::with_capacity(self.get_tuples().len());
        let tuples_len = self.get_tuples().len();
        for (pos, insert_tuple) in self.get_mut_tuples().iter_mut().enumerate() {
            for (idx, c) in delete_tuple_pattern.iter().enumerate() {
                if let TupleBuilderCommand::TakePosition(pos) = c {
                    let value = insert_tuple.get(*pos).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::TupleBuilderCommand,
                            Some(format!(
                                "expected position {pos} with tuple len: {}",
                                insert_tuple.len()
                            )),
                        )
                    })?;
                    if let Some(elem) = delete_tuple.get_mut(idx) {
                        *elem = value.clone();
                    }
                };
            }
            let mut old_shard_key: VTableTuple = Vec::with_capacity(old_shard_columns_len);
            for _ in 0..old_shard_columns_len {
                // Note that we are getting values from the end of `insert_tuple` using `pop` method
                // as soon as `old_shard_key_columns` are located in the end of `insert_tuple`
                let Some(v) = insert_tuple.pop() else {
                    return Err(SbroadError::Invalid(
                        Entity::MotionOpcode,
                        Some(format!(
                            "invalid number of old shard columns: {old_shard_columns_len}"
                        )),
                    ));
                };
                old_shard_key.push(v);
            }

            // When popping we got these keys in reverse order. That's why we need to use `reverse`.
            old_shard_key.reverse();
            let bucket_id =
                runtime.determine_bucket_id(&old_shard_key.iter().collect::<Vec<&Value>>())?;
            index.add_entry(bucket_id, tuples_len + pos);
            delete_tuples.push(delete_tuple.clone());
        }

        Ok((delete_tuples, index))
    }

    /// Rearrange virtual table under sharded `Update`.
    ///
    /// For more details, see `UpdateStrategy`.
    ///
    /// # Tuple format
    /// Each tuple in table will be used to create delete tuple
    /// and will be transformed to insert tuple itself.
    ///
    /// Original tuple format:
    /// ```text
    /// table_columns, old_shard_key_columns
    /// ```
    ///
    /// Insert tuple:
    /// ```text
    /// table_columns
    /// ```
    /// Bucket is calculated using `new_shard_columns_positions` and
    /// values from `table_columns`.
    ///
    /// Delete tuple:
    /// ```text
    /// pk_columns
    /// ```
    /// Values of `pk_columns` are taken from `table_columns`.
    /// Bucket is calculated using `old_shard_key_columns`.
    ///
    /// # Errors
    /// - invalid len of old shard key
    /// - invalid new shard key positions
    pub fn rearrange_for_update(
        &mut self,
        runtime: &(impl Vshard + Sized),
        old_shard_columns_len: usize,
        new_shard_columns_positions: &Vec<ColumnPosition>,
    ) -> Result<Option<usize>, SbroadError> {
        if new_shard_columns_positions.is_empty() {
            return Err(SbroadError::Invalid(
                Entity::Update,
                Some("No positions for new shard key!".into()),
            ));
        }
        if old_shard_columns_len == 0 {
            return Err(SbroadError::Invalid(
                Entity::Update,
                Some("Invalid len of old shard key: 0".into()),
            ));
        }
        if self.tuples.is_empty() {
            return Ok(None);
        };
        let (delete_tuples, mut index) =
            self.create_delete_tuples(runtime, old_shard_columns_len)?;

        // Index insert tuple, using new shard key values.
        for (pointer, update_tuple) in self.get_mut_tuples().iter_mut().enumerate() {
            let mut update_tuple_shard_key = Vec::with_capacity(new_shard_columns_positions.len());
            for pos in new_shard_columns_positions {
                let value = update_tuple.get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::TupleBuilderCommand,
                        Some(format!(
                            "invalid pos: {pos} for update tuple with len: {}",
                            update_tuple.len()
                        )),
                    )
                })?;
                update_tuple_shard_key.push(value);
            }
            let bucket_id = runtime.determine_bucket_id(&update_tuple_shard_key)?;
            index.add_entry(bucket_id, pointer);
        }
        let delete_tuple_len = delete_tuples.first().map(Vec::len);
        self.set_bucket_index(index.value);
        self.get_mut_tuples().extend(delete_tuples);
        Ok(delete_tuple_len)
    }

    /// Adds rows that are not present in `Self`
    /// from another virtual table.
    ///
    /// Assumptions:
    /// 1. All columns from `from_vtable` are located in
    /// a row from the beginning of the current vtable's columns.
    ///
    /// # Errors
    /// - invalid arguments
    pub fn add_missing_rows(&mut self, from_vtable: VirtualTable) -> Result<(), SbroadError> {
        if from_vtable.columns.len() >= self.columns.len() {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "from vtable must have less columns then self vtable!".into(),
            ));
        }
        let mut current_tuples: AHashSet<&[Value]> = AHashSet::with_capacity(self.tuples.len());

        let key_tuple_len = from_vtable.columns.len();
        for tuple in &self.tuples {
            let key_tuple = &tuple[..key_tuple_len];
            current_tuples.insert(key_tuple);
        }
        current_tuples.shrink_to_fit();

        let estimated_capacity = from_vtable.tuples.len().saturating_sub(self.tuples.len());
        let mut missing_tuples: HashMap<VTableTuple, usize, RepeatableState> =
            HashMap::with_capacity_and_hasher(estimated_capacity, RepeatableState);
        let mut missing_tuples_cnt: usize = 0;
        for tuple in from_vtable.tuples {
            if !current_tuples.contains(&tuple[..]) {
                missing_tuples
                    .entry(tuple)
                    .and_modify(|cnt| *cnt += 1)
                    .or_insert(1);
            }
            missing_tuples_cnt += 1;
        }

        let move_to_slice = |dst: &mut [Value], src: Vec<Value>| {
            for (to, from) in dst.iter_mut().zip(src) {
                *to = from;
            }
        };

        self.tuples.reserve(missing_tuples_cnt);
        for (key_tuple, count) in missing_tuples {
            let mut joined_tuple = vec![Value::Null; self.columns.len()];
            move_to_slice(&mut joined_tuple[0..key_tuple_len], key_tuple);
            for _ in 0..count - 1 {
                self.tuples.push(joined_tuple.clone());
            }
            self.tuples.push(joined_tuple);
        }

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
