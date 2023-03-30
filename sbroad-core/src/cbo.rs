//! Cost Based Optimizer.
//!
//! Module used to optimize IR tree using statistics and plan cost calculation algorithms.
//!
//! As soon as the biggest part of the logic is taken from
//! `PostgreSQL` implementation, you may see `PostgreSQL lines` comments
//! in some places with indication of function names and corresponding lines of code.
//! `PostgreSQL` version: `REL_15_2`.

use crate::cbo::histogram::Histogram;
use crate::errors::{Entity, SbroadError};
use crate::ir::value::Value;
use std::collections::HashMap;

/// Struct representing statistics for the whole table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableStats {
    /// Table name.
    table_name: String,
    /// Number of rows in the table.
    rows_number: u64,
    /// Counters of executed DML operations.
    ///
    /// We need them in order to understand when to
    /// actualize table statistics.
    ///
    /// Note, that `upsert` command execution is handled by core in a view of
    /// updating `update_counter` or `insert_counter`
    insert_counter: u32,
    update_counter: u32,
    remove_counter: u32,
}

impl TableStats {
    #[must_use]
    pub fn new(
        table_name: String,
        rows_number: u64,
        insert_counter: u32,
        update_counter: u32,
        remove_counter: u32,
    ) -> Self {
        Self {
            table_name,
            rows_number,
            insert_counter,
            update_counter,
            remove_counter,
        }
    }
}

/// Struct representing statistics for column.
///
/// May represent transformed statistics, appeared during application
/// of CBO algorithms. Note, that transformation of column statistics must
/// be applied to every field of the structure.
///
/// The reasons some values are stored in that structure and not in `Histogram` structure:
/// * Sometimes we do not want to receive whole histogram info. E.g. when
/// we don't want to apply WHERE and ON conditions, but want to estimate the size
/// of the table using only `avg_value_size` info.
/// * Some values may be useful for selectivity estimation
/// when histograms are on the stage of rebuilding and actualization. Such values as
/// MIN/MAX and `null_fraction` may be stored without histogram creation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnStats<'col_stats> {
    /// Number of elements in the column.
    ///
    /// Note, that the field is filled only ofter `TableStats` for the column table is retrieved.
    rows_number: usize,
    /// Min value in the column.
    min_value: &'col_stats Value,
    /// Max value in the column.
    max_value: &'col_stats Value,
    /// Average size of column row in bytes.
    avg_size: u64,
    /// Compressed histogram (equi-height histogram with mcv array).
    ///
    /// May have no values inside (`elements_count` field equal to 0)
    /// it's always presented in `ColumnStats` structure.
    histogram: &'col_stats Histogram<'col_stats>,
}

#[allow(dead_code)]
impl<'column_stats> ColumnStats<'column_stats> {
    #[must_use]
    pub fn new(
        elements_count: usize,
        min_value: &'column_stats Value,
        max_value: &'column_stats Value,
        avg_value_size: u64,
        histogram: &'column_stats Histogram,
    ) -> Self {
        Self {
            rows_number: elements_count,
            min_value,
            max_value,
            avg_size: avg_value_size,
            histogram,
        }
    }
}

// Alias for pair of table name and column id in the table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableColumnPair(String, usize);

#[allow(dead_code)]
impl TableColumnPair {
    pub(crate) fn new(table_name: String, column_id: usize) -> Self {
        Self(table_name, column_id)
    }
}

/// Structure for global optimizations
/// that contains whole statistics information
/// which may be useful for optimization.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) struct CostBasedOptimizer<'cbo> {
    /// Map of
    /// { (Table name, Column name) -> ColumnStats }
    /// that originates from `Scan` nodes during traversal of IR relational operators tree.
    /// Used in `calculate_cost` function in the `Scan` node in order to retrieve stats for
    /// requested columns.
    initial_column_stats: HashMap<TableColumnPair, ColumnStats<'cbo>>,
    /// Vector of `Histogram` structures.
    /// Initially it's filled with histograms gathered from storages.
    /// It's updated with new histograms during the statistics transformation process:
    /// every transformation like UNION, ARITHMETIC_MAP or other will create new histogram and
    /// append it to the `histograms` vector.
    histograms: Vec<Histogram<'cbo>>,
    /// Vector of `Value` structures.
    /// A storage of values used during the statistics transformation and application process.
    /// In order not to store values in histogram `Bucket` and in `ColumnStats` structures
    /// of histograms will store references to the values stored in this storage.
    values_cache: Vec<Value>,
}

#[allow(dead_code)]
impl<'cbo> CostBasedOptimizer<'cbo> {
    fn new() -> Self {
        CostBasedOptimizer {
            initial_column_stats: HashMap::new(),
            histograms: Vec::new(),
            values_cache: Vec::new(),
        }
    }

    /// Get `initial_column_stats` map.
    #[cfg(test)]
    fn get_initial_column_stats(&self) -> &HashMap<TableColumnPair, ColumnStats> {
        &self.initial_column_stats
    }

    /// Get value from `initial_column_stats` map by `key`
    fn get_from_initial_column_stats(&self, key: &TableColumnPair) -> Option<&ColumnStats> {
        self.initial_column_stats.get(key)
    }

    /// Add new initial column stats to the `initial_column_stats` map.
    fn update_initial_column_stats(
        &'cbo mut self,
        key: TableColumnPair,
        stats: ColumnStats<'cbo>,
    ) -> Option<ColumnStats> {
        self.initial_column_stats.insert(key, stats)
    }

    /// Adds new histogram to the `histograms` vector.
    /// Returns the reference to the newly added histogram.
    fn push_histogram(
        &'cbo mut self,
        histogram: Histogram<'cbo>,
    ) -> Result<&Histogram, SbroadError> {
        self.histograms.push(histogram);
        self.histograms.last().ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Histogram,
                Some(String::from("No values in the cbo histograms vector")),
            )
        })
    }

    /// Adds new value to the `values_cache` vector.
    /// Returns the reference to the newly added value.
    fn push_value(&mut self, value: Value) -> Result<&Value, SbroadError> {
        self.values_cache.push(value);
        self.values_cache.last().ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Value,
                Some(String::from("No values in the cbo values cache")),
            )
        })
    }
}

pub mod histogram;
