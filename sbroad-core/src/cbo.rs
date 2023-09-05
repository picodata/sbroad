//! Cost Based Optimizer (CBO).
//!
//! Module used to optimize IR tree using statistics and plan cost calculation algorithms.
//!
//! As soon as the biggest part of the logic is taken from
//! `PostgreSQL` implementation, you may see `PostgreSQL lines` comments
//! in some places with indication of function names and corresponding lines of code.
//! `PostgreSQL` version: `REL_15_2`.

use crate::cbo::histogram::{Histogram, Scalar};
use std::fmt::Debug;
use std::hash::Hash;

/// Struct representing statistics for the whole table.
#[derive(Debug, Clone, PartialEq)]
pub struct TableStats {
    /// Number of rows in the table.
    rows_number: u64,
}

impl TableStats {
    #[must_use]
    pub fn new(rows_number: u64) -> Self {
        Self { rows_number }
    }
}

/// Struct representing statistics for concrete column.
///
/// The reason some values are stored in that structure and not in `Histogram`: some values may be
/// useful for selectivity estimation when histograms are on the stage of rebuilding and
/// actualization. `min_value`, `max_value` and `null_fraction` may be stored without histogram
/// creation.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStats<T: Scalar> {
    /// Column MIN value.
    min_value: T,
    /// Column MAX value.
    max_value: T,
    /// Average size of column row in bytes.
    avg_size: u64,
    /// Column Compressed histogram.
    histogram: Option<Histogram<T>>,
}

#[allow(dead_code)]
impl<T: Scalar> ColumnStats<T> {
    #[must_use]
    pub fn new(min_value: T, max_value: T, avg_size: u64, histogram: Option<Histogram<T>>) -> Self {
        Self {
            min_value,
            max_value,
            avg_size,
            histogram,
        }
    }
}

/// Struct that represents column information needed for its size calculation (in bytes).
///
/// These are the only two fields that are passed from bottom to top during recursive
/// relational tree traversal.
/// These two fields change when relational operator or filter are applied to the column.
#[allow(dead_code)]
pub struct ColumnVolumeInfo {
    /// Number of rows.
    rows_number: u64,
    /// Average size of column rows in bytes.
    avg_size: u64,
}

/// Alias for pair of table name and column id in the table.
/// Used as a key for statistics retrieval from system space.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableColumnPair(String, usize);

#[allow(dead_code)]
impl TableColumnPair {
    pub(crate) fn new(table_name: String, column_id: usize) -> Self {
        Self(table_name, column_id)
    }
}

/// Helper struct representing pair of (`TableStats`, `ColumnStats` (upcasted)).
pub struct TableColumnStatsPair<T: Scalar>(pub TableStats, pub ColumnStats<T>);

pub mod helpers;
pub mod histogram;
#[cfg(feature = "mock")]
pub mod tests;
