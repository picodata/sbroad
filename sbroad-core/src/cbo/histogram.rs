//! Equi-height histogram.
//!
//! Module used to represent logic of applying and transforming histogram statistics during
//! CBO algorithms.

use crate::ir::value::Value;

/// Helper structure that represents pair of most common value in the column and its frequency.
#[derive(Debug, PartialEq, Clone)]
struct MostCommonValueWithFrequency {
    value: Value,
    frequency: f64,
}

impl MostCommonValueWithFrequency {
    #[allow(dead_code)]
    fn new(value: Value, frequency: f64) -> Self {
        MostCommonValueWithFrequency { value, frequency }
    }
}

/// Representation of histogram bucket.
#[derive(Clone, Debug, PartialEq)]
struct Bucket<'bucket> {
    /// From (left border) value of the bucket (not inclusive, except for the first bucket)
    pub from: &'bucket Value,
    /// To (right order) value of the bucket (inclusive)
    pub to: &'bucket Value,
    /// Bucket frequency.
    /// Represents the number of elements stored in the bucket.
    pub frequency: usize,
}

/// Representation of equi-height histogram.
///
/// It's assumed that if the histogram is present, then all
/// its fields are filled.
///
/// As soon as the biggest part of the logic is taken from
/// `PostgreSQL` implementation, you may see `PostgreSQL lines` comments
/// in some places. It means you can find
/// implementation of `PostgreSQL` logic by searching the provided text.
///
/// `PostgreSQL` version: `REL_15_2`
#[derive(Debug, PartialEq, Clone)]
pub struct Histogram<'histogram> {
    // Most common values and their frequencies.
    most_common: Vec<MostCommonValueWithFrequency>,
    /// Histogram buckets.
    ///
    /// **Note**: Values from mcv are not included in histogram buckets.
    ///
    /// Boundaries:
    /// * i = 0 -> [b_0; b_1] (where `from` field of the bucket is included)
    /// * i = 1 -> (b_1; b_2]
    /// * ...
    /// * i = n -> (b_(n-2); b_(n-1)]
    buckets: Vec<Bucket<'histogram>>,
    /// Fraction of NULL values among all column values.
    null_fraction: f64,
    /// Number of distinct values for the whole histogram.
    ///
    /// **Note**: It is easy during the histogram calculation
    /// phase to calculate ndv as soon as the elements have to be sorted
    /// in order to construct bucket_bounds Vec.
    ndv: usize,
    /// Number of elements added into histogram.
    ///
    /// **Note**: the number of values added into histogram don't
    /// have to be equal to the number of rows in the table as soon as
    /// some rows might have been added after the histogram was created.
    elements_count: usize,
}
