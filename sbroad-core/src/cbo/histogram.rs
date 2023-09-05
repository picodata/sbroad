//! Compressed histogram (Equi-height histogram with additional most common values array).
//!
//! Module used to represent logic of applying and transforming histogram statistics during
//! CBO algorithms.
use crate::cbo::helpers::{decimal_boundaries_occupied_fraction, scale_strings};
use crate::errors::{Entity, SbroadError};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::mem::take;
use tarantool::decimal::Decimal;

/// Trait that denotes types applicable to be stored in the statistics.
pub trait Scalar: Debug + Clone + PartialEq + Eq + PartialOrd + Ord + Hash + 'static {
    /// This type should always represent Self type.
    type Other;

    /// Function needed for finding fraction of a bucket that value covers in case it falls into it.
    ///
    /// # Errors
    /// - Error of casting to Decimal occurred.
    /// - Unable to find Strings fraction.
    fn boundaries_occupied_fraction(
        &self,
        left_boundary: &Self::Other,
        right_boundary: &Self::Other,
    ) -> Result<Decimal, SbroadError>;
}
impl Scalar for i64 {
    type Other = i64;

    fn boundaries_occupied_fraction(
        &self,
        left_boundary: &Self::Other,
        right_boundary: &Self::Other,
    ) -> Result<Decimal, SbroadError> {
        decimal_boundaries_occupied_fraction(
            Decimal::from(*self),
            Decimal::from(*left_boundary),
            Decimal::from(*right_boundary),
        )
    }
}
impl Scalar for u64 {
    type Other = u64;

    fn boundaries_occupied_fraction(
        &self,
        left_boundary: &Self::Other,
        right_boundary: &Self::Other,
    ) -> Result<Decimal, SbroadError> {
        decimal_boundaries_occupied_fraction(
            Decimal::from(*self),
            Decimal::from(*left_boundary),
            Decimal::from(*right_boundary),
        )
    }
}
impl Scalar for String {
    type Other = String;

    fn boundaries_occupied_fraction(
        &self,
        left_boundary: &Self::Other,
        right_boundary: &Self::Other,
    ) -> Result<Decimal, SbroadError> {
        let (value, left, right) = scale_strings(self, left_boundary, right_boundary)?;
        decimal_boundaries_occupied_fraction(value, left, right)
    }
}
impl Scalar for Decimal {
    type Other = Decimal;

    fn boundaries_occupied_fraction(
        &self,
        left_boundary: &Self::Other,
        right_boundary: &Self::Other,
    ) -> Result<Decimal, SbroadError> {
        decimal_boundaries_occupied_fraction(*self, *left_boundary, *right_boundary)
    }
}

/// Compressed histogram Most Common Value (MCV) with corresponding frequency.
#[derive(Clone, Debug)]
pub struct Mcv<T: Scalar> {
    pub(crate) value: T,
    /// Number of such a value divided by the number of all values in a column.
    ///
    /// Represented not with `f64`, but with `Decimal` type because the former doesn't support NaNs
    /// and implements `Eq` trait, that we need for putting this struct into the HashSet.
    pub(crate) frequency: Decimal,
}

impl<T: Scalar> PartialEq for Mcv<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Scalar> Eq for Mcv<T> {}
impl<T: Scalar> Hash for Mcv<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<T: Scalar> Mcv<T> {
    #[must_use]
    pub fn new(value: T, frequency: Decimal) -> Self {
        Mcv { value, frequency }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McvSet<T: Scalar> {
    pub(crate) inner: HashSet<Mcv<T>>,
}

impl<T: Scalar> Default for McvSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Scalar> McvSet<T> {
    #[must_use]
    pub fn new() -> McvSet<T> {
        McvSet {
            inner: HashSet::new(),
        }
    }

    /// Helper function to fill set from vec.
    #[must_use]
    pub fn from_vec(vec: &Vec<Mcv<T>>) -> McvSet<T> {
        let mut mcv_vec = Self::new();
        for mcv in vec {
            mcv_vec.insert(mcv.clone());
        }
        mcv_vec
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> McvSet<T> {
        McvSet {
            inner: HashSet::with_capacity(capacity),
        }
    }

    pub fn insert(&mut self, value: Mcv<T>) -> bool {
        self.inner.insert(value)
    }

    pub fn get(&self, element: &Mcv<T>) -> Option<&Mcv<T>> {
        self.inner.get(element)
    }

    pub fn remove(&mut self, element: &Mcv<T>) -> bool {
        self.inner.remove(element)
    }

    pub fn contains(&self, element: &Mcv<T>) -> bool {
        self.inner.contains(element)
    }

    /// Return the sum of all mcvs frequencies.
    #[must_use]
    pub fn frequencies_sum(&self) -> Decimal {
        let mut sum = Decimal::from(0);
        for mcv in &self.inner {
            sum += mcv.frequency;
        }
        sum
    }

    /// Calculate absolute number of most common values.
    ///
    /// # Errors
    /// - Cast errors appeared during values number calculation.
    pub fn values_number(&self, rows_number: u64) -> Result<u64, SbroadError> {
        let mut decimal_values_number = Decimal::from(0);
        for mcv in &self.inner {
            decimal_values_number += mcv.frequency * Decimal::from(rows_number);
        }
        let floored_values_number = decimal_values_number.floor();
        floored_values_number.to_u64().ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Statistics,
                Some(String::from("Unable to calculate mcv value number")),
            )
        })
    }
}

/// Type of a `Histogram` bucket (whether this bucket is (first in a whole sequence of buckets).
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
enum BucketType<T> {
    /// Representation of the (first histogram bucket with inclusive `from` edge.
    /// As soon as buckets sequence is represented in a view of a linked list, first bucket is the
    /// only bucket that can't take `from` bound from the previous bucket. That's why it has to
    /// store the value by itself.
    First { from_boundary: T },
    /// Representation of a non-first histogram bucket with non-inclusive `from` edge.
    NonFirst,
}

/// Histogram bucket structure.
///
/// Represented in a view of a linked list in order to maintain strictness of buckets' bounds.
/// Every bucket must contain two fields: `from` (right bound) and `to` (left bound). As soon as
/// `to` field of any bucket is equal to `from` field of following bucket (if one exists) we don't
/// want to duplicate it (don't want to store buckets in a view of bounds array like
/// [(`from_1`, `to_1`), (`from_2`, `to_2`), ...], where `from_i` = `from_(i-1)`).
///
/// Note, that the buckets' frequency is calculated as
/// `number_of_rows_in_the_histogram / number_of_buckets`.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Bucket<T> {
    /// Bucket type (whether this bucket is first in buckets sequence or not).
    bucket_type: BucketType<T>,
    /// Right bound of the bucket.
    to_boundary: T,
}

impl<T: Scalar> Bucket<T> {
    /// Checks that passed constant value falls into bucket boundaries.
    /// `from_boundary` is taken as a `to` boundary of preceding bucket or as self `from` boundary
    /// value, in case we deal with the first bucket.
    #[allow(dead_code)]
    fn constant_falls_into_bucket(
        &self,
        previous_bucket: Option<&Bucket<T>>,
        value: &T,
    ) -> Result<bool, SbroadError> {
        let from_boundary = match &self.bucket_type {
            BucketType::First { from_boundary } => from_boundary,
            BucketType::NonFirst => {
                if let Some(previous_bucket) = previous_bucket {
                    &previous_bucket.to_boundary
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Statistics,
                        Some(String::from(
                            "From boundary must have been passed for\
                    NonFirst bucket",
                        )),
                    ));
                }
            }
        };
        Ok(value > from_boundary && value < &self.to_boundary)
    }

    /// Helper function to get `from` boundary in case we are dealing with a first bucket.
    #[allow(dead_code)]
    fn get_from(&self) -> Result<T, SbroadError> {
        match &self.bucket_type {
            BucketType::First { from_boundary } => Ok(from_boundary.clone()),
            BucketType::NonFirst => Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(String::from("Can't get from value of non first bucket")),
            )),
        }
    }
}

/// Histogram buckets sequence.
/// For more details see `Bucket` structure comments.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct HistogramBuckets<T: Scalar> {
    pub(crate) inner: Vec<Bucket<T>>,
}

impl<T: Scalar> Default for HistogramBuckets<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Scalar> HistogramBuckets<T> {
    pub(crate) fn new() -> Self {
        Self { inner: Vec::new() }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<T: Scalar> TryFrom<&mut Vec<T>> for HistogramBuckets<T> {
    type Error = SbroadError;

    /// Helper function to construct buckets vec from the boundaries array that was
    /// gathered on storages.
    ///
    /// # Errors
    /// - Passed boundaries array contains less than 2 values.
    fn try_from(boundaries: &mut Vec<T>) -> Result<Self, Self::Error> {
        let mut buckets = HistogramBuckets::new();
        if boundaries.len() < 2 {
            return Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(String::from(
                    "Boundaries list must contain at least 2 values",
                )),
            ));
        }

        let mut prev_boundary: Option<T> = None;
        // Flag, indicating whether first bucket is already created.
        let mut first_added = false;

        for boundary in take(boundaries) {
            if first_added {
                let new_bucket = Bucket {
                    bucket_type: BucketType::NonFirst,
                    to_boundary: boundary,
                };
                buckets.inner.push(new_bucket);
            } else if let Some(ref prev_boundary) = prev_boundary {
                first_added = true;
                let new_bucket = Bucket {
                    bucket_type: BucketType::First {
                        // Trait `Default` is not satisfied for `Scalar` so that
                        // we can't use `take` method here.
                        from_boundary: prev_boundary.clone(),
                    },
                    to_boundary: boundary,
                };
                buckets.inner.push(new_bucket);
            } else {
                prev_boundary = Some(boundary);
            }
        }
        Ok(buckets)
    }
}

/// Representation of Compressed histogram.
///
/// **Note**: We don't keep the number of rows stored in the corresponding column during the process
/// of histogram creation in order to support cases of table size change. We always take the
/// information about `rows_number` from `TableStats` of column's table.
///
/// It's always implied that at least one of two `most_common` or `buckets` arrays is not empty.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::module_name_repetitions)]
pub struct Histogram<T: Scalar> {
    // Most common values and their frequencies.
    pub(crate) most_common_values: McvSet<T>,
    /// Histogram buckets.
    ///
    /// **Note**: Values from mcv are not included in histogram buckets.
    ///
    /// Boundaries:
    /// * i = 0 -> [b_0; b_1] (where `from` field of the bucket is included)
    /// * i = 1 -> (b_1; b_2]
    /// * ...
    /// * i = n -> (b_(n-2); b_(n-1)]
    pub(crate) buckets: HistogramBuckets<T>,
    /// Fraction of NULL values among all column rows.
    /// Always positive value from 0 to 1.
    ///
    /// Calculated as `number_of_null_values / rows_number`.
    pub(crate) null_fraction: Decimal,
    /// Number of distinct values divided by the number of rows.
    /// Always positive value from 0 to 1.
    ///
    /// **Note**: in order to calculate `number_of_distinct_values` (absolute value) we must
    /// use formula `rows_number * (1 - null_fraction) * distinct_values_fraction`.
    /// In case this field is near 1.0 value, it means the column must contains unique values.
    pub(crate) distinct_values_fraction: Decimal,
}

impl<T: Scalar> Histogram<T> {
    #[must_use]
    pub fn new(
        most_common_values: McvSet<T>,
        buckets: HistogramBuckets<T>,
        null_fraction: Decimal,
        distinct_values_fraction: Decimal,
    ) -> Self {
        Self {
            most_common_values,
            buckets,
            null_fraction,
            distinct_values_fraction,
        }
    }

    #[must_use]
    pub fn buckets_number(&self) -> usize {
        self.buckets.len()
    }

    /// Helper function to calculate the frequency of the histogram buckets.
    /// **Note**: in the context of buckets __frequency__ is the number of elements stored in each
    /// bucket. That's why it's an integer number and not float.
    ///
    /// # Errors
    /// - Cast errors appeared during frequency calculation.
    pub fn calculate_buckets_frequency(&self, rows_number: u64) -> Result<u64, SbroadError> {
        let decimal_freq = Decimal::from(rows_number)
            * (Decimal::from(1) - self.null_fraction - self.mcv_frequencies_sum())
            / self.buckets_number();
        decimal_freq.to_u64().ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Statistics,
                Some(String::from("Unable to calculate buckets frequency")),
            )
        })
    }

    /// Get mcv array frequencies sum.
    /// **Note**: sum of frequencies and not of absolute number values.
    #[allow(dead_code)]
    fn mcv_frequencies_sum(&self) -> Decimal {
        self.most_common_values.frequencies_sum()
    }

    /// Get the number of most common values.
    /// **Note**: in `Mcv` we store frequency of the value as a fraction from 0 to 1.
    /// In order to get the number of most common elements we have to multiply this float value
    /// on the number of rows in the column.
    ///
    /// # Errors
    /// - Cast errors appeared during values number calculation.
    pub fn most_common_values_number(&self, rows_number: u64) -> Result<u64, SbroadError> {
        self.most_common_values.values_number(rows_number)
    }
}

impl<T: Scalar> Default for Histogram<T> {
    fn default() -> Self {
        Self {
            most_common_values: McvSet::default(),
            buckets: HistogramBuckets::default(),
            null_fraction: Decimal::from(0),
            distinct_values_fraction: Decimal::from(0),
        }
    }
}
