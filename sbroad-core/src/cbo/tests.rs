use crate::cbo::histogram::HistogramBuckets;
use crate::errors::{Entity, SbroadError};

/// Helper function to construct `Buckets` struct (of `i64` type) that is similar to one gathered by
/// the real algorithm of statistics creation on storages.
///
/// # Errors
/// - Unable to construct `HistogramBucket` from gotten boundaries list.
pub fn construct_i64_buckets(
    buckets_count: usize,
    value_min: i64,
    value_max: i64,
) -> Result<HistogramBuckets<i64>, SbroadError> {
    let casted_buckets_count = i64::try_from(buckets_count).map_err(|_| {
        SbroadError::Invalid(
            Entity::Value,
            Some(String::from("Unable to cast buckets_count to i64")),
        )
    })?;
    let bucket_len = (value_max - value_min) / casted_buckets_count;
    let mut boundaries = vec![value_min];
    for i in 0..buckets_count {
        let next_index = i64::try_from(i + 1).map_err(|_| {
            SbroadError::Invalid(
                Entity::Value,
                Some(String::from("Unable to cast next index to i64")),
            )
        })?;
        let adding = next_index * bucket_len;
        boundaries.push(value_min + adding);
    }
    let buckets = HistogramBuckets::try_from(&mut boundaries)?;
    Ok(buckets)
}
