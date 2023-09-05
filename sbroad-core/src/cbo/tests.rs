use crate::cbo::histogram::HistogramBuckets;
use crate::errors::SbroadError;

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
    let bucket_len = (value_max - value_min) / (buckets_count as i64);
    let mut boundaries = vec![value_min];
    for i in 0..buckets_count {
        let next_index = i + 1;
        let adding = (next_index as i64) * bucket_len;
        boundaries.push(value_min + adding);
    }
    let buckets = HistogramBuckets::try_from(&mut boundaries)?;
    Ok(buckets)
}
