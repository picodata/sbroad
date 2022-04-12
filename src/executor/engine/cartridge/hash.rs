//! Bucket hash module.

use hash32::{Hasher, Murmur3Hasher};

#[must_use]
/// A simple function to calculate the bucket id from a string slice.
/// `(MurMur3 hash at str) % bucket_count + 1`
pub fn str_to_bucket_id(s: &str, bucket_count: usize) -> u64 {
    let mut hasher = Murmur3Hasher::default();
    hasher.write(s.as_bytes());
    u64::from(hasher.finish()) % bucket_count as u64 + 1
}

#[cfg(test)]
mod tests;
