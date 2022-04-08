//! Bucket hash module.

use fasthash::{murmur3::Hasher32, FastHasher};
use std::hash::Hasher;

/// Determine bucket value using `murmur3` hash function
pub(in crate::executor::engine) fn str_to_bucket_id(s: &str, bucket_count: usize) -> u64 {
    let mut hash = Hasher32::new();
    hash.write(s.as_bytes());
    hash.finish() % bucket_count as u64 + 1
}

#[cfg(test)]
mod tests;
