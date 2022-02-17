use std::convert::TryInto;
use std::hash::Hasher;

use fasthash::{murmur3::Hasher32, FastHasher};

/// Determine bucket value using `murmur3` hash function
pub(in crate::executor::engine::cartridge) fn str_to_bucket_id(
    s: &str,
    bucket_count: usize,
) -> usize {
    let mut hash = Hasher32::new();
    hash.write(s.as_bytes());

    let hash: usize = hash.finish().try_into().unwrap();
    hash % bucket_count + 1
}

#[cfg(test)]
mod tests;
