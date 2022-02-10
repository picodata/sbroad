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

#[test]
fn test_bucket_id_by_str() {
    assert_eq!(str_to_bucket_id("100тесты", 30000), 17339);

    assert_eq!(
        str_to_bucket_id("4TEST5501605647472000000100000000d92beee8-749f-4539-aa15-3d2941dbb0f1x32https://google.com", 30000),
        13815
    );

    assert_eq!(360, str_to_bucket_id("1123", 30000),);
}

#[test]
fn test_zero_bucket_id() {
    assert_eq!(str_to_bucket_id("18810", 30000), 1);
}
