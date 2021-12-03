use std::collections::HashMap;
use std::convert::TryInto;
use std::hash::Hasher;

use fasthash::{murmur3::Hasher32, FastHasher};

use crate::errors::QueryPlannerError;

pub fn get_bucket_id(
    filters: &HashMap<String, String>,
    sharding_key: &[String],
    bucket_count: usize,
) -> Result<usize, QueryPlannerError> {
    let mut str_shard_val = String::new();
    for key_part in sharding_key.iter() {
        if let Some(v) = filters.get(key_part) {
            str_shard_val.push_str(v);
        } else {
            return Err(QueryPlannerError::BucketIdError);
        }
    }

    Ok(str_to_bucket_id(&str_shard_val, bucket_count))
}

pub fn str_to_bucket_id(s: &str, bucket_count: usize) -> usize {
    let mut hash = Hasher32::new();
    hash.write(s.as_bytes());

    let hash: usize = hash.finish().try_into().unwrap();
    hash % bucket_count
}

#[test]
fn test_bucket_id() {
    let mut test_vals = HashMap::new();

    test_vals.insert("id".to_string(), "1".to_string());
    test_vals.insert("name".to_string(), "222".to_string());

    let mut sharding_key = vec!["id".to_string()];
    assert_eq!(
        get_bucket_id(&test_vals, &sharding_key, 30000).unwrap(),
        3939
    );

    sharding_key = vec!["id".to_string(), "name".to_string()];
    assert_eq!(
        get_bucket_id(&test_vals, &sharding_key, 30000).unwrap(),
        2926
    );

    test_vals.clear();
    test_vals.insert("id".to_string(), "100".to_string());
    test_vals.insert("name".to_string(), "тесты".to_string());

    sharding_key = vec!["id".to_string(), "name".to_string()];
    assert_eq!(
        get_bucket_id(&test_vals, &sharding_key, 30000).unwrap(),
        17338
    );

    test_vals.clear();
    test_vals.insert("id".to_string(), "4".to_string());
    test_vals.insert("varchar_col'".to_string(), "TEST".to_string());
    test_vals.insert("int_col".to_string(), "5".to_string());
    test_vals.insert("bigint_col".to_string(), "50".to_string());
    test_vals.insert("timestamp_col".to_string(), "1605647472000000".to_string());
    test_vals.insert("time_col".to_string(), "100000000".to_string());
    test_vals.insert(
        "uuid_col".to_string(),
        "d92beee8-749f-4539-aa15-3d2941dbb0f1".to_string(),
    );
    test_vals.insert("char_col".to_string(), "x".to_string());
    test_vals.insert("int32_col".to_string(), "32".to_string());
    test_vals.insert("link_col".to_string(), "https://google.com".to_string());

    sharding_key = vec![
        "id".to_string(),
        "varchar_col'".to_string(),
        "int_col".to_string(),
        "bigint_col".to_string(),
        "timestamp_col".to_string(),
        "time_col".to_string(),
        "uuid_col".to_string(),
        "char_col".to_string(),
        "int32_col".to_string(),
        "link_col".to_string(),
    ];
    assert_eq!(
        get_bucket_id(&test_vals, &sharding_key, 30000).unwrap(),
        13814
    );
}

#[test]
fn test_bucket_id_by_str() {
    assert_eq!(str_to_bucket_id("100тесты", 30000), 17338);

    assert_eq!(
        str_to_bucket_id("4TEST5501605647472000000100000000d92beee8-749f-4539-aa15-3d2941dbb0f1x32https://google.com", 30000),
        13814
    );

    let mut test_vals = HashMap::new();

    test_vals.insert("id".to_string(), "1".to_string());
    test_vals.insert("name".to_string(), "123".to_string());

    let sharding_key = vec!["id".to_string(), "name".to_string()];
    assert_eq!(
        get_bucket_id(&test_vals, &sharding_key, 30000).unwrap(),
        str_to_bucket_id("1123", 30000),
    );
}
