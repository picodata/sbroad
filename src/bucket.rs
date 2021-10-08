use std::collections::HashMap;
use std::hash::Hasher;
use fasthash::{murmur3::Hasher32, FastHasher};
use crate::errors::QueryPlannerError;

pub fn get_bucket_id(filters: &HashMap<String, String>, sharding_key: &Vec<String>, bucket_count: u64) -> Result<u64, QueryPlannerError> {
    let mut hash = Hasher32::new();

    for key_part in sharding_key.iter() {
        match filters.get(key_part) {
            Some(v) => hash.write(v.as_bytes()),
            None => return Err(QueryPlannerError::BucketIdError)
        }
    }

    Ok(hash.finish() % bucket_count)
}

#[test]
fn test_bucket_id() {
    let mut test_vals = HashMap::new();

    test_vals.insert("id".to_string(), "1".to_string());
    test_vals.insert("name".to_string(), "222".to_string());

    let mut sharding_key = vec!["id".to_string()];
    assert_eq!(get_bucket_id(&test_vals, &sharding_key, 30000).unwrap(), 3939);

    sharding_key = vec!["id".to_string(), "name".to_string()];
    assert_eq!(get_bucket_id(&test_vals, &sharding_key, 30000).unwrap(), 2926);


    test_vals.clear();
    test_vals.insert("id".to_string(), "100".to_string());
    test_vals.insert("name".to_string(), "тесты".to_string());

    sharding_key = vec!["id".to_string(), "name".to_string()];
    assert_eq!(get_bucket_id(&test_vals, &sharding_key, 30000).unwrap(), 17338);
}