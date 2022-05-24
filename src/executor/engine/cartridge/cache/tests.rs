use super::*;
use ahash::RandomState;
use pretty_assertions::assert_eq;

#[test]
fn test_yaml_schema_parser() {
    let test_schema = "spaces:
  EMPLOYEES:
    engine: \"memtx\"
    is_local: false
    temporary: false
    format:
      - name: \"ID\"
        is_nullable: false
        type: \"number\"
      - name: \"sysFrom\"
        is_nullable: false
        type: \"number\"
      - name: \"FIRST_NAME\"
        is_nullable: false
        type: \"string\"
      - name: \"sysOp\"
        is_nullable: false
        type: \"number\"
      - name: \"bucket_id\"
        is_nullable: true
        type: \"unsigned\"
    indexes:
      - type: \"TREE\"
        name: \"ID\"
        unique: true
        parts:
          - path: \"ID\"
            type: \"number\"
            is_nullable: false
          - path: \"sysFrom\"
            type: \"number\"
            is_nullable: false
      - type: \"TREE\"
        name: \"bucket_id\"
        unique: false
        parts:
          - path: \"bucket_id\"
            type: \"unsigned\"
            is_nullable: true
    sharding_key:
      - \"ID\"
  hash_testing:
    is_local: false
    temporary: false
    engine: \"memtx\"
    format:
      - name: \"identification_number\"
        type: \"integer\"
        is_nullable: false
      - name: \"product_code\"
        type: \"string\"
        is_nullable: false
      - name: \"product_units\"
        type: \"integer\"
        is_nullable: false
      - name: \"sys_op\"
        type: \"number\"
        is_nullable: false
      - name: \"bucket_id\"
        type: \"unsigned\"
        is_nullable: true
    indexes:
      - name: \"id\"
        unique: true
        type: \"TREE\"
        parts:
          - path: \"identification_number\"
            is_nullable: false
            type: \"integer\"
      - name: bucket_id
        unique: false
        parts:
          - path: \"bucket_id\"
            is_nullable: true
            type: \"unsigned\"
        type: \"TREE\"
    sharding_key:
      - \"identification_number\"
      - \"product_code\"";

    let mut s = ClusterAppConfig::new();
    s.load_schema(test_schema).unwrap();

    let expected_keys = vec!["identification_number", "product_code"];

    // FIXME: do we need "to_name()" here?
    let actual_keys = s.get_sharding_key_by_space("hash_testing").unwrap();
    assert_eq!(actual_keys, expected_keys)
}

#[test]
fn test_getting_table_segment() {
    let test_schema = "spaces:
  hash_testing:
    is_local: false
    temporary: false
    engine: \"memtx\"
    format:
      - name: \"identification_number\"
        type: \"integer\"
        is_nullable: false
      - name: \"product_code\"
        type: \"string\"
        is_nullable: false
      - name: \"product_units\"
        type: \"boolean\"
        is_nullable: false
      - name: \"sys_op\"
        type: \"number\"
        is_nullable: false
      - name: \"bucket_id\"
        type: \"unsigned\"
        is_nullable: true
    indexes:
      - name: \"id\"
        unique: true
        type: \"TREE\"
        parts:
          - path: \"identification_number\"
            is_nullable: false
            type: \"integer\"
      - name: bucket_id
        unique: false
        parts:
          - path: \"bucket_id\"
            is_nullable: true
            type: \"unsigned\"
        type: \"TREE\"
    sharding_key:
      - \"identification_number\"
      - \"product_code\"";

    let mut s = ClusterAppConfig::new();
    let system_columns: HashSet<String, RandomState> =
        [String::from("\"bucket_id\"")].iter().cloned().collect();
    s.set_exec_system_columns(system_columns);
    s.load_schema(test_schema).unwrap();

    let expected = Table::new_seg(
        "\"hash_testing\"",
        vec![
            Column::new("\"identification_number\"", Type::Integer, false),
            Column::new("\"product_code\"", Type::String, false),
            Column::new("\"product_units\"", Type::Boolean, false),
            Column::new("\"sys_op\"", Type::Number, false),
            Column::new("\"bucket_id\"", Type::Unsigned, true),
        ],
        &["\"identification_number\"", "\"product_code\""],
    )
    .unwrap();

    assert_eq!(
        s.get_table_segment("invalid_table").unwrap_err(),
        QueryPlannerError::SpaceNotFound
    );
    assert_eq!(s.get_table_segment("\"hash_testing\"").unwrap(), expected)
}

#[test]
fn test_waiting_timeout() {
    let mut s = ClusterAppConfig::new();
    s.set_exec_waiting_timeout(200);

    assert_ne!(s.get_exec_waiting_timeout(), 360);

    assert_eq!(s.get_exec_waiting_timeout(), 200);
}
