use super::*;
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

    let mut s = RouterConfiguration::new();
    s.load_schema(test_schema).unwrap();

    let expected_keys = vec!["\"identification_number\"", "\"product_code\""];
    let actual_keys = s.get_sharding_key_by_space("\"hash_testing\"").unwrap();
    assert_eq!(actual_keys, expected_keys);
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
      - name: detail
        type: array
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

    let mut s = RouterConfiguration::new();
    s.set_sharding_column("\"bucket_id\"".into());
    s.load_schema(test_schema).unwrap();

    let expected = Table::new_seg(
        "\"hash_testing\"",
        vec![
            Column::new("\"identification_number\"", Type::Integer, ColumnRole::User),
            Column::new("\"product_code\"", Type::String, ColumnRole::User),
            Column::new("\"product_units\"", Type::Boolean, ColumnRole::User),
            Column::new("\"sys_op\"", Type::Number, ColumnRole::User),
            Column::new("\"detail\"", Type::Array, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
        ],
        &["\"identification_number\"", "\"product_code\""],
    )
    .unwrap();

    assert_eq!(
        s.get_table_segment("invalid_table").unwrap_err(),
        SbroadError::NotFound(Entity::Space, r#""INVALID_TABLE""#.into())
    );
    assert_eq!(s.get_table_segment("\"hash_testing\"").unwrap(), expected);
}

#[test]
fn test_waiting_timeout() {
    let mut s = RouterConfiguration::new();
    s.set_waiting_timeout(200);

    assert_ne!(s.get_exec_waiting_timeout(), 360);

    assert_eq!(s.get_exec_waiting_timeout(), 200);
}

#[test]
fn test_invalid_schema() {
    let test_schema = r#"spaces:
      TEST_SPACE:
        engine: memtx
        is_local: false
        temporary: false
        format:
          - name: bucket_id
            type: unsigned
            is_nullable: false
          - name: FID
            type: integer
            is_nullable: false
          - name: DATE_START
            type: integer
            is_nullable: false
          - name: DATE_END
            type: integer
            is_nullable: false
          - name: COMMON_ID
            type: string
            is_nullable: false
          - name: EXCLUDE_ID
            type: string
            is_nullable: true
          - name: COMMON_TEXT
            type: string
            is_nullable: false
          - name: COMMON_DETAIL
            type: map
            is_nullable: false
          - name: TYPOLOGY_TYPE
            type: integer
            is_nullable: true
          - name: TYPOLOGY_ID
            type: string
            is_nullable: true
        indexes:
          - type: TREE
            name: primary
            unique: true
            parts:
              - path: FID
                type: integer
                is_nullable: false
              - path: COMMON_ID
                type: string
                is_nullable: false
              - path: DATE_START
                type: integer
                is_nullable: false
          - type: TREE
            name: bucket_id
            unique: false
            parts:
              - path: bucket_id
                type: unsigned
                is_nullable: false
        sharding_key:
          - FID
          - COMMON_ID
          - DATE_START
"#;

    let mut s = RouterConfiguration::new();
    s.set_sharding_column("\"bucket_id\"".into());

    assert_eq!(
        s.load_schema(test_schema).unwrap_err(),
        SbroadError::NotImplemented(Entity::Type, "map".into())
    );
}
