use crate::cache::Metadata;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use pretty_assertions::assert_eq;
use std::collections::HashSet;

const CARTRIDGE_SCHEMA: &str = r#"spaces:
  test_space:
    engine: "memtx"
    is_local: false
    temporary: false
    format:
      - name: "id"
        is_nullable: false
        type: "number"
      - name: "sysFrom"
        is_nullable: false
        type: "number"
      - name: "FIRST_NAME"
        is_nullable: false
        type: "string"
      - name: "sysOp"
        is_nullable: false
        type: "number"
      - name: "bucket_id"
        is_nullable: true
        type: "unsigned"
    indexes:
      - type: "TREE"
        name: "id"
        unique: true
        parts:
          - path: "id"
            type: "number"
            is_nullable: false
      - type: "TREE"
        name: "bucket_id"
        unique: false
        parts:
          - path: "bucket_id"
            type: "unsigned"
            is_nullable: true
    sharding_key:
      - id
  test_space_hist:
    engine: "memtx"
    is_local: false
    temporary: false
    format:
      - name: "id"
        is_nullable: false
        type: "number"
      - name: "sysFrom"
        is_nullable: false
        type: "number"
      - name: "FIRST_NAME"
        is_nullable: false
        type: "string"
      - name: "sysOp"
        is_nullable: false
        type: "number"
      - name: "bucket_id"
        is_nullable: true
        type: "unsigned"
    indexes:
      - type: "TREE"
        name: "id"
        unique: true
        parts:
          - path: "id"
            type: "number"
            is_nullable: false
      - type: "TREE"
        name: "bucket_id"
        unique: false
        parts:
          - path: "bucket_id"
            type: "unsigned"
            is_nullable: true
    sharding_key:
      - id
  hash_testing:
    is_local: false
    temporary: false
    engine: "memtx"
    format:
      - name: "identification_number"
        type: "integer"
        is_nullable: false
      - name: "product_code"
        type: "string"
        is_nullable: false
      - name: "product_units"
        type: "boolean"
        is_nullable: false
      - name: "sys_op"
        type: "number"
        is_nullable: false
      - name: "bucket_id"
        type: "unsigned"
        is_nullable: true
    indexes:
      - name: "id"
        unique: true
        type: "TREE"
        parts:
          - path: "identification_number"
            is_nullable: false
            type: "integer"
      - name: bucket_id
        unique: false
        parts:
          - path: "bucket_id"
            is_nullable: true
            type: "unsigned"
        type: "TREE"
    sharding_key:
      - identification_number
      - product_code
  hash_testing_hist:
    is_local: false
    temporary: false
    engine: "memtx"
    format:
      - name: "identification_number"
        type: "integer"
        is_nullable: false
      - name: "product_code"
        type: "string"
        is_nullable: false
      - name: "product_units"
        type: "boolean"
        is_nullable: false
      - name: "sys_op"
        type: "number"
        is_nullable: false
      - name: "bucket_id"
        type: "unsigned"
        is_nullable: true
    indexes:
      - name: "id"
        unique: true
        type: "TREE"
        parts:
          - path: "identification_number"
            is_nullable: false
            type: "integer"
      - name: bucket_id
        unique: false
        parts:
          - path: "bucket_id"
            is_nullable: true
            type: "unsigned"
        type: "TREE"
    sharding_key:
      - identification_number
      - product_code"#;

#[test]
fn simple_union_query() {
    let query = r#"SELECT * FROM (
    SELECT * FROM "test_space" WHERE "sysFrom" > 0
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "sysFrom" < 0
    ) as "t3"
    WHERE "id" = 1"#;

    let mut metadata = Metadata::new();
    metadata.load(CARTRIDGE_SCHEMA).unwrap();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    // let plan = ast.to_ir(&metadata).unwrap();
    let mut plan = ast.to_ir(&metadata).unwrap();
    plan.add_motions().unwrap();

    let top = plan.get_top().unwrap();
    let expected = HashSet::from([3940]);
    assert_eq!(Some(expected), plan.get_bucket_ids(top, 30000).unwrap())
}

#[test]
fn simple_disjunction_in_union_query() {
    let query = r#"SELECT * FROM (
    SELECT * FROM "test_space" WHERE "sysFrom" > 0
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "sysFrom" < 0
    ) as "t3"
    WHERE ("id" = 1) OR ("id" = 100)"#;

    let mut metadata = Metadata::new();
    metadata.load(CARTRIDGE_SCHEMA).unwrap();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(&metadata).unwrap();
    plan.add_motions().unwrap();

    let top = plan.get_top().unwrap();
    let expected = HashSet::from([3940, 18512]);

    assert_eq!(Some(expected), plan.get_bucket_ids(top, 30000).unwrap())
}

#[test]
fn complex_shard_key_union_query() {
    let query = r#"SELECT *
FROM
    (SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "sys_op" = 1
    UNION ALL
    SELECT "identification_number", "product_code"
    FROM "hash_testing_hist"
    WHERE "sys_op" > 1) AS "t3"
WHERE "identification_number" = 1 AND "product_code" = '222'"#;

    let mut metadata = Metadata::new();
    metadata.load(CARTRIDGE_SCHEMA).unwrap();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(&metadata).unwrap();
    plan.add_motions().unwrap();

    let top = plan.get_top().unwrap();
    // let expected = vec![2927];

    assert_eq!(None, plan.get_bucket_ids(top, 30000).unwrap())
}

#[test]
fn union_complex_cond_query() {
    let query = r#"SELECT *
FROM
    (SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "sys_op" = 1
    UNION ALL
    SELECT "identification_number", "product_code"
    FROM "hash_testing_hist"
    WHERE "sys_op" > 1) AS "t3"
WHERE ("identification_number" = 1
    OR ("identification_number" = 100
    OR "identification_number" = 1000))
    AND ("product_code" = '222'
    OR "product_code" = '111')"#;

    let mut metadata = Metadata::new();
    metadata.load(CARTRIDGE_SCHEMA).unwrap();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(&metadata).unwrap();
    plan.add_motions().unwrap();

    let top = plan.get_top().unwrap();
    // let expected = vec![2927, 22116, 6673, 4203, 23260, 6558];

    assert_eq!(None, plan.get_bucket_ids(top, 30000).unwrap())
}
