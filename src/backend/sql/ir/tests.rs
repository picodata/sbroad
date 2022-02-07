use crate::cache::Metadata;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use pretty_assertions::assert_eq;

const CARTRIDGE_SCHEMA: &str = r#"spaces:
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
fn one_table_projection() {
    let query = r#"SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "identification_number" = 1"#;

    let mut metadata = Metadata::new();
    metadata.load(CARTRIDGE_SCHEMA).unwrap();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(&metadata).unwrap();
    plan.build_relational_map();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("identification_number") = 1"#
        ),
        sql
    );
}

#[test]
fn one_table_with_asterisk() {
    let query = r#"SELECT *
    FROM "hash_testing"
    WHERE "identification_number" = 1"#;

    let mut metadata = Metadata::new();
    metadata.load(CARTRIDGE_SCHEMA).unwrap();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(&metadata).unwrap();
    plan.build_relational_map();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code","#,
            r#""product_units" as "product_units", "sys_op" as "sys_op", "bucket_id" as "bucket_id""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("identification_number") = 1"#
        ),
        sql
    );
}

#[test]
fn union_all() {
    let query = r#"SELECT product_code
    FROM "hash_testing"
    WHERE "identification_number" = 1
    UNION ALL
    SELECT product_code
    FROM "hash_testing_hist"
    WHERE "product_code" = 'a' 
    "#;

    let mut metadata = Metadata::new();
    metadata.load(CARTRIDGE_SCHEMA).unwrap();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(&metadata).unwrap();
    plan.build_relational_map();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "product_code" as "product_code" FROM "hash_testing" WHERE ("identification_number") = 1"#,
            r#"UNION ALL"#,
            r#"SELECT "product_code" as "product_code" FROM "hash_testing_hist" WHERE ("product_code") = 'a'"#
        ),
        sql
    );
}

#[test]
fn from_sub_query() {
    let query = r#"SELECT product_code
    FROM (SELECT product_code
    FROM "hash_testing"
    WHERE "identification_number" = 1) as t1
    WHERE "product_code" = 'a'"#;

    let mut metadata = Metadata::new();
    metadata.load(CARTRIDGE_SCHEMA).unwrap();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(&metadata).unwrap();
    plan.build_relational_map();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "product_code" as "product_code" FROM"#,
            r#"(SELECT "product_code" as "product_code" FROM "hash_testing" WHERE ("identification_number") = 1) as "t1""#,
            r#"WHERE ("product_code") = 'a'"#
        ),
        sql
    );
}

#[test]
fn from_sub_query_with_union() {
    let query = r#"SELECT product_code
  FROM (SELECT product_code
    FROM "hash_testing"
    WHERE "identification_number" = 1
    UNION ALL
    SELECT product_code
    FROM "hash_testing_hist"
    WHERE "product_code" = 'a') as "t1"
  WHERE "product_code" = 'a'"#;

    let mut metadata = Metadata::new();
    metadata.load(CARTRIDGE_SCHEMA).unwrap();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(&metadata).unwrap();
    plan.build_relational_map();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "product_code" as "product_code" FROM"#,
            r#"(SELECT "product_code" as "product_code" FROM "hash_testing" WHERE ("identification_number") = 1"#,
            r#"UNION ALL"#,
            r#"SELECT "product_code" as "product_code" FROM "hash_testing_hist" WHERE ("product_code") = 'a') as "t1""#,
            r#"WHERE ("product_code") = 'a'"#,
        ),
        sql
    );
}
