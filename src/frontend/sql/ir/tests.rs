use std::fs;
use std::path::Path;

use crate::ir::Plan;

use super::*;
use crate::executor::engine::mock::MetadataMock;
use pretty_assertions::assert_eq;

#[test]
fn simple_query_to_ir() {
    let query = r#"SELECT "identification_number", "product_code" FROM "hash_testing" WHERE "identification_number" = 1"#;

    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("ir")
        .join("simple_query.yaml");
    let yml_str = fs::read_to_string(path).unwrap();
    let expected = Plan::from_yaml(&yml_str).unwrap();

    let metadata = &MetadataMock::new();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let actual = ast.to_ir(metadata).unwrap();

    assert_eq!(expected, actual);
}

#[test]
fn complex_cond_query_transform() {
    let query = r#"SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "identification_number" = 1 AND "product_code" = '1'
    OR "identification_number" = 2 AND "product_code" = '2'"#;

    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("ir")
        .join("complex_cond_query.yaml");
    let yml_str = fs::read_to_string(path).unwrap();
    let expected = Plan::from_yaml(&yml_str).unwrap();

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let actual = ast.to_ir(metadata).unwrap();

    assert_eq!(expected, actual);
}

#[test]
fn simple_union_query_transform() {
    let query = r#"SELECT *
FROM
    (SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "sys_op" = 1
    UNION ALL
    SELECT "identification_number", "product_code"
    FROM "hash_testing_hist"
    WHERE "sys_op" > 1) AS "t3"
WHERE "identification_number" = 1"#;

    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("ir")
        .join("simple_union_query.yaml");
    let yml_str = fs::read_to_string(path).unwrap();
    let expected = Plan::from_yaml(&yml_str).unwrap();

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let actual = ast.to_ir(metadata).unwrap();

    assert_eq!(expected, actual);
}

#[test]
fn union_complex_cond_query_transform() {
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
    OR ("identification_number" = 2
    OR "identification_number" = 3))
    AND ("product_code" = '1'
    OR "product_code" = '2')"#;

    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("ir")
        .join("complex_union_cond_query.yaml");
    let yml_str = fs::read_to_string(path).unwrap();
    let expected = Plan::from_yaml(&yml_str).unwrap();

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let actual = ast.to_ir(metadata).unwrap();

    assert_eq!(expected, actual);
}
