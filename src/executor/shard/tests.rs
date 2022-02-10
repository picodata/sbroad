use crate::executor::engine::mock::MetadataMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use pretty_assertions::assert_eq;
use std::collections::HashSet;

#[test]
fn simple_union_query() {
    let query = r#"SELECT * FROM (
    SELECT * FROM "test_space" WHERE "sysFrom" > 0
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "sysFrom" < 0
    ) as "t3"
    WHERE "id" = 1"#;

    let metadata = &MetadataMock::new();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    // let plan = ast.to_ir(&metadata).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.add_motions().unwrap();

    let top = plan.get_top().unwrap();
    // let expected = HashSet::from([3940]);
    let expected = HashSet::from(["1".into()]);
    assert_eq!(Some(expected), plan.get_sharding_keys(top).unwrap())
}

#[test]
fn simple_disjunction_in_union_query() {
    let query = r#"SELECT * FROM (
    SELECT * FROM "test_space" WHERE "sysFrom" > 0
    UNION ALL
    SELECT * FROM "test_space_hist" WHERE "sysFrom" < 0
    ) as "t3"
    WHERE ("id" = 1) OR ("id" = 100)"#;

    let metadata = &MetadataMock::new();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.add_motions().unwrap();

    let top = plan.get_top().unwrap();
    // let expected = HashSet::from([3940, 18512]);

    let expected = HashSet::from(["1".into(), "100".into()]);
    assert_eq!(Some(expected), plan.get_sharding_keys(top).unwrap())
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

    let metadata = &MetadataMock::new();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.add_motions().unwrap();

    let top = plan.get_top().unwrap();
    // let expected = vec![2927];

    assert_eq!(None, plan.get_sharding_keys(top).unwrap())
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

    let metadata = &MetadataMock::new();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.add_motions().unwrap();

    let top = plan.get_top().unwrap();
    // let expected = vec![2927, 22116, 6673, 4203, 23260, 6558];

    assert_eq!(None, plan.get_sharding_keys(top).unwrap())
}
