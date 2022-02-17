use super::*;
use crate::executor::engine::mock::EngineMock;
use crate::executor::result::Value;
use pretty_assertions::assert_eq;

#[test]
fn shard_query() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" = 1"#;
    let engine = EngineMock::new();

    let mut query = Query::new(engine, sql).unwrap();
    query.optimize().unwrap();

    let mut expected = BoxExecuteFormat::new();
    expected
        .rows
        .push(vec![
            Value::String(String::from("query send to [1] shard")),
            Value::String(String::from(r#"SELECT "test_space"."FIRST_NAME" as "FIRST_NAME" FROM "test_space" WHERE ("test_space"."id") = (1)"#))
        ]);
    assert_eq!(expected, query.exec().unwrap())
}

#[test]
fn shard_union_query() {
    let sql = r#"SELECT *
    FROM
        (SELECT "id"
        FROM "test_space"
        WHERE "sys_op" = 1
        UNION ALL
        SELECT "id"
        FROM "test_space"
        WHERE "sys_op" > 1) AS "t3"
    WHERE "id" = 1"#;

    let engine = EngineMock::new();

    let mut query = Query::new(engine, sql).unwrap();
    query.optimize().unwrap();

    let mut expected = BoxExecuteFormat::new();
    expected
        .rows
        .push(vec![
            Value::String(String::from("query send to [1] shard")),
            Value::String(
                format!(
                    "{} {}{} {} {}{} {}",
                    r#"SELECT "t3"."id" as "id""#,
                    r#"FROM ("#,
                    r#"SELECT "test_space"."id" as "id" FROM "test_space" WHERE ("test_space"."sys_op") = (1)"#,
                    r#"UNION ALL"#,
                    r#"SELECT "test_space"."id" as "id" FROM "test_space" WHERE ("test_space"."sys_op") > (1)"#,
                    r#") as "t3""#,
                    r#"WHERE ("t3"."id") = (1)"#,
                )
            )
        ]);

    assert_eq!(expected, query.exec().unwrap())
}

#[test]
fn map_reduce_query() {
    let sql = r#"SELECT "product_code" FROM "hash_testing" where "identification_number" = 1 and "product_code" = '457'"#;
    let engine = EngineMock::new();

    let mut query = Query::new(engine, sql).unwrap();
    query.optimize().unwrap();

    let mut expected = BoxExecuteFormat::new();
    expected.rows.push(vec![
        Value::String(String::from("query send to all shards")),
        Value::String(
            format!(
                "{} {} {}",
                r#"SELECT "hash_testing"."product_code" as "product_code""#,
                r#"FROM "hash_testing""#,
                r#"WHERE ("hash_testing"."identification_number") = (1) and ("hash_testing"."product_code") = ('457')"#,
            )
        )
    ]);

    assert_eq!(expected, query.exec().unwrap())
}

#[test]
fn linker_test() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" in
    (SELECT "identification_number" FROM "hash_testing" where "identification_number" > 1)"#;
    let engine = EngineMock::new();

    let mut query = Query::new(engine, sql).unwrap();
    query.optimize().unwrap();

    let result = query.exec().unwrap();

    let mut expected = BoxExecuteFormat::new();
    expected.rows.extend(vec![
        vec![
            Value::String(String::from("query send to [2] shard")),
            Value::String(format!(
                "{} {} {}",
                r#"SELECT "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                r#"FROM "test_space""#,
                r#"WHERE ("test_space"."id") in (VALUES (2),(3))"#,
            )),
        ],
        vec![
            Value::String(String::from("query send to [3] shard")),
            Value::String(format!(
                "{} {} {}",
                r#"SELECT "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                r#"FROM "test_space""#,
                r#"WHERE ("test_space"."id") in (VALUES (2),(3))"#,
            )),
        ],
    ]);

    assert_eq!(expected, result)
}

#[test]
fn union_linker_test() {
    let sql = r#"SELECT * FROM (
            SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" < 0
            UNION ALL
            SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" > 0
        ) as "t1"
        WHERE "id" in (SELECT "identification_number" FROM (
            SELECT "product_code", "identification_number" FROM "hash_testing" WHERE "product_units" < 3
            UNION ALL
            SELECT "product_code", "identification_number" FROM "hash_testing_hist" WHERE "product_units" > 3
        ) as "t2"
        WHERE "product_code" = '123')"#;

    let engine = EngineMock::new();

    let mut query = Query::new(engine, sql).unwrap();
    query.optimize().unwrap();

    let result = query.exec().unwrap();

    let mut expected = BoxExecuteFormat::new();
    expected.rows.extend(vec![
        vec![
            Value::String(String::from("query send to [2] shard")),
            Value::String(
                format!(
                    "{} {}{} {} {} {} {} {} {}{} {}",
                    r#"SELECT "t1"."id" as "id", "t1"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM ("#,
                    r#"SELECT "test_space"."id" as "id", "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space""#,
                    r#"WHERE ("test_space"."sys_op") < (0)"#,
                    r#"UNION ALL"#,
                    r#"SELECT "test_space_hist"."id" as "id", "test_space_hist"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space_hist""#,
                    r#"WHERE ("test_space_hist"."sys_op") > (0)"#,
                    r#") as "t1""#,
                    r#"WHERE ("t1"."id") in (VALUES (2),(3))"#
                )
            )
        ],
        vec![
            Value::String(String::from("query send to [3] shard")),
            Value::String(
                format!(
                    "{} {}{} {} {} {} {} {} {}{} {}",
                    r#"SELECT "t1"."id" as "id", "t1"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM ("#,
                    r#"SELECT "test_space"."id" as "id", "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space""#,
                    r#"WHERE ("test_space"."sys_op") < (0)"#,
                    r#"UNION ALL"#,
                    r#"SELECT "test_space_hist"."id" as "id", "test_space_hist"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space_hist""#,
                    r#"WHERE ("test_space_hist"."sys_op") > (0)"#,
                    r#") as "t1""#,
                    r#"WHERE ("t1"."id") in (VALUES (2),(3))"#
                )
            )
        ],
    ]);

    assert_eq!(expected, result)
}
