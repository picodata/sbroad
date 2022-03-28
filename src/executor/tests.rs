use pretty_assertions::assert_eq;

use crate::executor::engine::mock::EngineMock;
use crate::executor::result::Value;

use super::*;

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
                r#"WHERE ("hash_testing"."identification_number", "hash_testing"."product_code") = (1, '457')"#,
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
                r#"WHERE ("test_space"."id") in (SELECT COLUMN_2 as "identification_number" FROM (VALUES (2),(3)))"#,
            )),
        ],
        vec![
            Value::String(String::from("query send to [3] shard")),
            Value::String(format!(
                "{} {} {}",
                r#"SELECT "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                r#"FROM "test_space""#,
                r#"WHERE ("test_space"."id") in (SELECT COLUMN_2 as "identification_number" FROM (VALUES (2),(3)))"#,
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
                    r#"WHERE ("t1"."id") in (SELECT COLUMN_2 as "identification_number" FROM (VALUES (2),(3)))"#
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
                    r#"WHERE ("t1"."id") in (SELECT COLUMN_2 as "identification_number" FROM (VALUES (2),(3)))"#
                )
            )
        ],
    ]);

    assert_eq!(expected, result)
}

#[test]
fn join_linker_test() {
    let sql = r#"SELECT *
FROM
    (SELECT "id", "FIRST_NAME"
    FROM "test_space"
    WHERE "sys_op" < 0
            AND "sysFrom" >= 0
    UNION ALL
    SELECT "id", "FIRST_NAME"
    FROM "test_space_hist"
    WHERE "sysFrom" <= 0) AS "t3"
INNER JOIN
    (SELECT "identification_number"
    FROM "hash_testing_hist"
    WHERE "sys_op" > 0
    UNION ALL
    SELECT "identification_number"
    FROM "hash_single_testing_hist"
    WHERE "sys_op" <= 0) AS "t8"
    ON "t3"."id" = "t8"."identification_number"
WHERE "t3"."id" = 1 AND "t8"."identification_number" = 1"#;

    let engine = EngineMock::new();

    let mut query = Query::new(engine, sql).unwrap();
    query.optimize().unwrap();

    let result = query.exec().unwrap();

    let mut expected = BoxExecuteFormat::new();

    expected.rows.extend(vec![
        vec![
            Value::String(String::from("query send to [1] shard")),
            Value::String(
                format!(
                    "{}, {}, {} {}{} {} {} {} {} {} {}{} {} {}{} {} {}",
                    r#"SELECT "t3"."id" as "id""#,
                    r#""t3"."FIRST_NAME" as "FIRST_NAME""#,
                    r#""t8"."identification_number" as "identification_number""#,
                    r#"FROM ("#,
                    r#"SELECT "test_space"."id" as "id", "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space""#,
                    r#"WHERE ("test_space"."sys_op") < (0) and ("test_space"."sysFrom") >= (0)"#,
                    r#"UNION ALL"#,
                    r#"SELECT "test_space_hist"."id" as "id", "test_space_hist"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space_hist""#,
                    r#"WHERE ("test_space_hist"."sysFrom") <= (0)"#,
                    r#") as "t3""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT COLUMN_2 as "identification_number" FROM (VALUES (2),(3))"#,
                    r#") as "t8""#,
                    r#"ON ("t3"."id") = ("t8"."identification_number")"#,
                    r#"WHERE ("t3"."id") = (1) and ("t8"."identification_number") = (1)"#
                )
            )
        ],
        vec![
            Value::String(String::from("query send to [2] shard")),
            Value::String(
                format!(
                    "{}, {}, {} {}{} {} {} {} {} {} {}{} {} {}{} {} {}",
                    r#"SELECT "t3"."id" as "id""#,
                    r#""t3"."FIRST_NAME" as "FIRST_NAME""#,
                    r#""t8"."identification_number" as "identification_number""#,
                    r#"FROM ("#,
                    r#"SELECT "test_space"."id" as "id", "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space""#,
                    r#"WHERE ("test_space"."sys_op") < (0) and ("test_space"."sysFrom") >= (0)"#,
                    r#"UNION ALL"#,
                    r#"SELECT "test_space_hist"."id" as "id", "test_space_hist"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space_hist""#,
                    r#"WHERE ("test_space_hist"."sysFrom") <= (0)"#,
                    r#") as "t3""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT COLUMN_2 as "identification_number" FROM (VALUES (2),(3))"#,
                    r#") as "t8""#,
                    r#"ON ("t3"."id") = ("t8"."identification_number")"#,
                    r#"WHERE ("t3"."id") = (1) and ("t8"."identification_number") = (1)"#
                )
            )
        ],
        vec![
            Value::String(String::from("query send to [3] shard")),
            Value::String(
                format!(
                    "{}, {}, {} {}{} {} {} {} {} {} {}{} {} {}{} {} {}",
                    r#"SELECT "t3"."id" as "id""#,
                    r#""t3"."FIRST_NAME" as "FIRST_NAME""#,
                    r#""t8"."identification_number" as "identification_number""#,
                    r#"FROM ("#,
                    r#"SELECT "test_space"."id" as "id", "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space""#,
                    r#"WHERE ("test_space"."sys_op") < (0) and ("test_space"."sysFrom") >= (0)"#,
                    r#"UNION ALL"#,
                    r#"SELECT "test_space_hist"."id" as "id", "test_space_hist"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space_hist""#,
                    r#"WHERE ("test_space_hist"."sysFrom") <= (0)"#,
                    r#") as "t3""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT COLUMN_2 as "identification_number" FROM (VALUES (2),(3))"#,
                    r#") as "t8""#,
                    r#"ON ("t3"."id") = ("t8"."identification_number")"#,
                    r#"WHERE ("t3"."id") = (1) and ("t8"."identification_number") = (1)"#
                )
            )
        ],
    ]);
    assert_eq!(expected, result)
}

// select * from "test_1" where "identification_number" in (select COLUMN_2 as "b" from (values (1), (2))) or "identification_number" in (select COLUMN_2 as "c" from (values (3), (4)));
#[test]
fn anonymous_col_index_test() {
    let sql = r#"SELECT * FROM "test_space"
    WHERE "id" in (SELECT "identification_number" FROM "hash_testing" WHERE "product_units" < 3)
        OR "id" in (SELECT "identification_number" FROM "hash_testing" WHERE "product_units" > 5)"#;

    let engine = EngineMock::new();

    let mut query = Query::new(engine, sql).unwrap();
    query.optimize().unwrap();

    let result = query.exec().unwrap();

    let mut expected = BoxExecuteFormat::new();
    expected.rows.extend(vec![
        vec![
            Value::String(String::from("query send to [2] shard")),
            Value::String(format!(
                "{} {}, {}, {}, {}, {} {} {} {} {} {}",
                "SELECT",
                r#""test_space"."id" as "id""#,
                r#""test_space"."sysFrom" as "sysFrom""#,
                r#""test_space"."FIRST_NAME" as "FIRST_NAME""#,
                r#""test_space"."sys_op" as "sys_op""#,
                r#""test_space"."bucket_id" as "bucket_id""#,
                r#"FROM "test_space""#,
                r#"WHERE (("test_space"."id") in"#,
                r#"(SELECT COLUMN_2 as "identification_number" FROM (VALUES (2),(3)))"#,
                r#"or ("test_space"."id") in"#,
                r#"(SELECT COLUMN_4 as "identification_number" FROM (VALUES (2),(3))))"#,
            )),
        ],
        vec![
            Value::String(String::from("query send to [3] shard")),
            Value::String(format!(
                "{} {}, {}, {}, {}, {} {} {} {} {} {}",
                "SELECT",
                r#""test_space"."id" as "id""#,
                r#""test_space"."sysFrom" as "sysFrom""#,
                r#""test_space"."FIRST_NAME" as "FIRST_NAME""#,
                r#""test_space"."sys_op" as "sys_op""#,
                r#""test_space"."bucket_id" as "bucket_id""#,
                r#"FROM "test_space""#,
                r#"WHERE (("test_space"."id") in"#,
                r#"(SELECT COLUMN_2 as "identification_number" FROM (VALUES (2),(3)))"#,
                r#"or ("test_space"."id") in"#,
                r#"(SELECT COLUMN_4 as "identification_number" FROM (VALUES (2),(3))))"#,
            )),
        ],
    ]);

    assert_eq!(expected, result)
}
