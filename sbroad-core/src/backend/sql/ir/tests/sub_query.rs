use super::*;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;

#[test]
fn sub_query1_latest() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1) as t1
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {}",
            r#"SELECT "T1"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)) as "T1""#,
            r#"WHERE ("T1"."product_code") = (?)"#
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, expected, Snapshot::Latest);
}

#[test]
fn sub_query1_oldest() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1) as t1
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {}",
            r#"SELECT "T1"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)) as "T1""#,
            r#"WHERE ("T1"."product_code") = (?)"#
        ),
        vec![Value::from(1_u64), Value::from("a")],
    );
    check_sql_with_snapshot(query, expected, Snapshot::Oldest);
}

#[test]
fn sub_query2_latest() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
            FROM "hash_testing"
            WHERE "identification_number" = 1
            UNION ALL
            SELECT "product_code"
            FROM "hash_testing_hist"
            WHERE "product_code" = 'a') as "t1"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {}",
            r#"SELECT "t1"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."product_code" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = (?)) as "t1""#,
            r#"WHERE ("t1"."product_code") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from("a"), Value::from("a")],
    );
    check_sql_with_snapshot(query, expected, Snapshot::Latest);
}

#[test]
fn sub_query2_oldest() {
    let query = r#"SELECT "product_code"
        FROM (SELECT "product_code"
            FROM "hash_testing"
            WHERE "identification_number" = 1
            UNION ALL
            SELECT "product_code"
            FROM "hash_testing_hist"
            WHERE "product_code" = 'a') as "t1"
        WHERE "product_code" = 'a'"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {}",
            r#"SELECT "t1"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."product_code" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = (?)) as "t1""#,
            r#"WHERE ("t1"."product_code") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from("a"), Value::from("a")],
    );
    check_sql_with_snapshot(query, expected, Snapshot::Oldest);
}
