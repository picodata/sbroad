use super::*;
use crate::ir::value::Value;

#[test]
fn concat1_test() {
    broadcast_check(
        r#"SELECT CAST('1' as string) || 'hello' FROM "t1""#,
        r#"SELECT (CAST (? as string)) || (?) as "COL_1" FROM "t1""#,
        vec![Value::from("1"), Value::from("hello")],
    );
}

#[test]
fn concat2_test() {
    broadcast_check(
        r#"SELECT bucket_id('hello') || CAST(42 as string) FROM "t1""#,
        r#"SELECT ("BUCKET_ID" (?)) || (CAST (? as string)) as "COL_1" FROM "t1""#,
        vec![Value::from("hello"), Value::from(42_u64)],
    );
}

#[test]
fn concat3_test() {
    broadcast_check(
        r#"SELECT 'a' || 'b' FROM "t1""#,
        r#"SELECT (?) || (?) as "COL_1" FROM "t1""#,
        vec![Value::from("a"), Value::from("b")],
    );
}

#[test]
fn concat4_test() {
    broadcast_check(
        r#"SELECT "a" FROM "t1" WHERE "a" || 'a' = CAST(42 as string) || BUCKET_ID('b') || 'a'"#,
        r#"SELECT "t1"."a" FROM "t1" WHERE (("t1"."a") || (?)) = ((CAST (? as string)) || (("BUCKET_ID" (?)) || (?)))"#,
        vec![
            Value::from("a"),
            Value::from(42_u64),
            Value::from("b"),
            Value::from("a"),
        ],
    );
}
