use super::*;
use crate::ir::value::Value;

#[test]
fn cast1_test() {
    broadcast_check(
        r#"SELECT CAST('1' as any) FROM "t1""#,
        r#"SELECT CAST (? as any) as "COLUMN_1" FROM "t1""#,
        vec![Value::from("1")],
    );
}

#[test]
fn cast2_test() {
    broadcast_check(
        r#"SELECT CAST(true as bool) FROM "t1""#,
        r#"SELECT CAST (? as bool) as "COLUMN_1" FROM "t1""#,
        vec![Value::from(true)],
    );
}

#[test]
fn cast3_test() {
    broadcast_check(
        r#"SELECT CAST(false as boolean) FROM "t1""#,
        r#"SELECT CAST (? as bool) as "COLUMN_1" FROM "t1""#,
        vec![Value::from(false)],
    );
}

#[test]
fn cast4_test() {
    broadcast_check(
        r#"SELECT CAST('1.0' as decimal) FROM "t1""#,
        r#"SELECT CAST (? as decimal) as "COLUMN_1" FROM "t1""#,
        vec![Value::from("1.0")],
    );
}

#[test]
fn cast5_test() {
    broadcast_check(
        r#"SELECT CAST('1.0' as double) FROM "t1""#,
        r#"SELECT CAST (? as double) as "COLUMN_1" FROM "t1""#,
        vec![Value::from("1.0")],
    );
}

#[test]
fn cast6_test() {
    broadcast_check(
        r#"SELECT CAST('1' as int) FROM "t1""#,
        r#"SELECT CAST (? as int) as "COLUMN_1" FROM "t1""#,
        vec![Value::from("1")],
    );
}

#[test]
fn cast7_test() {
    broadcast_check(
        r#"SELECT CAST('1' as integer) FROM "t1""#,
        r#"SELECT CAST (? as int) as "COLUMN_1" FROM "t1""#,
        vec![Value::from("1")],
    );
}

#[test]
fn cast8_test() {
    broadcast_check(
        r#"SELECT CAST('1' as number) FROM "t1""#,
        r#"SELECT CAST (? as number) as "COLUMN_1" FROM "t1""#,
        vec![Value::from("1")],
    );
}

#[test]
fn cast9_test() {
    broadcast_check(
        r#"SELECT CAST('1' as scalar) FROM "t1""#,
        r#"SELECT CAST (? as scalar) as "COLUMN_1" FROM "t1""#,
        vec![Value::from("1")],
    );
}

#[test]
fn cast10_test() {
    broadcast_check(
        r#"SELECT CAST(1 as string) FROM "t1""#,
        r#"SELECT CAST (? as string) as "COLUMN_1" FROM "t1""#,
        vec![Value::from(1_u64)],
    );
}

#[test]
fn cast11_test() {
    broadcast_check(
        r#"SELECT CAST(1 as text) FROM "t1""#,
        r#"SELECT CAST (? as text) as "COLUMN_1" FROM "t1""#,
        vec![Value::from(1_u64)],
    );
}

#[test]
fn cast12_test() {
    broadcast_check(
        r#"SELECT CAST('1' as unsigned) FROM "t1""#,
        r#"SELECT CAST (? as unsigned) as "COLUMN_1" FROM "t1""#,
        vec![Value::from("1")],
    );
}

#[test]
fn cast13_test() {
    broadcast_check(
        r#"SELECT CAST(1 as varchar(10)) FROM "t1""#,
        r#"SELECT CAST (? as varchar(10)) as "COLUMN_1" FROM "t1""#,
        vec![Value::from(1_u64)],
    );
}

#[test]
fn cast14_test() {
    broadcast_check(
        r#"SELECT CAST(bucket_id("a") as varchar(100)) FROM "t1""#,
        r#"SELECT CAST ("BUCKET_ID" ("t1"."a") as varchar(100)) as "COLUMN_1" FROM "t1""#,
        vec![],
    );
}
