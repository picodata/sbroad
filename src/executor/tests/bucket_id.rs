use pretty_assertions::assert_eq;

use crate::executor::engine::cartridge::backend::sql::ir::PatternWithParams;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::result::ProducerResult;
use crate::ir::value::Value;

use super::*;

#[test]
fn bucket1_test() {
    let sql = r#"SELECT *, "bucket_id" FROM "t1""#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, &mut vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    expected.rows.push(vec![
        Value::String(format!("Execute query on all buckets")),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{}",
                r#"SELECT "t1"."a", "t1"."b", "t1"."bucket_id" FROM "t1""#,
            ),
            vec![],
        ))),
    ]);
    assert_eq!(expected, result);
}

#[test]
fn bucket2_test() {
    let sql = r#"SELECT "a", "bucket_id", "b" FROM "t1"
        WHERE "a" = 1 AND "b" = 2"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, &mut vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();
    let param1 = Value::from(1_u64);
    let param2 = Value::from(2_u64);
    let bucket = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    expected.rows.push(vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{} {}",
                r#"SELECT "t1"."a", "t1"."bucket_id", "t1"."b" FROM "t1""#,
                r#"WHERE ("t1"."a", "t1"."b") = (?, ?)"#,
            ),
            vec![param1, param2],
        ))),
    ]);
    assert_eq!(expected, result);
}

#[test]
fn sharding_keys_from_tuple1() {
    let coordinator = RouterRuntimeMock::new();
    let tuple = vec![Value::from("123"), Value::from(1_u64)];
    let sharding_keys = coordinator
        .extract_sharding_keys_from_tuple("t1".into(), &tuple)
        .unwrap();
    assert_eq!(
        sharding_keys,
        vec![&Value::from("123"), &Value::from(1_u64)]
    );
}