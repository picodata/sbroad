use pretty_assertions::assert_eq;

use crate::executor::engine::cartridge::backend::sql::ir::PatternWithParams;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::ir::relation::{Column, ColumnRole, Type};
use crate::ir::transformation::redistribution::tests::get_motion_id;
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::value::Value;

use super::*;

#[test]
fn not_eq1_test() {
    let sql = r#"
        SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE "identification_number" <> 1 and "product_code" <> 2
        "#;

    // Initialize the query.
    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();

    // Validate the motion type.
    assert_eq!(true, plan.clone_slices().is_none());

    // Execute the query.
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    // Validate the result.
    let mut expected = ProducerResult::new();
    expected.rows.extend(vec![vec![
        Value::String("Execute query on all buckets".to_string()),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{} {}",
                r#"SELECT "t"."identification_number" FROM "hash_testing" as "t""#,
                r#"WHERE ("t"."product_code") <> (?) and ("t"."identification_number") <> (?)"#,
            ),
            vec![Value::from(2_u64), Value::from(1_u64)],
        ))),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn not_eq2_test() {
    let sql = r#"
        SELECT "identification_number" FROM "hash_testing" AS "t"
        WHERE "identification_number" != (
            SELECT "identification_number" as "id" FROM "hash_testing_hist"
            WHERE "identification_number" = 3
        )
        "#;

    // Initialize the query.
    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let plan = query.exec_plan.get_ir_plan();

    // Validate the motion type.
    let motion_id = *get_motion_id(plan, 0, 0).unwrap();
    assert_eq!(&MotionPolicy::Full, get_motion_policy(plan, motion_id));
    assert_eq!(true, get_motion_id(plan, 0, 1).is_none());

    // Mock a virtual table.
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "id".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(3_u64)]);
    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    // Execute the query.
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    // Validate the result.
    let mut expected = ProducerResult::new();
    expected.rows.extend(vec![vec![
        Value::String("Execute query on all buckets".to_string()),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{} {}",
                r#"SELECT "t"."identification_number" FROM "hash_testing" as "t""#,
                r#"WHERE ("t"."identification_number") <> (SELECT COLUMN_1 as "id" FROM (VALUES (?)))"#,
            ),
            vec![
                Value::from(3_u64),
            ],
        ))),
    ]]);
    assert_eq!(expected, result);
}