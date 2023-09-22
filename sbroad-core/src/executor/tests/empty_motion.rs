use pretty_assertions::assert_eq;

use crate::backend::sql::ir::PatternWithParams;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::ir::relation::{Column, ColumnRole, Type};
use crate::ir::transformation::redistribution::MotionPolicy;
use crate::ir::value::{LuaValue, Value};

use super::*;

#[test]
fn empty_motion1_test() {
    let sql = r#"SELECT * FROM (
        SELECT "t"."a", "t"."b" FROM "t" INNER JOIN "t2" ON "t"."a" = "t2"."g" and "t"."b" = "t2"."h"
        WHERE "t"."a" = 0
        EXCEPT
        SELECT "t"."a", "t"."b" FROM "t" INNER JOIN "t2" ON "t"."a" = "t2"."g" and "t"."b" = "t2"."h"
        WHERE "t"."a" = 1
    ) as q"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion1_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_t1 = t2_empty();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion1_id)
    {
        virtual_t1.reshard(key, &query.coordinator).unwrap();
    }
    query.coordinator.add_virtual_table(motion1_id, virtual_t1);
    let motion2_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(1)
        .unwrap();
    let mut virtual_t2 = t2_empty();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion2_id)
    {
        virtual_t2.reshard(key, &query.coordinator).unwrap();
    }
    query.coordinator.add_virtual_table(motion2_id, virtual_t2);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();
    expected.rows.extend(vec![vec![
        LuaValue::String("Execute query on all buckets".into()),
        LuaValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {} {} {} {} {} {} {} {} {} {} {} {}",
                r#"SELECT "Q"."a", "Q"."b" FROM"#,
                r#"(SELECT "t"."a", "t"."b" FROM"#,
                r#"(SELECT "t"."a", "t"."b", "t"."c", "t"."d" FROM "t") as "t""#,
                r#"INNER JOIN"#,
                r#"(SELECT "g","h" FROM "TMP_test_334") as "t2""#,
                r#"ON ("t"."a") = ("t2"."g") and ("t"."b") = ("t2"."h")"#,
                r#"WHERE ("t"."a") = (?)"#,
                r#"EXCEPT"#,
                r#"SELECT "t"."a", "t"."b" FROM"#,
                r#"(SELECT "t"."a", "t"."b", "t"."c", "t"."d" FROM "t") as "t""#,
                r#"INNER JOIN"#,
                r#"(SELECT "g","h" FROM "TMP_test_344") as "t2""#,
                r#"ON ("t"."a") = ("t2"."g") and ("t"."b") = ("t2"."h")"#,
                r#"WHERE ("t"."a") = (?)) as "Q""#,
            ),
            vec![Value::from(0_u64), Value::from(1_u64)],
        ))),
    ]]);

    assert_eq!(expected, result);
}

fn t2_empty() -> VirtualTable {
    let mut virtual_table = VirtualTable::new();

    virtual_table.add_column(Column {
        name: "g".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });

    virtual_table.add_column(Column {
        name: "h".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });

    virtual_table.set_alias("\"t2\"").unwrap();

    virtual_table
}
