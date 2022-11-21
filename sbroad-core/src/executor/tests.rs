use pretty_assertions::assert_eq;

use crate::backend::sql::ir::PatternWithParams;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::ir::operator::Relational;
use crate::ir::relation::{Column, ColumnRole, Type};
use crate::ir::transformation::redistribution::{DataGeneration, MotionPolicy};
use crate::ir::value::Value;

use super::*;

#[test]
fn shard_query() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" = 1"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let bucket = coordinator.determine_bucket_id(&[&param1]);
    expected.rows.push(vec![
        Value::from(format!("Execute query on a bucket [{}]", bucket)),
        Value::from(String::from(PatternWithParams::new(
            format!(
                "{} {}",
                r#"SELECT "test_space"."FIRST_NAME" FROM "test_space""#,
                r#"WHERE ("test_space"."id") = (?)"#
            ),
            vec![param1],
        ))),
    ]);
    assert_eq!(expected, result);
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

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();
    let param1 = Value::from(1_u64);
    let bucket = query.coordinator.determine_bucket_id(&[&param1]);
    expected.rows.push(vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{} {}{} {} {}{} {}",
                r#"SELECT "t3"."id""#,
                r#"FROM ("#,
                r#"SELECT "test_space"."id" FROM "test_space" WHERE ("test_space"."sys_op") = (?)"#,
                r#"UNION ALL"#,
                r#"SELECT "test_space"."id" FROM "test_space" WHERE ("test_space"."sys_op") > (?)"#,
                r#") as "t3""#,
                r#"WHERE ("t3"."id") = (?)"#,
            ),
            vec![Value::from(1_u64), Value::from(1_u64), Value::from(1_u64)],
        ))),
    ]);

    assert_eq!(expected, result);
}

#[test]
fn map_reduce_query() {
    let sql = r#"SELECT "product_code" FROM "hash_testing" where "identification_number" = 1 and "product_code" = '457'"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let param457 = Value::from("457");

    let bucket = query.coordinator.determine_bucket_id(&[&param1, &param457]);

    expected.rows.push(vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(
            String::from(
                PatternWithParams::new(
                    format!(
                        "{} {} {}",
                        r#"SELECT "hash_testing"."product_code""#,
                        r#"FROM "hash_testing""#,
                        r#"WHERE ("hash_testing"."identification_number") = (?) and ("hash_testing"."product_code") = (?)"#,
                    ), vec![param1, param457],
                )
            )
        )
    ]);

    assert_eq!(expected, result);
}

#[test]
fn linker_test() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" in
    (SELECT "identification_number" FROM "hash_testing" where "identification_number" > 1)"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();
    let mut virtual_table = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param2 = Value::from(2_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]);

    let param3 = Value::from(3_u64);
    let bucket3 = query.coordinator.determine_bucket_id(&[&param3]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket3)),
            Value::String(
                String::from(
                    PatternWithParams::new(
                        format!(
                        "{} {} {}",
                        r#"SELECT "test_space"."FIRST_NAME""#,
                        r#"FROM "test_space""#,
                        r#"WHERE ("test_space"."id") in (SELECT COLUMN_1 as "identification_number" FROM (VALUES (?)))"#,
                        ), vec![param3],
                    )
                )
            ),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(
                String::from(
                    PatternWithParams::new(
                        format!(
                        "{} {} {}",
                        r#"SELECT "test_space"."FIRST_NAME""#,
                        r#"FROM "test_space""#,
                        r#"WHERE ("test_space"."id") in (SELECT COLUMN_1 as "identification_number" FROM (VALUES (?)))"#,
                        ), vec![param2],
                    )
                )
            ),
        ],
    ]);

    assert_eq!(expected, result);
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

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();
    let mut virtual_table = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param2 = Value::from(2_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]);

    let param3 = Value::from(3_u64);
    let bucket3 = query.coordinator.determine_bucket_id(&[&param3]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket3)),
            Value::String(
                String::from(PatternWithParams::new(
                format!(
                    "{} {}{} {} {} {} {} {} {}{} {}",
                    r#"SELECT "t1"."id", "t1"."FIRST_NAME""#,
                    r#"FROM ("#,
                    r#"SELECT "test_space"."id", "test_space"."FIRST_NAME""#,
                    r#"FROM "test_space""#,
                    r#"WHERE ("test_space"."sys_op") < (?)"#,
                    r#"UNION ALL"#,
                    r#"SELECT "test_space_hist"."id", "test_space_hist"."FIRST_NAME""#,
                    r#"FROM "test_space_hist""#,
                    r#"WHERE ("test_space_hist"."sys_op") > (?)"#,
                    r#") as "t1""#,
                    r#"WHERE ("t1"."id") in (SELECT COLUMN_1 as "identification_number" FROM (VALUES (?)))"#,
                ), vec![Value::from(0_u64), Value::from(0_u64), Value::from(3_u64)])
            ))
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(
                String::from(PatternWithParams::new(
                format!(
                    "{} {}{} {} {} {} {} {} {}{} {}",
                    r#"SELECT "t1"."id", "t1"."FIRST_NAME""#,
                    r#"FROM ("#,
                    r#"SELECT "test_space"."id", "test_space"."FIRST_NAME""#,
                    r#"FROM "test_space""#,
                    r#"WHERE ("test_space"."sys_op") < (?)"#,
                    r#"UNION ALL"#,
                    r#"SELECT "test_space_hist"."id", "test_space_hist"."FIRST_NAME""#,
                    r#"FROM "test_space_hist""#,
                    r#"WHERE ("test_space_hist"."sys_op") > (?)"#,
                    r#") as "t1""#,
                    r#"WHERE ("t1"."id") in (SELECT COLUMN_1 as "identification_number" FROM (VALUES (?)))"#,
                ), vec![Value::from(0_u64), Value::from(0_u64), Value::from(2_u64)]))
            )
        ],
    ]);

    assert_eq!(expected, result);
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
WHERE "t3"."id" = 2 AND "t8"."identification_number" = 2"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();
    let mut virtual_table = virtual_table_23();
    virtual_table.set_alias("\"t8\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param2 = Value::from(2_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket2)),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{}, {}, {} {}{} {} {} {} {} {} {}{} {} {}{} {} {}",
                r#"SELECT "t3"."id""#,
                r#""t3"."FIRST_NAME""#,
                r#""t8"."identification_number""#,
                r#"FROM ("#,
                r#"SELECT "test_space"."id", "test_space"."FIRST_NAME""#,
                r#"FROM "test_space""#,
                r#"WHERE ("test_space"."sys_op") < (?) and ("test_space"."sysFrom") >= (?)"#,
                r#"UNION ALL"#,
                r#"SELECT "test_space_hist"."id", "test_space_hist"."FIRST_NAME""#,
                r#"FROM "test_space_hist""#,
                r#"WHERE ("test_space_hist"."sysFrom") <= (?)"#,
                r#") as "t3""#,
                r#"INNER JOIN"#,
                r#"(SELECT COLUMN_1 as "identification_number" FROM (VALUES (?))"#,
                r#") as "t8""#,
                r#"ON ("t3"."id") = ("t8"."identification_number")"#,
                r#"WHERE ("t3"."id") = (?) and ("t8"."identification_number") = (?)"#
            ),
            vec![
                Value::from(0_u64),
                Value::from(0_u64),
                Value::from(0_u64),
                Value::from(2_u64),
                Value::from(2_u64),
                Value::from(2_u64),
            ],
        ))),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn join_linker2_test() {
    let sql = r#"select "t1"."id" from "test_space" as "t1"
    inner join (
        select "id" as "id1", "id" as "id2" from "test_space_hist"
    ) as "t2" on "t1"."id" = 1"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "id1".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "id2".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(1_u64)]);
    virtual_table.add_tuple(vec![Value::from(2_u64), Value::from(2_u64)]);
    virtual_table.set_alias("\"t2\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket1)),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {} {} {} {}",
                r#"SELECT "t1"."id" FROM (SELECT"#,
                r#""t1"."id", "t1"."sysFrom", "t1"."FIRST_NAME", "t1"."sys_op""#,
                r#"FROM "test_space" as "t1") as "t1""#,
                r#"INNER JOIN"#,
                r#"(SELECT COLUMN_1 as "id1",COLUMN_2 as "id2" FROM (VALUES (?,?)))"#,
                r#"as "t2" ON ("t1"."id") = (?)"#
            ),
            vec![Value::from(1_u64), Value::from(1_u64), Value::from(1_u64)],
        ))),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn join_linker3_test() {
    let sql = r#"SELECT "t2"."id1" FROM
    (SELECT "id" FROM "test_space") AS "t1"
    INNER JOIN
    (SELECT "id" as "id1", "FIRST_NAME" FROM "test_space") AS "t2"
    ON "t2"."id1" = 1"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "id1".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "id2".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(1_u64)]);
    virtual_table.add_tuple(vec![Value::from(2_u64), Value::from(2_u64)]);
    virtual_table.set_alias("\"t2\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket1)),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {} {} {}",
                r#"SELECT "t2"."id1" FROM"#,
                r#"(SELECT "test_space"."id" FROM "test_space") as "t1""#,
                r#"INNER JOIN"#,
                r#"(SELECT COLUMN_3 as "id1",COLUMN_4 as "id2" FROM (VALUES (?,?),(?,?))) as "t2""#,
                r#"ON ("t2"."id1") = (?)"#,
            ),
            vec![
                Value::from(1_u64),
                Value::from(1_u64),
                Value::from(2_u64),
                Value::from(2_u64),
                Value::from(1_u64),
            ],
        ))),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn join_linker4_test() {
    let sql = r#"SELECT t1."id" FROM "test_space" as t1 JOIN
    (SELECT "FIRST_NAME" as "r_id" FROM "test_space") as t2
    on t1."id" = t2."r_id" and
    t1."FIRST_NAME" = (SELECT "FIRST_NAME" as "fn" FROM "test_space" WHERE "id" = 1)"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    let motion_t2_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();
    let mut virtual_t2 = VirtualTable::new();
    virtual_t2.add_column(Column {
        name: "r_id".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_t2.add_tuple(vec![Value::from(1_u64)]);
    virtual_t2.add_tuple(vec![Value::from(2_u64)]);
    virtual_t2.set_alias("\"T2\"").unwrap();
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_t2_id)
    {
        query
            .reshard_vtable(&mut virtual_t2, key, &DataGeneration::None)
            .unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion_t2_id, virtual_t2);

    let motion_sq_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(1)
        .unwrap()
        .clone();
    let mut virtual_sq = VirtualTable::new();
    virtual_sq.add_column(Column {
        name: "fn".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_sq.add_tuple(vec![Value::from(2_u64)]);
    virtual_sq.add_tuple(vec![Value::from(3_u64)]);
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_sq_id)
    {
        query
            .reshard_vtable(&mut virtual_sq, key, &DataGeneration::None)
            .unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion_sq_id, virtual_sq);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]);

    let param2 = Value::from(2_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {}",
                    r#"SELECT "T1"."id" FROM (SELECT"#,
                    r#""T1"."id", "T1"."sysFrom", "T1"."FIRST_NAME", "T1"."sys_op""#,
                    r#"FROM "test_space" as "T1") as "T1""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT COLUMN_1 as "r_id" FROM (VALUES (?))) as "T2""#,
                    r#"ON ("T1"."id") = ("T2"."r_id")"#,
                    r#"and ("T1"."FIRST_NAME") = (SELECT COLUMN_3 as "fn" FROM (VALUES (?),(?)))"#,
                ),
                vec![Value::from(2_u64), Value::from(2_u64), Value::from(3_u64)],
            ))),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket1)),
            Value::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {}",
                    r#"SELECT "T1"."id" FROM (SELECT"#,
                    r#""T1"."id", "T1"."sysFrom", "T1"."FIRST_NAME", "T1"."sys_op""#,
                    r#"FROM "test_space" as "T1") as "T1""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT COLUMN_1 as "r_id" FROM (VALUES (?))) as "T2""#,
                    r#"ON ("T1"."id") = ("T2"."r_id")"#,
                    r#"and ("T1"."FIRST_NAME") = (SELECT COLUMN_3 as "fn" FROM (VALUES (?),(?)))"#,
                ),
                vec![Value::from(1_u64), Value::from(2_u64), Value::from(3_u64)],
            ))),
        ],
    ]);
    assert_eq!(expected, result);
}

// select * from "test_1" where "identification_number" in (select COLUMN_2 as "b" from (values (1), (2))) or "identification_number" in (select COLUMN_2 as "c" from (values (3), (4)));
#[test]
fn anonymous_col_index_test() {
    let sql = r#"SELECT * FROM "test_space"
    WHERE "id" in (SELECT "identification_number" FROM "hash_testing" WHERE "product_units" < 3)
        OR "id" in (SELECT "identification_number" FROM "hash_testing" WHERE "product_units" > 5)"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion1_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();
    let mut virtual_t1 = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion1_id)
    {
        query
            .reshard_vtable(&mut virtual_t1, key, &DataGeneration::None)
            .unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion1_id, virtual_table_23());
    let motion2_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(1)
        .unwrap()
        .clone();
    let mut virtual_t2 = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion2_id)
    {
        query
            .reshard_vtable(&mut virtual_t2, key, &DataGeneration::None)
            .unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion2_id, virtual_table_23());

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();
    let param2 = Value::from(2_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]);

    let param3 = Value::from(3_u64);
    let bucket3 = query.coordinator.determine_bucket_id(&[&param3]);
    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket3)),
            Value::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {} {} {} {}",
                    "SELECT",
                    r#""test_space"."id","#,
                    r#""test_space"."sysFrom","#,
                    r#""test_space"."FIRST_NAME","#,
                    r#""test_space"."sys_op""#,
                    r#"FROM "test_space""#,
                    r#"WHERE (("test_space"."id") in"#,
                    r#"(SELECT COLUMN_1 as "identification_number" FROM (VALUES (?)))"#,
                    r#"or ("test_space"."id") in"#,
                    r#"(SELECT COLUMN_2 as "identification_number" FROM (VALUES (?))))"#,
                ),
                vec![Value::from(3_u64), Value::from(3_u64)],
            ))),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {} {} {} {}",
                    "SELECT",
                    r#""test_space"."id","#,
                    r#""test_space"."sysFrom","#,
                    r#""test_space"."FIRST_NAME","#,
                    r#""test_space"."sys_op""#,
                    r#"FROM "test_space""#,
                    r#"WHERE (("test_space"."id") in"#,
                    r#"(SELECT COLUMN_1 as "identification_number" FROM (VALUES (?)))"#,
                    r#"or ("test_space"."id") in"#,
                    r#"(SELECT COLUMN_2 as "identification_number" FROM (VALUES (?))))"#,
                ),
                vec![Value::from(2_u64), Value::from(2_u64)],
            ))),
        ],
    ]);

    assert_eq!(expected, result);
}

#[test]
fn sharding_column1_test() {
    let sql = r#"SELECT * FROM "test_space" where "id" = 1"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let bucket = query.coordinator.determine_bucket_id(&[&param1]);
    expected.rows.push(vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT "test_space"."id", "test_space"."sysFrom","#,
                r#""test_space"."FIRST_NAME", "test_space"."sys_op""#,
                r#"FROM "test_space" WHERE ("test_space"."id") = (?)"#,
            ),
            vec![Value::from(1_u64)],
        ))),
    ]);
    assert_eq!(expected, result);
}

#[test]
fn sharding_column2_test() {
    let sql = r#"SELECT *, "bucket_id" FROM "test_space" where "id" = 1"#;
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let bucket = query.coordinator.determine_bucket_id(&[&param1]);
    expected.rows.push(vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"SELECT "test_space"."id", "test_space"."sysFrom","#,
                r#""test_space"."FIRST_NAME", "test_space"."sys_op","#,
                r#""test_space"."bucket_id" FROM "test_space" WHERE ("test_space"."id") = (?)"#,
            ),
            vec![Value::from(1_u64)],
        ))),
    ]);
    assert_eq!(expected, result);
}

#[test]
fn insert1_test() {
    let sql = r#"insert into "t" ("b") select "a" from "t"
        where "a" = 1 and "b" = 2 or "a" = 2 and "b" = 3"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(1_u64)]);
    virtual_table.add_tuple(vec![Value::from(2_u64)]);

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Column::default_value();
    let param2 = Value::from(1_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    let param1 = Column::default_value();
    let param2 = Value::from(2_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket1)),
            Value::String(String::from(PatternWithParams::new(
                format!(
                    "{} {}",
                    r#"INSERT INTO "t" ("b", "bucket_id")"#,
                    r#"SELECT COLUMN_1 as "a",COLUMN_2 as "bucket_id" FROM (VALUES (?,?))"#,
                ),
                vec![Value::from(1_u64), Value::from(2156_u64)],
            ))),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(String::from(PatternWithParams::new(
                format!(
                    "{} {}",
                    r#"INSERT INTO "t" ("b", "bucket_id")"#,
                    r#"SELECT COLUMN_1 as "a",COLUMN_2 as "bucket_id" FROM (VALUES (?,?))"#,
                ),
                vec![Value::from(2_u64), Value::from(3832_u64)],
            ))),
        ],
    ]);
    assert_eq!(expected, result);
}

#[test]
fn insert2_test() {
    let sql = r#"insert into "t" ("a", "b") select "a", "b" from "t"
        where "a" = 1 and "b" = 2"#;

    let coordinator = RouterRuntimeMock::new();
    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    // Though projection row has the same distribution key as
    // the target table, we still add a motion and collect a
    // virtual table for it on coordinator to recalculate
    // a "bucket_id" field for "t".
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(2_u64)]);

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let param2 = Value::from(2_u64);
    let bucket = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(
            String::from(
                PatternWithParams::new(
                    format!(
                    "{} {}",
                    r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                    r#"SELECT COLUMN_1 as "a",COLUMN_2 as "b",COLUMN_3 as "bucket_id" FROM (VALUES (?,?,?))"#,
                    ), vec![Value::from(1_u64), Value::from(2_u64), Value::from(550_u64)],
                )
            )
        ),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn insert3_test() {
    let sql = r#"insert into "t" ("b", "a") select "a", "b" from "t"
        where "a" = 1 and "b" = 2 or "a" = 3 and "b" = 4"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(2_u64)]);
    virtual_table.add_tuple(vec![Value::from(3_u64), Value::from(4_u64)]);

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(2_u64);
    let param2 = Value::from(1_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    let param1 = Value::from(4_u64);
    let param2 = Value::from(3_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket1)),
            Value::String(
                String::from(
                    PatternWithParams::new(
                        format!(
                        "{} {}",
                        r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                        r#"SELECT COLUMN_1 as "a",COLUMN_2 as "b",COLUMN_3 as "bucket_id" FROM (VALUES (?,?,?))"#,
                        ), vec![Value::from(1_u64), Value::from(2_u64), Value::from(4427_u64)],
                    )
                )
            ),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(
                String::from(
                    PatternWithParams::new(
                        format!(
                        "{} {}",
                        r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                        r#"SELECT COLUMN_1 as "a",COLUMN_2 as "b",COLUMN_3 as "bucket_id" FROM (VALUES (?,?,?))"#,
                        ), vec![Value::from(3_u64), Value::from(4_u64), Value::from(7100_u64)],
                    )
                )
            ),
        ],
    ]);
    assert_eq!(expected, result);
}

#[test]
fn insert4_test() {
    let sql = r#"insert into "t" ("b", "a") select "b", "a" from "t"
        where "a" = 1 and "b" = 2"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    // Though data allows to be inserted locally still gather it on the
    // coordinator to recalculate a "bucket_id" field for "t".
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();
    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(2_u64), Value::from(1_u64)]);

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let param2 = Value::from(2_u64);
    let bucket = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(
            String::from(
                PatternWithParams::new(
                    format!(
                    "{} {}",
                    r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                    r#"SELECT COLUMN_1 as "b",COLUMN_2 as "a",COLUMN_3 as "bucket_id" FROM (VALUES (?,?,?))"#,
                    ), vec![Value::from(2_u64), Value::from(1_u64), Value::from(550_u64)],
                )
            )
        ),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn insert5_test() {
    let sql = r#"insert into "t" ("b", "a") select 5, 6 from "t"
        where "a" = 1 and "b" = 2"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(5_u64), Value::from(6_u64)]);
    virtual_table.add_tuple(vec![Value::from(5_u64), Value::from(6_u64)]);

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(6_u64);
    let param2 = Value::from(5_u64);
    let bucket = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(
            String::from(
                PatternWithParams::new(
                    format!(
                    "{} {}",
                    r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                    r#"SELECT COLUMN_4 as "a",COLUMN_5 as "b",COLUMN_6 as "bucket_id" FROM (VALUES (?,?,?),(?,?,?))"#,
                    ),
                    vec![
                        Value::from(5_u64),
                        Value::from(6_u64),
                        Value::from(8788_u64),
                        Value::from(5_u64),
                        Value::from(6_u64),
                        Value::from(8788_u64)
                    ],
                )
            )
        ),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn insert6_test() {
    let sql = r#"insert into "t" ("a", "b") values (1, 2), (1, 2), (3, 4)"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "COLUMN_5".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "COLUMN_6".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(2_u64)]);
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(2_u64)]);
    virtual_table.add_tuple(vec![Value::from(3_u64), Value::from(4_u64)]);

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let param2 = Value::from(2_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    let param3 = Value::from(3_u64);
    let param4 = Value::from(4_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param3, &param4]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket1)),
            Value::String(
                String::from(
                    PatternWithParams::new(
                        format!(
                        "{} {} {}",
                        r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                        r#"SELECT COLUMN_4 as "COLUMN_5",COLUMN_5 as "COLUMN_6",COLUMN_6 as "bucket_id""#,
                        r#"FROM (VALUES (?,?,?),(?,?,?))"#,
                        ),
                        vec![
                            Value::from(1_u64),
                            Value::from(2_u64),
                            Value::from(550_u64),
                            Value::from(1_u64),
                            Value::from(2_u64),
                            Value::from(550_u64)
                        ],
                    )
                )
            ),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(
                String::from(
                    PatternWithParams::new(
                        format!(
                        "{} {} {}",
                        r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                        r#"SELECT COLUMN_1 as "COLUMN_5",COLUMN_2 as "COLUMN_6",COLUMN_3 as "bucket_id""#,
                        r#"FROM (VALUES (?,?,?))"#,
                        ), vec![
                            Value::from(3_u64),
                            Value::from(4_u64),
                            Value::from(8906_u64),
                        ],
                    )
                )
            ),
        ],
    ]);
    assert_eq!(expected, result);
}

#[test]
fn insert7_test() {
    let sql = r#"insert into "hash_testing" ("sys_op", "bucket_id" ) values (1, 2)"#;

    let coordinator = RouterRuntimeMock::new();
    let result = Query::new(&coordinator, sql, vec![]).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(format!(
            "System column {} cannot be inserted",
            "\"bucket_id\""
        )),
        result
    );
}

#[test]
fn insert8_test() {
    let sql = r#"insert into "hash_testing" select * from "hash_single_testing""#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column::new(
        "identification_number",
        Type::Integer,
        ColumnRole::User,
    ));
    virtual_table.add_column(Column::new("product_code", Type::String, ColumnRole::User));
    virtual_table.add_column(Column::new(
        "product_units",
        Type::Boolean,
        ColumnRole::User,
    ));
    virtual_table.add_column(Column::new("sys_op", Type::Unsigned, ColumnRole::User));
    virtual_table.add_tuple(vec![
        Value::from(1_u64),
        Value::from("two"),
        Value::from(true),
        Value::from(4_u64),
    ]);

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();
    let param1 = Value::from(1_u64);
    let param2 = Value::from("two");
    let bucket = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(
            String::from(
                PatternWithParams::new(
                    format!(
                    "{} {}{}",
                    r#"INSERT INTO "hash_testing" ("identification_number", "product_code", "product_units", "sys_op", "bucket_id")"#,
                    r#"SELECT COLUMN_1 as "identification_number",COLUMN_2 as "product_code",COLUMN_3 as "product_units","#,
                    r#"COLUMN_4 as "sys_op",COLUMN_5 as "bucket_id" FROM (VALUES (?,?,?,?,?))"#,
                    ), vec![
                        Value::from(1_u64),
                        Value::from("two"),
                        Value::from(true),
                        Value::from(4_u64),
                        Value::from(3016_u64),
                    ],
                )
            )
        ),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn insert9_test() {
    let sql = r#"insert into "t" ("a", "b") values (?, ?)"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(
        &coordinator,
        sql,
        vec![Value::from(1_u64), Value::from(2_u64)],
    )
    .unwrap();
    let motion_id = query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap()
        .clone();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "COLUMN_1".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "COLUMN_2".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(2_u64)]);

    query
        .coordinator
        .add_virtual_table(motion_id, virtual_table);
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    let param1 = Value::from(1_u64);
    let param2 = Value::from(2_u64);
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket1)),
        Value::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                r#"SELECT COLUMN_1 as "COLUMN_1",COLUMN_2 as "COLUMN_2",COLUMN_3 as "bucket_id""#,
                r#"FROM (VALUES (?,?,?))"#,
            ),
            vec![Value::from(1_u64), Value::from(2_u64), Value::from(550_u64)],
        ))),
    ]]);
    assert_eq!(expected, result);
}

/// Helper function to create a "test" virtual table.
fn virtual_table_23() -> VirtualTable {
    let mut virtual_table = VirtualTable::new();

    virtual_table.add_column(Column {
        name: "identification_number".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });

    virtual_table.add_tuple(vec![Value::from(2_u64)]);
    virtual_table.add_tuple(vec![Value::from(3_u64)]);

    virtual_table
}

fn get_motion_policy(plan: &Plan, motion_id: usize) -> &MotionPolicy {
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        policy
    } else {
        panic!("Expected a motion node");
    }
}

pub(crate) fn broadcast_check(sql: &str, pattern: &str, params: Vec<Value>) {
    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();

    expected.rows.push(vec![
        Value::String("Execute query on all buckets".to_string()),
        Value::String(String::from(PatternWithParams::new(
            pattern.to_string(),
            params,
        ))),
    ]);
    assert_eq!(expected, result);
}

#[cfg(test)]
mod between;

#[cfg(test)]
mod bucket_id;

#[cfg(test)]
mod cast;

#[cfg(test)]
mod concat;

#[cfg(test)]
mod empty_motion;

#[cfg(test)]
mod frontend;

#[cfg(test)]
mod not_in;

#[cfg(test)]
mod not_eq;

#[cfg(test)]
mod subtree;
