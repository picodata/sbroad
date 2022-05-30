use pretty_assertions::assert_eq;

use crate::executor::engine::mock::EngineMock;
use crate::executor::result::{ExecutorResults, ProducerResults, Value};
use crate::executor::vtable::VirtualTable;
use crate::ir::operator::Relational;
use crate::ir::relation::{Column, ColumnRole, Type};
use crate::ir::transformation::redistribution::{DataGeneration, MotionPolicy};
use crate::ir::value::Value as IrValue;

use super::*;

#[test]
fn shard_query() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" = 1"#;
    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let bucket = query.engine.determine_bucket_id(&[&param1]);
    expected
        .rows
        .push(vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket)),
            Value::String(String::from(r#"SELECT "test_space"."FIRST_NAME" as "FIRST_NAME" FROM "test_space" WHERE ("test_space"."id") = (1)"#))
        ]);
    assert_eq!(ExecutorResults::from(expected), query.exec().unwrap())
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

    let mut query = Query::new(&engine, sql, &[]).unwrap();

    let mut expected = ProducerResults::new();
    let param1 = IrValue::number_from_str("1").unwrap();
    let bucket = query.engine.determine_bucket_id(&[&param1]);
    expected
        .rows
        .push(vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket)),
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

    assert_eq!(ExecutorResults::from(expected), query.exec().unwrap())
}

#[test]
fn map_reduce_query() {
    let sql = r#"SELECT "product_code" FROM "hash_testing" where "identification_number" = 1 and "product_code" = '457'"#;
    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let param457 = IrValue::string_from_str("457");

    let bucket = query.engine.determine_bucket_id(&[&param1, &param457]);

    expected.rows.push(vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(
            format!(
                "{} {} {}",
                r#"SELECT "hash_testing"."product_code" as "product_code""#,
                r#"FROM "hash_testing""#,
                r#"WHERE ("hash_testing"."identification_number", "hash_testing"."product_code") = (1, '457')"#,
            )
        )
    ]);

    assert_eq!(ExecutorResults::from(expected), query.exec().unwrap())
}

#[test]
fn linker_test() {
    let sql = r#"SELECT "FIRST_NAME" FROM "test_space" where "id" in
    (SELECT "identification_number" FROM "hash_testing" where "identification_number" > 1)"#;
    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];
    let mut virtual_table = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }
    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket2 = query.engine.determine_bucket_id(&[&param2]);

    let param3 = IrValue::number_from_str("3").unwrap();
    let bucket3 = query.engine.determine_bucket_id(&[&param3]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket3)),
            Value::String(format!(
                "{} {} {}",
                r#"SELECT "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                r#"FROM "test_space""#,
                r#"WHERE ("test_space"."id") in (SELECT COLUMN_1 as "identification_number" FROM (VALUES (3)))"#,
            )),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(format!(
                "{} {} {}",
                r#"SELECT "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                r#"FROM "test_space""#,
                r#"WHERE ("test_space"."id") in (SELECT COLUMN_1 as "identification_number" FROM (VALUES (2)))"#,
            )),
        ],
    ]);

    assert_eq!(ExecutorResults::from(expected), result)
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

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];
    let mut virtual_table = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }
    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket2 = query.engine.determine_bucket_id(&[&param2]);

    let param3 = IrValue::number_from_str("3").unwrap();
    let bucket3 = query.engine.determine_bucket_id(&[&param3]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket3)),
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
                    r#"WHERE ("t1"."id") in (SELECT COLUMN_1 as "identification_number" FROM (VALUES (3)))"#
                )
            )
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
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
                    r#"WHERE ("t1"."id") in (SELECT COLUMN_1 as "identification_number" FROM (VALUES (2)))"#
                )
            )
        ],
    ]);

    assert_eq!(ExecutorResults::from(expected), result)
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

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];
    let mut virtual_table = virtual_table_23();
    virtual_table.set_alias("\"t8\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }
    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket2 = query.engine.determine_bucket_id(&[&param2]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(
                format!(
                    "{}, {}, {} {}{} {} {} {} {} {} {}{} {} {}{} {} {}",
                    r#"SELECT "t3"."id" as "id""#,
                    r#""t3"."FIRST_NAME" as "FIRST_NAME""#,
                    r#""t8"."identification_number" as "identification_number""#,
                    r#"FROM ("#,
                    r#"SELECT "test_space"."id" as "id", "test_space"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space""#,
                    r#"WHERE (0) > ("test_space"."sys_op") and ("test_space"."sysFrom") >= (0)"#,
                    r#"UNION ALL"#,
                    r#"SELECT "test_space_hist"."id" as "id", "test_space_hist"."FIRST_NAME" as "FIRST_NAME""#,
                    r#"FROM "test_space_hist""#,
                    r#"WHERE ("test_space_hist"."sysFrom") <= (0)"#,
                    r#") as "t3""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT COLUMN_1 as "identification_number" FROM (VALUES (2))"#,
                    r#") as "t8""#,
                    r#"ON ("t3"."id") = ("t8"."identification_number")"#,
                    r#"WHERE ("t3"."id", "t3"."id", "t8"."identification_number") = ("t8"."identification_number", 2, 2)"#
                )
            )
        ],
    ]);
    assert_eq!(ExecutorResults::from(expected), result)
}

#[test]
fn join_linker2_test() {
    let sql = r#"select "t1"."id" from "test_space" as "t1"
    inner join (
        select "id" as "id1", "id" as "id2" from "test_space_hist"
    ) as "t2" on "t1"."id" = 1"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];

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
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("1").unwrap(),
        IrValue::number_from_str("1").unwrap(),
    ]);
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("2").unwrap(),
        IrValue::number_from_str("2").unwrap(),
    ]);
    virtual_table.set_alias("\"t2\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }

    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let bucket1 = query.engine.determine_bucket_id(&[&param1]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket1)),
        Value::String(format!(
            "{} {} {} {}",
            r#"SELECT "t1"."id" as "id" FROM "test_space" as "t1""#,
            r#"INNER JOIN"#,
            r#"(SELECT COLUMN_1 as "id1",COLUMN_2 as "id2" FROM (VALUES (1,1)))"#,
            r#"as "t2" ON ("t1"."id") = (1)"#
        )),
    ]]);
    assert_eq!(ExecutorResults::from(expected), result)
}

#[test]
fn join_linker3_test() {
    let sql = r#"SELECT "t2"."id1" FROM
    (SELECT "id" FROM "test_space") AS "t1"
    INNER JOIN
    (SELECT "id" as "id1", "FIRST_NAME" FROM "test_space") AS "t2"
    ON "t2"."id1" = 1"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];

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
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("1").unwrap(),
        IrValue::number_from_str("1").unwrap(),
    ]);
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("2").unwrap(),
        IrValue::number_from_str("2").unwrap(),
    ]);
    virtual_table.set_alias("\"t2\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query
            .reshard_vtable(&mut virtual_table, key, &DataGeneration::None)
            .unwrap();
    }

    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let bucket1 = query.engine.determine_bucket_id(&[&param1]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket1)),
        Value::String(format!(
            "{} {} {} {} {}",
            r#"SELECT "t2"."id1" as "id1" FROM"#,
            r#"(SELECT "test_space"."id" as "id" FROM "test_space") as "t1""#,
            r#"INNER JOIN"#,
            r#"(SELECT COLUMN_3 as "id1",COLUMN_4 as "id2" FROM (VALUES (1,1),(2,2))) as "t2""#,
            r#"ON ("t2"."id1") = (1)"#,
        )),
    ]]);
    assert_eq!(ExecutorResults::from(expected), result)
}

#[test]
fn join_linker4_test() {
    let sql = r#"SELECT t1."id" FROM "test_space" as t1 JOIN
    (SELECT "FIRST_NAME" as "r_id" FROM "test_space") as t2
    on t1."id" = t2."r_id" and
    t1."FIRST_NAME" = (SELECT "FIRST_NAME" as "fn" FROM "test_space" WHERE "id" = 1)"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();

    let motion_t2_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];
    let mut virtual_t2 = VirtualTable::new();
    virtual_t2.add_column(Column {
        name: "r_id".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_t2.add_values_tuple(vec![IrValue::number_from_str("1").unwrap()]);
    virtual_t2.add_values_tuple(vec![IrValue::number_from_str("2").unwrap()]);
    virtual_t2.set_alias("t2").unwrap();
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_t2_id)
    {
        query
            .reshard_vtable(&mut virtual_t2, key, &DataGeneration::None)
            .unwrap();
    }
    query.engine.add_virtual_table(motion_t2_id, virtual_t2);

    let motion_sq_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][1];
    let mut virtual_sq = VirtualTable::new();
    virtual_sq.add_column(Column {
        name: "fn".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_sq.add_values_tuple(vec![IrValue::number_from_str("2").unwrap()]);
    virtual_sq.add_values_tuple(vec![IrValue::number_from_str("3").unwrap()]);
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_sq_id)
    {
        query
            .reshard_vtable(&mut virtual_sq, key, &DataGeneration::None)
            .unwrap();
    }
    query.engine.add_virtual_table(motion_sq_id, virtual_sq);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let bucket1 = query.engine.determine_bucket_id(&[&param1]);

    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket2 = query.engine.determine_bucket_id(&[&param2]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(format!(
                "{} {} {} {} {}",
                r#"SELECT t1."id" as "id" FROM "test_space" as t1"#,
                r#"INNER JOIN"#,
                r#"(SELECT COLUMN_1 as "r_id" FROM (VALUES (2))) as t2"#,
                r#"ON (t1."id") = (t2."r_id")"#,
                r#"and (t1."FIRST_NAME") = (SELECT COLUMN_3 as "fn" FROM (VALUES (2),(3)))"#,
            )),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket1)),
            Value::String(format!(
                "{} {} {} {} {}",
                r#"SELECT t1."id" as "id" FROM "test_space" as t1"#,
                r#"INNER JOIN"#,
                r#"(SELECT COLUMN_1 as "r_id" FROM (VALUES (1))) as t2"#,
                r#"ON (t1."id") = (t2."r_id")"#,
                r#"and (t1."FIRST_NAME") = (SELECT COLUMN_3 as "fn" FROM (VALUES (2),(3)))"#,
            )),
        ],
    ]);
    assert_eq!(ExecutorResults::from(expected), result)
}

// select * from "test_1" where "identification_number" in (select COLUMN_2 as "b" from (values (1), (2))) or "identification_number" in (select COLUMN_2 as "c" from (values (3), (4)));
#[test]
fn anonymous_col_index_test() {
    let sql = r#"SELECT * FROM "test_space"
    WHERE "id" in (SELECT "identification_number" FROM "hash_testing" WHERE "product_units" < 3)
        OR "id" in (SELECT "identification_number" FROM "hash_testing" WHERE "product_units" > 5)"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion1_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];
    let mut virtual_t1 = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion1_id)
    {
        query
            .reshard_vtable(&mut virtual_t1, key, &DataGeneration::None)
            .unwrap();
    }
    query
        .engine
        .add_virtual_table(motion1_id, virtual_table_23());
    let motion2_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][1];
    let mut virtual_t2 = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion2_id)
    {
        query
            .reshard_vtable(&mut virtual_t2, key, &DataGeneration::None)
            .unwrap();
    }
    query
        .engine
        .add_virtual_table(motion2_id, virtual_table_23());

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();
    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket2 = query.engine.determine_bucket_id(&[&param2]);

    let param3 = IrValue::number_from_str("3").unwrap();
    let bucket3 = query.engine.determine_bucket_id(&[&param3]);
    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket3)),
            Value::String(format!(
                "{} {} {} {} {} {} {} {} {} {}",
                "SELECT",
                r#""test_space"."id" as "id","#,
                r#""test_space"."sysFrom" as "sysFrom","#,
                r#""test_space"."FIRST_NAME" as "FIRST_NAME","#,
                r#""test_space"."sys_op" as "sys_op""#,
                r#"FROM "test_space""#,
                r#"WHERE (("test_space"."id") in"#,
                r#"(SELECT COLUMN_1 as "identification_number" FROM (VALUES (3)))"#,
                r#"or ("test_space"."id") in"#,
                r#"(SELECT COLUMN_2 as "identification_number" FROM (VALUES (3))))"#,
            )),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(format!(
                "{} {} {} {} {} {} {} {} {} {}",
                "SELECT",
                r#""test_space"."id" as "id","#,
                r#""test_space"."sysFrom" as "sysFrom","#,
                r#""test_space"."FIRST_NAME" as "FIRST_NAME","#,
                r#""test_space"."sys_op" as "sys_op""#,
                r#"FROM "test_space""#,
                r#"WHERE (("test_space"."id") in"#,
                r#"(SELECT COLUMN_1 as "identification_number" FROM (VALUES (2)))"#,
                r#"or ("test_space"."id") in"#,
                r#"(SELECT COLUMN_2 as "identification_number" FROM (VALUES (2))))"#,
            )),
        ],
    ]);

    assert_eq!(ExecutorResults::from(expected), result)
}

#[test]
fn sharding_column1_test() {
    let sql = r#"SELECT * FROM "test_space" where "id" = 1"#;
    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let bucket = query.engine.determine_bucket_id(&[&param1]);
    expected.rows.push(vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(format!(
            "{} {} {}",
            r#"SELECT "test_space"."id" as "id", "test_space"."sysFrom" as "sysFrom","#,
            r#""test_space"."FIRST_NAME" as "FIRST_NAME", "test_space"."sys_op" as "sys_op""#,
            r#"FROM "test_space" WHERE ("test_space"."id") = (1)"#,
        )),
    ]);
    assert_eq!(ExecutorResults::from(expected), query.exec().unwrap())
}

#[test]
fn sharding_column2_test() {
    let sql = r#"SELECT *, "bucket_id" FROM "test_space" where "id" = 1"#;
    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let bucket = query.engine.determine_bucket_id(&[&param1]);
    expected.rows.push(vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(format!(
            "{} {} {}",
            r#"SELECT "test_space"."id" as "id", "test_space"."sysFrom" as "sysFrom","#,
            r#""test_space"."FIRST_NAME" as "FIRST_NAME", "test_space"."sys_op" as "sys_op","#,
            r#""test_space"."bucket_id" as "bucket_id" FROM "test_space" WHERE ("test_space"."id") = (1)"#,
        )),
    ]);
    assert_eq!(ExecutorResults::from(expected), query.exec().unwrap())
}

#[test]
fn insert1_test() {
    let sql = r#"insert into "t" ("b") select "a" from "t"
        where "a" = 1 and "b" = 2 or "a" = 2 and "b" = 3"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_values_tuple(vec![IrValue::number_from_str("1").unwrap()]);
    virtual_table.add_values_tuple(vec![IrValue::number_from_str("2").unwrap()]);

    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = Column::default_value();
    let param2 = IrValue::number_from_str("1").unwrap();
    let bucket1 = query.engine.determine_bucket_id(&[&param1, &param2]);

    let param1 = Column::default_value();
    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket2 = query.engine.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket1)),
            Value::String(format!(
                "{} {}",
                r#"INSERT INTO "t" ("b", "bucket_id")"#,
                r#"SELECT COLUMN_1 as "a",COLUMN_2 as "bucket_id" FROM (VALUES (1,2156))"#,
            )),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(format!(
                "{} {}",
                r#"INSERT INTO "t" ("b", "bucket_id")"#,
                r#"SELECT COLUMN_1 as "a",COLUMN_2 as "bucket_id" FROM (VALUES (2,3832))"#,
            )),
        ],
    ]);
    assert_eq!(ExecutorResults::from(expected), result);
}

#[test]
fn insert2_test() {
    let sql = r#"insert into "t" ("a", "b") select "a", "b" from "t"
        where "a" = 1 and "b" = 2"#;

    let engine = EngineMock::new();
    let mut query = Query::new(&engine, sql, &[]).unwrap();

    // Though projection row has the same distribution key as
    // the target table, we still add a motion and collect a
    // virtual table for it on coordinator to recalculate
    // a "bucket_id" field for "t".
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];
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
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("1").unwrap(),
        IrValue::number_from_str("2").unwrap(),
    ]);

    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket = query.engine.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(format!(
            "{} {}",
            r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
            r#"SELECT COLUMN_1 as "a",COLUMN_2 as "b",COLUMN_3 as "bucket_id" FROM (VALUES (1,2,550))"#,
        )),
    ]]);
    assert_eq!(ExecutorResults::from(expected), result);
}

#[test]
fn insert3_test() {
    let sql = r#"insert into "t" ("b", "a") select "a", "b" from "t"
        where "a" = 1 and "b" = 2 or "a" = 3 and "b" = 4"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];

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
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("1").unwrap(),
        IrValue::number_from_str("2").unwrap(),
    ]);
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("3").unwrap(),
        IrValue::number_from_str("4").unwrap(),
    ]);

    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("2").unwrap();
    let param2 = IrValue::number_from_str("1").unwrap();
    let bucket1 = query.engine.determine_bucket_id(&[&param1, &param2]);

    let param1 = IrValue::number_from_str("4").unwrap();
    let param2 = IrValue::number_from_str("3").unwrap();
    let bucket2 = query.engine.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket1)),
            Value::String(format!(
                "{} {}",
                r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                r#"SELECT COLUMN_1 as "a",COLUMN_2 as "b",COLUMN_3 as "bucket_id" FROM (VALUES (1,2,4427))"#,
            )),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(format!(
                "{} {}",
                r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                r#"SELECT COLUMN_1 as "a",COLUMN_2 as "b",COLUMN_3 as "bucket_id" FROM (VALUES (3,4,7100))"#,
            )),
        ],
    ]);
    assert_eq!(ExecutorResults::from(expected), result);
}

#[test]
fn insert4_test() {
    let sql = r#"insert into "t" ("b", "a") select "b", "a" from "t"
        where "a" = 1 and "b" = 2"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();

    // Though data allows to be inserted locally still gather it on the
    // coordinator to recalculate a "bucket_id" field for "t".
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];
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
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("2").unwrap(),
        IrValue::number_from_str("1").unwrap(),
    ]);

    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket = query.engine.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(format!(
            "{} {}",
            r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
            r#"SELECT COLUMN_1 as "b",COLUMN_2 as "a",COLUMN_3 as "bucket_id" FROM (VALUES (2,1,550))"#,
        )),
    ]]);
    assert_eq!(ExecutorResults::from(expected), result);
}

#[test]
fn insert5_test() {
    let sql = r#"insert into "t" ("b", "a") select 5, 6 from "t"
        where "a" = 1 and "b" = 2"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];

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
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("5").unwrap(),
        IrValue::number_from_str("6").unwrap(),
    ]);
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("5").unwrap(),
        IrValue::number_from_str("6").unwrap(),
    ]);

    query.engine.add_virtual_table(motion_id, virtual_table);

    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("6").unwrap();
    let param2 = IrValue::number_from_str("5").unwrap();
    let bucket = query.engine.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(format!(
            "{} {}",
            r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
            r#"SELECT COLUMN_4 as "a",COLUMN_5 as "b",COLUMN_6 as "bucket_id" FROM (VALUES (5,6,8788),(5,6,8788))"#,
        )),
    ]]);
    assert_eq!(ExecutorResults::from(expected), result);
}

#[test]
fn insert6_test() {
    let sql = r#"insert into "t" ("a", "b") values (1, 2), (1, 2), (3, 4)"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];

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
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("1").unwrap(),
        IrValue::number_from_str("2").unwrap(),
    ]);
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("1").unwrap(),
        IrValue::number_from_str("2").unwrap(),
    ]);
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("3").unwrap(),
        IrValue::number_from_str("4").unwrap(),
    ]);

    query.engine.add_virtual_table(motion_id, virtual_table);
    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket1 = query.engine.determine_bucket_id(&[&param1, &param2]);

    let param3 = IrValue::number_from_str("3").unwrap();
    let param4 = IrValue::number_from_str("4").unwrap();
    let bucket2 = query.engine.determine_bucket_id(&[&param3, &param4]);

    expected.rows.extend(vec![
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket1)),
            Value::String(format!(
                "{} {} {}",
                r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                r#"SELECT COLUMN_4 as "COLUMN_5",COLUMN_5 as "COLUMN_6",COLUMN_6 as "bucket_id""#,
                r#"FROM (VALUES (1,2,550),(1,2,550))"#,
            )),
        ],
        vec![
            Value::String(format!("Execute query on a bucket [{}]", bucket2)),
            Value::String(format!(
                "{} {} {}",
                r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                r#"SELECT COLUMN_1 as "COLUMN_5",COLUMN_2 as "COLUMN_6",COLUMN_3 as "bucket_id""#,
                r#"FROM (VALUES (3,4,8906))"#,
            )),
        ],
    ]);
    assert_eq!(ExecutorResults::from(expected), result);
}

#[test]
fn insert7_test() {
    let sql = r#"insert into "hash_testing" ("sys_op", "bucket_id" ) values (1, 2)"#;

    let engine = EngineMock::new();
    let result = Query::new(&engine, sql, &[]).unwrap_err();

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

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];

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
    virtual_table.add_column(Column::new("sys_op", Type::Number, ColumnRole::User));
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("1").unwrap(),
        IrValue::string_from_str("two"),
        IrValue::Boolean(true),
        IrValue::number_from_str("4").unwrap(),
    ]);

    query.engine.add_virtual_table(motion_id, virtual_table);
    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();
    let param1 = IrValue::number_from_str("1").unwrap();
    let param2 = IrValue::string_from_str("two");
    let bucket = query.engine.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(format!(
            "{} {}{}",
            r#"INSERT INTO "hash_testing" ("identification_number", "product_code", "product_units", "sys_op", "bucket_id")"#,
            r#"SELECT COLUMN_1 as "identification_number",COLUMN_2 as "product_code",COLUMN_3 as "product_units","#,
            r#"COLUMN_4 as "sys_op",COLUMN_5 as "bucket_id" FROM (VALUES (1,'two',true,4,3016))"#,
        )),
    ]]);
    assert_eq!(ExecutorResults::from(expected), result);
}

#[test]
fn insert9_test() {
    let sql = r#"insert into "t" ("a", "b") values (?, ?)"#;

    let engine = EngineMock::new();

    let mut query = Query::new(&engine, sql, &[IrValue::from(1), IrValue::from(2)]).unwrap();
    let motion_id = query.exec_plan.get_ir_plan().get_slices().unwrap()[0][0];

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
    virtual_table.add_values_tuple(vec![
        IrValue::number_from_str("1").unwrap(),
        IrValue::number_from_str("2").unwrap(),
    ]);

    query.engine.add_virtual_table(motion_id, virtual_table);
    let result = query.exec().unwrap();

    let mut expected = ProducerResults::new();

    let param1 = IrValue::number_from_str("1").unwrap();
    let param2 = IrValue::number_from_str("2").unwrap();
    let bucket1 = query.engine.determine_bucket_id(&[&param1, &param2]);

    expected.rows.extend(vec![vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket1)),
        Value::String(format!(
            "{} {} {}",
            r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
            r#"SELECT COLUMN_1 as "COLUMN_1",COLUMN_2 as "COLUMN_2",COLUMN_3 as "bucket_id""#,
            r#"FROM (VALUES (1,2,550))"#,
        )),
    ]]);
    assert_eq!(ExecutorResults::from(expected), result);
}

/// Helper function to create a "test" virtual table.
fn virtual_table_23() -> VirtualTable {
    let mut virtual_table = VirtualTable::new();

    virtual_table.add_column(Column {
        name: "identification_number".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });

    virtual_table.add_values_tuple(vec![IrValue::number_from_str("2").unwrap()]);
    virtual_table.add_values_tuple(vec![IrValue::number_from_str("3").unwrap()]);

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
