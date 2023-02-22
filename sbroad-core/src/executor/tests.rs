use pretty_assertions::assert_eq;

use crate::backend::sql::ir::PatternWithParams;

use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::ir::operator::Relational;
use crate::ir::relation::{Column, ColumnRole, Type};
use crate::ir::transformation::redistribution::MotionPolicy;

use crate::ir::value::{EncodedValue, Value};

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
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
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
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
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
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(
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
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query.reshard_vtable(&mut virtual_table, key).unwrap();
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
            EncodedValue::String(format!("Execute query on a bucket [{bucket3}]")),
            EncodedValue::String(
                String::from(
                    PatternWithParams::new(
                        format!(
                        "{} {} {}",
                        r#"SELECT "test_space"."FIRST_NAME""#,
                        r#"FROM "test_space""#,
                        r#"WHERE ("test_space"."id") in (SELECT "identification_number" FROM "TMP_test_76")"#,
                        ), vec![],
                    )
                )
            ),
        ],
        vec![
            EncodedValue::String(format!("Execute query on a bucket [{bucket2}]")),
            EncodedValue::String(
                String::from(
                    PatternWithParams::new(
                        format!(
                        "{} {} {}",
                        r#"SELECT "test_space"."FIRST_NAME""#,
                        r#"FROM "test_space""#,
                        r#"WHERE ("test_space"."id") in (SELECT "identification_number" FROM "TMP_test_76")"#,
                        ), vec![],
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
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query.reshard_vtable(&mut virtual_table, key).unwrap();
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
            EncodedValue::String(format!("Execute query on a bucket [{bucket3}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
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
                    r#"WHERE ("t1"."id") in (SELECT "identification_number" FROM "TMP_test_219")"#,
                ),
                vec![Value::from(0_u64), Value::from(0_u64)],
            ))),
        ],
        vec![
            EncodedValue::String(format!("Execute query on a bucket [{bucket2}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
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
                    r#"WHERE ("t1"."id") in (SELECT "identification_number" FROM "TMP_test_219")"#,
                ),
                vec![Value::from(0_u64), Value::from(0_u64)],
            ))),
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
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_table = virtual_table_23();
    virtual_table.set_alias("\"t8\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query.reshard_vtable(&mut virtual_table, key).unwrap();
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
        EncodedValue::String(format!("Execute query on a bucket [{bucket2}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
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
                r#"(SELECT "identification_number" FROM "TMP_test_253""#,
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
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

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
        query.reshard_vtable(&mut virtual_table, key).unwrap();
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
        EncodedValue::String(format!("Execute query on a bucket [{bucket1}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {} {} {} {}",
                r#"SELECT "t1"."id" FROM (SELECT"#,
                r#""t1"."id", "t1"."sysFrom", "t1"."FIRST_NAME", "t1"."sys_op""#,
                r#"FROM "test_space" as "t1") as "t1""#,
                r#"INNER JOIN"#,
                r#"(SELECT "id1","id2" FROM "TMP_test_87")"#,
                r#"as "t2" ON ("t1"."id") = (?)"#
            ),
            vec![Value::from(1_u64)],
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
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "id1".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "FIRST_NAME".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(1_u64)]);
    virtual_table.add_tuple(vec![Value::from(2_u64), Value::from(2_u64)]);
    virtual_table.set_alias("\"t2\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        query.reshard_vtable(&mut virtual_table, key).unwrap();
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
        EncodedValue::String(format!("Execute query on a bucket [{bucket1}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {} {} {}",
                r#"SELECT "t2"."id1" FROM"#,
                r#"(SELECT "test_space"."id" FROM "test_space") as "t1""#,
                r#"INNER JOIN"#,
                r#"(SELECT "id1","FIRST_NAME" FROM "TMP_test_69") as "t2""#,
                r#"ON ("t2"."id1") = (?)"#,
            ),
            vec![Value::from(1_u64)],
        ))),
    ]]);
    assert_eq!(expected, result);
}

#[test]
#[allow(clippy::too_many_lines)]
fn join_linker4_test() {
    let sql = r#"SELECT t1."id" FROM "test_space" as t1 JOIN
    (SELECT "FIRST_NAME" as "r_id" FROM "test_space") as t2
    on t1."id" = t2."r_id" and
    t1."FIRST_NAME" = (SELECT "FIRST_NAME" as "fn" FROM "test_space" WHERE "id" = 1)"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    let motion_t2_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
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
        query.reshard_vtable(&mut virtual_t2, key).unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion_t2_id, virtual_t2);

    let motion_sq_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(1)
        .unwrap();
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
        query.reshard_vtable(&mut virtual_sq, key).unwrap();
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
            EncodedValue::String(format!("Execute query on a bucket [{bucket2}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {}",
                    r#"SELECT "T1"."id" FROM (SELECT"#,
                    r#""T1"."id", "T1"."sysFrom", "T1"."FIRST_NAME", "T1"."sys_op""#,
                    r#"FROM "test_space" as "T1") as "T1""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT "r_id" FROM "TMP_test_137") as "T2""#,
                    r#"ON ("T1"."id") = ("T2"."r_id")"#,
                    r#"and ("T1"."FIRST_NAME") = (SELECT "fn" FROM "TMP_test_141")"#,
                ),
                vec![],
            ))),
        ],
        vec![
            EncodedValue::String(format!("Execute query on a bucket [{bucket1}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {}",
                    r#"SELECT "T1"."id" FROM (SELECT"#,
                    r#""T1"."id", "T1"."sysFrom", "T1"."FIRST_NAME", "T1"."sys_op""#,
                    r#"FROM "test_space" as "T1") as "T1""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT "r_id" FROM "TMP_test_137") as "T2""#,
                    r#"ON ("T1"."id") = ("T2"."r_id")"#,
                    r#"and ("T1"."FIRST_NAME") = (SELECT "fn" FROM "TMP_test_141")"#,
                ),
                vec![],
            ))),
        ],
    ]);
    assert_eq!(expected, result);
}

#[test]
fn join_linker5_test() {
    let sql = r#"select * from "t1" inner join (
      select "f", "b" as B from "t2"
      inner join "t3" on "t2"."g" = "t3"."b") as q
on q."f" = "t1"."a""#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    let motion_t2_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_t2 = VirtualTable::new();
    virtual_t2.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_t2.set_alias("\"t3\"").unwrap();
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_t2_id)
    {
        query.reshard_vtable(&mut virtual_t2, key).unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion_t2_id, virtual_t2);

    let motion_sq_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(1)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_sq = VirtualTable::new();
    virtual_sq.add_column(Column {
        name: "f".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_sq.add_column(Column {
        name: "B".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_sq.set_alias("Q").unwrap();
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_sq_id)
    {
        query.reshard_vtable(&mut virtual_sq, key).unwrap();
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

    expected.rows.extend(vec![vec![
        EncodedValue::String("Execute query on all buckets".to_string()),
        EncodedValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"SELECT "t1"."a", "t1"."b", "Q"."f", "Q"."B" FROM"#,
                r#"(SELECT "t1"."a", "t1"."b" FROM "t1") as "t1""#,
                r#"INNER JOIN (SELECT "f","B" FROM "TMP_test_146")"#,
                r#"as Q ON ("Q"."f") = ("t1"."a")"#,
            ),
            vec![],
        ))),
    ]]);
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
    let motion1_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let mut virtual_t1 = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion1_id)
    {
        query.reshard_vtable(&mut virtual_t1, key).unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion1_id, virtual_table_23());
    let motion2_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(1)
        .unwrap();
    let mut virtual_t2 = virtual_table_23();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion2_id)
    {
        query.reshard_vtable(&mut virtual_t2, key).unwrap();
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
            EncodedValue::String(format!("Execute query on a bucket [{bucket3}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {} {} {} {}",
                    "SELECT",
                    r#""test_space"."id","#,
                    r#""test_space"."sysFrom","#,
                    r#""test_space"."FIRST_NAME","#,
                    r#""test_space"."sys_op""#,
                    r#"FROM "test_space""#,
                    r#"WHERE (("test_space"."id") in"#,
                    r#"(SELECT "identification_number" FROM "TMP_test_134")"#,
                    r#"or ("test_space"."id") in"#,
                    r#"(SELECT "identification_number" FROM "TMP_test_130"))"#,
                ),
                vec![],
            ))),
        ],
        vec![
            EncodedValue::String(format!("Execute query on a bucket [{bucket2}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {} {} {} {}",
                    "SELECT",
                    r#""test_space"."id","#,
                    r#""test_space"."sysFrom","#,
                    r#""test_space"."FIRST_NAME","#,
                    r#""test_space"."sys_op""#,
                    r#"FROM "test_space""#,
                    r#"WHERE (("test_space"."id") in"#,
                    r#"(SELECT "identification_number" FROM "TMP_test_134")"#,
                    r#"or ("test_space"."id") in"#,
                    r#"(SELECT "identification_number" FROM "TMP_test_130"))"#,
                ),
                vec![],
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
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
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
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
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
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(1_u64)]);
    virtual_table.add_tuple(vec![Value::from(2_u64)]);
    virtual_table.set_alias("\"t\"").unwrap();

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
            EncodedValue::String(format!("Execute query on a bucket [{bucket1}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {}",
                    r#"INSERT INTO "t" ("b", "bucket_id")"#,
                    r#"SELECT COL_0, bucket_id (coalesce (CAST (? as string), ?) || coalesce (CAST (COL_0 as string), ?)) FROM"#,
                    r#"(SELECT CAST ("t"."a" as unsigned) as COL_0 FROM ((SELECT "a" FROM "TMP_test_142") as "t"))"#,
                ),
                vec![Value::Null, Value::String("".into()), Value::String("".into())],
            ))),
        ],
        vec![
            EncodedValue::String(format!("Execute query on a bucket [{bucket2}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {}",
                    r#"INSERT INTO "t" ("b", "bucket_id")"#,
                    r#"SELECT COL_0, bucket_id (coalesce (CAST (? as string), ?) || coalesce (CAST (COL_0 as string), ?)) FROM"#,
                    r#"(SELECT CAST ("t"."a" as unsigned) as COL_0 FROM ((SELECT "a" FROM "TMP_test_142") as "t"))"#,
                ),
                vec![Value::Null, Value::String("".into()), Value::String("".into())],
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
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                r#"SELECT COL_0, COL_1, bucket_id (coalesce (CAST (COL_0 as string), ?) || coalesce (CAST (COL_1 as string), ?)) FROM"#,
                r#"(SELECT CAST ("t"."a" as unsigned) as COL_0, CAST ("t"."b" as unsigned) as COL_1 FROM"#,
                r#"(SELECT "t"."a", "t"."b" FROM "t" WHERE ("t"."a") = (?) and ("t"."b") = (?)))"#,
            ),
            vec![Value::from(""), Value::from(""), Value::from(1_u64), Value::from(2_u64)],
        ))),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn insert3_test() {
    let sql = r#"insert into "t" ("b", "a") select "a", "b" from "t"
        where "a" = 1 and "b" = 2 or "a" = 3 and "b" = 4"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

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
    virtual_table.set_alias("\"t\"").unwrap();

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
            EncodedValue::String(format!("Execute query on a bucket [{bucket1}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {}",
                    r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                    r#"SELECT COL_0, COL_1, bucket_id (coalesce (CAST (COL_1 as string), ?) || coalesce (CAST (COL_0 as string), ?)) FROM"#,
                    r#"(SELECT CAST ("t"."a" as unsigned) as COL_0, CAST ("t"."b" as unsigned) as COL_1 FROM"#,
                    r#"((SELECT "a","b" FROM "TMP_test_155") as "t"))"#,
                ),
                vec![Value::from(""), Value::from("")],
            ))),
        ],
        vec![
            EncodedValue::String(format!("Execute query on a bucket [{bucket2}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {}",
                    r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                    r#"SELECT COL_0, COL_1, bucket_id (coalesce (CAST (COL_1 as string), ?) || coalesce (CAST (COL_0 as string), ?)) FROM"#,
                    r#"(SELECT CAST ("t"."a" as unsigned) as COL_0, CAST ("t"."b" as unsigned) as COL_1 FROM"#,
                    r#"((SELECT "a","b" FROM "TMP_test_155") as "t"))"#,
                ),
                vec![Value::from(""), Value::from("")],
            ))),
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
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                r#"SELECT COL_0, COL_1, bucket_id (coalesce (CAST (COL_1 as string), ?) || coalesce (CAST (COL_0 as string), ?)) FROM"#,
                r#"(SELECT CAST ("t"."b" as unsigned) as COL_0, CAST ("t"."a" as unsigned) as COL_1 FROM"#,
                r#"(SELECT "t"."b", "t"."a" FROM "t" WHERE ("t"."a") = (?) and ("t"."b") = (?)))"#,
            ),
            vec![Value::from(""), Value::from(""), Value::from(1_u64), Value::from(2_u64)],
        ))),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn insert5_test() {
    let sql = r#"insert into "t" ("b", "a") select 5, 6 from "t"
        where "a" = 1 and "b" = 2"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

    let mut virtual_table = VirtualTable::new();
    virtual_table.add_column(Column {
        name: "COL_1".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_column(Column {
        name: "COL_2".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    virtual_table.add_tuple(vec![Value::from(5_u64), Value::from(6_u64)]);
    virtual_table.add_tuple(vec![Value::from(5_u64), Value::from(6_u64)]);
    virtual_table.set_alias("\"t\"").unwrap();

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
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"INSERT INTO "t" ("b", "a", "bucket_id")"#,
                r#"SELECT COL_0, COL_1, bucket_id (coalesce (CAST (COL_1 as string), ?) || coalesce (CAST (COL_0 as string), ?)) FROM"#,
                r#"(SELECT CAST ("COL_1" as unsigned) as COL_0, CAST ("COL_2" as unsigned) as COL_1 FROM"#,
                r#"((SELECT "COL_1","COL_2" FROM "TMP_test_125") as "t"))"#,
            ),
            vec![Value::from(""), Value::from("")],
        ))),
    ]]);
    assert_eq!(expected, result);
}

#[test]
fn insert6_test() {
    let sql = r#"insert into "t" ("a", "b") values (1, 2), (1, 2), (3, 4)"#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

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
    virtual_table.set_alias("\"t\"").unwrap();

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
            EncodedValue::String(format!("Execute query on a bucket [{bucket1}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {}",
                    r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                    r#"SELECT COL_0, COL_1, bucket_id (coalesce (CAST (COL_0 as string), ?) || coalesce (CAST (COL_1 as string), ?)) FROM"#,
                    r#"(SELECT CAST (COLUMN_5 as unsigned) as COL_0, CAST (COLUMN_6 as unsigned) as COL_1 FROM"#,
                    r#"((SELECT "COLUMN_5","COLUMN_6" FROM "TMP_test_94") as "t"))"#,
                ),
                vec![Value::from(""), Value::from("")],
            ))),
        ],
        vec![
            EncodedValue::String(format!("Execute query on a bucket [{bucket2}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {}",
                    r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                    r#"SELECT COL_0, COL_1, bucket_id (coalesce (CAST (COL_0 as string), ?) || coalesce (CAST (COL_1 as string), ?)) FROM"#,
                    r#"(SELECT CAST (COLUMN_5 as unsigned) as COL_0, CAST (COLUMN_6 as unsigned) as COL_1 FROM"#,
                    r#"((SELECT "COLUMN_5","COLUMN_6" FROM "TMP_test_94") as "t"))"#,
                ),
                vec![Value::from(""), Value::from("")],
            ))),
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
        SbroadError::FailedTo(
            Action::Insert,
            Some(Entity::Column),
            format!("system column {} cannot be inserted", "\"bucket_id\""),
        ),
        result
    );
}

#[test]
fn insert8_test() {
    let sql = r#"insert into "hash_testing" select * from "hash_single_testing""#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

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
    virtual_table.set_alias("\"hash_single_testing\"").unwrap();

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
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(
            String::from(
                PatternWithParams::new(
                    format!(
                    "{} {} {} {} {}",
                    r#"INSERT INTO "hash_testing" ("identification_number", "product_code", "product_units", "sys_op", "bucket_id")"#,
                    r#"SELECT COL_0, COL_1, COL_2, COL_3, bucket_id (coalesce (CAST (COL_0 as string), ?) || coalesce (CAST (COL_1 as string), ?)) FROM"#,
                    r#"(SELECT CAST ("hash_single_testing"."identification_number" as int) as COL_0, CAST ("hash_single_testing"."product_code" as string) as COL_1,"#,
                    r#"CAST ("hash_single_testing"."product_units" as bool) as COL_2, CAST ("hash_single_testing"."sys_op" as unsigned) as COL_3 FROM"#,
                    r#"((SELECT "identification_number","product_code","product_units","sys_op" FROM "TMP_test_114") as "hash_single_testing"))"#,
                    ), vec![Value::from(""), Value::from("")],
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
    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();

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
    virtual_table.set_alias("\"t\"").unwrap();

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
        EncodedValue::String(format!("Execute query on a bucket [{bucket1}]")),
        EncodedValue::String(String::from(PatternWithParams::new(
            format!(
                "{} {} {}",
                r#"INSERT INTO "t" ("a", "b", "bucket_id")"#,
                r#"SELECT COL_0, COL_1, bucket_id (coalesce (CAST (COL_0 as string), ?) || coalesce (CAST (COL_1 as string), ?)) FROM"#,
                r#"(SELECT CAST (COLUMN_1 as unsigned) as COL_0, CAST (COLUMN_2 as unsigned) as COL_1 FROM ((SELECT "COLUMN_1","COLUMN_2" FROM "TMP_test_82") as "t"))"#,
            ),
            vec![Value::from(""), Value::from("")],
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
        EncodedValue::String("Execute query on all buckets".to_string()),
        EncodedValue::String(String::from(PatternWithParams::new(
            pattern.to_string(),
            params,
        ))),
    ]);
    assert_eq!(expected, result);
}

#[test]
fn groupby_linker_test() {
    let sql = r#"SELECT t1."id" as "ii" FROM "test_space" as t1 group by t1."id""#;

    let coordinator = RouterRuntimeMock::new();

    let mut query = Query::new(&coordinator, sql, vec![]).unwrap();

    let motion_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(0)
        .unwrap();
    let top_id = query.exec_plan.get_motion_subtree_root(motion_id).unwrap();
    if Buckets::All != query.bucket_discovery(top_id).unwrap() {
        panic!("Expected Buckets::All for local groupby")
    };
    let mut virtual_t1 = VirtualTable::new();
    virtual_t1.add_column(Column {
        name: "id".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });

    let mut buckets: Vec<u64> = vec![];
    let tuples: Vec<Vec<Value>> = vec![vec![Value::from(1_u64)], vec![Value::from(2_u64)]];

    for tuple in tuples.iter() {
        virtual_t1.add_tuple(tuple.clone());
        let mut ref_tuple: Vec<&Value> = Vec::with_capacity(tuple.len());
        for v in tuple.iter() {
            ref_tuple.push(v);
        }
        buckets.push(coordinator.determine_bucket_id(&ref_tuple));
    }

    virtual_t1.set_alias("").unwrap();

    query.coordinator.add_virtual_table(motion_id, virtual_t1);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();
    for buc in buckets {
        expected.rows.extend(vec![vec![
            EncodedValue::String(format!("Execute query on a bucket [{buc}]")),
            EncodedValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {}",
                    r#"SELECT "id" as "ii" FROM (SELECT"#,
                    r#""id" FROM "TMP_test_56")"#,
                    r#"GROUP BY "T1"."id""#,
                ),
                vec![],
            ))),
        ]]);
    }

    expected.rows.sort_by_key(|k| k[0].to_string());
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
