use pretty_assertions::assert_eq;

use crate::backend::sql::ir::PatternWithParams;

use crate::executor::engine::mock::RouterRuntimeMock;
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::ir::operator::Relational;
use crate::ir::tests::column_integer_user_non_null;
use crate::ir::transformation::redistribution::MotionPolicy;

use crate::ir::value::{LuaValue, Value};

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
    let bucket = coordinator.determine_bucket_id(&[&param1]).unwrap();
    expected.rows.push(vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket}]")),
        LuaValue::String(String::from(PatternWithParams::new(
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
    let bucket = query.coordinator.determine_bucket_id(&[&param1]).unwrap();
    expected.rows.push(vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket}]")),
        LuaValue::String(String::from(PatternWithParams::new(
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

    let bucket = query
        .coordinator
        .determine_bucket_id(&[&param1, &param457])
        .unwrap();

    expected.rows.push(vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket}]")),
        LuaValue::String(
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
    let mut virtual_table = virtual_table_23(None);
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
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
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]).unwrap();

    let param3 = Value::from(3_u64);
    let bucket3 = query.coordinator.determine_bucket_id(&[&param3]).unwrap();

    expected.rows.extend(vec![
        vec![
            LuaValue::String(format!("Execute query on a bucket [{bucket3}]")),
            LuaValue::String(
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
            LuaValue::String(format!("Execute query on a bucket [{bucket2}]")),
            LuaValue::String(
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
    let mut virtual_table = virtual_table_23(None);
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
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
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]).unwrap();

    let param3 = Value::from(3_u64);
    let bucket3 = query.coordinator.determine_bucket_id(&[&param3]).unwrap();

    expected.rows.extend(vec![
        vec![
            LuaValue::String(format!("Execute query on a bucket [{bucket3}]")),
            LuaValue::String(String::from(PatternWithParams::new(
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
            LuaValue::String(format!("Execute query on a bucket [{bucket2}]")),
            LuaValue::String(String::from(PatternWithParams::new(
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
    let mut virtual_table = virtual_table_23(Some("\"t8\""));
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
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
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]).unwrap();

    expected.rows.extend(vec![vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket2}]")),
        LuaValue::String(String::from(PatternWithParams::new(
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
                r#"(SELECT "identification_number" FROM "TMP_test_275""#,
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
    virtual_table.add_column(column_integer_user_non_null(String::from("id1")));
    virtual_table.add_column(column_integer_user_non_null(String::from("id2")));
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(1_u64)]);
    virtual_table.add_tuple(vec![Value::from(2_u64), Value::from(2_u64)]);
    virtual_table.set_alias("\"t2\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
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
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]).unwrap();

    expected.rows.extend(vec![vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket1}]")),
        LuaValue::String(String::from(PatternWithParams::new(
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
    virtual_table.add_column(column_integer_user_non_null(String::from("id1")));
    virtual_table.add_column(column_integer_user_non_null(String::from("FIRST_NAME")));
    virtual_table.add_tuple(vec![Value::from(1_u64), Value::from(1_u64)]);
    virtual_table.add_tuple(vec![Value::from(2_u64), Value::from(2_u64)]);
    virtual_table.set_alias("\"t2\"").unwrap();
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion_id)
    {
        virtual_table.reshard(key, &query.coordinator).unwrap();
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
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]).unwrap();

    expected.rows.extend(vec![vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket1}]")),
        LuaValue::String(String::from(PatternWithParams::new(
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
    virtual_t2.add_column(column_integer_user_non_null(String::from("r_id")));
    virtual_t2.add_tuple(vec![Value::from(1_u64)]);
    virtual_t2.add_tuple(vec![Value::from(2_u64)]);
    virtual_t2.set_alias("\"T2\"").unwrap();
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_t2_id)
    {
        virtual_t2.reshard(key, &query.coordinator).unwrap();
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
    virtual_sq.add_column(column_integer_user_non_null(String::from("fn")));
    virtual_sq.add_tuple(vec![Value::from(2_u64)]);
    virtual_sq.add_tuple(vec![Value::from(3_u64)]);
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_sq_id)
    {
        virtual_sq.reshard(key, &query.coordinator).unwrap();
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
    let bucket1 = query.coordinator.determine_bucket_id(&[&param1]).unwrap();

    let param2 = Value::from(2_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]).unwrap();

    expected.rows.extend(vec![
        vec![
            LuaValue::String(format!("Execute query on a bucket [{bucket2}]")),
            LuaValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {}",
                    r#"SELECT "T1"."id" FROM (SELECT"#,
                    r#""T1"."id", "T1"."sysFrom", "T1"."FIRST_NAME", "T1"."sys_op""#,
                    r#"FROM "test_space" as "T1") as "T1""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT "r_id" FROM "TMP_test_148") as "T2""#,
                    r#"ON ("T1"."id") = ("T2"."r_id")"#,
                    r#"and ("T1"."FIRST_NAME") = (SELECT "fn" FROM "TMP_test_152")"#,
                ),
                vec![],
            ))),
        ],
        vec![
            LuaValue::String(format!("Execute query on a bucket [{bucket1}]")),
            LuaValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {}",
                    r#"SELECT "T1"."id" FROM (SELECT"#,
                    r#""T1"."id", "T1"."sysFrom", "T1"."FIRST_NAME", "T1"."sys_op""#,
                    r#"FROM "test_space" as "T1") as "T1""#,
                    r#"INNER JOIN"#,
                    r#"(SELECT "r_id" FROM "TMP_test_148") as "T2""#,
                    r#"ON ("T1"."id") = ("T2"."r_id")"#,
                    r#"and ("T1"."FIRST_NAME") = (SELECT "fn" FROM "TMP_test_152")"#,
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
    virtual_t2.add_column(column_integer_user_non_null(String::from("b")));
    virtual_t2.set_alias("\"t3\"").unwrap();
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_t2_id)
    {
        virtual_t2.reshard(key, &query.coordinator).unwrap();
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
    virtual_sq.add_column(column_integer_user_non_null(String::from("f")));
    virtual_sq.add_column(column_integer_user_non_null(String::from("B")));
    virtual_sq.set_alias("Q").unwrap();
    if let MotionPolicy::Segment(key) =
        get_motion_policy(query.exec_plan.get_ir_plan(), motion_sq_id)
    {
        virtual_sq.reshard(key, &query.coordinator).unwrap();
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
        LuaValue::String("Execute query on all buckets".to_string()),
        LuaValue::String(String::from(PatternWithParams::new(
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
    let mut virtual_t1 = virtual_table_23(None);
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion1_id)
    {
        virtual_t1.reshard(key, &query.coordinator).unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion1_id, virtual_table_23(None));
    let motion2_id = *query
        .exec_plan
        .get_ir_plan()
        .clone_slices()
        .slice(0)
        .unwrap()
        .position(1)
        .unwrap();
    let mut virtual_t2 = virtual_table_23(None);
    if let MotionPolicy::Segment(key) = get_motion_policy(query.exec_plan.get_ir_plan(), motion2_id)
    {
        virtual_t2.reshard(key, &query.coordinator).unwrap();
    }
    query
        .coordinator
        .add_virtual_table(motion2_id, virtual_table_23(None));

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();
    let param2 = Value::from(2_u64);
    let bucket2 = query.coordinator.determine_bucket_id(&[&param2]).unwrap();

    let param3 = Value::from(3_u64);
    let bucket3 = query.coordinator.determine_bucket_id(&[&param3]).unwrap();
    expected.rows.extend(vec![
        vec![
            LuaValue::String(format!("Execute query on a bucket [{bucket3}]")),
            LuaValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {} {} {} {}",
                    "SELECT",
                    r#""test_space"."id","#,
                    r#""test_space"."sysFrom","#,
                    r#""test_space"."FIRST_NAME","#,
                    r#""test_space"."sys_op""#,
                    r#"FROM "test_space""#,
                    r#"WHERE ("test_space"."id") in"#,
                    r#"(SELECT "identification_number" FROM "TMP_test_130")"#,
                    r#"or ("test_space"."id") in"#,
                    r#"(SELECT "identification_number" FROM "TMP_test_134")"#,
                ),
                vec![],
            ))),
        ],
        vec![
            LuaValue::String(format!("Execute query on a bucket [{bucket2}]")),
            LuaValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {} {} {} {} {} {} {} {}",
                    "SELECT",
                    r#""test_space"."id","#,
                    r#""test_space"."sysFrom","#,
                    r#""test_space"."FIRST_NAME","#,
                    r#""test_space"."sys_op""#,
                    r#"FROM "test_space""#,
                    r#"WHERE ("test_space"."id") in"#,
                    r#"(SELECT "identification_number" FROM "TMP_test_130")"#,
                    r#"or ("test_space"."id") in"#,
                    r#"(SELECT "identification_number" FROM "TMP_test_134")"#,
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
    let bucket = query.coordinator.determine_bucket_id(&[&param1]).unwrap();
    expected.rows.push(vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket}]")),
        LuaValue::String(String::from(PatternWithParams::new(
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
    let bucket = query.coordinator.determine_bucket_id(&[&param1]).unwrap();
    expected.rows.push(vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket}]")),
        LuaValue::String(String::from(PatternWithParams::new(
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

/// Helper function to create a test virtual table.
/// Called `23` because it contains Integer values [2, 3] for `identification_number` column.
fn virtual_table_23(alias: Option<&str>) -> VirtualTable {
    let mut virtual_table = VirtualTable::new();

    virtual_table.add_column(column_integer_user_non_null(String::from(
        "identification_number",
    )));

    virtual_table.add_tuple(vec![Value::from(2_u64)]);
    virtual_table.add_tuple(vec![Value::from(3_u64)]);

    if let Some(alias) = alias {
        virtual_table.set_alias(alias).unwrap();
    }

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
        LuaValue::String("Execute query on all buckets".to_string()),
        LuaValue::String(String::from(PatternWithParams::new(
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
    assert!(
        !(Buckets::All != query.bucket_discovery(top_id).unwrap()),
        "Expected Buckets::All for local groupby"
    );
    let mut virtual_t1 = VirtualTable::new();
    virtual_t1.add_column(column_integer_user_non_null(String::from("id")));

    let mut buckets: Vec<u64> = vec![];
    let tuples: Vec<Vec<Value>> = vec![vec![Value::from(1_u64)], vec![Value::from(2_u64)]];

    for tuple in &tuples {
        virtual_t1.add_tuple(tuple.clone());
        let mut ref_tuple: Vec<&Value> = Vec::with_capacity(tuple.len());
        for v in tuple.iter() {
            ref_tuple.push(v);
        }
        let bucket_id = coordinator.determine_bucket_id(&ref_tuple).unwrap();
        buckets.push(bucket_id);
    }

    query.coordinator.add_virtual_table(motion_id, virtual_t1);

    let result = *query
        .dispatch()
        .unwrap()
        .downcast::<ProducerResult>()
        .unwrap();

    let mut expected = ProducerResult::new();
    for buc in buckets {
        expected.rows.extend(vec![vec![
            LuaValue::String(format!("Execute query on a bucket [{buc}]")),
            LuaValue::String(String::from(PatternWithParams::new(
                format!(
                    "{} {} {}",
                    r#"SELECT "column_12" as "ii" FROM"#,
                    r#"(SELECT "id" FROM "TMP_test_45")"#,
                    r#"GROUP BY "column_12""#,
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
mod exec_plan;
