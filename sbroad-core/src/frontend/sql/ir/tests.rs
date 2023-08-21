use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use crate::ir::value::Value;
use pretty_assertions::assert_eq;

#[test]
fn front_sql1() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."identification_number"::integer) = ROW(1::unsigned)
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql2() {
    let input = r#"SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1 AND "product_code" = '1'
        OR "identification_number" = 2 AND "product_code" = '2'"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."identification_number"::integer) = ROW(1::unsigned) and ROW("hash_testing"."product_code"::string) = ROW('1'::string) or ROW("hash_testing"."identification_number"::integer) = ROW(2::unsigned) and ROW("hash_testing"."product_code"::string) = ROW('2'::string)
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql3() {
    let input = r#"SELECT *
        FROM
            (SELECT "identification_number", "product_code"
            FROM "hash_testing"
            WHERE "sys_op" = 1
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 1) AS "t3"
        WHERE "identification_number" = 1"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t3"."identification_number"::integer -> "identification_number", "t3"."product_code"::string -> "product_code")
    selection ROW("t3"."identification_number"::integer) = ROW(1::unsigned)
        scan "t3"
            union all
                projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                    selection ROW("hash_testing"."sys_op"::unsigned) = ROW(1::unsigned)
                        scan "hash_testing"
                projection ("hash_testing_hist"."identification_number"::integer -> "identification_number", "hash_testing_hist"."product_code"::string -> "product_code")
                    selection ROW("hash_testing_hist"."sys_op"::unsigned) > ROW(1::unsigned)
                        scan "hash_testing_hist"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql4() {
    let input = r#"SELECT *
        FROM
            (SELECT "identification_number", "product_code"
            FROM "hash_testing"
            WHERE "sys_op" = 1
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 1) AS "t3"
        WHERE ("identification_number" = 1
            OR ("identification_number" = 2
            OR "identification_number" = 3))
            AND ("product_code" = '1'
            OR "product_code" = '2')"#;
    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t3"."identification_number"::integer -> "identification_number", "t3"."product_code"::string -> "product_code")
    selection ROW("t3"."identification_number"::integer) = ROW(1::unsigned) or ROW("t3"."identification_number"::integer) = ROW(2::unsigned) or ROW("t3"."identification_number"::integer) = ROW(3::unsigned) and ROW("t3"."product_code"::string) = ROW('1'::string) or ROW("t3"."product_code"::string) = ROW('2'::string)
        scan "t3"
            union all
                projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
                    selection ROW("hash_testing"."sys_op"::unsigned) = ROW(1::unsigned)
                        scan "hash_testing"
                projection ("hash_testing_hist"."identification_number"::integer -> "identification_number", "hash_testing_hist"."product_code"::string -> "product_code")
                    selection ROW("hash_testing_hist"."sys_op"::unsigned) > ROW(1::unsigned)
                        scan "hash_testing_hist"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql5() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" in (
        SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'a')"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."identification_number"::integer) in ROW($0)
        scan "hash_testing"
subquery $0:
motion [policy: full]
            scan
                projection ("hash_testing_hist"."identification_number"::integer -> "identification_number")
                    selection ROW("hash_testing_hist"."product_code"::string) = ROW('a'::string)
                        scan "hash_testing_hist"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql6() {
    let input = r#"SELECT "id", "product_units" FROM "hash_testing"
        INNER JOIN (SELECT "id" FROM "test_space") as t
        ON "hash_testing"."identification_number" = t."id"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("T"."id"::unsigned -> "id", "hash_testing"."product_units"::boolean -> "product_units")
    selection ROW("hash_testing"."identification_number"::integer) = ROW(5::unsigned) and ROW("hash_testing"."product_code"::string) = ROW('123'::string)
        join on ROW("hash_testing"."identification_number"::integer) = ROW("T"."id"::unsigned)
            scan "hash_testing"
                projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
                    scan "hash_testing"
            motion [policy: full]
                scan "T"
                    projection ("test_space"."id"::unsigned -> "id")
                        scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql8() {
    let input = r#"SELECT t."identification_number", "product_code" FROM "hash_testing" as t
        WHERE t."identification_number" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("T"."identification_number"::integer -> "identification_number", "T"."product_code"::string -> "product_code")
    selection ROW("T"."identification_number"::integer) = ROW(1::unsigned)
        scan "hash_testing" -> "T"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql9() {
    let input = r#"SELECT *
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
            (SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 0
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_single_testing_hist"
            WHERE "sys_op" <= 0) AS "t8"
            ON "t3"."id" = "t8"."identification_number"
        WHERE "id" = 1 AND "t8"."identification_number" = 1 AND "product_code" = '123'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t3"."id"::unsigned -> "id", "t3"."FIRST_NAME"::string -> "FIRST_NAME", "t8"."identification_number"::integer -> "identification_number", "t8"."product_code"::string -> "product_code")
    selection ROW("t3"."id"::unsigned) = ROW(1::unsigned) and ROW("t8"."identification_number"::integer) = ROW(1::unsigned) and ROW("t8"."product_code"::string) = ROW('123'::string)
        join on ROW("t3"."id"::unsigned) = ROW("t8"."identification_number"::integer)
            scan "t3"
                union all
                    projection ("test_space"."id"::unsigned -> "id", "test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection ROW("test_space"."sys_op"::unsigned) < ROW(0::unsigned) and ROW("test_space"."sysFrom"::unsigned) >= ROW(0::unsigned)
                            scan "test_space"
                    projection ("test_space_hist"."id"::unsigned -> "id", "test_space_hist"."FIRST_NAME"::string -> "FIRST_NAME")
                        selection ROW("test_space_hist"."sysFrom"::unsigned) <= ROW(0::unsigned)
                            scan "test_space_hist"
            motion [policy: segment([ref("identification_number")])]
                scan "t8"
                    union all
                        projection ("hash_testing_hist"."identification_number"::integer -> "identification_number", "hash_testing_hist"."product_code"::string -> "product_code")
                            selection ROW("hash_testing_hist"."sys_op"::unsigned) > ROW(0::unsigned)
                                scan "hash_testing_hist"
                        projection ("hash_single_testing_hist"."identification_number"::integer -> "identification_number", "hash_single_testing_hist"."product_code"::string -> "product_code")
                            selection ROW("hash_single_testing_hist"."sys_op"::unsigned) <= ROW(0::unsigned)
                                scan "hash_single_testing_hist"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql10() {
    let input = r#"INSERT INTO "t" VALUES(1, 2, 3, 4)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: local segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW(1::unsigned, 2::unsigned, 3::unsigned, 4::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql11() {
    let input = r#"INSERT INTO "t" ("a", "c") VALUES(1, 2)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: local segment([ref("COLUMN_1"), value(NULL)])]
        values
            value row (data=ROW(1::unsigned, 2::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql14() {
    let input = r#"INSERT INTO "t" ("a", "c") SELECT "b", "d" FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("b"), value(NULL)])]
        projection ("t"."b"::unsigned -> "b", "t"."d"::unsigned -> "d")
            scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

// check cyrillic strings support
#[test]
fn front_sql16() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "product_code" = 'кириллица'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."product_code"::string) = ROW('кириллица'::string)
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql17() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" IS NULL"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number")
    selection ROW("hash_testing"."product_code"::string) is null
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql18() {
    let input = r#"SELECT "product_code" FROM "hash_testing"
        WHERE "product_code" BETWEEN 1 AND 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."product_code"::string -> "product_code")
    selection ROW("hash_testing"."product_code"::string) >= ROW(1::unsigned) and ROW("hash_testing"."product_code"::string) <= ROW(2::unsigned)
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql19() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" IS NOT NULL"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number")
    selection ROW("hash_testing"."product_code"::string) is not null
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

// Check double angle quotation marks in the strings
#[test]
fn front_sql20() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" = '«123»'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number")
    selection ROW("hash_testing"."product_code"::string) = ROW('«123»'::string)
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_exists_subquery_select_from_table() {
    let input = r#"SELECT "id" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection exists ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection (0::unsigned -> "COL_1")
                    scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_not_exists_subquery_select_from_table() {
    let input = r#"SELECT "id" FROM "test_space" WHERE NOT EXISTS (SELECT 0 FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection not exists ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection (0::unsigned -> "COL_1")
                    scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_exists_subquery_select_from_table_with_condition() {
    let input = r#"SELECT "id" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing" WHERE "identification_number" != 42)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection exists ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection (0::unsigned -> "COL_1")
                    selection ROW("hash_testing"."identification_number"::integer) <> ROW(42::unsigned)
                        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing" group by "identification_number", "product_code""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_12"::integer -> "identification_number", "column_13"::string -> "product_code")
    group by ("column_13"::string, "column_12"::integer) output: ("column_12"::integer -> "column_12", "column_13"::string -> "column_13")
        motion [policy: segment([ref("column_13"), ref("column_12")])]
            scan
                projection ("hash_testing"."identification_number"::integer -> "column_12", "hash_testing"."product_code"::string -> "column_13")
                    group by ("hash_testing"."product_code"::string, "hash_testing"."identification_number"::integer) output: ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op", "hash_testing"."bucket_id"::unsigned -> "bucket_id")
                        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_less_cols_in_proj() {
    // check case when we specify less columns than in groupby clause
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        GROUP BY "identification_number", "product_units"
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_12"::integer -> "identification_number")
    group by ("column_12"::integer, "column_13"::boolean) output: ("column_12"::integer -> "column_12", "column_13"::boolean -> "column_13")
        motion [policy: segment([ref("column_12"), ref("column_13")])]
            scan
                projection ("hash_testing"."identification_number"::integer -> "column_12", "hash_testing"."product_units"::boolean -> "column_13")
                    group by ("hash_testing"."identification_number"::integer, "hash_testing"."product_units"::boolean) output: ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op", "hash_testing"."bucket_id"::unsigned -> "bucket_id")
                        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_union_1() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        GROUP BY "identification_number"
        UNION ALL
        SELECT "identification_number" FROM "hash_testing""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("column_12"::integer -> "identification_number")
        group by ("column_12"::integer) output: ("column_12"::integer -> "column_12")
            motion [policy: segment([ref("column_12")])]
                scan
                    projection ("hash_testing"."identification_number"::integer -> "column_12")
                        group by ("hash_testing"."identification_number"::integer) output: ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op", "hash_testing"."bucket_id"::unsigned -> "bucket_id")
                            scan "hash_testing"
    projection ("hash_testing"."identification_number"::integer -> "identification_number")
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_union_2() {
    let input = r#"SELECT "identification_number" FROM "hash_testing" UNION ALL
        SELECT * FROM (SELECT "identification_number" FROM "hash_testing"
        GROUP BY "identification_number"
        UNION ALL
        SELECT "identification_number" FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("hash_testing"."identification_number"::integer -> "identification_number")
        scan "hash_testing"
    projection ("identification_number"::integer -> "identification_number")
        scan
            union all
                projection ("column_28"::integer -> "identification_number")
                    group by ("column_28"::integer) output: ("column_28"::integer -> "column_28")
                        motion [policy: segment([ref("column_28")])]
                            scan
                                projection ("hash_testing"."identification_number"::integer -> "column_28")
                                    group by ("hash_testing"."identification_number"::integer) output: ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op", "hash_testing"."bucket_id"::unsigned -> "bucket_id")
                                        scan "hash_testing"
                projection ("hash_testing"."identification_number"::integer -> "identification_number")
                    scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_join_1() {
    // inner select is a kostyl because tables have the col sys_op
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_testing") as t2
        INNER JOIN (SELECT "id" from "test_space") as t
        ON t2."identification_number" = t."id"
        group by t2."product_code", t2."product_units"
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_63"::string -> "product_code", "column_64"::boolean -> "product_units")
    group by ("column_63"::string, "column_64"::boolean) output: ("column_64"::boolean -> "column_64", "column_63"::string -> "column_63")
        motion [policy: segment([ref("column_63"), ref("column_64")])]
            scan
                projection ("T2"."product_units"::boolean -> "column_64", "T2"."product_code"::string -> "column_63")
                    group by ("T2"."product_code"::string, "T2"."product_units"::boolean) output: ("T2"."product_units"::boolean -> "product_units", "T2"."product_code"::string -> "product_code", "T2"."identification_number"::integer -> "identification_number", "T"."id"::unsigned -> "id")
                        join on ROW("T2"."identification_number"::integer) = ROW("T"."id"::unsigned)
                            scan "T2"
                                projection ("hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."identification_number"::integer -> "identification_number")
                                    scan "hash_testing"
                            motion [policy: full]
                                scan "T"
                                    projection ("test_space"."id"::unsigned -> "id")
                                        scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_join() {
    // test we can have not null and bool kind of condition in join
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_testing") as t2
        INNER JOIN (SELECT "id" from "test_space") as t
        ON t2."identification_number" = t."id" and t."id" is not null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("T2"."product_code"::string -> "product_code", "T2"."product_units"::boolean -> "product_units")
    join on ROW("T2"."identification_number"::integer) = ROW("T"."id"::unsigned) and ROW("T"."id"::unsigned) is not null
        scan "T2"
            projection ("hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."identification_number"::integer -> "identification_number")
                scan "hash_testing"
        motion [policy: full]
            scan "T"
                projection ("test_space"."id"::unsigned -> "id")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    // here hash_single_testing is sharded by "identification_number", so it is a local join
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_single_testing") as t1
        INNER JOIN (SELECT "id" from "test_space") as t2
        ON t1."identification_number" = t2."id" and t2."id" is not null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("T1"."product_code"::string -> "product_code", "T1"."product_units"::boolean -> "product_units")
    join on ROW("T1"."identification_number"::integer) = ROW("T2"."id"::unsigned) and ROW("T2"."id"::unsigned) is not null
        scan "T1"
            projection ("hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."identification_number"::integer -> "identification_number")
                scan "hash_single_testing"
        scan "T2"
            projection ("test_space"."id"::unsigned -> "id")
                scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    // check we have no error, in case one of the join children has Distribution::Single
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_single_testing") as t1
        INNER JOIN (SELECT sum("id") as "id" from "test_space") as t2
        ON t1."identification_number" = t2."id" and t2."id" is not null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // TODO: For the  hash function in the cartrisge runtime we can apply
    //       `motion [policy: segment([ref("id")])]` instead of the `motion [policy: full]`.
    let expected_explain = String::from(
        r#"projection ("T1"."product_code"::string -> "product_code", "T1"."product_units"::boolean -> "product_units")
    join on ROW("T1"."identification_number"::integer) = ROW("T2"."id"::decimal) and ROW("T2"."id"::decimal) is not null
        scan "T1"
            projection ("hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."identification_number"::integer -> "identification_number")
                scan "hash_single_testing"
        motion [policy: full]
            scan "T2"
                projection (sum(("sum_41"::decimal))::decimal -> "id")
                    motion [policy: full]
                        scan
                            projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_41")
                                scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_insert() {
    let input = r#"INSERT INTO "t" ("a", "c") 
    SELECT "b", "d" FROM "t" group by "b", "d" ON CONFLICT DO FAIL"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("b"), value(NULL)])]
        projection ("column_24"::unsigned -> "b", "column_25"::unsigned -> "d")
            group by ("column_24"::unsigned, "column_25"::unsigned) output: ("column_25"::unsigned -> "column_25", "column_24"::unsigned -> "column_24")
                motion [policy: segment([ref("column_24"), ref("column_25")])]
                    scan
                        projection ("t"."d"::unsigned -> "column_25", "t"."b"::unsigned -> "column_24")
                            group by ("t"."b"::unsigned, "t"."d"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_invalid() {
    let input = r#"select "b", "a" from "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    let res = plan.optimize();

    assert_eq!(true, res.is_err());
}

#[test]
fn front_sql_distinct_invalid() {
    let input = r#"select "b", bucket_id(distinct cast("a" as string)) from "t" group by "b", bucket_id(distinct cast("a" as string))"#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let plan = ast.resolve_metadata(metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("DISTINCT modifier is allowed only for aggregate functions")
    );
}

#[test]
fn front_sql_aggregates() {
    let input = r#"SELECT "b", count("a") + count("b") FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_12"::unsigned -> "b", (sum(("count_28"::integer))::decimal) + (sum(("count_30"::integer))::decimal) -> "COL_1")
    group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "count_28"::integer -> "count_28", "count_30"::integer -> "count_30")
        motion [policy: segment([ref("column_12")])]
            scan
                projection ("t"."b"::unsigned -> "column_12", count(("t"."a"::unsigned))::integer -> "count_28", count(("t"."b"::unsigned))::integer -> "count_30")
                    group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_avg_aggregate() {
    let input = r#"SELECT avg("b"), avg(distinct "b"), avg("b") * avg("b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ((sum(("sum_13"::decimal::double))::decimal / sum(("count_13"::integer::double))::decimal) -> "COL_1", avg(distinct ("column_15"::decimal::double))::decimal -> "COL_2", ((sum(("sum_13"::decimal::double))::decimal / sum(("count_13"::integer::double))::decimal)) * ((sum(("sum_13"::decimal::double))::decimal / sum(("count_13"::integer::double))::decimal)) -> "COL_3")
    motion [policy: full]
        scan
            projection ("t"."b"::unsigned -> "column_15", sum(("t"."b"::unsigned))::decimal -> "sum_13", count(("t"."b"::unsigned))::integer -> "count_13")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_total_aggregate() {
    let input = r#"SELECT total("b"), total(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (total(("total_13"::double))::double -> "COL_1", total(distinct ("column_15"::double))::double -> "COL_2")
    motion [policy: full]
        scan
            projection ("t"."b"::unsigned -> "column_15", total(("t"."b"::unsigned))::double -> "total_13")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_min_aggregate() {
    let input = r#"SELECT min("b"), min(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (min(("min_13"::scalar))::scalar -> "COL_1", min(distinct ("column_15"::scalar))::scalar -> "COL_2")
    motion [policy: full]
        scan
            projection ("t"."b"::unsigned -> "column_15", min(("t"."b"::unsigned))::scalar -> "min_13")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_max_aggregate() {
    let input = r#"SELECT max("b"), max(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (max(("max_13"::scalar))::scalar -> "COL_1", max(distinct ("column_15"::scalar))::scalar -> "COL_2")
    motion [policy: full]
        scan
            projection ("t"."b"::unsigned -> "column_15", max(("t"."b"::unsigned))::scalar -> "max_13")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_group_concat_aggregate() {
    let input = r#"SELECT group_concat("b"), group_concat(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (group_concat(("group_concat_13"::string))::string -> "COL_1", group_concat(distinct ("column_15"::string))::string -> "COL_2")
    motion [policy: full]
        scan
            projection ("t"."b"::unsigned -> "column_15", group_concat(("t"."b"::unsigned))::string -> "group_concat_13")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_group_concat_aggregate2() {
    let input = r#"SELECT group_concat("b", ' '), group_concat(distinct "b") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (group_concat(("group_concat_14"::string, ' '::string))::string -> "COL_1", group_concat(distinct ("column_16"::string))::string -> "COL_2")
    motion [policy: full]
        scan
            projection ("t"."b"::unsigned -> "column_16", group_concat(("t"."b"::unsigned, ' '::string))::string -> "group_concat_14")
                group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_count_asterisk1() {
    let input = r#"SELECT count(*), count(*) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("count_13"::integer))::decimal -> "COL_1", sum(("count_13"::integer))::decimal -> "COL_2")
    motion [policy: full]
        scan
            projection (count((*::integer))::integer -> "count_13")
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_count_asterisk2() {
    let input = r#"SELECT cOuNt(*), "b" FROM "t" group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("count_26"::integer))::decimal -> "COL_1", "column_12"::unsigned -> "b")
    group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "count_26"::integer -> "count_26")
        motion [policy: segment([ref("column_12")])]
            scan
                projection ("t"."b"::unsigned -> "column_12", count((*::integer))::integer -> "count_26")
                    group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_invalid_count_asterisk1() {
    let input = r#"SELECT sum(*) FROM "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let plan = ast.resolve_metadata(metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("\"*\" is allowed only inside \"count\" aggregate function.")
    );
}

#[test]
fn front_sql_aggregates_with_subexpressions() {
    let input = r#"SELECT "b", count("a" * "b" + 1), count(func("a")) FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_12"::unsigned -> "b", sum(("count_35"::integer))::decimal -> "COL_1", sum(("count_39"::integer))::decimal -> "COL_2")
    group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "count_39"::integer -> "count_39", "count_35"::integer -> "count_35")
        motion [policy: segment([ref("column_12")])]
            scan
                projection ("t"."b"::unsigned -> "column_12", count(("FUNC"(("t"."a"::unsigned))::integer))::integer -> "count_39", count((("t"."a"::unsigned) * ("t"."b"::unsigned) + (1::unsigned)))::integer -> "count_35")
                    group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregates_with_distinct1() {
    let input = r#"SELECT "b", count(distinct "a"), count(distinct "b") FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_12"::unsigned -> "b", count(distinct ("column_27"::integer))::integer -> "COL_1", count(distinct ("column_12"::integer))::integer -> "COL_2")
    group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "column_27"::unsigned -> "column_27")
        motion [policy: segment([ref("column_12")])]
            scan
                projection ("t"."b"::unsigned -> "column_12", "t"."a"::unsigned -> "column_27")
                    group by ("t"."b"::unsigned, "t"."a"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregates_with_distinct2() {
    let input = r#"SELECT "b", sum(distinct "a" + "b" + 3) FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_12"::unsigned -> "b", sum(distinct ("column_34"::decimal))::decimal -> "COL_1")
    group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "column_34"::unsigned -> "column_34")
        motion [policy: segment([ref("column_12")])]
            scan
                projection ("t"."b"::unsigned -> "column_12", ("t"."a"::unsigned) + ("t"."b"::unsigned) + (3::unsigned) -> "column_34")
                    group by ("t"."b"::unsigned, ("t"."a"::unsigned) + ("t"."b"::unsigned) + (3::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregates_with_distinct3() {
    let input = r#"SELECT sum(distinct "a" + "b" + 3) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(distinct ("column_19"::decimal))::decimal -> "COL_1")
    motion [policy: full]
        scan
            projection (("t"."a"::unsigned) + ("t"."b"::unsigned) + (3::unsigned) -> "column_19")
                group by (("t"."a"::unsigned) + ("t"."b"::unsigned) + (3::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregate_inside_aggregate() {
    let input = r#"select "b", count(sum("a")) from "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let err = ast
        .resolve_metadata(metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!("invalid query: aggregate function inside aggregate function is not allowed. Got `sum` inside `count`", err.to_string());
}

#[test]
fn front_sql_column_outside_aggregate_no_groupby() {
    let input = r#"select "b", count("a") from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let err = ast
        .resolve_metadata(metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!(
        "invalid query: found column reference (\"b\") outside aggregate function",
        err.to_string()
    );
}

#[test]
fn front_sql_option_basic() {
    let input = r#"select * from "t" option(sql_vdbe_max_steps = 1000, vtable_max_rows = 10)"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
    scan "t"
execution options:
sql_vdbe_max_steps = 1000
vtable_max_rows = 10
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_option_with_param() {
    let input = r#"select * from "t" option(sql_vdbe_max_steps = ?, vtable_max_rows = ?)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(1000), Value::Unsigned(10)]);
    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
    scan "t"
execution options:
sql_vdbe_max_steps = 1000
vtable_max_rows = 10
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_option_defaults() {
    let input = r#"select * from "t" where "a" = ? and "b" = ?"#;

    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(1000), Value::Unsigned(10)]);
    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
    selection ROW("t"."a"::unsigned) = ROW(1000::unsigned) and ROW("t"."b"::unsigned) = ROW(10::unsigned)
        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_column_outside_aggregate() {
    let input = r#"select "b", "a", count("a") from "t" group by "b""#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let err = ast
        .resolve_metadata(metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!(
        "invalid query: column \"a\" is not found in grouping expressions!",
        err.to_string()
    );
}

#[test]
fn front_sql_aggregate_without_groupby() {
    let input = r#"SELECT sum("a" * "b" + 1) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("sum_20"::decimal))::decimal -> "COL_1")
    motion [policy: full]
        scan
            projection (sum((("t"."a"::unsigned) * ("t"."b"::unsigned) + (1::unsigned)))::decimal -> "sum_20")
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregate_without_groupby2() {
    let input = r#"SELECT * FROM (SELECT count("id") FROM "test_space") as "t1""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t1"."COL_1"::integer -> "COL_1")
    scan "t1"
        projection (sum(("count_13"::integer))::decimal -> "COL_1")
            motion [policy: full]
                scan
                    projection (count(("test_space"."id"::unsigned))::integer -> "count_13")
                        scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_union_single_left() {
    let input = r#"
        SELECT "a" FROM "t"
        UNION ALL
        SELECT sum("a") FROM "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("t"."a"::unsigned -> "a")
        scan "t"
    motion [policy: segment([ref("COL_1")])]
        projection (sum(("sum_29"::decimal))::decimal -> "COL_1")
            motion [policy: full]
                scan
                    projection (sum(("t"."a"::unsigned))::decimal -> "sum_29")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_union_single_right() {
    let input = r#"
        SELECT sum("a") FROM "t"
        UNION ALL
        SELECT "a" FROM "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    motion [policy: segment([ref("COL_1")])]
        projection (sum(("sum_13"::decimal))::decimal -> "COL_1")
            motion [policy: full]
                scan
                    projection (sum(("t"."a"::unsigned))::decimal -> "sum_13")
                        scan "t"
    projection ("t"."a"::unsigned -> "a")
        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_union_single_both() {
    let input = r#"
        SELECT sum("a") FROM "t"
        UNION ALL
        SELECT sum("a") FROM "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    motion [policy: segment([ref("COL_1")])]
        projection (sum(("sum_13"::decimal))::decimal -> "COL_1")
            motion [policy: full]
                scan
                    projection (sum(("t"."a"::unsigned))::decimal -> "sum_13")
                        scan "t"
    motion [policy: segment([ref("COL_1")])]
        projection (sum(("sum_30"::decimal))::decimal -> "COL_1")
            motion [policy: full]
                scan
                    projection (sum(("t"."a"::unsigned))::decimal -> "sum_30")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_single() {
    let input = r#"INSERT INTO "t" ("a", "c") SELECT sum("b"), count("d") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("COL_1"), value(NULL)])]
        projection (sum(("sum_25"::decimal))::decimal -> "COL_1", sum(("count_28"::integer))::decimal -> "COL_2")
            motion [policy: full]
                scan
                    projection (sum(("t"."b"::unsigned))::decimal -> "sum_25", count(("t"."d"::unsigned))::integer -> "count_28")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_except_single_right() {
    let input = r#"SELECT "a", "b" from "t" 
        EXCEPT
        SELECT sum("a"), count("b") from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"except
    projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
        scan "t"
    motion [policy: segment([ref("COL_1"), ref("COL_2")])]
        projection (sum(("sum_31"::decimal))::decimal -> "COL_1", sum(("count_34"::integer))::decimal -> "COL_2")
            motion [policy: full]
                scan
                    projection (sum(("t"."a"::unsigned))::decimal -> "sum_31", count(("t"."b"::unsigned))::integer -> "count_34")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());

    let input = r#"SELECT "b", "a" from "t" 
        EXCEPT
        SELECT sum("a"), count("b") from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection ("t"."b"::unsigned -> "b", "t"."a"::unsigned -> "a")
        scan "t"
    motion [policy: segment([ref("COL_2"), ref("COL_1")])]
        projection (sum(("sum_31"::decimal))::decimal -> "COL_1", sum(("count_34"::integer))::decimal -> "COL_2")
            motion [policy: full]
                scan
                    projection (sum(("t"."a"::unsigned))::decimal -> "sum_31", count(("t"."b"::unsigned))::integer -> "count_34")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_except_single_left() {
    let input = r#"SELECT sum("a"), count("b") from "t" 
        EXCEPT
        SELECT "a", "b" from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"except
    motion [policy: segment([ref("COL_1"), ref("COL_2")])]
        projection (sum(("sum_13"::decimal))::decimal -> "COL_1", sum(("count_16"::integer))::decimal -> "COL_2")
            motion [policy: full]
                scan
                    projection (sum(("t"."a"::unsigned))::decimal -> "sum_13", count(("t"."b"::unsigned))::integer -> "count_16")
                        scan "t"
    projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_except_single_both() {
    let input = r#"SELECT sum("a"), count("b") from "t" 
        EXCEPT
        SELECT sum("a"), sum("b") from "t"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"except
    motion [policy: segment([ref("COL_1")])]
        projection (sum(("sum_13"::decimal))::decimal -> "COL_1", sum(("count_16"::integer))::decimal -> "COL_2")
            motion [policy: full]
                scan
                    projection (sum(("t"."a"::unsigned))::decimal -> "sum_13", count(("t"."b"::unsigned))::integer -> "count_16")
                        scan "t"
    motion [policy: segment([ref("COL_1")])]
        projection (sum(("sum_33"::decimal))::decimal -> "COL_1", sum(("sum_36"::decimal))::decimal -> "COL_2")
            motion [policy: full]
                scan
                    projection (sum(("t"."b"::unsigned))::decimal -> "sum_36", sum(("t"."a"::unsigned))::decimal -> "sum_33")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_expression() {
    let input = r#"SELECT "a"+"b" FROM "t"
        group by "a"+"b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_16"::unsigned -> "COL_1")
    group by ("column_16"::unsigned) output: ("column_16"::unsigned -> "column_16")
        motion [policy: segment([ref("column_16")])]
            scan
                projection (("t"."a"::unsigned) + ("t"."b"::unsigned) -> "column_16")
                    group by (("t"."a"::unsigned) + ("t"."b"::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_expression2() {
    let input = r#"SELECT ("a"+"b") + count("a") FROM "t"
        group by ("a"+"b")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_16"::unsigned + (sum(("count_35"::integer))::decimal) -> "COL_1")
    group by ("column_16"::unsigned) output: ("column_16"::unsigned -> "column_16", "count_35"::integer -> "count_35")
        motion [policy: segment([ref("column_16")])]
            scan
                projection ((("t"."a"::unsigned) + ("t"."b"::unsigned)) -> "column_16", count(("t"."a"::unsigned))::integer -> "count_35")
                    group by ((("t"."a"::unsigned) + ("t"."b"::unsigned))) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_expression3() {
    let input = r#"SELECT "a"+"b", ("c"*"d")*sum("c"*"d")/count("a"*"b") FROM "t"
        group by "a"+"b", "a"+"b", ("c"*"d")"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_16"::unsigned -> "COL_1", "column_26"::unsigned * (sum(("sum_55"::decimal))::decimal) / (sum(("count_61"::integer))::decimal) -> "COL_2")
    group by ("column_16"::unsigned, "column_26"::unsigned) output: ("column_26"::unsigned -> "column_26", "column_16"::unsigned -> "column_16", "sum_55"::decimal -> "sum_55", "count_61"::integer -> "count_61")
        motion [policy: segment([ref("column_16"), ref("column_26")])]
            scan
                projection ((("t"."c"::unsigned) * ("t"."d"::unsigned)) -> "column_26", ("t"."a"::unsigned) + ("t"."b"::unsigned) -> "column_16", sum((("t"."c"::unsigned) * ("t"."d"::unsigned)))::decimal -> "sum_55", count((("t"."a"::unsigned) * ("t"."b"::unsigned)))::integer -> "count_61")
                    group by (("t"."a"::unsigned) + ("t"."b"::unsigned), (("t"."c"::unsigned) * ("t"."d"::unsigned))) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_expression4() {
    let input = r#"SELECT "a"+"b", "a" FROM "t"
        group by "a"+"b", "a""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_16"::unsigned -> "COL_1", "column_17"::unsigned -> "a")
    group by ("column_16"::unsigned, "column_17"::unsigned) output: ("column_17"::unsigned -> "column_17", "column_16"::unsigned -> "column_16")
        motion [policy: segment([ref("column_16"), ref("column_17")])]
            scan
                projection ("t"."a"::unsigned -> "column_17", ("t"."a"::unsigned) + ("t"."b"::unsigned) -> "column_16")
                    group by (("t"."a"::unsigned) + ("t"."b"::unsigned), "t"."a"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_with_aggregates() {
    let input = r#"
        select * from (select "a", "b", sum("c") as "c" from "t" group by "a", "b") as t1
        join (select "g", "e", sum("f") as "f" from "t2" group by "g", "e") as t2
        on (t1."a", t2."g") = (t2."e", t1."b")"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("T1"."a"::unsigned -> "a", "T1"."b"::unsigned -> "b", "T1"."c"::decimal -> "c", "T2"."g"::unsigned -> "g", "T2"."e"::unsigned -> "e", "T2"."f"::decimal -> "f")
    join on ROW("T1"."a"::unsigned, "T2"."g"::unsigned) = ROW("T2"."e"::unsigned, "T1"."b"::unsigned)
        scan "T1"
            projection ("column_12"::unsigned -> "a", "column_13"::unsigned -> "b", sum(("sum_31"::decimal))::decimal -> "c")
                group by ("column_13"::unsigned, "column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "column_13"::unsigned -> "column_13", "sum_31"::decimal -> "sum_31")
                    motion [policy: segment([ref("column_13"), ref("column_12")])]
                        scan
                            projection ("t"."a"::unsigned -> "column_12", "t"."b"::unsigned -> "column_13", sum(("t"."c"::unsigned))::decimal -> "sum_31")
                                group by ("t"."b"::unsigned, "t"."a"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                                    scan "t"
        motion [policy: full]
            scan "T2"
                projection ("column_55"::unsigned -> "g", "column_56"::unsigned -> "e", sum(("sum_74"::decimal))::decimal -> "f")
                    group by ("column_55"::unsigned, "column_56"::unsigned) output: ("column_56"::unsigned -> "column_56", "column_55"::unsigned -> "column_55", "sum_74"::decimal -> "sum_74")
                        motion [policy: segment([ref("column_55"), ref("column_56")])]
                            scan
                                projection ("t2"."e"::unsigned -> "column_56", "t2"."g"::unsigned -> "column_55", sum(("t2"."f"::unsigned))::decimal -> "sum_74")
                                    group by ("t2"."g"::unsigned, "t2"."e"::unsigned) output: ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h", "t2"."bucket_id"::unsigned -> "bucket_id")
                                        scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_multiple_motions_in_condition_row() {
    let input = r#"SELECT * from (select sum("a") as a, sum("b") as b from "t") as o 
        inner join (select count("b") as c, count("d") as d from "t") as i
        on (o.a, i.d) = (i.c, o.a) and i.c < i.d
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // TODO: For the  hash function in the cartrisge runtime we can apply
    //       `motion [policy: segment([ref("C")])]` instead of the `motion [policy: full]`.
    let expected_explain = String::from(
        r#"projection ("O"."A"::decimal -> "A", "O"."B"::decimal -> "B", "I"."C"::integer -> "C", "I"."D"::integer -> "D")
    join on ROW("O"."A"::decimal, "I"."D"::integer) = ROW("I"."C"::integer, "O"."A"::decimal) and ROW("I"."C"::integer) < ROW("I"."D"::integer)
        motion [policy: segment([ref("A")])]
            scan "O"
                projection (sum(("sum_13"::decimal))::decimal -> "A", sum(("sum_16"::decimal))::decimal -> "B")
                    motion [policy: full]
                        scan
                            projection (sum(("t"."b"::unsigned))::decimal -> "sum_16", sum(("t"."a"::unsigned))::decimal -> "sum_13")
                                scan "t"
        motion [policy: full]
            scan "I"
                projection (sum(("count_39"::integer))::decimal -> "C", sum(("count_42"::integer))::decimal -> "D")
                    motion [policy: full]
                        scan
                            projection (count(("t"."d"::unsigned))::integer -> "count_42", count(("t"."b"::unsigned))::integer -> "count_39")
                                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_left_join() {
    let input = r#"SELECT * from (select "a" as a from "t") as o 
        left outer join (select "b" as c, "d" as d from "t") as i
        on o.a = i.c
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("O"."A"::unsigned -> "A", "I"."C"::unsigned -> "C", "I"."D"::unsigned -> "D")
    left join on ROW("O"."A"::unsigned) = ROW("I"."C"::unsigned)
        scan "O"
            projection ("t"."a"::unsigned -> "A")
                scan "t"
        motion [policy: full]
            scan "I"
                projection ("t"."b"::unsigned -> "C", "t"."d"::unsigned -> "D")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_left_join_single_left() {
    let input = r#"
        select * from (select sum("id") / 3 as a from "test_space") as t1
        left outer join (select "id" as b from "test_space") as t2
        on t1.a = t2.b
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("T1"."A"::decimal -> "A", "T2"."B"::unsigned -> "B")
    left join on ROW("T1"."A"::decimal) = ROW("T2"."B"::unsigned)
        motion [policy: segment([ref("A")])]
            scan "T1"
                projection ((sum(("sum_13"::decimal))::decimal) / (3::unsigned) -> "A")
                    motion [policy: full]
                        scan
                            projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_13")
                                scan "test_space"
        motion [policy: full]
            scan "T2"
                projection ("test_space"."id"::unsigned -> "B")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_left_join_single_left2() {
    let input = r#"
        select * from (select sum("id") / 3 as a from "test_space") as t1
        left join (select "id" as b from "test_space") as t2
        on t1.a + 3 != t2.b
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // full motion should be under outer child
    let expected_explain = String::from(
        r#"projection ("T1"."A"::decimal -> "A", "T2"."B"::unsigned -> "B")
    left join on ROW("T1"."A"::decimal) + ROW(3::unsigned) <> ROW("T2"."B"::unsigned)
        motion [policy: segment([ref("A")])]
            scan "T1"
                projection ((sum(("sum_13"::decimal))::decimal) / (3::unsigned) -> "A")
                    motion [policy: full]
                        scan
                            projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_13")
                                scan "test_space"
        motion [policy: full]
            scan "T2"
                projection ("test_space"."id"::unsigned -> "B")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_left_join_single_both() {
    let input = r#"
        select * from (select sum("id") / 3 as a from "test_space") as t1
        left join (select count("id") as b from "test_space") as t2
        on t1.a != t2.b
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    // full motion should be under outer child
    let expected_explain = String::from(
        r#"projection ("T1"."A"::decimal -> "A", "T2"."B"::integer -> "B")
    left join on ROW("T1"."A"::decimal) <> ROW("T2"."B"::integer)
        motion [policy: segment([ref("A")])]
            scan "T1"
                projection ((sum(("sum_13"::decimal))::decimal) / (3::unsigned) -> "A")
                    motion [policy: full]
                        scan
                            projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_13")
                                scan "test_space"
        motion [policy: full]
            scan "T2"
                projection (sum(("count_38"::integer))::decimal -> "B")
                    motion [policy: full]
                        scan
                            projection (count(("test_space"."id"::unsigned))::integer -> "count_38")
                                scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_nested_subqueries() {
    let input = r#"SELECT "a" FROM "t"
        WHERE "a" in (SELECT "a" FROM "t1" WHERE "a" in (SELECT "b" FROM "t1"))"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."a"::unsigned -> "a")
    selection ROW("t"."a"::unsigned) in ROW($1)
        scan "t"
subquery $0:
motion [policy: full]
                            scan
                                projection ("t1"."b"::integer -> "b")
                                    scan "t1"
subquery $1:
motion [policy: full]
            scan
                projection ("t1"."a"::string -> "a")
                    selection ROW("t1"."a"::string) in ROW($0)
                        scan "t1"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_having1() {
    let input = r#"SELECT "a", sum("b") FROM "t"
        group by "a"
        having "a" > 1 and sum(distinct "b") > 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_12"::unsigned -> "a", sum(("sum_52"::decimal))::decimal -> "COL_1")
    having ROW("column_12"::unsigned) > ROW(1::unsigned) and ROW(sum(distinct ("column_30"::decimal))::decimal) > ROW(1::unsigned)
        group by ("column_12"::unsigned) output: ("column_30"::unsigned -> "column_30", "column_12"::unsigned -> "column_12", "sum_52"::decimal -> "sum_52")
            motion [policy: segment([ref("column_12")])]
                scan
                    projection ("t"."b"::unsigned -> "column_30", "t"."a"::unsigned -> "column_12", sum(("t"."b"::unsigned))::decimal -> "sum_52")
                        group by ("t"."a"::unsigned, "t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                            scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_having2() {
    let input = r#"SELECT sum("a") * count(distinct "b"), sum("a") FROM "t"
        having sum(distinct "b") > 1 and sum("a") > 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ((sum(("sum_38"::decimal))::decimal) * (count(distinct ("column_39"::integer))::integer) -> "COL_1", sum(("sum_38"::decimal))::decimal -> "COL_2")
    having ROW(sum(distinct ("column_39"::decimal))::decimal) > ROW(1::unsigned) and ROW(sum(("sum_38"::decimal))::decimal) > ROW(1::unsigned)
        motion [policy: full]
            scan
                projection ("t"."b"::unsigned -> "column_39", sum(("t"."a"::unsigned))::decimal -> "sum_38")
                    group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_having3() {
    let input = r#"SELECT sum("a") FROM "t"
        having sum("a") > 1
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection (sum(("sum_31"::decimal))::decimal -> "COL_1")
    having ROW(sum(("sum_31"::decimal))::decimal) > ROW(1::unsigned)
        motion [policy: full]
            scan
                projection (sum(("t"."a"::unsigned))::decimal -> "sum_31")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_having4() {
    let input = r#"SELECT sum("a") FROM "t"
        having sum("a") > 1 and "b" > 1
    "#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.replace_in_operator().unwrap();
    plan.split_columns().unwrap();
    plan.set_dnf().unwrap();
    plan.derive_equalities().unwrap();
    plan.merge_tuples().unwrap();
    let err = plan.add_motions().unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("HAVING argument must appear in the GROUP BY clause or be used in an aggregate function")
    );
}

#[test]
fn front_sql_having_with_sq() {
    let input = r#"
        SELECT "sysFrom", sum(distinct "id") as "sum", count(distinct "id") as "count" from "test_space"
        group by "sysFrom"
        having (select "sysFrom" from "test_space" where "sysFrom" = 2) > count(distinct "id")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_12"::unsigned -> "sysFrom", sum(distinct ("column_80"::decimal))::decimal -> "sum", count(distinct ("column_80"::integer))::integer -> "count")
    having ROW($0) > ROW(count(distinct ("column_80"::integer))::integer)
        group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "column_80"::unsigned -> "column_80")
            motion [policy: segment([ref("column_12")])]
                scan
                    projection ("test_space"."sysFrom"::unsigned -> "column_12", "test_space"."id"::unsigned -> "column_80")
                        group by ("test_space"."sysFrom"::unsigned, "test_space"."id"::unsigned) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                            scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection ("test_space"."sysFrom"::unsigned -> "sysFrom")
                    selection ROW("test_space"."sysFrom"::unsigned) = ROW(2::unsigned)
                        scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_unmatched_column_in_having() {
    let input = r#"SELECT sum("a"), "a" FROM "t"
        group by "a"
        having sum("a") > 1 and "a" > 1 or "c" = 1
    "#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.replace_in_operator().unwrap();
    plan.split_columns().unwrap();
    plan.set_dnf().unwrap();
    plan.derive_equalities().unwrap();
    plan.merge_tuples().unwrap();
    let err = plan.add_motions().unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("column \"c\" is not found in grouping expressions!")
    );
}

#[test]
fn front_sql_having_with_sq_segment_motion() {
    // check subquery has Segment Motion on groupby columns
    let input = r#"
        SELECT "sysFrom", "sys_op", sum(distinct "id") as "sum", count(distinct "id") as "count" from "test_space"
        group by "sysFrom", "sys_op"
        having ("sysFrom", "sys_op") in (select "a", "d" from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_12"::unsigned -> "sysFrom", "column_13"::unsigned -> "sys_op", sum(distinct ("column_70"::decimal))::decimal -> "sum", count(distinct ("column_70"::integer))::integer -> "count")
    having ROW("column_12"::unsigned, "column_13"::unsigned) in ROW($0, $0)
        group by ("column_12"::unsigned, "column_13"::unsigned) output: ("column_12"::unsigned -> "column_12", "column_70"::unsigned -> "column_70", "column_13"::unsigned -> "column_13")
            motion [policy: segment([ref("column_12"), ref("column_13")])]
                scan
                    projection ("test_space"."sysFrom"::unsigned -> "column_12", "test_space"."id"::unsigned -> "column_70", "test_space"."sys_op"::unsigned -> "column_13")
                        group by ("test_space"."sysFrom"::unsigned, "test_space"."sys_op"::unsigned, "test_space"."id"::unsigned) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                            scan "test_space"
subquery $0:
motion [policy: segment([ref("a"), ref("d")])]
            scan
                projection ("t"."a"::unsigned -> "a", "t"."d"::unsigned -> "d")
                    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_having_with_sq_segment_local_motion() {
    // check subquery has no Motion, as it has the same distribution
    // as columns used in GroupBy
    let input = r#"
        SELECT "sysFrom", "sys_op", sum(distinct "id") as "sum", count(distinct "id") as "count" from "test_space"
        group by "sysFrom", "sys_op"
        having ("sysFrom", "sys_op") in (select "a", "b" from "t")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("column_12"::unsigned -> "sysFrom", "column_13"::unsigned -> "sys_op", sum(distinct ("column_70"::decimal))::decimal -> "sum", count(distinct ("column_70"::integer))::integer -> "count")
    having ROW("column_12"::unsigned, "column_13"::unsigned) in ROW($0, $0)
        group by ("column_12"::unsigned, "column_13"::unsigned) output: ("column_12"::unsigned -> "column_12", "column_70"::unsigned -> "column_70", "column_13"::unsigned -> "column_13")
            motion [policy: segment([ref("column_12"), ref("column_13")])]
                scan
                    projection ("test_space"."sysFrom"::unsigned -> "column_12", "test_space"."id"::unsigned -> "column_70", "test_space"."sys_op"::unsigned -> "column_13")
                        group by ("test_space"."sysFrom"::unsigned, "test_space"."sys_op"::unsigned, "test_space"."id"::unsigned) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                            scan "test_space"
subquery $0:
scan
            projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_unique_local_aggregates() {
    // make sure we don't compute extra aggregates at local stage
    let input = r#"SELECT sum("a"), count("a"), sum("a") + count("a") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    // here we must compute only two aggregates at local stage: sum(a), count(a)
    let expected_explain = String::from(
        r#"projection (sum(("sum_13"::decimal))::decimal -> "COL_1", sum(("count_16"::integer))::decimal -> "COL_2", (sum(("sum_13"::decimal))::decimal) + (sum(("count_16"::integer))::decimal) -> "COL_3")
    motion [policy: full]
        scan
            projection (count(("t"."a"::unsigned))::integer -> "count_16", sum(("t"."a"::unsigned))::decimal -> "sum_13")
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_unique_local_groupings() {
    // make sure we don't compute extra group by columns at local stage
    let input = r#"SELECT sum(distinct "a"), count(distinct "a"), count(distinct "b") FROM "t"
        group by "b"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    // here we must compute only two groupby columns at local stage: a, b
    let expected_explain = String::from(
        r#"projection (sum(distinct ("column_25"::decimal))::decimal -> "COL_1", count(distinct ("column_25"::integer))::integer -> "COL_2", count(distinct ("column_12"::integer))::integer -> "COL_3")
    group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "column_25"::unsigned -> "column_25")
        motion [policy: segment([ref("column_12")])]
            scan
                projection ("t"."b"::unsigned -> "column_12", "t"."a"::unsigned -> "column_25")
                    group by ("t"."b"::unsigned, "t"."a"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_join_table_with_bucket_id_as_first_col() {
    // here we are joining t3 who has bucket_id as its first column,
    // check that we correctly handle references in join condition,
    // after inserting SQ with Projection under outer child
    let input = r#"
SELECT * FROM
    "t3"
INNER JOIN
    (SELECT * FROM "hash_single_testing" INNER JOIN (SELECT "id" FROM "test_space") as "ts"
     ON "hash_single_testing"."identification_number" = "ts"."id") as "ij"
ON "t3"."a" = "ij"."id"
"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t3"."a"::string -> "a", "t3"."b"::integer -> "b", "ij"."identification_number"::integer -> "identification_number", "ij"."product_code"::string -> "product_code", "ij"."product_units"::boolean -> "product_units", "ij"."sys_op"::unsigned -> "sys_op", "ij"."id"::unsigned -> "id")
    join on ROW("t3"."a"::string) = ROW("ij"."id"::unsigned)
        scan "t3"
            projection ("t3"."a"::string -> "a", "t3"."b"::integer -> "b")
                scan "t3"
        scan "ij"
            projection ("hash_single_testing"."identification_number"::integer -> "identification_number", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."sys_op"::unsigned -> "sys_op", "ts"."id"::unsigned -> "id")
                join on ROW("hash_single_testing"."identification_number"::integer) = ROW("ts"."id"::unsigned)
                    scan "hash_single_testing"
                        projection ("hash_single_testing"."identification_number"::integer -> "identification_number", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."sys_op"::unsigned -> "sys_op")
                            scan "hash_single_testing"
                    scan "ts"
                        projection ("test_space"."id"::unsigned -> "id")
                            scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_select_distinct() {
    // make sure we don't compute extra group by columns at local stage
    let input = r#"SELECT distinct "a", "a" + "b" FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    // here we must compute only two groupby columns at local stage: a, b
    let expected_explain = String::from(
        r#"projection ("column_22"::unsigned -> "a", "column_27"::unsigned -> "COL_1")
    group by ("column_22"::unsigned, "column_27"::unsigned) output: ("column_22"::unsigned -> "column_22", "column_27"::unsigned -> "column_27")
        motion [policy: segment([ref("column_22"), ref("column_27")])]
            scan
                projection ("t"."a"::unsigned -> "column_22", ("t"."a"::unsigned) + ("t"."b"::unsigned) -> "column_27")
                    group by ("t"."a"::unsigned, ("t"."a"::unsigned) + ("t"."b"::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_select_distinct_asterisk() {
    let input = r#"SELECT distinct * FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_23"::unsigned -> "a", "column_24"::unsigned -> "b", "column_25"::unsigned -> "c", "column_26"::unsigned -> "d")
    group by ("column_24"::unsigned, "column_26"::unsigned, "column_23"::unsigned, "column_25"::unsigned) output: ("column_24"::unsigned -> "column_24", "column_23"::unsigned -> "column_23", "column_26"::unsigned -> "column_26", "column_25"::unsigned -> "column_25")
        motion [policy: segment([ref("column_24"), ref("column_26"), ref("column_23"), ref("column_25")])]
            scan
                projection ("t"."b"::unsigned -> "column_24", "t"."a"::unsigned -> "column_23", "t"."d"::unsigned -> "column_26", "t"."c"::unsigned -> "column_25")
                    group by ("t"."b"::unsigned, "t"."d"::unsigned, "t"."a"::unsigned, "t"."c"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_invalid_select_distinct() {
    let input = r#"SELECT distinct "a" FROM "t"
        group by "b"
    "#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.replace_in_operator().unwrap();
    plan.split_columns().unwrap();
    plan.set_dnf().unwrap();
    plan.derive_equalities().unwrap();
    plan.merge_tuples().unwrap();
    let err = plan.add_motions().unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("column \"a\" is not found in grouping expressions!")
    );
}

#[test]
fn front_sql_select_distinct_with_aggr() {
    let input = r#"SELECT distinct sum("a"), "b" FROM "t"
    group by "b"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection (sum(("sum_26"::decimal))::decimal -> "COL_1", "column_12"::unsigned -> "b")
    group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "sum_26"::decimal -> "sum_26")
        motion [policy: segment([ref("column_12")])]
            scan
                projection ("t"."b"::unsigned -> "column_12", sum(("t"."a"::unsigned))::decimal -> "sum_26")
                    group by ("t"."b"::unsigned) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_select_distinct_with_aggr2() {
    let input = r#"SELECT distinct sum("a") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection (sum(("sum_13"::decimal))::decimal -> "COL_1")
    motion [policy: full]
        scan
            projection (sum(("t"."a"::unsigned))::decimal -> "sum_13")
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_on_conflict() {
    let mut input = r#"insert into "t" values (1, 1, 1, 1) on conflict do nothing"#;

    let mut plan = sql_to_optimized_ir(input, vec![]);
    let mut expected_explain = String::from(
        r#"insert "t" on conflict: nothing
    motion [policy: local segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW(1::unsigned, 1::unsigned, 1::unsigned, 1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    input = r#"insert into "t" values (1, 1, 1, 1) on conflict do replace"#;
    plan = sql_to_optimized_ir(input, vec![]);
    expected_explain = String::from(
        r#"insert "t" on conflict: replace
    motion [policy: local segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW(1::unsigned, 1::unsigned, 1::unsigned, 1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_1() {
    let input = r#"insert into "t" ("b") select "a" from "t"
        where "a" = 1 and "b" = 2 or "a" = 2 and "b" = 3"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("a")])]
        projection ("t"."a"::unsigned -> "a")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned) or ROW("t"."a"::unsigned) = ROW(2::unsigned) and ROW("t"."b"::unsigned) = ROW(3::unsigned)
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_2() {
    let input = r#"insert into "t" ("a", "b") select "a", "b" from "t"
        where "a" = 1 and "b" = 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: local segment([ref("a"), ref("b")])]
        projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned)
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_3() {
    // check different column order leads to Segment motion
    let input = r#"insert into "t" ("b", "a") select "a", "b" from "t"
        where "a" = 1 and "b" = 2 or "a" = 3 and "b" = 4"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("b"), ref("a")])]
        projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned) or ROW("t"."a"::unsigned) = ROW(3::unsigned) and ROW("t"."b"::unsigned) = ROW(4::unsigned)
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_4() {
    let input = r#"insert into "t" ("b", "a") select "b", "a" from "t"
        where "a" = 1 and "b" = 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: local segment([ref("a"), ref("b")])]
        projection ("t"."b"::unsigned -> "b", "t"."a"::unsigned -> "a")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned)
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_5() {
    let input = r#"insert into "t" ("b", "a") select 5, 6 from "t"
        where "a" = 1 and "b" = 2"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([ref("COL_2"), ref("COL_1")])]
        projection (5::unsigned -> "COL_1", 6::unsigned -> "COL_2")
            selection ROW("t"."a"::unsigned) = ROW(1::unsigned) and ROW("t"."b"::unsigned) = ROW(2::unsigned)
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_6() {
    // The values should be materialized on the router, and
    // then dispatched to storages.
    let input = r#"insert into "t" ("a", "b") values (1, 2), (1, 2), (3, 4)"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: local segment([ref("COLUMN_5"), ref("COLUMN_6")])]
        values
            value row (data=ROW(1::unsigned, 2::unsigned))
            value row (data=ROW(1::unsigned, 2::unsigned))
            value row (data=ROW(3::unsigned, 4::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_7() {
    // Check system column can't be inserted
    let input = r#"insert into "hash_testing" ("sys_op", "bucket_id" ) values (1, 2)"#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(input).unwrap();
    let plan = ast.resolve_metadata(metadata);
    let err = plan.unwrap_err();
    assert_eq!(
        true,
        err.to_string()
            .contains("system column \"bucket_id\" cannot be inserted")
    );
}

#[test]
fn front_sql_insert_8() {
    // Both table have the same columns, but hash_single_testing has different shard key
    let input = r#"insert into "hash_testing" select * from "hash_single_testing""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "hash_testing" on conflict: fail
    motion [policy: segment([ref("identification_number"), ref("product_code")])]
        projection ("hash_single_testing"."identification_number"::integer -> "identification_number", "hash_single_testing"."product_code"::string -> "product_code", "hash_single_testing"."product_units"::boolean -> "product_units", "hash_single_testing"."sys_op"::unsigned -> "sys_op")
            scan "hash_single_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_9() {
    let input = r#"insert into "t" ("a", "b") values (?, ?)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::from(1_u64), Value::from(2_u64)]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: local segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW(1::unsigned, 2::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[cfg(test)]
mod params;
mod single;
