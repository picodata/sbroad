use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use pretty_assertions::assert_eq;

#[test]
fn front_sql1() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code")
    selection ROW("hash_testing"."identification_number") = ROW(1)
        scan "hash_testing"
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
        r#"projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code")
    selection ROW("hash_testing"."identification_number") = ROW(1) and ROW("hash_testing"."product_code") = ROW('1') or ROW("hash_testing"."identification_number") = ROW(2) and ROW("hash_testing"."product_code") = ROW('2')
        scan "hash_testing"
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
        r#"projection ("t3"."identification_number" -> "identification_number", "t3"."product_code" -> "product_code")
    selection ROW("t3"."identification_number") = ROW(1)
        scan "t3"
            union all
                projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code")
                    selection ROW("hash_testing"."sys_op") = ROW(1)
                        scan "hash_testing"
                projection ("hash_testing_hist"."identification_number" -> "identification_number", "hash_testing_hist"."product_code" -> "product_code")
                    selection ROW("hash_testing_hist"."sys_op") > ROW(1)
                        scan "hash_testing_hist"
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
        r#"projection ("t3"."identification_number" -> "identification_number", "t3"."product_code" -> "product_code")
    selection ROW("t3"."identification_number") = ROW(1) or ROW("t3"."identification_number") = ROW(2) or ROW("t3"."identification_number") = ROW(3) and ROW("t3"."product_code") = ROW('1') or ROW("t3"."product_code") = ROW('2')
        scan "t3"
            union all
                projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code")
                    selection ROW("hash_testing"."sys_op") = ROW(1)
                        scan "hash_testing"
                projection ("hash_testing_hist"."identification_number" -> "identification_number", "hash_testing_hist"."product_code" -> "product_code")
                    selection ROW("hash_testing_hist"."sys_op") > ROW(1)
                        scan "hash_testing_hist"
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
        r#"projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code")
    selection ROW("hash_testing"."identification_number") in ROW($0)
        scan "hash_testing"
subquery $0:
motion [policy: full]
            scan
                projection ("hash_testing_hist"."identification_number" -> "identification_number")
                    selection ROW("hash_testing_hist"."product_code") = ROW('a')
                        scan "hash_testing_hist"
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
        r#"projection ("T"."id" -> "id", "hash_testing"."product_units" -> "product_units")
    selection ROW("hash_testing"."identification_number") = ROW(5) and ROW("hash_testing"."product_code") = ROW('123')
        join on ROW("hash_testing"."identification_number") = ROW("T"."id")
            scan "hash_testing"
                projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code", "hash_testing"."product_units" -> "product_units", "hash_testing"."sys_op" -> "sys_op")
                    scan "hash_testing"
            motion [policy: full]
                scan "T"
                    projection ("test_space"."id" -> "id")
                        scan "test_space"
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
        r#"projection ("T"."identification_number" -> "identification_number", "T"."product_code" -> "product_code")
    selection ROW("T"."identification_number") = ROW(1)
        scan "hash_testing" -> "T"
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
        r#"projection ("t3"."id" -> "id", "t3"."FIRST_NAME" -> "FIRST_NAME", "t8"."identification_number" -> "identification_number", "t8"."product_code" -> "product_code")
    selection ROW("t3"."id") = ROW(1) and ROW("t8"."identification_number") = ROW(1) and ROW("t8"."product_code") = ROW('123')
        join on ROW("t3"."id") = ROW("t8"."identification_number")
            scan "t3"
                union all
                    projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                        selection ROW("test_space"."sys_op") < ROW(0) and ROW("test_space"."sysFrom") >= ROW(0)
                            scan "test_space"
                    projection ("test_space_hist"."id" -> "id", "test_space_hist"."FIRST_NAME" -> "FIRST_NAME")
                        selection ROW("test_space_hist"."sysFrom") <= ROW(0)
                            scan "test_space_hist"
            motion [policy: segment([ref("identification_number")])]
                scan "t8"
                    union all
                        projection ("hash_testing_hist"."identification_number" -> "identification_number", "hash_testing_hist"."product_code" -> "product_code")
                            selection ROW("hash_testing_hist"."sys_op") > ROW(0)
                                scan "hash_testing_hist"
                        projection ("hash_single_testing_hist"."identification_number" -> "identification_number", "hash_single_testing_hist"."product_code" -> "product_code")
                            selection ROW("hash_single_testing_hist"."sys_op") <= ROW(0)
                                scan "hash_single_testing_hist"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql10() {
    let input = r#"INSERT INTO "t" VALUES(1, 2, 3, 4)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t"
    motion [policy: local segment([ref("COLUMN_1"), ref("COLUMN_2")])]
        values
            value row (data=ROW(1, 2, 3, 4))
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql11() {
    let input = r#"INSERT INTO "t" ("a", "c") VALUES(1, 2)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t"
    motion [policy: local segment([ref("COLUMN_1"), value(NULL)])]
        values
            value row (data=ROW(1, 2))
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql14() {
    let input = r#"INSERT INTO "t" ("a", "c") SELECT "b", "d" FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t"
    motion [policy: segment([ref("b"), value(NULL)])]
        projection ("t"."b" -> "b", "t"."d" -> "d")
            scan "t"
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
        r#"projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code")
    selection ROW("hash_testing"."product_code") = ROW('кириллица')
        scan "hash_testing"
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
        r#"projection ("hash_testing"."identification_number" -> "identification_number")
    selection ROW("hash_testing"."product_code") is null
        scan "hash_testing"
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
        r#"projection ("hash_testing"."product_code" -> "product_code")
    selection ROW("hash_testing"."product_code") >= ROW(1) and ROW("hash_testing"."product_code") <= ROW(2)
        scan "hash_testing"
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
        r#"projection ("hash_testing"."identification_number" -> "identification_number")
    selection ROW("hash_testing"."product_code") is not null
        scan "hash_testing"
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
        r#"projection ("hash_testing"."identification_number" -> "identification_number")
    selection ROW("hash_testing"."product_code") = ROW('«123»')
        scan "hash_testing"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_exists_subquery_select_from_table() {
    let input = r#"SELECT "id" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id" -> "id")
    selection exists ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection (0 -> "COL_1")
                    scan "hash_testing"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_not_exists_subquery_select_from_table() {
    let input = r#"SELECT "id" FROM "test_space" WHERE NOT EXISTS (SELECT 0 FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id" -> "id")
    selection not exists ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection (0 -> "COL_1")
                    scan "hash_testing"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_exists_subquery_select_from_table_with_condition() {
    let input = r#"SELECT "id" FROM "test_space" WHERE EXISTS (SELECT 0 FROM "hash_testing" WHERE "identification_number" != 42)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id" -> "id")
    selection exists ROW($0)
        scan "test_space"
subquery $0:
motion [policy: full]
            scan
                projection (0 -> "COL_1")
                    selection ROW("hash_testing"."identification_number") <> ROW(42)
                        scan "hash_testing"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing" group by "identification_number", "product_code""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("column_12" -> "identification_number", "column_13" -> "product_code")
    group by ("column_13", "column_12") output: ("column_12" -> "column_12", "column_13" -> "column_13")
        motion [policy: segment([ref("column_13"), ref("column_12")])]
            scan 
                projection ("hash_testing"."identification_number" -> "column_12", "hash_testing"."product_code" -> "column_13")
                    group by ("hash_testing"."product_code", "hash_testing"."identification_number") output: ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code", "hash_testing"."product_units" -> "product_units", "hash_testing"."sys_op" -> "sys_op", "hash_testing"."bucket_id" -> "bucket_id")
                        scan "hash_testing"
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
        r#"projection ("column_12" -> "identification_number")
    group by ("column_12", "column_13") output: ("column_12" -> "column_12", "column_13" -> "column_13")
        motion [policy: segment([ref("column_12"), ref("column_13")])]
            scan 
                projection ("hash_testing"."identification_number" -> "column_12", "hash_testing"."product_units" -> "column_13")
                    group by ("hash_testing"."identification_number", "hash_testing"."product_units") output: ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code", "hash_testing"."product_units" -> "product_units", "hash_testing"."sys_op" -> "sys_op", "hash_testing"."bucket_id" -> "bucket_id")
                        scan "hash_testing"
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
    projection ("column_12" -> "identification_number")
        group by ("column_12") output: ("column_12" -> "column_12")
            motion [policy: segment([ref("column_12")])]
                scan 
                    projection ("hash_testing"."identification_number" -> "column_12")
                        group by ("hash_testing"."identification_number") output: ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code", "hash_testing"."product_units" -> "product_units", "hash_testing"."sys_op" -> "sys_op", "hash_testing"."bucket_id" -> "bucket_id")
                            scan "hash_testing"
    projection ("hash_testing"."identification_number" -> "identification_number")
        scan "hash_testing"
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
    projection ("hash_testing"."identification_number" -> "identification_number")
        scan "hash_testing"
    projection ("identification_number" -> "identification_number")
        scan
            union all
                projection ("column_28" -> "identification_number")
                    group by ("column_28") output: ("column_28" -> "column_28")
                        motion [policy: segment([ref("column_28")])]
                            scan 
                                projection ("hash_testing"."identification_number" -> "column_28")
                                    group by ("hash_testing"."identification_number") output: ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code", "hash_testing"."product_units" -> "product_units", "hash_testing"."sys_op" -> "sys_op", "hash_testing"."bucket_id" -> "bucket_id")
                                        scan "hash_testing"
                projection ("hash_testing"."identification_number" -> "identification_number")
                    scan "hash_testing"
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
        r#"projection ("column_63" -> "product_code", "column_64" -> "product_units")
    group by ("column_63", "column_64") output: ("column_64" -> "column_64", "column_63" -> "column_63")
        motion [policy: segment([ref("column_63"), ref("column_64")])]
            scan 
                projection ("T2"."product_units" -> "column_64", "T2"."product_code" -> "column_63")
                    group by ("T2"."product_code", "T2"."product_units") output: ("T2"."product_units" -> "product_units", "T2"."product_code" -> "product_code", "T2"."identification_number" -> "identification_number", "T"."id" -> "id")
                        join on ROW("T2"."identification_number") = ROW("T"."id")
                            scan "T2"
                                projection ("hash_testing"."product_units" -> "product_units", "hash_testing"."product_code" -> "product_code", "hash_testing"."identification_number" -> "identification_number")
                                    scan "hash_testing"
                            motion [policy: full]
                                scan "T"
                                    projection ("test_space"."id" -> "id")
                                        scan "test_space"
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
        r#"projection ("T2"."product_code" -> "product_code", "T2"."product_units" -> "product_units")
    join on ROW("T2"."identification_number") = ROW("T"."id") and ROW("T"."id") is not null
        scan "T2"
            projection ("hash_testing"."product_units" -> "product_units", "hash_testing"."product_code" -> "product_code", "hash_testing"."identification_number" -> "identification_number")
                scan "hash_testing"
        motion [policy: full]
            scan "T"
                projection ("test_space"."id" -> "id")
                    scan "test_space"
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
        r#"projection ("T1"."product_code" -> "product_code", "T1"."product_units" -> "product_units")
    join on ROW("T1"."identification_number") = ROW("T2"."id") and ROW("T2"."id") is not null
        scan "T1"
            projection ("hash_single_testing"."product_units" -> "product_units", "hash_single_testing"."product_code" -> "product_code", "hash_single_testing"."identification_number" -> "identification_number")
                scan "hash_single_testing"
        scan "T2"
            projection ("test_space"."id" -> "id")
                scan "test_space"
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());

    // check we have no error, in case one of the join children has Distribution::Single
    let input = r#"SELECT "product_code", "product_units" FROM (SELECT "product_units", "product_code", "identification_number" FROM "hash_single_testing") as t1
        INNER JOIN (SELECT sum("id") as "id" from "test_space") as t2
        ON t1."identification_number" = t2."id" and t2."id" is not null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("T1"."product_code" -> "product_code", "T1"."product_units" -> "product_units")
    join on ROW("T1"."identification_number") = ROW("T2"."id") and ROW("T2"."id") is not null
        scan "T1"
            projection ("hash_single_testing"."product_units" -> "product_units", "hash_single_testing"."product_code" -> "product_code", "hash_single_testing"."identification_number" -> "identification_number")
                scan "hash_single_testing"
        motion [policy: segment([ref("id")])]
            scan "T2"
                projection (sum(("sum_41")) -> "id")
                    motion [policy: full]
                        scan 
                            projection (sum(("test_space"."id")) -> "sum_41")
                                scan "test_space"
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_insert() {
    let input = r#"INSERT INTO "t" ("a", "c") SELECT "b", "d" FROM "t" group by "b", "d""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t"
    motion [policy: segment([ref("b"), value(NULL)])]
        projection ("column_24" -> "b", "column_25" -> "d")
            group by ("column_24", "column_25") output: ("column_25" -> "column_25", "column_24" -> "column_24")
                motion [policy: segment([ref("column_24"), ref("column_25")])]
                    scan 
                        projection ("t"."d" -> "column_25", "t"."b" -> "column_24")
                            group by ("t"."b", "t"."d") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                                scan "t"
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
        r#"projection ("column_12" -> "b", (sum(("count_28"))) + (sum(("count_30"))) -> "COL_1")
    group by ("column_12") output: ("column_12" -> "column_12", "count_28" -> "count_28", "count_30" -> "count_30")
        motion [policy: segment([ref("column_12")])]
            scan 
                projection ("t"."b" -> "column_12", count(("t"."a")) -> "count_28", count(("t"."b")) -> "count_30")
                    group by ("t"."b") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_count_asterisk1() {
    let input = r#"SELECT count(*), count(*) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("count_13")) -> "COL_1", sum(("count_13")) -> "COL_2")
    motion [policy: full]
        scan 
            projection (count((*)) -> "count_13")
                scan "t"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_count_asterisk2() {
    let input = r#"SELECT cOuNt(*), "b" FROM "t" group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(("count_26")) -> "COL_1", "column_12" -> "b")
    group by ("column_12") output: ("column_12" -> "column_12", "count_26" -> "count_26")
        motion [policy: segment([ref("column_12")])]
            scan 
                projection ("t"."b" -> "column_12", count((*)) -> "count_26")
                    group by ("t"."b") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
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
        r#"projection ("column_12" -> "b", sum(("count_35")) -> "COL_1", sum(("count_39")) -> "COL_2")
    group by ("column_12") output: ("column_12" -> "column_12", "count_39" -> "count_39", "count_35" -> "count_35")
        motion [policy: segment([ref("column_12")])]
            scan 
                projection ("t"."b" -> "column_12", count(("FUNC"(("t"."a")))) -> "count_39", count((("t"."a") * ("t"."b") + (1))) -> "count_35")
                    group by ("t"."b") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
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
        r#"projection ("column_12" -> "b", count(distinct ("column_27")) -> "COL_1", count(distinct ("column_12")) -> "COL_2")
    group by ("column_12") output: ("column_12" -> "column_12", "column_27" -> "column_27")
        motion [policy: segment([ref("column_12")])]
            scan 
                projection ("t"."b" -> "column_12", "t"."a" -> "column_27")
                    group by ("t"."b", "t"."a") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
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
        r#"projection ("column_12" -> "b", sum(distinct ("column_34")) -> "COL_1")
    group by ("column_12") output: ("column_12" -> "column_12", "column_34" -> "column_34")
        motion [policy: segment([ref("column_12")])]
            scan 
                projection ("t"."b" -> "column_12", ("t"."a") + ("t"."b") + (3) -> "column_34")
                    group by ("t"."b", ("t"."a") + ("t"."b") + (3)) output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregates_with_distinct3() {
    let input = r#"SELECT sum(distinct "a" + "b" + 3) FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (sum(distinct ("column_19")) -> "COL_1")
    motion [policy: full]
        scan 
            projection (("t"."a") + ("t"."b") + (3) -> "column_19")
                group by (("t"."a") + ("t"."b") + (3)) output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                    scan "t"
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
        r#"projection (sum(("sum_20")) -> "COL_1")
    motion [policy: full]
        scan 
            projection (sum((("t"."a") * ("t"."b") + (1))) -> "sum_20")
                scan "t"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregate_without_groupby2() {
    let input = r#"SELECT * FROM (SELECT count("id") FROM "test_space") as "t1""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t1"."COL_1" -> "COL_1")
    scan "t1"
        projection (sum(("count_13")) -> "COL_1")
            motion [policy: full]
                scan 
                    projection (count(("test_space"."id")) -> "count_13")
                        scan "test_space"
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
    projection ("t"."a" -> "a")
        scan "t"
    motion [policy: segment([ref("COL_1")])]
        projection (sum(("sum_29")) -> "COL_1")
            motion [policy: full]
                scan 
                    projection (sum(("t"."a")) -> "sum_29")
                        scan "t"
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
        projection (sum(("sum_13")) -> "COL_1")
            motion [policy: full]
                scan 
                    projection (sum(("t"."a")) -> "sum_13")
                        scan "t"
    projection ("t"."a" -> "a")
        scan "t"
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
        projection (sum(("sum_13")) -> "COL_1")
            motion [policy: full]
                scan 
                    projection (sum(("t"."a")) -> "sum_13")
                        scan "t"
    motion [policy: segment([ref("COL_1")])]
        projection (sum(("sum_30")) -> "COL_1")
            motion [policy: full]
                scan 
                    projection (sum(("t"."a")) -> "sum_30")
                        scan "t"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_insert_single() {
    let input = r#"INSERT INTO "t" ("a", "c") SELECT sum("b"), count("d") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t"
    motion [policy: segment([ref("COL_1"), value(NULL)])]
        projection (sum(("sum_25")) -> "COL_1", sum(("count_28")) -> "COL_2")
            motion [policy: full]
                scan 
                    projection (sum(("t"."b")) -> "sum_25", count(("t"."d")) -> "count_28")
                        scan "t"
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
    projection ("t"."a" -> "a", "t"."b" -> "b")
        scan "t"
    motion [policy: segment([ref("COL_1"), ref("COL_2")])]
        projection (sum(("sum_31")) -> "COL_1", sum(("count_34")) -> "COL_2")
            motion [policy: full]
                scan 
                    projection (sum(("t"."a")) -> "sum_31", count(("t"."b")) -> "count_34")
                        scan "t"
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
    projection ("t"."b" -> "b", "t"."a" -> "a")
        scan "t"
    motion [policy: segment([ref("COL_2"), ref("COL_1")])]
        projection (sum(("sum_31")) -> "COL_1", sum(("count_34")) -> "COL_2")
            motion [policy: full]
                scan 
                    projection (sum(("t"."a")) -> "sum_31", count(("t"."b")) -> "count_34")
                        scan "t"
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
        projection (sum(("sum_13")) -> "COL_1", sum(("count_16")) -> "COL_2")
            motion [policy: full]
                scan 
                    projection (sum(("t"."a")) -> "sum_13", count(("t"."b")) -> "count_16")
                        scan "t"
    projection ("t"."a" -> "a", "t"."b" -> "b")
        scan "t"
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
        projection (sum(("sum_13")) -> "COL_1", sum(("count_16")) -> "COL_2")
            motion [policy: full]
                scan 
                    projection (sum(("t"."a")) -> "sum_13", count(("t"."b")) -> "count_16")
                        scan "t"
    motion [policy: segment([ref("COL_1")])]
        projection (sum(("sum_33")) -> "COL_1", sum(("sum_36")) -> "COL_2")
            motion [policy: full]
                scan 
                    projection (sum(("t"."b")) -> "sum_36", sum(("t"."a")) -> "sum_33")
                        scan "t"
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
        r#"projection ("column_16" -> "COL_1")
    group by ("column_16") output: ("column_16" -> "column_16")
        motion [policy: segment([ref("column_16")])]
            scan 
                projection (("t"."a") + ("t"."b") -> "column_16")
                    group by (("t"."a") + ("t"."b")) output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
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
        r#"projection ("column_16" + (sum(("count_35"))) -> "COL_1")
    group by ("column_16") output: ("column_16" -> "column_16", "count_35" -> "count_35")
        motion [policy: segment([ref("column_16")])]
            scan 
                projection ((("t"."a") + ("t"."b")) -> "column_16", count(("t"."a")) -> "count_35")
                    group by ((("t"."a") + ("t"."b"))) output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
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
        r#"projection ("column_16" -> "COL_1", "column_26" * (sum(("sum_55"))) / (sum(("count_61"))) -> "COL_2")
    group by ("column_16", "column_26") output: ("column_26" -> "column_26", "column_16" -> "column_16", "sum_55" -> "sum_55", "count_61" -> "count_61")
        motion [policy: segment([ref("column_16"), ref("column_26")])]
            scan 
                projection ((("t"."c") * ("t"."d")) -> "column_26", ("t"."a") + ("t"."b") -> "column_16", sum((("t"."c") * ("t"."d"))) -> "sum_55", count((("t"."a") * ("t"."b"))) -> "count_61")
                    group by (("t"."a") + ("t"."b"), (("t"."c") * ("t"."d"))) output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
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
        r#"projection ("column_16" -> "COL_1", "column_17" -> "a")
    group by ("column_16", "column_17") output: ("column_17" -> "column_17", "column_16" -> "column_16")
        motion [policy: segment([ref("column_16"), ref("column_17")])]
            scan 
                projection ("t"."a" -> "column_17", ("t"."a") + ("t"."b") -> "column_16")
                    group by (("t"."a") + ("t"."b"), "t"."a") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
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
        r#"projection ("T1"."a" -> "a", "T1"."b" -> "b", "T1"."c" -> "c", "T2"."g" -> "g", "T2"."e" -> "e", "T2"."f" -> "f")
    join on ROW("T1"."a", "T2"."g") = ROW("T2"."e", "T1"."b")
        scan "T1"
            projection ("column_12" -> "a", "column_13" -> "b", sum(("sum_31")) -> "c")
                group by ("column_13", "column_12") output: ("column_12" -> "column_12", "column_13" -> "column_13", "sum_31" -> "sum_31")
                    motion [policy: segment([ref("column_13"), ref("column_12")])]
                        scan 
                            projection ("t"."a" -> "column_12", "t"."b" -> "column_13", sum(("t"."c")) -> "sum_31")
                                group by ("t"."b", "t"."a") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                                    scan "t"
        motion [policy: full]
            scan "T2"
                projection ("column_55" -> "g", "column_56" -> "e", sum(("sum_74")) -> "f")
                    group by ("column_55", "column_56") output: ("column_56" -> "column_56", "column_55" -> "column_55", "sum_74" -> "sum_74")
                        motion [policy: segment([ref("column_55"), ref("column_56")])]
                            scan 
                                projection ("t2"."e" -> "column_56", "t2"."g" -> "column_55", sum(("t2"."f")) -> "sum_74")
                                    group by ("t2"."g", "t2"."e") output: ("t2"."e" -> "e", "t2"."f" -> "f", "t2"."g" -> "g", "t2"."h" -> "h", "t2"."bucket_id" -> "bucket_id")
                                        scan "t2"
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

    let expected_explain = String::from(
        r#"projection ("O"."A" -> "A", "O"."B" -> "B", "I"."C" -> "C", "I"."D" -> "D")
    join on ROW("O"."A", "I"."D") = ROW("I"."C", "O"."A") and ROW("I"."C") < ROW("I"."D")
        motion [policy: segment([ref("A")])]
            scan "O"
                projection (sum(("sum_13")) -> "A", sum(("sum_16")) -> "B")
                    motion [policy: full]
                        scan 
                            projection (sum(("t"."b")) -> "sum_16", sum(("t"."a")) -> "sum_13")
                                scan "t"
        motion [policy: segment([ref("C")])]
            scan "I"
                projection (sum(("count_39")) -> "C", sum(("count_42")) -> "D")
                    motion [policy: full]
                        scan 
                            projection (count(("t"."d")) -> "count_42", count(("t"."b")) -> "count_39")
                                scan "t"
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
        r#"projection ("O"."A" -> "A", "I"."C" -> "C", "I"."D" -> "D")
    left join on ROW("O"."A") = ROW("I"."C")
        scan "O"
            projection ("t"."a" -> "A")
                scan "t"
        motion [policy: full]
            scan "I"
                projection ("t"."b" -> "C", "t"."d" -> "D")
                    scan "t"
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
        r#"projection ("T1"."A" -> "A", "T2"."B" -> "B")
    left join on ROW("T1"."A") = ROW("T2"."B")
        motion [policy: segment([ref("A")])]
            scan "T1"
                projection ((sum(("sum_13"))) / (3) -> "A")
                    motion [policy: full]
                        scan 
                            projection (sum(("test_space"."id")) -> "sum_13")
                                scan "test_space"
        scan "T2"
            projection ("test_space"."id" -> "B")
                scan "test_space"
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
        r#"projection ("T1"."A" -> "A", "T2"."B" -> "B")
    left join on ROW("T1"."A") + ROW(3) <> ROW("T2"."B")
        motion [policy: segment([ref("A")])]
            scan "T1"
                projection ((sum(("sum_13"))) / (3) -> "A")
                    motion [policy: full]
                        scan 
                            projection (sum(("test_space"."id")) -> "sum_13")
                                scan "test_space"
        motion [policy: full]
            scan "T2"
                projection ("test_space"."id" -> "B")
                    scan "test_space"
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
        r#"projection ("T1"."A" -> "A", "T2"."B" -> "B")
    left join on ROW("T1"."A") <> ROW("T2"."B")
        motion [policy: segment([ref("A")])]
            scan "T1"
                projection ((sum(("sum_13"))) / (3) -> "A")
                    motion [policy: full]
                        scan 
                            projection (sum(("test_space"."id")) -> "sum_13")
                                scan "test_space"
        motion [policy: full]
            scan "T2"
                projection (sum(("count_38")) -> "B")
                    motion [policy: full]
                        scan 
                            projection (count(("test_space"."id")) -> "count_38")
                                scan "test_space"
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
        r#"projection ("t"."a" -> "a")
    selection ROW("t"."a") in ROW($1)
        scan "t"
subquery $0:
motion [policy: full]
                            scan
                                projection ("t1"."b" -> "b")
                                    scan "t1"
subquery $1:
motion [policy: full]
            scan
                projection ("t1"."a" -> "a")
                    selection ROW("t1"."a") in ROW($0)
                        scan "t1"
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
        r#"projection (sum(("sum_13")) -> "COL_1", sum(("count_16")) -> "COL_2", (sum(("sum_13"))) + (sum(("count_16"))) -> "COL_3")
    motion [policy: full]
        scan 
            projection (count(("t"."a")) -> "count_16", sum(("t"."a")) -> "sum_13")
                scan "t"
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
        r#"projection (sum(distinct ("column_25")) -> "COL_1", count(distinct ("column_25")) -> "COL_2", count(distinct ("column_12")) -> "COL_3")
    group by ("column_12") output: ("column_12" -> "column_12", "column_25" -> "column_25")
        motion [policy: segment([ref("column_12")])]
            scan 
                projection ("t"."b" -> "column_12", "t"."a" -> "column_25")
                    group by ("t"."b", "t"."a") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
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
        r#"projection ("t3"."a" -> "a", "t3"."b" -> "b", "ij"."identification_number" -> "identification_number", "ij"."product_code" -> "product_code", "ij"."product_units" -> "product_units", "ij"."sys_op" -> "sys_op", "ij"."id" -> "id")
    join on ROW("t3"."a") = ROW("ij"."id")
        scan "t3"
            projection ("t3"."a" -> "a", "t3"."b" -> "b")
                scan "t3"
        scan "ij"
            projection ("hash_single_testing"."identification_number" -> "identification_number", "hash_single_testing"."product_code" -> "product_code", "hash_single_testing"."product_units" -> "product_units", "hash_single_testing"."sys_op" -> "sys_op", "ts"."id" -> "id")
                join on ROW("hash_single_testing"."identification_number") = ROW("ts"."id")
                    scan "hash_single_testing"
                        projection ("hash_single_testing"."identification_number" -> "identification_number", "hash_single_testing"."product_code" -> "product_code", "hash_single_testing"."product_units" -> "product_units", "hash_single_testing"."sys_op" -> "sys_op")
                            scan "hash_single_testing"
                    scan "ts"
                        projection ("test_space"."id" -> "id")
                            scan "test_space"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[cfg(test)]
mod params;
mod single;
