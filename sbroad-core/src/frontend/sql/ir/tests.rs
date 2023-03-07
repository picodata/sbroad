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
    projection (COL_0 -> COL_0, COL_1 -> COL_1, COL_2 -> COL_2, COL_3 -> COL_3, bucket_id((coalesce(('NULL', COL_0::string)) || coalesce(('NULL', COL_1::string)))))
        scan
            projection (COLUMN_1::unsigned -> COL_0, COLUMN_2::unsigned -> COL_1, COLUMN_3::unsigned -> COL_2, COLUMN_4::unsigned -> COL_3)
                scan
                    motion [policy: segment([ref(COLUMN_1), ref(COLUMN_2)])]
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
    projection (COL_0 -> COL_0, COL_1 -> COL_1, bucket_id((coalesce(('NULL', COL_0::string)) || coalesce(('NULL', NULL::string)))))
        scan
            projection (COLUMN_1::unsigned -> COL_0, COLUMN_2::unsigned -> COL_1)
                scan
                    motion [policy: segment([ref(COLUMN_1), value(NULL)])]
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
    projection (COL_0 -> COL_0, COL_1 -> COL_1, bucket_id((coalesce(('NULL', COL_0::string)) || coalesce(('NULL', NULL::string)))))
        scan
            projection ("t"."b"::unsigned -> COL_0, "t"."d"::unsigned -> COL_1)
                scan
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
fn front_sql_groupby() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing" group by "identification_number", "product_code""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"projection ("identification_number" -> "identification_number", "product_code" -> "product_code")
    group by ("hash_testing"."identification_number", "hash_testing"."product_code") output: ("identification_number" -> "identification_number", "product_code" -> "product_code")
        motion [policy: segment([ref("identification_number"), ref("product_code")])]
            scan 
                projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code")
                    group by ("hash_testing"."identification_number", "hash_testing"."product_code") output: ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code", "hash_testing"."product_units" -> "product_units", "hash_testing"."sys_op" -> "sys_op", "hash_testing"."bucket_id" -> "bucket_id")
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
        r#"projection ("identification_number" -> "identification_number")
    group by ("hash_testing"."identification_number", "hash_testing"."product_units") output: ("identification_number" -> "identification_number", "product_units" -> "product_units")
        motion [policy: segment([ref("identification_number"), ref("product_units")])]
            scan 
                projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_units" -> "product_units")
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
    projection ("identification_number" -> "identification_number")
        group by ("hash_testing"."identification_number") output: ("identification_number" -> "identification_number")
            motion [policy: segment([ref("identification_number")])]
                scan 
                    projection ("hash_testing"."identification_number" -> "identification_number")
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
                projection ("identification_number" -> "identification_number")
                    group by ("hash_testing"."identification_number") output: ("identification_number" -> "identification_number")
                        motion [policy: segment([ref("identification_number")])]
                            scan 
                                projection ("hash_testing"."identification_number" -> "identification_number")
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
        r#"projection ("product_code" -> "product_code", "product_units" -> "product_units")
    group by ("T2"."product_code", "T2"."product_units") output: ("product_code" -> "product_code", "product_units" -> "product_units")
        motion [policy: segment([ref("product_code"), ref("product_units")])]
            scan 
                projection ("T2"."product_code" -> "product_code", "T2"."product_units" -> "product_units")
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
fn front_sql_groupby_insert() {
    let input = r#"INSERT INTO "t" ("a", "c") SELECT "b", "d" FROM "t" group by "b", "d""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t"
    projection (COL_0 -> COL_0, COL_1 -> COL_1, bucket_id((coalesce(('NULL', COL_0::string)) || coalesce(('NULL', NULL::string)))))
        scan
            projection ("b"::unsigned -> COL_0, "d"::unsigned -> COL_1)
                scan
                    motion [policy: segment([ref("b"), value(NULL)])]
                        projection ("b" -> "b", "d" -> "d")
                            group by ("t"."b", "t"."d") output: ("b" -> "b", "d" -> "d")
                                motion [policy: segment([ref("b"), ref("d")])]
                                    scan 
                                        projection ("t"."b" -> "b", "t"."d" -> "d")
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
    let plan = ast.resolve_metadata(metadata);

    assert_eq!(true, plan.is_err());
}

#[test]
fn front_sql_aggregates() {
    let input = r#"SELECT "b", count("a") + count("b") FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("b" -> "b", (sum(("count_28"))) + (sum(("count_30"))) -> "COL_1")
    group by ("t"."b") output: ("b" -> "b", "count_28" -> "count_28", "count_30" -> "count_30")
        motion [policy: segment([ref("b")])]
            scan 
                projection ("t"."b" -> "b", count(("t"."a")) -> "count_28", count(("t"."b")) -> "count_30")
                    group by ("t"."b") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
                        scan "t"
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_aggregates_with_subexpressions() {
    let input = r#"SELECT "b", count("a" * "b" + 1), count(bucket_id("a")) FROM "t"
        group by "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("b" -> "b", sum(("count_35")) -> "COL_1", sum(("count_39")) -> "COL_2")
    group by ("t"."b") output: ("b" -> "b", "count_35" -> "count_35", "count_39" -> "count_39")
        motion [policy: segment([ref("b")])]
            scan 
                projection ("t"."b" -> "b", count((("t"."a") * ("t"."b") + (1))) -> "count_35", count(("BUCKET_ID"(("t"."a")))) -> "count_39")
                    group by ("t"."b") output: ("t"."a" -> "a", "t"."b" -> "b", "t"."c" -> "c", "t"."d" -> "d", "t"."bucket_id" -> "bucket_id")
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
    let err = ast.resolve_metadata(metadata).unwrap_err();

    assert_eq!("invalid query: aggregate function inside aggregate function is not allowed. Got `sum` inside `count`", err.to_string());
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

#[cfg(test)]
mod params;
