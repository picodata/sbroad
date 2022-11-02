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
motion [policy: full, generation: none]
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
            motion [policy: full, generation: none]
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
            motion [policy: segment([ref("identification_number")]), generation: none]
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
    motion [policy: segment([ref(COLUMN_1), ref(COLUMN_2)]), generation: sharding_column]
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
    motion [policy: segment([ref(COLUMN_1), value(NULL)]), generation: sharding_column]
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
    motion [policy: segment([ref("b"), value(NULL)]), generation: sharding_column]
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

#[cfg(test)]
mod params;
