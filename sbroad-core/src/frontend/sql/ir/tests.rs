use crate::errors::SbroadError;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::operator::Relational;
use crate::ir::transformation::helpers::{sql_to_ir, sql_to_optimized_ir};
use crate::ir::tree::traversal::PostOrder;
use crate::ir::value::Value;
use crate::ir::Plan;
use crate::{executor::engine::mock::RouterConfigurationMock, ir::Positions};
use pretty_assertions::assert_eq;
use time::{format_description, OffsetDateTime, Time};

fn sql_to_optimized_ir_add_motions_err(query: &str) -> SbroadError {
    let metadata = &RouterConfigurationMock::new();
    let mut plan = AbstractSyntaxTree::transform_into_plan(query, metadata).unwrap();
    plan.replace_in_operator().unwrap();
    plan.push_down_not().unwrap();
    plan.split_columns().unwrap();
    plan.set_dnf().unwrap();
    plan.derive_equalities().unwrap();
    plan.merge_tuples().unwrap();
    plan.add_motions().unwrap_err()
}

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
    selection (ROW("t3"."identification_number"::integer) = ROW(1::unsigned) or (ROW("t3"."identification_number"::integer) = ROW(2::unsigned) or ROW("t3"."identification_number"::integer) = ROW(3::unsigned))) and (ROW("t3"."product_code"::string) = ROW('1'::string) or ROW("t3"."product_code"::string) = ROW('2'::string))
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
    motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
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
    let input = r#"INSERT INTO "t" ("b", "d") VALUES(1, 2)"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("COLUMN_1")])]
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
    let input = r#"INSERT INTO "t" ("b", "c") SELECT "b", "d" FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("b")])]
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
    selection not ROW("hash_testing"."product_code"::string) is null
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_with_additional_non_bool_value_from_left() {
    let input = r#"SELECT * FROM "test_space" WHERE 42 and 1 between 2 and 3"#;
    let mut plan = sql_to_ir(input, vec![]);
    let err = plan.optimize().unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("Left expression is not a boolean expression or NULL")
    );
}

#[test]
fn front_sql_between_with_additional_and_from_left() {
    // Was previously misinterpreted as
    //                  SELECT "id" FROM "test_space" as "t" WHERE ("t"."id" > 1 AND "t"."id") BETWEEN "t"."id" AND "t"."id" + 10
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE "t"."id" > 1 AND "t"."id" BETWEEN "t"."id" AND "t"."id" + 10"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection ROW("t"."id"::unsigned) > ROW(1::unsigned) and ROW("t"."id"::unsigned) >= ROW("t"."id"::unsigned) and ROW("t"."id"::unsigned) <= ROW("t"."id"::unsigned) + ROW(10::unsigned)
        scan "test_space" -> "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_with_additional_not_from_left() {
    // Was previously misinterpreted as
    //                  SELECT "id" FROM "test_space" as "t" WHERE (not "t"."id") BETWEEN "t"."id" AND "t"."id" + 10 and true
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE not "t"."id" BETWEEN "t"."id" AND "t"."id" + 10 and true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection not (ROW("t"."id"::unsigned) >= ROW("t"."id"::unsigned) and ROW("t"."id"::unsigned) <= ROW("t"."id"::unsigned) + ROW(10::unsigned)) and ROW(true::boolean)
        scan "test_space" -> "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_with_additional_and_from_left_and_right() {
    // Was previously misinterpreted as
    //                  SELECT "id" FROM "test_space" as "t" WHERE ("t"."id" > 1 AND "t"."id") BETWEEN "t"."id" AND "t"."id" + 10 AND true
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE "t"."id" > 1 AND "t"."id" BETWEEN "t"."id" AND "t"."id" + 10 AND true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection ROW("t"."id"::unsigned) > ROW(1::unsigned) and ROW("t"."id"::unsigned) >= ROW("t"."id"::unsigned) and ROW("t"."id"::unsigned) <= ROW("t"."id"::unsigned) + ROW(10::unsigned) and ROW(true::boolean)
        scan "test_space" -> "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_with_nested_not_from_the_left() {
    // `not not false between false and true` should be interpreted as
    // `not (not (false between false and true))`
    let input =
        r#"SELECT "id" FROM "test_space" as "t" WHERE not not false between false and true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection not not (ROW(false::boolean) >= ROW(false::boolean) and ROW(false::boolean) <= ROW(true::boolean))
        scan "test_space" -> "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_with_nested_and_from_the_left() {
    // `false and true and false between false and true` should be interpreted as
    // `(false and true) and (false between false and true)`
    let input = r#"SELECT "id" FROM "test_space" as "t" WHERE false and true and false between false and true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t"."id"::unsigned -> "id")
    selection ROW(false::boolean) and ROW(true::boolean) and ROW(false::boolean) >= ROW(false::boolean) and ROW(false::boolean) <= ROW(true::boolean)
        scan "test_space" -> "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_between_invalid() {
    let invalid_between_expressions = vec![
        r#"SELECT * FROM "test_space" WHERE 1 BETWEEN 2"#,
        r#"SELECT * FROM "test_space" WHERE 1 BETWEEN 2 OR 3"#,
    ];

    for invalid_expr in invalid_between_expressions {
        let metadata = &RouterConfigurationMock::new();
        let plan = AbstractSyntaxTree::transform_into_plan(invalid_expr, metadata);
        let err = plan.unwrap_err();

        assert_eq!(
            true,
            err.to_string().contains(
                "BETWEEN operator should have a view of `expr_1 BETWEEN expr_2 AND expr_3`."
            )
        );
    }
}

#[test]
fn front_sql_check_arbitrary_utf_in_single_quote_strings() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" = '«123»§#*&%@/// / // \\ ƵǖḘỺʥ ͑ ͑  ͕ΆΨѮښ ۞ܤ'"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number")
    selection ROW("hash_testing"."product_code"::string) = ROW('«123»§#*&%@/// / // \\ ƵǖḘỺʥ ͑ ͑  ͕ΆΨѮښ ۞ܤ'::string)
        scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_check_arbitraty_utf_in_identifiers() {
    let input = r#"SELECT "id" "from", "id" as "select", "id"
                               "123»&%ښ۞@Ƶǖselect.""''\\"
                                , "id" aц1&@$Ƶǖ%^&«»§&%ښ۞@Ƶǖ FROM "test_space" &%ښ۞@Ƶǖ"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("&%ښ۞@ƵǕ"."id"::unsigned -> "from", "&%ښ۞@ƵǕ"."id"::unsigned -> "select", "&%ښ۞@ƵǕ"."id"::unsigned -> "123»&%ښ۞@Ƶǖselect.""''\\", "&%ښ۞@ƵǕ"."id"::unsigned -> "AЦ1&@$ƵǕ%^&«»§&%ښ۞@ƵǕ")
    scan "test_space" -> "&%ښ۞@ƵǕ"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_check_inapplicatable_symbols() {
    let input = r#"
    SELECT a*a, B+B, a-a
    FROM TBL
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (ROW("TBL"."A"::unsigned) * ROW("TBL"."A"::unsigned) -> "COL_1", ROW("TBL"."B"::unsigned) + ROW("TBL"."B"::unsigned) -> "COL_2", ROW("TBL"."A"::unsigned) - ROW("TBL"."A"::unsigned) -> "COL_3")
    scan "TBL"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_projection_with_scan_specification_under_scan() {
    let input = r#"SELECT "hash_testing".* FROM "hash_testing""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
    scan "hash_testing"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_projection_with_scan_specification_under_join() {
    let input = r#"SELECT "hash_testing".* FROM "hash_testing" join "test_space" on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
    join on true::boolean
        scan "hash_testing"
            projection ("hash_testing"."identification_number"::integer -> "identification_number", "hash_testing"."product_code"::string -> "product_code", "hash_testing"."product_units"::boolean -> "product_units", "hash_testing"."sys_op"::unsigned -> "sys_op")
                scan "hash_testing"
        motion [policy: full]
            scan "test_space"
                projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_projection_with_scan_specification_under_join_of_subqueries() {
    let input = r#"SELECT "ts_sq".*, "hs".* FROM "hash_testing" as "hs"
                                join (select "ts".* from "test_space" as "ts") as "ts_sq" on true"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("ts_sq"."id"::unsigned -> "id", "ts_sq"."sysFrom"::unsigned -> "sysFrom", "ts_sq"."FIRST_NAME"::string -> "FIRST_NAME", "ts_sq"."sys_op"::unsigned -> "sys_op", "hs"."identification_number"::integer -> "identification_number", "hs"."product_code"::string -> "product_code", "hs"."product_units"::boolean -> "product_units", "hs"."sys_op"::unsigned -> "sys_op")
    join on true::boolean
        scan "hs"
            projection ("hs"."identification_number"::integer -> "identification_number", "hs"."product_code"::string -> "product_code", "hs"."product_units"::boolean -> "product_units", "hs"."sys_op"::unsigned -> "sys_op")
                scan "hash_testing" -> "hs"
        motion [policy: full]
            scan "ts_sq"
                projection ("ts"."id"::unsigned -> "id", "ts"."sysFrom"::unsigned -> "sysFrom", "ts"."FIRST_NAME"::string -> "FIRST_NAME", "ts"."sys_op"::unsigned -> "sys_op")
                    scan "test_space" -> "ts"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_with_simple_select() {
    let input = r#"select * from "test_space" order by "id""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("id"::unsigned -> "id", "sysFrom"::unsigned -> "sysFrom", "FIRST_NAME"::string -> "FIRST_NAME", "sys_op"::unsigned -> "sys_op")
    order by ("id"::unsigned)
        motion [policy: full]
            scan
                projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_with_param() {
    let input = r#"select * from "test_space" order by ?"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string().contains(
            "Using parameter as a standalone ORDER BY expression doesn't influence sorting"
        )
    );
}

#[test]
fn front_order_by_without_position_and_reference() {
    let input = r#"select * from "test_space" order by 1 + 8 asc, true and 'value'"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("ORDER BY element that is not position and doesn't contain reference doesn't influence ordering")
    );
}

#[test]
fn front_order_by_with_order_type_specification() {
    let input = r#"select * from "test_space" order by "id" desc, "sysFrom" asc"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("id"::unsigned -> "id", "sysFrom"::unsigned -> "sysFrom", "FIRST_NAME"::string -> "FIRST_NAME", "sys_op"::unsigned -> "sys_op")
    order by ("id"::unsigned desc, "sysFrom"::unsigned asc)
        motion [policy: full]
            scan
                projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_with_indices() {
    let input = r#"select * from "test_space" order by 2, 1 desc"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("id"::unsigned -> "id", "sysFrom"::unsigned -> "sysFrom", "FIRST_NAME"::string -> "FIRST_NAME", "sys_op"::unsigned -> "sys_op")
    order by (2, 1 desc)
        motion [policy: full]
            scan
                projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_ordering_by_expressions_from_projection() {
    let input =
        r#"select "id" as "my_col", "id" from "test_space" order by "my_col", "id", 1 desc, 2 asc"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("my_col"::unsigned -> "my_col", "id"::unsigned -> "id")
    order by ("my_col"::unsigned, "id"::unsigned, 1 desc, 2 asc)
        motion [policy: full]
            scan
                projection ("test_space"."id"::unsigned -> "my_col", "test_space"."id"::unsigned -> "id")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_order_by_with_indices_bigger_than_projection_output_length() {
    let input = r#"select "id" from "test_space" order by 1 asc, 2 desc, 3"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("Ordering index (2) is bigger than child projection output length (1).")
    );
}

#[test]
fn front_order_by_over_single_distribution_must_not_add_motion() {
    let input = r#"select "id_count" from
                        (select count("id") as "id_count" from "test_space")
                        order by "id_count""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("id_count"::integer -> "id_count")
    order by ("id_count"::integer)
        scan
            projection ("id_count"::integer -> "id_count")
                scan
                    projection (sum(("count_13"::integer))::decimal -> "id_count")
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
fn front_case_search() {
    let input = r#"select
                            case "id" when 1 then true end
                        from
                        "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (case "test_space"."id"::unsigned when 1::unsigned then true::boolean end -> "COL_1")
    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_case_simple() {
    let input = r#"select
                            case
                                when true = true then 'Moscow'
                                when 1 != 2 and 4 < 5 then 42
                                else false
                            end as "case_result"
                        from
                        "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (case when ROW(true::boolean) = ROW(true::boolean) then 'Moscow'::string when ROW(1::unsigned) <> ROW(2::unsigned) and ROW(4::unsigned) < ROW(5::unsigned) then 42::unsigned else false::boolean end -> "case_result")
    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_case_nested() {
    let input = r#"select
                            case "id"
                                when 1 then
                                    case "sysFrom"
                                        when 69 then true
                                        when 42 then false
                                    end
                                when 2 then 42
                                else false
                            end as "case_result"
                        from
                        "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (case "test_space"."id"::unsigned when 1::unsigned then case "test_space"."sysFrom"::unsigned when 69::unsigned then true::boolean when 42::unsigned then false::boolean end when 2::unsigned then 42::unsigned else false::boolean end -> "case_result")
    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_subquery_column_duplicates() {
    let input = r#"SELECT "id" FROM "test_space" WHERE ("id", "id")
        IN (SELECT "id", "id" from "test_space")"#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."id"::unsigned -> "id")
    selection ROW("test_space"."id"::unsigned, "test_space"."id"::unsigned) in ROW($0, $0)
        scan "test_space"
subquery $0:
scan
            projection ("test_space"."id"::unsigned -> "id", "test_space"."id"::unsigned -> "id")
                scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

impl Plan {
    fn get_positions(&self, node_id: usize) -> Option<Positions> {
        let mut context = self.context_mut();
        context
            .get_shard_columns_positions(node_id, self)
            .unwrap()
            .copied()
    }
}

#[test]
fn track_shard_col_pos() {
    let input = r#"
    select "e", "bucket_id", "f"
    from "t2"
    where "e" + "f" = 3
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    let mut dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), 10);
    for (_, node_id) in dfs.iter(top) {
        let node = plan.get_relation_node(node_id).unwrap();
        match node {
            Relational::ScanRelation { .. } | Relational::Selection { .. } => {
                assert_eq!([Some(4_usize), None], plan.get_positions(node_id).unwrap())
            }
            Relational::Projection { .. } => {
                assert_eq!([Some(1_usize), None], plan.get_positions(node_id).unwrap())
            }
            _ => {}
        }
    }

    let input = r#"select t_mv."bucket_id", "t2"."bucket_id" from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" = "t2"."bucket_id";
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    let mut dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), 10);
    for (_, node_id) in dfs.iter(top) {
        let node = plan.get_relation_node(node_id).unwrap();
        if let Relational::Join { .. } = node {
            assert_eq!(
                [Some(4_usize), Some(5_usize)],
                plan.get_positions(node_id).unwrap()
            );
        }
    }
    assert_eq!(
        [Some(0_usize), Some(1_usize)],
        plan.get_positions(top).unwrap()
    );

    let input = r#"select t_mv."bucket_id", "t2"."bucket_id" from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" < "t2"."bucket_id";
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    let mut dfs = PostOrder::with_capacity(|x| plan.nodes.rel_iter(x), 10);
    for (_, node_id) in dfs.iter(top) {
        let node = plan.get_relation_node(node_id).unwrap();
        if let Relational::Join { .. } = node {
            assert_eq!([Some(4_usize), None], plan.get_positions(node_id).unwrap());
        }
    }
    assert_eq!([Some(1_usize), None], plan.get_positions(top).unwrap());

    let input = r#"
    select "bucket_id", "e" from "t2"
    union all
    select "id", "bucket_id" from "test_space"
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!(None, plan.get_positions(top));

    let input = r#"
    select "bucket_id", "e" from "t2"
    union all
    select "bucket_id", "id" from "test_space"
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!([Some(0_usize), None], plan.get_positions(top).unwrap());

    let input = r#"
    select "e" from (select "bucket_id" as "e" from "t2")
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!([Some(0_usize), None], plan.get_positions(top).unwrap());

    let input = r#"
    select "e" as "bucket_id" from "t2"
    "#;
    let plan = sql_to_optimized_ir(input, vec![]);
    let top = plan.get_top().unwrap();
    assert_eq!(None, plan.get_positions(top));
}

#[test]
fn front_sql_join_on_bucket_id1() {
    let input = r#"select * from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" = "t2"."bucket_id";
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
    join on ROW("T_MV"."bucket_id"::unsigned) = ROW("t2"."bucket_id"::unsigned)
        scan "t2"
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h", "t2"."bucket_id"::unsigned -> "bucket_id")
                scan "t2"
        scan "T_MV"
            projection ("test_space"."bucket_id"::unsigned -> "bucket_id")
                selection ROW("test_space"."id"::unsigned) = ROW(1::unsigned)
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_join_on_bucket_id2() {
    let input = r#"select * from "t2" join (
        select "bucket_id" from "test_space" where "id" = 1
    ) as t_mv
    on t_mv."bucket_id" = "t2"."bucket_id" or "t2"."e" = "t2"."f";
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h")
    join on ROW("T_MV"."bucket_id"::unsigned) = ROW("t2"."bucket_id"::unsigned) or ROW("t2"."e"::unsigned) = ROW("t2"."f"::unsigned)
        scan "t2"
            projection ("t2"."e"::unsigned -> "e", "t2"."f"::unsigned -> "f", "t2"."g"::unsigned -> "g", "t2"."h"::unsigned -> "h", "t2"."bucket_id"::unsigned -> "bucket_id")
                scan "t2"
        motion [policy: full]
            scan "T_MV"
                projection ("test_space"."bucket_id"::unsigned -> "bucket_id")
                    selection ROW("test_space"."id"::unsigned) = ROW(1::unsigned)
                        scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_groupby_on_bucket_id() {
    let input = r#"
    select b, count(*) from (select "bucket_id" as b from "t2") as t
    group by b
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("T"."B"::unsigned -> "B", count((*::integer))::integer -> "COL_1")
    group by ("T"."B"::unsigned) output: ("T"."B"::unsigned -> "B")
        scan "T"
            projection ("t2"."bucket_id"::unsigned -> "B")
                scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_sq_on_bucket_id() {
    let input = r#"
    select b, e from (select "bucket_id" as b, "e" as e from "t2") as t
    where (b, e) in (select "bucket_id", "id" from "test_space")
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("T"."B"::unsigned -> "B", "T"."E"::unsigned -> "E")
    selection ROW("T"."B"::unsigned, "T"."E"::unsigned) in ROW($0, $0)
        scan "T"
            projection ("t2"."bucket_id"::unsigned -> "B", "t2"."e"::unsigned -> "E")
                scan "t2"
subquery $0:
scan
            projection ("test_space"."bucket_id"::unsigned -> "bucket_id", "test_space"."id"::unsigned -> "id")
                scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_except_on_bucket_id() {
    let input = r#"
    select "e", "bucket_id" from "t2"
    except
    select "id", "bucket_id" from "test_space"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    projection ("t2"."e"::unsigned -> "e", "t2"."bucket_id"::unsigned -> "bucket_id")
        scan "t2"
    projection ("test_space"."id"::unsigned -> "id", "test_space"."bucket_id"::unsigned -> "bucket_id")
        scan "test_space"
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
    join on ROW("T2"."identification_number"::integer) = ROW("T"."id"::unsigned) and not ROW("T"."id"::unsigned) is null
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
        ON t1."identification_number" = t2."id" and not t2."id" is null
        "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection ("T1"."product_code"::string -> "product_code", "T1"."product_units"::boolean -> "product_units")
    join on ROW("T1"."identification_number"::integer) = ROW("T2"."id"::unsigned) and not ROW("T2"."id"::unsigned) is null
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
    join on ROW("T1"."identification_number"::integer) = ROW("T2"."id"::decimal) and not ROW("T2"."id"::decimal) is null
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
    let input = r#"INSERT INTO "t" ("c", "b")
    SELECT "b", "d" FROM "t" group by "b", "d" ON CONFLICT DO FAIL"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("d")])]
        projection ("column_12"::unsigned -> "b", "column_13"::unsigned -> "d")
            group by ("column_12"::unsigned, "column_13"::unsigned) output: ("column_12"::unsigned -> "column_12", "column_13"::unsigned -> "column_13")
                motion [policy: segment([ref("column_12"), ref("column_13")])]
                    scan
                        projection ("t"."b"::unsigned -> "column_12", "t"."d"::unsigned -> "column_13")
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
    let mut plan = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap();
    let res = plan.optimize();

    assert_eq!(true, res.is_err());
}

#[test]
fn front_sql_distinct_invalid() {
    let input = r#"select "b", bucket_id(distinct cast("a" as string)) from "t" group by "b", bucket_id(distinct cast("a" as string))"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
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
        r#"projection ("column_12"::unsigned -> "b", ROW(sum(("count_29"::integer))::decimal) + ROW(sum(("count_31"::integer))::decimal) -> "COL_1")
    group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "count_29"::integer -> "count_29", "count_31"::integer -> "count_31")
        motion [policy: segment([ref("column_12")])]
            scan
                projection ("t"."b"::unsigned -> "column_12", count(("t"."a"::unsigned))::integer -> "count_29", count(("t"."b"::unsigned))::integer -> "count_31")
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
        r#"projection (sum(("sum_13"::decimal::double))::decimal / sum(("count_13"::decimal::double))::decimal -> "COL_1", avg(distinct ("column_15"::decimal::double))::decimal -> "COL_2", ROW(sum(("sum_13"::decimal::double))::decimal / sum(("count_13"::decimal::double))::decimal) * ROW(sum(("sum_13"::decimal::double))::decimal / sum(("count_13"::decimal::double))::decimal) -> "COL_3")
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
        r#"projection (min(("min_13"::unsigned))::scalar -> "COL_1", min(distinct ("column_15"::unsigned))::scalar -> "COL_2")
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
        r#"projection (max(("max_13"::unsigned))::scalar -> "COL_1", max(distinct ("column_15"::unsigned))::scalar -> "COL_2")
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
    let input = r#"SELECT group_concat("FIRST_NAME"), group_concat(distinct "FIRST_NAME") FROM "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (group_concat(("group_concat_13"::string))::string -> "COL_1", group_concat(distinct ("column_15"::string))::string -> "COL_2")
    motion [policy: full]
        scan
            projection ("test_space"."FIRST_NAME"::string -> "column_15", group_concat(("test_space"."FIRST_NAME"::string))::string -> "group_concat_13")
                group by ("test_space"."FIRST_NAME"::string) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_group_concat_aggregate2() {
    let input = r#"SELECT group_concat("FIRST_NAME", ' '), group_concat(distinct "FIRST_NAME") FROM "test_space""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    println!("{}", plan.as_explain().unwrap());
    let expected_explain = String::from(
        r#"projection (group_concat(("group_concat_14"::string, ' '::string))::string -> "COL_1", group_concat(distinct ("column_16"::string))::string -> "COL_2")
    motion [policy: full]
        scan
            projection ("test_space"."FIRST_NAME"::string -> "column_16", group_concat(("test_space"."FIRST_NAME"::string, ' '::string))::string -> "group_concat_14")
                group by ("test_space"."FIRST_NAME"::string) output: ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op", "test_space"."bucket_id"::unsigned -> "bucket_id")
                    scan "test_space"
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
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
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
                projection ("t"."b"::unsigned -> "column_12", count(("FUNC"(("t"."a"::unsigned))::integer))::integer -> "count_39", count((ROW("t"."a"::unsigned) * ROW("t"."b"::unsigned) + ROW(1::unsigned)))::integer -> "count_35")
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
                projection ("t"."b"::unsigned -> "column_12", ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) + ROW(3::unsigned) -> "column_34")
                    group by ("t"."b"::unsigned, ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) + ROW(3::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
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
            projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) + ROW(3::unsigned) -> "column_19")
                group by (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) + ROW(3::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
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
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata)
        .unwrap()
        .optimize()
        .unwrap_err();

    assert_eq!(
        "invalid query: aggregate function inside aggregate function is not allowed.",
        err.to_string()
    );
}

#[test]
fn front_sql_column_outside_aggregate_no_groupby() {
    let input = r#"select "b", count("a") from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata)
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
fn front_sql_pg_style_params1() {
    let input = r#"select $1, $2, $1 from "t""#;

    let plan = sql_to_optimized_ir(
        input,
        vec![Value::Unsigned(1000), Value::String("hi".into())],
    );
    let expected_explain = String::from(
        r#"projection (1000::unsigned -> "COL_1", 'hi'::string -> "COL_2", 1000::unsigned -> "COL_3")
    scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_pg_style_params2() {
    let input =
        r#"select $1, $2, $1 from "t" option(sql_vdbe_max_steps = $1, vtable_max_rows = $1)"#;

    let plan = sql_to_optimized_ir(
        input,
        vec![Value::Unsigned(1000), Value::String("hi".into())],
    );
    let expected_explain = String::from(
        r#"projection (1000::unsigned -> "COL_1", 'hi'::string -> "COL_2", 1000::unsigned -> "COL_3")
    scan "t"
execution options:
sql_vdbe_max_steps = 1000
vtable_max_rows = 1000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_pg_style_params3() {
    let input = r#"select "a" + $1 from "t"
        where "a" = $1
        group by "a" + $1
        having count("b") > $1
        option(sql_vdbe_max_steps = $1, vtable_max_rows = $1)"#;

    let plan = sql_to_optimized_ir(input, vec![Value::Unsigned(42)]);
    let expected_explain = String::from(
        r#"projection ("column_31"::unsigned -> "COL_1")
    having ROW(sum(("count_46"::integer))::decimal) > ROW(42::unsigned)
        group by ("column_31"::unsigned) output: ("column_31"::unsigned -> "column_31", "count_46"::integer -> "count_46")
            motion [policy: segment([ref("column_31")])]
                scan
                    projection (ROW("t"."a"::unsigned) + ROW(42::unsigned) -> "column_31", count(("t"."b"::unsigned))::integer -> "count_46")
                        group by (ROW("t"."a"::unsigned) + ROW(42::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
                            selection ROW("t"."a"::unsigned) = ROW(42::unsigned)
                                scan "t"
execution options:
sql_vdbe_max_steps = 42
vtable_max_rows = 42
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_pg_style_params4() {
    let input = r#"select $1, ? from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap_err();

    assert_eq!(
        "invalid parameters usage. Got $n and ? parameters in one query!",
        err.to_string()
    );
}

#[test]
fn front_sql_pg_style_params5() {
    let input = r#"select $0 from "t""#;

    let metadata = &RouterConfigurationMock::new();
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata).unwrap_err();

    assert_eq!(
        "invalid query: $n parameters are indexed from 1!",
        err.to_string()
    );
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
    let err = AbstractSyntaxTree::transform_into_plan(input, metadata)
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
            projection (sum((ROW("t"."a"::unsigned) * ROW("t"."b"::unsigned) + ROW(1::unsigned)))::decimal -> "sum_20")
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
fn front_sql_aggregate_on_aggregate() {
    let input = r#"SELECT max(c) FROM (SELECT count("id") as c FROM "test_space") as "t1""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"projection (max(("t1"."C"::integer))::scalar -> "COL_1")
    scan "t1"
        projection (sum(("count_13"::integer))::decimal -> "C")
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
    let input = r#"INSERT INTO "t" ("c", "b") SELECT sum("b"), count("d") FROM "t""#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"insert "t" on conflict: fail
    motion [policy: segment([value(NULL), ref("COL_2")])]
        projection (sum(("sum_13"::decimal))::decimal -> "COL_1", sum(("count_16"::integer))::decimal -> "COL_2")
            motion [policy: full]
                scan
                    projection (sum(("t"."b"::unsigned))::decimal -> "sum_13", count(("t"."d"::unsigned))::integer -> "count_16")
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
                projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "column_16")
                    group by (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
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
        r#"projection ("column_17"::unsigned + ROW(sum(("count_37"::integer))::decimal) -> "COL_1")
    group by ("column_17"::unsigned) output: ("column_17"::unsigned -> "column_17", "count_37"::integer -> "count_37")
        motion [policy: segment([ref("column_17")])]
            scan
                projection ((ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned)) -> "column_17", count(("t"."a"::unsigned))::integer -> "count_37")
                    group by ((ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned))) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
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
        r#"projection ("column_16"::unsigned -> "COL_1", "column_27"::unsigned * ROW(sum(("sum_59"::decimal))::decimal) / ROW(sum(("count_65"::integer))::decimal) -> "COL_2")
    group by ("column_27"::unsigned, "column_16"::unsigned) output: ("column_16"::unsigned -> "column_16", "column_27"::unsigned -> "column_27", "count_65"::integer -> "count_65", "sum_59"::decimal -> "sum_59")
        motion [policy: segment([ref("column_27"), ref("column_16")])]
            scan
                projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "column_16", (ROW("t"."c"::unsigned) * ROW("t"."d"::unsigned)) -> "column_27", count((ROW("t"."a"::unsigned) * ROW("t"."b"::unsigned)))::integer -> "count_65", sum((ROW("t"."c"::unsigned) * ROW("t"."d"::unsigned)))::decimal -> "sum_59")
                    group by ((ROW("t"."c"::unsigned) * ROW("t"."d"::unsigned)), ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
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
    group by ("column_17"::unsigned, "column_16"::unsigned) output: ("column_17"::unsigned -> "column_17", "column_16"::unsigned -> "column_16")
        motion [policy: segment([ref("column_17"), ref("column_16")])]
            scan
                projection ("t"."a"::unsigned -> "column_17", ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "column_16")
                    group by ("t"."a"::unsigned, ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
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
                projection (ROW(sum(("sum_14"::decimal))::decimal) / ROW(3::unsigned) -> "A")
                    motion [policy: full]
                        scan
                            projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_14")
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
                projection (ROW(sum(("sum_14"::decimal))::decimal) / ROW(3::unsigned) -> "A")
                    motion [policy: full]
                        scan
                            projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_14")
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
        scan "T1"
            projection (ROW(sum(("sum_14"::decimal))::decimal) / ROW(3::unsigned) -> "A")
                motion [policy: full]
                    scan
                        projection (sum(("test_space"."id"::unsigned))::decimal -> "sum_14")
                            scan "test_space"
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
    having ROW("column_12"::unsigned) > ROW(1::unsigned) and ROW(sum(distinct ("column_27"::decimal))::decimal) > ROW(1::unsigned)
        group by ("column_12"::unsigned) output: ("column_12"::unsigned -> "column_12", "column_27"::unsigned -> "column_27", "sum_52"::decimal -> "sum_52")
            motion [policy: segment([ref("column_12")])]
                scan
                    projection ("t"."a"::unsigned -> "column_12", "t"."b"::unsigned -> "column_27", sum(("t"."b"::unsigned))::decimal -> "sum_52")
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
        r#"projection (ROW(sum(("sum_39"::decimal))::decimal) * ROW(count(distinct ("column_38"::integer))::integer) -> "COL_1", sum(("sum_39"::decimal))::decimal -> "COL_2")
    having ROW(sum(distinct ("column_38"::decimal))::decimal) > ROW(1::unsigned) and ROW(sum(("sum_39"::decimal))::decimal) > ROW(1::unsigned)
        motion [policy: full]
            scan
                projection ("t"."b"::unsigned -> "column_38", sum(("t"."a"::unsigned))::decimal -> "sum_39")
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

    let err = sql_to_optimized_ir_add_motions_err(input);

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

    let err = sql_to_optimized_ir_add_motions_err(input);

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
        r#"projection (sum(("sum_13"::decimal))::decimal -> "COL_1", sum(("count_16"::integer))::decimal -> "COL_2", ROW(sum(("sum_13"::decimal))::decimal) + ROW(sum(("count_16"::integer))::decimal) -> "COL_3")
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
                projection ("t"."a"::unsigned -> "column_22", ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> "column_27")
                    group by ("t"."a"::unsigned, ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned)) output: ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d", "t"."bucket_id"::unsigned -> "bucket_id")
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

    let err = sql_to_optimized_ir_add_motions_err(input);

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
    motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
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
    motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
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
    motion [policy: segment([ref("COLUMN_5"), ref("COLUMN_6")])]
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
    let input = r#"insert into "hash_testing" ("identification_number", "product_code", "bucket_id") values (1, 2, 3)"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
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
    motion [policy: segment([ref("COLUMN_1"), ref("COLUMN_2")])]
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
fn front_sql_update1() {
    let input = r#"update "t" set "a" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t"
"b" = COL_1
"d" = COL_3
"a" = COL_0
"c" = COL_2
    motion [policy: segment([])]
        projection (1::unsigned -> COL_0, "t"."b"::unsigned -> COL_1, "t"."c"::unsigned -> COL_2, "t"."d"::unsigned -> COL_3, "t"."a"::unsigned -> COL_4, "t"."b"::unsigned -> COL_5)
            scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update2() {
    let input = r#"update "t" set "c" = "a" + "b""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t"
"c" = COL_0
    motion [policy: local]
        projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> COL_0, "t"."b"::unsigned -> COL_1)
            scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update3() {
    let input = r#"update "t" set "c" = "a" + "b" where "c" = 1"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t"
"c" = COL_0
    motion [policy: local]
        projection (ROW("t"."a"::unsigned) + ROW("t"."b"::unsigned) -> COL_0, "t"."b"::unsigned -> COL_1)
            selection ROW("t"."c"::unsigned) = ROW(1::unsigned)
                scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update4() {
    let input = r#"update "t" set
    "d" = "b1"*2,
    "c" = "b1"*2
    from (select "a" as "a1", "b" as "b1" from "t1")
    where "c" = "b1""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t"
"d" = COL_0
"c" = COL_0
    motion [policy: local]
        projection (ROW("b1"::integer) * ROW(2::unsigned) -> COL_0, "t"."b"::unsigned -> COL_1)
            join on ROW("t"."c"::unsigned) = ROW("b1"::integer)
                scan "t"
                    projection ("t"."a"::unsigned -> "a", "t"."b"::unsigned -> "b", "t"."c"::unsigned -> "c", "t"."d"::unsigned -> "d")
                        scan "t"
                motion [policy: full]
                    scan
                        projection ("t1"."a"::string -> "a1", "t1"."b"::integer -> "b1")
                            scan "t1"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update5() {
    let input = r#"update "t3" set
    "b" = "id"
    from "test_space"
    where "a" = "id""#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t3"
"b" = COL_0
    motion [policy: local]
        projection ("test_space"."id"::unsigned -> COL_0, "t3"."a"::string -> COL_1)
            join on ROW("t3"."a"::string) = ROW("test_space"."id"::unsigned)
                scan "t3"
                    projection ("t3"."a"::string -> "a", "t3"."b"::integer -> "b")
                        scan "t3"
                scan "test_space"
                    projection ("test_space"."id"::unsigned -> "id", "test_space"."sysFrom"::unsigned -> "sysFrom", "test_space"."FIRST_NAME"::string -> "FIRST_NAME", "test_space"."sys_op"::unsigned -> "sys_op")
                        scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_update6() {
    let input = r#"update "t3" set
    "b" = 2
    where "b" in (select sum("b") as s from "t3")"#;

    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = String::from(
        r#"update "t3"
"b" = COL_0
    motion [policy: local]
        projection (2::unsigned -> COL_0, "t3"."a"::string -> COL_1)
            selection ROW("t3"."b"::integer) in ROW($0)
                scan "t3"
subquery $0:
motion [policy: full]
                    scan
                        projection (sum(("sum_17"::decimal))::decimal -> "S")
                            motion [policy: full]
                                scan
                                    projection (sum(("t3"."b"::integer))::decimal -> "sum_17")
                                        scan "t3"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_not_true() {
    assert_explain_eq(
        r#"
            SELECT "a" FROM "t" WHERE not true
        "#,
        vec![],
        r#"projection ("t"."a"::unsigned -> "a")
    selection not ROW(true::boolean)
        scan "t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_equal() {
    assert_explain_eq(
        r#"
            SELECT * FROM (VALUES (1)) where not true = true
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not ROW(true::boolean) = ROW(true::boolean)
        scan
            values
                value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_cast() {
    assert_explain_eq(
        r#"
            SELECT * FROM (values (1)) where not cast('true' as boolean)
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not 'true'::string::bool
        scan
            values
                value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn from_sql_not_column() {
    assert_explain_eq(
        r#"
            SELECT * FROM (values (true)) where not "COLUMN_1"
        "#,
        vec![],
        r#"projection ("COLUMN_1"::boolean -> "COLUMN_1")
    selection not "COLUMN_1"::boolean
        scan
            values
                value row (data=ROW(true::boolean))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_or() {
    assert_explain_eq(
        r#"
            SELECT * FROM (values (1)) where not true or true
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not ROW(true::boolean) or ROW(true::boolean)
        scan
            values
                value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_and_with_parentheses() {
    assert_explain_eq(
        r#"
            SELECT not (true and false) FROM (values (1))
        "#,
        vec![],
        r#"projection (not (ROW(true::boolean) and ROW(false::boolean)) -> "COL_1")
    scan
        values
            value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_or_with_parentheses() {
    assert_explain_eq(
        r#"
            SELECT * FROM (values (1)) where not (true or true)
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not (ROW(true::boolean) or ROW(true::boolean))
        scan
            values
                value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_exists() {
    assert_explain_eq(
        r#"
            select * from (values (1)) where not exists (select * from (values (1)))
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not exists ROW($0)
        scan
            values
                value row (data=ROW(1::unsigned))
subquery $0:
scan
            projection ("COLUMN_2"::unsigned -> "COLUMN_2")
                scan
                    values
                        value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_in() {
    assert_explain_eq(
        r#"
            select * from (values (1)) where 1 not in (select * from (values (1)))
        "#,
        vec![],
        r#"projection ("COLUMN_1"::unsigned -> "COLUMN_1")
    selection not ROW(1::unsigned) in ROW($0)
        scan
            values
                value row (data=ROW(1::unsigned))
subquery $0:
motion [policy: full]
            scan
                projection ("COLUMN_2"::unsigned -> "COLUMN_2")
                    scan
                        values
                            value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_not_complex_query() {
    assert_explain_eq(
        r#"
            select not (not (cast('true' as boolean)) and 1 + (?) != false)
            from
                (select not "id" as "nid" from "test_space") as "ts"
                inner join
                (select not not "id" as "nnid" from "test_space") as "nts"
                on not "nid" = "nnid" * (?) or not false = cast((not not true) as bool)
            where not exists (select * from (values (1)) where not true = (?))
        "#,
        vec![Value::from(1), Value::from(1), Value::from(true)],
        r#"projection (not (not ('true'::string::bool) and ROW(1::unsigned) + (1::integer) <> ROW(false::boolean)) -> "COL_1")
    selection not exists ROW($0)
        join on ROW("ts"."nid"::boolean) <> ROW("nts"."nnid"::boolean) * (1::integer) or ROW(false::boolean) <> ROW((not not ROW(true::boolean))::bool)
            scan "ts"
                projection (not ROW("test_space"."id"::unsigned) -> "nid")
                    scan "test_space"
            motion [policy: full]
                scan "nts"
                    projection (not not ROW("test_space"."id"::unsigned) -> "nnid")
                        scan "test_space"
subquery $0:
scan
            projection ("COLUMN_1"::unsigned -> "COLUMN_1")
                selection not ROW(true::boolean) = (true::boolean)
                    scan
                        values
                            value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_arithmetic_with_parentheses() {
    assert_explain_eq(
        r#"
            SELECT (1 + 2) * 3 FROM (values (1))
        "#,
        vec![],
        r#"projection ((ROW(1::unsigned) + ROW(2::unsigned)) * ROW(3::unsigned) -> "COL_1")
    scan
        values
            value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_to_date() {
    assert_explain_eq(
        r#"
            SELECT to_date(COLUMN_1, '%Y/%d/%m') FROM (values ('2010/10/10'))
        "#,
        vec![],
        r#"projection ("TO_DATE"(("COLUMN_1"::string, '%Y/%d/%m'::string))::datetime -> "COL_1")
    scan
        values
            value row (data=ROW('2010/10/10'::string))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    )
}

#[test]
fn front_sql_current_date() {
    let input = r#"
    SELECT current_date FROM (values ('2010/10/10'))
    where to_date('2010/10/10', '%Y/%d/%m') < current_Date"#;

    let today = OffsetDateTime::now_utc().replace_time(Time::MIDNIGHT);
    let format = format_description::parse("[year]-[month]-[day]").unwrap();
    let plan = sql_to_optimized_ir(input, vec![]);
    let expected_explain = format!(
        r#"projection ({today} 0:00:00.0 +00:00:00::datetime -> "COL_1")
    selection ROW("TO_DATE"(('2010/10/10'::string, '%Y/%d/%m'::string))::datetime) < ROW({today} 0:00:00.0 +00:00:00::datetime)
        scan
            values
                value row (data=ROW('2010/10/10'::string))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
        today = today.format(&format).unwrap()
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_sql_check_non_null_columns_specified() {
    let input = r#"insert into "test_space" ("sys_op") values (1)"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();
    assert_eq!(
        true,
        err.to_string()
            .contains("NonNull column \"id\" must be specified")
    );
}

fn assert_explain_eq(query: &str, params: Vec<Value>, expected: &str) {
    let plan = sql_to_optimized_ir(query, params);
    let actual = plan.as_explain().unwrap();
    assert_eq!(expected, actual);
}

#[test]
fn non_existent_references_in_values_do_not_panic() {
    // scenario: somebody mixed up " with '
    let input = r#"insert into "test_space" values(1, "nonexistent_reference")"#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert!(err
        .to_string()
        .contains("Reference must point to some relational node"));
}

#[test]
fn front_count_no_params() {
    let input = r#"select count() from "test_space""#;

    let metadata = &RouterConfigurationMock::new();
    let plan = AbstractSyntaxTree::transform_into_plan(input, metadata);
    let err = plan.unwrap_err();

    assert_eq!(
        true,
        err.to_string()
            .contains("invalid query: Expected one argument for aggregate: count")
    );
}

#[test]
fn front_mock_set_param_transaction() {
    let queries_to_check = vec![
        r#"set session default_param = default"#,
        r#"set session stringparam = 'value'"#,
        r#"set session identparam to ident"#,
        r#"set local intparam to -3"#,
        r#"set local doubleparam = -42.5"#,
        r#"set doubleparam = -42.5"#,
        r#"set local time zone local"#,
        r#"set time zone -3"#,
        r#"set time zone 'value'"#,
        r#"SET search_path TO my_schema, public;"#,
        r#"SET datestyle TO postgres, dmy;"#,
        r#"SET TIME ZONE 'PST8PDT';"#,
        r#"SET TIME ZONE 'Europe/Rome';"#,
        r#"SET param To list, 'of', 4, valuez;"#,
        r#"set transaction snapshot 'snapshot-string'"#,
        r#"set transaction read write"#,
        r#"set transaction read only"#,
        r#"set transaction deferrable"#,
        r#"set transaction not deferrable"#,
        r#"set transaction isolation level serializable"#,
        r#"set transaction isolation level repeatable read"#,
        r#"set transaction isolation level read commited"#,
        r#"set transaction isolation level read uncommited"#,
        r#"set session characteristics as transaction read only"#,
        r#"set session characteristics as transaction isolation level read commited"#,
    ];

    let metadata = &RouterConfigurationMock::new();
    for query in queries_to_check {
        let plan = AbstractSyntaxTree::transform_into_plan(query, metadata);
        assert!(plan.is_ok())
    }
}

#[cfg(test)]
mod cte;
#[cfg(test)]
mod ddl;
#[cfg(test)]
mod global;
#[cfg(test)]
mod insert;
#[cfg(test)]
mod join;
#[cfg(test)]
mod params;
#[cfg(test)]
mod single;
#[cfg(test)]
mod trim;
#[cfg(test)]
mod union;
#[cfg(test)]
mod update;
