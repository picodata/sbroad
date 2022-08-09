use crate::errors::QueryPlannerError;
use crate::executor::engine::cartridge::backend::sql::ir::PatternWithParams;
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn no_transform(_plan: &mut Plan) {}

#[test]
fn front_sql1() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" = 1"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT "hash_testing"."identification_number","#,
            r#""hash_testing"."product_code""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."identification_number") = (?)"#,
        ),
        vec![Value::from(1_u64)],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_sql2() {
    let input = r#"SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "identification_number" = 1 AND "product_code" = '1'
        OR "identification_number" = 2 AND "product_code" = '2'"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT "hash_testing"."identification_number", "hash_testing"."product_code""#,
            r#"FROM "hash_testing" WHERE (("hash_testing"."identification_number") = (?) and ("hash_testing"."product_code") = (?)"#,
            r#"or ("hash_testing"."identification_number") = (?) and ("hash_testing"."product_code") = (?))"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from("1"),
            Value::from(2_u64),
            Value::from("2"),
        ],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
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
    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {}",
            r#"SELECT "t3"."identification_number", "t3"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."identification_number", "hash_testing"."product_code""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."sys_op") = (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."identification_number", "hash_testing_hist"."product_code""#,
            r#"FROM "hash_testing_hist" WHERE ("hash_testing_hist"."sys_op") > (?)) as "t3""#,
            r#"WHERE ("t3"."identification_number") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from(1_u64), Value::from(1_u64)],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
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
    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {} {}",
            r#"SELECT "t3"."identification_number", "t3"."product_code" FROM"#,
            r#"(SELECT "hash_testing"."identification_number", "hash_testing"."product_code""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."sys_op") = (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."identification_number", "hash_testing_hist"."product_code""#,
            r#"FROM "hash_testing_hist" WHERE ("hash_testing_hist"."sys_op") > (?)) as "t3""#,
            r#"WHERE (("t3"."identification_number") = (?) or (("t3"."identification_number") = (?) or ("t3"."identification_number") = (?)))"#,
            r#"and (("t3"."product_code") = (?) or ("t3"."product_code") = (?))"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(3_u64),
            Value::from("1"),
            Value::from("2"),
        ],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_sql5() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "identification_number" in (
        SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'a')"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {}",
            r#"SELECT "hash_testing"."identification_number", "hash_testing"."product_code""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."identification_number") in"#,
            r#"(SELECT "hash_testing_hist"."identification_number" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = (?))"#,
        ),
        vec![Value::from("a")],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_sql6() {
    let input = r#"SELECT "id", "product_units" FROM "hash_testing"
        INNER JOIN (SELECT "id" FROM "test_space") as t
        ON "hash_testing"."identification_number" = t."id"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {} {}",
            r#"SELECT t."id", "hash_testing"."product_units""#,
            r#"FROM (SELECT "hash_testing"."identification_number","#,
            r#""hash_testing"."product_code","#,
            r#""hash_testing"."product_units","#,
            r#""hash_testing"."sys_op" FROM "hash_testing") as "hash_testing""#,
            r#"INNER JOIN (SELECT "test_space"."id" FROM "test_space") as t"#,
            r#"ON ("hash_testing"."identification_number") = (t."id")"#,
            r#"WHERE ("hash_testing"."identification_number") = (?) and ("hash_testing"."product_code") = (?)"#,
        ),
        vec![Value::from(5_u64), Value::from("123")],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_sql7() {
    // Tables "test_space" and "hash_testing" have the same columns "sys_op" and "bucket_id",
    // that cause a "duplicate column" error in the output tuple of the INNER JOIN node.
    // The error is handled by renaming the columns within a sub-query (inner_join test).
    //
    // TODO: improve reference resolution to avoid this error.
    let query = r#"SELECT "id", "product_units" FROM "hash_testing"
        INNER JOIN "test_space" as t
        ON "hash_testing"."identification_number" = t."id"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan_err = ast.resolve_metadata(metadata).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(
            "Row can't be added because `\"sys_op\"` already has an alias".into()
        ),
        plan_err
    );
}

#[test]
fn front_sql8() {
    let input = r#"SELECT t."identification_number", "product_code" FROM "hash_testing" as t
        WHERE t."identification_number" = 1"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT t."identification_number", t."product_code""#,
            r#"FROM "hash_testing" as t WHERE (t."identification_number") = (?)"#,
        ),
        vec![Value::from(1_u64)],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
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
    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}",
            r#"SELECT "t3"."id", "t3"."FIRST_NAME","#,
            r#""t8"."identification_number","#,
            r#""t8"."product_code" FROM"#,
            r#"(SELECT "test_space"."id", "test_space"."FIRST_NAME""#,
            r#"FROM "test_space" WHERE ("test_space"."sys_op") < (?) and ("test_space"."sysFrom") >= (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "test_space_hist"."id", "test_space_hist"."FIRST_NAME""#,
            r#"FROM "test_space_hist" WHERE ("test_space_hist"."sysFrom") <= (?))"#,
            r#"as "t3""#,
            r#"INNER JOIN"#,
            r#"(SELECT "hash_testing_hist"."identification_number","#,
            r#""hash_testing_hist"."product_code" FROM "hash_testing_hist" WHERE ("hash_testing_hist"."sys_op") > (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_single_testing_hist"."identification_number","#,
            r#""hash_single_testing_hist"."product_code" FROM "hash_single_testing_hist""#,
            r#"WHERE ("hash_single_testing_hist"."sys_op") <= (?))"#,
            r#"as "t8" ON ("t3"."id") = ("t8"."identification_number")"#,
            r#"WHERE ("t3"."id") = (?) and ("t8"."identification_number") = (?) and ("t8"."product_code") = (?)"#,
        ),
        vec![
            Value::from(0_u64),
            Value::from(0_u64),
            Value::from(0_u64),
            Value::from(0_u64),
            Value::from(0_u64),
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from("123"),
        ],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_sql10() {
    let input = r#"INSERT INTO "t" VALUES(1, 2, 3, 4)"#;
    let expected = PatternWithParams::new(
        format!(
            "{}",
            r#"INSERT INTO "t" ("a", "b", "c", "d") VALUES (?, ?, ?, ?)"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(3_u64),
            Value::from(4_u64),
        ],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_sql11() {
    let input = r#"INSERT INTO "t" ("a", "c") VALUES(1, 2)"#;
    let expected = PatternWithParams::new(
        format!("{}", r#"INSERT INTO "t" ("a", "c") VALUES (?, ?)"#,),
        vec![Value::from(1_u64), Value::from(2_u64)],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_sql12() {
    let query = r#"INSERT INTO "t" VALUES(1, 2)"#;
    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan_err = ast.resolve_metadata(metadata).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(
            r#"Invalid number of values: 2. Table "t" expects 4 column(s)."#.into()
        ),
        plan_err
    );
}

#[test]
fn front_sql13() {
    let query = r#"INSERT INTO "t" ("a") VALUES(1, 2)"#;
    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan_err = ast.resolve_metadata(metadata).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(
            r#"Invalid number of values: 2. Table "t" expects 1 column(s)."#.into()
        ),
        plan_err
    );
}

#[test]
fn front_sql14() {
    let input = r#"INSERT INTO "t" ("a", "c") SELECT "b", "d" FROM "t""#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"INSERT INTO "t" ("a", "c")"#, r#"SELECT "t"."b", "t"."d" FROM "t""#,
        ),
        vec![],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_sql15() {
    let query = r#"INSERT INTO "t" SELECT "b", "d" FROM "t""#;
    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan_err = ast.resolve_metadata(metadata).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(
            r#"Invalid number of values: 2. Table "t" expects 4 column(s)."#.into()
        ),
        plan_err
    );
}

// check cyrillic strings support
#[test]
fn front_sql16() {
    let input = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
        WHERE "product_code" = 'кириллица'"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT "hash_testing"."identification_number","#,
            r#""hash_testing"."product_code""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."product_code") = (?)"#,
        ),
        vec![Value::from("кириллица")],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_sql17() {
    let input = r#"SELECT "identification_number" FROM "hash_testing"
        WHERE "product_code" IS NULL"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "hash_testing"."identification_number""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."product_code") is null"#,
        ),
        vec![],
    );

    assert_eq!(sql_to_sql(input, &[], &no_transform), expected);
}

#[test]
fn front_params1() {
    let pattern = r#"SELECT "id", "FIRST_NAME" FROM "test_space"
        WHERE "sys_op" = ? AND "sysFrom" > ?"#;
    let params = vec![Value::from(0_i64), Value::from(1_i64)];
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "test_space"."id", "test_space"."FIRST_NAME" FROM "test_space""#,
            r#"WHERE ("test_space"."sys_op") = (?) and ("test_space"."sysFrom") > (?)"#,
        ),
        params.clone(),
    );

    assert_eq!(sql_to_sql(pattern, &params, &no_transform), expected);
}

#[test]
fn front_params2() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? AND "FIRST_NAME" = ?"#;
    let params = vec![Value::Null, Value::from("hello")];
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "test_space"."id" FROM "test_space""#,
            r#"WHERE ("test_space"."sys_op") = (?) and ("test_space"."FIRST_NAME") = (?)"#,
        ),
        params.clone(),
    );

    assert_eq!(sql_to_sql(pattern, &params, &no_transform), expected);
}

// check cyrillic params support
#[test]
fn front_params3() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? AND "FIRST_NAME" = ?"#;
    let params = vec![Value::Null, Value::from("кириллица")];
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "test_space"."id" FROM "test_space""#,
            r#"WHERE ("test_space"."sys_op") = (?) and ("test_space"."FIRST_NAME") = (?)"#,
        ),
        params.clone(),
    );

    assert_eq!(sql_to_sql(pattern, &params, &no_transform), expected);
}

// check symbols in values (grammar)
#[test]
fn front_params4() {
    let pattern = r#"SELECT "id" FROM "test_space"
        WHERE "FIRST_NAME" = '''± !@#$%^&*()_+=-\/><";:,.`~'"#;

    let params = vec![Value::from(r#"''± !@#$%^&*()_+=-\/><";:,.`~"#)];
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "test_space"."id" FROM "test_space""#,
            r#"WHERE ("test_space"."FIRST_NAME") = (?)"#,
        ),
        params,
    );

    assert_eq!(sql_to_sql(pattern, &vec![], &no_transform), expected);
}
