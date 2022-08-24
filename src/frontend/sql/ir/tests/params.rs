use super::*;
use crate::executor::engine::cartridge::backend::sql::ir::PatternWithParams;
use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::value::Value;
use pretty_assertions::assert_eq;

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

    assert_eq!(sql_to_sql(pattern, params, &no_transform), expected);
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

    assert_eq!(sql_to_sql(pattern, params, &no_transform), expected);
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

    assert_eq!(sql_to_sql(pattern, params, &no_transform), expected);
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

    assert_eq!(sql_to_sql(pattern, vec![], &no_transform), expected);
}

// check parameter binding order, when selection has sub-queries
#[test]
fn front_params5() {
    let pattern = r#"
        SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? OR "id" IN (
            SELECT "sysFrom" FROM "test_space_hist"
            WHERE "sys_op" = ?
        )
    "#;
    let params = vec![Value::from(0_i64), Value::from(1_i64)];
    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT "test_space"."id" FROM "test_space""#,
            r#"WHERE (("test_space"."sys_op") = (?) or ("test_space"."id") in"#,
            r#"(SELECT "test_space_hist"."sysFrom" FROM "test_space_hist" WHERE ("test_space_hist"."sys_op") = (?)))"#,
        ),
        params.clone(),
    );

    assert_eq!(sql_to_sql(pattern, params, &no_transform), expected);
}

#[test]
fn front_params6() {
    let pattern = r#"
        SELECT "id" FROM "test_space"
        WHERE "sys_op" = ? OR "id" NOT IN (
            SELECT "id" FROM "test_space"
            WHERE "sys_op" = ?
            UNION ALL
            SELECT "id" FROM "test_space"
            WHERE "sys_op" = ?
        )
    "#;
    let params = vec![Value::from(0_i64), Value::from(1_i64), Value::from(2_i64)];
    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "test_space"."id" FROM "test_space""#,
            r#"WHERE (("test_space"."sys_op") = (?) or ("test_space"."id") not in"#,
            r#"(SELECT "test_space"."id" FROM "test_space" WHERE ("test_space"."sys_op") = (?)"#,
            r#"UNION ALL"#,
            r#"SELECT "test_space"."id" FROM "test_space" WHERE ("test_space"."sys_op") = (?)))"#,
        ),
        params.clone(),
    );

    assert_eq!(sql_to_sql(pattern, params, &no_transform), expected);
}
