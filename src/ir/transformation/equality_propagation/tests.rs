use crate::executor::engine::cartridge::backend::sql::ir::PatternWithParams;
use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn derive_equalities(plan: &mut Plan) {
    plan.derive_equalities().unwrap();
}

#[test]
fn equality_propagation1() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = 2 AND "c" = 1 OR "d" = 1"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE (("t"."a") = (?) and ("t"."b") = (?) and ("t"."c") = (?)"#,
            r#"and ("t"."c") = ("t"."a") or ("t"."d") = (?))"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(1_u64),
            Value::from(1_u64),
        ],
    );

    assert_eq!(sql_to_sql(input, vec![], &derive_equalities), expected);
}

#[test]
fn equality_propagation2() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = NULL AND "b" = NULL"#;

    let expected = PatternWithParams::new(
        format!(
            "{}",
            r#"SELECT "t"."a" FROM "t" WHERE ("t"."a") = (?) and ("t"."b") = (?)"#,
        ),
        vec![Value::Null, Value::Null],
    );

    assert_eq!(sql_to_sql(input, vec![], &derive_equalities), expected);
}

#[test]
fn equality_propagation3() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = null AND "a" = null"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."a") = (?) and ("t"."b") = (?) and ("t"."a") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::Null, Value::Null],
    );

    assert_eq!(sql_to_sql(input, vec![], &derive_equalities), expected);
}

#[test]
fn equality_propagation4() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = null AND "a" = null AND "b" = 1"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."a") = (?) and ("t"."b") = (?) and ("t"."a") = (?)"#,
            r#"and ("t"."b") = (?) and ("t"."b") = ("t"."a")"#,
        ),
        vec![
            Value::from(1_u64),
            Value::Null,
            Value::Null,
            Value::from(1_u64),
        ],
    );

    assert_eq!(sql_to_sql(input, vec![], &derive_equalities), expected);
}

#[test]
fn equality_propagation5() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = 1 AND "c" = 1 AND "d" = 1"#;

    let expected = PatternWithParams::new(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."a") = (?) and ("t"."b") = (?)"#,
            r#"and ("t"."c") = (?) and ("t"."d") = (?)"#,
            r#"and ("t"."c") = ("t"."b") and ("t"."b") = ("t"."a")"#,
            r#"and ("t"."a") = ("t"."d")"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(1_u64),
            Value::from(1_u64),
        ],
    );

    assert_eq!(sql_to_sql(input, vec![], &derive_equalities), expected);
}
