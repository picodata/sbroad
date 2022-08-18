use crate::executor::engine::cartridge::backend::sql::ir::PatternWithParams;
use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn replace_in_operator(plan: &mut Plan) {
    plan.replace_in_operator().unwrap();
}

#[test]
fn bool_in1() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2, 3)"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ((("t"."a") = (?) or ("t"."a") = (?)) or ("t"."a") = (?))"#,
        ),
        vec![Value::from(1_u64), Value::from(2_u64), Value::from(3_u64)],
    );

    assert_eq!(
        sql_to_sql(input, &mut vec![], &replace_in_operator),
        expected
    );
}

#[test]
fn bool_in2() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a", "b") IN ((1, 10), (2, 20), (3, 30))"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ((("t"."a", "t"."b") = (?, ?) or ("t"."a", "t"."b") = (?, ?)) or ("t"."a", "t"."b") = (?, ?))"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(10_u64),
            Value::from(2_u64),
            Value::from(20_u64),
            Value::from(3_u64),
            Value::from(30_u64),
        ],
    );

    assert_eq!(
        sql_to_sql(input, &mut vec![], &replace_in_operator),
        expected
    );
}

#[test]
fn bool_in3() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2) AND "b" IN (3)"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE (("t"."a") = (?) or ("t"."a") = (?)) and ("t"."b") = (?)"#,
        ),
        vec![Value::from(1_u64), Value::from(2_u64), Value::from(3_u64)],
    );

    assert_eq!(
        sql_to_sql(input, &mut vec![], &replace_in_operator),
        expected
    );
}
