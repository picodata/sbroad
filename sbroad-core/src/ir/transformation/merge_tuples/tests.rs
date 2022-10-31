use crate::backend::sql::ir::PatternWithParams;
use crate::ir::transformation::helpers::check_transformation;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn merge_tuples(plan: &mut Plan) {
    plan.merge_tuples().unwrap();
}

#[test]
fn merge_tuples1() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" = 1 and "b" = 2 and "c" < 3 and 4 < "a""#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."a", "t"."b") = (?, ?) and (?) < ("t"."a") and ("t"."c") < (?)"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(2_u64),
            Value::from(4_u64),
            Value::from(3_u64),
        ],
    );

    assert_eq!(check_transformation(input, vec![], &merge_tuples), expected);
}

#[test]
fn merge_tuples2() {
    let input = r#"SELECT "a" FROM "t"
        WHERE "a" = 1 and null and "b" = 2
        or true and "c" >= 3 and 4 <= "a""#;
    let expected = PatternWithParams::new(
        format!(
            "{} {} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE (("t"."a", "t"."b") = (?, ?) and (?)"#,
            r#"or (?) <= ("t"."a") and ("t"."c") >= (?) and (?))"#,
        ),
        vec![
            Value::from(1_u64),
            Value::from(2_u64),
            Value::Null,
            Value::from(4_u64),
            Value::from(3_u64),
            Value::Boolean(true),
        ],
    );

    assert_eq!(check_transformation(input, vec![], &merge_tuples), expected);
}

#[test]
fn merge_tuples3() {
    let input = r#"SELECT "a" FROM "t" WHERE true"#;
    let expected = PatternWithParams::new(
        r#"SELECT "t"."a" FROM "t" WHERE ?"#.to_string(),
        vec![Value::Boolean(true)],
    );

    assert_eq!(check_transformation(input, vec![], &merge_tuples), expected);
}

#[test]
fn merge_tuples4() {
    let input = r#"SELECT "a" FROM "t" WHERE ("a", "b") = (1, 2) and 3 = "c""#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#, r#"WHERE ("t"."a", "t"."b", "t"."c") = (?, ?, ?)"#,
        ),
        vec![Value::from(1_u64), Value::from(2_u64), Value::from(3_u64)],
    );

    assert_eq!(check_transformation(input, vec![], &merge_tuples), expected);
}

#[test]
fn merge_tuples5() {
    let input = r#"SELECT "a" FROM "t" WHERE 3 < "c" and ("a", "b") > (1, 2)"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#,
            r#"WHERE ("t"."a", "t"."b") > (?, ?) and (?) < ("t"."c")"#,
        ),
        vec![Value::from(1_u64), Value::from(2_u64), Value::from(3_u64)],
    );

    assert_eq!(check_transformation(input, vec![], &merge_tuples), expected);
}

#[test]
fn merge_tuples6() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" <> 1 and "b" <> 2"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t""#, r#"WHERE ("t"."b") <> (?) and ("t"."a") <> (?)"#,
        ),
        vec![Value::from(2_u64), Value::from(1_u64)],
    );

    assert_eq!(check_transformation(input, vec![], &merge_tuples), expected);
}
