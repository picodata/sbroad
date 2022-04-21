use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn replace_in_operator(plan: &mut Plan) {
    plan.replace_in_operator().unwrap();
}

#[test]
fn bool_in1() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2, 3)"#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE ((("t"."a") = (1) or ("t"."a") = (2)) or ("t"."a") = (3))"#,
    );

    assert_eq!(sql_to_sql(input, &[], &replace_in_operator), expected);
}

#[test]
fn bool_in2() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a", "b") IN ((1, 10), (2, 20), (3, 30))"#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE ((("t"."a", "t"."b") = (1, 10) or ("t"."a", "t"."b") = (2, 20)) or ("t"."a", "t"."b") = (3, 30))"#,
    );

    assert_eq!(sql_to_sql(input, &[], &replace_in_operator), expected);
}

#[test]
fn bool_in3() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2) AND "b" IN (3)"#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE (("t"."a") = (1) or ("t"."a") = (2)) and ("t"."b") = (3)"#,
    );

    assert_eq!(sql_to_sql(input, &[], &replace_in_operator), expected);
}
