use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn set_dnf(plan: &mut Plan) {
    plan.set_dnf().unwrap();
}

#[test]
fn dnf1() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 AND "b" = 2 OR "a" = 3) AND "c" = 4"#;
    let expected = format!(
        "{} {} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE (("t"."a") = (1) and ("t"."b") = (2) and ("t"."c") = (4)"#,
        r#"or ("t"."a") = (3) and ("t"."c") = (4))"#,
    );

    assert_eq!(sql_to_sql(input, &set_dnf), expected);
}

#[test]
fn dnf2() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND ("a" = 3 OR "c" = 4)"#;
    let expected = format!(
        "{} {} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE (((("t"."a") = (3) and ("t"."a") = (1) or ("t"."c") = (4) and ("t"."a") = (1))"#,
        r#"or ("t"."a") = (3) and ("t"."b") = (2)) or ("t"."c") = (4) and ("t"."b") = (2))"#,
    );

    assert_eq!(sql_to_sql(input, &set_dnf), expected);
}

#[test]
fn dnf3() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND NULL"#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE (("t"."a") = (1) and (NULL) or ("t"."b") = (2) and (NULL))"#,
    );

    assert_eq!(sql_to_sql(input, &set_dnf), expected);
}

#[test]
fn dnf4() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND true"#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE (("t"."a") = (1) and (true) or ("t"."b") = (2) and (true))"#,
    );

    assert_eq!(sql_to_sql(input, &set_dnf), expected);
}

#[test]
fn dnf5() {
    let input = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND ((false))"#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE (("t"."a") = (1) and ((false)) or ("t"."b") = (2) and ((false)))"#,
    );

    assert_eq!(sql_to_sql(input, &set_dnf), expected);
}

#[test]
fn dnf6() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 and "c" = 1 OR "b" = 2"#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE (("t"."a") = (1) and ("t"."c") = (1) or ("t"."b") = (2))"#,
    );

    assert_eq!(sql_to_sql(input, &set_dnf), expected);
}
