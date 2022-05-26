use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn derive_equalities(plan: &mut Plan) {
    plan.derive_equalities().unwrap();
}

#[test]
fn equality_propagation1() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = 2 AND "c" = 1 OR "d" = 1"#;

    let expected = format!(
        "{} {} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE (("t"."a") = (1) and ("t"."b") = (2) and ("t"."c") = (1)"#,
        r#"and ("t"."c") = ("t"."a") or ("t"."d") = (1))"#,
    );

    assert_eq!(sql_to_sql(input, &[], &derive_equalities), expected);
}

#[test]
fn equality_propagation2() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = NULL AND "b" = NULL"#;

    let expected = format!(
        "{}",
        r#"SELECT "t"."a" as "a" FROM "t" WHERE ("t"."a") = (NULL) and ("t"."b") = (NULL)"#,
    );

    assert_eq!(sql_to_sql(input, &[], &derive_equalities), expected);
}

#[test]
fn equality_propagation3() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = null AND "a" = null"#;

    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE ("t"."a") = (1) and ("t"."b") = (NULL) and ("t"."a") = (NULL)"#,
    );

    assert_eq!(sql_to_sql(input, &[], &derive_equalities), expected);
}

#[test]
fn equality_propagation4() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = null AND "a" = null AND "b" = 1"#;

    let expected = format!(
        "{} {} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE ("t"."a") = (1) and ("t"."b") = (NULL) and ("t"."a") = (NULL)"#,
        r#"and ("t"."b") = (1) and ("t"."b") = ("t"."a")"#,
    );

    assert_eq!(sql_to_sql(input, &[], &derive_equalities), expected);
}

#[test]
fn equality_propagation5() {
    let input = r#"SELECT "a" FROM "t"
    WHERE "a" = 1 AND "b" = 1 AND "c" = 1 AND "d" = 1"#;

    let expected = format!(
        "{} {} {} {} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE ("t"."a") = (1) and ("t"."b") = (1)"#,
        r#"and ("t"."c") = (1) and ("t"."d") = (1)"#,
        r#"and ("t"."c") = ("t"."b") and ("t"."b") = ("t"."d")"#,
        r#"and ("t"."d") = ("t"."a")"#,
    );

    assert_eq!(sql_to_sql(input, &[], &derive_equalities), expected);
}
