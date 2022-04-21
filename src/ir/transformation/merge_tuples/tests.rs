use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn merge_tuples(plan: &mut Plan) {
    plan.merge_tuples().unwrap();
}

#[test]
fn merge_tuples1() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" = 1 and "b" = 2 and "c" < 3 and 4 < "a""#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE ("t"."a", "t"."b") = (1, 2) and (3, "t"."a") > ("t"."c", 4)"#,
    );

    assert_eq!(sql_to_sql(input, &[], &merge_tuples), expected);
}

#[test]
fn merge_tuples2() {
    let input = r#"SELECT "a" FROM "t"
        WHERE "a" = 1 and null and "b" = 2
        or true and "c" >= 3 and 4 <= "a""#;
    let expected = format!(
        "{} {} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#,
        r#"WHERE (("t"."a", "t"."b") = (1, 2) and (NULL)"#,
        r#"or ("t"."c", "t"."a") >= (3, 4) and (true))"#,
    );

    assert_eq!(sql_to_sql(input, &[], &merge_tuples), expected);
}

#[test]
fn merge_tuples3() {
    let input = r#"SELECT "a" FROM "t" WHERE true"#;
    let expected = format!("{}", r#"SELECT "t"."a" as "a" FROM "t" WHERE true"#);

    assert_eq!(sql_to_sql(input, &[], &merge_tuples), expected);
}

#[test]
fn merge_tuples4() {
    let input = r#"SELECT "a" FROM "t" WHERE ("a", "b") = (1, 2) and 3 = "c""#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#, r#"WHERE ("t"."a", "t"."b", "t"."c") = (1, 2, 3)"#,
    );

    assert_eq!(sql_to_sql(input, &[], &merge_tuples), expected);
}

#[test]
fn merge_tuples5() {
    let input = r#"SELECT "a" FROM "t" WHERE 3 < "c" and ("a", "b") > (1, 2)"#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t""#, r#"WHERE ("t"."c", "t"."a", "t"."b") > (3, 1, 2)"#,
    );

    assert_eq!(sql_to_sql(input, &[], &merge_tuples), expected);
}
