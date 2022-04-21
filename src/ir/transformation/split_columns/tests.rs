use crate::executor::engine::mock::MetadataMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn split_columns(plan: &mut Plan) {
    plan.split_columns().unwrap();
}

#[test]
fn split_columns1() {
    let input = r#"SELECT "a" FROM "t" WHERE ("a", 2) = (1, "b")"#;
    let expected = format!(
        "{}",
        r#"SELECT "t"."a" as "a" FROM "t" WHERE ("t"."a") = (1) and (2) = ("t"."b")"#,
    );

    assert_eq!(sql_to_sql(input, &[], &split_columns), expected);
}

#[test]
fn split_columns2() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" = 1"#;
    let expected = format!(
        "{}",
        r#"SELECT "t"."a" as "a" FROM "t" WHERE ("t"."a") = (1)"#,
    );

    assert_eq!(sql_to_sql(input, &[], &split_columns), expected);
}

#[test]
fn split_columns3() {
    let query = r#"SELECT "a" FROM "t" WHERE ("a", 2, "b") = (1, "b")"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata, &[]).unwrap();
    let plan_err = plan.split_columns().unwrap_err();
    assert_eq!(
        format!(
            "{} {} {}",
            r#"Left and right rows have different number of columns:"#,
            r#"Row { list: [10, 11, 12], distribution: None },"#,
            r#"Row { list: [14, 15], distribution: None }"#,
        ),
        format!("{}", plan_err)
    );
}

#[test]
fn split_columns4() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" in (1, 2)"#;
    let expected = format!(
        "{}",
        r#"SELECT "t"."a" as "a" FROM "t" WHERE ("t"."a") in (1, 2)"#,
    );

    assert_eq!(sql_to_sql(input, &[], &split_columns), expected);
}

#[test]
fn split_columns5() {
    let input = r#"SELECT "a" FROM "t" WHERE ("a", 2) < (1, "b") and "a" > 2"#;
    let expected = format!(
        "{} {}",
        r#"SELECT "t"."a" as "a" FROM "t" WHERE ("t"."a") < (1) and (2) < ("t"."b")"#,
        r#"and ("t"."a") > (2)"#,
    );

    assert_eq!(sql_to_sql(input, &[], &split_columns), expected);
}
