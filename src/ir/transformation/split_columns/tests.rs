use crate::executor::engine::cartridge::backend::sql::ir::PatternWithParams;
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::transformation::helpers::sql_to_sql;
use crate::ir::value::Value;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

fn split_columns(plan: &mut Plan) {
    plan.split_columns().unwrap();
}

#[test]
fn split_columns1() {
    let input = r#"SELECT "a" FROM "t" WHERE ("a", 2) = (1, "b")"#;
    let expected = PatternWithParams::new(
        format!(
            "{}",
            r#"SELECT "t"."a" FROM "t" WHERE ("t"."a") = (?) and (?) = ("t"."b")"#,
        ),
        vec![Value::from(1_u64), Value::from(2_u64)],
    );

    assert_eq!(sql_to_sql(input, &mut vec![], &split_columns), expected);
}

#[test]
fn split_columns2() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" = 1"#;
    let expected = PatternWithParams::new(
        format!("{}", r#"SELECT "t"."a" FROM "t" WHERE ("t"."a") = (?)"#,),
        vec![Value::from(1_u64)],
    );

    assert_eq!(sql_to_sql(input, &mut vec![], &split_columns), expected);
}

#[test]
fn split_columns3() {
    let query = r#"SELECT "a" FROM "t" WHERE ("a", 2, "b") = (1, "b")"#;

    let metadata = &RouterConfigurationMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(&mut vec![]).unwrap();
    let plan_err = plan.split_columns().unwrap_err();
    assert_eq!(
        format!(
            "{} {} {}",
            r#"Left and right rows have different number of columns:"#,
            r#"Row { list: [12, 13, 14], distribution: None },"#,
            r#"Row { list: [16, 17], distribution: None }"#,
        ),
        format!("{}", plan_err)
    );
}

#[test]
fn split_columns4() {
    let input = r#"SELECT "a" FROM "t" WHERE "a" in (1, 2)"#;
    let expected = PatternWithParams::new(
        format!("{}", r#"SELECT "t"."a" FROM "t" WHERE ("t"."a") in (?, ?)"#,),
        vec![Value::from(1_u64), Value::from(2_u64)],
    );

    assert_eq!(sql_to_sql(input, &mut vec![], &split_columns), expected);
}

#[test]
fn split_columns5() {
    let input = r#"SELECT "a" FROM "t" WHERE ("a", 2) < (1, "b") and "a" > 2"#;
    let expected = PatternWithParams::new(
        format!(
            "{} {}",
            r#"SELECT "t"."a" FROM "t" WHERE ("t"."a") < (?) and (?) < ("t"."b")"#,
            r#"and ("t"."a") > (?)"#,
        ),
        vec![Value::from(1_u64), Value::from(2_u64), Value::from(2_u64)],
    );

    assert_eq!(sql_to_sql(input, &mut vec![], &split_columns), expected);
}
