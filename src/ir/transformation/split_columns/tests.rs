use pretty_assertions::assert_eq;

use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;

#[test]
fn split_columns1() {
    let query = r#"SELECT "a" FROM "t" WHERE ("a", 2) = (1, "b")"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.split_columns().unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{}",
            r#"SELECT "t"."a" as "a" FROM "t" WHERE ("t"."a") = (1) and (2) = ("t"."b")"#,
        ),
        sql
    );
}

#[test]
fn split_columns2() {
    let query = r#"SELECT "a" FROM "t" WHERE "a" = 1"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.split_columns().unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{}",
            r#"SELECT "t"."a" as "a" FROM "t" WHERE ("t"."a") = (1)"#,
        ),
        sql
    );
}

#[test]
fn split_columns3() {
    let query = r#"SELECT "a" FROM "t" WHERE ("a", 2, "b") = (1, "b")"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    let plan_err = plan.split_columns().unwrap_err();
    assert_eq!(
        format!(
            "{} {} {}",
            r#"Left and right rows have different number of columns:"#,
            r#"Row { list: [8, 9, 10], distribution: None },"#,
            r#"Row { list: [12, 13], distribution: None }"#,
        ),
        format!("{}", plan_err)
    );
}

#[test]
fn split_columns4() {
    let query = r#"SELECT "a" FROM "t" WHERE "a" in (1, 2)"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.split_columns().unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{}",
            r#"SELECT "t"."a" as "a" FROM "t" WHERE ("t"."a") in (1, 2)"#,
        ),
        sql
    );
}

#[test]
fn split_columns5() {
    let query = r#"SELECT "a" FROM "t" WHERE ("a", 2) < (1, "b") and "a" > 2"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.split_columns().unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t" WHERE ("t"."a") < (1) and (2) < ("t"."b")"#,
            r#"and ("t"."a") > (2)"#,
        ),
        sql
    );
}
