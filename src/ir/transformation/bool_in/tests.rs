use pretty_assertions::assert_eq;

use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;

#[test]
fn bool_in1() {
    let query = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2, 3)"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.replace_in_operator().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE ((("t"."a") = (1) or ("t"."a") = (2)) or ("t"."a") = (3))"#,
        ),
        sql
    );
}

#[test]
fn bool_in2() {
    let query = r#"SELECT "a" FROM "t"
    WHERE ("a", "b") IN ((1, 10), (2, 20), (3, 30))"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.replace_in_operator().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE ((("t"."a", "t"."b") = (1, 10) or ("t"."a", "t"."b") = (2, 20)) or ("t"."a", "t"."b") = (3, 30))"#,
        ),
        sql
    );
}

#[test]
fn bool_in3() {
    let query = r#"SELECT "a" FROM "t" WHERE "a" IN (1, 2) AND "b" IN (3)"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.replace_in_operator().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE (("t"."a") = (1) or ("t"."a") = (2)) and ("t"."b") = (3)"#,
        ),
        sql
    );
}
