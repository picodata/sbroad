use pretty_assertions::assert_eq;

use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;

#[test]
fn dnf1() {
    let query = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 AND "b" = 2 OR "a" = 3) AND "c" = 4"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.set_dnf().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE (("t"."a") = (1) and ("t"."b") = (2) and ("t"."c") = (4)"#,
            r#"or ("t"."a") = (3) and ("t"."c") = (4))"#,
        ),
        sql
    );
}

#[test]
fn dnf2() {
    let query = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND ("a" = 3 OR "c" = 4)"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.set_dnf().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE (((("t"."a") = (3) and ("t"."a") = (1) or ("t"."c") = (4) and ("t"."a") = (1))"#,
            r#"or ("t"."a") = (3) and ("t"."b") = (2)) or ("t"."c") = (4) and ("t"."b") = (2))"#,
        ),
        sql
    );
}

#[test]
fn dnf3() {
    let query = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND NULL"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.set_dnf().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE (("t"."a") = (1) and (NULL) or ("t"."b") = (2) and (NULL))"#,
        ),
        sql
    );
}

#[test]
fn dnf4() {
    let query = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND true"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.set_dnf().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE (("t"."a") = (1) and (true) or ("t"."b") = (2) and (true))"#,
        ),
        sql
    );
}

#[test]
fn dnf5() {
    let query = r#"SELECT "a" FROM "t"
    WHERE ("a" = 1 OR "b" = 2) AND ((false))"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.set_dnf().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE (("t"."a") = (1) and ((false)) or ("t"."b") = (2) and ((false)))"#,
        ),
        sql
    );
}
