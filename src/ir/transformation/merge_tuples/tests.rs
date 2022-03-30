use pretty_assertions::assert_eq;

use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;

#[test]
fn merge_tuples1() {
    let query = r#"SELECT "a" FROM "t" WHERE "a" = 1 and "b" = 2 and "c" < 3 and 4 < "a""#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.merge_tuples().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE ("t"."a", "t"."b") = (1, 2) and (3, "t"."a") > ("t"."c", 4)"#,
        ),
        sql
    );
}

#[test]
fn merge_tuples2() {
    let query = r#"SELECT "a" FROM "t"
        WHERE "a" = 1 and null and "b" = 2
        or true and "c" >= 3 and 4 <= "a""#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.merge_tuples().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#,
            r#"WHERE (("t"."a", "t"."b") = (1, 2) and (NULL)"#,
            r#"or ("t"."c", "t"."a") >= (3, 4) and (true))"#,
        ),
        sql
    );
}

#[test]
fn merge_tuples3() {
    let query = r#"SELECT "a" FROM "t" WHERE true"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.merge_tuples().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!("{}", r#"SELECT "t"."a" as "a" FROM "t" WHERE true"#,),
        sql
    );
}

#[test]
fn merge_tuples4() {
    let query = r#"SELECT "a" FROM "t" WHERE ("a", "b") = (1, 2) and 3 = "c""#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.merge_tuples().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#, r#"WHERE ("t"."a", "t"."b", "t"."c") = (1, 2, 3)"#,
        ),
        sql
    );
}

#[test]
fn merge_tuples5() {
    let query = r#"SELECT "a" FROM "t" WHERE 3 < "c" and ("a", "b") > (1, 2)"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.to_ir(metadata).unwrap();
    plan.merge_tuples().unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "t"."a" as "a" FROM "t""#, r#"WHERE ("t"."c", "t"."a", "t"."b") > (3, 1, 2)"#,
        ),
        sql
    );
}
