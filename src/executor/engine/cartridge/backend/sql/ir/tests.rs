use pretty_assertions::assert_eq;

use crate::executor::bucket::Buckets;
use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::value::Value;

use super::*;

#[test]
fn one_table_projection() {
    let query = r#"SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "identification_number" = 1"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(&[]).unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let nodes = ex_plan.get_sql_order(top_id).unwrap();
    let sql = ex_plan.syntax_nodes_as_sql(&nodes, &Buckets::All).unwrap();

    assert_eq!(
        PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"SELECT "hash_testing"."identification_number" as "identification_number","#,
                r#""hash_testing"."product_code" as "product_code""#,
                r#"FROM "hash_testing""#,
                r#"WHERE ("hash_testing"."identification_number") = (?)"#,
            ),
            vec![Value::from(1_u64)]
        ),
        sql
    );
}

#[test]
fn one_table_with_asterisk() {
    let query = r#"SELECT *
    FROM "hash_testing"
    WHERE "identification_number" = 1"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(&[]).unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let nodes = ex_plan.get_sql_order(top_id).unwrap();
    let sql = ex_plan.syntax_nodes_as_sql(&nodes, &Buckets::All).unwrap();

    assert_eq!(
        PatternWithParams::new(
            format!(
                "{} {} {} {} {}",
                r#"SELECT "hash_testing"."identification_number" as "identification_number","#,
                r#""hash_testing"."product_code" as "product_code","#,
                r#""hash_testing"."product_units" as "product_units", "hash_testing"."sys_op" as "sys_op""#,
                r#"FROM "hash_testing""#,
                r#"WHERE ("hash_testing"."identification_number") = (?)"#
            ),
            vec![Value::from(1_u64)]
        ),
        sql
    );
}

#[test]
fn union_all() {
    let query = r#"SELECT "product_code"
    FROM "hash_testing"
    WHERE "identification_number" = 1
    UNION ALL
    SELECT "product_code"
    FROM "hash_testing_hist"
    WHERE "product_code" = 'a' 
    "#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(&[]).unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let nodes = ex_plan.get_sql_order(top_id).unwrap();
    let sql = ex_plan.syntax_nodes_as_sql(&nodes, &Buckets::All).unwrap();

    assert_eq!(
        PatternWithParams::new(
            format!(
                "{} {} {} {} {}",
                r#"SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
                r#"WHERE ("hash_testing"."identification_number") = (?)"#,
                r#"UNION ALL"#,
                r#"SELECT "hash_testing_hist"."product_code" as "product_code" FROM "hash_testing_hist""#,
                r#"WHERE ("hash_testing_hist"."product_code") = (?)"#
            ),
            vec![Value::from(1_u64), Value::from("a")],
        ),
        sql
    );
}

#[test]
fn from_sub_query() {
    let query = r#"SELECT "product_code"
    FROM (SELECT "product_code"
    FROM "hash_testing"
    WHERE "identification_number" = 1) as t1
    WHERE "product_code" = 'a'"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(&[]).unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let nodes = ex_plan.get_sql_order(top_id).unwrap();
    let sql = ex_plan.syntax_nodes_as_sql(&nodes, &Buckets::All).unwrap();

    assert_eq!(
        PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"SELECT t1."product_code" as "product_code" FROM"#,
                r#"(SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
                r#"WHERE ("hash_testing"."identification_number") = (?)) as t1"#,
                r#"WHERE (t1."product_code") = (?)"#
            ),
            vec![Value::from(1_u64), Value::from("a")],
        ),
        sql
    );
}

#[test]
fn from_sub_query_with_union() {
    let query = r#"SELECT "product_code"
  FROM (SELECT "product_code"
    FROM "hash_testing"
    WHERE "identification_number" = 1
    UNION ALL
    SELECT "product_code"
    FROM "hash_testing_hist"
    WHERE "product_code" = 'a') as "t1"
  WHERE "product_code" = 'a'"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(&[]).unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let nodes = ex_plan.get_sql_order(top_id).unwrap();
    let sql = ex_plan.syntax_nodes_as_sql(&nodes, &Buckets::All).unwrap();

    assert_eq!(
        PatternWithParams::new(
            format!(
                "{} {} {} {} {} {} {}",
                r#"SELECT "t1"."product_code" as "product_code" FROM"#,
                r#"(SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
                r#"WHERE ("hash_testing"."identification_number") = (?)"#,
                r#"UNION ALL"#,
                r#"SELECT "hash_testing_hist"."product_code" as "product_code" FROM "hash_testing_hist""#,
                r#"WHERE ("hash_testing_hist"."product_code") = (?)) as "t1""#,
                r#"WHERE ("t1"."product_code") = (?)"#,
            ),
            vec![Value::from(1_u64), Value::from("a"), Value::from("a")],
        ),
        sql
    );
}

#[test]
fn inner_join() {
    let query = r#"SELECT "product_code" FROM "hash_testing" join "history"
    on "hash_testing"."identification_number" = "history"."id"
    WHERE "product_code" = 'a'"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(&[]).unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let nodes = ex_plan.get_sql_order(top_id).unwrap();
    let sql = ex_plan.syntax_nodes_as_sql(&nodes, &Buckets::All).unwrap();

    assert_eq!(
        PatternWithParams::new(
            format!(
                "{} {} {} {}",
                r#"SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
                r#"INNER JOIN (SELECT "history"."id" as "id" FROM "history") as "history""#,
                r#"ON ("hash_testing"."identification_number") = ("history"."id")"#,
                r#"WHERE ("hash_testing"."product_code") = (?)"#,
            ),
            vec![Value::from("a")],
        ),
        sql
    );
}

#[test]
fn inner_join_with_sq() {
    let query = r#"SELECT "product_code" FROM "hash_testing" join
    (SELECT * FROM "history" WHERE "id" = 1) as "t"
    on "hash_testing"."identification_number" = "t"."id"
    WHERE "product_code" = 'a'"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(&[]).unwrap();
    let ex_plan = ExecutionPlan::from(plan);

    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let nodes = ex_plan.get_sql_order(top_id).unwrap();
    let sql = ex_plan.syntax_nodes_as_sql(&nodes, &Buckets::All).unwrap();

    assert_eq!(
        PatternWithParams::new(
            format!(
                "{} {} {} {} {}",
                r#"SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
                r#"INNER JOIN"#,
                r#"(SELECT "history"."id" as "id" FROM "history" WHERE ("history"."id") = (?)) as "t""#,
                r#"ON ("hash_testing"."identification_number") = ("t"."id")"#,
                r#"WHERE ("hash_testing"."product_code") = (?)"#,
            ),
            vec![Value::from(1_u64), Value::from("a")],
        ),
        sql
    );
}

#[test]
fn selection_with_sq() {
    let query = r#"SELECT "product_code" FROM "hash_testing"
    WHERE "identification_number" in
    (SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'b') and "product_code" < 'a'"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let mut plan = ast.resolve_metadata(metadata).unwrap();
    plan.bind_params(&[]).unwrap();
    let ex_plan = ExecutionPlan::from(plan);
    let top_id = ex_plan.get_ir_plan().get_top().unwrap();
    let nodes = ex_plan.get_sql_order(top_id).unwrap();
    let sql = ex_plan.syntax_nodes_as_sql(&nodes, &Buckets::All).unwrap();

    assert_eq!(
        PatternWithParams::new(
            format!(
                "{} {} {} {} {}",
                r#"SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
                r#"WHERE ("hash_testing"."identification_number") in"#,
                r#"(SELECT "hash_testing_hist"."identification_number" as "identification_number" FROM "hash_testing_hist""#,
                r#"WHERE ("hash_testing_hist"."product_code") = (?))"#,
                r#"and ("hash_testing"."product_code") < (?)"#,
            ),
            vec![Value::from("b"), Value::from("a")],
        ),
        sql
    );
}
