use pretty_assertions::assert_eq;

use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;
use crate::frontend::sql::ast::AbstractSyntaxTree;

#[test]
fn one_table_projection() {
    let query = r#"SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "identification_number" = 1"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "hash_testing"."identification_number" as "identification_number", "hash_testing"."product_code" as "product_code""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (1)"#
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
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT "hash_testing"."identification_number" as "identification_number", "hash_testing"."product_code" as "product_code","#,
            r#""hash_testing"."product_units" as "product_units", "hash_testing"."sys_op" as "sys_op", "hash_testing"."bucket_id" as "bucket_id""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (1)"#
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
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing" WHERE ("hash_testing"."identification_number") = (1)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."product_code" as "product_code" FROM "hash_testing_hist" WHERE ("hash_testing_hist"."product_code") = ('a')"#
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
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT t1."product_code" as "product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") = (1)) as t1"#,
            r#"WHERE (t1."product_code") = ('a')"#
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
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "t1"."product_code" as "product_code" FROM"#,
            r#"(SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing" WHERE ("hash_testing"."identification_number") = (1)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."product_code" as "product_code" FROM "hash_testing_hist" WHERE ("hash_testing_hist"."product_code") = ('a')) as "t1""#,
            r#"WHERE ("t1"."product_code") = ('a')"#,
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
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
            r#"INNER JOIN "history""#,
            r#"ON ("hash_testing"."identification_number") = ("history"."id")"#,
            r#"WHERE ("hash_testing"."product_code") = ('a')"#,
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
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
            r#"INNER JOIN"#,
            r#"(SELECT "history"."id" as "id" FROM "history" WHERE ("history"."id") = (1)) as "t""#,
            r#"ON ("hash_testing"."identification_number") = ("t"."id")"#,
            r#"WHERE ("hash_testing"."product_code") = ('a')"#,
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
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);
    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "hash_testing"."product_code" as "product_code" FROM "hash_testing""#,
            r#"WHERE ("hash_testing"."identification_number") in"#,
            r#"(SELECT "hash_testing_hist"."identification_number" as "identification_number" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = ('b'))"#,
            r#"and ("hash_testing"."product_code") < ('a')"#,
        ),
        sql
    );
}
