use pretty_assertions::assert_eq;

use crate::executor::engine::mock::MetadataMock;
use crate::executor::ir::ExecutionPlan;

use super::*;

#[test]
fn simple_query_to_ir() {
    let query = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
    WHERE "identification_number" = 1"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "hash_testing"."identification_number" as "identification_number", "hash_testing"."product_code" as "product_code""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."identification_number") = (1)"#,
        ),
        sql
    );
}

#[test]
fn complex_cond_query_transform() {
    let query = r#"SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "identification_number" = 1 AND "product_code" = '1'
    OR "identification_number" = 2 AND "product_code" = '2'"#;

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
            r#"FROM "hash_testing" WHERE (("hash_testing"."identification_number") = (1) and ("hash_testing"."product_code") = ('1')"#,
            r#"or ("hash_testing"."identification_number") = (2) and ("hash_testing"."product_code") = ('2'))"#,
        ),
        sql
    );
}

#[test]
fn simple_union_query_transform() {
    let query = r#"SELECT *
    FROM
        (SELECT "identification_number", "product_code"
        FROM "hash_testing"
        WHERE "sys_op" = 1
        UNION ALL
        SELECT "identification_number", "product_code"
        FROM "hash_testing_hist"
        WHERE "sys_op" > 1) AS "t3"
    WHERE "identification_number" = 1"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();
    assert_eq!(
        format!(
            "{} {} {} {} {} {} {}",
            r#"SELECT "t3"."identification_number" as "identification_number", "t3"."product_code" as "product_code" FROM"#,
            r#"(SELECT "hash_testing"."identification_number" as "identification_number", "hash_testing"."product_code" as "product_code""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."sys_op") = (1)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."identification_number" as "identification_number", "hash_testing_hist"."product_code" as "product_code""#,
            r#"FROM "hash_testing_hist" WHERE ("hash_testing_hist"."sys_op") > (1)) as "t3""#,
            r#"WHERE ("t3"."identification_number") = (1)"#,
        ),
        sql
    );
}

#[test]
fn union_complex_cond_query_transform() {
    let query = r#"SELECT *
FROM
    (SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "sys_op" = 1
    UNION ALL
    SELECT "identification_number", "product_code"
    FROM "hash_testing_hist"
    WHERE "sys_op" > 1) AS "t3"
WHERE ("identification_number" = 1
    OR ("identification_number" = 2
    OR "identification_number" = 3))
    AND ("product_code" = '1'
    OR "product_code" = '2')"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {} {} {} {}",
            r#"SELECT "t3"."identification_number" as "identification_number", "t3"."product_code" as "product_code" FROM"#,
            r#"(SELECT "hash_testing"."identification_number" as "identification_number", "hash_testing"."product_code" as "product_code""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."sys_op") = (1)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_testing_hist"."identification_number" as "identification_number", "hash_testing_hist"."product_code" as "product_code""#,
            r#"FROM "hash_testing_hist" WHERE ("hash_testing_hist"."sys_op") > (1)) as "t3""#,
            r#"WHERE (("t3"."identification_number") = (1) or (("t3"."identification_number") = (2) or ("t3"."identification_number") = (3)))"#,
            r#"and (("t3"."product_code") = ('1') or ("t3"."product_code") = ('2'))"#,
        ),
        sql
    );
}

#[test]
fn sub_query_in_selection() {
    let query = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
    WHERE "identification_number" in (
    SELECT "identification_number" FROM "hash_testing_hist" WHERE "product_code" = 'a')"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT "hash_testing"."identification_number" as "identification_number", "hash_testing"."product_code" as "product_code""#,
            r#"FROM "hash_testing" WHERE ("hash_testing"."identification_number") in"#,
            r#"(SELECT "hash_testing_hist"."identification_number" as "identification_number" FROM "hash_testing_hist""#,
            r#"WHERE ("hash_testing_hist"."product_code") = ('a'))"#,
        ),
        sql
    );
}

#[test]
fn inner_join() {
    let query = r#"SELECT "id", "product_units" FROM "hash_testing"
        INNER JOIN (SELECT "id" FROM "test_space") as t
        ON "hash_testing"."identification_number" = t."id"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT t."id" as "id", "hash_testing"."product_units" as "product_units""#,
            r#"FROM "hash_testing" INNER JOIN (SELECT "test_space"."id" as "id" FROM "test_space") as t"#,
            r#"ON ("hash_testing"."identification_number") = (t."id")"#,
            r#"WHERE ("hash_testing"."identification_number") = (5) and ("hash_testing"."product_code") = ('123')"#,
        ),
        sql
    );
}

#[test]
fn inner_join_duplicate_columns() {
    // Tables "test_space" and "hash_testing" have the same columns "sys_op" and "bucket_id",
    // that cause a "duplicate column" error in the output tuple of the INNER JOIN node.
    // The error is handled by renaming the columns within a sub-query (inner_join test).
    //
    // TODO: improve reference resolution to avoid this error.
    let query = r#"SELECT "id", "product_units" FROM "hash_testing"
        INNER JOIN "test_space" as t
        ON "hash_testing"."identification_number" = t."id"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan_err = ast.to_ir(metadata).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(
            "Row can't be added because `\"sys_op\"` already has an alias".into()
        ),
        plan_err
    );
}

#[test]
fn simple_query_with_unquoted_aliases() {
    let query = r#"SELECT t."identification_number", "product_code" FROM "hash_testing" as t
    WHERE t."identification_number" = 1"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT t."identification_number" as "identification_number", t."product_code" as "product_code""#,
            r#"FROM "hash_testing" as t WHERE (t."identification_number") = (1)"#,
        ),
        sql
    );
}

#[test]
fn inner_join_1() {
    let query = r#"SELECT *
    FROM
        (SELECT "id", "FIRST_NAME"
        FROM "test_space"
        WHERE "sys_op" < 0
                AND "sysFrom" >= 0
        UNION ALL
        SELECT "id", "FIRST_NAME"
        FROM "test_space_hist"
        WHERE "sysFrom" <= 0) AS "t3"
    INNER JOIN
        (SELECT "identification_number", "product_code"
        FROM "hash_testing_hist"
        WHERE "sys_op" > 0
        UNION ALL
        SELECT "identification_number", "product_code"
        FROM "hash_single_testing_hist"
        WHERE "sys_op" <= 0) AS "t8"
        ON "t3"."id" = "t8"."identification_number"
    WHERE "id" = 1 AND "t8"."identification_number" = 1 AND "product_code" = '123'"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();
    let ex_plan = ExecutionPlan::from(&plan);

    let top_id = plan.get_top().unwrap();
    let sql = ex_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {} {}",
            r#"SELECT "t3"."id" as "id", "t3"."FIRST_NAME" as "FIRST_NAME","#,
            r#""t8"."identification_number" as "identification_number","#,
            r#""t8"."product_code" as "product_code" FROM"#,
            r#"(SELECT "test_space"."id" as "id", "test_space"."FIRST_NAME" as "FIRST_NAME""#,
            r#"FROM "test_space" WHERE ("test_space"."sys_op") < (0) and ("test_space"."sysFrom") >= (0)"#,
            r#"UNION ALL"#,
            r#"SELECT "test_space_hist"."id" as "id", "test_space_hist"."FIRST_NAME" as "FIRST_NAME""#,
            r#"FROM "test_space_hist" WHERE ("test_space_hist"."sysFrom") <= (0))"#,
            r#"as "t3""#,
            r#"INNER JOIN"#,
            r#"(SELECT "hash_testing_hist"."identification_number" as "identification_number","#,
            r#""hash_testing_hist"."product_code" as "product_code" FROM "hash_testing_hist" WHERE ("hash_testing_hist"."sys_op") > (0)"#,
            r#"UNION ALL"#,
            r#"SELECT "hash_single_testing_hist"."identification_number" as "identification_number","#,
            r#""hash_single_testing_hist"."product_code" as "product_code" FROM "hash_single_testing_hist""#,
            r#"WHERE ("hash_single_testing_hist"."sys_op") <= (0))"#,
            r#"as "t8" ON ("t3"."id") = ("t8"."identification_number")"#,
            r#"WHERE ("t3"."id") = (1) and ("t8"."identification_number") = (1) and ("t8"."product_code") = ('123')"#,
        ),
        sql
    );
}
