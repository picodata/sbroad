use super::*;
use crate::executor::engine::mock::MetadataMock;
use pretty_assertions::assert_eq;

#[test]
fn simple_query_to_ir() {
    let query = r#"SELECT "identification_number", "product_code" FROM "hash_testing"
    WHERE "identification_number" = 1"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {}",
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code""#,
            r#"FROM "hash_testing" WHERE ("identification_number") = (1)"#,
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

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code""#,
            r#"FROM "hash_testing" WHERE (("identification_number") = (1) and ("product_code") = ('1')"#,
            r#"or ("identification_number") = (2) and ("product_code") = ('2'))"#,
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

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {} {} {}",
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code" FROM"#,
            r#"(SELECT "identification_number" as "identification_number", "product_code" as "product_code""#,
            r#"FROM "hash_testing" WHERE ("sys_op") = (1)"#,
            r#"UNION ALL"#,
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code""#,
            r#"FROM "hash_testing_hist" WHERE ("sys_op") > (1)) as "t3""#,
            r#"WHERE ("identification_number") = (1)"#,
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

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {} {} {} {}",
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code" FROM"#,
            r#"(SELECT "identification_number" as "identification_number", "product_code" as "product_code""#,
            r#"FROM "hash_testing" WHERE ("sys_op") = (1)"#,
            r#"UNION ALL"#,
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code""#,
            r#"FROM "hash_testing_hist" WHERE ("sys_op") > (1)) as "t3""#,
            r#"WHERE (("identification_number") = (1) or (("identification_number") = (2) or ("identification_number") = (3)))"#,
            r#"and (("product_code") = ('1') or ("product_code") = ('2'))"#,
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

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code""#,
            r#"FROM "hash_testing" WHERE ("identification_number") in"#,
            r#"(SELECT "identification_number" as "identification_number" FROM "hash_testing_hist""#,
            r#"WHERE ("product_code") = ('a'))"#,
        ),
        sql
    );
}

#[test]
fn inner_join() {
    let query = r#"SELECT * FROM "hash_testing"
        INNER JOIN "hash_testing_hist"
        ON "hash_testing"."identification_number" = "hash_testing_hist"."identification_number"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;

    let metadata = &MetadataMock::new();
    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan_err = ast.to_ir(metadata).unwrap_err();

    assert_eq!("Joins are not implemented yet.", format!("{}", plan_err));
}
