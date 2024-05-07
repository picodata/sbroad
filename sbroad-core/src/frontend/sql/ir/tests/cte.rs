use crate::errors::{Entity, SbroadError};
use crate::executor::engine::mock::RouterConfigurationMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::frontend::Ast;
use crate::ir::transformation::helpers::sql_to_optimized_ir;
use pretty_assertions::assert_eq;

#[test]
fn cte() {
    let sql = r#"WITH cte (a) AS (SELECT first_name FROM "test_space") SELECT * FROM cte"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("CTE"."A"::string -> "A")
    scan cte "CTE"($0)
subquery $0:
motion [policy: full]
            projection ("test_space"."FIRST_NAME"::string -> "A")
                scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn global_cte() {
    let sql = r#"WITH cte (a) AS (SELECT "a" FROM "global_t") SELECT * FROM cte"#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("CTE"."A"::integer -> "A")
    scan cte "CTE"($0)
subquery $0:
projection ("global_t"."a"::integer -> "A")
            scan "global_t"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn nested_cte() {
    let sql = r#"
        WITH cte1 (a) AS (SELECT first_name FROM "test_space"),
        cte2 AS (SELECT * FROM cte1)
        SELECT * FROM cte2
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("CTE2"."A"::string -> "A")
    scan cte "CTE2"($1)
subquery $0:
motion [policy: full]
                    projection ("test_space"."FIRST_NAME"::string -> "A")
                        scan "test_space"
subquery $1:
projection ("CTE1"."A"::string -> "A")
            scan cte "CTE1"($0)
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn reuse_cte() {
    let sql = r#"
        WITH cte (a) AS (SELECT first_name FROM "test_space")
        SELECT * FROM cte
        UNION ALL
        SELECT * FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"union all
    projection ("CTE"."A"::string -> "A")
        scan cte "CTE"($0)
    projection ("CTE"."A"::string -> "A")
        scan cte "CTE"($0)
subquery $0:
motion [policy: full]
                projection ("test_space"."FIRST_NAME"::string -> "A")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn reuse_cte_values() {
    let sql = r#"
        WITH cte (b) AS (VALUES(1))
        SELECT t.c FROM (SELECT count(*) as c FROM cte c1 JOIN cte c2 ON true) t
        JOIN cte ON true
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("T"."C"::integer -> "C")
    join on true::boolean
        scan "T"
            projection (count((*::integer))::integer -> "C")
                join on true::boolean
                    scan cte "C1"($0)
                    scan cte "C2"($0)
        scan cte "CTE"($1)
subquery $0:
motion [policy: full]
                            projection ("CTE"."COLUMN_1"::unsigned -> "B")
                                scan "CTE"
                                    values
                                        value row (data=ROW(1::unsigned))
subquery $1:
motion [policy: full]
                projection ("CTE"."COLUMN_1"::unsigned -> "B")
                    scan "CTE"
                        values
                            value row (data=ROW(1::unsigned))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn join_cte() {
    let sql = r#"
        WITH cte (a) AS (SELECT first_name FROM "test_space")
        SELECT t.first_name FROM "test_space" t
        JOIN cte ON t.first_name = cte.a
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("T"."FIRST_NAME"::string -> "FIRST_NAME")
    join on ROW("T"."FIRST_NAME"::string) = ROW("CTE"."A"::string)
        scan "T"
            projection ("T"."id"::unsigned -> "id", "T"."sysFrom"::unsigned -> "sysFrom", "T"."FIRST_NAME"::string -> "FIRST_NAME", "T"."sys_op"::unsigned -> "sys_op")
                scan "test_space" -> "T"
        scan cte "CTE"($0)
subquery $0:
motion [policy: full]
                projection ("test_space"."FIRST_NAME"::string -> "A")
                    scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn agg_cte() {
    let sql = r#"
        WITH cte (a) AS (SELECT first_name FROM "test_space")
        SELECT count(a) FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection (count(("CTE"."A"::string))::integer -> "COL_1")
    scan cte "CTE"($0)
subquery $0:
motion [policy: full]
            projection ("test_space"."FIRST_NAME"::string -> "A")
                scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn sq_cte() {
    let sql = r#"
        WITH cte (a) AS (SELECT first_name FROM "test_space" WHERE first_name = 'hi')
        SELECT first_name FROM "test_space" WHERE first_name IN (SELECT a FROM cte)
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("test_space"."FIRST_NAME"::string -> "FIRST_NAME")
    selection ROW("test_space"."FIRST_NAME"::string) in ROW($1)
        scan "test_space"
subquery $0:
motion [policy: full]
                        projection ("test_space"."FIRST_NAME"::string -> "A")
                            selection ROW("test_space"."FIRST_NAME"::string) = ROW('hi'::string)
                                scan "test_space"
subquery $1:
scan
            projection ("CTE"."A"::string -> "A")
                scan cte "CTE"($0)
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn values_in_cte() {
    let sql = r#"
        WITH cte (a) AS (VALUES ('a'))
        SELECT * FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("CTE"."A"::string -> "A")
    scan cte "CTE"($0)
subquery $0:
motion [policy: full]
            projection ("CTE"."COLUMN_1"::string -> "A")
                scan "CTE"
                    values
                        value row (data=ROW('a'::string))
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn union_in_cte() {
    let sql = r#"
        WITH cte1 (a) AS (VALUES ('a')),
        cte2 as (SELECT * FROM cte1 UNION ALL SELECT * FROM cte1)
        SELECT * FROM cte2
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("CTE2"."A"::string -> "A")
    scan cte "CTE2"($1)
subquery $0:
motion [policy: full]
                            motion [policy: full]
                                projection ("CTE1"."COLUMN_1"::string -> "A")
                                    scan "CTE1"
                                        values
                                            value row (data=ROW('a'::string))
subquery $1:
motion [policy: full]
            union all
                projection ("CTE1"."A"::string -> "A")
                    scan cte "CTE1"($0)
                projection ("CTE1"."A"::string -> "A")
                    scan cte "CTE1"($0)
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn join_in_cte() {
    let sql = r#"
        WITH cte AS (
            SELECT t1.first_name FROM "test_space" t1
            JOIN "test_space" t2 ON t1.first_name = t2."id"
        )
        SELECT * FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("CTE"."FIRST_NAME"::string -> "FIRST_NAME")
    scan cte "CTE"($0)
subquery $0:
motion [policy: full]
            projection ("T1"."FIRST_NAME"::string -> "FIRST_NAME")
                join on ROW("T1"."FIRST_NAME"::string) = ROW("T2"."id"::unsigned)
                    scan "T1"
                        projection ("T1"."id"::unsigned -> "id", "T1"."sysFrom"::unsigned -> "sysFrom", "T1"."FIRST_NAME"::string -> "FIRST_NAME", "T1"."sys_op"::unsigned -> "sys_op")
                            scan "test_space" -> "T1"
                    motion [policy: full]
                        scan "T2"
                            projection ("T2"."id"::unsigned -> "id", "T2"."sysFrom"::unsigned -> "sysFrom", "T2"."FIRST_NAME"::string -> "FIRST_NAME", "T2"."sys_op"::unsigned -> "sys_op")
                                scan "test_space" -> "T2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn order_by_in_cte() {
    let sql = r#"
        WITH cte AS (
            SELECT first_name FROM "test_space"
            ORDER BY 1
        )
        SELECT * FROM cte
    "#;
    let plan = sql_to_optimized_ir(sql, vec![]);

    let expected_explain = String::from(
        r#"projection ("CTE"."FIRST_NAME"::string -> "FIRST_NAME")
    scan cte "CTE"($0)
subquery $0:
projection ("FIRST_NAME"::string -> "FIRST_NAME")
            order by (1)
                motion [policy: full]
                    scan
                        projection ("test_space"."FIRST_NAME"::string -> "FIRST_NAME")
                            scan "test_space"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );
    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn table_name_conflict() {
    let sql = r#"
        WITH "test_space" AS (SELECT first_name FROM "test_space")
        SELECT * FROM "test_space"
    "#;
    let metadata = &RouterConfigurationMock::new();
    let plan_error = AbstractSyntaxTree::transform_into_plan(sql, metadata);
    assert_eq!(
        plan_error,
        Err(SbroadError::Invalid(
            Entity::Table,
            Some(r#"table with name "test_space" is already defined as a CTE"#.into())
        ))
    );
}

#[test]
fn cte_name_conflict() {
    let sql = r#"
        WITH cte AS (SELECT first_name FROM "test_space"),
        cte as (SELECT 'a' as a from "test_space")
        SELECT * FROM cte
    "#;
    let metadata = &RouterConfigurationMock::new();
    let plan_error = AbstractSyntaxTree::transform_into_plan(sql, metadata);
    assert_eq!(
        plan_error,
        Err(SbroadError::Invalid(
            Entity::Cte,
            Some(r#"CTE with name "CTE" is already defined"#.into())
        ))
    );
}

#[test]
fn cte_column_mismatch() {
    let sql = r#"
        WITH cte(a) AS (SELECT first_name, first_name FROM "test_space")
        SELECT * FROM cte
    "#;
    let metadata = &RouterConfigurationMock::new();
    let plan_error = AbstractSyntaxTree::transform_into_plan(sql, metadata);
    assert_eq!(
        plan_error,
        Err(SbroadError::UnexpectedNumberOfValues(
            "expected 2 columns in CTE, got 1".into()
        ))
    );
}
