use super::*;

use crate::executor::engine::mock::RouterRuntimeMock;
use pretty_assertions::assert_eq;

#[test]
fn front_ivalid_sql1() {
    // Tables "test_space" and "hash_testing" have the same columns "sys_op" and "bucket_id",
    // that cause a "duplicate column" error in the output tuple of the INNER JOIN node.
    // The error is handled by renaming the columns within a sub-query (inner_join test).
    //
    // TODO: improve reference resolution to avoid this error.
    let query = r#"SELECT "id", "product_units" FROM "hash_testing"
        INNER JOIN "test_space" as t
        ON "hash_testing"."identification_number" = t."id"
        WHERE "hash_testing"."identification_number" = 5 and "hash_testing"."product_code" = '123'"#;

    let metadata = &RouterRuntimeMock::new();
    let plan_err = Query::new(metadata, query, vec![]).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(
            "Row can't be added because `\"sys_op\"` already has an alias".into()
        ),
        plan_err
    );
}

#[test]
fn front_invalid_sql2() {
    let query = r#"INSERT INTO "t" ("a") VALUES(1, 2)"#;

    let metadata = &RouterRuntimeMock::new();
    let plan_err = Query::new(metadata, query, vec![]).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(
            r#"Invalid number of values: 2. Table "t" expects 1 column(s)."#.into()
        ),
        plan_err
    );
}

#[test]
fn front_invalid_sql3() {
    let query = r#"INSERT INTO "t" SELECT "b", "d" FROM "t""#;

    let metadata = &RouterRuntimeMock::new();
    let plan_err = Query::new(metadata, query, vec![]).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(
            r#"Invalid number of values: 2. Table "t" expects 4 column(s)."#.into()
        ),
        plan_err
    );
}

#[test]
fn front_invalid_sql4() {
    let query = r#"INSERT INTO "t" VALUES(1, 2)"#;

    let metadata = &RouterRuntimeMock::new();
    let plan_err = Query::new(metadata, query, vec![]).unwrap_err();

    assert_eq!(
        QueryPlannerError::CustomError(
            r#"Invalid number of values: 2. Table "t" expects 4 column(s)."#.into()
        ),
        plan_err
    );
}

#[test]
fn front_explain_select_sql1() {
    let sql = r#"EXPLAIN SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t""#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();

    let expected_explain = String::from(
        r#"projection ("t"."identification_number" -> "c1", "t"."product_code" -> "product_code")
    scan "hash_testing" -> "t"
"#,
    );

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<String>() {
        assert_eq!(expected_explain, *actual_explain);
    } else {
        panic!("Explain must be string")
    }
}

#[test]
fn front_explain_select_sql2() {
    let sql = r#"EXPLAIN SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t"
        UNION ALL
        SELECT "t2"."identification_number", "product_code" FROM "hash_testing_hist" as "t2""#;

    let metadata = &RouterRuntimeMock::new();
    let mut query = Query::new(metadata, sql, vec![]).unwrap();

    let expected_explain = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        r#"union all"#,
        r#"    projection ("t"."identification_number" -> "c1", "t"."product_code" -> "product_code")"#,
        r#"        scan "hash_testing" -> "t""#,
        r#"    projection ("t2"."identification_number" -> "identification_number", "t2"."product_code" -> "product_code")"#,
        r#"        scan "hash_testing_hist" -> "t2""#,
    );

    if let Ok(actual_explain) = query.dispatch().unwrap().downcast::<String>() {
        assert_eq!(expected_explain, *actual_explain);
    } else {
        panic!("Explain must be string")
    }
}