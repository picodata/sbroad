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
