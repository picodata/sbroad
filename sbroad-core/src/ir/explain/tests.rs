use pretty_assertions::assert_eq;

use super::*;
use crate::ir::transformation::helpers::sql_to_optimized_ir;

#[test]
fn simple_query_without_cond_plan() {
    let query =
        r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("t"."identification_number" -> "c1", "t"."product_code" -> "product_code")
    scan "hash_testing" -> "t"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn simple_query_with_cond_plan() {
    let query = r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t" WHERE "t"."identification_number" = 1 AND "t"."product_code" = '222'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("t"."identification_number" -> "c1", "t"."product_code" -> "product_code")
    selection ROW("t"."identification_number", "t"."product_code") = ROW(1, '222')
        scan "hash_testing" -> "t"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn union_query_plan() {
    let query = r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t"
        UNION ALL
        SELECT "t2"."identification_number", "product_code" FROM "hash_testing_hist" as "t2""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let expected = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        r#"union all"#,
        r#"    projection ("t"."identification_number" -> "c1", "t"."product_code" -> "product_code")"#,
        r#"        scan "hash_testing" -> "t""#,
        r#"    projection ("t2"."identification_number" -> "identification_number", "t2"."product_code" -> "product_code")"#,
        r#"        scan "hash_testing_hist" -> "t2""#,
    );
    assert_eq!(expected, explain_tree.to_string());
}

#[test]
fn union_subquery_plan() {
    let query = r#"SELECT * FROM (
SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
UNION ALL
SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
) as "t"
WHERE "id" = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t"."id" -> "id", "t"."FIRST_NAME" -> "FIRST_NAME")
    selection ROW("t"."id") = ROW(1)
        scan "t"
            union all
                projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space"."sysFrom") < ROW(0) and ROW("test_space"."sys_op") > ROW(0)
                        scan "test_space"
                projection ("test_space_hist"."id" -> "id", "test_space_hist"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space_hist"."sys_op") < ROW(0)
                        scan "test_space_hist"
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn union_cond_subquery_plan() {
    let query = r#"SELECT * FROM (
SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
UNION ALL
SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
) as "t"
WHERE "id" IN (SELECT "id"
   FROM (
      SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0
      UNION ALL
      SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
  ) as "t2"
  WHERE "t2"."id" = 4)
"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t"."id" -> "id", "t"."FIRST_NAME" -> "FIRST_NAME")
    selection ROW("t"."id") in ROW($0)
        scan "t"
            union all
                projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space"."sysFrom") < ROW(0) and ROW("test_space"."sys_op") > ROW(0)
                        scan "test_space"
                projection ("test_space_hist"."id" -> "id", "test_space_hist"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space_hist"."sys_op") < ROW(0)
                        scan "test_space_hist"
subquery $0:
scan
            projection ("t2"."id" -> "id")
                selection ROW("t2"."id") = ROW(4)
                    scan "t2"
                        union all
                            projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                                selection ROW("test_space"."sys_op") > ROW(0)
                                    scan "test_space"
                            projection ("test_space_hist"."id" -> "id", "test_space_hist"."FIRST_NAME" -> "FIRST_NAME")
                                selection ROW("test_space_hist"."sys_op") < ROW(0)
                                    scan "test_space_hist"
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn explain_except1() {
    let query = r#"SELECT "product_code" as "pc" FROM "hash_testing" AS "t"
        EXCEPT DISTINCT
        SELECT "identification_number" FROM "hash_testing_hist""#;

    let plan = sql_to_optimized_ir(query, vec![]);
    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let expected = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n",
        r#"except"#,
        r#"    projection ("t"."product_code" -> "pc")"#,
        r#"        scan "hash_testing" -> "t""#,
        r#"    motion [policy: full, generation: none]"#,
        r#"        projection ("hash_testing_hist"."identification_number" -> "identification_number")"#,
        r#"            scan "hash_testing_hist""#,
    );
    assert_eq!(expected, explain_tree.to_string());
}

#[test]
fn motion_subquery_plan() {
    let query = r#"SELECT * FROM (
SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
UNION ALL
SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
) as "t"
WHERE "id" IN (SELECT "id"
   FROM (
      SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0
      UNION ALL
      SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
  ) as "t2"
  WHERE "t2"."id" = 4)
  OR "id" IN (SELECT "identification_number" FROM "hash_testing" WHERE "identification_number" = 5 AND "product_code" = '123')
"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t"."id" -> "id", "t"."FIRST_NAME" -> "FIRST_NAME")
    selection ROW("t"."id") in ROW($0) or ROW("t"."id") in ROW($1)
        scan "t"
            union all
                projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space"."sysFrom") < ROW(0) and ROW("test_space"."sys_op") > ROW(0)
                        scan "test_space"
                projection ("test_space_hist"."id" -> "id", "test_space_hist"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space_hist"."sys_op") < ROW(0)
                        scan "test_space_hist"
subquery $0:
scan
            projection ("t2"."id" -> "id")
                selection ROW("t2"."id") = ROW(4)
                    scan "t2"
                        union all
                            projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                                selection ROW("test_space"."sys_op") > ROW(0)
                                    scan "test_space"
                            projection ("test_space_hist"."id" -> "id", "test_space_hist"."FIRST_NAME" -> "FIRST_NAME")
                                selection ROW("test_space_hist"."sys_op") < ROW(0)
                                    scan "test_space_hist"
subquery $1:
motion [policy: segment([ref("identification_number")]), generation: none]
            scan
                projection ("hash_testing"."identification_number" -> "identification_number")
                    selection ROW("hash_testing"."identification_number", "hash_testing"."product_code") = ROW(5, '123')
                        scan "hash_testing"
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn motion_join_plan() {
    let query = r#"SELECT "t1"."FIRST_NAME"
FROM (SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" = 3) as "t1"
    JOIN (SELECT "identification_number", "product_code" FROM "hash_testing") as "t2" ON "t1"."id"="t2"."identification_number"
WHERE "t2"."product_code" = '123'"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t1"."FIRST_NAME" -> "FIRST_NAME")
    selection ROW("t2"."product_code") = ROW('123')
        join on ROW("t1"."id") = ROW("t2"."identification_number")
            scan "t1"
                projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space"."id") = ROW(3)
                        scan "test_space"
            motion [policy: segment([ref("identification_number")]), generation: none]
                scan "t2"
                    projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code")
                        scan "hash_testing"
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn sq_join_plan() {
    let query = r#"SELECT "t1"."FIRST_NAME"
FROM (SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" = 3) as "t1"
    JOIN "hash_testing" ON "t1"."id"=(SELECT "identification_number" FROM "hash_testing")"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t1"."FIRST_NAME" -> "FIRST_NAME")
    join on ROW("t1"."id") = ROW($0)
        scan "t1"
            projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                selection ROW("test_space"."id") = ROW(3)
                    scan "test_space"
        motion [policy: full, generation: none]
            scan "hash_testing"
                projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code", "hash_testing"."product_units" -> "product_units", "hash_testing"."sys_op" -> "sys_op")
                    scan "hash_testing"
subquery $0:
motion [policy: segment([ref("identification_number")]), generation: none]
            scan
                projection ("hash_testing"."identification_number" -> "identification_number")
                    scan "hash_testing"
"#);

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn unary_condition_plan() {
    let query = r#"SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "id" IS NULL and "FIRST_NAME" IS NOT NULL"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
    selection ROW("test_space"."FIRST_NAME") is not null and ROW("test_space"."id") is null
        scan "test_space"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn insert_plan() {
    let query = r#"INSERT INTO "test_space" ("id", "FIRST_NAME") VALUES (1, '123')"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"insert "test_space"
    motion [policy: segment([ref(COLUMN_1)]), generation: sharding_column]
        values
            value row (data=ROW(1, '123'))
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn multiply_insert_plan() {
    let query = r#"INSERT INTO "test_space" ("id", "FIRST_NAME") VALUES (1, '123'), (2, '456'), (3, '789')"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"insert "test_space"
    motion [policy: segment([ref(COLUMN_5)]), generation: sharding_column]
        values
            value row (data=ROW(1, '123'))
            value row (data=ROW(2, '456'))
            value row (data=ROW(3, '789'))
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn insert_select_plan() {
    let query = r#"INSERT INTO "test_space" ("id", "FIRST_NAME")
SELECT "identification_number", "product_code" FROM "hash_testing""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"insert "test_space"
    motion [policy: segment([ref("identification_number")]), generation: sharding_column]
        projection ("hash_testing"."identification_number" -> "identification_number", "hash_testing"."product_code" -> "product_code")
            scan "hash_testing"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_value_plan() {
    let query = r#"select * from (values (1))"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection (COLUMN_1 -> COLUMN_1)
    scan
        values
            value row (data=ROW(1))
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan1() {
    let query = r#"SELECT CAST("id" as unsigned) as "b" FROM "test_space""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id"::unsigned -> "b")
    scan "test_space"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan2() {
    let query = r#"SELECT "id", "FIRST_NAME" FROM "test_space" WHERE CAST("id" as integer) = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
    selection ROW("test_space"."id"::int) = ROW(1)
        scan "test_space"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan_nested() {
    let query = r#"SELECT cast(bucket_id("id") as string) FROM "test_space""#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("BUCKET_ID"("test_space"."id")::string -> "COLUMN_1")
    scan "test_space"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan_nested_where() {
    let query = r#"SELECT "id" FROM "test_space" WHERE cast(bucket_id("id") as string) = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id" -> "id")
    selection ROW("BUCKET_ID"("test_space"."id")::string) = ROW(1)
        scan "test_space"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

#[test]
fn select_cast_plan_nested_where2() {
    let query = r#"SELECT "id" FROM "test_space" WHERE bucket_id(cast(42 as string)) = 1"#;

    let plan = sql_to_optimized_ir(query, vec![]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("test_space"."id" -> "id")
    selection ROW("BUCKET_ID"(42::string)) = ROW(1)
        scan "test_space"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string());
}

pub(crate) fn explain_check(sql: &str, explain: &str) {
    let plan = sql_to_optimized_ir(sql, vec![]);
    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();
    assert_eq!(explain, explain_tree.to_string());
}

#[cfg(test)]
mod concat;
