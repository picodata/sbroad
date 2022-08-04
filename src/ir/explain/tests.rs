use pretty_assertions::assert_eq;

use super::*;
use crate::ir::transformation::helpers::sql_to_ir;

#[test]
fn simple_query_without_cond_plan() {
    let query =
        r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t""#;

    let plan = sql_to_ir(query, &[]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("t"."identification_number" -> "c1", "t"."product_code" -> "product_code")
    scan "hash_testing" -> "t"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string())
}

#[test]
fn simple_query_with_cond_plan() {
    let query = r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t" WHERE "t"."identification_number" = 1 AND "t"."product_code" = '222'"#;

    let plan = sql_to_ir(query, &[]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(
        r#"projection ("t"."identification_number" -> "c1", "t"."product_code" -> "product_code")
    selection ROW("t"."identification_number") = ROW(1) and ROW("t"."product_code") = ROW('222')
        scan "hash_testing" -> "t"
"#,
    );

    assert_eq!(actual_explain, explain_tree.to_string())
}

#[test]
fn union_query_plan() {
    let query = r#"SELECT "t"."identification_number" as "c1", "product_code" FROM "hash_testing" as "t"
        UNION ALL
        SELECT "t2"."identification_number", "product_code" FROM "hash_testing_hist" as "t2""#;

    let plan = sql_to_ir(query, &[]);

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
    assert_eq!(expected, explain_tree.to_string())
}

#[test]
fn union_subquery_plan() {
    let query = r#"SELECT * FROM (
SELECT "id", "FIRST_NAME" FROM "test_space" WHERE "sys_op" > 0 and "sysFrom" < 0
UNION ALL
SELECT "id", "FIRST_NAME" FROM "test_space_hist" WHERE "sys_op" < 0
) as "t"
WHERE "id" = 1"#;

    let plan = sql_to_ir(query, &[]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t"."id" -> "id", "t"."FIRST_NAME" -> "FIRST_NAME")
    selection ROW("t"."id") = ROW(1)
        scan "t"
            union all
                projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space"."sys_op") > ROW(0) and ROW("test_space"."sysFrom") < ROW(0)
                        scan "test_space"
                projection ("test_space_hist"."id" -> "id", "test_space_hist"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space_hist"."sys_op") < ROW(0)
                        scan "test_space_hist"
"#);

    assert_eq!(actual_explain, explain_tree.to_string())
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
  AND "FIRST_NAME" IN (SELECT "FIRST_NAME" FROM "test_space" WHERE "id" = 5)
"#;

    let plan = sql_to_ir(query, &[]);

    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let mut actual_explain = String::new();
    actual_explain.push_str(r#"projection ("t"."id" -> "id", "t"."FIRST_NAME" -> "FIRST_NAME")
    selection ROW("t"."id") in ROW($0) and ROW("t"."FIRST_NAME") in ROW($1)
        scan "t"
            union all
                projection ("test_space"."id" -> "id", "test_space"."FIRST_NAME" -> "FIRST_NAME")
                    selection ROW("test_space"."sys_op") > ROW(0) and ROW("test_space"."sysFrom") < ROW(0)
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
scan
            projection ("test_space"."FIRST_NAME" -> "FIRST_NAME")
                selection ROW("test_space"."id") = ROW(5)
                    scan "test_space"
"#);

    assert_eq!(actual_explain, explain_tree.to_string())
}

#[test]
fn explain_except1() {
    let query = r#"SELECT "product_code" as "pc" FROM "hash_testing" AS "t"
        EXCEPT DISTINCT
        SELECT "identification_number" FROM "hash_testing_hist""#;

    let plan = sql_to_ir(query, &[]);
    let top = &plan.get_top().unwrap();
    let explain_tree = FullExplain::new(&plan, *top).unwrap();

    let expected = format!(
        "{}\n{}\n{}\n{}\n{}\n",
        r#"except"#,
        r#"    projection ("t"."product_code" -> "pc")"#,
        r#"        scan "hash_testing" -> "t""#,
        r#"    projection ("hash_testing_hist"."identification_number" -> "identification_number")"#,
        r#"        scan "hash_testing_hist""#,
    );
    assert_eq!(expected, explain_tree.to_string())
}
