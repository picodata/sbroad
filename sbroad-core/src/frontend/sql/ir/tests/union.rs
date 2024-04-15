use pretty_assertions::assert_eq;

use crate::ir::transformation::helpers::sql_to_optimized_ir;

#[test]
fn front_select_chaning_1() {
    let input = r#"
    select "product_code" from "hash_testing"
    union all
    select "e" from "t2"
    union all
    select "a" from "t3"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    union all
        projection ("hash_testing"."product_code"::string -> "product_code")
            scan "hash_testing"
        projection ("t2"."e"::unsigned -> "e")
            scan "t2"
    projection ("t3"."a"::string -> "a")
        scan "t3"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_select_chaining_2() {
    let input = r#"
    select "product_code" from "hash_testing"
    union all
    select "e" from "t2"
    union
    select "a" from "t3"
    except
    select "b" from "t3"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"except
    motion [policy: full]
        union
            union all
                projection ("hash_testing"."product_code"::string -> "product_code")
                    scan "hash_testing"
                projection ("t2"."e"::unsigned -> "e")
                    scan "t2"
            projection ("t3"."a"::string -> "a")
                scan "t3"
    motion [policy: full]
        intersect
            projection ("t3"."b"::integer -> "b")
                scan "t3"
            motion [policy: full]
                union
                    union all
                        projection ("hash_testing"."product_code"::string -> "product_code")
                            scan "hash_testing"
                        projection ("t2"."e"::unsigned -> "e")
                            scan "t2"
                    projection ("t3"."a"::string -> "a")
                        scan "t3"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}

#[test]
fn front_select_chaining_3() {
    let input = r#"
    select "product_code" from "hash_testing"
    order by "product_code"
    union all
    select "e" from "t2"
    order by "e"
    "#;

    let plan = sql_to_optimized_ir(input, vec![]);

    let expected_explain = String::from(
        r#"union all
    motion [policy: segment([ref("product_code")])]
        projection ("product_code"::string -> "product_code")
            order by ("product_code"::string)
                motion [policy: full]
                    projection ("hash_testing"."product_code"::string -> "product_code")
                        scan "hash_testing"
    motion [policy: segment([ref("e")])]
        projection ("e"::unsigned -> "e")
            order by ("e"::unsigned)
                motion [policy: full]
                    projection ("t2"."e"::unsigned -> "e")
                        scan "t2"
execution options:
sql_vdbe_max_steps = 45000
vtable_max_rows = 5000
"#,
    );

    assert_eq!(expected_explain, plan.as_explain().unwrap());
}
