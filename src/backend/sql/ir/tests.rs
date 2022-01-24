use crate::ir::Plan;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn one_table_ir_sql() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("frontend")
        .join("sql")
        .join("ir")
        .join("simple_query.yaml");
    let yml_str = fs::read_to_string(path).unwrap();
    let mut test_plan = Plan::from_yaml(&yml_str).unwrap();

    test_plan.build_relational_map();

    let top_id = test_plan.get_top().unwrap();
    let actual = test_plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        r#"SELECT "identification_number", "product_code" FROM "hash_testing" WHERE "identification_number" = 1"#,
        actual
    )
}
