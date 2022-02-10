use crate::executor::engine::mock::MetadataMock;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::operator::Bool;
use crate::ir::relation::*;
use crate::ir::value::*;
use crate::ir::Plan;
use pretty_assertions::assert_eq;

#[test]
fn one_table_projection() {
    let query = r#"SELECT "identification_number", "product_code"
    FROM "hash_testing"
    WHERE "identification_number" = 1"#;

    let metadata = &MetadataMock::new();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("identification_number") = 1"#
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

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT "identification_number" as "identification_number", "product_code" as "product_code","#,
            r#""product_units" as "product_units", "sys_op" as "sys_op", "bucket_id" as "bucket_id""#,
            r#"FROM "hash_testing""#,
            r#"WHERE ("identification_number") = 1"#
        ),
        sql
    );
}

#[test]
fn union_all() {
    let query = r#"SELECT product_code
    FROM "hash_testing"
    WHERE "identification_number" = 1
    UNION ALL
    SELECT product_code
    FROM "hash_testing_hist"
    WHERE "product_code" = 'a' 
    "#;

    let metadata = &MetadataMock::new();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "product_code" as "product_code" FROM "hash_testing" WHERE ("identification_number") = 1"#,
            r#"UNION ALL"#,
            r#"SELECT "product_code" as "product_code" FROM "hash_testing_hist" WHERE ("product_code") = 'a'"#
        ),
        sql
    );
}

#[test]
fn from_sub_query() {
    let query = r#"SELECT product_code
    FROM (SELECT product_code
    FROM "hash_testing"
    WHERE "identification_number" = 1) as t1
    WHERE "product_code" = 'a'"#;

    let metadata = &MetadataMock::new();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {}",
            r#"SELECT "product_code" as "product_code" FROM"#,
            r#"(SELECT "product_code" as "product_code" FROM "hash_testing" WHERE ("identification_number") = 1) as "t1""#,
            r#"WHERE ("product_code") = 'a'"#
        ),
        sql
    );
}

#[test]
fn from_sub_query_with_union() {
    let query = r#"SELECT product_code
  FROM (SELECT product_code
    FROM "hash_testing"
    WHERE "identification_number" = 1
    UNION ALL
    SELECT product_code
    FROM "hash_testing_hist"
    WHERE "product_code" = 'a') as "t1"
  WHERE "product_code" = 'a'"#;

    let metadata = &MetadataMock::new();

    let ast = AbstractSyntaxTree::new(query).unwrap();
    let plan = ast.to_ir(metadata).unwrap();

    let top_id = plan.get_top().unwrap();
    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "product_code" as "product_code" FROM"#,
            r#"(SELECT "product_code" as "product_code" FROM "hash_testing" WHERE ("identification_number") = 1"#,
            r#"UNION ALL"#,
            r#"SELECT "product_code" as "product_code" FROM "hash_testing_hist" WHERE ("product_code") = 'a') as "t1""#,
            r#"WHERE ("product_code") = 'a'"#,
        ),
        sql
    );
}

#[test]
fn inner_join() {
    // TODO: Replace with SQL when frontend implements inner joins.

    // SELECT product_code FROM "hash_testing" join "history"
    // on "identification_number" = "id"
    // WHERE "product_code" = 'a'
    let mut plan = Plan::new();

    let hash_testing = Table::new_seg(
        "hash_testing",
        vec![
            Column::new("product_code", Type::Number),
            Column::new("identification_number", Type::String),
        ],
        &["identification_number", "product_code"],
    )
    .unwrap();
    plan.add_rel(hash_testing);
    let scan_hash_testing = plan.add_scan("hash_testing").unwrap();

    let history =
        Table::new_seg("history", vec![Column::new("id", Type::Number)], &["id"]).unwrap();
    plan.add_rel(history);
    let scan_history = plan.add_scan("history").unwrap();

    let left_row = plan
        .add_row_from_left_branch(scan_hash_testing, scan_history, &["identification_number"])
        .unwrap();
    let right_row = plan
        .add_row_from_right_branch(scan_hash_testing, scan_history, &["id"])
        .unwrap();
    let condition = plan.nodes.add_bool(left_row, Bool::Eq, right_row).unwrap();
    let join = plan
        .add_join(scan_hash_testing, scan_history, condition)
        .unwrap();
    let product_code = plan.add_row_from_child(join, &["product_code"]).unwrap();
    let const_a = plan.add_const(Value::string_from_str("a"));
    let condition = plan
        .nodes
        .add_bool(product_code, Bool::Eq, const_a)
        .unwrap();
    let select = plan.add_select(&[join], condition).unwrap();
    let project = plan.add_proj(select, &["product_code"]).unwrap();
    plan.set_top(project).unwrap();

    let top_id = plan.get_top().unwrap();

    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT "product_code" as "product_code" FROM "hash_testing""#,
            r#"INNER JOIN "history""#,
            r#"on ("identification_number") = ("id")"#,
            r#"WHERE ("product_code") = 'a'"#,
        ),
        sql
    );
}

#[test]
fn inner_join_with_sq() {
    // TODO: Replace with SQL when frontend implements inner joins.

    // SELECT product_code FROM "hash_testing" join
    // (SELECT * FROM "history" WHERE "id" = 1) as "t"
    // on "identification_number" = "id"
    // WHERE "product_code" = 'a'
    let mut plan = Plan::new();

    let hash_testing = Table::new_seg(
        "hash_testing",
        vec![
            Column::new("product_code", Type::Number),
            Column::new("identification_number", Type::String),
        ],
        &["identification_number", "product_code"],
    )
    .unwrap();
    plan.add_rel(hash_testing);
    let scan_hash_testing = plan.add_scan("hash_testing").unwrap();

    let history =
        Table::new_seg("history", vec![Column::new("id", Type::Number)], &["id"]).unwrap();
    plan.add_rel(history);
    let scan_history = plan.add_scan("history").unwrap();
    let history_id_row = plan.add_row_from_child(scan_history, &["id"]).unwrap();
    let hs_const_1 = plan.add_const(Value::number_from_str("1").unwrap());
    let hs_condition = plan
        .nodes
        .add_bool(history_id_row, Bool::Eq, hs_const_1)
        .unwrap();
    let hs_select = plan.add_select(&[scan_history], hs_condition).unwrap();
    let hs_project = plan.add_proj(hs_select, &[]).unwrap();
    let hs_sq = plan.add_sub_query(hs_project, Some("t")).unwrap();

    let left_row = plan
        .add_row_from_left_branch(scan_hash_testing, hs_sq, &["identification_number"])
        .unwrap();
    let right_row = plan
        .add_row_from_right_branch(scan_hash_testing, hs_sq, &["id"])
        .unwrap();
    let condition = plan.nodes.add_bool(left_row, Bool::Eq, right_row).unwrap();
    let join = plan.add_join(scan_hash_testing, hs_sq, condition).unwrap();
    let product_code = plan.add_row_from_child(join, &["product_code"]).unwrap();
    let const_a = plan.add_const(Value::string_from_str("a"));
    let condition = plan
        .nodes
        .add_bool(product_code, Bool::Eq, const_a)
        .unwrap();
    let select = plan.add_select(&[join], condition).unwrap();
    let project = plan.add_proj(select, &["product_code"]).unwrap();
    plan.set_top(project).unwrap();

    let top_id = plan.get_top().unwrap();

    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {} {}",
            r#"SELECT "product_code" as "product_code" FROM "hash_testing""#,
            r#"INNER JOIN"#,
            r#"(SELECT "id" as "id" FROM "history" WHERE ("id") = 1) as "t""#,
            r#"on ("identification_number") = ("id")"#,
            r#"WHERE ("product_code") = 'a'"#,
        ),
        sql
    );
}

#[test]
fn selection_with_sq() {
    // TODO: Replace with SQL when frontend implements sub-queries.

    // SELECT product_code FROM "hash_testing"
    // WHERE "identification_number" in
    // (SELECT id FROM "history" WHERE "id" = 1)
    // AND "product_code" < 'a'
    let mut plan = Plan::new();

    let hash_testing = Table::new_seg(
        "hash_testing",
        vec![
            Column::new("product_code", Type::Number),
            Column::new("identification_number", Type::String),
        ],
        &["identification_number", "product_code"],
    )
    .unwrap();
    plan.add_rel(hash_testing);
    let scan_hash_testing = plan.add_scan("hash_testing").unwrap();

    let history =
        Table::new_seg("history", vec![Column::new("id", Type::Number)], &["id"]).unwrap();
    plan.add_rel(history);
    let scan_history = plan.add_scan("history").unwrap();
    let history_id_row = plan.add_row_from_child(scan_history, &["id"]).unwrap();
    let hs_const_1 = plan.add_const(Value::number_from_str("1").unwrap());
    let hs_condition = plan
        .nodes
        .add_bool(history_id_row, Bool::Eq, hs_const_1)
        .unwrap();
    let hs_select = plan.add_select(&[scan_history], hs_condition).unwrap();
    let hs_project = plan.add_proj(hs_select, &["id"]).unwrap();
    let hs_sq = plan.add_sub_query(hs_project, None).unwrap();

    let identification_number = plan
        .add_row_from_child(scan_hash_testing, &["identification_number"])
        .unwrap();
    let bool_in = plan
        .nodes
        .add_bool(identification_number, Bool::In, hs_sq)
        .unwrap();
    let product_code = plan
        .add_row_from_child(scan_hash_testing, &["product_code"])
        .unwrap();
    let const_a = plan.add_const(Value::string_from_str("a"));
    let bool_lt = plan
        .nodes
        .add_bool(product_code, Bool::Lt, const_a)
        .unwrap();
    let bool_and = plan.nodes.add_bool(bool_in, Bool::And, bool_lt).unwrap();
    let select = plan.add_select(&[scan_hash_testing], bool_and).unwrap();
    let project = plan.add_proj(select, &["product_code"]).unwrap();
    plan.set_top(project).unwrap();

    let top_id = plan.get_top().unwrap();

    let sql = plan.subtree_as_sql(top_id).unwrap();

    assert_eq!(
        format!(
            "{} {} {} {}",
            r#"SELECT "product_code" as "product_code" FROM "hash_testing""#,
            r#"WHERE ("identification_number") in"#,
            r#"(SELECT "id" as "id" FROM "history" WHERE ("id") = 1)"#,
            r#"and ("product_code") < 'a'"#,
        ),
        sql
    );
}
