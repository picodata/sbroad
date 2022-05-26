use std::fs;
use std::path::Path;

use pretty_assertions::assert_eq;

use crate::collection;
use crate::errors::QueryPlannerError;
use crate::ir::distribution::*;
use crate::ir::relation::*;
use crate::ir::value::*;
use crate::ir::*;

use super::*;

#[test]
fn scan_rel() {
    let mut plan = Plan::new();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Number, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
            Column::new("d", Type::String, ColumnRole::User),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_output = 8;
    let scan_node = 9;

    let scan_id = plan.add_scan("t", None).unwrap();
    assert_eq!(scan_node, scan_id);
    plan.top = Some(scan_node);

    plan.set_distribution(scan_output).unwrap();
    if let Node::Expression(row) = plan.get_node(scan_output).unwrap() {
        assert_eq!(
            row.distribution().unwrap(),
            &Distribution::Segment {
                keys: collection! { Key::new(vec![1, 0]) }
            }
        );
    } else {
        panic!("Wrong output node type!");
    }
}

#[test]
fn scan_rel_serialized() {
    let mut plan = Plan::new();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Number, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
            Column::new("d", Type::String, ColumnRole::User),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();
    plan.top = Some(scan_id);

    let scan_output = scan_id - 1;

    plan.set_distribution(scan_output).unwrap();

    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("scan_rel.yaml");
    let s = fs::read_to_string(path).unwrap();
    assert_eq!(plan, Plan::from_yaml(&s).unwrap());
}

#[test]
fn projection() {
    let mut plan = Plan::new();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Number, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
            Column::new("d", Type::String, ColumnRole::User),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();

    // Invalid alias names in the output
    assert_eq!(
        QueryPlannerError::CustomError(
            r#"Some of the columns ["a", "e"] were not found in the table"#.into()
        ),
        plan.add_proj(scan_id, &["a", "e"]).unwrap_err()
    );

    // Expression node instead of relational one
    assert_eq!(
        QueryPlannerError::CustomError("Node isn't relational".into()),
        plan.add_proj(1, &["a"]).unwrap_err()
    );

    // Try to build projection from the non-existing node
    assert_eq!(
        QueryPlannerError::ValueOutOfRange,
        plan.add_proj(42, &["a"]).unwrap_err()
    );
}

#[test]
fn projection_serialize() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("projection.yaml");
    let s = fs::read_to_string(path).unwrap();
    Plan::from_yaml(&s).unwrap();
}

#[test]
fn selection() {
    // select * from t where (a, b) = (1, 10)

    let mut plan = Plan::new();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Number, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
            Column::new("d", Type::String, ColumnRole::User),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();

    let ref_row = plan.add_row_from_child(scan_id, &["a", "b"]).unwrap();
    let const_1 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let const_10 = plan.nodes.add_const(Value::number_from_str("10").unwrap());
    let const_row = plan.nodes.add_row(vec![const_1, const_10], None);
    let gt_id = plan.nodes.add_bool(ref_row, Bool::Gt, const_row).unwrap();

    // Correct Selection operator
    plan.add_select(&[scan_id], gt_id).unwrap();

    // Non-trivalent filter
    assert_eq!(
        QueryPlannerError::CustomError("Filter expression is not a trivalent expression.".into()),
        plan.add_select(&[scan_id], const_row).unwrap_err()
    );

    // Non-relational child
    assert_eq!(
        QueryPlannerError::InvalidRelation,
        plan.add_select(&[const_row], gt_id).unwrap_err()
    );
}

#[test]
fn selection_serialize() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("selection.yaml");
    let s = fs::read_to_string(path).unwrap();
    Plan::from_yaml(&s).unwrap();
}

#[test]
fn union_all() {
    let mut plan = Plan::new();

    let t1 = Table::new_seg(
        "t1",
        vec![Column::new("a", Type::Number, ColumnRole::User)],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_seg(
        "t2",
        vec![Column::new("a", Type::Number, ColumnRole::User)],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();

    plan.add_union_all(scan_t2_id, scan_t1_id).unwrap();
}

#[test]
fn union_all_col_amount_mismatch() {
    let mut plan = Plan::new();

    let t1 = Table::new_seg(
        "t1",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Number, ColumnRole::User),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);

    let scan_t1_id = plan.add_scan("t1", None).unwrap();

    // Check errors for children with different amount of column
    let t2 = Table::new_seg(
        "t2",
        vec![Column::new("b", Type::Number, ColumnRole::User)],
        &["b"],
    )
    .unwrap();
    plan.add_rel(t2);

    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    assert_eq!(
        QueryPlannerError::NotEqualRows,
        plan.add_union_all(scan_t2_id, scan_t1_id).unwrap_err()
    );
}

#[test]
fn sub_query() {
    let mut plan = Plan::new();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Number, ColumnRole::User),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();
    plan.add_sub_query(scan_id, Some("sq")).unwrap();

    // Non-relational child node
    let a = 1;
    assert_eq!(
        QueryPlannerError::CustomError("Node isn't relational".into()),
        plan.add_sub_query(a, Some("sq")).unwrap_err()
    );

    // Invalid name
    assert_eq!(
        QueryPlannerError::InvalidName,
        plan.add_sub_query(scan_id, Some("")).unwrap_err()
    );
}

#[test]
fn sub_query_serialize() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("sub_query.yaml");
    let s = fs::read_to_string(path).unwrap();
    Plan::from_yaml(&s).unwrap();
}

#[test]
fn selection_with_sub_query() {
    // t1(a int) key [a]
    // t2(b int) key [b]
    // select * from t1 where a = (select b from t2)

    let mut plan = Plan::new();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_seg(
        "t1",
        vec![Column::new("a", Type::Integer, ColumnRole::User)],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_seg(
        "t2",
        vec![Column::new("b", Type::Integer, ColumnRole::User)],
        &["b"],
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["b"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let b_id = plan
        .add_row_from_sub_query(&children[..], children.len() - 1, &["b"])
        .unwrap();
    let a_id = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let eq_id = plan.add_cond(a_id, Bool::Eq, b_id).unwrap();

    let select_id = plan.add_select(&children[..], eq_id).unwrap();
    plan.set_top(select_id).unwrap();

    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("selection_with_sub_query.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();

    assert_eq!(expected_plan, plan);
}

#[test]
fn join() {
    // t1(a, b), t2(c, d)
    // select * from t1 join t2 on a = d;
    //
    // Treat a = d as (a) = (d),
    // i.e. (a), (d) - tuples containing a single column.
    let mut plan = Plan::new();

    let t1 = Table::new_seg(
        "t1",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Number, ColumnRole::User),
            Column::new("bucket_id", Type::Number, ColumnRole::Sharding),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1 = plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_seg(
        "t2",
        vec![
            Column::new("c", Type::Boolean, ColumnRole::User),
            Column::new("d", Type::Number, ColumnRole::User),
            Column::new("bucket_id", Type::Number, ColumnRole::Sharding),
        ],
        &["d"],
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2 = plan.add_scan("t2", None).unwrap();

    let a_row = plan
        .add_row_from_left_branch(scan_t1, scan_t2, &["a"])
        .unwrap();
    let d_row = plan
        .add_row_from_right_branch(scan_t1, scan_t2, &["d"])
        .unwrap();
    let condition = plan.nodes.add_bool(a_row, Bool::Eq, d_row).unwrap();
    let join = plan.add_join(scan_t1, scan_t2, condition).unwrap();
    plan.top = Some(join);
}

#[test]
fn join_serialize() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("join.yaml");
    let s = fs::read_to_string(path).unwrap();
    Plan::from_yaml(&s).unwrap();
}

#[test]
fn join_duplicate_columns() {
    // t1(a, b), t2(a, d)
    // select * from t1 join t2 on t1.a = t2.d
    let mut plan = Plan::new();

    let t1 = Table::new_seg(
        "t1",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Number, ColumnRole::User),
            Column::new("bucket_id", Type::Number, ColumnRole::Sharding),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1 = plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_seg(
        "t2",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("d", Type::Number, ColumnRole::User),
            Column::new("bucket_id", Type::Number, ColumnRole::Sharding),
        ],
        &["d"],
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2 = plan.add_scan("t2", None).unwrap();

    let a_row = plan
        .add_row_from_left_branch(scan_t1, scan_t2, &["a"])
        .unwrap();
    let d_row = plan
        .add_row_from_right_branch(scan_t1, scan_t2, &["d"])
        .unwrap();
    let condition = plan.nodes.add_bool(a_row, Bool::Eq, d_row).unwrap();
    assert_eq!(
        QueryPlannerError::CustomError(
            "Row can't be added because `a` already has an alias".into()
        ),
        plan.add_join(scan_t1, scan_t2, condition).unwrap_err()
    );
}
