use super::*;
use crate::errors::QueryPlannerError;
use crate::ir::distribution::*;
use crate::ir::relation::*;
use crate::ir::value::*;
use crate::ir::*;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn scan_rel() {
    let mut plan = Plan::empty();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
            Column::new("c", Type::String),
            Column::new("d", Type::String),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_output = 8;
    let scan_node = 9;

    let scan_id = plan.add_scan("t").unwrap();
    assert_eq!(scan_node, scan_id);
    plan.top = Some(scan_node);

    let map = plan.relational_id_map();

    plan.set_distribution(scan_output, &map).unwrap();
    if let Node::Expression(row) = plan.get_node(scan_output).unwrap() {
        assert_eq!(
            row.distribution().unwrap(),
            &Distribution::Segment { key: vec![1, 0] }
        );
    } else {
        panic!("Wrong output node type!");
    }
}

#[test]
fn scan_rel_serialized() {
    let mut plan = Plan::empty();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
            Column::new("c", Type::String),
            Column::new("d", Type::String),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t").unwrap();
    plan.top = Some(scan_id);

    let scan_output = scan_id - 1;

    let map = plan.relational_id_map();
    plan.set_distribution(scan_output, &map).unwrap();

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
    let mut plan = Plan::empty();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
            Column::new("c", Type::String),
            Column::new("d", Type::String),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t").unwrap();

    // Invalid alias names in the output
    assert_eq!(
        QueryPlannerError::InvalidRow,
        plan.add_proj(scan_id, &["a", "e"]).unwrap_err()
    );

    // Expression node instead of relational one
    assert_eq!(
        QueryPlannerError::InvalidNode,
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
    let mut plan = Plan::empty();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
            Column::new("c", Type::String),
            Column::new("d", Type::String),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t").unwrap();

    let ref_a_id = plan.nodes.add_ref(scan_id + 1, Some(vec![0]), 0);
    let a_id = plan.nodes.add_alias("a", ref_a_id).unwrap();
    let const_id = plan.nodes.add_const(Value::number_from_str("10").unwrap());
    let gt_id = plan.nodes.add_bool(a_id, Bool::Gt, const_id).unwrap();

    // Correct Selection operator
    Relational::new_select(&mut plan, scan_id, gt_id).unwrap();

    // Non-boolean filter
    assert_eq!(
        QueryPlannerError::InvalidBool,
        Relational::new_select(&mut plan, scan_id, const_id).unwrap_err()
    );

    // Non-relational child
    assert_eq!(
        QueryPlannerError::InvalidNode,
        Relational::new_select(&mut plan, const_id, gt_id).unwrap_err()
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
fn union_all_col_amount_mismatch() {
    let mut plan = Plan::empty();

    let t1 = Table::new_seg(
        "t1",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);

    let scan_t1_id = plan.add_scan("t1").unwrap();

    // Check errors for children with different amount of column
    let t2 = Table::new_seg("t2", vec![Column::new("b", Type::Number)], &["b"]).unwrap();
    plan.add_rel(t2);

    let scan_t2_id = plan.add_scan("t2").unwrap();
    assert_eq!(
        QueryPlannerError::NotEqualRows,
        Relational::new_union_all(&mut plan, scan_t2_id, scan_t1_id).unwrap_err()
    );
}

#[test]
fn sub_query() {
    let mut plan = Plan::empty();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t").unwrap();

    Relational::new_sub_query(&mut plan, scan_id, "sq").unwrap();

    // Non-relational child node
    let a = 1;
    assert_eq!(
        QueryPlannerError::InvalidNode,
        Relational::new_sub_query(&mut plan, a, "sq").unwrap_err()
    );

    // Invalid name
    assert_eq!(
        QueryPlannerError::InvalidName,
        Relational::new_sub_query(&mut plan, scan_id, "").unwrap_err()
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
