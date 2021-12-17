use super::*;
use crate::ir::relation::*;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn plan_no_top() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("plan_no_top.yaml");
    let s = fs::read_to_string(path).unwrap();
    assert_eq!(
        QueryPlannerError::InvalidPlan,
        Plan::from_yaml(&s).unwrap_err()
    );
}

#[test]
fn plan_oor_top() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("plan_oor_top.yaml");
    let s = fs::read_to_string(path).unwrap();
    assert_eq!(
        QueryPlannerError::ValueOutOfRange,
        Plan::from_yaml(&s).unwrap_err()
    );
}

#[test]
fn get_node() {
    let mut plan = Plan::empty();

    let t = Table::new_seg("t", vec![Column::new("a", Type::Boolean)], &["a"]).unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t").unwrap();

    if let Node::Relational(Relational::ScanRelation { relation, .. }) =
        plan.get_node(scan_id).unwrap()
    {
        assert_eq!(relation, "t");
    } else {
        panic!("Unexpected node returned!")
    }
}

#[test]
fn get_node_oor() {
    let plan = Plan::empty();
    assert_eq!(
        QueryPlannerError::ValueOutOfRange,
        plan.get_node(42).unwrap_err()
    );
}

//TODO: add relation test
