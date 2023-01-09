use super::*;
use crate::ir::relation::{Column, ColumnRole, Table, Type};
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
        SbroadError::Invalid(Entity::Plan, Some("plan tree top is None".into())),
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
        SbroadError::NotFound(Entity::Node, "from arena with index 42".into()),
        Plan::from_yaml(&s).unwrap_err()
    );
}

#[test]
fn get_node() {
    let mut plan = Plan::default();

    let t = Table::new_seg(
        "t",
        vec![Column::new("a", Type::Boolean, ColumnRole::User)],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();

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
    let plan = Plan::default();
    assert_eq!(
        SbroadError::NotFound(Entity::Node, "from arena with index 42".into()),
        plan.get_node(42).unwrap_err()
    );
}

//TODO: add relation test
