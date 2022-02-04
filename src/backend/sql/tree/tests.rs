use super::*;
use crate::ir::operator::Bool;
use crate::ir::relation::{Column, Table, Type};
use crate::ir::value::Value;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn sql_order_selection() {
    // select a from t where a = 1

    let mut plan = Plan::new();
    let t = Table::new_seg("t", vec![Column::new("a", Type::Boolean)], &["a"]).unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t").unwrap();

    let logical_id = plan.nodes.next_id();
    let a_id = plan
        .add_row_from_child(logical_id, scan_id, &["a"])
        .unwrap();
    let const_1 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let const_row = plan.nodes.add_row(vec![const_1], None);
    let eq_id = plan.nodes.add_bool(a_id, Bool::Eq, const_row).unwrap();
    let select_id = plan.add_select(&[scan_id], eq_id, logical_id).unwrap();

    let proj_id = plan.add_proj(select_id, &["a"]).unwrap();
    plan.set_top(proj_id).unwrap();
    plan.build_relational_map();

    // check the plan
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("backend")
        .join("sql")
        .join("tree")
        .join("sql_order_selection.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(expected_plan, plan);

    // test the syntax plan
    let sp = SyntaxPlan::new(&plan, plan.get_top().unwrap()).unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("backend")
        .join("sql")
        .join("tree")
        .join("sql_order_selection_syntax_nodes.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_syntax_nodes = SyntaxNodes::from_yaml(&s).unwrap();
    assert_eq!(expected_syntax_nodes, sp.nodes);

    // get nodes in the sql-convenient order
    let nodes = plan.get_sql_order(plan.get_top().unwrap()).unwrap();
    let mut nodes_iter = nodes.into_iter();
    assert_eq!(Some(SyntaxData::PlanId(16)), nodes_iter.next()); // projection
    assert_eq!(Some(SyntaxData::PlanId(13)), nodes_iter.next()); // ref
    assert_eq!(Some(SyntaxData::PlanId(14)), nodes_iter.next()); // alias
    assert_eq!(Some(SyntaxData::Alias("a".into())), nodes_iter.next()); // name a
    assert_eq!(Some(SyntaxData::PlanId(3)), nodes_iter.next()); // scan
    assert_eq!(Some(SyntaxData::PlanId(12)), nodes_iter.next()); // selection
    assert_eq!(Some(SyntaxData::PlanId(5)), nodes_iter.next()); // row
    assert_eq!(Some(SyntaxData::OpenParenthesis), nodes_iter.next()); // (
    assert_eq!(Some(SyntaxData::PlanId(4)), nodes_iter.next()); // ref a
    assert_eq!(Some(SyntaxData::CloseParenthesis), nodes_iter.next()); // )
    assert_eq!(Some(SyntaxData::PlanId(8)), nodes_iter.next()); // eq
    assert_eq!(Some(SyntaxData::PlanId(7)), nodes_iter.next()); // row
    assert_eq!(Some(SyntaxData::OpenParenthesis), nodes_iter.next()); // (
    assert_eq!(Some(SyntaxData::PlanId(6)), nodes_iter.next()); // const 1
    assert_eq!(Some(SyntaxData::CloseParenthesis), nodes_iter.next()); // )
    assert_eq!(None, nodes_iter.next());
}
