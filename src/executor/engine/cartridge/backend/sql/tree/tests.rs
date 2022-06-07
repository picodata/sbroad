use std::fs;
use std::path::Path;

use pretty_assertions::assert_eq;

use crate::ir::operator::Bool;
use crate::ir::relation::{Column, ColumnRole, Table, Type};
use crate::ir::value::Value;
use crate::ir::Plan;

use super::*;

#[test]
fn sql_order_selection() {
    // select a from t where a = 1

    let mut plan = Plan::new();
    let t = Table::new_seg(
        "t",
        vec![Column::new("a", Type::Boolean, ColumnRole::User)],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t", None).unwrap();

    let a_id = plan.add_row_from_child(scan_id, &["a"]).unwrap();
    let const_1 = plan.nodes.add_const(Value::from(1_u64));
    let const_row = plan.nodes.add_row(vec![const_1], None);
    let eq_id = plan.nodes.add_bool(a_id, Bool::Eq, const_row).unwrap();
    let select_id = plan.add_select(&[scan_id], eq_id).unwrap();

    let proj_id = plan.add_proj(select_id, &["a"]).unwrap();
    plan.set_top(proj_id).unwrap();

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
    let exec_plan = ExecutionPlan::from(plan.clone());
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    // test the syntax plan
    let sp = SyntaxPlan::new(&exec_plan, top_id).unwrap();
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

    let exec_plan = ExecutionPlan::from(plan);
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    // get nodes in the sql-convenient order
    let nodes = exec_plan.get_sql_order(top_id).unwrap();
    let mut nodes_iter = nodes.into_iter();
    assert_eq!(Some(SyntaxData::PlanId(16)), nodes_iter.next()); // projection
    assert_eq!(Some(SyntaxData::PlanId(13)), nodes_iter.next()); // ref
    assert_eq!(Some(SyntaxData::PlanId(14)), nodes_iter.next()); // alias
    assert_eq!(Some(SyntaxData::Alias("a".into())), nodes_iter.next()); // name a
    assert_eq!(Some(SyntaxData::From), nodes_iter.next()); // from
    assert_eq!(Some(SyntaxData::PlanId(3)), nodes_iter.next()); // scan
    assert_eq!(Some(SyntaxData::PlanId(12)), nodes_iter.next()); // selection
    assert_eq!(Some(SyntaxData::PlanId(5)), nodes_iter.next()); // row
    assert_eq!(Some(SyntaxData::OpenParenthesis), nodes_iter.next()); // (
    assert_eq!(Some(SyntaxData::PlanId(4)), nodes_iter.next()); // ref a
    assert_eq!(Some(SyntaxData::CloseParenthesis), nodes_iter.next()); // )
    assert_eq!(Some(SyntaxData::PlanId(8)), nodes_iter.next()); // bool
    assert_eq!(Some(SyntaxData::Operator("=".into())), nodes_iter.next()); // =
    assert_eq!(Some(SyntaxData::PlanId(7)), nodes_iter.next()); // row
    assert_eq!(Some(SyntaxData::OpenParenthesis), nodes_iter.next()); // (
    assert_eq!(Some(SyntaxData::PlanId(6)), nodes_iter.next()); // const 1
    assert_eq!(Some(SyntaxData::CloseParenthesis), nodes_iter.next()); // )
    assert_eq!(None, nodes_iter.next());
}
