use std::fs;
use std::path::Path;

use pretty_assertions::assert_eq;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::ir::operator::{Arithmetic, Bool, Unary};
use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table, Type};
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;
use crate::ir::Plan;

use super::*;

#[test]
fn sql_order_selection() {
    // select a from t where a = 1

    let mut plan = Plan::default();
    let t = Table::new_seg(
        "t",
        vec![Column::new("a", Type::Boolean, ColumnRole::User)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t", None).unwrap();

    let a_id = plan.add_row_from_child(scan_id, &["a"]).unwrap();
    let const_1 = plan.nodes.add_const(Value::from(1_u64));
    let const_row = plan.nodes.add_row(vec![const_1], None);
    let eq_id = plan.nodes.add_bool(a_id, Bool::Eq, const_row).unwrap();
    let select_id = plan.add_select(&[scan_id], eq_id).unwrap();

    let proj_id = plan.add_proj(select_id, &["a"], false).unwrap();
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
    let sp = SyntaxPlan::new(&exec_plan, top_id, Snapshot::Latest).unwrap();
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
    let sp = SyntaxPlan::new(&exec_plan, top_id, Snapshot::Latest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let mut nodes_iter = nodes.into_iter();
    assert_eq!(Some(&SyntaxData::PlanId(16)), nodes_iter.next()); // projection
    assert_eq!(Some(&SyntaxData::PlanId(14)), nodes_iter.next()); // alias
    assert_eq!(Some(&SyntaxData::PlanId(13)), nodes_iter.next()); // ref
    assert_eq!(Some(&SyntaxData::From), nodes_iter.next()); // from
    assert_eq!(Some(&SyntaxData::PlanId(3)), nodes_iter.next()); // scan
    assert_eq!(Some(&SyntaxData::PlanId(12)), nodes_iter.next()); // selection
    assert_eq!(Some(&SyntaxData::PlanId(5)), nodes_iter.next()); // row
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next()); // (
    assert_eq!(Some(&SyntaxData::PlanId(4)), nodes_iter.next()); // ref a
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next()); // )
    assert_eq!(Some(&SyntaxData::PlanId(8)), nodes_iter.next()); // bool
    assert_eq!(Some(&SyntaxData::Operator("=".into())), nodes_iter.next()); // =
    assert_eq!(Some(&SyntaxData::PlanId(7)), nodes_iter.next()); // row
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next()); // (
    assert_eq!(Some(&SyntaxData::Parameter(6)), nodes_iter.next()); // parameter
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next()); // )
    assert_eq!(None, nodes_iter.next());
}

#[test]
#[allow(clippy::too_many_lines)]
fn sql_arithmetic_selection_plan() {
    // select a from t where a + (b/c + d*e) * f - b = 1
    let mut plan = Plan::default();
    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Integer, ColumnRole::User),
            Column::new("b", Type::Integer, ColumnRole::User),
            Column::new("c", Type::Integer, ColumnRole::User),
            Column::new("d", Type::Integer, ColumnRole::User),
            Column::new("e", Type::Integer, ColumnRole::User),
            Column::new("f", Type::Integer, ColumnRole::User),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t", None).unwrap();
    let a_id = plan.add_row_from_child(scan_id, &["a"]).unwrap();
    let b_id = plan.add_row_from_child(scan_id, &["b"]).unwrap();
    let c_id = plan.add_row_from_child(scan_id, &["c"]).unwrap();
    let d_id = plan.add_row_from_child(scan_id, &["d"]).unwrap();
    let e_id = plan.add_row_from_child(scan_id, &["e"]).unwrap();
    let f_id = plan.add_row_from_child(scan_id, &["f"]).unwrap();
    let const_1 = plan.nodes.add_const(Value::from(1_u64));
    let const_row = plan.nodes.add_row(vec![const_1], None);

    // b/c
    let arith_divide_id = plan
        .add_arithmetic_to_plan(b_id, Arithmetic::Divide, c_id, false)
        .unwrap();
    // d*e
    let arith_multiply_id = plan
        .add_arithmetic_to_plan(d_id, Arithmetic::Multiply, e_id, false)
        .unwrap();
    // (b/c + d*e)
    let arith_addition_id = plan
        .add_arithmetic_to_plan(arith_divide_id, Arithmetic::Add, arith_multiply_id, true)
        .unwrap();
    // (b/c + d*e) * f
    let arith_multiply_id2 = plan
        .add_arithmetic_to_plan(arith_addition_id, Arithmetic::Multiply, f_id, false)
        .unwrap();
    // a + (b/c + d*e) * f
    let arith_addition_id2 = plan
        .add_arithmetic_to_plan(a_id, Arithmetic::Add, arith_multiply_id2, false)
        .unwrap();
    // a + (b/c + d*e) * f - b
    let arith_subract_id = plan
        .add_arithmetic_to_plan(arith_addition_id2, Arithmetic::Subtract, b_id, false)
        .unwrap();
    // a + (b/c + d*e) * f - b = 1
    let equal_id = plan
        .nodes
        .add_bool(arith_subract_id, Bool::Eq, const_row)
        .unwrap();
    // where a + (b/c + d*e) * f - b = 1
    let select_id = plan.add_select(&[scan_id], equal_id).unwrap();

    let proj_id = plan.add_proj(select_id, &["a"], false).unwrap();
    plan.set_top(proj_id).unwrap();

    // check the plan
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("backend")
        .join("sql")
        .join("tree")
        .join("arithmetic_selection_plan.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(expected_plan, plan);

    let exec_plan = ExecutionPlan::from(plan.clone());
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    // get nodes in the sql-convenient order
    let sp = SyntaxPlan::new(&exec_plan, top_id, Snapshot::Latest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let mut nodes_iter = nodes.into_iter();
    // projection
    assert_eq!(Some(&SyntaxData::PlanId(56)), nodes_iter.next());
    // alias
    assert_eq!(Some(&SyntaxData::PlanId(54)), nodes_iter.next());
    // ref
    assert_eq!(Some(&SyntaxData::PlanId(53)), nodes_iter.next());
    // from
    assert_eq!(Some(&SyntaxData::From), nodes_iter.next());
    // scan
    assert_eq!(Some(&SyntaxData::PlanId(15)), nodes_iter.next());
    // selection
    assert_eq!(Some(&SyntaxData::PlanId(52)), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(17)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref a
    assert_eq!(Some(&SyntaxData::PlanId(16)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression: [a] + [(b/c + d*e)]
    assert_eq!(Some(&SyntaxData::PlanId(34)), nodes_iter.next());
    // arithmetic operator add (+)
    assert_eq!(Some(&SyntaxData::Operator("+".into())), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // arithmetic expression add: ([b/c] + [d*e])
    assert_eq!(Some(&SyntaxData::PlanId(32)), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(19)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref b
    assert_eq!(Some(&SyntaxData::PlanId(18)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression divide: [b] / [c]
    assert_eq!(Some(&SyntaxData::PlanId(30)), nodes_iter.next());
    // arithmetic operator divide (/)
    assert_eq!(Some(&SyntaxData::Operator("/".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(21)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref c
    assert_eq!(Some(&SyntaxData::PlanId(20)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic operator add (+)
    assert_eq!(Some(&SyntaxData::Operator("+".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(23)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref d
    assert_eq!(Some(&SyntaxData::PlanId(22)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression multiply: [d] * [e]
    assert_eq!(Some(&SyntaxData::PlanId(31)), nodes_iter.next());
    // arithmetic operator multiply (*)
    assert_eq!(Some(&SyntaxData::Operator("*".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(25)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref e
    assert_eq!(Some(&SyntaxData::PlanId(24)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression multiply: [(b/c + d*e)] * [f]
    assert_eq!(Some(&SyntaxData::PlanId(33)), nodes_iter.next());
    // arithmetic operator multiply (*)
    assert_eq!(Some(&SyntaxData::Operator("*".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(27)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref f
    assert_eq!(Some(&SyntaxData::PlanId(26)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression subtract: [a + (b/c + d*e) * f] - [b]
    assert_eq!(Some(&SyntaxData::PlanId(35)), nodes_iter.next());
    // arithmetic operator subtract (-)
    assert_eq!(Some(&SyntaxData::Operator("-".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(19)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref b
    assert_eq!(Some(&SyntaxData::PlanId(18)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // bool expression eq: [a + (b/c + d*e) * f - b] = [1]
    assert_eq!(Some(&SyntaxData::PlanId(36)), nodes_iter.next());
    // bool operator eq (=)
    assert_eq!(Some(&SyntaxData::Operator("=".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(29)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // parameter
    assert_eq!(Some(&SyntaxData::Parameter(28)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    assert_eq!(None, nodes_iter.next());
}

#[test]
fn sql_arithmetic_projection_plan() {
    // select a + (b/c + d*e) * f - b from t
    let mut plan = Plan::default();
    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Integer, ColumnRole::User),
            Column::new("b", Type::Integer, ColumnRole::User),
            Column::new("c", Type::Integer, ColumnRole::User),
            Column::new("d", Type::Integer, ColumnRole::User),
            Column::new("e", Type::Integer, ColumnRole::User),
            Column::new("f", Type::Integer, ColumnRole::User),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t", None).unwrap();
    let a_id = plan.add_row_from_child(scan_id, &["a"]).unwrap();
    let b_id = plan.add_row_from_child(scan_id, &["b"]).unwrap();
    let c_id = plan.add_row_from_child(scan_id, &["c"]).unwrap();
    let d_id = plan.add_row_from_child(scan_id, &["d"]).unwrap();
    let e_id = plan.add_row_from_child(scan_id, &["e"]).unwrap();
    let f_id = plan.add_row_from_child(scan_id, &["f"]).unwrap();

    // b/c
    let arith_divide_id = plan
        .add_arithmetic_to_plan(b_id, Arithmetic::Divide, c_id, false)
        .unwrap();
    // d*e
    let arith_multiply_id = plan
        .add_arithmetic_to_plan(d_id, Arithmetic::Multiply, e_id, false)
        .unwrap();
    // (b/c + d*e)
    let arith_addition_id = plan
        .add_arithmetic_to_plan(arith_divide_id, Arithmetic::Add, arith_multiply_id, true)
        .unwrap();
    // (b/c + d*e) * f
    let arith_multiply_id2 = plan
        .add_arithmetic_to_plan(arith_addition_id, Arithmetic::Multiply, f_id, false)
        .unwrap();
    // a + (b/c + d*e) * f
    let arith_addition_id2 = plan
        .add_arithmetic_to_plan(a_id, Arithmetic::Add, arith_multiply_id2, false)
        .unwrap();
    // a + (b/c + d*e) * f - b
    let arith_subract_id = plan
        .add_arithmetic_to_plan(arith_addition_id2, Arithmetic::Subtract, b_id, false)
        .unwrap();

    let proj_id = plan
        .add_proj_internal(scan_id, &[arith_subract_id], false)
        .unwrap();
    plan.set_top(proj_id).unwrap();

    // check the plan
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("backend")
        .join("sql")
        .join("tree")
        .join("arithmetic_projection_plan.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(expected_plan, plan);

    let exec_plan = ExecutionPlan::from(plan.clone());
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    // get nodes in the sql-convenient order
    let sp = SyntaxPlan::new(&exec_plan, top_id, Snapshot::Latest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let mut nodes_iter = nodes.into_iter();

    // projection
    assert_eq!(Some(&SyntaxData::PlanId(35)), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(17)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref a
    assert_eq!(Some(&SyntaxData::PlanId(16)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression: [a] + [(b/c + d*e)]
    assert_eq!(Some(&SyntaxData::PlanId(32)), nodes_iter.next());
    // arithmetic operator add (+)
    assert_eq!(Some(&SyntaxData::Operator("+".into())), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // arithmetic expression add: ([b/c] + [d*e])
    assert_eq!(Some(&SyntaxData::PlanId(30)), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(19)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref b
    assert_eq!(Some(&SyntaxData::PlanId(18)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression divide: [b] / [c]
    assert_eq!(Some(&SyntaxData::PlanId(28)), nodes_iter.next());
    // arithmetic operator divide (/)
    assert_eq!(Some(&SyntaxData::Operator("/".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(21)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref c
    assert_eq!(Some(&SyntaxData::PlanId(20)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic operator add (+)
    assert_eq!(Some(&SyntaxData::Operator("+".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(23)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref d
    assert_eq!(Some(&SyntaxData::PlanId(22)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression multiply: [d] * [e]
    assert_eq!(Some(&SyntaxData::PlanId(29)), nodes_iter.next());
    // arithmetic operator multiply (*)
    assert_eq!(Some(&SyntaxData::Operator("*".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(25)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref e
    assert_eq!(Some(&SyntaxData::PlanId(24)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression multiply: [(b/c + d*e)] * f
    assert_eq!(Some(&SyntaxData::PlanId(31)), nodes_iter.next());
    // arithmetic operator multiply (*)
    assert_eq!(Some(&SyntaxData::Operator("*".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(27)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // f
    assert_eq!(Some(&SyntaxData::PlanId(26)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression subtract: [a + (b/c + d*e) * f] - [b]
    assert_eq!(Some(&SyntaxData::PlanId(33)), nodes_iter.next());
    // arithmetic operator subtract (-)
    assert_eq!(Some(&SyntaxData::Operator("-".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(19)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref b
    assert_eq!(Some(&SyntaxData::PlanId(18)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // from
    assert_eq!(Some(&SyntaxData::From), nodes_iter.next());
    // scan
    assert_eq!(Some(&SyntaxData::PlanId(15)), nodes_iter.next());

    assert_eq!(None, nodes_iter.next());
}

#[test]
fn sql_arbitrary_projection_plan() {
    // select a + b > c and d is not null from t
    let mut plan = Plan::default();
    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Integer, ColumnRole::User),
            Column::new("b", Type::Integer, ColumnRole::User),
            Column::new("c", Type::Integer, ColumnRole::User),
            Column::new("d", Type::Integer, ColumnRole::User),
            Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t", None).unwrap();
    let a_id = plan.add_row_from_child(scan_id, &["a"]).unwrap();
    let b_id = plan.add_row_from_child(scan_id, &["b"]).unwrap();
    let c_id = plan.add_row_from_child(scan_id, &["c"]).unwrap();
    let d_id = plan.add_row_from_child(scan_id, &["d"]).unwrap();

    // a + b
    let arith_addition_id = plan
        .add_arithmetic_to_plan(a_id, Arithmetic::Add, b_id, false)
        .unwrap();

    // a + b > c
    let gt_id = plan
        .nodes
        .add_bool(arith_addition_id, Bool::Gt, c_id)
        .unwrap();

    // d is not null
    let unary_id = plan.nodes.add_unary_bool(Unary::IsNotNull, d_id).unwrap();

    // a + b > c and d is not null
    let and_id = plan.nodes.add_bool(gt_id, Bool::And, unary_id).unwrap();

    let proj_id = plan.add_proj_internal(scan_id, &[and_id], false).unwrap();
    plan.set_top(proj_id).unwrap();

    // check the plan
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("backend")
        .join("sql")
        .join("tree")
        .join("arbitrary_projection_plan.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(expected_plan, plan);

    let exec_plan = ExecutionPlan::from(plan.clone());
    let top_id = exec_plan.get_ir_plan().get_top().unwrap();

    // get nodes in the sql-convenient order
    let sp = SyntaxPlan::new(&exec_plan, top_id, Snapshot::Latest).unwrap();
    let ordered = OrderedSyntaxNodes::try_from(sp).unwrap();
    let nodes = ordered.to_syntax_data().unwrap();
    let mut nodes_iter = nodes.into_iter();

    // projection
    assert_eq!(Some(&SyntaxData::PlanId(25)), nodes_iter.next());
    // row 12
    assert_eq!(Some(&SyntaxData::PlanId(13)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref a
    assert_eq!(Some(&SyntaxData::PlanId(12)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // arithmetic expression: [a] + [b]
    assert_eq!(Some(&SyntaxData::PlanId(20)), nodes_iter.next());
    // arithmetic operator Add (+)
    assert_eq!(Some(&SyntaxData::Operator("+".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(15)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref b
    assert_eq!(Some(&SyntaxData::PlanId(14)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // bool expression gt: [a + b] > [c]
    assert_eq!(Some(&SyntaxData::PlanId(21)), nodes_iter.next());
    // bool operator Gt (>)
    assert_eq!(Some(&SyntaxData::Operator(">".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(17)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref c
    assert_eq!(Some(&SyntaxData::PlanId(16)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // bool expression and: [a + b > c] and [d is not null]
    assert_eq!(Some(&SyntaxData::PlanId(23)), nodes_iter.next());
    // bool operator And (and)
    assert_eq!(Some(&SyntaxData::Operator("and".into())), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(19)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref d
    assert_eq!(Some(&SyntaxData::PlanId(18)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // unary expression is not null: [d] is not null 19
    assert_eq!(Some(&SyntaxData::PlanId(22)), nodes_iter.next());
    // unary operator IsNotNull (is not null)
    assert_eq!(
        Some(&SyntaxData::Operator("is not null".into())),
        nodes_iter.next()
    );
    // from
    assert_eq!(Some(&SyntaxData::From), nodes_iter.next());
    // scan
    assert_eq!(Some(&SyntaxData::PlanId(11)), nodes_iter.next());

    assert_eq!(None, nodes_iter.next());
}
