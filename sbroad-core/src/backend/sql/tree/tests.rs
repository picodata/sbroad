use std::fs;
use std::path::Path;

use pretty_assertions::assert_eq;
use smol_str::ToSmolStr;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::ir::operator::{Arithmetic, Bool, Unary};
use crate::ir::relation::{SpaceEngine, Table, Type};
use crate::ir::tests::sharding_column;
use crate::ir::tree::Snapshot;
use crate::ir::value::Value;
use crate::ir::Plan;

use super::*;
use crate::ir::tests::{column_integer_user_non_null, column_user_non_null};

#[test]
fn sql_order_selection() {
    // select a from t where a = 1

    let mut plan = Plan::default();
    let t = Table::new_sharded(
        "t",
        vec![column_user_non_null(SmolStr::from("a"), Type::Boolean)],
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

    let proj_id = plan.add_proj(select_id, &["a"], false, false).unwrap();
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
fn sql_arbitrary_projection_plan() {
    // select a + b > c and d is not null from t
    let mut plan = Plan::default();
    let t = Table::new_sharded(
        "t",
        vec![
            column_integer_user_non_null(SmolStr::from("a")),
            column_integer_user_non_null(SmolStr::from("b")),
            column_integer_user_non_null(SmolStr::from("c")),
            column_integer_user_non_null(SmolStr::from("d")),
            sharding_column(),
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
        .add_arithmetic_to_plan(a_id, Arithmetic::Add, b_id)
        .unwrap();

    // a + b > c
    let gt_id = plan
        .nodes
        .add_bool(arith_addition_id, Bool::Gt, c_id)
        .unwrap();

    // d is not null
    let is_null_id = plan.nodes.add_unary_bool(Unary::IsNull, d_id).unwrap();
    let not_null_id = plan.nodes.add_unary_bool(Unary::Not, is_null_id).unwrap();

    // a + b > c and d is not null
    let and_id = plan.nodes.add_bool(gt_id, Bool::And, not_null_id).unwrap();
    let alias_id = plan.nodes.add_alias("COL_1", and_id).unwrap();

    let proj_id = plan.add_proj_internal(scan_id, &[alias_id], false).unwrap();
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
    assert_eq!(Some(&SyntaxData::PlanId(27)), nodes_iter.next());
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
    assert_eq!(Some(&SyntaxData::PlanId(24)), nodes_iter.next());
    // bool operator And (and)
    assert_eq!(Some(&SyntaxData::Operator("and".into())), nodes_iter.next());
    // unary operator Not (not)
    assert_eq!(Some(&SyntaxData::Operator("not".into())), nodes_iter.next());
    // unary expression not is null: not [d] is null 19
    assert_eq!(Some(&SyntaxData::PlanId(23)), nodes_iter.next());
    // row
    assert_eq!(Some(&SyntaxData::PlanId(19)), nodes_iter.next());
    // (
    assert_eq!(Some(&SyntaxData::OpenParenthesis), nodes_iter.next());
    // ref d
    assert_eq!(Some(&SyntaxData::PlanId(18)), nodes_iter.next());
    // )
    assert_eq!(Some(&SyntaxData::CloseParenthesis), nodes_iter.next());
    // unary expression is null: [d] is null
    assert_eq!(Some(&SyntaxData::PlanId(22)), nodes_iter.next());
    // unary operator IsNull (is null)
    assert_eq!(
        Some(&SyntaxData::Operator("is null".into())),
        nodes_iter.next()
    );
    // alias
    assert_eq!(Some(&SyntaxData::PlanId(25)), nodes_iter.next());
    assert_eq!(
        Some(&SyntaxData::Alias("COL_1".to_smolstr())),
        nodes_iter.next()
    );
    // from
    assert_eq!(Some(&SyntaxData::From), nodes_iter.next());
    // scan
    assert_eq!(Some(&SyntaxData::PlanId(11)), nodes_iter.next());

    assert_eq!(None, nodes_iter.next());
}
