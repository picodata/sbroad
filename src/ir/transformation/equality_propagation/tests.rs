use super::*;
use crate::errors::QueryPlannerError;
use crate::ir::relation::*;
use crate::ir::value::*;
use crate::ir::*;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn selection() {
    // select * from t
    // where (a) = 1 and (b) = 2 and (c) = 1 or (d) = 1
    // ->
    // select * from t
    // where (a) = 1 and (b) = 2 and (c) = (a) and (c) = 1 or (d) = 1

    let mut plan = Plan::new();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
            Column::new("c", Type::String),
            Column::new("d", Type::String),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t").unwrap();

    // a = 1
    let a = plan.add_row_from_child(scan_id, &["a"]).unwrap();
    let const_a1 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let a1 = plan.nodes.add_bool(a, Bool::Eq, const_a1).unwrap();
    // b = 2
    let b = plan.add_row_from_child(scan_id, &["b"]).unwrap();
    let const_b2 = plan.nodes.add_const(Value::number_from_str("2").unwrap());
    let b2 = plan.nodes.add_bool(b, Bool::Eq, const_b2).unwrap();
    // a = 1 and b = 2
    let a1b2 = plan.nodes.add_bool(a1, Bool::And, b2).unwrap();
    // c = 1
    let c = plan.add_row_from_child(scan_id, &["c"]).unwrap();
    let const_c1 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let c1 = plan.nodes.add_bool(c, Bool::Eq, const_c1).unwrap();
    // a = 1 and b = 2 and c = 1
    let a1b2c1 = plan.nodes.add_bool(a1b2, Bool::And, c1).unwrap();
    // d = 1
    let d = plan.add_row_from_child(scan_id, &["d"]).unwrap();
    let const_d1 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let d1 = plan.nodes.add_bool(d, Bool::Eq, const_d1).unwrap();
    // a = 1 and b = 2 and c = 1 or d = 1
    let a1b2c1_d1 = plan.nodes.add_bool(a1b2c1, Bool::Or, d1).unwrap();

    let select_id = plan.add_select(&[scan_id], a1b2c1_d1).unwrap();
    plan.set_top(select_id).unwrap();

    let candidates = plan.nodes.gather_expr_for_eq_propagation().unwrap();
    assert_eq!(candidates, vec![a1b2c1_d1]);

    // Expect {a1b2c1: [{a, c}, {b}]}
    // We don't have equivalence class (EC) chain [{d}] as it is a single
    // equivalence that is not under "AND" node. There is no sense to build
    // EC from a single equivalence.
    let suggestions = plan.nodes.eq_class_suggestions().unwrap();

    let mut expected: HashMap<usize, EqClassChain> = HashMap::new();
    let a_ec_ref: EqClassRef = if let Node::Expression(a_expr) = plan.get_node(a).unwrap() {
        EqClassRef::from_single_col_row(a_expr, &plan.nodes)
    } else {
        Err(QueryPlannerError::InvalidNode)
    }
    .unwrap();
    let c_ec_ref: EqClassRef = if let Node::Expression(c_expr) = plan.get_node(c).unwrap() {
        EqClassRef::from_single_col_row(c_expr, &plan.nodes)
    } else {
        Err(QueryPlannerError::InvalidNode)
    }
    .unwrap();
    let mut ec_ac = EqClass::new();
    ec_ac.set.insert(EqClassExpr::EqClassRef(a_ec_ref));
    ec_ac.set.insert(EqClassExpr::EqClassRef(c_ec_ref));

    let b_ec_ref: EqClassRef = if let Node::Expression(b_expr) = plan.get_node(b).unwrap() {
        EqClassRef::from_single_col_row(b_expr, &plan.nodes)
    } else {
        Err(QueryPlannerError::InvalidNode)
    }
    .unwrap();
    let mut ec_b = EqClass::new();
    ec_b.set.insert(EqClassExpr::EqClassRef(b_ec_ref));

    let mut chain = EqClassChain::new();
    chain.list.push(ec_ac);
    chain.list.push(ec_b);

    expected.insert(a1b2c1, chain);
    assert_eq!(suggestions, expected);

    // Check the modified plan
    plan.nodes.add_new_equalities().unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("transformation")
        .join("equality_propagation")
        .join("selection.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(plan, expected_plan);
}
