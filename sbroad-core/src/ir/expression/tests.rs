use crate::ir::operator::Arithmetic;
use crate::ir::tests::{column_integer_user_non_null, sharding_column};
use pretty_assertions::assert_eq;

use crate::ir::relation::{Column, SpaceEngine, Table, Type};
use crate::ir::value::Value;
use crate::ir::{Plan, SbroadError};

#[test]
fn row_duplicate_column_names() {
    let mut plan = Plan::default();

    let c1 = plan.nodes.add_const(Value::from(1_i64));
    let c1_alias_a = plan.nodes.add_alias("a", c1).unwrap();
    let c2 = plan.nodes.add_const(Value::from(2_u64));
    let c2_alias_a = plan.nodes.add_alias("a", c2).unwrap();
    plan.nodes
        .add_row_of_aliases(vec![c1_alias_a, c2_alias_a], None)
        .unwrap();
}

#[test]
fn rel_nodes_from_reference_in_scan() {
    // t(a int) [a]
    // select * from t
    let mut plan = Plan::default();

    let t = Table::new_sharded(
        "t",
        vec![column_integer_user_non_null(String::from("a"))],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t", None).unwrap();
    let output = plan.get_relational_output(scan_id).unwrap();

    let rel_set = plan.get_relational_nodes_from_row(output).unwrap();
    assert_eq!(true, rel_set.is_empty());
}

#[test]
fn rel_nodes_from_reference_in_proj() {
    // t(a int) [a]
    // select a from t
    let mut plan = Plan::default();

    let t = Table::new_sharded(
        "t",
        vec![column_integer_user_non_null(String::from("a"))],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t", None).unwrap();
    let proj_id = plan.add_proj(scan_id, &["a"], false).unwrap();
    let output = plan.get_relational_output(proj_id).unwrap();

    let rel_set = plan.get_relational_nodes_from_row(output).unwrap();
    assert_eq!(1, rel_set.len());
    assert_eq!(Some(&scan_id), rel_set.get(&scan_id));
}

#[test]
fn derive_expr_type() {
    fn column(name: String, ty: Type) -> Column {
        Column {
            name,
            r#type: ty,
            role: Default::default(),
            is_nullable: false,
        }
    }

    let mut plan = Plan::default();
    let t = Table::new_sharded(
        "t",
        vec![
            column(String::from("a"), Type::Integer),
            column(String::from("b"), Type::Integer),
            column(String::from("c"), Type::Unsigned),
            column(String::from("d"), Type::Decimal),
            column(String::from("e"), Type::Decimal),
            column(String::from("f"), Type::Double),
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
    let e_id = plan.add_row_from_child(scan_id, &["e"]).unwrap();
    let f_id = plan.add_row_from_child(scan_id, &["f"]).unwrap();

    // b/c
    let arith_divide_id = plan
        .add_arithmetic_to_plan(b_id, Arithmetic::Divide, c_id, false)
        .unwrap();
    let expr = plan.get_expression_node(arith_divide_id).unwrap();
    assert_eq!(expr.calculate_type(&plan).unwrap(), Type::Integer);

    // d*e
    let arith_multiply_id = plan
        .add_arithmetic_to_plan(d_id, Arithmetic::Multiply, e_id, false)
        .unwrap();
    let expr = plan.get_expression_node(arith_multiply_id).unwrap();
    assert_eq!(expr.calculate_type(&plan).unwrap(), Type::Decimal);

    // (b/c + d*e)
    let arith_addition_id = plan
        .add_arithmetic_to_plan(arith_divide_id, Arithmetic::Add, arith_multiply_id, true)
        .unwrap();
    let expr = plan.get_expression_node(arith_addition_id).unwrap();
    assert_eq!(expr.calculate_type(&plan).unwrap(), Type::Decimal);

    // (b/c + d*e) * f
    let arith_multiply_id2 = plan
        .add_arithmetic_to_plan(arith_addition_id, Arithmetic::Multiply, f_id, false)
        .unwrap();
    let expr = plan.get_expression_node(arith_multiply_id2).unwrap();
    assert_eq!(expr.calculate_type(&plan).unwrap(), Type::Double);

    // a + (b/c + d*e) * f
    let arith_addition_id2 = plan
        .add_arithmetic_to_plan(a_id, Arithmetic::Add, arith_multiply_id2, false)
        .unwrap();
    let expr = plan.get_expression_node(arith_addition_id2).unwrap();
    assert_eq!(expr.calculate_type(&plan).unwrap(), Type::Double);

    // a + (b/c + d*e) * f - b
    let arith_subract_id = plan
        .add_arithmetic_to_plan(arith_addition_id2, Arithmetic::Subtract, b_id, false)
        .unwrap();
    let expr = plan.get_expression_node(arith_subract_id).unwrap();
    assert_eq!(expr.calculate_type(&plan).unwrap(), Type::Double);
}
