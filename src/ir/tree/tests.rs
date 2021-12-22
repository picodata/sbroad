use crate::ir::operator::*;
use crate::ir::value::*;
use crate::ir::*;
use pretty_assertions::assert_eq;
use traversal::Bft;

#[test]
fn expression_bft() {
    // ((c1 = c2) and (c2 = c3)) or (c4 = c5)

    let mut plan = Plan::new();
    let c1 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let c2 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let c3 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let c4 = plan.nodes.add_const(Value::number_from_str("2").unwrap());
    let c5 = plan.nodes.add_const(Value::number_from_str("3").unwrap());

    let c1_eq_c2 = plan.nodes.add_bool(c1, Bool::Eq, c2).unwrap();
    let c2_eq_c3 = plan.nodes.add_bool(c2, Bool::Eq, c3).unwrap();
    let c1c2_and_c2c3 = plan.nodes.add_bool(c1_eq_c2, Bool::And, c2_eq_c3).unwrap();
    let c4_eq_c5 = plan.nodes.add_bool(c4, Bool::Eq, c5).unwrap();
    let top = plan
        .nodes
        .add_bool(c1c2_and_c2c3, Bool::Or, c4_eq_c5)
        .unwrap();

    let mut btf_tree = Bft::new(&top, |node| plan.nodes.expr_iter(node));
    assert_eq!(btf_tree.next(), Some((0, &top)));
    assert_eq!(btf_tree.next(), Some((1, &c1c2_and_c2c3)));
    assert_eq!(btf_tree.next(), Some((1, &c4_eq_c5)));
    assert_eq!(btf_tree.next(), Some((2, &c1_eq_c2)));
    assert_eq!(btf_tree.next(), Some((2, &c2_eq_c3)));
    assert_eq!(btf_tree.next(), Some((2, &c4)));
    assert_eq!(btf_tree.next(), Some((2, &c5)));
    assert_eq!(btf_tree.next(), Some((3, &c1)));
    assert_eq!(btf_tree.next(), Some((3, &c2)));
    assert_eq!(btf_tree.next(), Some((3, &c2)));
    assert_eq!(btf_tree.next(), Some((3, &c3)));
    assert_eq!(btf_tree.next(), None);
}
