use crate::ir::operator::*;
use crate::ir::relation::*;
use crate::ir::value::*;
use crate::ir::*;
use pretty_assertions::assert_eq;
use traversal::{Bft, DftPost, DftPre};

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

    let mut bft_tree = Bft::new(&top, |node| plan.nodes.expr_iter(node));
    assert_eq!(bft_tree.next(), Some((0, &top)));
    assert_eq!(bft_tree.next(), Some((1, &c1c2_and_c2c3)));
    assert_eq!(bft_tree.next(), Some((1, &c4_eq_c5)));
    assert_eq!(bft_tree.next(), Some((2, &c1_eq_c2)));
    assert_eq!(bft_tree.next(), Some((2, &c2_eq_c3)));
    assert_eq!(bft_tree.next(), Some((2, &c4)));
    assert_eq!(bft_tree.next(), Some((2, &c5)));
    assert_eq!(bft_tree.next(), Some((3, &c1)));
    assert_eq!(bft_tree.next(), Some((3, &c2)));
    assert_eq!(bft_tree.next(), Some((3, &c2)));
    assert_eq!(bft_tree.next(), Some((3, &c3)));
    assert_eq!(bft_tree.next(), None);
}

#[test]
fn and_chain_pre() {
    // (((b1 or b2) and b3) and b4) and (b5 = (b6 = b7))

    let mut plan = Plan::new();
    let b1 = plan.nodes.add_const(Value::Boolean(true));
    let b2 = plan.nodes.add_const(Value::Boolean(true));
    let b3 = plan.nodes.add_const(Value::Boolean(true));
    let b4 = plan.nodes.add_const(Value::Boolean(true));
    let b5 = plan.nodes.add_const(Value::Boolean(true));
    let b6 = plan.nodes.add_const(Value::Boolean(true));
    let b7 = plan.nodes.add_const(Value::Boolean(true));

    let b1_2 = plan.nodes.add_bool(b1, Bool::Or, b2).unwrap();
    let b1_23 = plan.nodes.add_bool(b1_2, Bool::And, b3).unwrap();
    let b1_234 = plan.nodes.add_bool(b1_23, Bool::And, b4).unwrap();
    let b6b7 = plan.nodes.add_bool(b6, Bool::Eq, b7).unwrap();
    let b5b6b7 = plan.nodes.add_bool(b5, Bool::Eq, b6b7).unwrap();
    let top = plan.nodes.add_bool(b1_234, Bool::And, b5b6b7).unwrap();

    let mut dft_pre = DftPre::new(&top, |node| plan.nodes.eq_iter(node));
    assert_eq!(dft_pre.next(), Some((0, &top)));
    assert_eq!(dft_pre.next(), Some((1, &b1_234)));
    assert_eq!(dft_pre.next(), Some((2, &b1_23)));
    assert_eq!(dft_pre.next(), Some((3, &b1_2)));
    assert_eq!(dft_pre.next(), Some((3, &b3)));
    assert_eq!(dft_pre.next(), Some((2, &b4)));
    assert_eq!(dft_pre.next(), Some((1, &b5b6b7)));
    assert_eq!(dft_pre.next(), Some((2, &b5)));
    assert_eq!(dft_pre.next(), Some((2, &b6b7)));
    assert_eq!(dft_pre.next(), Some((3, &b6)));
    assert_eq!(dft_pre.next(), Some((3, &b7)));
    assert_eq!(dft_pre.next(), None);
}

#[test]
fn relational_post() {
    // select * from t1 union all (select * from t2 where a = 1)
    // output: scan t1, scan t2, selection, union all

    // Initialize plan
    let mut plan = Plan::new();

    let t1 = Table::new_seg("t1", vec![Column::new("a", Type::Boolean)], &["a"]).unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1").unwrap();

    let t2 = Table::new_seg("t2", vec![Column::new("a", Type::Boolean)], &["a"]).unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2").unwrap();

    let id = plan.nodes.next_id();
    let a = plan.add_row_from_child(id, scan_t2_id, &["a"]).unwrap();
    let const1 = plan.add_const(Value::number_from_str("1").unwrap());
    let eq = plan.nodes.add_bool(a, Bool::Eq, const1).unwrap();
    let selection_id = plan.add_select(&[scan_t2_id], eq, id).unwrap();

    let union_id = plan.add_union_all(scan_t1_id, selection_id).unwrap();
    plan.set_top(union_id).unwrap();
    let top = plan.get_top().unwrap();

    // Traverse the tree
    let mut dft_post = DftPost::new(&top, |node| plan.nodes.rel_iter(node));
    assert_eq!(dft_post.next(), Some((1, &scan_t1_id)));
    assert_eq!(dft_post.next(), Some((2, &scan_t2_id)));
    assert_eq!(dft_post.next(), Some((1, &selection_id)));
    assert_eq!(dft_post.next(), Some((0, &union_id)));
    assert_eq!(dft_post.next(), None);
}
