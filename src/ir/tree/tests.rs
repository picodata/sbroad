use crate::ir::operator::*;
use crate::ir::relation::*;
use crate::ir::value::*;
use crate::ir::*;
use pretty_assertions::assert_eq;
use traversal::{Bft, DftPost, DftPre};

#[test]
fn expression_bft() {
    // ((c1 = c2) and (c2 = c3)) or (c4 = c5)

    let mut plan = Plan::default();
    let c1 = plan.nodes.add_const(Value::from(1_u64));
    let c2 = plan.nodes.add_const(Value::from(1_u64));
    let c3 = plan.nodes.add_const(Value::from(1_u64));
    let c4 = plan.nodes.add_const(Value::from(2_u64));
    let c5 = plan.nodes.add_const(Value::from(3_u64));

    let c1_eq_c2 = plan.nodes.add_bool(c1, Bool::Eq, c2).unwrap();
    let c2_eq_c3 = plan.nodes.add_bool(c2, Bool::Eq, c3).unwrap();
    let c1c2_and_c2c3 = plan.nodes.add_bool(c1_eq_c2, Bool::And, c2_eq_c3).unwrap();
    let c4_eq_c5 = plan.nodes.add_bool(c4, Bool::Eq, c5).unwrap();
    let top = plan
        .nodes
        .add_bool(c1c2_and_c2c3, Bool::Or, c4_eq_c5)
        .unwrap();

    let mut bft_tree = Bft::new(&top, |node| plan.nodes.expr_iter(node, true));
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

    let mut plan = Plan::default();
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
    let mut plan = Plan::default();

    let t1 = Table::new_seg(
        "t1",
        vec![Column::new("a", Type::Boolean, ColumnRole::User)],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();

    let t2 = Table::new_seg(
        "t2",
        vec![Column::new("a", Type::Boolean, ColumnRole::User)],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();

    let a = plan.add_row_from_child(scan_t2_id, &["a"]).unwrap();
    let const1 = plan.add_const(Value::from(1_i64));
    let eq = plan.nodes.add_bool(a, Bool::Eq, const1).unwrap();
    let selection_id = plan.add_select(&[scan_t2_id], eq).unwrap();

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

#[test]
fn selection_subquery_dfs_post() {
    // select * from t1 where a in (select c from t2 where b = 1)
    //
    // ir tree:
    // selection
    // - scan t1
    // - subquery
    //     - projection: (c)
    //         - selection
    //             - scan t2
    //             - filter
    //                 - eq
    //                     - (b)
    //                     - (1)
    // - filter
    //     - in
    //         - (a)
    //         - (c)

    // Initialize plan
    let mut plan = Plan::default();

    let t1 = Table::new_seg(
        "t1",
        vec![Column::new("a", Type::Boolean, ColumnRole::User)],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    let a = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();

    let t2 = Table::new_seg(
        "t2",
        vec![
            Column::new("b", Type::Boolean, ColumnRole::User),
            Column::new("c", Type::Boolean, ColumnRole::User),
        ],
        &["b"],
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();

    let b = plan.add_row_from_child(scan_t2_id, &["b"]).unwrap();
    let const1 = plan.add_const(Value::from(1_u64));
    let eq_op = plan.nodes.add_bool(b, Bool::Eq, const1).unwrap();
    let selection_t2_id = plan.add_select(&[scan_t2_id], eq_op).unwrap();
    let proj_id = plan.add_proj(selection_t2_id, &["c"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    let c = plan.get_row_from_rel_node(sq_id).unwrap();

    let in_op = plan.nodes.add_bool(a, Bool::In, c).unwrap();
    let selection_t1_id = plan.add_select(&[scan_t1_id, sq_id], in_op).unwrap();

    plan.set_top(selection_t1_id).unwrap();
    let top = plan.get_top().unwrap();

    // Traverse relational nodes in the plan tree
    let mut dft_post = DftPost::new(&top, |node| plan.nodes.rel_iter(node));
    assert_eq!(dft_post.next(), Some((1, &scan_t1_id)));
    assert_eq!(dft_post.next(), Some((4, &scan_t2_id)));
    assert_eq!(dft_post.next(), Some((3, &selection_t2_id)));
    assert_eq!(dft_post.next(), Some((2, &proj_id)));
    assert_eq!(dft_post.next(), Some((1, &sq_id)));
    assert_eq!(dft_post.next(), Some((0, &selection_t1_id)));
    assert_eq!(dft_post.next(), None);

    // Traverse expression nodes in the selection t2 filter
    let mut dft_post = DftPost::new(&eq_op, |node| plan.nodes.expr_iter(node, true));
    assert_eq!(dft_post.next(), Some((1, &b)));
    assert_eq!(dft_post.next(), Some((1, &const1)));
    assert_eq!(dft_post.next(), Some((0, &eq_op)));
    assert_eq!(dft_post.next(), None);

    // Traverse expression nodes in the selection t1 filter
    let mut dft_post = DftPost::new(&in_op, |node| plan.nodes.expr_iter(node, true));
    assert_eq!(dft_post.next(), Some((1, &a)));
    assert_eq!(dft_post.next(), Some((1, &c)));
    assert_eq!(dft_post.next(), Some((0, &in_op)));
    assert_eq!(dft_post.next(), None);
}

#[test]
fn subtree_dfs_post() {
    // select c from t1 where a = 1
    //
    // ir tree:
    // - projection
    //    - (c)
    //    - selection
    //      - scan t1
    //      - filter
    //          - eq
    //              - (a)
    //              - (1)

    // Initialize plan
    let mut plan = Plan::default();

    let t1 = Table::new_seg(
        "t1",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("c", Type::Boolean, ColumnRole::User),
        ],
        &["a", "c"],
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    let a_ref = plan.nodes.next_id();
    let a = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let const1 = plan.add_const(Value::from(1_i64));
    let eq_op = plan.nodes.add_bool(a, Bool::Eq, const1).unwrap();
    let selection_t1_id = plan.add_select(&[scan_t1_id], eq_op).unwrap();
    let proj_id = plan.add_proj(selection_t1_id, &["c"]).unwrap();

    plan.set_top(proj_id).unwrap();
    let top = plan.get_top().unwrap();

    let proj_row_id = plan.get_relation_node(proj_id).unwrap().output();
    let row_children = plan
        .get_expression_node(proj_row_id)
        .unwrap()
        .clone_row_list()
        .unwrap();
    let alias_id = row_children.get(0).unwrap();
    let c_ref_id =
        if let Expression::Alias { child, .. } = plan.get_expression_node(*alias_id).unwrap() {
            child
        } else {
            panic!("invalid child in the row");
        };

    // Traverse relational nodes in the plan tree
    let mut dft_post = DftPost::new(&top, |node| plan.subtree_iter(node));
    assert_eq!(dft_post.next(), Some((3, c_ref_id)));
    assert_eq!(dft_post.next(), Some((2, alias_id)));
    assert_eq!(dft_post.next(), Some((1, &proj_row_id)));
    assert_eq!(dft_post.next(), Some((2, &scan_t1_id)));
    assert_eq!(dft_post.next(), Some((4, &a_ref)));
    assert_eq!(dft_post.next(), Some((3, &a)));
    assert_eq!(dft_post.next(), Some((3, &const1)));
    assert_eq!(dft_post.next(), Some((2, &eq_op)));
    assert_eq!(dft_post.next(), Some((1, &selection_t1_id)));
    assert_eq!(dft_post.next(), Some((0, &proj_id)));
    assert_eq!(dft_post.next(), None);
}
