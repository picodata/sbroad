use pretty_assertions::assert_eq;

use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table, Type};
use crate::ir::value::Value;
use crate::ir::{Plan, SbroadError};

#[test]
fn row_duplicate_column_names() {
    let mut plan = Plan::default();

    let c1 = plan.nodes.add_const(Value::from(1_i64));
    let c1_alias_a = plan.nodes.add_alias("a", c1).unwrap();
    let c2 = plan.nodes.add_const(Value::from(2_u64));
    let c2_alias_a = plan.nodes.add_alias("a", c2).unwrap();
    assert_eq!(
        SbroadError::DuplicatedValue("row can't be added because `a` already has an alias".into()),
        plan.nodes
            .add_row_of_aliases(vec![c1_alias_a, c2_alias_a], None)
            .unwrap_err()
    );
}

#[test]
fn rel_nodes_from_reference_in_scan() {
    // t(a int) [a]
    // select * from t
    let mut plan = Plan::default();

    let t = Table::new_seg(
        "t",
        vec![Column::new("a", Type::Integer, ColumnRole::User)],
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

    let t = Table::new_seg(
        "t",
        vec![Column::new("a", Type::Integer, ColumnRole::User)],
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
