use crate::ir::relation::*;
use crate::ir::value::*;
use crate::ir::*;
use pretty_assertions::assert_eq;

#[test]
fn row_duplicate_column_names() {
    let mut plan = Plan::new();

    let c1 = plan.nodes.add_const(Value::number_from_str("1").unwrap());
    let c1_alias_a = plan.nodes.add_alias("a", c1).unwrap();
    let c2 = plan.nodes.add_const(Value::number_from_str("2").unwrap());
    let c2_alias_a = plan.nodes.add_alias("a", c2).unwrap();
    assert_eq!(
        QueryPlannerError::DuplicateColumn,
        plan.nodes
            .add_row_of_aliases(vec![c1_alias_a, c2_alias_a], None)
            .unwrap_err()
    );
}

#[test]
fn rel_nodes_from_reference_in_scan() {
    // t(a int) [a]
    // select * from t
    let mut plan = Plan::new();

    let t = Table::new_seg("t", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t").unwrap();
    let output = plan.get_relational_output(scan_id).unwrap();

    plan.build_relational_map();
    let rel_set = plan.get_relational_from_row_nodes(output).unwrap();
    assert_eq!(true, rel_set.is_empty());
}

#[test]
fn rel_nodes_from_reference_in_proj() {
    // t(a int) [a]
    // select a from t
    let mut plan = Plan::new();

    let t = Table::new_seg("t", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t);
    let scan_id = plan.add_scan("t").unwrap();
    let proj_id = plan.add_proj(scan_id, &["a"]).unwrap();
    let output = plan.get_relational_output(proj_id).unwrap();

    plan.build_relational_map();
    let rel_set = plan.get_relational_from_row_nodes(output).unwrap();
    assert_eq!(1, rel_set.len());
    assert_eq!(Some(&scan_id), rel_set.get(&scan_id));
}
