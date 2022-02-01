use super::*;
use crate::errors::QueryPlannerError;
use crate::ir::relation::*;
use crate::ir::*;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn segment_motion_for_sub_query() {
    // t1(a int) key [a]
    // t2(a int, b int) key [a]
    // select * from t1 where a = (select b from t2)
    let mut plan = Plan::new();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_seg("t1", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1").unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_seg(
        "t2",
        vec![
            Column::new("a", Type::Integer),
            Column::new("b", Type::Integer),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2").unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["b"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let id = plan.nodes.next_id();
    let b_id = plan
        .add_row_from_sub_query(id, &children[..], children.len() - 1, &["b"])
        .unwrap();
    let a_id = plan.add_row_from_child(id, scan_t1_id, &["a"]).unwrap();
    let eq_id = plan.add_cond(a_id, Bool::Eq, b_id).unwrap();

    let select_id = plan.add_select(&children[..], eq_id, id).unwrap();
    plan.set_top(select_id).unwrap();

    plan.build_relational_map();

    let mut expected_rel_set: HashSet<usize> = HashSet::new();
    expected_rel_set.insert(sq_id);
    assert_eq!(
        expected_rel_set,
        plan.get_relational_from_row_nodes(b_id).unwrap()
    );
    assert_eq!(Some(sq_id), plan.get_sub_query_from_row_node(b_id).unwrap());

    assert_eq!(
        QueryPlannerError::UninitializedDistribution,
        plan.resolve_sub_query_conflicts(eq_id).unwrap_err()
    );

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.nodes.add_new_equalities().unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("transformation")
        .join("redistribution")
        .join("segment_motion_for_sub_query.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(plan, expected_plan);
}

#[test]
fn full_motion_less_for_sub_query() {
    // t1(a int) key [a]
    // t2(a int, b int) key [a]
    // select * from t1 where a < (select b from t2)
    let mut plan = Plan::new();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_seg("t1", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1").unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_seg(
        "t2",
        vec![
            Column::new("a", Type::Integer),
            Column::new("b", Type::Integer),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2").unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["b"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let id = plan.nodes.next_id();
    let b_id = plan
        .add_row_from_sub_query(id, &children[..], children.len() - 1, &["b"])
        .unwrap();
    let a_id = plan.add_row_from_child(id, scan_t1_id, &["a"]).unwrap();
    let less_id = plan.add_cond(a_id, Bool::Lt, b_id).unwrap();

    let select_id = plan.add_select(&children[..], less_id, id).unwrap();
    plan.set_top(select_id).unwrap();

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.nodes.add_new_equalities().unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("transformation")
        .join("redistribution")
        .join("full_motion_less_for_sub_query.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(plan, expected_plan);
}

#[test]
fn full_motion_non_segment_outer_for_sub_query() {
    // t1(a int, b int) key [a]
    // t2(a int) key [a]
    // select * from t1 where b = (select a from t2)
    let mut plan = Plan::new();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_seg(
        "t1",
        vec![
            Column::new("a", Type::Integer),
            Column::new("b", Type::Integer),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1").unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_seg("t2", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2").unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["a"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let id = plan.nodes.next_id();
    let a_id = plan
        .add_row_from_sub_query(id, &children[..], children.len() - 1, &["a"])
        .unwrap();
    let b_id = plan.add_row_from_child(id, scan_t1_id, &["b"]).unwrap();
    let eq_id = plan.add_cond(b_id, Bool::Eq, a_id).unwrap();

    let select_id = plan.add_select(&children[..], eq_id, id).unwrap();
    plan.set_top(select_id).unwrap();

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.nodes.add_new_equalities().unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("transformation")
        .join("redistribution")
        .join("full_motion_non_segment_outer_for_sub_query.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(plan, expected_plan);
}

#[test]
fn local_sub_query() {
    // t1(a int) key [a]
    // t2(a int, b int) key [a]
    // select * from t1 where a = (select a from t2)
    let mut plan = Plan::new();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_seg("t1", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1").unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_seg("t2", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2").unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["a"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let id = plan.nodes.next_id();
    let inner_a_id = plan
        .add_row_from_sub_query(id, &children[..], children.len() - 1, &["a"])
        .unwrap();
    let outer_a_id = plan.add_row_from_child(id, scan_t1_id, &["a"]).unwrap();
    let eq_id = plan.add_cond(outer_a_id, Bool::Eq, inner_a_id).unwrap();

    let select_id = plan.add_select(&children[..], eq_id, id).unwrap();
    plan.set_top(select_id).unwrap();

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.nodes.add_new_equalities().unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("transformation")
        .join("redistribution")
        .join("local_sub_query.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(plan, expected_plan);
}

#[test]
fn multiple_sub_queries() {
    // t1(a int) key [a]
    // t2(a int, b int) key [a]
    // select * from t1 where a < (select a from t2) or a = (select b from t2)
    let mut plan = Plan::new();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_seg("t1", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1").unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_seg(
        "t2",
        vec![
            Column::new("a", Type::Integer),
            Column::new("b", Type::Integer),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t2);
    let sq1_scan_t2_id = plan.add_scan("t2").unwrap();
    let sq1_proj_id = plan.add_proj(sq1_scan_t2_id, &["a"]).unwrap();
    let sq1_id = plan.add_sub_query(sq1_proj_id, None).unwrap();
    children.push(sq1_id);
    let sq1_pos = children.len() - 1;

    let sq2_scan_t2_id = plan.add_scan("t2").unwrap();
    let sq2_proj_id = plan.add_proj(sq2_scan_t2_id, &["b"]).unwrap();
    let sq2_id = plan.add_sub_query(sq2_proj_id, None).unwrap();
    children.push(sq2_id);
    let sq2_pos = children.len() - 1;

    let id = plan.nodes.next_id();
    let sq1_inner_a_id = plan
        .add_row_from_sub_query(id, &children[..], sq1_pos, &["a"])
        .unwrap();
    let sq1_outer_a_id = plan.add_row_from_child(id, scan_t1_id, &["a"]).unwrap();
    let less_id = plan
        .add_cond(sq1_outer_a_id, Bool::Lt, sq1_inner_a_id)
        .unwrap();

    let sq2_inner_a_id = plan
        .add_row_from_sub_query(id, &children[..], sq2_pos, &["b"])
        .unwrap();
    let sq2_outer_a_id = plan.add_row_from_child(id, scan_t1_id, &["a"]).unwrap();
    let eq_id = plan
        .add_cond(sq2_outer_a_id, Bool::Eq, sq2_inner_a_id)
        .unwrap();

    let or_id = plan.add_cond(less_id, Bool::Or, eq_id).unwrap();

    let select_id = plan.add_select(&children[..], or_id, id).unwrap();
    plan.set_top(select_id).unwrap();

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.nodes.add_new_equalities().unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("transformation")
        .join("redistribution")
        .join("multiple_sub_queries.yaml");
    let s = fs::read_to_string(path).unwrap();
    let expected_plan = Plan::from_yaml(&s).unwrap();
    assert_eq!(plan, expected_plan);
}
