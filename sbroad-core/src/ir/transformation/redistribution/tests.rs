use super::*;
use crate::ir::operator::Relational;
use crate::ir::relation::{SpaceEngine, Table};
use crate::ir::tests::column_integer_user_non_null;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::Plan;
use crate::ir::Slices;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn full_motion_less_for_sub_query() {
    // t1(a int) key [a]
    // t2(a int, b int) key [a]
    // select * from t1 where a < (select b from t2)
    let mut plan = Plan::default();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_sharded(
        "t1",
        vec![column_integer_user_non_null(String::from("a"))],
        &["a"],
        &["a"],
        SpaceEngine::Vinyl,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_sharded(
        "t2",
        vec![
            column_integer_user_non_null(String::from("a")),
            column_integer_user_non_null(String::from("b")),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Vinyl,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["b"], false).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let b_id = plan
        .add_row_from_subquery(&children[..], children.len() - 1, None)
        .unwrap();
    let a_id = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let less_id = plan.add_cond(a_id, Bool::Lt, b_id).unwrap();

    let select_id = plan.add_select(&children[..], less_id).unwrap();
    plan.set_top(select_id).unwrap();

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.derive_equalities().unwrap();
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
#[allow(clippy::similar_names)]
fn full_motion_non_segment_outer_for_sub_query() {
    // t1(a int, b int) key [a]
    // t2(a int) key [a]
    // select * from t1 where b = (select a from t2)
    let mut plan = Plan::default();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_sharded(
        "t1",
        vec![
            column_integer_user_non_null(String::from("a")),
            column_integer_user_non_null(String::from("b")),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Vinyl,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_sharded(
        "t2",
        vec![column_integer_user_non_null(String::from("a"))],
        &["a"],
        &["a"],
        SpaceEngine::Vinyl,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["a"], false).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let a_id = plan
        .add_row_from_subquery(&children[..], children.len() - 1, None)
        .unwrap();
    let b_id = plan.add_row_from_child(scan_t1_id, &["b"]).unwrap();
    let eq_id = plan.add_cond(b_id, Bool::Eq, a_id).unwrap();

    let select_id = plan.add_select(&children[..], eq_id).unwrap();
    plan.set_top(select_id).unwrap();

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.derive_equalities().unwrap();
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
#[allow(clippy::similar_names)]
fn local_sub_query() {
    // t1(a int) key [a]
    // t2(a int, b int) key [a]
    // select * from t1 where a = (select a from t2)
    let mut plan = Plan::default();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_sharded(
        "t1",
        vec![column_integer_user_non_null(String::from("a"))],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_sharded(
        "t2",
        vec![column_integer_user_non_null(String::from("a"))],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["a"], false).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let inner_a_id = plan
        .add_row_from_subquery(&children[..], children.len() - 1, None)
        .unwrap();
    let outer_a_id = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let eq_id = plan.add_cond(outer_a_id, Bool::Eq, inner_a_id).unwrap();

    let select_id = plan.add_select(&children[..], eq_id).unwrap();
    plan.set_top(select_id).unwrap();

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.derive_equalities().unwrap();
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
    let mut plan = Plan::default();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_sharded(
        "t1",
        vec![column_integer_user_non_null(String::from("a"))],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_sharded(
        "t2",
        vec![
            column_integer_user_non_null(String::from("a")),
            column_integer_user_non_null(String::from("b")),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let sq1_scan_t2_id = plan.add_scan("t2", None).unwrap();
    let sq1_proj_id = plan.add_proj(sq1_scan_t2_id, &["a"], false).unwrap();
    let sq1_id = plan.add_sub_query(sq1_proj_id, None).unwrap();
    children.push(sq1_id);
    let sq1_pos = children.len() - 1;

    let sq2_scan_t2_id = plan.add_scan("t2", None).unwrap();
    let sq2_proj_id = plan.add_proj(sq2_scan_t2_id, &["b"], false).unwrap();
    let sq2_id = plan.add_sub_query(sq2_proj_id, None).unwrap();
    children.push(sq2_id);
    let sq2_pos = children.len() - 1;

    let sq1_inner_a_id = plan
        .add_row_from_subquery(&children[..], sq1_pos, None)
        .unwrap();
    let sq1_outer_a_id = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let less_id = plan
        .add_cond(sq1_outer_a_id, Bool::Lt, sq1_inner_a_id)
        .unwrap();

    let sq2_inner_a_id = plan
        .add_row_from_subquery(&children[..], sq2_pos, None)
        .unwrap();
    let sq2_outer_a_id = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let eq_id = plan
        .add_cond(sq2_outer_a_id, Bool::Eq, sq2_inner_a_id)
        .unwrap();

    let or_id = plan.add_cond(less_id, Bool::Or, eq_id).unwrap();

    let select_id = plan.add_select(&children[..], or_id).unwrap();
    plan.set_top(select_id).unwrap();

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.derive_equalities().unwrap();
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

#[test]
fn union_all_in_sq() {
    let query = r#"SELECT *
        FROM
            (SELECT "identification_number", "product_code"
            FROM "hash_testing"
            WHERE "sys_op" = 1
            UNION ALL
            SELECT "identification_number", "product_code"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 1) AS "t3"
        WHERE "identification_number" = 1"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_sq_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_sq_eq_for_keys_with_const() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", 1, "t1"."product_code") = ("t2"."id", 1, "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_sq_less_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") < ("t2"."id", "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn join_inner_sq_eq_no_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", 1) = (1, "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn join_inner_sq_eq_no_outer_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", 1) = ("t2"."id", "t2"."pc")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn inner_join_full_policy_sq_in_filter() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") >=
        (SELECT "hash_testing"."sys_op", "hash_testing"."bucket_id" FROM "hash_testing")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn inner_join_local_policy_sq_in_filter() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") =
        (SELECT "hash_testing"."identification_number", "hash_testing"."product_code" FROM "hash_testing")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn inner_join_local_policy_sq_with_union_all_in_filter() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") =
        (SELECT "hash_testing"."identification_number", "hash_testing"."product_code" FROM "hash_testing"
        UNION ALL
        SELECT "hash_testing_hist"."identification_number", "hash_testing_hist"."product_code" FROM "hash_testing_hist")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_and_local_full_policies() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")
        AND "t1"."identification_number" = "t2"."pc""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    assert_eq!(Slices::empty(), plan.slices);
}

#[test]
fn join_inner_or_local_full_policies() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")
        OR "t1"."identification_number" = "t2"."pc""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

/// Helper function to extract a motion id from a plan.
///
/// # Panics
///   Motion node does not found
#[must_use]
pub fn get_motion_id(plan: &Plan, slice_id: usize, motion_idx: usize) -> Option<&usize> {
    plan.slices.slice(slice_id).unwrap().position(motion_idx)
}

#[cfg(test)]
mod between;

#[cfg(test)]
mod except;

#[cfg(test)]
mod not_in;

#[cfg(test)]
mod segment;
