use super::*;
use crate::collection;
use crate::errors::QueryPlannerError;
use crate::ir::distribution::*;
use crate::ir::operator::Relational;
use crate::ir::relation::*;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::Plan;
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
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
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
    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["b"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let b_id = plan
        .add_row_from_sub_query(&children[..], children.len() - 1, &["b"])
        .unwrap();
    let a_id = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let eq_id = plan.add_cond(a_id, Bool::Eq, b_id).unwrap();

    let select_id = plan.add_select(&children[..], eq_id).unwrap();
    plan.set_top(select_id).unwrap();

    let mut expected_rel_set: HashSet<usize> = HashSet::new();
    expected_rel_set.insert(sq_id);
    assert_eq!(
        expected_rel_set,
        plan.get_relational_from_row_nodes(b_id).unwrap()
    );
    assert_eq!(Some(sq_id), plan.get_sub_query_from_row_node(b_id).unwrap());

    assert_eq!(
        QueryPlannerError::UninitializedDistribution,
        plan.resolve_sub_query_conflicts(select_id, eq_id)
            .unwrap_err()
    );

    plan.add_motions().unwrap();

    // Check the modified plan
    plan.derive_equalities().unwrap();
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
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
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
    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["b"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let b_id = plan
        .add_row_from_sub_query(&children[..], children.len() - 1, &["b"])
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
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_seg("t2", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["a"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let a_id = plan
        .add_row_from_sub_query(&children[..], children.len() - 1, &["a"])
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
fn local_sub_query() {
    // t1(a int) key [a]
    // t2(a int, b int) key [a]
    // select * from t1 where a = (select a from t2)
    let mut plan = Plan::new();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_seg("t1", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
    children.push(scan_t1_id);

    let t2 = Table::new_seg("t2", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["a"]).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let inner_a_id = plan
        .add_row_from_sub_query(&children[..], children.len() - 1, &["a"])
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
    let mut plan = Plan::new();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_seg("t1", vec![Column::new("a", Type::Integer)], &["a"]).unwrap();
    plan.add_rel(t1);
    let scan_t1_id = plan.add_scan("t1", None).unwrap();
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
    let sq1_scan_t2_id = plan.add_scan("t2", None).unwrap();
    let sq1_proj_id = plan.add_proj(sq1_scan_t2_id, &["a"]).unwrap();
    let sq1_id = plan.add_sub_query(sq1_proj_id, None).unwrap();
    children.push(sq1_id);
    let sq1_pos = children.len() - 1;

    let sq2_scan_t2_id = plan.add_scan("t2", None).unwrap();
    let sq2_proj_id = plan.add_proj(sq2_scan_t2_id, &["b"]).unwrap();
    let sq2_id = plan.add_sub_query(sq2_proj_id, None).unwrap();
    children.push(sq2_id);
    let sq2_pos = children.len() - 1;

    let sq1_inner_a_id = plan
        .add_row_from_sub_query(&children[..], sq1_pos, &["a"])
        .unwrap();
    let sq1_outer_a_id = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let less_id = plan
        .add_cond(sq1_outer_a_id, Bool::Lt, sq1_inner_a_id)
        .unwrap();

    let sq2_inner_a_id = plan
        .add_row_from_sub_query(&children[..], sq2_pos, &["b"])
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

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let expected: Option<Vec<Vec<usize>>> = None;
    assert_eq!(expected, plan.slices);
}

#[test]
fn inner_join_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")"#;

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let expected: Option<Vec<Vec<usize>>> = None;
    assert_eq!(expected, plan.slices);
}

#[test]
fn join_inner_sq_eq_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")"#;

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let expected: Option<Vec<Vec<usize>>> = None;
    assert_eq!(expected, plan.slices);
}

#[test]
fn join_inner_eq_non_match_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."pc", "t2"."id")"#;

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let motion_id = *plan
        .slices
        .as_ref()
        .unwrap()
        .get(0)
        .unwrap()
        .get(0)
        .unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(Key {
                positions: vec![0, 1]
            })
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn join_inner_sq_eq_for_keys_with_const() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", 1, "t1"."product_code") = ("t2"."id", 1, "t2"."pc")"#;

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let expected: Option<Vec<Vec<usize>>> = None;
    assert_eq!(expected, plan.slices);
}

#[test]
fn join_inner_sq_less_for_keys() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") < ("t2"."id", "t2"."pc")"#;

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let motion_id = *plan
        .slices
        .as_ref()
        .unwrap()
        .get(0)
        .unwrap()
        .get(0)
        .unwrap();
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

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let motion_id = *plan
        .slices
        .as_ref()
        .unwrap()
        .get(0)
        .unwrap()
        .get(0)
        .unwrap();
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

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let motion_id = *plan
        .slices
        .as_ref()
        .unwrap()
        .get(0)
        .unwrap()
        .get(0)
        .unwrap();
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

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let motion_id = *plan
        .slices
        .as_ref()
        .unwrap()
        .get(0)
        .unwrap()
        .get(0)
        .unwrap();
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

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let expected: Option<Vec<Vec<usize>>> = None;
    assert_eq!(expected, plan.slices);
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

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let expected: Option<Vec<Vec<usize>>> = None;
    assert_eq!(expected, plan.slices);
}

#[test]
fn inner_join_full_policy_sq_with_union_all_in_filter() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") =
        (SELECT "hash_testing"."identification_number", "hash_testing"."product_code" FROM "hash_testing"
        UNION ALL
        SELECT "hash_testing_hist"."product_code", "hash_testing_hist"."identification_number" FROM "hash_testing_hist")"#;

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let motion_id = *plan
        .slices
        .as_ref()
        .unwrap()
        .get(0)
        .unwrap()
        .get(0)
        .unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(Key {
                positions: vec![0, 1]
            })
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn join_inner_and_local_full_policies() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")
        AND "t1"."identification_number" = "t2"."pc""#;

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let expected: Option<Vec<Vec<usize>>> = None;
    assert_eq!(expected, plan.slices);
}

#[test]
fn join_inner_or_local_full_policies() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."id", "t2"."pc")
        OR "t1"."identification_number" = "t2"."pc""#;

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let motion_id = *plan
        .slices
        .as_ref()
        .unwrap()
        .get(0)
        .unwrap()
        .get(0)
        .unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn join1() {
    let query = r#"SELECT *
        FROM
            (SELECT "id", "FIRST_NAME"
            FROM "test_space"
            WHERE "sys_op" < 0
                    AND "sysFrom" >= 0
            UNION ALL
            SELECT "id", "FIRST_NAME"
            FROM "test_space_hist"
            WHERE "sysFrom" <= 0) AS "t3"
        INNER JOIN
            (SELECT "identification_number"
            FROM "hash_testing_hist"
            WHERE "sys_op" > 0
            UNION ALL
            SELECT "identification_number"
            FROM "hash_single_testing_hist"
            WHERE "sys_op" <= 0) AS "t8"
            ON "t3"."id" = "t8"."identification_number"
        WHERE "t3"."id" = 1 AND "t8"."identification_number" = 1"#;

    let mut plan = sql_to_ir(query);
    plan.add_motions().unwrap();
    let motion_id = *plan
        .slices
        .as_ref()
        .unwrap()
        .get(0)
        .unwrap()
        .get(0)
        .unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(*policy, MotionPolicy::Segment(Key { positions: vec![0] }));
    } else {
        panic!("Expected a motion node");
    }

    // Check distribution of the join output tuple.
    let mut join_node: Option<&Relational> = None;
    for node in &plan.nodes.arena {
        if let Node::Relational(rel) = node {
            if matches!(rel, Relational::InnerJoin { .. }) {
                join_node = Some(rel);
                break;
            }
        }
    }
    let join = join_node.unwrap();
    let dist = plan.get_distribution(join.output()).unwrap();
    assert_eq!(
        &Distribution::Segment {
            keys: collection! { Key::new(vec![0]) },
        },
        dist,
    );
}
