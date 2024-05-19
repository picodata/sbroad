use super::*;
use crate::ir::operator::Relational;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::Plan;
use crate::ir::Slices;
use pretty_assertions::assert_eq;

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
pub fn get_motion_id(plan: &Plan, slice_id: usize, motion_idx: usize) -> Option<&NodeId> {
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
