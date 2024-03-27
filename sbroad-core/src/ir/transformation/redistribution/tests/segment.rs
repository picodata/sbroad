use crate::collection;
use crate::errors::{Entity, SbroadError};
use crate::ir::distribution::{Distribution, Key};
use crate::ir::helpers::RepeatableState;
use crate::ir::operator::{Bool, Relational};
use crate::ir::relation::{Column, SpaceEngine, Table};
use crate::ir::tests::column_integer_user_non_null;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::transformation::redistribution::{MotionKey, MotionPolicy, Target};
use crate::ir::{Node, Plan};
use ahash::RandomState;
use pretty_assertions::assert_eq;
use smol_str::SmolStr;
use std::collections::HashSet;
use std::fs;
use std::path::Path;

#[test]
#[allow(clippy::similar_names)]
fn sub_query1() {
    // t1(a int) key [a]
    // t2(a int, b int) key [a]
    // select * from t1 where a = (select b from t2)
    let mut plan = Plan::default();
    let mut children: Vec<usize> = Vec::new();

    let t1 = Table::new_sharded(
        "t1",
        vec![column_integer_user_non_null(SmolStr::from("a"))],
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
            column_integer_user_non_null(SmolStr::from("a")),
            column_integer_user_non_null(SmolStr::from("b")),
        ],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    plan.add_rel(t2);
    let scan_t2_id = plan.add_scan("t2", None).unwrap();
    let proj_id = plan.add_proj(scan_t2_id, &["b"], false, false).unwrap();
    let sq_id = plan.add_sub_query(proj_id, None).unwrap();
    children.push(sq_id);

    let b_id = plan
        .add_row_from_subquery(&children[..], children.len() - 1, None)
        .unwrap();
    let a_id = plan.add_row_from_child(scan_t1_id, &["a"]).unwrap();
    let eq_id = plan.add_cond(a_id, Bool::Eq, b_id).unwrap();

    let select_id = plan.add_select(&children[..], eq_id).unwrap();
    plan.set_top(select_id).unwrap();

    let mut expected_rel_set: HashSet<usize, RandomState> =
        HashSet::with_hasher(RandomState::new());
    expected_rel_set.insert(sq_id);
    assert_eq!(
        expected_rel_set,
        plan.get_relational_nodes_from_row(b_id).unwrap()
    );
    assert_eq!(Some(sq_id), plan.get_sub_query_from_row_node(b_id).unwrap());

    assert_eq!(
        SbroadError::Invalid(
            Entity::Distribution,
            Some("distribution is uninitialized".into()),
        ),
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
fn inner_join1() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN
        (SELECT "identification_number" as "id", "product_code" as "pc" FROM "hash_testing_hist") AS "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."pc", "t2"."id")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![1, 0]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn inner_join2() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t"
        ON ("t1"."identification_number", "t1"."product_code") = ("t"."a", "t"."b")
        AND ("t"."a", "t"."b") =
        (SELECT "hash_testing"."identification_number", "hash_testing"."product_code" FROM "hash_testing"
        UNION ALL
        SELECT "hash_testing_hist"."product_code", "hash_testing_hist"."identification_number" FROM "hash_testing_hist")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![0, 1]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn inner_join3() {
    /*
       test_space is sharded by ("id") pos: (0)
       hash_testing & hash_testing_hist have the same columns and sharded
       by (indentification_number, product_code) pos: (0, 1)

       outer join child will have distribution Segment(0)
       inner join child will have distribution Any
       Motion for inner child will have policy Segment(0)
       and motion distribution will be Segment(0)
       then for join distribution we will have 2 keys:
       outer child gives Key(0)
       inner child gives Key(2) (shifted first column of inner child:
       outer child have two columns)
    */
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

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment((Key { positions: vec![0] }).into())
        );
    } else {
        panic!("Expected a motion node");
    }

    // Check distribution of the join output tuple.
    let mut join_node: Option<&Relational> = None;
    for node in &plan.nodes.arena {
        if let Node::Relational(rel) = node {
            if matches!(rel, Relational::Join { .. }) {
                join_node = Some(rel);
                break;
            }
        }
    }
    let join = join_node.unwrap();
    let dist = plan.get_distribution(join.output()).unwrap();
    let keys: HashSet<_, RepeatableState> = collection! { Key::new(vec![0]), Key::new(vec![2]) };
    assert_eq!(&Distribution::Segment { keys: keys.into() }, dist,);
}

#[test]
fn inner_join4() {
    let query = r#"SELECT * FROM "hash_testing" AS "t1"
        INNER JOIN "t" as "t2"
        ON ("t1"."identification_number", "t1"."product_code") = ("t2"."d", "t2"."a")"#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![3, 0]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert1() {
    let query = r#"INSERT INTO "t" SELECT "d", "c", "b", "a" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![0, 1]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert2() {
    let query = r#"INSERT INTO "t" SELECT * FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::LocalSegment(
                (Key {
                    positions: vec![0, 1]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert3() {
    let query = r#"INSERT INTO "t" ("a", "b") SELECT "a", "b" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::LocalSegment(
                (Key {
                    positions: vec![0, 1]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert4() {
    let query = r#"INSERT INTO "t" ("b", "a") SELECT "a", "b" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(
                (Key {
                    positions: vec![1, 0]
                })
                .into()
            )
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert5() {
    let query = r#"INSERT INTO "t" ("c", "d", "b") SELECT "a", "c", "b" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(MotionKey {
                targets: vec![Target::Value(Column::default_value()), Target::Reference(2),]
            })
        );
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn insert6() {
    let query = r#"INSERT INTO "t" ("c", "b") SELECT "b", "c" FROM "t""#;

    let mut plan = sql_to_ir(query, vec![]);
    plan.add_motions().unwrap();
    let motion_id = *plan.slices.slice(0).unwrap().position(0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(
            *policy,
            MotionPolicy::Segment(MotionKey {
                targets: vec![Target::Value(Column::default_value()), Target::Reference(1)]
            })
        );
    } else {
        panic!("Expected a motion node");
    }
}
