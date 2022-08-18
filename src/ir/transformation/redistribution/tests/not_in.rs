use crate::ir::operator::Relational;
use crate::ir::transformation::helpers::sql_to_ir;
use crate::ir::transformation::redistribution::tests::get_motion_id;
use crate::ir::transformation::redistribution::*;
use pretty_assertions::assert_eq;

#[test]
fn not_in1() {
    let query = r#"SELECT 1 FROM "hash_testing" AS "t" WHERE "product_code" NOT IN (
        SELECT "product_code" FROM "hash_testing_hist")"#;

    let mut plan = sql_to_ir(query, &mut vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
    let motion = plan.get_relation_node(motion_id).unwrap();
    if let Relational::Motion { policy, .. } = motion {
        assert_eq!(*policy, MotionPolicy::Full);
    } else {
        panic!("Expected a motion node");
    }
}

#[test]
fn not_in2() {
    let query = r#"SELECT 1 FROM "hash_testing" AS "t" WHERE ("identification_number", "product_code") NOT IN (
        SELECT "identification_number", "product_code" FROM "hash_testing_hist" AS "t")"#;

    let mut plan = sql_to_ir(query, &mut vec![]);
    plan.add_motions().unwrap();
    let expected: Option<Vec<Vec<usize>>> = None;
    assert_eq!(expected, plan.slices);
}

#[test]
fn not_in3() {
    let query = r#"SELECT 1 FROM "hash_testing" AS "t" WHERE ("identification_number", "product_code") NOT IN (
        SELECT "product_code", 42 FROM "hash_testing_hist")"#;

    let mut plan = sql_to_ir(query, &mut vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
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
fn not_in4() {
    let query = r#"SELECT 1 FROM "hash_testing" AS "t" WHERE ("product_code", "identification_number") NOT IN (
        SELECT "product_code", 42 FROM "hash_testing_hist")"#;

    let mut plan = sql_to_ir(query, &mut vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
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
fn not_in5() {
    let query = r#"SELECT 1 FROM "hash_testing" AS "t" WHERE ("identification_number", "product_code") NOT IN (
        SELECT 42, 666 FROM "hash_testing_hist" AS "t")"#;

    let mut plan = sql_to_ir(query, &mut vec![]);
    plan.add_motions().unwrap();
    let motion_id = *get_motion_id(&plan, 0, 0).unwrap();
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