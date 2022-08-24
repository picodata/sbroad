use super::*;
use crate::ir::relation::*;
use crate::ir::*;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn proj_preserve_dist_key() {
    let mut plan = Plan::default();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Unsigned, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
            Column::new("d", Type::String, ColumnRole::User),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan_id = plan.add_scan("t", None).unwrap();
    let proj_id = plan.add_proj(scan_id, &["a", "b"]).unwrap();

    plan.top = Some(proj_id);

    let scan_output: usize = if let Node::Relational(scan) = plan.get_node(scan_id).unwrap() {
        scan.output()
    } else {
        panic!("Invalid plan!");
    };
    plan.set_distribution(scan_output).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![1, 0]) }
            },
            scan_row.distribution().unwrap()
        );
    }

    let proj_output: usize = if let Node::Relational(proj) = plan.get_node(proj_id).unwrap() {
        proj.output()
    } else {
        panic!("Invalid plan!");
    };
    plan.set_distribution(proj_output).unwrap();
    if let Node::Expression(proj_row) = plan.get_node(proj_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![1, 0]) }
            },
            proj_row.distribution().unwrap()
        );
    }
}

#[test]
fn proj_shuffle_dist_key() {
    // Load a table "t (a, b, c, d)" distributed by ["b", "a"]
    // with projection ["a", "b"].
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("distribution")
        .join("shuffle_dist_key.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let scan_output = 8;
    let proj_output = 14;

    plan.set_distribution(scan_output).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![1, 0]) }
            },
            scan_row.distribution().unwrap()
        );
    }

    plan.set_distribution(proj_output).unwrap();
    if let Node::Expression(proj_row) = plan.get_node(proj_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![0, 1]) }
            },
            proj_row.distribution().unwrap()
        );
    }
}

#[test]
fn proj_shrink_dist_key_1() {
    // Load a table "t (a, b, c, d)" distributed by ["b", "a"]
    // with projection ["c", "a"].
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("distribution")
        .join("shrink_dist_key_1.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let scan_output = 8;
    let proj_output = 14;

    plan.set_distribution(scan_output).unwrap();
    assert_eq!(
        &Distribution::Segment {
            keys: collection! { Key::new(vec![1, 0]) }
        },
        plan.get_distribution(scan_output).unwrap()
    );

    plan.set_distribution(proj_output).unwrap();
    assert_eq!(
        &Distribution::Any,
        plan.get_distribution(proj_output).unwrap()
    );
}

#[test]
fn proj_shrink_dist_key_2() {
    // Load a table "t (a, b, c, d)" distributed by ["b", "a"]
    // with projection ["a"].
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("distribution")
        .join("shrink_dist_key_2.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let scan_output = 8;
    let proj_output = 12;

    plan.set_distribution(scan_output).unwrap();
    assert_eq!(
        &Distribution::Segment {
            keys: collection! { Key::new(vec![1, 0]) }
        },
        plan.get_distribution(scan_output).unwrap()
    );

    plan.set_distribution(proj_output).unwrap();
    assert_eq!(
        &Distribution::Any,
        plan.get_distribution(proj_output).unwrap()
    );
}

#[test]
fn union_all_fallback_to_random() {
    // Load table "t1 (a, b)" distributed by ["a"],
    // table "t2 (a, b)" distributed by ["b"],
    // union all (t1, t2)
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("distribution")
        .join("union_fallback_to_random.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let scan_t1_output = 4;
    let scan_t2_output = 10;
    let union_output = 16;

    plan.set_distribution(scan_t1_output).unwrap();
    assert_eq!(
        &Distribution::Segment {
            keys: collection! { Key::new(vec![0]) }
        },
        plan.get_distribution(scan_t1_output).unwrap()
    );

    plan.set_distribution(scan_t2_output).unwrap();
    assert_eq!(
        &Distribution::Segment {
            keys: collection! { Key::new(vec![1]) }
        },
        plan.get_distribution(scan_t2_output).unwrap()
    );

    plan.set_distribution(union_output).unwrap();
    assert_eq!(
        &Distribution::Any,
        plan.get_distribution(union_output).unwrap()
    );
}

#[test]
fn union_preserve_dist() {
    // Load table "t1 (a, b)" distributed by ["a"],
    // table "t2 (a, b)" distributed by ["b"],
    // union all (t1, t2)
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("distribution")
        .join("union_preserve_dist.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let scan_t1_output = 4;
    let scan_t2_output = 10;
    let union_output = 16;

    plan.set_distribution(scan_t1_output).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_t1_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![0]) }
            },
            scan_row.distribution().unwrap()
        );
    }

    plan.set_distribution(scan_t2_output).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_t2_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![0]) }
            },
            scan_row.distribution().unwrap()
        );
    }

    plan.set_distribution(union_output).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(union_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![0]) }
            },
            scan_row.distribution().unwrap()
        );
    }
}

#[test]
fn join_unite_keys() {
    // Load table "t1 (a, b)" distributed by ["a"],
    // table "t2 (c, d)" distributed by ["d"],
    // select * from t1 join t2 on t1.a = t2.d
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("distribution")
        .join("join_unite_keys.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let scan_t1_output = 4;
    let scan_t2_output = 10;
    let join_output = 27;

    plan.set_distribution(scan_t1_output).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_t1_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![0]) }
            },
            scan_row.distribution().unwrap()
        );
    }

    plan.set_distribution(scan_t2_output).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_t2_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![1]) }
            },
            scan_row.distribution().unwrap()
        );
    }

    plan.set_distribution(join_output).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(join_output).unwrap() {
        assert_eq!(
            &Distribution::Segment {
                keys: collection! { Key::new(vec![0]), Key::new(vec![3]) }
            },
            scan_row.distribution().unwrap()
        );
    }
}

//TODO: add other distribution variants to the test cases.
