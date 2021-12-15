use super::*;
use crate::ir::relation::*;
use crate::ir::*;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn proj_preserve_dist_key() {
    let mut plan = Plan::empty();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
            Column::new("c", Type::String),
            Column::new("d", Type::String),
        ],
        &["b", "a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan = Relational::new_scan("t", &mut plan).unwrap();
    let scan_id = vec_alloc(&mut plan.nodes, Node::Relational(scan));

    let proj = Relational::new_proj(&mut plan, scan_id, &["a", "b"]).unwrap();
    let proj_id = vec_alloc(&mut plan.nodes, Node::Relational(proj));

    plan.top = Some(proj_id);

    let map = plan.relational_id_map();

    let scan_output: usize = if let Node::Relational(scan) = plan.get_node(scan_id).unwrap() {
        scan.output()
    } else {
        panic!("Invalid plan!");
    };
    set_distribution(scan_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![1, 0] },
            scan_row.distribution().unwrap()
        );
    }

    let proj_output: usize = if let Node::Relational(proj) = plan.get_node(proj_id).unwrap() {
        proj.output()
    } else {
        panic!("Invalid plan!");
    };
    set_distribution(proj_output, &map, &mut plan).unwrap();
    if let Node::Expression(proj_row) = plan.get_node(proj_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![1, 0] },
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
        .join("expression")
        .join("shuffle_dist_key.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let map = plan.relational_id_map();

    let scan_output = 8;
    let proj_output = 14;

    set_distribution(scan_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![1, 0] },
            scan_row.distribution().unwrap()
        );
    }

    set_distribution(proj_output, &map, &mut plan).unwrap();
    if let Node::Expression(proj_row) = plan.get_node(proj_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![0, 1] },
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
        .join("expression")
        .join("shrink_dist_key_1.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let map = plan.relational_id_map();

    let scan_output = 8;
    let proj_output = 14;

    set_distribution(scan_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![1, 0] },
            scan_row.distribution().unwrap()
        );
    }

    set_distribution(proj_output, &map, &mut plan).unwrap();
    if let Node::Expression(proj_row) = plan.get_node(proj_output).unwrap() {
        assert_eq!(&Distribution::Random, proj_row.distribution().unwrap());
    }
}

#[test]
fn proj_shrink_dist_key_2() {
    // Load a table "t (a, b, c, d)" distributed by ["b", "a"]
    // with projection ["a"].
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("expression")
        .join("shrink_dist_key_2.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let map = plan.relational_id_map();

    let scan_output = 8;
    let proj_output = 12;

    set_distribution(scan_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![1, 0] },
            scan_row.distribution().unwrap()
        );
    }

    set_distribution(proj_output, &map, &mut plan).unwrap();
    if let Node::Expression(proj_row) = plan.get_node(proj_output).unwrap() {
        assert_eq!(&Distribution::Random, proj_row.distribution().unwrap());
    }
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
        .join("expression")
        .join("union_fallback_to_random.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let map = plan.relational_id_map();

    let scan_t1_output = 4;
    let scan_t2_output = 10;
    let union_output = 16;

    set_distribution(scan_t1_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_t1_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![0] },
            scan_row.distribution().unwrap()
        );
    }

    set_distribution(scan_t2_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_t2_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![1] },
            scan_row.distribution().unwrap()
        );
    }

    set_distribution(union_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(union_output).unwrap() {
        assert_eq!(&Distribution::Random, scan_row.distribution().unwrap());
    }
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
        .join("expression")
        .join("union_preserve_dist.yaml");
    let s = fs::read_to_string(path).unwrap();
    let mut plan = Plan::from_yaml(&s).unwrap();

    let map = plan.relational_id_map();

    let scan_t1_output = 4;
    let scan_t2_output = 10;
    let union_output = 16;

    set_distribution(scan_t1_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_t1_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![0] },
            scan_row.distribution().unwrap()
        );
    }

    set_distribution(scan_t2_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(scan_t2_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![0] },
            scan_row.distribution().unwrap()
        );
    }

    set_distribution(union_output, &map, &mut plan).unwrap();
    if let Node::Expression(scan_row) = plan.get_node(union_output).unwrap() {
        assert_eq!(
            &Distribution::Segment { key: vec![0] },
            scan_row.distribution().unwrap()
        );
    }
}

//TODO: add other distribution variants to the test cases.
