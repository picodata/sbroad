use super::*;
use crate::errors::QueryPlannerError;
use crate::ir::expression::*;
use crate::ir::relation::*;
use crate::ir::value::*;
use itertools::Itertools;
use pretty_assertions::assert_eq;
use std::fs;
use std::path::Path;

#[test]
fn scan_rel() {
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
    assert_eq!(
        Relational::ScanRelation {
            output: 8,
            relation: String::from("t"),
        },
        scan
    );

    if let Node::Expression(row) = plan.get_node(8).unwrap() {
        assert_eq!(
            *row.distribution().unwrap(),
            Distribution::Segment { key: vec![1, 0] }
        );
    } else {
        panic!("Wrong output node type!");
    }

    assert_eq!(9, vec_alloc(&mut plan.nodes, Node::Relational(scan)));
}

#[test]
fn scan_rel_serialized() {
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
    plan.nodes.push(Node::Relational(scan));
    plan.top = Some(9);

    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("scan_rel.yaml");
    let s = fs::read_to_string(path).unwrap();
    assert_eq!(plan, Plan::from_yaml(&s).unwrap());
}

#[test]
fn projection() {
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
    let proj_seg = Relational::new_proj(&mut plan, scan_id, &["b", "a"]).unwrap();
    assert_eq!(
        Relational::Projection {
            child: scan_id,
            output: 14
        },
        proj_seg
    );

    if let Node::Expression(row) = plan.get_node(14).unwrap() {
        assert_eq!(
            *row.distribution().unwrap(),
            Distribution::Segment { key: vec![0, 1] }
        );
    }

    let proj_rand = Relational::new_proj(&mut plan, scan_id, &["a", "d"]).unwrap();
    assert_eq!(
        Relational::Projection {
            child: scan_id,
            output: 19
        },
        proj_rand
    );

    if let Node::Expression(row) = plan.get_node(19).unwrap() {
        assert_eq!(*row.distribution().unwrap(), Distribution::Random);
    }

    // Empty output
    assert_eq!(
        QueryPlannerError::InvalidRow,
        Relational::new_proj(&mut plan, scan_id, &[]).unwrap_err()
    );

    // Invalid alias names in the output
    assert_eq!(
        QueryPlannerError::InvalidRow,
        Relational::new_proj(&mut plan, scan_id, &["a", "e"]).unwrap_err()
    );

    // Expression node instead of relational one
    assert_eq!(
        QueryPlannerError::InvalidPlan,
        Relational::new_proj(&mut plan, 1, &["a"]).unwrap_err()
    );

    // Try to build projection from the invalid node
    assert_eq!(
        QueryPlannerError::ValueOutOfRange,
        Relational::new_proj(&mut plan, 42, &["a"]).unwrap_err()
    );
}

#[test]
fn projection_serialize() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("projection.yaml");
    let s = fs::read_to_string(path).unwrap();
    Plan::from_yaml(&s).unwrap();
}

#[test]
fn selection() {
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

    let new_aliases = new_alias_nodes(&mut plan, scan_id, &["b"], &Branch::Left).unwrap();
    let a_id = new_aliases.get(0).unwrap();
    let const_id = vec_alloc(
        &mut plan.nodes,
        Node::Expression(Expression::new_const(Value::number_from_str("10").unwrap())),
    );
    let gt_id = vec_alloc(
        &mut plan.nodes,
        Node::Expression(Expression::new_bool(*a_id, Bool::Gt, const_id)),
    );

    // Correct Selection operator
    Relational::new_select(&mut plan, scan_id, gt_id).unwrap();

    // Non-boolean filter
    assert_eq!(
        QueryPlannerError::InvalidBool,
        Relational::new_select(&mut plan, scan_id, const_id).unwrap_err()
    );

    // Non-relational child
    assert_eq!(
        QueryPlannerError::InvalidRow,
        Relational::new_select(&mut plan, const_id, gt_id).unwrap_err()
    );
}

#[test]
fn selection_serialize() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("selection.yaml");
    let s = fs::read_to_string(path).unwrap();
    Plan::from_yaml(&s).unwrap();
}

#[test]
fn union_all() {
    let mut plan = Plan::empty();

    let t1 = Table::new_seg(
        "t1",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t1);

    let scan_t1 = Relational::new_scan("t1", &mut plan).unwrap();
    let scan_t1_id = vec_alloc(&mut plan.nodes, Node::Relational(scan_t1));

    // Check fallback to random distribution
    let t2 = Table::new_seg(
        "t2",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
        ],
        &["b"],
    )
    .unwrap();
    plan.add_rel(t2);

    let scan_t2 = Relational::new_scan("t2", &mut plan).unwrap();
    let scan_t2_id = vec_alloc(&mut plan.nodes, Node::Relational(scan_t2));

    let union_all = Relational::new_union_all(&mut plan, scan_t1_id, scan_t2_id).unwrap();
    if let Node::Expression(row) = plan.get_node(union_all.output()).unwrap() {
        assert_eq!(Distribution::Random, *row.distribution().unwrap());
    } else {
        panic!("Invalid output!");
    }

    // Check preserving the original distribution
    let scan_t3 = Relational::new_scan("t1", &mut plan).unwrap();
    let scan_t3_id = vec_alloc(&mut plan.nodes, Node::Relational(scan_t3));

    let union_all = Relational::new_union_all(&mut plan, scan_t1_id, scan_t3_id).unwrap();
    if let Node::Expression(row) = plan.get_node(union_all.output()).unwrap() {
        assert_eq!(
            Distribution::Segment { key: vec![0] },
            *row.distribution().unwrap()
        );
    } else {
        panic!("Invalid output!");
    }

    // Check errors for children with different column names
    let t4 = Table::new_seg(
        "t4",
        vec![
            Column::new("c", Type::Boolean),
            Column::new("b", Type::Number),
        ],
        &["b"],
    )
    .unwrap();
    plan.add_rel(t4);

    let scan_t4 = Relational::new_scan("t4", &mut plan).unwrap();
    let scan_t4_id = vec_alloc(&mut plan.nodes, Node::Relational(scan_t4));
    assert_eq!(
        QueryPlannerError::NotEqualRows,
        Relational::new_union_all(&mut plan, scan_t4_id, scan_t1_id).unwrap_err()
    );

    // Check errors for children with different amount of column
    let t5 = Table::new_seg("t5", vec![Column::new("b", Type::Number)], &["b"]).unwrap();
    plan.add_rel(t5);

    let scan_t5 = Relational::new_scan("t5", &mut plan).unwrap();
    let scan_t5_id = vec_alloc(&mut plan.nodes, Node::Relational(scan_t5));
    assert_eq!(
        QueryPlannerError::NotEqualRows,
        Relational::new_union_all(&mut plan, scan_t5_id, scan_t1_id).unwrap_err()
    );
}

#[test]
fn union_all_serialize() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("union_all.yaml");
    let s = fs::read_to_string(path).unwrap();
    Plan::from_yaml(&s).unwrap();
}

#[test]
fn sub_query() {
    let mut plan = Plan::empty();

    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
        ],
        &["a"],
    )
    .unwrap();
    plan.add_rel(t);

    let scan = Relational::new_scan("t", &mut plan).unwrap();
    let scan_id = vec_alloc(&mut plan.nodes, Node::Relational(scan));

    Relational::new_sub_query(&mut plan, scan_id).unwrap();

    // Check non-relational child node error
    let a = 1;
    assert_eq!(
        QueryPlannerError::InvalidRow,
        Relational::new_sub_query(&mut plan, a).unwrap_err()
    );
}

#[test]
fn sub_query_serialize() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("sub_query.yaml");
    let s = fs::read_to_string(path).unwrap();
    Plan::from_yaml(&s).unwrap();
}

#[test]
fn output_alias_position_map() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("output_aliases.yaml");
    let s = fs::read_to_string(path).unwrap();
    let plan = Plan::from_yaml(&s).unwrap();

    let top = plan.nodes.get(plan.top.unwrap()).unwrap();
    if let Node::Relational(rel) = top {
        let col_map = rel.output_alias_position_map(&plan).unwrap();

        let expected_keys = vec!["a", "b"];
        assert_eq!(expected_keys.len(), col_map.len());
        expected_keys
            .iter()
            .zip(col_map.keys().sorted())
            .for_each(|(e, k)| assert_eq!(e, k));

        let expected_val = vec![0, 1];
        expected_val
            .iter()
            .zip(col_map.values().sorted())
            .for_each(|(e, v)| assert_eq!(e, v));
    } else {
        panic!("Plan top should be a relational operator!");
    }
}

#[test]
fn output_alias_position_map_duplicates() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("output_aliases_duplicates.yaml");
    let s = fs::read_to_string(path).unwrap();
    let plan = Plan::from_yaml(&s).unwrap();

    let top = plan.nodes.get(plan.top.unwrap()).unwrap();
    if let Node::Relational(rel) = top {
        assert_eq!(
            QueryPlannerError::InvalidPlan,
            rel.output_alias_position_map(&plan).unwrap_err()
        );
    } else {
        panic!("Plan top should be a relational operator!");
    }
}

#[test]
fn output_alias_position_map_unsupported_type() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("output_aliases_unsupported_type.yaml");
    let s = fs::read_to_string(path).unwrap();
    let plan = Plan::from_yaml(&s).unwrap();

    let top = plan.nodes.get(plan.top.unwrap()).unwrap();
    if let Node::Relational(rel) = top {
        assert_eq!(
            QueryPlannerError::InvalidPlan,
            rel.output_alias_position_map(&plan).unwrap_err()
        );
    } else {
        panic!("Plan top should be a relational operator!");
    }
}

#[test]
fn output_alias_oor() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("operator")
        .join("output_aliases_oor.yaml");
    let s = fs::read_to_string(path).unwrap();
    let plan = Plan::from_yaml(&s).unwrap();

    let top = plan.nodes.get(plan.top.unwrap()).unwrap();
    if let Node::Relational(rel) = top {
        assert_eq!(
            QueryPlannerError::ValueOutOfRange,
            rel.output_alias_position_map(&plan).unwrap_err()
        );
    } else {
        panic!("Plan top should be a relational operator!");
    }
}
