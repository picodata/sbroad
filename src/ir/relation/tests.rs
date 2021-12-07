use super::*;
use pretty_assertions::{assert_eq, assert_ne};
use std::fs;
use std::path::Path;

#[test]
fn column() {
    let a = Column {
        name: String::from("a"),
        type_name: Type::Boolean,
    };
    assert_eq!(a, Column::new("a", Type::Boolean));
    assert_ne!(a, Column::new("a", Type::String));
    assert_ne!(a, Column::new("b", Type::Boolean));
}

#[test]
fn table_seg() {
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
    if let Table::Segment { key, .. } = &t {
        assert_eq!(2, key.len());
        assert_eq!(0, key[1]);
        assert_eq!(1, key[0]);
    }
}

#[test]
fn table_seg_name() {
    let t = Table::new_seg("t", vec![Column::new("a", Type::Boolean)], &["a"]).unwrap();
    assert_eq!("t", t.name());
}

#[test]
fn table_seg_duplicate_columns() {
    assert_eq!(
        Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean),
                Column::new("b", Type::Number),
                Column::new("c", Type::String),
                Column::new("a", Type::String),
            ],
            &["b", "a"],
        )
        .unwrap_err(),
        QueryPlannerError::DuplicateColumn
    );
}

#[test]
fn table_seg_wrong_key() {
    assert_eq!(
        Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean),
                Column::new("b", Type::Number),
                Column::new("c", Type::String),
                Column::new("d", Type::String),
            ],
            &["a", "e"],
        )
        .unwrap_err(),
        QueryPlannerError::InvalidShardingKey
    );
}

#[test]
fn table_seg_serialized() {
    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean),
            Column::new("b", Type::Number),
            Column::new("c", Type::String),
            Column::new("d", Type::String),
        ],
        &["a", "d"],
    )
    .unwrap();
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("relation")
        .join("table_seg_serialized.yaml");
    let s = fs::read_to_string(path).unwrap();
    let t_yaml = Table::seg_from_yaml(&s).unwrap();
    assert_eq!(t, t_yaml);
}

#[test]
fn table_seg_serialized_duplicate_columns() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("relation")
        .join("table_seg_serialized_duplicate_columns.yaml");
    let s = fs::read_to_string(path).unwrap();
    assert_eq!(
        Table::seg_from_yaml(&s).unwrap_err(),
        QueryPlannerError::DuplicateColumn
    );
}

#[test]
fn table_seg_serialized_out_of_range_key() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("relation")
        .join("table_seg_serialized_out_of_range_key.yaml");
    let s = fs::read_to_string(path).unwrap();
    assert_eq!(
        Table::seg_from_yaml(&s).unwrap_err(),
        QueryPlannerError::ValueOutOfRange
    );
}

#[test]
fn table_seg_serialized_no_key() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("relation")
        .join("table_seg_serialized_no_key.yaml");
    let s = fs::read_to_string(path).unwrap();
    let t = Table::seg_from_yaml(&s);
    assert_eq!(t.unwrap_err(), QueryPlannerError::Serialization);
}

#[test]
fn table_seg_serialized_no_columns() {
    let path = Path::new("")
        .join("tests")
        .join("artifactory")
        .join("ir")
        .join("relation")
        .join("table_seg_serialized_no_columns.yaml");
    let s = fs::read_to_string(path).unwrap();
    assert_eq!(
        Table::seg_from_yaml(&s).unwrap_err(),
        QueryPlannerError::Serialization
    );
}
