use std::fs;
use std::path::Path;

use pretty_assertions::{assert_eq, assert_ne};

use super::*;

#[test]
fn column() {
    let a = Column {
        name: String::from("a"),
        r#type: Type::Boolean,
        is_system: false,
    };
    assert_eq!(a, Column::new("a", Type::Boolean, false));
    assert_ne!(a, Column::new("a", Type::String, false));
    assert_ne!(a, Column::new("b", Type::Boolean, false));
}

#[test]
fn table_seg() {
    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, false),
            Column::new("b", Type::Number, false),
            Column::new("c", Type::String, false),
            Column::new("d", Type::String, false),
        ],
        &["b", "a"],
    )
    .unwrap();

    assert_eq!(2, t.key.positions.len());
    assert_eq!(0, t.key.positions[1]);
    assert_eq!(1, t.key.positions[0]);
}

#[test]
fn table_seg_name() {
    let t = Table::new_seg("t", vec![Column::new("a", Type::Boolean, false)], &["a"]).unwrap();
    assert_eq!("t", t.name());
}

#[test]
fn table_seg_duplicate_columns() {
    assert_eq!(
        Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean, false),
                Column::new("b", Type::Number, false),
                Column::new("c", Type::String, false),
                Column::new("a", Type::String, false),
            ],
            &["b", "a"],
        )
        .unwrap_err(),
        QueryPlannerError::CustomError(
            "Table has duplicated columns and couldn't be loaded".into()
        )
    );
}

#[test]
fn table_seg_wrong_key() {
    assert_eq!(
        Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean, false),
                Column::new("b", Type::Number, false),
                Column::new("c", Type::String, false),
                Column::new("d", Type::String, false),
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
            Column::new("a", Type::Boolean, false),
            Column::new("b", Type::Number, false),
            Column::new("c", Type::String, false),
            Column::new("d", Type::String, false),
            Column::new("e", Type::Integer, false),
            Column::new("f", Type::Unsigned, false),
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
        QueryPlannerError::CustomError(
            "Table contains duplicate columns. Unable to convert to YAML.".into()
        )
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

#[test]
fn column_msgpack_serialize() {
    let c = Column {
        name: "name".into(),
        r#type: Type::Boolean,
        is_system: false,
    };

    assert_eq!(
        vec![
            0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA7, 0x62, 0x6F, 0x6F, 0x6C, 0x65, 0x61, 0x6E,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = Column {
        name: "name".into(),
        r#type: Type::String,
        is_system: false,
    };

    assert_eq!(
        vec![
            0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA6, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = Column {
        name: "name".into(),
        r#type: Type::Integer,
        is_system: false,
    };

    assert_eq!(
        vec![
            0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA7, 0x69, 0x6E, 0x74, 0x65, 0x67, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = Column {
        name: "name".into(),
        r#type: Type::Unsigned,
        is_system: false,
    };

    assert_eq!(
        vec![
            0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA8, 0x75, 0x6E, 0x73, 0x69, 0x67, 0x6E, 0x65, 0x64,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = Column {
        name: "name".into(),
        r#type: Type::Number,
        is_system: false,
    };

    assert_eq!(
        vec![
            0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA6, 0x6E, 0x75, 0x6D, 0x62, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    )
}

#[test]
fn column_msgpack_deserialize() {
    let c = Column {
        name: "name".into(),
        r#type: Type::Boolean,
        is_system: false,
    };

    let expected_msgpack = vec![
        0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79, 0x70,
        0x65, 0xA7, 0x62, 0x6F, 0x6F, 0x6C, 0x65, 0x61, 0x6E,
    ];

    assert_eq!(expected_msgpack, rmp_serde::to_vec(&c).unwrap());

    assert_eq!(
        rmp_serde::from_slice::<Column>(expected_msgpack.as_slice()).unwrap(),
        c
    );
}

#[test]
fn table_converting() {
    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, false),
            Column::new("b", Type::Number, false),
            Column::new("c", Type::String, false),
            Column::new("d", Type::String, false),
        ],
        &["b", "a"],
    )
    .unwrap();

    let s = serde_yaml::to_string(&t).unwrap();
    assert_eq!(t, Table::seg_from_yaml(&s).unwrap());
}
