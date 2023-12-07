use std::fs;
use std::path::Path;

use pretty_assertions::{assert_eq, assert_ne};

use super::*;
use crate::ir::tests::column_user_non_null;

#[test]
fn column() {
    let a = column_user_non_null(String::from("a"), Type::Boolean);
    assert_eq!(a, column_user_non_null(String::from("a"), Type::Boolean));
    assert_ne!(a, column_user_non_null(String::from("a"), Type::String));
    assert_ne!(a, column_user_non_null(String::from("b"), Type::Boolean));
}

#[test]
fn table_seg() {
    let t = Table::new_sharded(
        "t",
        vec![
            column_user_non_null(String::from("a"), Type::Boolean),
            column_user_non_null(String::from("b"), Type::Unsigned),
            column_user_non_null(String::from("c"), Type::String),
            column_user_non_null(String::from("d"), Type::String),
        ],
        &["b", "a"],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();

    let sk = t.get_sk().unwrap();
    assert_eq!(2, sk.len());
    assert_eq!(0, sk[1]);
    assert_eq!(1, sk[0]);
}

#[test]
fn table_seg_name() {
    let t = Table::new_sharded(
        "t",
        vec![column_user_non_null(String::from("a"), Type::Boolean)],
        &["a"],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    assert_eq!("t", t.name());
}

#[test]
fn table_seg_duplicate_columns() {
    assert_eq!(
        Table::new_sharded(
            "t",
            vec![
                column_user_non_null(String::from("a"), Type::Boolean),
                column_user_non_null(String::from("b"), Type::Unsigned),
                column_user_non_null(String::from("c"), Type::String),
                column_user_non_null(String::from("a"), Type::String),
            ],
            &["b", "a"],
            &["b", "a"],
            SpaceEngine::Memtx,
        )
        .unwrap_err(),
        SbroadError::DuplicatedValue(format!(
            r#"Table "t" has a duplicating column "a" at positions 0 and 3"#,
        ))
    );
}

#[test]
fn table_seg_dno_bucket_id_column() {
    let t1 = Table::new_sharded(
        "t",
        vec![
            column_user_non_null(String::from("a"), Type::Boolean),
            column_user_non_null(String::from("b"), Type::Unsigned),
            column_user_non_null(String::from("c"), Type::String),
        ],
        &["b", "a"],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();

    assert_eq!(
        SbroadError::UnexpectedNumberOfValues(format!("Table {} has no bucket_id columns", "t")),
        t1.get_bucket_id_position().unwrap_err()
    );

    let t2 = Table::new_sharded(
        "t",
        vec![
            column_user_non_null(String::from("a"), Type::Boolean),
            column_user_non_null(String::from("b"), Type::Unsigned),
            column_user_non_null(String::from("c"), Type::String),
            Column::new("bucket_id", Type::String, ColumnRole::Sharding, false),
            Column::new("bucket_id2", Type::String, ColumnRole::Sharding, false),
        ],
        &["b", "a"],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();

    assert_eq!(
        SbroadError::UnexpectedNumberOfValues("Table has more than one bucket_id column".into()),
        t2.get_bucket_id_position().unwrap_err()
    );
}

#[test]
fn table_seg_wrong_key() {
    assert_eq!(
        Table::new_sharded(
            "t",
            vec![
                column_user_non_null(String::from("a"), Type::Boolean),
                column_user_non_null(String::from("b"), Type::Unsigned),
                column_user_non_null(String::from("c"), Type::String),
                column_user_non_null(String::from("d"), Type::String),
            ],
            &["a", "e"],
            &["a"],
            SpaceEngine::Memtx,
        )
        .unwrap_err(),
        SbroadError::Invalid(Entity::ShardingKey, None)
    );
}

#[test]
fn table_seg_compound_type_in_key() {
    assert_eq!(
        Table::new_sharded(
            "t",
            vec![
                Column::new("bucket_id", Type::Unsigned, ColumnRole::Sharding, false),
                column_user_non_null(String::from("a"), Type::Array),
            ],
            &["a"],
            &["a"],
            SpaceEngine::Memtx,
        )
        .unwrap_err(),
        SbroadError::Invalid(
            Entity::Column,
            Some("column a at position 1 is not scalar".into()),
        )
    );
}

#[test]
fn table_seg_serialized() {
    let t = Table::new_sharded(
        "t",
        vec![
            column_user_non_null(String::from("a"), Type::Boolean),
            column_user_non_null(String::from("b"), Type::Number),
            column_user_non_null(String::from("c"), Type::String),
            column_user_non_null(String::from("d"), Type::String),
            column_user_non_null(String::from("e"), Type::Integer),
            column_user_non_null(String::from("f"), Type::Unsigned),
        ],
        &["a", "d"],
        &["a", "d"],
        SpaceEngine::Memtx,
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
        SbroadError::DuplicatedValue(
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
        SbroadError::Invalid(
            Entity::Value,
            Some("key positions must be less than 1".into())
        )
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
    assert_eq!(t.unwrap_err(), SbroadError::FailedTo(
        Action::Serialize,
        Some(Entity::Table),
        "Message(\"invalid type: unit value, expected struct Key\", Some(Pos { marker: Marker { index: 121, line: 10, col: 18 }, path: \"kind.ShardedSpace.sharding_key\" }))".into()),
    );
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
        SbroadError::FailedTo(
            Action::Serialize,
            Some(Entity::Table),
            "Message(\"invalid type: unit value, expected a sequence\", Some(Pos { marker: Marker { index: 9, line: 1, col: 9 }, path: \"columns\" }))".into()
        )
    );
}

#[test]
fn column_msgpack_serialize() {
    let c = column_user_non_null(String::from("name"), Type::Boolean);

    assert_eq!(
        vec![
            0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA7, 0x62, 0x6F, 0x6F, 0x6C, 0x65, 0x61, 0x6E, 0xA4, 0x72, 0x6F, 0x6C,
            0x65, 0xA4, 0x75, 0x73, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = column_user_non_null(String::from("name"), Type::String);

    assert_eq!(
        vec![
            0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA6, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0xA4, 0x72, 0x6F, 0x6C, 0x65,
            0xA4, 0x75, 0x73, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = column_user_non_null(String::from("name"), Type::Integer);

    assert_eq!(
        vec![
            0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA7, 0x69, 0x6E, 0x74, 0x65, 0x67, 0x65, 0x72, 0xA4, 0x72, 0x6F, 0x6C,
            0x65, 0xA4, 0x75, 0x73, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = column_user_non_null(String::from("name"), Type::Unsigned);

    assert_eq!(
        vec![
            0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA8, 0x75, 0x6E, 0x73, 0x69, 0x67, 0x6E, 0x65, 0x64, 0xA4, 0x72, 0x6F,
            0x6C, 0x65, 0xA4, 0x75, 0x73, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = column_user_non_null(String::from("name"), Type::Number);

    assert_eq!(
        vec![
            0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA6, 0x6E, 0x75, 0x6D, 0x62, 0x65, 0x72, 0xA4, 0x72, 0x6F, 0x6C, 0x65,
            0xA4, 0x75, 0x73, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );
}

#[test]
fn column_msgpack_deserialize() {
    let c = column_user_non_null(String::from("name"), Type::Boolean);

    let expected_msgpack = vec![
        0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79, 0x70,
        0x65, 0xA7, 0x62, 0x6F, 0x6F, 0x6C, 0x65, 0x61, 0x6E, 0xA4, 0x72, 0x6F, 0x6C, 0x65, 0xA4,
        0x75, 0x73, 0x65, 0x72,
    ];

    assert_eq!(expected_msgpack, rmp_serde::to_vec(&c).unwrap());

    assert_eq!(
        rmp_serde::from_slice::<Column>(expected_msgpack.as_slice()).unwrap(),
        c
    );
}

#[test]
fn table_converting() {
    let t = Table::new_sharded(
        "t",
        vec![
            column_user_non_null(String::from("a"), Type::Boolean),
            column_user_non_null(String::from("b"), Type::Unsigned),
            column_user_non_null(String::from("c"), Type::String),
            column_user_non_null(String::from("d"), Type::String),
        ],
        &["b", "a"],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();

    let s = serde_yaml::to_string(&t).unwrap();
    assert_eq!(t, Table::seg_from_yaml(&s).unwrap());
}
