use std::fs;
use std::path::Path;

use pretty_assertions::{assert_eq, assert_ne};

use super::*;

#[test]
fn column() {
    let a = Column {
        name: String::from("a"),
        r#type: Type::Boolean,
        role: ColumnRole::User,
    };
    assert_eq!(a, Column::new("a", Type::Boolean, ColumnRole::User));
    assert_ne!(a, Column::new("a", Type::String, ColumnRole::User));
    assert_ne!(a, Column::new("b", Type::Boolean, ColumnRole::User));
}

#[test]
fn table_seg() {
    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Unsigned, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
            Column::new("d", Type::String, ColumnRole::User),
        ],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();

    assert_eq!(2, t.key.positions.len());
    assert_eq!(0, t.key.positions[1]);
    assert_eq!(1, t.key.positions[0]);
}

#[test]
fn table_seg_name() {
    let t = Table::new_seg(
        "t",
        vec![Column::new("a", Type::Boolean, ColumnRole::User)],
        &["a"],
        SpaceEngine::Memtx,
    )
    .unwrap();
    assert_eq!("t", t.name());
}

#[test]
fn table_seg_duplicate_columns() {
    assert_eq!(
        Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean, ColumnRole::User),
                Column::new("b", Type::Unsigned, ColumnRole::User),
                Column::new("c", Type::String, ColumnRole::User),
                Column::new("a", Type::String, ColumnRole::User),
            ],
            &["b", "a"],
            SpaceEngine::Memtx,
        )
        .unwrap_err(),
        SbroadError::DuplicatedValue("Table has duplicated columns and couldn't be loaded".into())
    );
}

#[test]
fn table_seg_dno_bucket_id_column() {
    let t1 = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Unsigned, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
        ],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();

    assert_eq!(
        SbroadError::UnexpectedNumberOfValues("Table has no bucket_id columns".into()),
        t1.get_bucket_id_position().unwrap_err()
    );

    let t2 = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Unsigned, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
            Column::new("bucket_id", Type::String, ColumnRole::Sharding),
            Column::new("bucket_id2", Type::String, ColumnRole::Sharding),
        ],
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
        Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean, ColumnRole::User),
                Column::new("b", Type::Unsigned, ColumnRole::User),
                Column::new("c", Type::String, ColumnRole::User),
                Column::new("d", Type::String, ColumnRole::User),
            ],
            &["a", "e"],
            SpaceEngine::Memtx,
        )
        .unwrap_err(),
        SbroadError::Invalid(Entity::ShardingKey, None)
    );
}

#[test]
fn table_seg_serialized() {
    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Number, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
            Column::new("d", Type::String, ColumnRole::User),
            Column::new("e", Type::Integer, ColumnRole::User),
            Column::new("f", Type::Unsigned, ColumnRole::User),
        ],
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
        "Message(\"invalid type: unit value, expected struct Key\", Some(Pos { marker: Marker { index: 52, line: 5, col: 5 }, path: \"key\" }))".into())
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
    let c = Column {
        name: "name".into(),
        r#type: Type::Boolean,
        role: ColumnRole::User,
    };

    assert_eq!(
        vec![
            0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA7, 0x62, 0x6F, 0x6F, 0x6C, 0x65, 0x61, 0x6E, 0xA4, 0x72, 0x6F, 0x6C,
            0x65, 0xA4, 0x75, 0x73, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = Column {
        name: "name".into(),
        r#type: Type::String,
        role: ColumnRole::User,
    };

    assert_eq!(
        vec![
            0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA6, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0xA4, 0x72, 0x6F, 0x6C, 0x65,
            0xA4, 0x75, 0x73, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = Column {
        name: "name".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    };

    assert_eq!(
        vec![
            0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA7, 0x69, 0x6E, 0x74, 0x65, 0x67, 0x65, 0x72, 0xA4, 0x72, 0x6F, 0x6C,
            0x65, 0xA4, 0x75, 0x73, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = Column {
        name: "name".into(),
        r#type: Type::Unsigned,
        role: ColumnRole::User,
    };

    assert_eq!(
        vec![
            0x83, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79,
            0x70, 0x65, 0xA8, 0x75, 0x6E, 0x73, 0x69, 0x67, 0x6E, 0x65, 0x64, 0xA4, 0x72, 0x6F,
            0x6C, 0x65, 0xA4, 0x75, 0x73, 0x65, 0x72,
        ],
        rmp_serde::to_vec(&c).unwrap()
    );

    let c = Column {
        name: "name".into(),
        r#type: Type::Number,
        role: ColumnRole::User,
    };

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
    let c = Column {
        name: "name".into(),
        r#type: Type::Boolean,
        role: ColumnRole::User,
    };

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
    let t = Table::new_seg(
        "t",
        vec![
            Column::new("a", Type::Boolean, ColumnRole::User),
            Column::new("b", Type::Unsigned, ColumnRole::User),
            Column::new("c", Type::String, ColumnRole::User),
            Column::new("d", Type::String, ColumnRole::User),
        ],
        &["b", "a"],
        SpaceEngine::Memtx,
    )
    .unwrap();

    let s = serde_yaml::to_string(&t).unwrap();
    assert_eq!(t, Table::seg_from_yaml(&s).unwrap());
}
