use super::*;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::ir::relation::{ColumnRole, Type};
use crate::ir::transformation::redistribution::{MotionKey, Target};
use pretty_assertions::assert_eq;
use std::collections::HashMap;

/// Test initialisation of the virtual table.
#[test]
fn virtual_table_1() {
    let mut vtable = VirtualTable::new();

    vtable.add_column(Column {
        name: "name".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });

    vtable.add_tuple(vec![Value::from(1_u64)]);

    vtable.set_alias("test").unwrap();

    let expected = VirtualTable {
        columns: vec![Column {
            name: "name".into(),
            r#type: Type::Integer,
            role: ColumnRole::User,
        }],
        tuples: vec![vec![Value::from(1_u64)]],
        name: Some(String::from("test")),
        primary_key: None,
        bucket_index: VTableIndex::new(),
    };

    assert_eq!(expected, vtable);
}

/// Test resharding.
#[test]
fn virtual_table_2() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    let tuple1 = vec![Value::from(1_u64), Value::from(2_u64)];
    let tuple2 = vec![Value::from(3_u64), Value::from(4_u64)];
    vtable.add_tuple(tuple1.clone());
    vtable.add_tuple(tuple2.clone());
    vtable.set_alias("t").unwrap();

    let engine = RouterRuntimeMock::new();
    let key = MotionKey {
        targets: vec![Target::Reference(0)],
    };
    vtable.reshard(&key, &engine).unwrap();

    let expected = VirtualTable {
        columns: vec![
            Column {
                name: "a".into(),
                r#type: Type::Integer,
                role: ColumnRole::User,
            },
            Column {
                name: "b".into(),
                r#type: Type::Integer,
                role: ColumnRole::User,
            },
        ],
        tuples: vec![tuple1, tuple2],
        name: Some(String::from("t")),
        primary_key: None,
        bucket_index: VTableIndex {
            value: HashMap::from_iter(vec![(1301, vec![1]), (3940, vec![0])]),
        },
    };

    assert_eq!(expected, vtable);
}

/// Test resharding virtual table with a primary key.
#[test]
fn virtual_table_3() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    let tuple1 = vec![Value::from(1_u64), Value::from(2_u64)];
    let tuple2 = vec![Value::from(3_u64), Value::from(4_u64)];
    vtable.add_tuple(tuple1.clone());
    vtable.add_tuple(tuple2.clone());
    vtable.set_alias("t").unwrap();
    vtable.set_primary_key(&[1]).unwrap();

    let engine = RouterRuntimeMock::new();
    let key = MotionKey {
        targets: vec![Target::Reference(0)],
    };
    vtable.reshard(&key, &engine).unwrap();

    let expected = VirtualTable {
        columns: vec![
            Column {
                name: "a".into(),
                r#type: Type::Integer,
                role: ColumnRole::User,
            },
            Column {
                name: "b".into(),
                r#type: Type::Integer,
                role: ColumnRole::User,
            },
        ],
        tuples: vec![tuple1, tuple2],
        name: Some(String::from("t")),
        primary_key: Some(vec![1]),
        bucket_index: VTableIndex {
            value: HashMap::from_iter(vec![(1301, vec![1]), (3940, vec![0])]),
        },
    };

    assert_eq!(expected, vtable);
}

/// Test projecting a virtual table:
/// - no duplicating columns in projection
/// - no primary key
#[test]
fn virtual_table_4() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    let tuple1 = vec![Value::from(1_u64), Value::from(2_u64)];
    let tuple2 = vec![Value::from(3_u64), Value::from(4_u64)];
    vtable.add_tuple(tuple1);
    vtable.add_tuple(tuple2);
    vtable.set_alias("t").unwrap();
    vtable.project(&[1]).unwrap();

    let tuple3 = vec![Value::from(2_u64)];
    let tuple4 = vec![Value::from(4_u64)];
    let expected = VirtualTable {
        columns: vec![Column {
            name: "b".into(),
            r#type: Type::Integer,
            role: ColumnRole::User,
        }],
        tuples: vec![tuple3, tuple4],
        name: Some(String::from("t")),
        primary_key: None,
        bucket_index: VTableIndex::new(),
    };

    assert_eq!(expected, vtable);
}

/// Test projecting a virtual table:
/// - there are some duplicating columns in projection
/// - no primary key
#[test]
fn virtual_table_5() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    let tuple1 = vec![Value::from(1_u64), Value::from(2_u64)];
    let tuple2 = vec![Value::from(3_u64), Value::from(4_u64)];
    vtable.add_tuple(tuple1);
    vtable.add_tuple(tuple2);
    vtable.set_alias("t").unwrap();
    vtable.project(&[0, 0]).unwrap();

    let tuple3 = vec![Value::from(1_u64), Value::from(1_u64)];
    let tuple4 = vec![Value::from(3_u64), Value::from(3_u64)];
    let expected = VirtualTable {
        columns: vec![
            Column {
                name: "a".into(),
                r#type: Type::Integer,
                role: ColumnRole::User,
            },
            Column {
                name: "a".into(),
                r#type: Type::Integer,
                role: ColumnRole::User,
            },
        ],
        tuples: vec![tuple3, tuple4],
        name: Some(String::from("t")),
        primary_key: None,
        bucket_index: VTableIndex::new(),
    };

    assert_eq!(expected, vtable);
}

/// Test projecting a virtual table:
/// - no duplicating columns in projection
/// - primary key changes column order after projection
#[test]
fn virtual_table_6() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    let tuple1 = vec![Value::from(1_u64), Value::from(2_u64)];
    let tuple2 = vec![Value::from(3_u64), Value::from(4_u64)];
    vtable.add_tuple(tuple1);
    vtable.add_tuple(tuple2);
    vtable.set_alias("t").unwrap();
    vtable.set_primary_key(&[1]).unwrap();
    vtable.project(&[1]).unwrap();

    let tuple3 = vec![Value::from(2_u64)];
    let tuple4 = vec![Value::from(4_u64)];
    let expected = VirtualTable {
        columns: vec![Column {
            name: "b".into(),
            r#type: Type::Integer,
            role: ColumnRole::User,
        }],
        tuples: vec![tuple3, tuple4],
        name: Some(String::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: VTableIndex::new(),
    };

    assert_eq!(expected, vtable);
}

/// Test projecting a virtual table:
/// - some duplicating columns in projection
/// - primary key is one of duplicated columns
#[test]
fn virtual_table_7() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    let tuple1 = vec![Value::from(1_u64), Value::from(2_u64)];
    let tuple2 = vec![Value::from(3_u64), Value::from(4_u64)];
    vtable.add_tuple(tuple1);
    vtable.add_tuple(tuple2);
    vtable.set_alias("t").unwrap();
    vtable.set_primary_key(&[1]).unwrap();
    vtable.project(&[1, 1, 0]).unwrap();

    let tuple3 = vec![Value::from(2_u64), Value::from(2_u64), Value::from(1_u64)];
    let tuple4 = vec![Value::from(4_u64), Value::from(4_u64), Value::from(3_u64)];
    let expected = VirtualTable {
        columns: vec![
            Column {
                name: "b".into(),
                r#type: Type::Integer,
                role: ColumnRole::User,
            },
            Column {
                name: "b".into(),
                r#type: Type::Integer,
                role: ColumnRole::User,
            },
            Column {
                name: "a".into(),
                r#type: Type::Integer,
                role: ColumnRole::User,
            },
        ],
        tuples: vec![tuple3, tuple4],
        name: Some(String::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: VTableIndex::new(),
    };

    assert_eq!(expected, vtable);
}

/// Test projecting a virtual table:
/// - primary key is not in projection
#[test]
fn virtual_table_8() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.add_column(Column {
        name: "b".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.set_alias("t").unwrap();
    vtable.set_primary_key(&[1]).unwrap();
    let err_msg = vtable.project(&[0]).unwrap_err().to_string();
    assert_eq!(
        err_msg.contains("not present in the projection column"),
        true
    );
}

/// Test projecting a virtual table:
/// - projection points to a column that does not exist
#[test]
fn virtual_table_9() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.set_alias("t").unwrap();
    let err_msg = vtable.project(&[1]).unwrap_err().to_string();
    assert_eq!(err_msg.contains("invalid position 1"), true);
}

/// Test setting a primary key in a virtual table:
/// - primary key points to a column that does not exist
#[test]
fn virtual_table_10() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(Column {
        name: "a".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });
    vtable.set_alias("t").unwrap();
    let err_msg = vtable.set_primary_key(&[1]).unwrap_err().to_string();
    assert_eq!(err_msg.contains("invalid position 1"), true);
}
