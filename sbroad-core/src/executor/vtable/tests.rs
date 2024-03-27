use super::*;
use crate::executor::engine::mock::RouterRuntimeMock;
use crate::ir::tests::column_integer_user_non_null;
use crate::ir::transformation::redistribution::{MotionKey, Target};
use pretty_assertions::assert_eq;
use std::collections::HashMap;

/// Test initialisation of the virtual table.
#[test]
fn virtual_table_1() {
    let mut vtable = VirtualTable::new();

    vtable.add_column(column_integer_user_non_null(SmolStr::from("name")));

    vtable.add_tuple(vec![Value::from(1_u64)]);

    vtable.set_alias("test").unwrap();

    let expected = VirtualTable {
        columns: vec![column_integer_user_non_null(SmolStr::from("name"))],
        tuples: vec![vec![Value::from(1_u64)]],
        name: Some(SmolStr::from("test")),
        primary_key: None,
        bucket_index: VTableIndex::new(),
    };

    assert_eq!(expected, vtable);
}

/// Test resharding.
#[test]
fn virtual_table_2() {
    let mut vtable = VirtualTable::new();
    vtable.add_column(column_integer_user_non_null(SmolStr::from("a")));
    vtable.add_column(column_integer_user_non_null(SmolStr::from("b")));
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
            column_integer_user_non_null(SmolStr::from("a")),
            column_integer_user_non_null(SmolStr::from("b")),
        ],
        tuples: vec![tuple1, tuple2],
        name: Some(SmolStr::from("t")),
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
    vtable.add_column(column_integer_user_non_null(SmolStr::from("a")));
    vtable.add_column(column_integer_user_non_null(SmolStr::from("b")));
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
            column_integer_user_non_null(SmolStr::from("a")),
            column_integer_user_non_null(SmolStr::from("b")),
        ],
        tuples: vec![tuple1, tuple2],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![1]),
        bucket_index: VTableIndex {
            value: HashMap::from_iter(vec![(1301, vec![1]), (3940, vec![0])]),
        },
    };

    assert_eq!(expected, vtable);
}

#[test]
fn vtable_rearrange_for_update() {
    let mut vtable = VirtualTable::new();

    // t: a (pk) b (shard key)
    let pk_value = Value::from(1_u64);
    let new_sh_key_value = Value::from(1_u64);
    let old_sh_key_value = Value::from(2_u64);
    let tuple = vec![
        pk_value.clone(),
        new_sh_key_value.clone(),
        old_sh_key_value.clone(),
    ];
    vtable.add_column(column_integer_user_non_null(SmolStr::from("a")));
    vtable.add_column(column_integer_user_non_null(SmolStr::from("b")));
    vtable.set_alias("t").unwrap();
    vtable.add_tuple(tuple);

    vtable.set_primary_key(&[0]).unwrap();

    let engine = RouterRuntimeMock::new();

    let old_shard_cols_len = 1;
    let new_shard_cols_positions: Vec<usize> = vec![1];
    vtable
        .rearrange_for_update(&engine, old_shard_cols_len, &new_shard_cols_positions)
        .unwrap();

    let mut expected_index = VTableIndex::new();
    let delete_tuple_bucket = engine.determine_bucket_id(&[&old_sh_key_value]).unwrap();
    let insert_tuple_bucket = engine.determine_bucket_id(&[&new_sh_key_value]).unwrap();
    expected_index.add_entry(delete_tuple_bucket, 1);
    expected_index.add_entry(insert_tuple_bucket, 0);

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("a")),
            column_integer_user_non_null(SmolStr::from("b")),
        ],
        tuples: vec![vec![pk_value.clone(), new_sh_key_value], vec![pk_value]],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: expected_index,
    };

    assert_eq!(expected, vtable);
}

#[test]
fn vtable_add_missing_from1() {
    let mut vtable = VirtualTable::new();

    // t: a (pk)
    vtable.add_column(column_integer_user_non_null(SmolStr::from("a")));
    vtable.add_column(column_integer_user_non_null(SmolStr::from("b")));
    vtable.set_alias("t").unwrap();
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(3_u64)]);
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(4_u64)]);
    vtable.add_tuple(vec![Value::from(2_u64), Value::from(5_u64)]);

    vtable.set_primary_key(&[0]).unwrap();

    let mut from_vtable = VirtualTable::new();
    from_vtable.add_column(column_integer_user_non_null(SmolStr::from("a")));
    from_vtable.set_alias("s").unwrap();
    from_vtable.add_tuple(vec![Value::from(1_u64)]);
    from_vtable.add_tuple(vec![Value::from(1_u64)]);
    from_vtable.add_tuple(vec![Value::from(3_u64)]);

    vtable.add_missing_rows(from_vtable).unwrap();

    let expected_index = VTableIndex::new();

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("a")),
            column_integer_user_non_null(SmolStr::from("b")),
        ],
        tuples: vec![
            vec![Value::from(1_u64), Value::from(3_u64)],
            vec![Value::from(1_u64), Value::from(4_u64)],
            vec![Value::from(2_u64), Value::from(5_u64)],
            vec![Value::from(3_u64), Value::Null],
        ],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: expected_index,
    };

    assert_eq!(expected, vtable);
}

#[test]
fn vtable_add_missing_from2() {
    let mut vtable = VirtualTable::new();

    // t: a (pk)
    vtable.add_column(column_integer_user_non_null(SmolStr::from("a")));
    vtable.add_column(column_integer_user_non_null(SmolStr::from("b")));
    vtable.set_alias("t").unwrap();
    vtable.set_alias("t").unwrap();
    vtable.add_tuple(vec![Value::from(1_u64), Value::from(3_u64)]);
    vtable.add_tuple(vec![Value::from(2_u64), Value::from(4_u64)]);

    vtable.set_primary_key(&[0]).unwrap();

    let mut from_vtable = VirtualTable::new();
    from_vtable.add_column(column_integer_user_non_null(SmolStr::from("a")));
    from_vtable.set_alias("s").unwrap();
    from_vtable.add_tuple(vec![Value::from(1_u64)]);
    from_vtable.add_tuple(vec![Value::from(2_u64)]);
    from_vtable.add_tuple(vec![Value::from(3_u64)]);
    from_vtable.add_tuple(vec![Value::from(3_u64)]);
    from_vtable.add_tuple(vec![Value::from(3_u64)]);

    vtable.add_missing_rows(from_vtable).unwrap();

    let expected_index = VTableIndex::new();

    let expected = VirtualTable {
        columns: vec![
            column_integer_user_non_null(SmolStr::from("a")),
            column_integer_user_non_null(SmolStr::from("b")),
        ],
        tuples: vec![
            vec![Value::from(1_u64), Value::from(3_u64)],
            vec![Value::from(2_u64), Value::from(4_u64)],
            vec![Value::from(3_u64), Value::Null],
            vec![Value::from(3_u64), Value::Null],
            vec![Value::from(3_u64), Value::Null],
        ],
        name: Some(SmolStr::from("t")),
        primary_key: Some(vec![0]),
        bucket_index: expected_index,
    };

    assert_eq!(expected, vtable);
}
