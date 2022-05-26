use super::*;
use crate::ir::relation::{ColumnRole, Type};

#[test]
fn virtual_table() {
    let mut vtable = VirtualTable::new();

    vtable.add_column(Column {
        name: "name".into(),
        r#type: Type::Integer,
        role: ColumnRole::User,
    });

    vtable.add_values_tuple(vec![Value::number_from_str("1").unwrap()]);

    vtable.set_alias("test").unwrap();

    let expected = VirtualTable {
        columns: vec![Column {
            name: "name".into(),
            r#type: Type::Integer,
            role: ColumnRole::User,
        }],
        tuples: vec![vec![Value::number_from_str("1").unwrap()]],
        name: Some(String::from("test")),
        distribution_key: None,
        index: HashMap::with_hasher(RandomState::new()),
    };

    assert_eq!(expected, vtable)
}
