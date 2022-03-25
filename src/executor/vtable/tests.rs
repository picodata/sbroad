use crate::ir::relation::Type;

use super::*;

#[test]
fn virtual_table() {
    let mut vtable = VirtualTable::new();

    vtable.add_column(Column {
        name: "name".into(),
        r#type: Type::Integer,
    });

    vtable.add_values_tuple(vec![Value::number_from_str("1").unwrap()]);

    vtable.set_alias("test").unwrap();

    let expected = VirtualTable {
        columns: vec![Column {
            name: "name".into(),
            r#type: Type::Integer,
        }],
        tuples: vec![vec![Value::number_from_str("1").unwrap()]],
        name: Some(String::from("test")),
        hashing: HashMap::new(),
    };

    assert_eq!(expected, vtable)
}
