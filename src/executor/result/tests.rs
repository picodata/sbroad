use pretty_assertions::{assert_eq, assert_ne};

use crate::ir::relation::Type;

use super::*;

#[test]
fn box_execute_result_serialize() {
    let r = BoxExecuteFormat {
        metadata: vec![
            Column {
                name: "id".into(),
                r#type: Type::Integer,
            },
            Column {
                name: "name".into(),
                r#type: Type::String,
            },
            Column {
                name: "count".into(),
                r#type: Type::Unsigned,
            },
        ],
        rows: vec![vec![
            Value::Integer(1),
            Value::String("тест".into()),
            Value::Unsigned(1),
        ]],
    };

    let actual = rmp_serde::to_vec(&r).unwrap();

    // Incorrect serialize message with default msgpack serializer
    let default_serialize_msg = vec![
        0x92, 0x93, 0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA2, 0x69, 0x64, 0xA4, 0x74, 0x79, 0x70,
        0x65, 0xA7, 0x69, 0x6E, 0x74, 0x65, 0x67, 0x65, 0x72, 0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65,
        0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x74, 0x79, 0x70, 0x65, 0xA6, 0x73, 0x74, 0x72, 0x69,
        0x6E, 0x67, 0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA5, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0xA4,
        0x74, 0x79, 0x70, 0x65, 0xA8, 0x75, 0x6E, 0x73, 0x69, 0x67, 0x6E, 0x65, 0x64, 0x91, 0x93,
        0x81, 0x2, 0x1, 0x81, 0x3, 0xA8, 0xD1, 0x82, 0xD0, 0xB5, 0xD1, 0x81, 0xD1, 0x82, 0x81, 0x4,
        0x1,
    ];
    assert_ne!(default_serialize_msg, actual);

    // Expected value was got in debugging interaction lua app and current rust lib
    let correct_serialize_msg_expected = vec![
        0x82, 0xA8, 0x6D, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x93, 0x82, 0xA4, 0x6E, 0x61,
        0x6D, 0x65, 0xA2, 0x69, 0x64, 0xA4, 0x74, 0x79, 0x70, 0x65, 0xA7, 0x69, 0x6E, 0x74, 0x65,
        0x67, 0x65, 0x72, 0x82, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4, 0x6E, 0x61, 0x6D, 0x65, 0xA4,
        0x74, 0x79, 0x70, 0x65, 0xA6, 0x73, 0x74, 0x72, 0x69, 0x6E, 0x67, 0x82, 0xA4, 0x6E, 0x61,
        0x6D, 0x65, 0xA5, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0xA4, 0x74, 0x79, 0x70, 0x65, 0xA8, 0x75,
        0x6E, 0x73, 0x69, 0x67, 0x6E, 0x65, 0x64, 0xA4, 0x72, 0x6F, 0x77, 0x73, 0x91, 0x93, 0x1,
        0xA8, 0xD1, 0x82, 0xD0, 0xB5, 0xD1, 0x81, 0xD1, 0x82, 0x1,
    ];
    assert_eq!(correct_serialize_msg_expected, actual);
}

#[test]
fn convert_to_vtable() {
    let r = BoxExecuteFormat {
        metadata: vec![
            Column {
                name: "id".into(),
                r#type: Type::Integer,
            },
            Column {
                name: "name".into(),
                r#type: Type::String,
            },
            Column {
                name: "count".into(),
                r#type: Type::Unsigned,
            },
            Column {
                name: "price".into(),
                r#type: Type::Number,
            },
        ],
        rows: vec![
            vec![
                Value::Integer(1),
                Value::String("тест".into()),
                Value::Unsigned(1),
                Value::Number(1.5),
            ],
            vec![
                Value::Integer(2),
                Value::String("тест2".into()),
                Value::Unsigned(5),
                Value::Number(2.0),
            ],
        ],
    };

    let mut excepted = VirtualTable::new("test_vtable");

    excepted.add_column(Column {
        name: "id".into(),
        r#type: Type::Integer,
    });
    excepted.add_column(Column {
        name: "name".into(),
        r#type: Type::String,
    });
    excepted.add_column(Column {
        name: "count".into(),
        r#type: Type::Unsigned,
    });

    excepted.add_column(Column {
        name: "price".into(),
        r#type: Type::Number,
    });

    excepted.add_values_tuple(vec![
        IrValue::number_from_str("1").unwrap(),
        IrValue::string_from_str("тест"),
        IrValue::number_from_str("1").unwrap(),
        IrValue::number_from_str("1.5").unwrap(),
    ]);

    excepted.add_values_tuple(vec![
        IrValue::number_from_str("2").unwrap(),
        IrValue::string_from_str("тест2"),
        IrValue::number_from_str("5").unwrap(),
        IrValue::number_from_str("2.0").unwrap(),
    ]);

    assert_eq!(excepted, r.as_virtual_table("test_vtable").unwrap());
}
