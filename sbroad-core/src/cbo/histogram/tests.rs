use crate::cbo::histogram::scale_values;
use crate::ir::value::double::Double;
use crate::ir::value::Value;
use tarantool::decimal;

#[test]
fn values_scale() {
    // Numerical
    assert!(
        (0.5 - scale_values(
            &Value::Decimal(decimal!(1)),
            &Value::Decimal(decimal!(0)),
            &Value::Decimal(decimal!(2))
        )
        .unwrap())
            < f64::EPSILON
    );

    assert!(
        (0.1 - scale_values(
            &Value::Decimal(decimal!(1)),
            &Value::Decimal(decimal!(0)),
            &Value::Integer(10)
        )
        .unwrap())
            < f64::EPSILON
    );

    assert!(
        (0.0 - scale_values(
            &Value::Decimal(decimal!(1)),
            &Value::Double(Double::from(1.0)),
            &Value::Unsigned(3)
        )
        .unwrap())
            < f64::EPSILON
    );

    assert!(
        (0.3333333333
            - scale_values(
                &Value::Decimal(decimal!(1)),
                &Value::Double(Double::from(0.0)),
                &Value::Decimal(decimal!(3))
            )
            .unwrap())
            < f64::EPSILON
    );

    assert_eq!(
        String::from("invalid value: Scaling may be done only String to String or Numerical to Numerical. Values Integer(1), Boolean(true) and Integer(1) were passed"),
        scale_values(&Value::Integer(1), &Value::Boolean(true), &Value::Integer(1)).unwrap_err().to_string()
    );

    assert_eq!(
        String::from("invalid value: Scaling may be done only String to String or Numerical to Numerical. Values Integer(1), Decimal(Decimal { inner: 1 }) and String(\"1\") were passed"),
        scale_values(&Value::Integer(1), &Value::Decimal(decimal!(1)), &Value::from("1")).unwrap_err().to_string()
    );

    // Strings
    // One of the passed strings is empty.
    assert_eq!(
        "invalid value: One of the passed strings is empty",
        scale_values(&Value::from(""), &Value::from(""), &Value::from(""))
            .unwrap_err()
            .to_string()
    );

    assert_eq!(
        "invalid value: One of the passed strings is empty",
        scale_values(&Value::from(""), &Value::from("a"), &Value::from("b"))
            .unwrap_err()
            .to_string()
    );

    // One-letter.
    //
    // Logic:
    // 1.) As soon as only lower case letters are used
    // 1.1) low_bound  =  'a' <-> 97
    // 1.2) high_bound =  'z' <-> 122
    // 2.) No chars are removed as common prefix.
    // 3.) Conversion process is made on assumption that base = 26
    // 3.1) value_f64       = (98 - 97) / 26 = 0.038461538461538464
    // 3.2) left_bound_f64  = (97 - 97) / 26 = 0.0
    // 3.3) right_bound_f64 = (99 - 97) / 26 = 0.07692307692307693
    // 4.) result = (0.038461538461538464) / (0.07692307692307693) = 0.5
    assert!(
        (0.5 - scale_values(&Value::from("b"), &Value::from("a"), &Value::from("c")).unwrap())
            < f64::EPSILON
    );

    // Two-letter.
    //
    // Logic:
    // <The same calculations as in the example above>.
    assert!(
        (0.5 - scale_values(&Value::from("ba"), &Value::from("aa"), &Value::from("ca")).unwrap())
            < f64::EPSILON
    );

    // Telephone-number.
    //
    // Logic:
    // 1.) As soon as only numbers are used
    // 1.1) low_bound  =  '0' <-> 48
    // 1.2) high_bound =  '9' <-> 57
    // 2.) First 4 common chars are removed as common prefix.
    // 3.) Conversion process is made on assumption that base = 10
    // 3.1) value_f64       = (49 - 48) / 10 + (53 - 48) / 100 = 0.15000000000000002
    // 3.2) left_bound_f64  = (49 - 48) / 10 = 0.10000000000000001
    // 3.3) right_bound_f64 = (50 - 48) / 10 = 0.20000000000000001
    // 4.) result = (0.05000000000000001) / (0.1) =~ 0.5
    assert!(
        (0.5000000000000001
            - scale_values(
                &Value::from("891215"), // "15" after prefix removal.
                &Value::from("891210"), // "10" after prefix removal.
                &Value::from("891220")  // "20" after prefix removal.
            )
            .unwrap())
            < f64::EPSILON
    );

    // ASCII chars that made the base equal to odd number.
    //
    // Logic:
    // 1.) As soon as only lower case letters are used
    // 1.1) low_bound  =  '/' <-> 47
    // 1.2) high_bound =  '9' <-> 57
    // 2.) No chars are removed as common prefix.
    // 3.) Conversion process is made on assumption that base = 11
    // 3.1) value_f64       = (52 - 47) / 11 = 0.4545454545454546
    // 3.2) left_bound_f64  = (47 - 47) / 11 = 0.0
    // 3.3) right_bound_f64 = (57 - 47) / 11 = 0.9090909090909092
    // 4.) result = (0.4545454545454546) / (0.9090909090909092) = 0.5
    assert!(
        (0.5 - scale_values(&Value::from("4"), &Value::from("/"), &Value::from("9")).unwrap())
            < f64::EPSILON
    );

    // Non-ASCII, russian chars.
    assert!(
        (0.6938775510204116
            - scale_values(&Value::from("р"), &Value::from("а"), &Value::from("я")).unwrap())
            < f64::EPSILON
    );

    // Non-ASCII, russian char sequence.
    //
    // Logic:
    // <The same calculations as in the example above>.
    assert!(
        (0.6938775510204116
            - scale_values(
                &Value::from("пикр"),
                &Value::from("пика"),
                &Value::from("пикя")
            )
            .unwrap())
            < f64::EPSILON
    );

    // Seems okay that sequence of "ж" letters lies nearer to "а" sequence than to "я" sequence.
    assert!(
        (0.041379310344835614
            - scale_values(
                &Value::from("жжж жжж жжж"),
                &Value::from("ааа ааа ааа"),
                &Value::from("яяя яяя яяя")
            )
            .unwrap())
            < f64::EPSILON
    );

    // In reverse.
    // Seems okay that sequence of "э" letters lies nearer to "я" sequence than to "а" sequence.
    assert!(
        (0.9862068965517214
            - scale_values(
                &Value::from("эээ эээ эээ"),
                &Value::from("ааа ааа ааа"),
                &Value::from("яяя яяя яяя")
            )
            .unwrap())
            < f64::EPSILON
    );

    // Emoji test.
    // Seems okay that very happy face lies nearer to happy face than to angry face.
    assert!(
        (0.1212121212121212
            - scale_values(&Value::from("😄"), &Value::from("😀"), &Value::from("😡")).unwrap())
            < f64::EPSILON
    );
}
