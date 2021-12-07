use super::*;
use pretty_assertions::assert_eq;

// Helpers

fn test_valid_number(s: &str) {
    let v = Value::number_from_str(s);
    let d = d128::from_str(&s.to_string()).unwrap();

    if let Ok(Value::Number(n)) = v {
        assert_eq!(d, n);
    } else {
        panic!("incorrect enum!");
    }
}

fn test_nan(s: &str) {
    let v = Value::number_from_str(s);

    if let Ok(Value::Number(n)) = v {
        assert_eq!(true, n.is_nan());
    } else {
        panic!("incorrect enum!");
    }
}

// Tests

#[test]
fn boolean() {
    let v = Value::Boolean(true);

    if let Value::Boolean(b) = v {
        assert_eq!(true, b);
    } else {
        panic!("incorrect enum!");
    }
}

#[test]
fn number() {
    test_valid_number("1e1");
    test_valid_number("1e+1");
    test_valid_number("1.53E+1");
    test_valid_number("1E-10");
    test_valid_number("inf");
    test_valid_number("INF");
    test_valid_number("-inf");
    test_valid_number("-INF");

    test_nan("nan");
    test_nan("NaN");
    test_nan("NAN");
    test_nan("hello");
    test_nan("   1e1 ")
}

#[test]
fn string() {
    let d = "hello";
    let v = Value::string_from_str(d);

    if let Value::String(s) = v {
        assert_eq!(s.eq(d), true);
    } else {
        panic!("incorrect enum!");
    }
}

#[test]
fn equivalence() {
    // Boolean
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::Boolean(false))
    );
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::number_from_str("1e0").unwrap())
    );
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::string_from_str("hello"))
    );
    assert_eq!(
        Trivalent::True,
        Value::Boolean(true).eq(&Value::Boolean(true))
    );
    assert_eq!(Trivalent::Unknown, Value::Boolean(true).eq(&Value::Null));

    // Null
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::Null));
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::Boolean(false)));
    assert_eq!(
        Trivalent::Unknown,
        Value::Null.eq(&Value::number_from_str("nan").unwrap())
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::Null.eq(&Value::string_from_str(""))
    );

    // Number
    assert_eq!(
        Trivalent::False,
        Value::number_from_str("nan")
            .unwrap()
            .eq(&Value::string_from_str("nan"))
    );
    assert_eq!(
        Trivalent::False,
        Value::number_from_str("0")
            .unwrap()
            .eq(&Value::Boolean(false))
    );
    assert_eq!(
        Trivalent::False,
        Value::number_from_str("inf")
            .unwrap()
            .eq(&Value::number_from_str("nan").unwrap())
    );
    assert_eq!(
        Trivalent::False,
        Value::number_from_str("-inf")
            .unwrap()
            .eq(&Value::number_from_str("hello").unwrap())
    );
    assert_eq!(
        Trivalent::False,
        Value::number_from_str("1e0")
            .unwrap()
            .eq(&Value::number_from_str("1e100").unwrap())
    );
    assert_eq!(
        Trivalent::True,
        Value::number_from_str("1e0")
            .unwrap()
            .eq(&Value::number_from_str("1.0e0").unwrap())
    );
    assert_eq!(
        Trivalent::True,
        Value::number_from_str("1e0")
            .unwrap()
            .eq(&Value::number_from_str("1E0").unwrap())
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::number_from_str("inf").unwrap().eq(&Value::Null)
    );

    // String
    assert_eq!(
        Trivalent::False,
        Value::string_from_str("hello").eq(&Value::string_from_str(" hello "))
    );
    assert_eq!(
        Trivalent::True,
        Value::string_from_str("hello").eq(&Value::string_from_str("hello"))
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::string_from_str("hello").eq(&Value::Null)
    );
}

#[test]
fn trivalent() {
    assert_eq!(
        Trivalent::False,
        Value::from(Trivalent::False).eq(&Value::Boolean(true))
    );
    assert_eq!(
        Trivalent::True,
        Value::from(Trivalent::True).eq(&Value::Boolean(true))
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::from(Trivalent::False).eq(&Value::Null)
    );
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::Null));
}
