use super::*;
use pretty_assertions::assert_eq;
use tarantool::decimal;

#[test]
fn boolean() {
    assert_eq!(Value::Boolean(true), Value::from(true));
    assert_eq!(Value::Boolean(false), Value::from(false));
    assert_ne!(Value::from(true), Value::from(false));
}

#[test]
fn decimal() {
    assert_eq!(
        Value::Decimal(decimal!(0)),
        Value::from(Decimal::from(0_u64))
    );
    assert_eq!(
        Value::Decimal(Decimal::from(0_u64)),
        Value::from(Decimal::from_str("0.0").unwrap())
    );
    assert_eq!(
        Value::Decimal(Decimal::from_str("1.000000000000000").unwrap()),
        Value::from(Decimal::from_str("1").unwrap())
    );
    assert_eq!(
        Value::Decimal(Decimal::from_str("9223372036854775807").unwrap()),
        Value::from(Decimal::from(9_223_372_036_854_775_807_u64))
    );
    assert_ne!(
        Value::Decimal(decimal!(1)),
        Value::from("0.9999999999999999".parse::<Decimal>().unwrap())
    );
}

#[test]
#[allow(clippy::excessive_precision)]
fn double() {
    assert_eq!(
        Value::Double(0.0_f64.into()),
        Value::from(Double::from(0.0000_f64))
    );
    assert_eq!(
        Value::Double(0.999_999_999_999_999_7e-308_f64.into()),
        Value::from(Double::from(1e-308_f64))
    );
    assert_eq!(Value::from(f64::NAN), Value::Null);
    assert_ne!(
        Value::Double(Double::from(0.999_999_999_999_999_6e-308_f64)),
        Value::from(Double::from(0.999_999_999_999_999_7e-308_f64))
    );
}

#[test]
fn integer() {
    assert_eq!(Value::Integer(0), Value::from(0_i64));
    assert_eq!(
        Value::Integer(9_223_372_036_854_775_807_i64),
        Value::from(9_223_372_036_854_775_807_i64)
    );
    assert_eq!(
        Value::Integer(-9_223_372_036_854_775_807_i64),
        Value::from(-9_223_372_036_854_775_807_i64)
    );
    assert_ne!(
        Value::Integer(9_223_372_036_854_775_807_i64),
        Value::from(-9_223_372_036_854_775_807_i64)
    );
}

#[test]
fn string() {
    assert_eq!(Value::String("".to_string()), Value::from(""));
    assert_eq!(Value::String("hello".to_string()), Value::from("hello"));
    assert_eq!(
        Value::String("hello".to_string()),
        Value::from("hello".to_string())
    );
    assert_ne!(
        Value::String("hello".to_string()),
        Value::from("world".to_string())
    );
}

#[test]
fn unsigned() {
    assert_eq!(Value::Unsigned(0), Value::from(0_u64));
    assert_eq!(
        Value::Unsigned(u64::MAX),
        Value::from(18_446_744_073_709_551_615_u64)
    );
    assert_ne!(Value::Unsigned(0_u64), Value::from(1_u64));
}

#[test]
fn tuple() {
    let t = Tuple::from(vec![Value::Unsigned(0), Value::String("hello".to_string())]);
    assert_eq!(
        Value::Tuple(t),
        Value::from(vec![Value::from(0_u64), Value::from("hello")])
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn equivalence() {
    // Boolean
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::Boolean(false))
    );
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::from(Double::from(1e0_f64)))
    );
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::from(decimal!(1e0)))
    );
    assert_eq!(
        Trivalent::False,
        Value::Boolean(false).eq(&Value::from(0_u64))
    );
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::from(1_i64))
    );
    assert_eq!(Trivalent::False, Value::Boolean(false).eq(&Value::from("")));
    assert_eq!(
        Trivalent::False,
        Value::Boolean(true).eq(&Value::from("hello"))
    );
    assert_eq!(
        Trivalent::True,
        Value::Boolean(true).eq(&Value::Boolean(true))
    );
    assert_eq!(Trivalent::Unknown, Value::Boolean(true).eq(&Value::Null));

    // Decimal
    assert_eq!(
        Trivalent::True,
        Value::Decimal(decimal!(0.000)).eq(&Value::from(0_u64))
    );
    assert_eq!(
        Trivalent::True,
        Value::Decimal(decimal!(0.000)).eq(&Value::from(decimal!(0)))
    );
    assert_eq!(
        Trivalent::True,
        Value::Decimal(decimal!(0.000)).eq(&Value::from(0_u64))
    );
    assert_eq!(
        Trivalent::True,
        Value::Decimal(decimal!(0.000)).eq(&Value::from(0_i64))
    );
    assert_eq!(
        Trivalent::False,
        Value::Decimal(decimal!(0.000)).eq(&Value::from(false))
    );
    assert_eq!(
        Trivalent::False,
        Value::Decimal(decimal!(0.000)).eq(&Value::from(""))
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::Decimal(decimal!(0.000)).eq(&Value::Null)
    );

    // Double
    assert_eq!(
        Trivalent::True,
        Value::Double(Double::from(0.000_f64)).eq(&Value::from(0_u64))
    );
    assert_eq!(
        Trivalent::True,
        Value::Double(Double::from(0.000_f64)).eq(&Value::from(0_i64))
    );
    assert_eq!(
        Trivalent::False,
        Value::Double(Double::from(0.000_f64)).eq(&Value::from(false))
    );
    assert_eq!(
        Trivalent::False,
        Value::Double(Double::from(0.000_f64)).eq(&Value::from(""))
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::Double(Double::from(0.000_f64)).eq(&Value::Null)
    );
    assert_eq!(
        Trivalent::True,
        Value::Double(Double::from(f64::INFINITY)).eq(&Value::from(f64::INFINITY))
    );
    assert_eq!(
        Trivalent::True,
        Value::Double(Double::from(f64::NEG_INFINITY)).eq(&Value::from(f64::NEG_INFINITY))
    );
    assert_eq!(
        Trivalent::Unknown,
        Value::Double(Double::from(f64::NAN)).eq(&Value::from(f64::NAN))
    );

    // Null
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::Null));
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::Boolean(false)));
    assert_eq!(
        Trivalent::Unknown,
        Value::Null.eq(&Value::Double(f64::NAN.into()))
    );
    assert_eq!(Trivalent::Unknown, Value::Null.eq(&Value::from("")));

    // String
    assert_eq!(
        Trivalent::False,
        Value::from("hello").eq(&Value::from(" hello "))
    );
    assert_eq!(
        Trivalent::True,
        Value::from("hello").eq(&Value::from("hello".to_string()))
    );
    assert_eq!(Trivalent::Unknown, Value::from("hello").eq(&Value::Null));
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
