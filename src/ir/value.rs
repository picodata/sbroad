use decimal::d128;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Trivalent {
    False,
    True,
    Unknown,
}

impl From<bool> for Trivalent {
    fn from(f: bool) -> Self {
        if f {
            Trivalent::True
        } else {
            Trivalent::False
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Value {
    Boolean(bool),
    Null,
    Number(d128),
    String(String),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Null => write!(f, "NULL"),
            Value::Number(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
        }
    }
}

#[allow(dead_code)]
impl Value {
    pub fn number_from_str(f: &str) -> Self {
        let d = d128::from_str(&f.to_string()).unwrap();
        Value::Number(d)
    }

    pub fn string_from_str(f: &str) -> Self {
        Value::String(String::from(f))
    }

    pub fn eq(&self, other: &Value) -> Trivalent {
        match &*self {
            Value::Boolean(s) => match other {
                Value::Boolean(o) => (s == o).into(),
                Value::Null => Trivalent::Unknown,
                Value::Number(_) | Value::String(_) => Trivalent::False,
            },
            Value::Null => Trivalent::Unknown,
            Value::Number(s) => match other {
                Value::Boolean(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Number(o) => (s == o).into(),
                Value::String(o) => (s == &d128::from_str(&o.to_string()).unwrap()).into(),
            },
            Value::String(s) => match other {
                Value::Boolean(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Number(o) => (o == &d128::from_str(&s.to_string()).unwrap()).into(),
                Value::String(o) => s.eq(o).into(),
            },
        }
    }
}

impl From<Trivalent> for Value {
    fn from(f: Trivalent) -> Self {
        match f {
            Trivalent::False => Value::Boolean(false),
            Trivalent::True => Value::Boolean(true),
            Trivalent::Unknown => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    // Helpers

    fn test_valid_number(s: &str) {
        let v = Value::number_from_str(s);
        let d = d128::from_str(&s.to_string()).unwrap();

        if let Value::Number(n) = v {
            assert_eq!(d, n);
        } else {
            panic!("incorrect enum!");
        }
    }

    fn test_nan(s: &str) {
        let v = Value::number_from_str(s);

        if let Value::Number(n) = v {
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
            Value::Boolean(true).eq(&Value::number_from_str("1e0"))
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
            Value::Null.eq(&Value::number_from_str("nan"))
        );
        assert_eq!(
            Trivalent::Unknown,
            Value::Null.eq(&Value::string_from_str(""))
        );

        // Number
        assert_eq!(
            Trivalent::False,
            Value::number_from_str("nan").eq(&Value::string_from_str("nan"))
        );
        assert_eq!(
            Trivalent::False,
            Value::number_from_str("0").eq(&Value::Boolean(false))
        );
        assert_eq!(
            Trivalent::False,
            Value::number_from_str("inf").eq(&Value::number_from_str("nan"))
        );
        assert_eq!(
            Trivalent::False,
            Value::number_from_str("-inf").eq(&Value::number_from_str("hello"))
        );
        assert_eq!(
            Trivalent::False,
            Value::number_from_str("1e0").eq(&Value::number_from_str("1e100"))
        );
        assert_eq!(
            Trivalent::True,
            Value::number_from_str("1e0").eq(&Value::number_from_str("1.0e0"))
        );
        assert_eq!(
            Trivalent::True,
            Value::number_from_str("1e0").eq(&Value::number_from_str("1E0"))
        );
        assert_eq!(
            Trivalent::Unknown,
            Value::number_from_str("inf").eq(&Value::Null)
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
}
