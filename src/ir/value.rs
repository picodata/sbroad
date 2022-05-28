//! Value module.

use std::fmt;
use std::str::FromStr;

use decimal::d128;
use serde::{Deserialize, Serialize};

use crate::errors::QueryPlannerError;
use crate::executor::engine::cartridge::hash::ToHashString;

/// SQL uses three-valued logic. We need to implement
/// it to compare values with each other.
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

/// Interface for converting execute engine `Value` (const, results, etc)
/// types to IR `Value` type
pub trait AsIrVal {
    /// Transforms object to IR `Value`.
    ///
    /// # Errors
    /// Returns transformation
    fn as_ir_value(&self) -> Result<Value, QueryPlannerError>;
}

/// Values are used to keep constants from the query
/// or results for the virtual tables.
#[derive(Serialize, Deserialize, Hash, PartialEq, Debug, Clone)]
pub enum Value {
    /// Boolean type.
    Boolean(bool),
    /// SQL NULL (unknown in the terms of three-valued logic).
    Null,
    /// The number uses libdecnumber to correctly serialize any form
    /// of the number to the same bytes (eg. `1e0`, `1` and `1.0`
    /// should be equivalent).
    Number(d128),
    /// String type.
    String(String),
}

impl Eq for Value {}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Null => write!(f, "NULL"),
            Value::Number(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "'{}'", v),
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Number(d128::from(v))
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Number(d128::from(v))
    }
}

impl Value {
    /// Constructs a number from the string.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when a string is not a number.
    pub fn number_from_str(f: &str) -> Result<Self, QueryPlannerError> {
        if let Ok(d) = d128::from_str(f) {
            return Ok(Value::Number(d));
        }
        Err(QueryPlannerError::InvalidNumber)
    }

    /// Constructs a string from the Rust `String`.
    #[must_use]
    pub fn string_from_str(f: &str) -> Self {
        Value::String(String::from(f))
    }

    /// Checks equality of the two values.
    /// The result uses three-valued logic.
    #[must_use]
    pub fn eq(&self, other: &Value) -> Trivalent {
        match &*self {
            Value::Boolean(s) => match other {
                Value::Boolean(o) => (s == o).into(),
                Value::Null => Trivalent::Unknown,
                Value::Number(_) | Value::String(_) => Trivalent::False,
            },
            Value::Null => Trivalent::Unknown,
            Value::Number(s) => match other {
                Value::Boolean(_) | Value::String(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Number(o) => (s == o).into(),
            },
            Value::String(s) => match other {
                Value::Boolean(_) | Value::Number(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::String(o) => s.eq(o).into(),
            },
        }
    }
}

impl ToHashString for Value {
    fn to_hash_string(&self) -> String {
        match self {
            Value::Boolean(v) => v.to_string(),
            Value::Number(v) => v.to_string(),
            Value::String(v) => v.to_string(),
            Value::Null => "NULL".to_string(),
        }
    }
}

impl From<Value> for String {
    fn from(v: Value) -> Self {
        match v {
            Value::Boolean(b) => b.to_string(),
            Value::Null => "NULL".to_string(),
            Value::Number(n) => n.to_string(),
            Value::String(s) => s,
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
mod tests;
