//! Value module.

use crate::errors::QueryPlannerError;
use decimal::d128;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

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

/// Values are used to keep constants from the query
/// or results for the virtual tables.
#[derive(Serialize, Deserialize, Hash, PartialEq, Debug, Clone)]
pub enum Value {
    /// Boolean type.
    Boolean(bool),
    /// SQL NULL (unknown in the terms of three-valued logic).
    Null,
    /// Number uses libdecnumber to correctly serialize any form
    /// of the number to the same bytes (`1e0`, `1` and `1.0`
    /// should be equivalent).
    Number(d128),
    /// String type.
    String(String),
}

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

impl Value {
    /// Construct a number from the string.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when a string is not a number.
    pub fn number_from_str(f: &str) -> Result<Self, QueryPlannerError> {
        if let Ok(d) = d128::from_str(&f.to_string()) {
            return Ok(Value::Number(d));
        }
        Err(QueryPlannerError::InvalidNumber)
    }

    /// Construct a string from the Rust `String`.
    #[must_use]
    pub fn string_from_str(f: &str) -> Self {
        Value::String(String::from(f))
    }

    /// Check equality of the two values.
    /// Result uses three-valued logic.
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
