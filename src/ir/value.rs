//! Value module.

use std::fmt;
use std::hash::Hash;
use std::num::NonZeroI32;
use std::str::FromStr;

use serde::ser;
use serde::{Deserialize, Serialize};
use tarantool::decimal::Decimal;
use tarantool::tlua;

use crate::executor::engine::cartridge::hash::ToHashString;
use crate::ir::value::double::Double;

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

/// Values are used to keep constants in the IR tree
/// or results in the virtual tables.
#[derive(Hash, PartialEq, Debug, Clone)]
pub enum Value {
    /// Boolean type.
    Boolean(bool),
    /// Fixed point type.
    Decimal(Decimal),
    /// Floating point type.
    Double(Double),
    /// Signed integer type.
    Integer(i64),
    /// SQL NULL ("unknown" in the terms of three-valued logic).
    Null,
    /// String type.
    String(String),
    /// Unsigned integer type.
    Unsigned(u64),
}

/// As a side effect, `NaN == NaN` is true.
/// We should manually care about this case in the code.
impl Eq for Value {}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Null => write!(f, "NULL"),
            Value::Unsigned(v) => write!(f, "{}", v),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Double(v) => fmt::Display::fmt(&v, f),
            Value::Decimal(v) => fmt::Display::fmt(v, f),
            Value::String(v) => write!(f, "'{}'", v),
        }
    }
}

impl From<bool> for Value {
    fn from(f: bool) -> Self {
        Value::Boolean(f)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Unsigned(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(i64::from(v))
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::Unsigned(u64::from(v))
    }
}

impl From<Double> for Value {
    fn from(v: Double) -> Self {
        Value::Double(v)
    }
}

impl From<Decimal> for Value {
    fn from(v: Decimal) -> Self {
        Value::Decimal(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        if v.is_nan() {
            return Value::Null;
        }
        Value::Double(v.into())
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

impl Value {
    /// Checks equality of the two values.
    /// The result uses three-valued logic.
    #[must_use]
    pub fn eq(&self, other: &Value) -> Trivalent {
        match &*self {
            Value::Boolean(s) => match other {
                Value::Boolean(o) => (s == o).into(),
                Value::Null => Trivalent::Unknown,
                Value::Unsigned(_)
                | Value::Integer(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::String(_) => Trivalent::False,
            },
            Value::Null => Trivalent::Unknown,
            Value::Integer(s) => match other {
                Value::Boolean(_) | Value::String(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (s == o).into(),
                Value::Decimal(o) => (&Decimal::from(*s) == o).into(),
                // If double can't be converted to decimal without error then it is not equal to integer.
                Value::Double(o) => (Decimal::from_str(&format!("{}", s))
                    == Decimal::from_str(&format!("{}", o)))
                .into(),
                Value::Unsigned(o) => (&Decimal::from(*s) == o).into(),
            },
            Value::Double(s) => match other {
                Value::Boolean(_) | Value::String(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (*s == Double::from(*o)).into(),
                // If double can't be converted to decimal without error then it is not equal to decimal.
                Value::Decimal(o) => (Decimal::from_str(&format!("{}", s)) == Ok(*o)).into(),
                Value::Double(o) => (s == o).into(),
                // If double can't be converted to decimal without error then it is not equal to unsigned.
                Value::Unsigned(o) => {
                    (Decimal::from_str(&format!("{}", s)) == Ok(Decimal::from(*o))).into()
                }
            },
            Value::Decimal(s) => match other {
                Value::Boolean(_) | Value::String(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (s == &Decimal::from(*o)).into(),
                Value::Decimal(o) => (s == o).into(),
                // If double can't be converted to decimal without error then it is not equal to decimal.
                Value::Double(o) => (Ok(*s) == Decimal::from_str(&format!("{}", o))).into(),
                Value::Unsigned(o) => (s == &Decimal::from(*o)).into(),
            },
            Value::Unsigned(s) => match other {
                Value::Boolean(_) | Value::String(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (Decimal::from(*s) == *o).into(),
                Value::Decimal(o) => (&Decimal::from(*s) == o).into(),
                // If double can't be converted to decimal without error then it is not equal to unsigned.
                Value::Double(o) => {
                    (Ok(Decimal::from(*s)) == Decimal::from_str(&format!("{}", o))).into()
                }
                Value::Unsigned(o) => (s == o).into(),
            },
            Value::String(s) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::String(o) => s.eq(o).into(),
            },
        }
    }
}

impl ToHashString for Value {
    fn to_hash_string(&self) -> String {
        match self {
            Value::Unsigned(v) => v.to_string(),
            Value::Integer(v) => v.to_string(),
            // It is important to trim trailing zeros when converting to string.
            // Otherwise, the hash from `1.000` and `1` would be different,
            // though the values are the same.
            // We don't use internal hash function because we calculate the hash
            // from the string representation for all other types.
            Value::Decimal(v) => v.trim().to_string(),
            Value::Double(v) => v.to_string(),
            Value::Boolean(v) => v.to_string(),
            Value::String(v) => v.to_string(),
            Value::Null => "NULL".to_string(),
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        match &self {
            Value::Unsigned(v) => serializer.serialize_u64(*v),
            Value::Integer(v) => serializer.serialize_i64(*v),
            Value::Decimal(v) => v.serialize(serializer),
            Value::Double(Double { value }) => serializer.serialize_f64(*value),
            Value::Boolean(v) => serializer.serialize_bool(*v),
            Value::String(v) => serializer.serialize_str(v),
            Value::Null => serializer.serialize_none(),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum EncodedValue {
            Unsigned(u64),
            Integer(i64),
            Decimal(Decimal),
            Double(Double),
            Float(f64),
            Boolean(bool),
            String(String),
            Null(()),
        }

        impl From<EncodedValue> for Value {
            fn from(v: EncodedValue) -> Self {
                match v {
                    EncodedValue::Unsigned(v) => Value::Unsigned(v),
                    EncodedValue::Integer(v) => Value::Integer(v),
                    EncodedValue::Decimal(v) => Value::Decimal(v),
                    EncodedValue::Double(v) => {
                        if v.value.is_nan() {
                            return Value::Null;
                        }
                        Value::Double(v)
                    }
                    EncodedValue::Float(v) => {
                        if v.is_nan() {
                            return Value::Null;
                        }
                        Value::Double(Double::from(v))
                    }
                    EncodedValue::Boolean(v) => Value::Boolean(v),
                    EncodedValue::String(v) => Value::String(v),
                    EncodedValue::Null(_) => Value::Null,
                }
            }
        }

        let encoded = EncodedValue::deserialize(deserializer)?;
        Ok(encoded.into())
    }
}

impl From<Value> for String {
    fn from(v: Value) -> Self {
        match v {
            Value::Unsigned(v) => v.to_string(),
            Value::Integer(v) => v.to_string(),
            Value::Decimal(v) => v.to_string(),
            Value::Double(v) => v.to_string(),
            Value::Boolean(v) => v.to_string(),
            Value::String(v) => v,
            Value::Null => "NULL".to_string(),
        }
    }
}

impl<L: tlua::AsLua> tlua::Push<L> for Value {
    type Err = tlua::Void;

    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        match self {
            Value::Unsigned(v) => v.push_to_lua(lua),
            Value::Integer(v) => v.push_to_lua(lua),
            Value::Decimal(v) => v.push_to_lua(lua),
            Value::Double(v) => v.push_to_lua(lua),
            Value::Boolean(v) => v.push_to_lua(lua),
            Value::String(v) => v.push_to_lua(lua),
            Value::Null => tlua::Null.push_to_lua(lua),
        }
    }
}

impl<L> tlua::PushInto<L> for Value
where
    L: tlua::AsLua,
{
    type Err = tlua::Void;
    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (tlua::Void, L)> {
        match self {
            Value::Unsigned(v) => v.push_into_lua(lua),
            Value::Integer(v) => v.push_into_lua(lua),
            Value::Decimal(v) => v.push_into_lua(lua),
            Value::Double(v) => v.push_into_lua(lua),
            Value::Boolean(v) => v.push_into_lua(lua),
            Value::String(v) => v.push_into_lua(lua),
            Value::Null => tlua::Null.push_into_lua(lua),
        }
    }
}

impl<L> tlua::PushOneInto<L> for Value where L: tlua::AsLua {}

impl<L> tlua::LuaRead<L> for Value
where
    L: tlua::AsLua,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Value, L> {
        // At the moment Tarantool module can't distinguish between
        // double and integer/unsigned. So we have to do it manually.
        if let Ok(v) = f64::lua_read_at_position(&lua, index) {
            if v.is_subnormal()
                || v.is_nan()
                || v.is_infinite()
                || v.is_finite() && v.fract().abs() >= std::f64::EPSILON
            {
                return Ok(Value::Double(Double::from(v)));
            }
        }
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::Unsigned(v)),
            Err(lua) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::Integer(v)),
            Err(lua) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => {
                let value: Decimal = v;
                return Ok(Self::Decimal(value));
            }
            Err(lua) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => {
                let value: Double = v;
                return Ok(Self::Double(value));
            }
            Err(lua) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::Boolean(v)),
            Err(lua) => lua,
        };
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::String(v)),
            Err(lua) => lua,
        };
        let lua = match tlua::Null::lua_read_at_position(lua, index) {
            Ok(_) => return Ok(Self::Null),
            Err(lua) => lua,
        };

        Err(lua)
    }
}

pub mod double;
#[cfg(test)]
mod tests;
