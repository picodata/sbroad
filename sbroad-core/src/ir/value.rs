//! Value module.

use std::fmt::{self, Display};
use std::hash::Hash;
use std::num::NonZeroI32;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use tarantool::decimal::Decimal;
use tarantool::tlua::{self, LuaRead};

use crate::error;
use crate::executor::hash::ToHashString;
use crate::ir::value::double::Double;

#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq, Clone)]
pub struct Tuple(Vec<Value>);

impl Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "[{}]",
            self.0
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
                .join(",")
        )
    }
}

impl From<Vec<Value>> for Tuple {
    fn from(v: Vec<Value>) -> Self {
        Tuple(v)
    }
}

impl<L: tlua::AsLua> tlua::Push<L> for Tuple {
    type Err = tlua::Void;

    #[allow(unreachable_code)]
    fn push_to_lua(&self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        match self.0.push_to_lua(lua) {
            Ok(r) => Ok(r),
            Err(e) => {
                error!(Option::from("push ir tuple to lua"), &format!("{:?}", e.0),);
                Err((tlua::Void::from(e.0), e.1))
            }
        }
    }
}

impl<L> tlua::PushInto<L> for Tuple
where
    L: tlua::AsLua,
{
    type Err = tlua::Void;

    #[allow(unreachable_code)]
    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        match self.0.push_into_lua(lua) {
            Ok(r) => Ok(r),
            Err(e) => {
                error!(
                    Option::from("push ir tuple into lua"),
                    &format!("{:?}", e.0),
                );
                Err((tlua::Void::from(e.0), e.1))
            }
        }
    }
}

impl<L> tlua::PushOneInto<L> for Tuple where L: tlua::AsLua {}

impl<L> tlua::LuaRead<L> for Tuple
where
    L: tlua::AsLua,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Tuple, L> {
        match Vec::lua_read_at_position(lua, index) {
            Ok(v) => Ok(Tuple::from(v)),
            Err(lua) => Err(lua),
        }
    }
}

/// SQL uses three-valued logic. We need to implement
/// it to compare values with each other.
#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
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
#[derive(Hash, PartialEq, Debug, Clone, Deserialize, Serialize)]
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
    /// Tuple type
    Tuple(Tuple),
}

/// As a side effect, `NaN == NaN` is true.
/// We should manually care about this case in the code.
impl Eq for Value {}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Boolean(v) => write!(f, "{v}"),
            Value::Null => write!(f, "NULL"),
            Value::Unsigned(v) => write!(f, "{v}"),
            Value::Integer(v) => write!(f, "{v}"),
            Value::Double(v) => fmt::Display::fmt(&v, f),
            Value::Decimal(v) => fmt::Display::fmt(v, f),
            Value::String(v) => write!(f, "'{v}'"),
            Value::Tuple(v) => write!(f, "{v}"),
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

impl From<Tuple> for Value {
    fn from(v: Tuple) -> Self {
        Value::Tuple(v)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        let t = Tuple::from(v);
        Value::Tuple(t)
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
        match self {
            Value::Boolean(s) => match other {
                Value::Boolean(o) => (s == o).into(),
                Value::Null => Trivalent::Unknown,
                Value::Unsigned(_)
                | Value::Integer(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::String(_)
                | Value::Tuple(_) => Trivalent::False,
            },
            Value::Null => Trivalent::Unknown,
            Value::Integer(s) => match other {
                Value::Boolean(_) | Value::String(_) | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (s == o).into(),
                Value::Decimal(o) => (&Decimal::from(*s) == o).into(),
                // If double can't be converted to decimal without error then it is not equal to integer.
                Value::Double(o) => (Decimal::from_str(&format!("{s}"))
                    == Decimal::from_str(&format!("{o}")))
                .into(),
                Value::Unsigned(o) => (&Decimal::from(*s) == o).into(),
            },
            Value::Double(s) => match other {
                Value::Boolean(_) | Value::String(_) | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (*s == Double::from(*o)).into(),
                // If double can't be converted to decimal without error then it is not equal to decimal.
                Value::Decimal(o) => (Decimal::from_str(&format!("{s}")) == Ok(*o)).into(),
                Value::Double(o) => (s == o).into(),
                // If double can't be converted to decimal without error then it is not equal to unsigned.
                Value::Unsigned(o) => {
                    (Decimal::from_str(&format!("{s}")) == Ok(Decimal::from(*o))).into()
                }
            },
            Value::Decimal(s) => match other {
                Value::Boolean(_) | Value::String(_) | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (s == &Decimal::from(*o)).into(),
                Value::Decimal(o) => (s == o).into(),
                // If double can't be converted to decimal without error then it is not equal to decimal.
                Value::Double(o) => (Ok(*s) == Decimal::from_str(&format!("{o}"))).into(),
                Value::Unsigned(o) => (s == &Decimal::from(*o)).into(),
            },
            Value::Unsigned(s) => match other {
                Value::Boolean(_) | Value::String(_) | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::Integer(o) => (Decimal::from(*s) == *o).into(),
                Value::Decimal(o) => (&Decimal::from(*s) == o).into(),
                // If double can't be converted to decimal without error then it is not equal to unsigned.
                Value::Double(o) => {
                    (Ok(Decimal::from(*s)) == Decimal::from_str(&format!("{o}"))).into()
                }
                Value::Unsigned(o) => (s == o).into(),
            },
            Value::String(s) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_)
                | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
                Value::String(o) => s.eq(o).into(),
            },
            Value::Tuple(_) => match other {
                Value::Boolean(_)
                | Value::Integer(_)
                | Value::Decimal(_)
                | Value::Double(_)
                | Value::Unsigned(_)
                | Value::String(_)
                | Value::Tuple(_) => Trivalent::False,
                Value::Null => Trivalent::Unknown,
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
            Value::Tuple(v) => v.to_string(),
            Value::Null => "NULL".to_string(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, LuaRead, PartialEq, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum EncodedValue {
    Boolean(bool),
    Decimal(Decimal),
    Double(f64),
    Integer(i64),
    Unsigned(u64),
    String(String),
    Tuple(Tuple),
    Null(()),
}

impl fmt::Display for EncodedValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EncodedValue::Boolean(v) => write!(f, "{v}"),
            EncodedValue::Decimal(v) => fmt::Display::fmt(v, f),
            EncodedValue::Double(v) => write!(f, "{v}"),
            EncodedValue::Integer(v) => write!(f, "{v}"),
            EncodedValue::Unsigned(v) => write!(f, "{v}"),
            EncodedValue::String(v) => write!(f, "'{v}'"),
            EncodedValue::Tuple(v) => write!(f, "{v}"),
            EncodedValue::Null(_) => write!(f, "NULL"),
        }
    }
}

impl From<Value> for EncodedValue {
    fn from(value: Value) -> Self {
        match value {
            Value::Boolean(v) => EncodedValue::Boolean(v),
            Value::Decimal(v) => EncodedValue::Decimal(v),
            Value::Double(v) => EncodedValue::Double(v.value),
            Value::Integer(v) => EncodedValue::Integer(v),
            Value::Null => EncodedValue::Null(()),
            Value::String(v) => EncodedValue::String(v),
            Value::Tuple(v) => EncodedValue::Tuple(v),
            Value::Unsigned(v) => EncodedValue::Unsigned(v),
        }
    }
}

impl From<EncodedValue> for Value {
    #[allow(clippy::cast_possible_truncation)]
    fn from(value: EncodedValue) -> Self {
        match value {
            EncodedValue::Unsigned(v) => Value::Unsigned(v),
            EncodedValue::Integer(v) => Value::Integer(v),
            EncodedValue::Decimal(v) => Value::Decimal(v),
            EncodedValue::Double(v) => {
                if v.is_nan() {
                    Value::Null
                } else if v.is_subnormal()
                    || v.is_infinite()
                    || v.is_finite() && v.fract().abs() >= std::f64::EPSILON
                {
                    Value::Double(Double::from(v))
                } else {
                    Value::Integer(v as i64)
                }
            }
            EncodedValue::Boolean(v) => Value::Boolean(v),
            EncodedValue::String(v) => Value::String(v),
            EncodedValue::Tuple(v) => Value::Tuple(v),
            EncodedValue::Null(_) => Value::Null,
        }
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
            Value::Tuple(v) => v.to_string(),
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
            Value::Tuple(v) => v.push_to_lua(lua),
            Value::Null => tlua::Null.push_to_lua(lua),
        }
    }
}

impl<L> tlua::PushInto<L> for Value
where
    L: tlua::AsLua,
{
    type Err = tlua::Void;

    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        match self {
            Value::Unsigned(v) => v.push_into_lua(lua),
            Value::Integer(v) => v.push_into_lua(lua),
            Value::Decimal(v) => v.push_into_lua(lua),
            Value::Double(v) => v.push_into_lua(lua),
            Value::Boolean(v) => v.push_into_lua(lua),
            Value::String(v) => v.push_into_lua(lua),
            Value::Tuple(v) => v.push_into_lua(lua),
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
        let lua = match tlua::LuaRead::lua_read_at_position(lua, index) {
            Ok(v) => return Ok(Self::Tuple(v)),
            Err(lua) => lua,
        };
        let Err(lua) = tlua::Null::lua_read_at_position(lua, index) else {
            return Ok(Self::Null)
        };

        Err(lua)
    }
}

pub mod double;
#[cfg(test)]
mod tests;
