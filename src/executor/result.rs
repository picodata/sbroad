use std::fmt;

use decimal::d128;
use serde::de::Visitor;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde::Deserialize;
use tarantool::tlua::{self, LuaRead};

use crate::errors::QueryPlannerError;
use crate::executor::vtable::VirtualTable;
use crate::ir::relation::Column;
use crate::ir::value::{AsIrVal, Value as IrValue};

/// `Value` cointains tarantool datatypes
#[derive(LuaRead, Debug, PartialEq, Clone)]
pub enum Value {
    Boolean(bool),
    Number(f64),
    Integer(i64),
    String(String),
    Unsigned(u64),
    Null(tlua::Null),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Value::Boolean(v) => write!(f, "{}", v),
            Value::Number(v) => write!(f, "{}", v),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Unsigned(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "'{}'", v),
            Value::Null(_) => write!(f, "NULL"),
        }
    }
}

/// IR `Value` convertor implementation
impl AsIrVal for Value {
    fn as_ir_value(&self) -> Result<IrValue, QueryPlannerError> {
        match &self {
            Value::Boolean(v) => Ok(IrValue::Boolean(*v)),
            Value::Number(v) => Ok(IrValue::number_from_str(&v.to_string())?),
            Value::Integer(v) => Ok(IrValue::Number(d128::from(*v))),
            Value::String(v) => Ok(IrValue::String(v.clone())),
            Value::Unsigned(v) => Ok(IrValue::Number(d128::from(*v))),
            Value::Null(_) => Ok(IrValue::Null),
        }
    }
}

/// Custom Implementation `ser::Serialize`, because if using standard `#derive[Serialize]` then each `Value` type
/// record serialize as list and it doesn't correct for result format
impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self {
            Value::Boolean(v) => serializer.serialize_bool(*v),
            Value::Number(v) => serializer.serialize_f64(*v),
            Value::Integer(v) => serializer.serialize_i64(*v),
            Value::String(v) => serializer.serialize_str(v),
            Value::Unsigned(v) => serializer.serialize_u64(*v),
            Value::Null(_) => serializer.serialize_none(),
        }
    }
}

struct ValueVistor;

impl<'de> Visitor<'de> for ValueVistor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a tarantool value enum implementation")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Boolean(value))
    }
    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Integer(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Unsigned(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Number(value))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::String(v.to_string()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::String(value))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Value::Null(tlua::Null))
    }
}

/// Custom Implementation `de::Deserialize`, because if using standard `#derive[Deserialize]`
/// then each `Value` type record deserialize as Value
/// and it doesn't correct for result format
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(ValueVistor)
    }
}

impl Eq for Value {}

type BoxExecuteTuple = Vec<Value>;

#[derive(LuaRead, Debug, PartialEq, Eq, Clone)]
pub struct BoxExecuteFormat {
    pub metadata: Vec<Column>,
    pub rows: Vec<BoxExecuteTuple>,
}

impl Default for BoxExecuteFormat {
    fn default() -> Self {
        Self::new()
    }
}

impl BoxExecuteFormat {
    /// Create empty query result set
    #[allow(dead_code)]
    #[must_use]
    pub fn new() -> Self {
        BoxExecuteFormat {
            metadata: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Merge two record sets. If current recordset is empty function sets metadata and appends result rows.
    /// If the current recordset is not empty compare its metadata with the one from the new recordset.
    /// If they differ return error.
    ///
    ///  # Errors
    ///  - metadata isn't equal.
    #[allow(dead_code)]
    pub fn extend(&mut self, recordset: BoxExecuteFormat) -> Result<(), QueryPlannerError> {
        if self.metadata.is_empty() {
            self.metadata = recordset.clone().metadata;
        }

        if self.metadata != recordset.metadata {
            return Err(QueryPlannerError::CustomError(String::from(
                "Different metadata. BoxExecuteFormat can't be extended",
            )));
        }
        self.rows.extend(recordset.rows);
        Ok(())
    }

    /// Converts result to virtual table for linker
    ///
    /// # Errors
    /// - convert to virtual table error
    #[allow(dead_code)]
    pub fn as_virtual_table(&self) -> Result<VirtualTable, QueryPlannerError> {
        let mut result = VirtualTable::new();

        for col in &self.metadata {
            result.add_column(col.clone());
        }

        for t in &self.rows {
            result.add_tuple(t.clone())?;
        }

        Ok(result)
    }
}

/// Custom Implementation `ser::Serialize`, because if using standard `#derive[Serialize]` then each `BoxExecuteResult`
/// record is serialized to a list. That is not the result we expect.
impl Serialize for BoxExecuteFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("metadata", &self.metadata)?;
        map.serialize_entry("rows", &self.rows)?;

        map.end()
    }
}

#[cfg(test)]
mod tests;
