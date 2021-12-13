use serde::ser::{Serialize, SerializeMap, Serializer};
use tarantool::hlua::{self, LuaRead};

use crate::ir::relation::Column;

#[derive(LuaRead, Debug, PartialEq)]
pub enum Value {
    Boolean(bool),
    Number(f64),
    Integer(i64),
    String(String),
    Unsigned(u64),
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
        }
    }
}

impl Eq for Value {}

#[derive(LuaRead, Debug, PartialEq, Eq)]
pub struct BoxExecuteResult {
    pub metadata: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
}

/// Custom Implementation `ser::Serialize`, because if using standard `#derive[Serialize]` then each `BoxExecuteResult`
/// record is serialized to a list. That is not the result we expect.
impl Serialize for BoxExecuteResult {
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
