use crate::errors::QueryPlannerError;
use serde::ser::{Serialize, SerializeMap, Serializer};
use tarantool::hlua::{self, LuaRead};

use crate::ir::relation::Column;

#[derive(LuaRead, Debug, PartialEq, Clone)]
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

#[derive(LuaRead, Debug, PartialEq, Eq, Clone)]
pub struct BoxExecuteFormat {
    pub metadata: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
}

impl BoxExecuteFormat {
    /// Create empty query result set
    #[allow(dead_code)]
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
