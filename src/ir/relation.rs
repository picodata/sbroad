//! Relation module.

use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;

use serde::de::{Error, MapAccess, Visitor};
use serde::ser::{Serialize as SerSerialize, SerializeMap, Serializer};
use serde::{Deserialize, Deserializer, Serialize};
use tarantool::hlua::{self, LuaRead};

use crate::errors::QueryPlannerError;

use super::distribution::Key;
use super::value::Value;

/// Supported column types.
#[derive(LuaRead, Serialize, Deserialize, PartialEq, Debug, Eq, Clone)]
pub enum Type {
    Boolean,
    Number,
    String,
    Integer,
    Unsigned,
}

impl Type {
    /// Type constructor
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the input arguments are invalid.
    pub fn new(s: &str) -> Result<Self, QueryPlannerError> {
        match s.to_string().to_lowercase().as_str() {
            "boolean" => Ok(Type::Boolean),
            "number" => Ok(Type::Number),
            "string" => Ok(Type::String),
            "integer" => Ok(Type::Integer),
            "unsigned" => Ok(Type::Unsigned),
            _ => Err(QueryPlannerError::TypeNotImplemented),
        }
    }
}

/// Relation column.
#[derive(LuaRead, PartialEq, Debug, Eq, Clone)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Column type.
    pub r#type: Type,
}

/// Msgpack serializer for column
impl SerSerialize for Column {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("name", &self.name)?;
        match &self.r#type {
            Type::Boolean => map.serialize_entry("type", "boolean")?,
            Type::Number => map.serialize_entry("type", "number")?,
            Type::Integer => map.serialize_entry("type", "integer")?,
            Type::Unsigned => map.serialize_entry("type", "unsigned")?,
            Type::String => map.serialize_entry("type", "string")?,
        }

        map.end()
    }
}

struct ColumnVisitor;

impl<'de> Visitor<'de> for ColumnVisitor {
    type Value = Column;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("column parsing failed")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut column_name = String::new();
        let mut column_type = String::new();
        while let Some((key, value)) = map.next_entry::<String, String>()? {
            match key.as_str() {
                "name" => column_name.push_str(&value),
                "type" => column_type.push_str(&value.to_lowercase()),
                _ => return Err(Error::custom("invalid column param")),
            }
        }

        match column_type.as_str() {
            "boolean" => Ok(Column::new(&column_name, Type::Boolean)),
            "number" => Ok(Column::new(&column_name, Type::Number)),
            "string" => Ok(Column::new(&column_name, Type::String)),
            "integer" => Ok(Column::new(&column_name, Type::Integer)),
            "unsigned" => Ok(Column::new(&column_name, Type::Unsigned)),
            _ => Err(Error::custom("unsupported column type")),
        }
    }
}

impl<'de> Deserialize<'de> for Column {
    fn deserialize<D>(deserializer: D) -> Result<Column, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(ColumnVisitor)
    }
}

impl Column {
    /// Column constructor.
    #[must_use]
    pub fn new(n: &str, t: Type) -> Self {
        Column {
            name: n.into(),
            r#type: t,
        }
    }
}

/// Table is a tuple storage in the cluster.
///
/// Tables are the tuple storages in the cluster.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Table {
    /// Already existing table segment on some cluster data node.
    Segment {
        /// List of the columns.
        columns: Vec<Column>,
        /// Distribution key of the output tuples (column positions).
        key: Key,
        /// Unique table name.
        name: String,
    },
    /// Result tuple storage, created by the executor. All tuples
    /// are distributed randomly.
    Virtual {
        /// List of the columns.
        columns: Vec<Column>,
        /// List of the "raw" tuples (list of values).
        data: Vec<Vec<Value>>,
        /// Unique table name (we need to generate it ourselves).
        name: String,
    },
    /// Result tuple storage, created by the executor. All tuples
    /// have a distribution key.
    VirtualSegment {
        /// List of the columns.
        columns: Vec<Column>,
        /// "Raw" tuples (list of values) in a hash map (hashed by distribution key)
        data: HashMap<String, Vec<Vec<Value>>>,
        /// Distribution key (list of the column positions)
        key: Key,
        /// Unique table name (we need to generate it ourselves).
        name: String,
    },
}

impl Table {
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Table::Segment { name, .. }
            | Table::Virtual { name, .. }
            | Table::VirtualSegment { name, .. } => name,
        }
    }

    /// Table segment constructor.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the input arguments are invalid.
    pub fn new_seg(n: &str, c: Vec<Column>, k: &[&str]) -> Result<Self, QueryPlannerError> {
        let mut pos_map: HashMap<&str, usize> = HashMap::new();
        let cols = &c;
        let no_duplicates = cols
            .iter()
            .enumerate()
            .all(|(pos, col)| matches!(pos_map.insert(&col.name, pos), None));

        if !no_duplicates {
            return Err(QueryPlannerError::DuplicateColumn);
        }

        let keys = &k;
        let res_positions: Result<Vec<_>, _> = keys
            .iter()
            .map(|name| match pos_map.get(*name) {
                Some(pos) => Ok(*pos),
                None => Err(QueryPlannerError::InvalidShardingKey),
            })
            .collect();
        let positions = res_positions?;

        Ok(Table::Segment {
            name: n.into(),
            columns: c,
            key: Key::new(positions),
        })
    }

    /// Table segment from YAML.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the YAML-serialized table is invalid.
    pub fn seg_from_yaml(s: &str) -> Result<Self, QueryPlannerError> {
        let ts: Table = match serde_yaml::from_str(s) {
            Ok(t) => t,
            Err(_) => return Err(QueryPlannerError::Serialization),
        };
        if let Table::Segment { columns, key, .. } = &ts {
            let mut uniq_cols: HashSet<&str> = HashSet::new();
            let cols = columns;

            let no_duplicates = cols.iter().all(|col| uniq_cols.insert(&col.name));

            if !no_duplicates {
                return Err(QueryPlannerError::DuplicateColumn);
            }

            let in_range = key.positions.iter().all(|pos| *pos < cols.len());

            if !in_range {
                return Err(QueryPlannerError::ValueOutOfRange);
            }

            Ok(ts)
        } else {
            Err(QueryPlannerError::Serialization)
        }
    }

    //TODO: constructors for Virtual and VirtualSegment
}

#[cfg(test)]
mod tests;
