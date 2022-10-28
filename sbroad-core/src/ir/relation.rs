//! Relation module.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;

use serde::de::{Error, MapAccess, Visitor};
use serde::ser::{Serialize as SerSerialize, SerializeMap, Serializer};
use serde::{Deserialize, Deserializer, Serialize};
use tarantool::tlua::{self, LuaRead};

use crate::errors::QueryPlannerError;
use crate::ir::value::Value;

use super::distribution::Key;

const DEFAULT_VALUE: Value = Value::Null;

/// Supported column types, which is used in a schema only.
/// This `Type` doesn't have any relation with `Type` from IR.
#[derive(LuaRead, Serialize, Deserialize, PartialEq, Debug, Eq, Clone)]
pub enum Type {
    Boolean,
    Decimal,
    Double,
    Integer,
    Number,
    Scalar,
    String,
    Unsigned,
    Array,
}

impl Type {
    /// Type constructor
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the input arguments are invalid.
    pub fn new(s: &str) -> Result<Self, QueryPlannerError> {
        match s.to_string().to_lowercase().as_str() {
            "boolean" => Ok(Type::Boolean),
            "decimal" => Ok(Type::Decimal),
            "double" => Ok(Type::Double),
            "integer" => Ok(Type::Integer),
            "number" => Ok(Type::Number),
            "scalar" => Ok(Type::Scalar),
            "string" => Ok(Type::String),
            "unsigned" => Ok(Type::Unsigned),
            "array" => Ok(Type::Array),
            v => Err(QueryPlannerError::CustomError(format!(
                "type `{}` not implemented",
                v
            ))),
        }
    }
}

/// A role of the column in the relation.
#[derive(PartialEq, Debug, Eq, Clone)]
pub enum ColumnRole {
    /// General purpose column available for the user.
    User,
    /// Column is used for sharding (contains `bucket_id` in terms of `vshard`).
    Sharding,
}

/// Relation column.
#[derive(PartialEq, Debug, Eq, Clone)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Column type.
    pub r#type: Type,
    /// Column role.
    pub role: ColumnRole,
}

impl Column {
    #[must_use]
    pub fn default_value() -> Value {
        DEFAULT_VALUE.clone()
    }
}

/// Msgpack serializer for a column
impl SerSerialize for Column {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("name", &self.name)?;
        match &self.r#type {
            Type::Boolean => map.serialize_entry("type", "boolean")?,
            Type::Decimal => map.serialize_entry("type", "decimal")?,
            Type::Double => map.serialize_entry("type", "double")?,
            Type::Integer => map.serialize_entry("type", "integer")?,
            Type::Number => map.serialize_entry("type", "number")?,
            Type::Scalar => map.serialize_entry("type", "scalar")?,
            Type::String => map.serialize_entry("type", "string")?,
            Type::Unsigned => map.serialize_entry("type", "unsigned")?,
            Type::Array => map.serialize_entry("type", "array")?,
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
            "boolean" => Ok(Column::new(&column_name, Type::Boolean, ColumnRole::User)),
            "decimal" => Ok(Column::new(&column_name, Type::Decimal, ColumnRole::User)),
            "double" => Ok(Column::new(&column_name, Type::Double, ColumnRole::User)),
            "integer" => Ok(Column::new(&column_name, Type::Integer, ColumnRole::User)),
            "number" | "numeric" => Ok(Column::new(&column_name, Type::Number, ColumnRole::User)),
            "scalar" => Ok(Column::new(&column_name, Type::Scalar, ColumnRole::User)),
            "string" | "text" | "varchar" => {
                Ok(Column::new(&column_name, Type::String, ColumnRole::User))
            }
            "unsigned" => Ok(Column::new(&column_name, Type::Unsigned, ColumnRole::User)),
            "array" => Ok(Column::new(&column_name, Type::Array, ColumnRole::User)),
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
    pub fn new(n: &str, t: Type, role: ColumnRole) -> Self {
        Column {
            name: n.into(),
            r#type: t,
            role,
        }
    }

    /// Get column role.
    #[must_use]
    pub fn get_role(&self) -> &ColumnRole {
        &self.role
    }
}

/// Table is a tuple storage in the cluster.
///
/// Tables are tuple storages in the cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Table {
    /// List of the columns.
    pub columns: Vec<Column>,
    /// Distribution key of the output tuples (column positions).
    pub key: Key,
    /// Unique table name.
    name: String,
}

impl Table {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Table segment constructor.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the input arguments are invalid.
    pub fn new_seg(
        name: &str,
        columns: Vec<Column>,
        keys: &[&str],
    ) -> Result<Self, QueryPlannerError> {
        let mut pos_map: HashMap<&str, usize> = HashMap::new();
        let no_duplicates = &columns
            .iter()
            .enumerate()
            .all(|(pos, col)| matches!(pos_map.insert(&col.name, pos), None));

        if !no_duplicates {
            return Err(QueryPlannerError::CustomError(
                "Table has duplicated columns and couldn't be loaded".into(),
            ));
        }

        let positions = keys
            .iter()
            .map(|name| match pos_map.get(*name) {
                Some(pos) => Ok(*pos),
                None => Err(QueryPlannerError::InvalidShardingKey),
            })
            .collect::<Result<Vec<usize>, _>>()?;

        Ok(Table {
            name: name.into(),
            columns,
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
        let mut uniq_cols: HashSet<&str> = HashSet::new();
        let cols = ts.columns.clone();

        let no_duplicates = cols.iter().all(|col| uniq_cols.insert(&col.name));

        if !no_duplicates {
            return Err(QueryPlannerError::CustomError(
                "Table contains duplicate columns. Unable to convert to YAML.".into(),
            ));
        }

        let in_range = ts.key.positions.iter().all(|pos| *pos < cols.len());

        if !in_range {
            return Err(QueryPlannerError::ValueOutOfRange);
        }

        Ok(ts)
    }

    /// Get position of the `bucket_id` system column in the table.
    ///
    /// # Errors
    /// - Table doesn't have an exactly one `bucket_id` column.
    pub fn get_bucket_id_position(&self) -> Result<usize, QueryPlannerError> {
        let positions: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.role == ColumnRole::Sharding)
            .map(|(pos, _)| pos)
            .collect();
        match positions.len().cmp(&1) {
            Ordering::Equal => Ok(positions[0]),
            Ordering::Greater => Err(QueryPlannerError::CustomError(
                "Table has more than one bucket_id column".into(),
            )),
            Ordering::Less => Err(QueryPlannerError::CustomError(
                "Table has no bucket_id columns".into(),
            )),
        }
    }

    /// Get a vector of the sharding column names.
    ///
    /// # Errors
    /// - Table internal inconsistency.
    pub fn get_sharding_column_names(&self) -> Result<Vec<String>, QueryPlannerError> {
        let mut names: Vec<String> = Vec::with_capacity(self.key.positions.len());
        for pos in &self.key.positions {
            names.push(
                self.columns
                    .get(*pos)
                    .ok_or_else(|| {
                        QueryPlannerError::CustomError(format!(
                            "Table {} has no distribution column at position {}",
                            self.name, *pos
                        ))
                    })?
                    .name
                    .clone(),
            );
        }
        Ok(names)
    }

    #[must_use]
    pub fn get_sharding_positions(&self) -> &[usize] {
        &self.key.positions
    }
}

#[cfg(test)]
mod tests;
