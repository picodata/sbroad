//! Relation module.
//!
//! Contains following structs:
//! * Column type (`Type`)
//! * Table column (`Column`)
//! * Engine (memtx/vinyl), used by a table (`SpaceEngine`)
//! * Table, representing unnamed tuples storage (`Table`)
//! * Relation, representing named tables (`Relations` as a map of { name -> table })

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Formatter};
use tarantool::index::Metadata as IndexMetadata;
use tarantool::space::{Field, FieldType as SpaceFieldType, Space, SpaceEngineType, SystemSpace};
use tarantool::tuple::{FieldType, KeyDef, KeyDefPart};
use tarantool::util::NumOrStr;

use serde::de::{Error, MapAccess, Visitor};
use serde::ser::{Serialize as SerSerialize, SerializeMap, Serializer};
use serde::{Deserialize, Deserializer, Serialize};

use crate::errors::{Action, Entity, SbroadError};
use crate::ir::value::Value;

use super::distribution::Key;

const DEFAULT_VALUE: Value = Value::Null;

/// Supported column types, which is used in a schema only.
/// This `Type` is derived from the result's metadata.
#[derive(Serialize, Default, Deserialize, PartialEq, Hash, Debug, Eq, Clone)]
pub enum Type {
    Array,
    Boolean,
    Decimal,
    Double,
    Integer,
    #[default]
    Scalar,
    String,
    Number,
    Unsigned,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Type::Array => write!(f, "array"),
            Type::Boolean => write!(f, "boolean"),
            Type::Decimal => write!(f, "decimal"),
            Type::Double => write!(f, "double"),
            Type::Integer => write!(f, "integer"),
            Type::Scalar => write!(f, "scalar"),
            Type::String => write!(f, "string"),
            Type::Number => write!(f, "number"),
            Type::Unsigned => write!(f, "unsigned"),
        }
    }
}

impl From<&Type> for FieldType {
    fn from(data_type: &Type) -> Self {
        match data_type {
            Type::Boolean => FieldType::Boolean,
            Type::Decimal => FieldType::Decimal,
            Type::Double => FieldType::Double,
            Type::Integer => FieldType::Integer,
            Type::Number => FieldType::Number,
            Type::Scalar => FieldType::Scalar,
            Type::String => FieldType::String,
            Type::Unsigned => FieldType::Unsigned,
            Type::Array => FieldType::Array,
        }
    }
}

impl From<&Type> for SpaceFieldType {
    fn from(data_type: &Type) -> Self {
        match data_type {
            Type::Boolean => SpaceFieldType::Boolean,
            Type::Decimal => SpaceFieldType::Decimal,
            Type::Double => SpaceFieldType::Double,
            Type::Integer => SpaceFieldType::Integer,
            Type::Number => SpaceFieldType::Number,
            Type::Scalar => SpaceFieldType::Scalar,
            Type::String => SpaceFieldType::String,
            Type::Unsigned => SpaceFieldType::Unsigned,
            Type::Array => SpaceFieldType::Array,
        }
    }
}

impl Type {
    /// Type constructor
    ///
    /// # Errors
    /// - Invalid type name.
    pub fn new(s: &str) -> Result<Self, SbroadError> {
        match s.to_string().to_lowercase().as_str() {
            "boolean" => Ok(Type::Boolean),
            "decimal" => Ok(Type::Decimal),
            "double" => Ok(Type::Double),
            "integer" => Ok(Type::Integer),
            "number" => Ok(Type::Number),
            "scalar" => Ok(Type::Scalar),
            "string" | "text" => Ok(Type::String),
            "unsigned" => Ok(Type::Unsigned),
            "array" => Ok(Type::Array),
            v => Err(SbroadError::NotImplemented(Entity::Type, v.to_string())),
        }
    }

    /// Type constructor (in a case of the possibly incorrect input -
    /// VALUES with the first NULL row can return incorrect type in the metadata).
    ///
    /// # Errors
    /// - Invalid type name.
    pub fn new_from_possibly_incorrect(s: &str) -> Result<Self, SbroadError> {
        match s.to_string().to_lowercase().as_str() {
            "boolean" | "decimal" | "double" | "integer" | "number" | "numeric" | "scalar"
            | "string" | "text" | "unsigned" => Ok(Type::Scalar),
            "array" => Ok(Type::Array),
            v => Err(SbroadError::NotImplemented(Entity::Type, v.to_string())),
        }
    }

    /// The type of the column is scalar.
    /// Only scalar types can be used as a distribution key.
    #[must_use]
    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            Type::Boolean
                | Type::Decimal
                | Type::Double
                | Type::Integer
                | Type::Number
                | Type::Scalar
                | Type::String
                | Type::Unsigned
        )
    }
}

/// A role of the column in the relation.
#[derive(Default, PartialEq, Debug, Eq, Clone)]
pub enum ColumnRole {
    /// General purpose column available for the user.
    #[default]
    User,
    /// Column is used for sharding (contains `bucket_id` in terms of `vshard`).
    Sharding,
}

/// Table column.
#[derive(Default, PartialEq, Debug, Eq, Clone)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Column type.
    pub r#type: Type,
    /// Column role.
    pub role: ColumnRole,
}

impl From<Column> for Field {
    fn from(column: Column) -> Self {
        let field = match column.r#type {
            Type::Boolean => Field::boolean(column.name),
            Type::Decimal => Field::decimal(column.name),
            Type::Double => Field::double(column.name),
            Type::Integer => Field::integer(column.name),
            Type::Number => Field::number(column.name),
            Type::Scalar => Field::scalar(column.name),
            Type::String => Field::string(column.name),
            Type::Unsigned => Field::unsigned(column.name),
            Type::Array => Field::array(column.name),
        };
        field.is_nullable(true)
    }
}

impl From<&Column> for FieldType {
    fn from(column: &Column) -> Self {
        FieldType::from(&column.r#type)
    }
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
        let mut map = serializer.serialize_map(Some(3))?;
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
        map.serialize_entry(
            "role",
            match self.role {
                ColumnRole::User => "user",
                ColumnRole::Sharding => "sharding",
            },
        )?;

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
        let mut column_role = String::new();
        while let Some((key, value)) = map.next_entry::<String, String>()? {
            match key.as_str() {
                "name" => column_name.push_str(&value),
                "type" => column_type.push_str(&value.to_lowercase()),
                "role" => column_role.push_str(&value.to_lowercase()),
                _ => return Err(Error::custom(&format!("invalid column param: {key}"))),
            }
        }

        let role: ColumnRole = match column_role.as_str() {
            "sharding" => ColumnRole::Sharding,
            _ => ColumnRole::User,
        };

        match column_type.as_str() {
            "boolean" => Ok(Column::new(&column_name, Type::Boolean, role)),
            "decimal" => Ok(Column::new(&column_name, Type::Decimal, role)),
            "double" => Ok(Column::new(&column_name, Type::Double, role)),
            "integer" => Ok(Column::new(&column_name, Type::Integer, role)),
            "number" | "numeric" => Ok(Column::new(&column_name, Type::Number, role)),
            "scalar" => Ok(Column::new(&column_name, Type::Scalar, role)),
            "string" | "text" | "varchar" => Ok(Column::new(&column_name, Type::String, role)),
            "unsigned" => Ok(Column::new(&column_name, Type::Unsigned, role)),
            "array" => Ok(Column::new(&column_name, Type::Array, role)),
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

/// Space engine type.
/// Duplicates tarantool module's `SpaceEngine` enum.
/// The reason for duplication - `SpaceEngine` can't be
/// deserialized with `serde_yaml` crate as it doesn't support
/// borrowed strings.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum SpaceEngine {
    Memtx,
    Vinyl,
}

impl From<SpaceEngineType> for SpaceEngine {
    #[inline]
    fn from(engine_type: SpaceEngineType) -> Self {
        Self::from(&engine_type)
    }
}

impl From<&SpaceEngineType> for SpaceEngine {
    #[inline]
    fn from(engine_type: &SpaceEngineType) -> Self {
        #[allow(unreachable_patterns)]
        match engine_type {
            SpaceEngineType::Memtx => SpaceEngine::Memtx,
            SpaceEngineType::Vinyl => SpaceEngine::Vinyl,
            system_space => panic!("Space engine '{system_space:?}' is not supported"),
        }
    }
}

impl From<SpaceEngine> for SpaceEngineType {
    fn from(space_type: SpaceEngine) -> Self {
        match space_type {
            SpaceEngine::Memtx => SpaceEngineType::Memtx,
            SpaceEngine::Vinyl => SpaceEngineType::Vinyl,
        }
    }
}

impl From<&SpaceEngine> for SpaceEngineType {
    fn from(space_type: &SpaceEngine) -> Self {
        match space_type {
            SpaceEngine::Memtx => SpaceEngineType::Memtx,
            SpaceEngine::Vinyl => SpaceEngineType::Vinyl,
        }
    }
}

impl TryFrom<&str> for SpaceEngine {
    type Error = SbroadError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "memtx" => Ok(SpaceEngine::Memtx),
            "vinyl" => Ok(SpaceEngine::Vinyl),
            _ => Err(SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::SpaceEngine),
                format!("unsupported space engine type: {value}"),
            )),
        }
    }
}

/// Table is a tuple storage in the cluster.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Table {
    /// List of the columns.
    pub columns: Vec<Column>,
    /// Sharding key of the output tuples (column positions).
    pub shard_key: Key,
    /// Primary key of the table (column positions).
    pub primary_key: Key,
    /// Unique table name.
    pub(crate) name: String,
    /// Table engine.
    pub engine: SpaceEngine,
}

impl Table {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Table segment constructor.
    ///
    /// # Errors
    /// Returns `SbroadError` when the input arguments are invalid.
    pub fn new_seg(
        name: &str,
        columns: Vec<Column>,
        sharding_key: &[&str],
        primary_key: &[&str],
        engine: SpaceEngine,
    ) -> Result<Self, SbroadError> {
        let mut pos_map: HashMap<&str, usize> = HashMap::new();
        let no_duplicates = &columns
            .iter()
            .enumerate()
            .all(|(pos, col)| matches!(pos_map.insert(&col.name, pos), None));

        if !no_duplicates {
            return Err(SbroadError::DuplicatedValue(
                "Table has duplicated columns and couldn't be loaded".into(),
            ));
        }

        let shard_positions = sharding_key
            .iter()
            .map(|name| match pos_map.get(*name) {
                Some(pos) => {
                    // Check that the column type is scalar.
                    // Compound types are not supported as sharding keys.
                    let column = &columns.get(*pos).ok_or_else(|| {
                        SbroadError::FailedTo(
                            Action::Create,
                            Some(Entity::Column),
                            format!("column {name} not found at position {pos}"),
                        )
                    })?;
                    if !column.r#type.is_scalar() {
                        return Err(SbroadError::Invalid(
                            Entity::Column,
                            Some(format!("column {name} at position {pos} is not scalar",)),
                        ));
                    }
                    Ok(*pos)
                }
                None => Err(SbroadError::Invalid(Entity::ShardingKey, None)),
            })
            .collect::<Result<Vec<usize>, _>>()?;

        let primary_positions = primary_key
            .iter()
            .map(|name| match pos_map.get(*name) {
                Some(pos) => {
                    let _ = &columns.get(*pos).ok_or_else(|| {
                        SbroadError::FailedTo(
                            Action::Create,
                            Some(Entity::Column),
                            format!("column {name} not found at position {pos}"),
                        )
                    })?;
                    Ok(*pos)
                }
                None => Err(SbroadError::Invalid(Entity::PrimaryKey, None)),
            })
            .collect::<Result<Vec<usize>, _>>()?;

        Ok(Table {
            name: name.into(),
            columns,
            shard_key: Key::new(shard_positions),
            primary_key: Key::new(primary_positions),
            engine,
        })
    }

    /// Table segment from YAML.
    ///
    /// # Errors
    /// Returns `SbroadError` when the YAML-serialized table is invalid.
    pub fn seg_from_yaml(s: &str) -> Result<Self, SbroadError> {
        let ts: Table = match serde_yaml::from_str(s) {
            Ok(t) => t,
            Err(e) => {
                return Err(SbroadError::FailedTo(
                    Action::Serialize,
                    Some(Entity::Table),
                    format!("{e:?}"),
                ))
            }
        };
        let mut uniq_cols: HashSet<&str> = HashSet::new();
        let cols = ts.columns.clone();

        let no_duplicates = cols.iter().all(|col| uniq_cols.insert(&col.name));

        if !no_duplicates {
            return Err(SbroadError::DuplicatedValue(
                "Table contains duplicate columns. Unable to convert to YAML.".into(),
            ));
        }

        let in_range = ts.shard_key.positions.iter().all(|pos| *pos < cols.len());

        if !in_range {
            return Err(SbroadError::Invalid(
                Entity::Value,
                Some(format!("key positions must be less than {}", cols.len())),
            ));
        }

        Ok(ts)
    }

    /// Get position of the `bucket_id` system column in the table.
    ///
    /// # Errors
    /// - Table doesn't have an exactly one `bucket_id` column.
    pub fn get_bucket_id_position(&self) -> Result<usize, SbroadError> {
        let positions: Vec<usize> = self
            .columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.role == ColumnRole::Sharding)
            .map(|(pos, _)| pos)
            .collect();
        match positions.len().cmp(&1) {
            Ordering::Equal => Ok(positions[0]),
            Ordering::Greater => Err(SbroadError::UnexpectedNumberOfValues(
                "Table has more than one bucket_id column".into(),
            )),
            Ordering::Less => Err(SbroadError::UnexpectedNumberOfValues(
                "Table has no bucket_id columns".into(),
            )),
        }
    }

    /// Get a vector of the sharding column names.
    ///
    /// # Errors
    /// - Table internal inconsistency.
    pub fn get_sharding_column_names(&self) -> Result<Vec<String>, SbroadError> {
        let mut names: Vec<String> = Vec::with_capacity(self.shard_key.positions.len());
        for pos in &self.shard_key.positions {
            names.push(
                self.columns
                    .get(*pos)
                    .ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Column,
                            format!(
                                "(distribution column) at position {} for Table {}",
                                *pos, self.name
                            ),
                        )
                    })?
                    .name
                    .clone(),
            );
        }
        Ok(names)
    }

    #[must_use]
    pub fn get_sharding_positions(&self) -> &[usize] {
        &self.shard_key.positions
    }

    /// Get a sharding key definition for the table.
    ///
    /// # Errors
    /// - Table internal inconsistency.
    /// - Invalid sharding key position.
    pub fn get_key_def(&self) -> Result<KeyDef, SbroadError> {
        let mut parts = Vec::with_capacity(self.get_sharding_positions().len());
        for pos in self.get_sharding_positions() {
            let column = self.columns.get(*pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Column,
                    format!(
                        "(distribution column) at position {} for Table {}",
                        *pos, self.name
                    ),
                )
            })?;
            let field_no = u32::try_from(*pos).map_err(|e| {
                SbroadError::Invalid(
                    Entity::Table,
                    Some(format!("sharding key (position {pos}) error: {e}")),
                )
            })?;
            let part = KeyDefPart {
                field_no,
                field_type: FieldType::from(column),
                is_nullable: true,
                ..Default::default()
            };
            parts.push(part);
        }
        KeyDef::new(&parts).map_err(|e| SbroadError::Invalid(Entity::Table, Some(e.to_string())))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Relations {
    pub tables: HashMap<String, Table>,
}

impl Default for Relations {
    fn default() -> Self {
        Self::new()
    }
}

impl Relations {
    #[must_use]
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn insert(&mut self, table: Table) {
        self.tables.insert(table.name().into(), table);
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn drain(&mut self) -> HashMap<String, Table> {
        std::mem::take(&mut self.tables)
    }
}

/// Retrieve primary key columns for a space.
///
/// # Errors
/// - Space not found or invalid.
pub fn space_pk_columns(
    space_name: &str,
    space_columns: &[Column],
) -> Result<Vec<String>, SbroadError> {
    let space = Space::find(space_name)
        .ok_or_else(|| SbroadError::NotFound(Entity::Space, space_name.to_string()))?;
    let index: Space = SystemSpace::Index.into();
    let tuple = index
        .get(&[space.id(), 0])
        .map_err(|e| SbroadError::FailedTo(Action::Get, Some(Entity::Index), format!("{e}")))?
        .ok_or_else(|| {
            SbroadError::NotFound(Entity::PrimaryKey, format!("for space {space_name}"))
        })?;
    let pk_meta = tuple.decode::<IndexMetadata>().map_err(|e| {
        SbroadError::FailedTo(Action::Decode, Some(Entity::PrimaryKey), format!("{e}"))
    })?;
    let mut primary_key = Vec::with_capacity(pk_meta.parts.len());
    for part in pk_meta.parts {
        let col_pos = if let NumOrStr::Num(pos) = part.field {
            pos as usize
        } else {
            return Err(SbroadError::Invalid(
                Entity::PrimaryKey,
                Some(format!(
                    "part of {space_name} has unexpected format: {part:?}"
                )),
            ));
        };
        let col = space_columns
            .get(col_pos)
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::PrimaryKey,
                    Some(format!(
                        "{space_name} part referes to unknown column position: {col_pos}"
                    )),
                )
            })?
            .name
            .clone();
        primary_key.push(col);
    }
    Ok(primary_key)
}
#[cfg(test)]
mod tests;
