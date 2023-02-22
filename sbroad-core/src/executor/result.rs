use core::fmt::Debug;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde::Deserialize;
use tarantool::tlua::{self, LuaRead};

use crate::debug;
use crate::errors::{Entity, SbroadError};
use crate::executor::vtable::VirtualTable;
use crate::ir::operator::Relational;
use crate::ir::relation::{Column, ColumnRole, Type};
use crate::ir::tree::traversal::{PostOrder, REL_CAPACITY};
use crate::ir::value::{EncodedValue, Value};
use crate::ir::Plan;

type ExecutorTuple = Vec<EncodedValue>;

#[derive(LuaRead, Debug, Deserialize, PartialEq, Eq, Clone)]
pub struct MetadataColumn {
    name: String,
    r#type: String,
}

impl MetadataColumn {
    #[must_use]
    pub fn new(name: String, r#type: String) -> Self {
        MetadataColumn { name, r#type }
    }
}

impl Serialize for MetadataColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("name", &self.name)?;
        map.serialize_entry("type", &self.r#type)?;
        map.end()
    }
}

impl TryInto<Column> for &MetadataColumn {
    type Error = SbroadError;

    fn try_into(self) -> Result<Column, Self::Error> {
        match self.r#type.as_str() {
            "boolean" => Ok(Column::new(&self.name, Type::Boolean, ColumnRole::User)),
            "decimal" => Ok(Column::new(&self.name, Type::Decimal, ColumnRole::User)),
            "double" => Ok(Column::new(&self.name, Type::Double, ColumnRole::User)),
            "integer" => Ok(Column::new(&self.name, Type::Integer, ColumnRole::User)),
            "number" | "numeric" => Ok(Column::new(&self.name, Type::Number, ColumnRole::User)),
            "scalar" => Ok(Column::new(&self.name, Type::Scalar, ColumnRole::User)),
            "string" | "text" | "varchar" => {
                Ok(Column::new(&self.name, Type::String, ColumnRole::User))
            }
            "unsigned" => Ok(Column::new(&self.name, Type::Unsigned, ColumnRole::User)),
            _ => Err(SbroadError::Unsupported(
                Entity::Type,
                Some(format!("column type {}", self.r#type)),
            )),
        }
    }
}

/// Results of query execution for `SELECT`.
#[allow(clippy::module_name_repetitions)]
#[derive(LuaRead, Debug, Deserialize, PartialEq, Clone)]
pub struct ProducerResult {
    pub metadata: Vec<MetadataColumn>,
    pub rows: Vec<ExecutorTuple>,
}

impl Default for ProducerResult {
    fn default() -> Self {
        Self::new()
    }
}

impl ProducerResult {
    /// Create an empty result set for a query producing tuples.
    #[allow(dead_code)]
    #[must_use]
    pub fn new() -> Self {
        ProducerResult {
            metadata: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Converts result to virtual table for linker
    ///
    /// # Errors
    /// - convert to virtual table error
    pub fn as_virtual_table(
        &self,
        column_names: Vec<String>,
        possibly_incorrect_types: bool,
    ) -> Result<VirtualTable, SbroadError> {
        let mut vtable = VirtualTable::new();

        for encoded_tuple in &self.rows {
            let tuple: Vec<Value> = encoded_tuple
                .iter()
                .map(|v| Value::from(v.clone()))
                .collect();
            vtable.add_tuple(tuple);
        }

        for col in &self.metadata {
            let column: Column = if possibly_incorrect_types {
                let column_type = Type::new_from_possibly_incorrect(&col.r#type)?;
                Column::new(&col.name, column_type, ColumnRole::User)
            } else {
                col.try_into()?
            };
            vtable.add_column(column);
        }
        debug!(
            Option::from("as_virtual_table"),
            &format!("virtual table columns: {:?}", vtable.get_columns())
        );

        for (vcol, name) in vtable
            .get_mut_columns()
            .iter_mut()
            .zip(column_names.into_iter().map(|qsq| {
                if let Some(qs) = qsq.strip_suffix('"') {
                    if let Some(s) = qs.strip_prefix('"') {
                        s.to_string()
                    } else {
                        qsq
                    }
                } else {
                    qsq
                }
            }))
        {
            vcol.name = name;
        }

        Ok(vtable)
    }
}

impl Serialize for ProducerResult {
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

/// Results of query execution for `INSERT`.
#[allow(clippy::module_name_repetitions)]
#[derive(LuaRead, Debug, PartialEq, Eq, Clone)]
pub struct ConsumerResult {
    pub row_count: u64,
}

impl Default for ConsumerResult {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsumerResult {
    /// Create an empty result set for a query consuming tuples.
    #[allow(dead_code)]
    #[must_use]
    pub fn new() -> Self {
        ConsumerResult { row_count: 0 }
    }
}

impl Serialize for ConsumerResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("row_count", &self.row_count)?;
        map.end()
    }
}

impl Plan {
    /// Checks if the plan contains a `Values` node.
    ///
    /// # Errors
    /// - If relational iterator fails to return a correct node.
    pub fn subtree_contains_values(&self, top_id: usize) -> Result<bool, SbroadError> {
        let mut rel_tree = PostOrder::with_capacity(|node| self.nodes.rel_iter(node), REL_CAPACITY);
        let nodes: Vec<usize> = rel_tree.iter(top_id).map(|(_, id)| id).collect();
        for node_id in nodes {
            let rel = self.get_relation_node(node_id)?;
            if let Relational::Values { .. } = rel {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests;
