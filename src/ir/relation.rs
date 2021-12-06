//! Relation module.

use super::value::Value;
use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Supported column types.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Type {
    Boolean,
    Number,
    String,
}

/// Relation column.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Column type.
    pub type_name: Type,
}

#[allow(dead_code)]
impl Column {
    /// Column constructor.
    #[must_use]
    pub fn new(n: &str, t: Type) -> Self {
        Column {
            name: n.into(),
            type_name: t,
        }
    }
}

/// Table is a tuple storage in the cluster.
///
/// Tables are the tuple storages in the cluster.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Table {
    /// Already existing table segment on some cluster data node.
    Segment {
        /// List of the columns.
        columns: Vec<Column>,
        /// Distribution key of the output tuples (column positions).
        key: Vec<usize>,
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
        key: Vec<usize>,
        /// Unique table name (we need to generate it ourselves).
        name: String,
    },
}

#[allow(dead_code)]
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
            key: positions,
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

            let keys = key;
            let in_range = keys.iter().all(|pos| *pos < cols.len());

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
mod tests {
    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};
    use std::fs;
    use std::path::Path;

    #[test]
    fn column() {
        let a = Column {
            name: String::from("a"),
            type_name: Type::Boolean,
        };
        assert_eq!(a, Column::new("a", Type::Boolean));
        assert_ne!(a, Column::new("a", Type::String));
        assert_ne!(a, Column::new("b", Type::Boolean));
    }

    #[test]
    fn table_seg() {
        let t = Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean),
                Column::new("b", Type::Number),
                Column::new("c", Type::String),
                Column::new("d", Type::String),
            ],
            &["b", "a"],
        )
        .unwrap();
        if let Table::Segment { key, .. } = &t {
            assert_eq!(2, key.len());
            assert_eq!(0, key[1]);
            assert_eq!(1, key[0]);
        }
    }

    #[test]
    fn table_seg_name() {
        let t = Table::new_seg("t", vec![Column::new("a", Type::Boolean)], &["a"]).unwrap();
        assert_eq!("t", t.name());
    }

    #[test]
    fn table_seg_duplicate_columns() {
        assert_eq!(
            Table::new_seg(
                "t",
                vec![
                    Column::new("a", Type::Boolean),
                    Column::new("b", Type::Number),
                    Column::new("c", Type::String),
                    Column::new("a", Type::String),
                ],
                &["b", "a"],
            )
            .unwrap_err(),
            QueryPlannerError::DuplicateColumn
        );
    }

    #[test]
    fn table_seg_wrong_key() {
        assert_eq!(
            Table::new_seg(
                "t",
                vec![
                    Column::new("a", Type::Boolean),
                    Column::new("b", Type::Number),
                    Column::new("c", Type::String),
                    Column::new("d", Type::String),
                ],
                &["a", "e"],
            )
            .unwrap_err(),
            QueryPlannerError::InvalidShardingKey
        );
    }

    #[test]
    fn table_seg_serialized() {
        let t = Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean),
                Column::new("b", Type::Number),
                Column::new("c", Type::String),
                Column::new("d", Type::String),
            ],
            &["a", "d"],
        )
        .unwrap();
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("relation")
            .join("table_seg_serialized.yaml");
        let s = fs::read_to_string(path).unwrap();
        let t_yaml = Table::seg_from_yaml(&s).unwrap();
        assert_eq!(t, t_yaml);
    }

    #[test]
    fn table_seg_serialized_duplicate_columns() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("relation")
            .join("table_seg_serialized_duplicate_columns.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            Table::seg_from_yaml(&s).unwrap_err(),
            QueryPlannerError::DuplicateColumn
        );
    }

    #[test]
    fn table_seg_serialized_out_of_range_key() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("relation")
            .join("table_seg_serialized_out_of_range_key.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            Table::seg_from_yaml(&s).unwrap_err(),
            QueryPlannerError::ValueOutOfRange
        );
    }

    #[test]
    fn table_seg_serialized_no_key() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("relation")
            .join("table_seg_serialized_no_key.yaml");
        let s = fs::read_to_string(path).unwrap();
        let t = Table::seg_from_yaml(&s);
        assert_eq!(t.unwrap_err(), QueryPlannerError::Serialization);
    }

    #[test]
    fn table_seg_serialized_no_columns() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("relation")
            .join("table_seg_serialized_no_columns.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            Table::seg_from_yaml(&s).unwrap_err(),
            QueryPlannerError::Serialization
        );
    }
}
