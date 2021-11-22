use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Type {
    Boolean,
    Number,
    String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Column {
    name: String,
    type_name: Type,
}

#[allow(dead_code)]
impl Column {
    fn new(n: &str, t: Type) -> Self {
        Column {
            name: n.into(),
            type_name: t,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TableShard {
    name: String,
    columns: Vec<Column>,
    sharding_key: Vec<usize>,
}

#[allow(dead_code)]
impl TableShard {
    fn new(n: &str, c: Vec<Column>, k: &[&str]) -> Result<Self, QueryPlannerError> {
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

        Ok(TableShard {
            name: n.into(),
            columns: c,
            sharding_key: positions,
        })
    }

    fn from_yaml(s: &str) -> Result<Self, QueryPlannerError> {
        let ts: TableShard = match serde_yaml::from_str(s) {
            Ok(t) => t,
            Err(_) => return Err(QueryPlannerError::Serialization),
        };
        let mut uniq_cols: HashSet<&str> = HashSet::new();
        let cols = &ts.columns;

        let no_duplicates = cols.iter().all(|col| uniq_cols.insert(&col.name));

        if !no_duplicates {
            return Err(QueryPlannerError::DuplicateColumn);
        }

        let keys = &ts.sharding_key;
        let in_range = keys.iter().all(|pos| *pos < cols.len());

        if !in_range {
            return Err(QueryPlannerError::ValueOutOfRange);
        }

        Ok(ts)
    }
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
    fn table() {
        let t = TableShard::new(
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
        assert_eq!(2, t.sharding_key.len());
        assert_eq!(0, t.sharding_key[1]);
        assert_eq!(1, t.sharding_key[0]);
    }

    #[test]
    fn table_duplicate_columns() {
        assert_eq!(
            TableShard::new(
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
    fn table_wrong_sharding_key() {
        assert_eq!(
            TableShard::new(
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
    fn table_serialized() {
        let t = TableShard::new(
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
            .join("table_serialized.yaml");
        let s = fs::read_to_string(path).unwrap();
        let t_yaml = TableShard::from_yaml(&s).unwrap();
        assert_eq!(t, t_yaml);
    }

    #[test]
    fn table_serialized_duplicate_columns() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("relation")
            .join("table_serialized_duplicate_columns.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            TableShard::from_yaml(&s).unwrap_err(),
            QueryPlannerError::DuplicateColumn
        );
    }

    #[test]
    fn table_serialized_out_of_range_sharding_key() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("relation")
            .join("table_serialized_out_of_range_sharding_key.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            TableShard::from_yaml(&s).unwrap_err(),
            QueryPlannerError::ValueOutOfRange
        );
    }

    #[test]
    fn table_serialized_no_sharding_key() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("relation")
            .join("table_serialized_no_sharding_key.yaml");
        let s = fs::read_to_string(path).unwrap();
        let t = TableShard::from_yaml(&s);
        assert_eq!(t.unwrap_err(), QueryPlannerError::Serialization);
    }

    #[test]
    fn table_serialized_no_columns() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("relation")
            .join("table_serialized_no_columns.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            TableShard::from_yaml(&s).unwrap_err(),
            QueryPlannerError::Serialization
        );
    }
}
