extern crate yaml_rust;

use std::collections::HashMap;

use yaml_rust::YamlLoader;

use crate::errors::QueryPlannerError;
use crate::ir::relation::{Column, Table, Type};

use self::yaml_rust::yaml;

/// Cluster metadata information
///
/// Information based on tarantool cartridge schema. Cache knows nothing about bucket distribution in the cluster,
/// as it is managed by Tarantool's vshard module.
#[derive(Debug, Clone, PartialEq)]
pub struct Metadata {
    /// Tarantool cartridge schema
    schema: yaml::Yaml,

    /// IR table segments from the cluster spaces
    tables: HashMap<String, Table>,
}

impl Metadata {
    pub fn new() -> Self {
        Metadata {
            schema: yaml::Yaml::Null,
            tables: HashMap::new(),
        }
    }

    /// Load yaml cartridge schema to cache
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when process was terminated.
    pub fn load(&mut self, s: &str) -> Result<(), QueryPlannerError> {
        if let Ok(docs) = YamlLoader::load_from_str(s) {
            self.schema = docs[0].clone();
            self.init_table_segments()?;
            return Ok(());
        }

        Err(QueryPlannerError::InvalidClusterSchema)
    }

    pub fn is_empty(&self) -> bool {
        self.schema.is_null()
    }

    pub fn get_sharding_key_by_space(self, space: &str) -> Vec<String> {
        let mut result = Vec::new();
        let spaces = self.schema["spaces"].as_hash().unwrap();

        for (space_name, params) in spaces.iter() {
            let current_space_name = space_name.as_str().unwrap();
            if current_space_name == space {
                for k in params["sharding_key"].as_vec().unwrap() {
                    result.push(k.as_str().unwrap().to_string().to_lowercase());
                }
            }
        }
        result
    }

    /// Get table segment form cache by table name
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when table was not found.
    #[allow(dead_code)]
    pub fn get_table_segment(&self, table_name: &str) -> Result<Table, QueryPlannerError> {
        match self.tables.get(table_name) {
            Some(v) => Ok(v.clone()),
            None => Err(QueryPlannerError::SpaceNotFound),
        }
    }

    /// Transform space information from schema to table segments
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when schema contains errors.
    pub fn init_table_segments(&mut self) -> Result<(), QueryPlannerError> {
        self.tables.clear();
        let spaces = match self.schema["spaces"].as_hash() {
            Some(v) => v,
            None => return Err(QueryPlannerError::InvalidSchemaSpaces),
        };

        for (space_name, params) in spaces.iter() {
            if let Some(current_space_name) = space_name.as_str() {
                let fields = match params["format"].as_vec() {
                    Some(fields) => {
                        let mut result = Vec::new();
                        for val in fields {
                            let name: &str = match val["name"].as_str() {
                                Some(s) => s,
                                None => return Err(QueryPlannerError::InvalidColumnName),
                            };
                            let t = match val["type"].as_str() {
                                Some(t) => Type::new(t)?,
                                None => return Err(QueryPlannerError::TypeNotImplemented),
                            };
                            result.push(Column::new(name, t));
                        }
                        result
                    }
                    None => return Err(QueryPlannerError::SpaceFormatNotFound),
                };

                let keys = match params["sharding_key"].as_vec() {
                    Some(keys) => {
                        let mut result = Vec::new();
                        for k in keys {
                            let key: &str = match k.as_str() {
                                Some(k) => k,
                                None => return Err(QueryPlannerError::InvalidColumnName),
                            };
                            result.push(key);
                        }
                        result
                    }
                    None => return Err(QueryPlannerError::SpaceFormatNotFound),
                };

                let t = Table::new_seg(current_space_name, fields, &keys)?;
                self.tables.insert(current_space_name.to_string(), t);
            } else {
                return Err(QueryPlannerError::InvalidSpaceName);
            }
        }

        Ok(())
    }
}

#[test]
fn test_yaml_schema_parser() {
    let test_schema = "spaces:
  EMPLOYEES:
    engine: \"memtx\"
    is_local: false
    temporary: false
    format:
      - name: \"ID\"
        is_nullable: false
        type: \"number\"
      - name: \"sysFrom\"
        is_nullable: false
        type: \"number\"
      - name: \"FIRST_NAME\"
        is_nullable: false
        type: \"string\"
      - name: \"sysOp\"
        is_nullable: false
        type: \"number\"
      - name: \"bucket_id\"
        is_nullable: true
        type: \"unsigned\"
    indexes:
      - type: \"TREE\"
        name: \"ID\"
        unique: true
        parts:
          - path: \"ID\"
            type: \"number\"
            is_nullable: false
          - path: \"sysFrom\"
            type: \"number\"
            is_nullable: false
      - type: \"TREE\"
        name: \"bucket_id\"
        unique: false
        parts:
          - path: \"bucket_id\"
            type: \"unsigned\"
            is_nullable: true
    sharding_key:
      - ID
  hash_testing:
    is_local: false
    temporary: false
    engine: \"memtx\"
    format:
      - name: \"identification_number\"
        type: \"integer\"
        is_nullable: false
      - name: \"product_code\"
        type: \"string\"
        is_nullable: false
      - name: \"product_units\"
        type: \"integer\"
        is_nullable: false
      - name: \"sys_op\"
        type: \"number\"
        is_nullable: false
      - name: \"bucket_id\"
        type: \"unsigned\"
        is_nullable: true
    indexes:
      - name: \"id\"
        unique: true
        type: \"TREE\"
        parts:
          - path: \"identification_number\"
            is_nullable: false
            type: \"integer\"
      - name: bucket_id
        unique: false
        parts:
          - path: \"bucket_id\"
            is_nullable: true
            type: \"unsigned\"
        type: \"TREE\"
    sharding_key:
      - identification_number
      - product_code";

    let mut s = Metadata::new();
    s.load(test_schema).unwrap();

    let mut expected_keys = Vec::new();
    expected_keys.push("identification_number".to_string());
    expected_keys.push("product_code".to_string());

    let actual_keys = s.get_sharding_key_by_space("hash_testing");
    assert_eq!(actual_keys, expected_keys)
}

#[test]
fn test_getting_table_segment() {
    let test_schema = "spaces:
  hash_testing:
    is_local: false
    temporary: false
    engine: \"memtx\"
    format:
      - name: \"identification_number\"
        type: \"integer\"
        is_nullable: false
      - name: \"product_code\"
        type: \"string\"
        is_nullable: false
      - name: \"product_units\"
        type: \"boolean\"
        is_nullable: false
      - name: \"sys_op\"
        type: \"number\"
        is_nullable: false
      - name: \"bucket_id\"
        type: \"unsigned\"
        is_nullable: true
    indexes:
      - name: \"id\"
        unique: true
        type: \"TREE\"
        parts:
          - path: \"identification_number\"
            is_nullable: false
            type: \"integer\"
      - name: bucket_id
        unique: false
        parts:
          - path: \"bucket_id\"
            is_nullable: true
            type: \"unsigned\"
        type: \"TREE\"
    sharding_key:
      - identification_number
      - product_code";

    let mut s = Metadata::new();
    s.load(test_schema).unwrap();

    let expected = Table::new_seg(
        "hash_testing",
        vec![
            Column::new("identification_number", Type::Integer),
            Column::new("product_code", Type::String),
            Column::new("product_units", Type::Boolean),
            Column::new("sys_op", Type::Number),
            Column::new("bucket_id", Type::Unsigned),
        ],
        &["identification_number", "product_code"],
    )
    .unwrap();

    assert_eq!(
        s.get_table_segment("invalid_table").unwrap_err(),
        QueryPlannerError::SpaceNotFound
    );
    assert_eq!(s.get_table_segment("hash_testing").unwrap(), expected)
}
