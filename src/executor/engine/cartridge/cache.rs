//! Metadata cache module.

extern crate yaml_rust;

use std::collections::HashMap;

use yaml_rust::YamlLoader;

use crate::errors::QueryPlannerError;
use crate::executor::Metadata;
use crate::ir::relation::{Column, Table, Type};

use self::yaml_rust::yaml;

/// Cluster metadata information
///
/// Information based on tarantool cartridge schema. Cache knows nothing about bucket distribution in the cluster,
/// as it is managed by Tarantool's vshard module.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterAppConfig {
    /// Tarantool cartridge schema
    schema: yaml::Yaml,

    /// Execute response waiting timeout in seconds.
    waiting_timeout: u64,

    /// IR table segments from the cluster spaces
    tables: HashMap<String, Table>,
}

impl ClusterAppConfig {
    pub fn new() -> Self {
        ClusterAppConfig {
            schema: yaml::Yaml::Null,
            waiting_timeout: 360,
            tables: HashMap::new(),
        }
    }

    /// Load yaml cartridge schema to cache
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when process was terminated.
    pub fn load_schema(&mut self, s: &str) -> Result<(), QueryPlannerError> {
        if let Ok(docs) = YamlLoader::load_from_str(s) {
            if let Some(doc) = docs.get(0) {
                self.schema = doc.clone();
                self.init_table_segments()?;
                return Ok(());
            }
        }

        Err(QueryPlannerError::InvalidClusterSchema)
    }

    #[allow(dead_code)]
    pub fn get_sharding_key_by_space(self, space: &str) -> Vec<String> {
        let mut result = Vec::new();
        let spaces = self.schema["spaces"].as_hash().unwrap();

        for (space_name, params) in spaces.iter() {
            let current_space_name = space_name.as_str().unwrap();
            if current_space_name == space {
                for k in params["sharding_key"].as_vec().unwrap() {
                    result.push(Self::to_name(k.as_str().unwrap()));
                }
            }
        }
        result
    }

    pub(in crate::executor::engine::cartridge) fn is_empty(&self) -> bool {
        self.schema.is_null()
    }

    /// Transform space information from schema to table segments
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when schema contains errors.
    fn init_table_segments(&mut self) -> Result<(), QueryPlannerError> {
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
                            result.push(Column::new(&Self::to_name(name), t));
                        }
                        result
                    }
                    None => return Err(QueryPlannerError::SpaceFormatNotFound),
                };

                let keys: Vec<String> = match params["sharding_key"].as_vec() {
                    Some(keys) => {
                        let mut result = Vec::new();
                        for k in keys {
                            let key: &str = match k.as_str() {
                                Some(k) => k,
                                None => return Err(QueryPlannerError::InvalidColumnName),
                            };
                            result.push(Self::to_name(key));
                        }
                        result
                    }
                    None => return Err(QueryPlannerError::SpaceFormatNotFound),
                };

                let table_name: String = ClusterAppConfig::to_name(current_space_name);
                let keys_str = keys.iter().map(String::as_str).collect::<Vec<&str>>();
                let t = Table::new_seg(&table_name, fields, keys_str.as_slice())?;
                self.tables.insert(table_name, t);
            } else {
                return Err(QueryPlannerError::InvalidSpaceName);
            }
        }

        Ok(())
    }

    /// Setup response waiting timeout for executor
    pub fn set_exec_waiting_timeout(&mut self, timeout: u64) {
        if timeout > 0 {
            self.waiting_timeout = timeout;
        }
    }
}

impl Metadata for ClusterAppConfig {
    /// Get table segment form cache by table name
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when table was not found.
    #[allow(dead_code)]
    fn get_table_segment(&self, table_name: &str) -> Result<Table, QueryPlannerError> {
        match self.tables.get(table_name) {
            Some(v) => Ok(v.clone()),
            None => Err(QueryPlannerError::SpaceNotFound),
        }
    }

    /// Get response waiting timeout for executor
    fn get_exec_waiting_timeout(&self) -> u64 {
        self.waiting_timeout
    }
}

#[cfg(test)]
mod tests;
