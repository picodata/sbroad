//! Metadata cache module.

extern crate yaml_rust;

use std::collections::HashMap;
use yaml_rust::YamlLoader;

use crate::errors::QueryPlannerError;
use crate::executor::engine::cartridge::cache::lru::DEFAULT_CAPACITY;
use crate::executor::Metadata;
use crate::ir::relation::{Column, ColumnRole, Table, Type};

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

    /// Query cache capacity.
    cache_capacity: usize,

    /// Sharding column names.
    sharding_column: String,

    /// IR table segments from the cluster spaces
    tables: HashMap<String, Table>,
}

impl Default for ClusterAppConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterAppConfig {
    #[must_use]
    pub fn new() -> Self {
        ClusterAppConfig {
            schema: yaml::Yaml::Null,
            waiting_timeout: 360,
            cache_capacity: DEFAULT_CAPACITY,
            tables: HashMap::new(),
            sharding_column: String::new(),
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
                            let qualified_name = Self::to_name(name);
                            let role = if self.get_sharding_column().eq(&qualified_name) {
                                ColumnRole::Sharding
                            } else {
                                ColumnRole::User
                            };
                            let col = Column::new(&qualified_name, t, role);
                            result.push(col);
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

    pub fn set_exec_cache_capacity(&mut self, capacity: usize) {
        if capacity > 0 {
            self.cache_capacity = capacity;
        } else {
            self.cache_capacity = DEFAULT_CAPACITY;
        }
    }

    pub fn set_exec_sharding_column(&mut self, column: String) {
        self.sharding_column = column;
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

    fn get_sharding_column(&self) -> &str {
        self.sharding_column.as_str()
    }

    /// Get sharding keys by space name
    fn get_sharding_key_by_space(&self, space: &str) -> Result<Vec<&str>, QueryPlannerError> {
        if let Some(vec) = self.schema["spaces"][space]["sharding_key"].as_vec() {
            return vec
                .iter()
                .try_fold(Vec::new(), |mut acc: Vec<&str>, str| match str.as_str() {
                    Some(val) => {
                        acc.push(val);
                        Ok(acc)
                    }
                    _ => Err(QueryPlannerError::CustomError(format!(
                        "Schema {} contains incorrect sharding keys format",
                        space
                    ))),
                });
        }
        Err(QueryPlannerError::CustomError(format!(
            "Failed to get space {} or sharding key",
            space
        )))
    }
}

pub mod lru;
#[cfg(test)]
mod tests;
