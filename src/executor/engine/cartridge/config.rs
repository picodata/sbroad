//! Cartridge configuration cache module.

extern crate yaml_rust;

use std::collections::HashMap;
use yaml_rust::{Yaml, YamlLoader};

use crate::errors::QueryPlannerError;
use crate::executor::engine::{normalize_name_from_schema, normalize_name_from_sql};
use crate::executor::lru::DEFAULT_CAPACITY;
use crate::executor::CoordinatorMetadata;
use crate::ir::relation::{Column, ColumnRole, Table, Type};

#[cfg(not(feature = "mock"))]
use tarantool::log::{say, SayLevel};

/// Cluster metadata information
///
/// Information based on tarantool cartridge schema. Cache knows nothing about bucket distribution in the cluster,
/// as it is managed by Tarantool's vshard module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouterConfiguration {
    /// Execute response waiting timeout in seconds.
    waiting_timeout: u64,

    /// Query cache capacity.
    cache_capacity: usize,

    /// Sharding column names.
    sharding_column: String,

    /// Jaeger agent host.
    jaeger_agent_host: String,

    /// Jaeger agent port.
    jaeger_agent_port: u16,

    /// IR table segments from the cluster spaces
    tables: HashMap<String, Table>,
}

impl Default for RouterConfiguration {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterConfiguration {
    #[must_use]
    pub fn new() -> Self {
        RouterConfiguration {
            waiting_timeout: 360,
            cache_capacity: DEFAULT_CAPACITY,
            jaeger_agent_host: "localhost".to_string(),
            jaeger_agent_port: 6831,
            tables: HashMap::new(),
            sharding_column: String::new(),
        }
    }

    /// Parse and load yaml cartridge schema to cache
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when process was terminated.
    pub fn load_schema(&mut self, s: &str) -> Result<(), QueryPlannerError> {
        if let Ok(docs) = YamlLoader::load_from_str(s) {
            if let Some(schema) = docs.get(0) {
                self.init_table_segments(schema)?;
                return Ok(());
            }
        }

        Err(QueryPlannerError::InvalidClusterSchema)
    }

    pub(in crate::executor::engine::cartridge) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Transform space information from schema to table segments
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when schema contains errors.
    #[allow(clippy::too_many_lines)]
    fn init_table_segments(&mut self, schema: &Yaml) -> Result<(), QueryPlannerError> {
        self.tables.clear();
        let spaces = match schema["spaces"].as_hash() {
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
                            let qualified_name = normalize_name_from_schema(name);
                            #[cfg(not(feature = "mock"))]
                            {
                                say(
                                    SayLevel::Debug,
                                    file!(),
                                    line!().try_into().unwrap_or(0),
                                    Option::from("configuration parsing"),
                                    &format!(
                                        "Column's original name: {}, qualified name {}",
                                        name, qualified_name
                                    ),
                                );
                            }
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
                    None => {
                        #[cfg(not(feature = "mock"))]
                        {
                            say(
                                SayLevel::Warn,
                                file!(),
                                line!().try_into().unwrap_or(0),
                                Option::from("configuration parsing"),
                                &format!("Skip space {}: fields not found.", current_space_name),
                            );
                        }
                        continue;
                    }
                };

                let keys: Vec<String> = match params["sharding_key"].as_vec() {
                    Some(keys) => {
                        let mut result = Vec::new();
                        for k in keys {
                            let key: &str = match k.as_str() {
                                Some(k) => k,
                                None => {
                                    #[cfg(not(feature = "mock"))]
                                    {
                                        say(
                                            SayLevel::Warn,
                                            file!(),
                                            line!().try_into().unwrap_or(0),
                                            Option::from("configuration parsing"),
                                            &format!(
                                                "Skip space {}: failed to convert key {:?} to string.",
                                                current_space_name, k
                                            ),
                                        );
                                    }
                                    continue;
                                }
                            };
                            result.push(normalize_name_from_schema(key));
                        }
                        result
                    }
                    None => {
                        #[cfg(not(feature = "mock"))]
                        {
                            say(
                                SayLevel::Warn,
                                file!(),
                                line!().try_into().unwrap_or(0),
                                Option::from("configuration parsing"),
                                &format!("Skip space {}: keys not found.", current_space_name),
                            );
                        }
                        continue;
                    }
                };

                let table_name: String = normalize_name_from_schema(current_space_name);
                #[cfg(not(feature = "mock"))]
                {
                    say(
                        SayLevel::Debug,
                        file!(),
                        line!().try_into().unwrap_or(0),
                        Option::from("configuration parsing"),
                        &format!(
                            "Table's original name: {}, qualified name {}",
                            current_space_name, table_name
                        ),
                    );
                }
                let keys_str = keys.iter().map(String::as_str).collect::<Vec<&str>>();
                let t = Table::new_seg(&table_name, fields, keys_str.as_slice())?;
                self.tables.insert(table_name, t);
            } else {
                return Err(QueryPlannerError::InvalidSpaceName);
            }
        }

        Ok(())
    }

    #[must_use]
    pub fn get_jaeger_agent_host(&self) -> &str {
        self.jaeger_agent_host.as_str()
    }

    #[must_use]
    pub fn get_jaeger_agent_port(&self) -> u16 {
        self.jaeger_agent_port
    }

    pub fn set_jaeger_agent_host(&mut self, host: String) {
        self.jaeger_agent_host = host;
    }

    pub fn set_jaeger_agent_port(&mut self, port: u16) {
        self.jaeger_agent_port = port;
    }

    /// Setup response waiting timeout for executor
    pub fn set_waiting_timeout(&mut self, timeout: u64) {
        if timeout > 0 {
            self.waiting_timeout = timeout;
        }
    }

    pub fn set_cache_capacity(&mut self, capacity: usize) {
        if capacity > 0 {
            self.cache_capacity = capacity;
        } else {
            self.cache_capacity = DEFAULT_CAPACITY;
        }
    }

    pub fn set_sharding_column(&mut self, column: String) {
        self.sharding_column = column;
    }
}

impl CoordinatorMetadata for RouterConfiguration {
    /// Get table segment form cache by table name
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when table was not found.
    #[allow(dead_code)]
    fn get_table_segment(&self, table_name: &str) -> Result<Table, QueryPlannerError> {
        let name = normalize_name_from_sql(table_name);
        match self.tables.get(&name) {
            Some(v) => Ok(v.clone()),
            None => Err(QueryPlannerError::CustomError(format!(
                "Space {} not found",
                name
            ))),
        }
    }

    /// Get response waiting timeout for executor
    fn get_exec_waiting_timeout(&self) -> u64 {
        self.waiting_timeout
    }

    fn get_sharding_column(&self) -> &str {
        self.sharding_column.as_str()
    }

    /// Get sharding key's column names by a space name
    fn get_sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, QueryPlannerError> {
        let table = self.get_table_segment(space)?;
        table.get_sharding_column_names()
    }

    fn get_sharding_positions_by_space(
        &self,
        space: &str,
    ) -> Result<Vec<usize>, QueryPlannerError> {
        let table = self.get_table_segment(space)?;
        Ok(table.get_sharding_positions().to_vec())
    }

    fn get_fields_amount_by_space(&self, space: &str) -> Result<usize, QueryPlannerError> {
        let table = self.get_table_segment(space)?;
        Ok(table.columns.len())
    }
}

pub struct StorageConfiguration {
    /// Prepared statements cache capacity (on the storage).
    pub storage_capacity: usize,
    /// Prepared statements cache size in bytes (on the storage).
    /// If a new statement is bigger doesn't fit into the cache,
    /// it would not be cached but executed directly.
    pub storage_size_bytes: usize,
    /// Jaeger agent host
    pub jaeger_agent_host: String,
    /// Jaeger agent port
    pub jaeger_agent_port: u16,
}

impl Default for StorageConfiguration {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageConfiguration {
    #[must_use]
    pub fn new() -> Self {
        StorageConfiguration {
            storage_capacity: 0,
            storage_size_bytes: 0,
            jaeger_agent_host: "localhost".to_string(),
            jaeger_agent_port: 6831,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage_capacity == 0 && self.storage_size_bytes == 0
    }
}

#[cfg(test)]
mod tests;
