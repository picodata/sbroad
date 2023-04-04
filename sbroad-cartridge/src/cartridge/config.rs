//! Cartridge configuration cache module.

extern crate yaml_rust;

use std::collections::HashMap;
use yaml_rust::{Yaml, YamlLoader};

use sbroad::errors::{Entity, SbroadError};
use sbroad::executor::engine::helpers::{normalize_name_from_schema, normalize_name_from_sql};
use sbroad::executor::engine::Metadata;
use sbroad::executor::lru::DEFAULT_CAPACITY;
use sbroad::ir::function::Function;
use sbroad::ir::relation::{Column, ColumnRole, SpaceEngine, Table, Type};
use sbroad::{debug, warn};

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

    /// IR functions
    functions: HashMap<String, Function>,
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
            functions: HashMap::new(),
        }
    }

    /// Parse and load yaml cartridge schema to cache
    ///
    /// # Errors
    /// Returns `SbroadError` when process was terminated.
    pub fn load_schema(&mut self, s: &str) -> Result<(), SbroadError> {
        self.init_core_functions();
        if let Ok(docs) = YamlLoader::load_from_str(s) {
            if let Some(schema) = docs.get(0) {
                self.init_table_segments(schema)?;
                return Ok(());
            }
        }

        Err(SbroadError::Invalid(Entity::ClusterSchema, None))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Populate metadata about core functions.
    /// Core functions should be present in the cluster
    /// (imported by the core.lua file).
    fn init_core_functions(&mut self) {
        let name_bucket_id = normalize_name_from_sql("bucket_id");
        let fn_bucket_id = Function::new_stable(name_bucket_id.clone());
        self.functions.insert(name_bucket_id, fn_bucket_id);
    }

    /// Transform space information from schema to table segments
    ///
    /// # Errors
    /// Returns `SbroadError` when schema contains errors.
    #[allow(clippy::too_many_lines)]
    fn init_table_segments(&mut self, schema: &Yaml) -> Result<(), SbroadError> {
        self.tables.clear();
        let Some(spaces) = schema["spaces"].as_hash() else {
            return Err(SbroadError::Invalid(
                Entity::ClusterSchema,
                Some("schema.spaces is invalid".into()),
            ))
        };

        for (space_name, params) in spaces.iter() {
            if let Some(current_space_name) = space_name.as_str() {
                let fields = if let Some(fields) = params["format"].as_vec() {
                    let mut result = Vec::new();
                    for val in fields {
                        let name: &str = match val["name"].as_str() {
                            Some(s) => s,
                            None => {
                                return Err(SbroadError::Invalid(
                                    Entity::ClusterSchema,
                                    Some(format!(
                                        "column name of table {current_space_name} is invalid"
                                    )),
                                ))
                            }
                        };
                        let t = match val["type"].as_str() {
                            Some(t) => Type::new(t)?,
                            None => {
                                return Err(SbroadError::Invalid(
                                    Entity::ClusterSchema,
                                    Some(format!(
                                        "Type not found for columns {name} of table {current_space_name}"
                                    )),
                                ))
                            }
                        };
                        let qualified_name = normalize_name_from_schema(name);
                        debug!(
                            Option::from("configuration parsing"),
                            &format!(
                                "Column's original name: {name}, qualified name {qualified_name}"
                            ),
                        );
                        let role = if self.sharding_column().eq(&qualified_name) {
                            ColumnRole::Sharding
                        } else {
                            ColumnRole::User
                        };
                        let col = Column::new(&qualified_name, t, role);
                        result.push(col);
                    }
                    result
                } else {
                    warn!(
                        Option::from("configuration parsing"),
                        &format!("Skip space {current_space_name}: fields not found."),
                    );
                    continue;
                };

                let keys: Vec<String> = if let Some(keys) = params["sharding_key"].as_vec() {
                    let mut result = Vec::new();
                    for k in keys {
                        let key: &str = if let Some(k) = k.as_str() {
                            k
                        } else {
                            warn!(
                                Option::from("configuration parsing"),
                                &format!(
                                    "Skip space {current_space_name}: failed to convert key {k:?} to string."
                                ),
                            );
                            continue;
                        };
                        result.push(normalize_name_from_schema(key));
                    }
                    result
                } else {
                    warn!(
                        Option::from("configuration parsing"),
                        &format!("Skip space {current_space_name}: keys not found."),
                    );
                    continue;
                };

                let engine: SpaceEngine = if let Some(engine) = params["engine"].as_str() {
                    if let Ok(v) = SpaceEngine::try_from(engine) {
                        v
                    } else {
                        warn!(
                            Option::from("configuration parsing"),
                            &format!("Skip space {current_space_name}: unknown engine {engine}."),
                        );
                        continue;
                    }
                } else {
                    warn!(
                        Option::from("configuration parsing"),
                        &format!("Skip space {current_space_name}: engine not found."),
                    );
                    continue;
                };

                let table_name: String = normalize_name_from_schema(current_space_name);
                debug!(
                    Option::from("configuration parsing"),
                    &format!(
                        "Table's original name: {current_space_name}, qualified name {table_name}"
                    ),
                );
                let keys_str = keys.iter().map(String::as_str).collect::<Vec<&str>>();
                let t = Table::new_seg(&table_name, fields, keys_str.as_slice(), engine)?;
                self.tables.insert(table_name, t);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::ClusterSchema,
                    Some("space name is invalid".into()),
                ));
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

impl Metadata for RouterConfiguration {
    /// Get table segment form cache by table name
    ///
    /// # Errors
    /// Returns `SbroadError` when table was not found.
    #[allow(dead_code)]
    fn table(&self, table_name: &str) -> Result<Table, SbroadError> {
        let name = normalize_name_from_sql(table_name);
        match self.tables.get(&name) {
            Some(v) => Ok(v.clone()),
            None => Err(SbroadError::NotFound(Entity::Space, name)),
        }
    }

    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError> {
        let name = normalize_name_from_sql(fn_name);
        match self.functions.get(&name) {
            Some(v) => Ok(v),
            None => Err(SbroadError::NotFound(Entity::SQLFunction, name)),
        }
    }

    /// Get response waiting timeout for executor
    fn waiting_timeout(&self) -> u64 {
        self.waiting_timeout
    }

    fn sharding_column(&self) -> &str {
        self.sharding_column.as_str()
    }

    /// Get sharding key's column names by a space name
    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, SbroadError> {
        let table = self.table(space)?;
        table.get_sharding_column_names()
    }

    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError> {
        let table = self.table(space)?;
        Ok(table.get_sharding_positions().to_vec())
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
