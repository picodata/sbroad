use crate::cartridge::config::StorageConfiguration;
use crate::cartridge::{bucket_count, update_tracing};
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::storage::runtime::{
    prepare, read_prepared, read_unprepared, unprepare, write_prepared, write_unprepared,
};
use sbroad::executor::engine::helpers::storage::PreparedStmt;
use sbroad::executor::engine::helpers::vshard::get_random_bucket;
use sbroad::executor::engine::helpers::{compile_encoded_optional, execute_dml};
use sbroad::executor::engine::{QueryCache, Vshard};
use sbroad::executor::hash::bucket_id_by_tuple;
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::protocol::{Binary, RequiredData};
use sbroad::ir::value::Value;
use sbroad::{debug, error, warn};
use std::any::Any;
use std::cell::{Ref, RefCell};
use std::fmt::Display;
use tarantool::tlua::LuaFunction;
use tarantool::tuple::Tuple;

use super::ConfigurationProvider;

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    metadata: RefCell<StorageConfiguration>,
    bucket_count: u64,
    cache: RefCell<LRUCache<String, PreparedStmt>>,
}

impl QueryCache for StorageRuntime {
    type Cache = LRUCache<String, PreparedStmt>;

    fn cache(&self) -> &RefCell<Self::Cache> {
        &self.cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self
            .cache()
            .try_borrow()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Borrow, Some(Entity::Cache), format!("{e:?}"))
            })?
            .capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format!("{e:?}"))
        })? = Self::Cache::new(DEFAULT_CAPACITY, None)?;
        Ok(())
    }
}

impl ConfigurationProvider for StorageRuntime {
    type Configuration = StorageConfiguration;

    fn cached_config(&self) -> Result<Ref<Self::Configuration>, SbroadError> {
        self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e}"))
        })
    }

    fn clear_config(&self) -> Result<(), SbroadError> {
        let mut metadata = self.metadata.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e}"))
        })?;
        *metadata = Self::Configuration::new();
        Ok(())
    }

    fn is_config_empty(&self) -> Result<bool, SbroadError> {
        let metadata = self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e:?}"))
        })?;
        Ok(metadata.is_empty())
    }

    fn retrieve_config(&self) -> Result<Option<Self::Configuration>, SbroadError> {
        if self.is_config_empty()? {
            let lua = tarantool::lua_state();

            let storage_cache_capacity: LuaFunction<_> =
                lua.eval("return get_storage_cache_capacity;").unwrap();
            let capacity: u64 = match storage_cache_capacity.call() {
                Ok(capacity) => capacity,
                Err(e) => {
                    error!(
                        Option::from("getting storage cache capacity"),
                        &format!("{e:?}"),
                    );
                    return Err(SbroadError::LuaError(format!("{e:?}")));
                }
            };
            let storage_capacity = usize::try_from(capacity)
                .map_err(|e| SbroadError::Invalid(Entity::Cache, Some(format!("{e:?}"))))?;

            let storage_cache_size_bytes: LuaFunction<_> =
                lua.eval("return get_storage_cache_size_bytes;").unwrap();
            let cache_size_bytes = match storage_cache_size_bytes.call::<u64>() {
                Ok(size_bytes) => size_bytes,
                Err(e) => {
                    error!(
                        Option::from("getting storage cache size bytes"),
                        &format!("{e:?}"),
                    );
                    return Err(SbroadError::LuaError(format!("{e:?}")));
                }
            };
            let storage_size_bytes = usize::try_from(cache_size_bytes)
                .map_err(|e| SbroadError::Invalid(Entity::Cache, Some(format!("{e}"))))?;

            let jaeger_agent_host: LuaFunction<_> =
                lua.eval("return get_jaeger_agent_host;").unwrap();
            let jaeger_host: String = match jaeger_agent_host.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting jaeger agent host"), &format!("{e:?}"),);
                    return Err(SbroadError::LuaError(format!("{e:?}")));
                }
            };

            let jaeger_agent_port: LuaFunction<_> =
                lua.eval("return get_jaeger_agent_port;").unwrap();
            let jaeger_port: u16 = match jaeger_agent_port.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting jaeger agent port"), &format!("{e:?}"),);
                    return Err(SbroadError::LuaError(format!("{e:?}")));
                }
            };

            let mut metadata = StorageConfiguration::new();
            metadata.storage_capacity = storage_capacity;
            metadata.storage_size_bytes = storage_size_bytes;
            metadata.jaeger_agent_host = jaeger_host;
            metadata.jaeger_agent_port = jaeger_port;
            update_tracing(&metadata.jaeger_agent_host, metadata.jaeger_agent_port)?;

            return Ok(Some(metadata));
        }
        Ok(None)
    }

    fn update_config(&self, metadata: Self::Configuration) -> Result<(), SbroadError> {
        let mut cached_metadata = self.metadata.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e:?}"))
        })?;
        let storage_size_bytes = metadata.storage_size_bytes;
        *cached_metadata = metadata;
        update_box_param("sql_cache_size", storage_size_bytes);
        Ok(())
    }
}

impl Vshard for StorageRuntime {
    fn exec_ir_on_all(
        &self,
        _required: Binary,
        _optional: Binary,
        _query_type: QueryType,
        _conn_type: ConnectionType,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_all is not supported on the storage".to_string()),
        ))
    }

    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> u64 {
        bucket_id_by_tuple(s, self.bucket_count())
    }

    fn exec_ir_on_some(
        &self,
        _sub_plan: ExecutionPlan,
        _buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_some is not supported on the storage".to_string()),
        ))
    }
}

impl StorageRuntime {
    /// Build a new storage runtime.
    ///
    /// # Errors
    /// - Failed to initialize the LRU cache.
    pub fn new() -> Result<Self, SbroadError> {
        let cache: LRUCache<String, PreparedStmt> =
            LRUCache::new(DEFAULT_CAPACITY, Some(Box::new(unprepare)))?;
        let result = StorageRuntime {
            metadata: RefCell::new(StorageConfiguration::new()),
            bucket_count: bucket_count()?,
            cache: RefCell::new(cache),
        };

        Ok(result)
    }

    #[allow(unused_variables)]
    pub fn execute_plan(
        &self,
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, SbroadError> {
        match required.query_type {
            QueryType::DML => self.execute_dml(required, raw_optional),
            QueryType::DQL => {
                if required.can_be_cached {
                    self.execute_cacheable_dql(required, raw_optional)
                } else {
                    execute_non_cacheable_dql(required, raw_optional)
                }
            }
        }
    }

    #[allow(unused_variables)]
    fn execute_dml(
        &self,
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, SbroadError> {
        if required.query_type != QueryType::DML {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("Expected a DML plan.".to_string()),
            ));
        }

        let result = execute_dml(self, raw_optional)?;
        let tuple = Tuple::new(&(result,))
            .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format!("{e:?}"))))?;
        Ok(Box::new(tuple) as Box<dyn Any>)
    }

    #[allow(unused_variables)]
    fn execute_cacheable_dql(
        &self,
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let plan_id = required.plan_id.clone();

        if !required.can_be_cached || required.query_type != QueryType::DQL {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("Expected a DQL plan that can be cached.".to_string()),
            ));
        }

        // Look for the prepared statement in the cache.
        if let Some(stmt) = self
            .cache
            .try_borrow_mut()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Borrow, Some(Entity::Cache), format!("{e}"))
            })?
            .get(&plan_id)?
        {
            let stmt_id = stmt.id()?;
            // The statement was found in the cache, so we can execute it.
            debug!(
                Option::from("execute plan"),
                &format!("Execute prepared statement: {stmt:?}"),
            );
            let result = match required.query_type {
                QueryType::DML => write_prepared(stmt_id, "", &required.parameters),
                QueryType::DQL => read_prepared(stmt_id, "", &required.parameters),
            };

            // If prepared statement is invalid for some reason, fallback to the long pass
            // and recompile the query.
            if result.is_ok() {
                return result;
            }
        }
        debug!(
            Option::from("execute plan"),
            &format!("Failed to find a plan (id {plan_id}) in the cache."),
        );

        let (pattern_with_params, _tmp_spaces) = compile_encoded_optional(raw_optional)?;
        let result = match prepare(&pattern_with_params.pattern) {
            Ok(stmt) => {
                let stmt_id = stmt.id()?;
                debug!(
                    Option::from("execute plan"),
                    &format!(
                        "Created prepared statement {} for the pattern {}",
                        stmt_id,
                        stmt.pattern()?
                    ),
                );
                self.cache
                    .try_borrow_mut()
                    .map_err(|e| {
                        SbroadError::FailedTo(
                            Action::Put,
                            None,
                            format!("prepared statement {stmt:?} into the cache: {e:?}"),
                        )
                    })?
                    .put(plan_id, stmt)?;
                // The statement was found in the cache, so we can execute it.
                debug!(
                    Option::from("execute plan"),
                    &format!("Execute prepared statement: {stmt_id}"),
                );
                if required.query_type == QueryType::DML {
                    write_prepared(
                        stmt_id,
                        &pattern_with_params.pattern,
                        &pattern_with_params.params,
                    )
                } else {
                    read_prepared(
                        stmt_id,
                        &pattern_with_params.pattern,
                        &pattern_with_params.params,
                    )
                }
            }
            Err(e) => {
                // Possibly the statement is correct, but doesn't fit into
                // Tarantool's prepared statements cache (`sql_cache_size`).
                // So we try to execute it bypassing the cache.
                warn!(
                    Option::from("execute"),
                    &format!(
                        "Failed to prepare the statement: {}, error: {e}",
                        pattern_with_params.pattern
                    ),
                );
                if required.query_type == QueryType::DML {
                    write_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
                } else {
                    read_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
                }
            }
        };

        result
    }
}

fn execute_non_cacheable_dql(
    required: &mut RequiredData,
    raw_optional: &mut Vec<u8>,
) -> Result<Box<dyn Any>, SbroadError> {
    if required.can_be_cached || required.query_type != QueryType::DQL {
        return Err(SbroadError::Invalid(
            Entity::Plan,
            Some("Expected a DQL plan that can not be cached.".to_string()),
        ));
    }

    let (pattern_with_params, _tmp_spaces) = compile_encoded_optional(raw_optional)?;
    debug!(
        Option::from("execute"),
        &format!(
            "Failed to execute the statement: {}",
            pattern_with_params.pattern
        ),
    );
    warn!(
        Option::from("execute"),
        &format!("SQL pattern: {}", pattern_with_params.pattern),
    );
    read_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
}

fn update_box_param<T>(param: &str, val: T)
where
    T: Display,
{
    let lua = tarantool::lua_state();
    match lua.exec(&format!("box.cfg{{{param} = {val}}}")) {
        Ok(_) => debug!(
            Option::from("update_box_param"),
            &format!("box.cfg param {param} was updated to {val}")
        ),
        Err(e) => warn!(
            Option::from("update_box_param"),
            &format!("box.cfg update error: {e}")
        ),
    }
}
