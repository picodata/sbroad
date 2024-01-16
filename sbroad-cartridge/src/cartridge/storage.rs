use crate::cartridge::bucket_count;
use crate::cartridge::config::StorageConfiguration;
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::helpers::storage::runtime::unprepare;
use sbroad::executor::engine::helpers::storage::PreparedStmt;
use sbroad::executor::engine::helpers::vshard::get_random_bucket;
use sbroad::executor::engine::{helpers, StorageCache};
use sbroad::executor::engine::{QueryCache, Vshard};
use sbroad::executor::hash::bucket_id_by_tuple;
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::{Cache, LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::protocol::{Binary, RequiredData, SchemaInfo};
use sbroad::ir::value::Value;
use sbroad::{debug, error, warn};
use std::any::Any;
use std::cell::{Ref, RefCell};
use std::fmt::Display;
use tarantool::tlua::LuaFunction;

use super::ConfigurationProvider;

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    metadata: RefCell<StorageConfiguration>,
    bucket_count: u64,
    cache: RefCell<CartridgeCache>,
}

pub struct CartridgeCache(LRUCache<String, PreparedStmt>);

impl StorageCache for CartridgeCache {
    fn put(
        &mut self,
        plan_id: String,
        stmt: PreparedStmt,
        _: &SchemaInfo,
    ) -> Result<(), SbroadError> {
        self.0.put(plan_id, stmt)
    }

    fn get(&mut self, plan_id: &String) -> Result<Option<&PreparedStmt>, SbroadError> {
        self.0.get(plan_id)
    }

    fn clear(&mut self) -> Result<(), SbroadError> {
        self.0.clear()
    }
}

impl QueryCache for StorageRuntime {
    type Cache = CartridgeCache;

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
            .0
            .capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        self.cache
            .try_borrow_mut()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format!("{e:?}"))
            })?
            .clear()?;
        Ok(())
    }

    fn provides_versions(&self) -> bool {
        false
    }

    fn get_table_version(&self, _: &str) -> Result<u64, SbroadError> {
        Err(SbroadError::DoSkip)
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

            let mut metadata = StorageConfiguration::new();
            metadata.storage_capacity = storage_capacity;
            metadata.storage_size_bytes = storage_size_bytes;

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
        _vtable_max_rows: u64,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_all is not supported on the storage".to_string()),
        ))
    }

    fn exec_ir_on_any_node(&self, _sub_plan: ExecutionPlan) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_locally is not supported for the cartridge runtime".to_string()),
        ))
    }

    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        Ok(bucket_id_by_tuple(s, self.bucket_count()))
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
            cache: RefCell::new(CartridgeCache(cache)),
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
            QueryType::DML => helpers::execute_dml(self, required, raw_optional),
            QueryType::DQL => {
                if required.can_be_cached {
                    helpers::execute_cacheable_dql_with_raw_optional(self, required, raw_optional)
                } else {
                    helpers::execute_non_cacheable_dql_with_raw_optional(
                        raw_optional,
                        required.options.vtable_max_rows,
                        std::mem::take(&mut required.options.execute_options),
                    )
                }
            }
        }
    }
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
