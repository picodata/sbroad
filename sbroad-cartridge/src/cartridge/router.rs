//! Tarantool cartridge engine module.

use sbroad::cbo::{TableColumnPair, TableStats};
use sbroad::executor::engine::helpers::vshard::{
    exec_ir_on_all_buckets, exec_ir_on_some_buckets, get_random_bucket,
};
use sbroad::executor::engine::{InitialColumnStats, QueryCache, Vshard};

use std::any::Any;
use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::convert::TryInto;
use std::rc::Rc;

use tarantool::tlua::LuaFunction;

use crate::cartridge::config::RouterConfiguration;
use crate::cartridge::{bucket_count, update_tracing};
use sbroad::executor::protocol::Binary;

use sbroad::error;
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::{
    helpers::{
        dispatch, explain_format, materialize_motion, normalize_name_from_schema,
        sharding_keys_from_map, sharding_keys_from_tuple,
    },
    Router, Statistics,
};
use sbroad::executor::hash::bucket_id_by_tuple;
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::Cache;
use sbroad::executor::lru::{LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::vtable::VirtualTable;
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::ir::value::Value;
use sbroad::ir::Plan;
use sbroad::otm::child_span;
use sbroad_proc::otm_child_span;

use super::ConfigurationProvider;

/// The runtime (cluster configuration, buckets, IR cache) of the dispatcher node.
#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntime {
    metadata: RefCell<RouterConfiguration>,
    bucket_count: u64,
    ir_cache: RefCell<LRUCache<String, Plan>>,
}

impl ConfigurationProvider for RouterRuntime {
    type Configuration = RouterConfiguration;

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
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e}"))
        })?;
        Ok(metadata.is_empty())
    }

    #[allow(clippy::too_many_lines)]
    fn retrieve_config(&self) -> Result<Option<Self::Configuration>, SbroadError> {
        if self.is_config_empty()? {
            let lua = tarantool::lua_state();

            let get_schema: LuaFunction<_> = lua.eval("return get_schema;").unwrap();
            let schema: String = match get_schema.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting schema"), &format!("{e:?}"));
                    return Err(SbroadError::LuaError(format!("{e:?}")));
                }
            };

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

            let waiting_timeout: LuaFunction<_> = lua.eval("return get_waiting_timeout;").unwrap();
            let timeout: u64 = match waiting_timeout.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting waiting timeout"), &format!("{e:?}"));
                    return Err(SbroadError::LuaError(format!("{e:?}")));
                }
            };

            let router_cache_capacity: LuaFunction<_> =
                lua.eval("return get_router_cache_capacity;").unwrap();
            let router_capacity: usize = match router_cache_capacity.call() {
                Ok(capacity) => {
                    let val: u64 = capacity;
                    usize::try_from(val).map_err(|_| {
                        SbroadError::Invalid(
                            Entity::Cache,
                            Some(format!("router cache capacity is too big: {capacity}")),
                        )
                    })?
                }
                Err(e) => {
                    error!(
                        Option::from("getting router cache capacity"),
                        &format!("{e:?}"),
                    );
                    return Err(SbroadError::LuaError(format!("{e:?}")));
                }
            };

            let sharding_column: LuaFunction<_> = lua.eval("return get_sharding_column;").unwrap();
            let column: String = match sharding_column.call() {
                Ok(column) => column,
                Err(e) => {
                    error!(Option::from("getting sharding column"), &format!("{e:?}"));
                    return Err(SbroadError::LuaError(format!("{e:?}")));
                }
            };

            let mut metadata = RouterConfiguration::new();
            metadata.set_jaeger_agent_host(jaeger_host);
            metadata.set_jaeger_agent_port(jaeger_port);
            metadata.set_waiting_timeout(timeout);
            metadata.set_cache_capacity(router_capacity);
            metadata.set_sharding_column(normalize_name_from_schema(column.as_str()));
            // We should always load the schema **after** setting the sharding column.
            metadata.load_schema(&schema)?;
            update_tracing(
                metadata.get_jaeger_agent_host(),
                metadata.get_jaeger_agent_port(),
            )?;

            return Ok(Some(metadata));
        }
        Ok(None)
    }

    fn update_config(&self, metadata: Self::Configuration) -> Result<(), SbroadError> {
        let mut cached_metadata = self.metadata.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e}"))
        })?;
        *cached_metadata = metadata;
        Ok(())
    }
}

impl QueryCache for RouterRuntime {
    type Cache = LRUCache<String, Plan>;

    fn cache(&self) -> &RefCell<Self::Cache>
    where
        Self: Sized,
    {
        &self.ir_cache
    }

    fn clear_cache(&self) -> Result<(), SbroadError>
    where
        Self: Sized,
    {
        *self.ir_cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format!("{e:?}"))
        })? = Self::Cache::new(self.cache_capacity()?, None)?;
        Ok(())
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self
            .cache()
            .try_borrow()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Borrow, Some(Entity::Cache), format!("{e}"))
            })?
            .capacity())
    }
}

impl Router for RouterRuntime {
    type ParseTree = AbstractSyntaxTree;
    type MetadataProvider = RouterConfiguration;

    fn metadata(&self) -> Result<Ref<Self::MetadataProvider>, SbroadError> {
        self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e}"))
        })
    }

    /// Execute a sub tree on the nodes
    #[otm_child_span("query.dispatch.cartridge")]
    fn dispatch(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        dispatch(self, plan, top_id, buckets)
    }

    fn explain_format(&self, explain: String) -> Result<Box<dyn Any>, SbroadError> {
        explain_format(&explain)
    }

    /// Transform sub query results into a virtual table.
    #[otm_child_span("query.motion.materialize")]
    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
        buckets: &Buckets,
    ) -> Result<VirtualTable, SbroadError> {
        materialize_motion(self, plan, motion_node_id, buckets)
    }

    fn extract_sharding_keys_from_map<'rec>(
        &self,
        space: String,
        map: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        let metadata = self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e:?}"))
        })?;
        sharding_keys_from_map(&*metadata, &space, map)
    }

    fn extract_sharding_keys_from_tuple<'rec>(
        &self,
        space: String,
        rec: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_keys_from_tuple(&*self.cached_config()?, &space, rec)
    }
}

impl Statistics for RouterRuntime {
    #[allow(unused_variables)]
    fn get_table_stats(&self, table_name: String) -> Result<Rc<TableStats>, SbroadError> {
        // Will be added later.
        todo!()
    }

    #[allow(unused_variables)]
    fn get_initial_column_stats(
        &self,
        table_column_pair: TableColumnPair,
    ) -> Result<Rc<InitialColumnStats>, SbroadError> {
        // Will be added later.
        todo!()
    }

    #[allow(unused_variables)]
    fn update_table_stats_cache(
        &mut self,
        table_name: String,
        table_stats: TableStats,
    ) -> Result<(), SbroadError> {
        // Will be added later.
        todo!()
    }

    #[allow(unused_variables)]
    fn update_column_initial_stats_cache(
        &self,
        table_column_pair: TableColumnPair,
        initial_column_stats: InitialColumnStats,
    ) -> Result<(), SbroadError> {
        // Will be added later.
        todo!()
    }
}

impl RouterRuntime {
    /// Create new Tarantool cartridge runtime.
    ///
    /// # Errors
    /// - Failed to detect the correct amount of buckets.
    pub fn new() -> Result<Self, SbroadError> {
        let cache: LRUCache<String, Plan> = LRUCache::new(DEFAULT_CAPACITY, None)?;
        let result = RouterRuntime {
            metadata: RefCell::new(RouterConfiguration::new()),
            bucket_count: bucket_count()?,
            ir_cache: RefCell::new(cache),
        };

        Ok(result)
    }
}

impl Vshard for RouterRuntime {
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
        vtable_max_rows: u64,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_all_buckets(
            &*self.metadata()?,
            required,
            optional,
            query_type,
            conn_type,
            vtable_max_rows,
        )
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
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_some_buckets(self, sub_plan, buckets)
    }
}

impl Vshard for &RouterRuntime {
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
        vtable_max_rows: u64,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_all_buckets(
            &*self.metadata()?,
            required,
            optional,
            query_type,
            conn_type,
            vtable_max_rows,
        )
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
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_some_buckets(*self, sub_plan, buckets)
    }
}
