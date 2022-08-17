//! Tarantool cartridge engine module.

use rand::prelude::*;

use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use tarantool::log::{say, SayLevel};
use tarantool::tlua::LuaFunction;
use tarantool::tuple::Tuple;

use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::engine::cartridge::backend::sql::ir::{get_sql_order, PatternWithParams};
use crate::executor::engine::cartridge::backend::sql::tree::SyntaxPlan;
use crate::executor::engine::cartridge::config::RouterConfiguration;
use crate::executor::engine::cartridge::hash::bucket_id_by_tuple;
use crate::executor::engine::{
    normalize_name_from_schema, sharding_keys_from_map, sharding_keys_from_tuple, Configuration,
    Coordinator,
};
use crate::executor::ir::ExecutionPlan;
use crate::executor::lru::{LRUCache, DEFAULT_CAPACITY};
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::executor::{Cache, CoordinatorMetadata};
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::helpers::RepeatableState;
use crate::ir::value::Value;
use crate::ir::Plan;

type GroupedBuckets = HashMap<String, Vec<u64>>;

/// The runtime (cluster configuration, buckets, IR cache) of the dispatcher node.
#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntime {
    metadata: RouterConfiguration,
    bucket_count: usize,
    ir_cache: RefCell<LRUCache<String, Plan>>,
}

impl Configuration for RouterRuntime {
    type Configuration = RouterConfiguration;

    fn cached_config(&self) -> &Self::Configuration {
        &self.metadata
    }

    fn clear_config(&mut self) {
        self.metadata = Self::Configuration::new();
    }

    fn is_config_empty(&self) -> bool {
        self.metadata.is_empty()
    }

    fn get_config(&self) -> Result<Option<Self::Configuration>, QueryPlannerError> {
        if self.is_config_empty() {
            let lua = tarantool::lua_state();

            let get_schema: LuaFunction<_> = lua.eval("return get_schema;").unwrap();
            let schema: String = match get_schema.call() {
                Ok(res) => res,
                Err(e) => {
                    say(
                        SayLevel::Error,
                        file!(),
                        line!().try_into().unwrap_or(0),
                        Option::from("getting schema"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                }
            };

            let waiting_timeout: LuaFunction<_> = lua.eval("return get_waiting_timeout;").unwrap();
            let timeout: u64 = match waiting_timeout.call() {
                Ok(res) => res,
                Err(e) => {
                    say(
                        SayLevel::Error,
                        file!(),
                        line!().try_into().unwrap_or(0),
                        Option::from("getting waiting timeout"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                }
            };

            let router_cache_capacity: LuaFunction<_> =
                lua.eval("return get_router_cache_capacity;").unwrap();
            let router_capacity: usize = match router_cache_capacity.call() {
                Ok(capacity) => {
                    let val: u64 = capacity;
                    usize::try_from(val).map_err(|_| {
                        QueryPlannerError::CustomError(format!(
                            "Router cache capacity is too big: {}",
                            capacity
                        ))
                    })?
                }
                Err(e) => {
                    say(
                        SayLevel::Error,
                        file!(),
                        line!().try_into().unwrap_or(0),
                        Option::from("getting router cache capacity"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                }
            };

            let sharding_column: LuaFunction<_> = lua.eval("return get_sharding_column;").unwrap();
            let column: String = match sharding_column.call() {
                Ok(column) => column,
                Err(e) => {
                    say(
                        SayLevel::Error,
                        file!(),
                        line!().try_into().unwrap_or(0),
                        Option::from("getting sharding column"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                }
            };

            let mut metadata = RouterConfiguration::new();
            metadata.set_waiting_timeout(timeout);
            metadata.set_cache_capacity(router_capacity);
            metadata.set_sharding_column(normalize_name_from_schema(column.as_str()));
            // We should always load the schema **after** setting the sharding column.
            metadata.load_schema(&schema)?;

            return Ok(Some(metadata));
        }
        Ok(None)
    }

    fn update_config(&mut self, metadata: Self::Configuration) {
        self.metadata = metadata;
    }
}

impl Coordinator for RouterRuntime {
    type ParseTree = AbstractSyntaxTree;
    type Cache = LRUCache<String, Plan>;

    fn clear_ir_cache(&self) -> Result<(), QueryPlannerError> {
        *self.ir_cache.try_borrow_mut().map_err(|e| {
            QueryPlannerError::CustomError(format!("Failed to clear the cache: {:?}", e))
        })? = Self::Cache::new(DEFAULT_CAPACITY, None)?;
        Ok(())
    }

    fn ir_cache(&self) -> &RefCell<Self::Cache> {
        &self.ir_cache
    }

    /// Execute a sub tree on the nodes
    fn dispatch(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, QueryPlannerError> {
        let mut sp = SyntaxPlan::new(plan, top_id)?;
        let nodes = get_sql_order(&mut sp)?;
        let is_data_modifier = plan.subtree_modifies_data(top_id)?;

        let mut rs_query: HashMap<String, PatternWithParams> = HashMap::new();
        if let Buckets::Filtered(bucket_set) = buckets {
            let random_bucket = self.get_random_bucket();
            let buckets = if bucket_set.is_empty() {
                // There are no buckets to execute the query on.
                // At the moment we don't keep types inside our IR tree and
                // there is no easy way to get column types in the result.
                // So we just choose a random bucket and to execute the query on,
                // as we are sure that any bucket returns an empty result.

                // TODO: return an empty result without actual execution.
                &random_bucket
            } else {
                buckets
            };

            let rs_buckets = buckets.group()?;
            rs_query.reserve(rs_buckets.len());

            for (rs, bucket_ids) in &rs_buckets {
                let bucket_set = bucket_ids
                    .iter()
                    .copied()
                    .collect::<HashSet<u64, RepeatableState>>();
                let pattern_with_params =
                    plan.syntax_nodes_as_sql(&nodes, &Buckets::new_filtered(bucket_set))?;
                rs_query.insert(rs.to_string(), pattern_with_params);
            }

            if rs_query.is_empty() {
                return Err(QueryPlannerError::CustomError(format!(
                    "No no replica sets were found for the buckets {:?} to execute the query on",
                    buckets
                )));
            }

            return self.exec_on_some(&rs_query, is_data_modifier);
        }

        let pattern_with_params = plan.syntax_nodes_as_sql(&nodes, &Buckets::All)?;
        self.exec_on_all(&pattern_with_params, is_data_modifier)
    }

    /// Transform sub query results into a virtual table.
    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
        buckets: &Buckets,
    ) -> Result<VirtualTable, QueryPlannerError> {
        let top_id = plan.get_motion_subtree_root(motion_node_id)?;
        let result = self.dispatch(plan, top_id, buckets)?;
        let mut vtable = if let Ok(tuple) = result.downcast::<Tuple>() {
            let data = tuple.decode::<Vec<ProducerResult>>().map_err(|e| {
                QueryPlannerError::CustomError(format!("Motion node {}. {}", motion_node_id, e))
            })?;
            data.get(0)
                .ok_or_else(|| {
                    QueryPlannerError::CustomError(
                        "Failed to retrieve producer result from the tuple".into(),
                    )
                })?
                .as_virtual_table()?
        } else {
            return Err(QueryPlannerError::CustomError(
                "The result of the motion is not a tuple".to_string(),
            ));
        };
        if let Some(name) = &plan.get_motion_alias(motion_node_id)? {
            vtable.set_alias(name)?;
        }

        Ok(vtable)
    }

    fn extract_sharding_keys_from_map<'engine, 'rec>(
        &'engine self,
        space: String,
        map: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, QueryPlannerError> {
        sharding_keys_from_map(&self.metadata, &space, map)
    }

    fn extract_sharding_keys_from_tuple<'engine, 'rec>(
        &'engine self,
        space: String,
        rec: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, QueryPlannerError> {
        sharding_keys_from_tuple(self.cached_config(), &space, rec)
    }

    /// Calculate bucket for a key.
    fn determine_bucket_id(&self, s: &[&Value]) -> u64 {
        bucket_id_by_tuple(s, self.bucket_count)
    }
}

impl RouterRuntime {
    /// Create new Tarantool cartridge runtime.
    ///
    /// # Errors
    /// - Failed to detect the correct amount of buckets.
    pub fn new() -> Result<Self, QueryPlannerError> {
        let cache: LRUCache<String, Plan> = LRUCache::new(DEFAULT_CAPACITY, None)?;
        let mut result = RouterRuntime {
            metadata: RouterConfiguration::new(),
            bucket_count: 0,
            ir_cache: RefCell::new(cache),
        };

        result.set_bucket_count()?;

        Ok(result)
    }

    fn get_random_bucket(&self) -> Buckets {
        let mut rng = thread_rng();
        let bucket_id: u64 = rng.gen_range(1..=self.bucket_count as u64);
        let bucket_set: HashSet<u64, RepeatableState> = HashSet::from_iter(vec![bucket_id]);
        Buckets::Filtered(bucket_set)
    }

    /// Function get summary count of bucket from `vshard`
    fn set_bucket_count(&mut self) -> Result<(), QueryPlannerError> {
        let lua = tarantool::lua_state();

        let bucket_count_fn: LuaFunction<_> =
            match lua.eval("return require('vshard').router.bucket_count") {
                Ok(v) => v,
                Err(e) => {
                    say(
                        SayLevel::Error,
                        file!(),
                        line!().try_into().unwrap_or(0),
                        Option::from("set_bucket_count"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!(
                        "Failed lua function load: {}",
                        e
                    )));
                }
            };

        let bucket_count: u64 = match bucket_count_fn.call() {
            Ok(r) => r,
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("set_bucket_count"),
                    &format!("{:?}", e),
                );
                return Err(QueryPlannerError::LuaError(e.to_string()));
            }
        };

        self.bucket_count = match bucket_count.try_into() {
            Ok(v) => v,
            Err(_) => {
                return Err(QueryPlannerError::CustomError(String::from(
                    "Invalid bucket count",
                )));
            }
        };

        Ok(())
    }

    fn read_on_some(
        &self,
        rs_query: &HashMap<String, PatternWithParams>,
    ) -> Result<Box<dyn Any>, QueryPlannerError> {
        let lua = tarantool::lua_state();

        let exec_sql: LuaFunction<_> = lua.get("read_on_some").ok_or_else(|| {
            QueryPlannerError::LuaError("Lua function `read_on_some` not found".into())
        })?;

        let waiting_timeout = &self.cached_config().get_exec_waiting_timeout();
        match exec_sql.call_with_args::<Tuple, _>((rs_query, waiting_timeout)) {
            Ok(v) => Ok(Box::new(v)),
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("read_on_some"),
                    &format!("{:?}", e),
                );
                Err(QueryPlannerError::LuaError(format!(
                    "Lua error: {:?}. Query and parameters: {:?}",
                    e, rs_query
                )))
            }
        }
    }

    fn write_on_some(
        &self,
        rs_query: &HashMap<String, PatternWithParams>,
    ) -> Result<Box<dyn Any>, QueryPlannerError> {
        let lua = tarantool::lua_state();

        let exec_sql: LuaFunction<_> = lua.get("write_on_some").ok_or_else(|| {
            QueryPlannerError::LuaError("Lua function `write_on_some` not found".into())
        })?;

        let waiting_timeout = &self.cached_config().get_exec_waiting_timeout();
        match exec_sql.call_with_args::<Tuple, _>((rs_query, waiting_timeout)) {
            Ok(v) => Ok(Box::new(v)),
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("write_on_some"),
                    &format!("{:?}", e),
                );
                Err(QueryPlannerError::LuaError(format!(
                    "Lua error: {:?}. Query and parameters: {:?}",
                    e, rs_query
                )))
            }
        }
    }

    fn exec_on_some(
        &self,
        rs_query: &HashMap<String, PatternWithParams>,
        is_data_modifier: bool,
    ) -> Result<Box<dyn Any>, QueryPlannerError> {
        if is_data_modifier {
            self.write_on_some(rs_query)
        } else {
            self.read_on_some(rs_query)
        }
    }

    fn read_on_all(&self, query: &PatternWithParams) -> Result<Box<dyn Any>, QueryPlannerError> {
        let lua = tarantool::lua_state();

        let exec_sql: LuaFunction<_> = lua.get("read_on_all").ok_or_else(|| {
            QueryPlannerError::LuaError("Lua function `read_on_all` not found".into())
        })?;

        let waiting_timeout = &self.cached_config().get_exec_waiting_timeout();
        match exec_sql.call_with_args::<Tuple, _>((query, waiting_timeout)) {
            Ok(v) => Ok(Box::new(v)),
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("read_on_all"),
                    &format!("{:?}", e),
                );
                Err(QueryPlannerError::LuaError(format!(
                    "Lua error: {:?}. Query and parameters: {:?}",
                    e, query
                )))
            }
        }
    }

    fn write_on_all(&self, query: &PatternWithParams) -> Result<Box<dyn Any>, QueryPlannerError> {
        let lua = tarantool::lua_state();

        let exec_sql: LuaFunction<_> = lua.get("write_on_all").ok_or_else(|| {
            QueryPlannerError::LuaError("Lua function `write_on_all` not found".into())
        })?;

        let waiting_timeout = &self.cached_config().get_exec_waiting_timeout();
        match exec_sql.call_with_args::<Tuple, _>((query, waiting_timeout)) {
            Ok(v) => Ok(Box::new(v)),
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("write_on_all"),
                    &format!("{:?}", e),
                );
                Err(QueryPlannerError::LuaError(format!(
                    "Lua error: {:?}. Query and parameters: {:?}",
                    e, query
                )))
            }
        }
    }

    fn exec_on_all(
        &self,
        query: &PatternWithParams,
        is_data_modifier: bool,
    ) -> Result<Box<dyn Any>, QueryPlannerError> {
        if is_data_modifier {
            self.write_on_all(query)
        } else {
            self.read_on_all(query)
        }
    }
}

impl Buckets {
    fn group(&self) -> Result<HashMap<String, Vec<u64>>, QueryPlannerError> {
        let lua_buckets: Vec<u64> = match self {
            Buckets::All => {
                return Err(QueryPlannerError::CustomError(
                    "Grouping buckets is not supported for all buckets".into(),
                ))
            }
            Buckets::Filtered(list) => list.iter().copied().collect(),
        };

        let lua = tarantool::lua_state();

        let fn_group: LuaFunction<_> =
            lua.get("group_buckets_by_replicasets").ok_or_else(|| {
                QueryPlannerError::LuaError(
                    "Lua function `group_buckets_by_replicasets` not found".into(),
                )
            })?;

        let res: GroupedBuckets = match fn_group.call_with_args(lua_buckets) {
            Ok(v) => v,
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("buckets group"),
                    &format!("{:?}", e),
                );
                return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
            }
        };

        Ok(res)
    }
}
