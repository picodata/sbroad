//! Tarantool cartridge engine module.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use tarantool::log::{say, SayLevel};
use tarantool::tlua::LuaFunction;

use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::engine::cartridge::cache::lru::{LRUCache, DEFAULT_CAPACITY};
use crate::executor::engine::cartridge::cache::ClusterAppConfig;
use crate::executor::engine::{Engine, LocalMetadata};
use crate::executor::ir::ExecutionPlan;
use crate::executor::result::{ConsumerResults, ExecutorResults, ProducerResults};
use crate::executor::vtable::VirtualTable;
use crate::executor::{Metadata, QueryCache};
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::helpers::RepeatableState;
use crate::ir::operator::Relational;
use crate::ir::value::Value as IrValue;
use crate::ir::Plan;

use self::hash::bucket_id_by_tuple;

mod backend;
pub mod cache;
pub mod hash;

type GroupedBuckets = HashMap<String, Vec<u64>>;

/// Tarantool cartridge metadata and topology.
#[derive(Debug, Clone)]
pub struct Runtime {
    // query_cache:
    metadata: ClusterAppConfig,
    bucket_count: usize,
    query_cache: RefCell<LRUCache<String, Plan>>,
}

/// Implements `Engine` interface for tarantool cartridge application
impl Engine for Runtime {
    type Metadata = ClusterAppConfig;
    type ParseTree = AbstractSyntaxTree;
    type QueryCache = LRUCache<String, Plan>;

    fn clear_query_cache(&self, capacity: usize) -> Result<(), QueryPlannerError> {
        *self.query_cache.borrow_mut() = Self::QueryCache::new(capacity)?;
        Ok(())
    }

    fn query_cache(&self) -> &RefCell<Self::QueryCache> {
        &self.query_cache
    }

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn clear_metadata(&mut self) {
        self.metadata = Self::Metadata::new();
    }

    fn is_metadata_empty(&self) -> bool {
        self.metadata.is_empty()
    }

    fn get_metadata(&self) -> Result<Option<LocalMetadata>, QueryPlannerError> {
        if self.metadata.is_empty() {
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

            let cache_capacity: LuaFunction<_> = lua.eval("return get_cache_capacity;").unwrap();
            let capacity: usize = match cache_capacity.call() {
                Ok(capacity) => {
                    let val: u64 = capacity;
                    usize::try_from(val).map_err(|_| {
                        QueryPlannerError::CustomError(format!(
                            "Cache capacity is too big: {}",
                            capacity
                        ))
                    })?
                }
                Err(e) => {
                    say(
                        SayLevel::Error,
                        file!(),
                        line!().try_into().unwrap_or(0),
                        Option::from("getting cache capacity"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                }
            };

            let metadata = LocalMetadata {
                schema,
                timeout,
                capacity,
            };
            return Ok(Some(metadata));
        }
        Ok(None)
    }

    fn update_metadata(&mut self, metadata: LocalMetadata) -> Result<(), QueryPlannerError> {
        self.metadata.load_schema(&metadata.schema)?;
        self.metadata.set_exec_waiting_timeout(metadata.timeout);
        self.metadata.set_exec_cache_capacity(metadata.capacity);
        Ok(())
    }

    /// Execute sub tree on the nodes
    fn exec(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<ExecutorResults, QueryPlannerError> {
        let nodes = plan.get_sql_order(top_id)?;
        let is_data_modifier = plan.subtree_modifies_data(top_id)?;

        let mut rs_query: HashMap<String, String> = HashMap::new();
        if let Buckets::Filtered(_) = buckets {
            let rs_buckets = buckets.group()?;
            rs_query.reserve(rs_buckets.len());

            for (rs, bucket_ids) in &rs_buckets {
                let bucket_set = bucket_ids
                    .iter()
                    .copied()
                    .collect::<HashSet<u64, RepeatableState>>();
                let sql = plan.syntax_nodes_as_sql(&nodes, &Buckets::new_filtered(bucket_set))?;
                rs_query.insert(rs.to_string(), sql);
            }
        }
        if rs_query.is_empty() {
            // TODO: We send the query to all the nodes even when there
            // are no buckets in `Buckets::Filtered`. Such queries (when
            // we should not execute anything because we can predict an
            // empty result) can be executed on a round-robin node in
            // future (not implemented yet).
            let sql = plan.syntax_nodes_as_sql(&nodes, &Buckets::All)?;
            self.exec_on_all(&sql, is_data_modifier)
        } else {
            self.exec_on_replicas(&rs_query, is_data_modifier)
        }
    }

    /// Transform sub query results into a virtual table.
    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
        buckets: &Buckets,
    ) -> Result<VirtualTable, QueryPlannerError> {
        let top_id = plan.get_motion_subtree_root(motion_node_id)?;

        // We need to handle a special case when the output of the top node has
        // a replicated distribution (currently it means VALUES of constants).
        let top = plan.get_ir_plan().get_relation_node(top_id)?;
        let mut vtable = if let Relational::Values { .. } = top {
            plan.vtable_from_values(top_id)?
        } else {
            let result = self.exec(plan, top_id, buckets)?;
            result.as_virtual_table()?
        };

        if let Some(name) = &plan.get_motion_alias(motion_node_id)? {
            vtable.set_alias(name)?;
        }

        Ok(vtable)
    }

    /// Extract from the `HashMap<String, Value>` only those values that
    /// correspond to the fields of the sharding key.
    /// Returns the values of sharding keys in ordered form
    fn extract_sharding_keys<'engine, 'rec>(
        &'engine self,
        space: String,
        rec: &'rec HashMap<String, IrValue>,
    ) -> Result<Vec<&'rec IrValue>, QueryPlannerError> {
        self.metadata()
            .get_sharding_key_by_space(space.as_str())?
            .iter()
            .try_fold(Vec::new(), |mut acc: Vec<&IrValue>, &val| {
                match rec.get(val) {
                    Some(value) => {
                        acc.push(value);
                        Ok(acc)
                    }
                    None => Err(QueryPlannerError::CustomError(format!(
                        "The dict of args missed key/value to calculate bucket_id. Column: {}",
                        val
                    ))),
                }
            })
    }

    /// Calculate bucket for a key.
    fn determine_bucket_id(&self, s: &[&IrValue]) -> u64 {
        bucket_id_by_tuple(s, self.bucket_count)
    }
}

impl Runtime {
    /// Create new Tarantool cartridge runtime.
    ///
    /// # Errors
    /// - Failed to detect the correct amount of buckets.
    pub fn new() -> Result<Self, QueryPlannerError> {
        let cache: LRUCache<String, Plan> = LRUCache::new(DEFAULT_CAPACITY)?;
        let mut result = Runtime {
            metadata: ClusterAppConfig::new(),
            bucket_count: 0,
            query_cache: RefCell::new(cache),
        };

        result.set_bucket_count()?;

        Ok(result)
    }

    /// Function get summary count of bucket from vshard
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

    fn exec_on_replicas(
        &self,
        rs_query: &HashMap<String, String>,
        is_data_modifier: bool,
    ) -> Result<ExecutorResults, QueryPlannerError> {
        let lua = tarantool::lua_state();

        let exec_sql: LuaFunction<_> = lua.get("execute_on_replicas").ok_or_else(|| {
            QueryPlannerError::LuaError("Lua function `execute_on_replicas` not found".into())
        })?;

        let waiting_timeout = &self.metadata().get_exec_waiting_timeout();
        if is_data_modifier {
            let res: ConsumerResults =
                match exec_sql.call_with_args((rs_query, waiting_timeout, is_data_modifier)) {
                    Ok(v) => v,
                    Err(e) => {
                        say(
                            SayLevel::Error,
                            file!(),
                            line!().try_into().unwrap_or(0),
                            Option::from("execute_on_replicas"),
                            &format!("{:?}", e),
                        );
                        return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                    }
                };

            Ok(res.into())
        } else {
            let res: ProducerResults =
                match exec_sql.call_with_args((rs_query, waiting_timeout, is_data_modifier)) {
                    Ok(v) => v,
                    Err(e) => {
                        say(
                            SayLevel::Error,
                            file!(),
                            line!().try_into().unwrap_or(0),
                            Option::from("execute_on_replicas"),
                            &format!("{:?}", e),
                        );
                        return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                    }
                };

            Ok(res.into())
        }
    }

    fn exec_on_all(
        &self,
        query: &str,
        is_data_modifier: bool,
    ) -> Result<ExecutorResults, QueryPlannerError> {
        let lua = tarantool::lua_state();

        let exec_sql: LuaFunction<_> = lua.get("execute_on_all").ok_or_else(|| {
            QueryPlannerError::LuaError("Lua function `execute_on_all` not found".into())
        })?;

        let waiting_timeout = &self.metadata().get_exec_waiting_timeout();
        if is_data_modifier {
            let res: ConsumerResults =
                match exec_sql.call_with_args((query, waiting_timeout, is_data_modifier)) {
                    Ok(v) => v,
                    Err(e) => {
                        say(
                            SayLevel::Error,
                            file!(),
                            line!().try_into().unwrap_or(0),
                            Option::from("execute_on_all"),
                            &format!("{:?}", e),
                        );
                        return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                    }
                };

            Ok(res.into())
        } else {
            let res: ProducerResults =
                match exec_sql.call_with_args((query, waiting_timeout, is_data_modifier)) {
                    Ok(v) => v,
                    Err(e) => {
                        say(
                            SayLevel::Error,
                            file!(),
                            line!().try_into().unwrap_or(0),
                            Option::from("execute_on_all"),
                            &format!("{:?}", e),
                        );
                        return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                    }
                };

            Ok(res.into())
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

/// Extra lua functions loader. It is necessary for query execution.
///
/// # Errors
/// - Failed to load lua code.
#[allow(clippy::too_many_lines)]
pub fn load_extra_function() -> Result<(), QueryPlannerError> {
    let lua = tarantool::lua_state();

    match lua.exec(
        r#"local vshard = require('vshard')
    local yaml = require('yaml')
    local log = require('log')
    local cartridge = require('cartridge')

    function get_schema()
        return cartridge.get_schema()
    end

    function get_waiting_timeout()
        local cfg = cartridge.config_get_readonly()

        if cfg["executor_waiting_timeout"] == nil then
            return 0
        end

        return cfg["executor_waiting_timeout"]
    end

    function get_cache_capacity()
        local cfg = cartridge.config_get_readonly()

        if cfg["executor_cache_capacity"] == nil then
            return 50
        end

        return cfg["executor_cache_capacity"]
    end

    function group_buckets_by_replicasets(buckets)
        local map = {}
        for _, bucket_id in pairs(buckets) do
            local rs = vshard.router.route(bucket_id).uuid
            if map[rs] then
                table.insert(map[rs], bucket_id)
            else
                map[rs] = {bucket_id}
            end
        end

        return map
    end

    function execute_on_replicas(tbl_rs_query, waiting_timeout, is_data_modifier)
        local result = nil
        local futures = {}

        for rs_uuid, query in pairs(tbl_rs_query) do
            local replica = vshard.router.routeall()[rs_uuid]
            if is_data_modifier then
                local future, err = replica:callrw("box.execute", { query }, {is_async = true})
                if err ~= nil then
                    error(err)
                end
                table.insert(futures, future) 
            else
                local future, err = replica:callbre("box.execute", { query }, {is_async = true})
                if err ~= nil then
                    error(err)
                end
                table.insert(futures, future) 
            end
        end

        for _, future in ipairs(futures) do
            future:wait_result(waiting_timeout)
            local res = future:result()

            if res[1] == nil then
                error(res[2])
            end

            if result == nil then
                result = res[1]
            else
                if is_data_modifier then
                    result.row_count = result.row_count + res[1].row_count
                else
                    for _, item in pairs(res[1].rows) do
                        table.insert(result.rows, item)
                    end
                end
            end
        end

        return result
    end

    function execute_on_all(query, waiting_timeout, is_data_modifier)

        local replicas = vshard.router.routeall()
        local result = nil
        local futures = {}

        for _, replica in pairs(replicas) do
            if is_data_modifier then
                local future, err = replica:callrw("box.execute", { query }, {is_async = true})
                if err ~= nil then
                    error(err)
                end
                table.insert(futures, future) 
            else
                local future, err = replica:callbre("box.execute", { query }, {is_async = true})
                if err ~= nil then
                    error(err)
                end
                table.insert(futures, future) 
            end
        end

        for _, future in ipairs(futures) do
             future:wait_result(waiting_timeout)
             local res = future:result()

             if res[1] == nil then
                 error(res[2])
             end

             if result == nil then
                 result = res[1]
             else
                 for _, item in pairs(res[1].rows) do
                    table.insert(result.rows, item)
                 end
             end
         end

        return result
    end
"#,
    ) {
        Ok(_) => Ok(()),
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("exec_query"),
                &format!("{:?}", e),
            );
            Err(QueryPlannerError::LuaError(format!(
                "Failed lua code loading: {:?}",
                e
            )))
        }
    }
}
