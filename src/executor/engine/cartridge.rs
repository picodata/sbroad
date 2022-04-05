use std::collections::HashSet;
use std::convert::TryInto;

use tarantool::log::{say, SayLevel};
use tarantool::tlua::LuaFunction;

use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::engine::cartridge::cache::ClusterSchema;
use crate::executor::engine::cartridge::hash::str_to_bucket_id;
use crate::executor::engine::Engine;
use crate::executor::ir::ExecutionPlan;
use crate::executor::result::BoxExecuteFormat;
use crate::executor::vtable::VirtualTable;

mod backend;
pub mod cache;
pub mod hash;

#[derive(Debug, Clone)]
pub struct Runtime {
    metadata: ClusterSchema,
    bucket_count: usize,
}

/// Implements `Engine` interface for tarantool cartridge application
impl Engine for Runtime {
    type Metadata = ClusterSchema;

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    fn has_metadata(&self) -> bool {
        self.metadata.is_empty()
    }

    fn clear_metadata(&mut self) {
        self.metadata = ClusterSchema::new();
    }

    fn load_metadata(&mut self) -> Result<(), QueryPlannerError> {
        let lua = tarantool::lua_state();
        let get_schema: LuaFunction<_> =
            lua.eval("return require('cartridge').get_schema").unwrap();

        let res: String = match get_schema.call() {
            Ok(res) => res,
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("load metadata"),
                    &format!("{:?}", e),
                );
                return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
            }
        };

        self.metadata.load(&res)
    }

    /// Execute sub tree on the nodes
    fn exec(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<BoxExecuteFormat, QueryPlannerError> {
        let mut result = BoxExecuteFormat::new();
        let sql = plan.subtree_as_sql(top_id)?;

        match buckets {
            Buckets::All => {
                result.extend(cluster_exec_query(&sql)?)?;
            }
            Buckets::Filtered(list) => {
                result.extend(bucket_exec_query(list, &sql)?)?;
            }
        }

        Ok(result)
    }

    /// Transform sub query results into a virtual table.
    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
        buckets: &Buckets,
    ) -> Result<VirtualTable, QueryPlannerError> {
        let top = &plan.get_motion_subtree_root(motion_node_id)?;

        let result = self.exec(plan, *top, buckets)?;
        let mut vtable = result.as_virtual_table()?;

        if let Some(name) = &plan.get_motion_alias(motion_node_id)? {
            vtable.set_alias(name)?;
        }

        Ok(vtable)
    }

    /// Calculate bucket for a key.
    fn determine_bucket_id(&self, s: &str) -> u64 {
        str_to_bucket_id(s, self.bucket_count)
    }
}

impl Runtime {
    pub fn new() -> Result<Self, QueryPlannerError> {
        let mut result = Runtime {
            metadata: ClusterSchema::new(),
            bucket_count: 0,
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
}

/// Send the query to a single bucket and merge results (map-reduce).
fn bucket_exec_query(
    buckets: &HashSet<u64>,
    query: &str,
) -> Result<BoxExecuteFormat, QueryPlannerError> {
    let lua = tarantool::lua_state();
    match lua.exec(
        r#"
    local vshard = require('vshard')
    local yaml = require('yaml')
    local log = require('log')

    ---get_uniq_replicaset_for_buckets - gets unique set of replicaset by bucket list
    ---@param buckets table - list of buckets.
    function get_uniq_replicaset_for_buckets(buckets)
        local res = {}
        local uniq_replicas = {}
        for _, bucket_id in pairs(buckets) do
            local current_replicaset = vshard.router.route(bucket_id)
            uniq_replicas[current_replicaset.uuid] = current_replicaset
        end

        for _, r in pairs(uniq_replicas) do
            table.insert(res, r)
        end

        return res
    end

    function execute_sql(buckets, query)
        local replicas = get_uniq_replicaset_for_buckets(buckets)
        local result = nil
        local futures = {}

        for _, replica in pairs(replicas) do
             local future, err = replica:callro("box.execute", { query }, {is_async = true})
             if err ~= nil then
                 error(err)
             end
             table.insert(futures, future)
             log.debug("Execute a query " .. query .. " on replica")
        end

        for _, future in ipairs(futures) do
             future:wait_result(360)
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
        Ok(_) => {}
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("exec_query"),
                &format!("{:?}", e),
            );
            return Err(QueryPlannerError::LuaError(format!(
                "Failed lua code loading: {:?}",
                e
            )));
        }
    }

    let exec_sql: LuaFunction<_> = lua.get("execute_sql").ok_or_else(|| {
        QueryPlannerError::LuaError("Lua function `execute_sql` not found".into())
    })?;

    let lua_buckets: Vec<u64> = buckets.iter().copied().collect();
    let res: BoxExecuteFormat = match exec_sql.call_with_args((lua_buckets, query)) {
        Ok(v) => v,
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("exec_query"),
                &format!("{:?}", e),
            );
            return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
        }
    };

    Ok(res)
}

/// Send the query to all instances and merge results (map-reduce).
fn cluster_exec_query(query: &str) -> Result<BoxExecuteFormat, QueryPlannerError> {
    say(
        SayLevel::Debug,
        file!(),
        line!().try_into().unwrap_or(0),
        None,
        &format!("Execute a query {:?} on all instances", query),
    );

    let lua = tarantool::lua_state();

    match lua.exec(
        r#"
        local vshard = require('vshard')
        local yaml = require('yaml')

        function map_reduce_execute_sql(query)
            local replicas = vshard.router.routeall()

            local result = nil
            local futures = {}
            for _, replica in pairs(replicas) do
                 local future, err = replica:callro("box.execute", { query }, {is_async = true})
                 if err ~= nil then
                     error(err)
                 end
                 table.insert(futures, future)
            end

            for _, future in ipairs(futures) do
                 future:wait_result(360)
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
        Ok(_) => {}
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("cluster_exec_query"),
                &format!("{:?}", e),
            );
            return Err(QueryPlannerError::LuaError(format!(
                "Failed lua code loading: {:?}",
                e
            )));
        }
    }

    let exec_sql: LuaFunction<_> = lua.get("map_reduce_execute_sql").ok_or_else(|| {
        QueryPlannerError::LuaError("Lua function `map_reduce_execute_sql` not found".into())
    })?;

    let res: BoxExecuteFormat = match exec_sql.call_with_args(query) {
        Ok(v) => v,
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("cluster_exec_query"),
                &format!("{:?}", e),
            );
            return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
        }
    };

    Ok(res)
}
