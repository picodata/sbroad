use std::convert::TryInto;

use tarantool::log::{say, SayLevel};
use tarantool::tlua::LuaFunction;

use crate::errors::QueryPlannerError;
use crate::executor::engine::cartridge::bucket::str_to_bucket_id;
use crate::executor::engine::cartridge::cache::ClusterSchema;
use crate::executor::engine::Engine;
use crate::executor::result::BoxExecuteFormat;

pub mod bucket;
pub mod cache;

#[derive(Debug, Clone)]
pub struct Runtime {
    metadata: ClusterSchema,
    bucket_count: usize,
}

/// Implements `Engine` interface for tarantool cartridge application
impl Engine for Runtime {
    type Metadata = ClusterSchema;

    fn metadata(&self) -> Self::Metadata {
        self.metadata.clone()
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

    /// Function execute sql query on selected node
    fn exec_query(
        &self,
        shard_key: &str,
        query: &str,
    ) -> Result<BoxExecuteFormat, QueryPlannerError> {
        let cast_bucket_id: u64 = match self.determine_bucket_id(shard_key).try_into() {
            Ok(v) => v,
            Err(_) => {
                return Err(QueryPlannerError::CustomError("Invalid bucket id".into()));
            }
        };

        let lua = tarantool::lua_state();
        lua.exec(
            r#"
        local vshard = require('vshard')
        local yaml = require('yaml')

        function execute_sql(bucket_id, query)
            local res, err = vshard.router.call(
                bucket_id,
                'read',
                'box.execute',
                { query }
            )

            if err ~= nil then
                error(err)
            end

            return res
        end
    "#,
        )
        .unwrap();

        let exec_sql: LuaFunction<_> = lua.get("execute_sql").unwrap();
        let res: BoxExecuteFormat = match exec_sql.call_with_args((cast_bucket_id, query)) {
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

    /// Sends query to all instances and merges results after (map-reduce).
    fn mp_exec_query(&self, query: &str) -> Result<BoxExecuteFormat, QueryPlannerError> {
        let lua = tarantool::lua_state();

        lua.exec(
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
        )
        .unwrap();

        let exec_sql: LuaFunction<_> = lua.get("map_reduce_execute_sql").unwrap();
        let res: BoxExecuteFormat = match exec_sql.call_with_args(query) {
            Ok(v) => v,
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("mp_exec_query"),
                    &format!("{:?}", e),
                );
                return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
            }
        };

        Ok(res)
    }

    /// Calculation ``bucket_id`` function
    fn determine_bucket_id(&self, s: &str) -> usize {
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

        let bucket_count_fn: LuaFunction<_> = lua
            .eval("return require('vshard').router.bucket_count")
            .unwrap();
        let bucket_count: u64 = match bucket_count_fn.call() {
            Ok(r) => r,
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("load metadata"),
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
