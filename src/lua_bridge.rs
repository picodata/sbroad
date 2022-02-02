use crate::executor::result::BoxExecuteFormat;
use tarantool::ffi::tarantool::luaT_state;
use tarantool::hlua::{Lua, LuaError, LuaFunction};

/// Function get cartridge cluster schema
pub fn get_cluster_schema() -> Result<String, LuaError> {
    let lua = unsafe { Lua::from_existing_state(luaT_state(), false) };

    let get_schema: LuaFunction<_> = lua.eval("return require('cartridge').get_schema")?;
    let res = get_schema.call()?;

    Ok(res)
}

/// Function execute sql query on selected node
pub fn exec_query(bucket_id: u64, query: &str) -> Result<BoxExecuteFormat, LuaError> {
    let lua = unsafe { Lua::from_existing_state(luaT_state(), false) };

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
    )?;

    let exec_sql: LuaFunction<_> = lua.get("execute_sql").unwrap();
    let res: BoxExecuteFormat = exec_sql.call_with_args((bucket_id, query))?;

    Ok(res)
}

/// Sends query to all instances and merges results after (map-reduce).
pub fn mp_exec_query(query: &str) -> Result<BoxExecuteFormat, LuaError> {
    let lua = unsafe { Lua::from_existing_state(luaT_state(), false) };

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
    )?;

    let exec_sql: LuaFunction<_> = lua.get("map_reduce_execute_sql").unwrap();
    let res: BoxExecuteFormat = exec_sql.call_with_args(query)?;

    Ok(res)
}

/// Function get summary count of bucket from vshard
pub fn bucket_count() -> Result<u64, LuaError> {
    let lua = unsafe { Lua::from_existing_state(luaT_state(), false) };

    let bucket_count_fn: LuaFunction<_> =
        lua.eval("return require('vshard').router.bucket_count")?;
    let result = bucket_count_fn.call()?;

    Ok(result)
}
