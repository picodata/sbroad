use crate::executor::result::BoxExecuteResult;
use tarantool::ffi::tarantool::luaT_state;
use tarantool::hlua::{Lua, LuaError, LuaFunction};
use tarantool::log::{say, SayLevel};

/// Function get cartridge cluster schema
pub fn get_cluster_schema() -> Result<String, LuaError> {
    let lua = unsafe { Lua::from_existing_state(luaT_state(), false) };

    let get_schema: LuaFunction<_> = lua.eval("return require('cartridge').get_schema")?;
    let res = get_schema.call()?;

    Ok(res)
}

/// Function execute sql query on selected node
pub fn exec_query(bucket_id: u64, query: &str) -> Result<BoxExecuteResult, LuaError> {
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
    let res: BoxExecuteResult = exec_sql.call_with_args((bucket_id, query))?;

    say(
        SayLevel::Error,
        "lua_bridge.rs",
        110,
        None,
        &format!("{:?}", res),
    );

    Ok(res)
}
