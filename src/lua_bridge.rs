use tarantool::ffi::tarantool::luaT_state;
use tarantool::hlua::{Lua, LuaError, LuaFunction};

pub fn get_cluster_schema() -> Result<String, LuaError> {
    let lua = unsafe { Lua::from_existing_state(luaT_state(), false) };

    let get_schema: LuaFunction<_> = lua.eval("return require('cartridge').get_schema")?;
    let res = get_schema.call()?;

    Ok(res)
}

pub fn exec_query(bucket_id: u64, query: &str) -> Result<String, LuaError> {
    let lua = unsafe { Lua::from_existing_state(luaT_state(), false) };

    lua.exec(
        r#"
        local vshard = require('vshard')
        local yaml = require('yaml')

        function execute_sql(bucket_id, query)
            local data, err = vshard.router.call(
                bucket_id,
                'read',
                'box.execute',
                { query }
            )

            if err ~= nil then
                error(err)
            end

            return yaml.encode(data)
        end
    "#,
    )?;

    let exec_sql: LuaFunction<_> = lua.get("execute_sql").unwrap();
    let res = exec_sql.call_with_args((bucket_id, query))?;

    Ok(res)
}
