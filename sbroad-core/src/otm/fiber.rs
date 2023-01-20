#[cfg(not(feature = "mock"))]
pub fn fiber_id() -> u64 {
    let lua = tarantool::lua_state();

    lua.get::<tarantool::tlua::LuaFunction<_>, _>("fiber_id")
        .unwrap()
        .into_call()
        .unwrap()
}
