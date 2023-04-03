#[cfg(not(feature = "mock"))]
pub fn fiber_id() -> u64 {
    let lua = tarantool::lua_state();
    lua.eval("return require('fiber').id()").unwrap()
}
