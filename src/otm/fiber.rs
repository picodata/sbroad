#[cfg(not(feature = "mock"))]
pub(super) fn fiber_id() -> u64 {
    static mut FIBER_FUNC_DEFINED: bool = false;
    let lua = global_lua();

    if !unsafe { FIBER_FUNC_DEFINED } {
        #[rustfmt::skip]
        lua.exec(
            r#"
                function fiber_id()
                    local fiber = require('fiber')
                    return fiber.id()
                end
            "#,
        ).unwrap();

        unsafe {
            FIBER_FUNC_DEFINED = true;
        }
    }
    lua.get::<tarantool::tlua::LuaFunction<_>, _>("fiber_id")
        .unwrap()
        .into_call()
        .unwrap()
}

#[cfg(not(feature = "mock"))]
fn global_lua() -> tarantool::tlua::StaticLua {
    use tarantool::tlua::Lua;

    unsafe { Lua::from_static(tarantool::ffi::tarantool::luaT_state()) }
}
