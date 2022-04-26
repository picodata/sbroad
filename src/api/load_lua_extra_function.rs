use std::os::raw::c_int;

use tarantool::error::TarantoolErrorCode;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

use crate::executor::engine::cartridge::load_extra_function;

/// Loads extra Lua code for the engine.
#[no_mangle]
pub extern "C" fn load_lua_extra_function(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    match load_extra_function() {
        Ok(_) => {
            ctx.return_mp(&true).unwrap();
            0
        }
        Err(e) => {
            tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string())
        }
    }
}
