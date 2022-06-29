use std::os::raw::c_int;
use tarantool::error::TarantoolErrorCode;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

use crate::executor::engine::cartridge::router::load_router_functions;
use crate::executor::engine::cartridge::storage::load_storage_functions;

/// Loads extra Lua code for the router engine.
#[no_mangle]
pub extern "C" fn load_lua_router_functions(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    match load_router_functions() {
        Ok(_) => {
            ctx.return_mp(&true).unwrap();
            0
        }
        Err(e) => {
            tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string())
        }
    }
}

/// Loads extra Lua code for the storage engine.
#[no_mangle]
pub extern "C" fn load_lua_storage_functions(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    match load_storage_functions() {
        Ok(_) => {
            ctx.return_mp(&true).unwrap();
            0
        }
        Err(e) => {
            tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string())
        }
    }
}
