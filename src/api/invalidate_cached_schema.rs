use std::os::raw::c_int;

use tarantool::tuple::{FunctionArgs, FunctionCtx};

use crate::api::COORDINATOR_ENGINE;
use crate::executor::engine::Coordinator;

/// Lua invalidates schema cache function, then it updates schema before next query.
/// It must be called in function `apply_config()` in lua cartridge application.
#[no_mangle]
pub extern "C" fn invalidate_cached_schema(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    COORDINATOR_ENGINE.with(|s| {
        let v = &mut *s.borrow_mut();
        v.clear_metadata();
    });

    ctx.return_mp(&true).unwrap();
    0
}
