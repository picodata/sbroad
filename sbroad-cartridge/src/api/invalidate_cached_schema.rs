use std::os::raw::c_int;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

use crate::api::{COORDINATOR_ENGINE, SEGMENT_ENGINE};
use sbroad::executor::engine::{Configuration, Coordinator};
use sbroad::log::tarantool_error;

/// Flush cached configuration in the Rust memory of the coordinator runtime.
/// This function should be invoked in the Lua cartridge application with `apply_config()`.
#[no_mangle]
pub extern "C" fn invalidate_coordinator_cache(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    COORDINATOR_ENGINE.with(|runtime| match runtime.try_borrow_mut() {
        Ok(mut runtime) => {
            runtime.clear_config();
            if let Err(e) = runtime.clear_ir_cache() {
                return tarantool_error(&format!(
                    "Failed to clear the IR cache on router: {:?}",
                    e
                ));
            }
            ctx.return_mp(&true).unwrap();
            0
        }
        Err(e) => tarantool_error(&format!(
            "Failed to borrow the runtime while clearing cached configuration on router: {}",
            e
        )),
    })
}

/// Flush cached configuration in the Rust memory of the segment runtime.
/// This function should be invoked in the Lua cartridge application with `apply_config()`.
#[no_mangle]
pub extern "C" fn invalidate_segment_cache(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    SEGMENT_ENGINE.with(|runtime| match runtime.try_borrow_mut() {
        Ok(mut runtime) => {
            runtime.clear_config();
            ctx.return_mp(&true).unwrap();
            0
        }
        Err(e) => tarantool_error(&format!(
            "Failed to borrow the runtime while clearing cached configuration on a storage: {}",
            e
        )),
    })
}
