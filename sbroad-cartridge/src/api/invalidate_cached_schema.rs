use std::os::raw::c_int;
use tarantool::tuple::{FunctionArgs, FunctionCtx};

use crate::{
    api::{COORDINATOR_ENGINE, SEGMENT_ENGINE},
    cartridge::ConfigurationProvider,
};
use sbroad::{executor::engine::QueryCache, log::tarantool_error};

/// Flush cached configuration in the Rust memory of the coordinator runtime.
/// This function should be invoked in the Lua cartridge application with `apply_config()`.
#[no_mangle]
pub extern "C" fn invalidate_coordinator_cache(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    COORDINATOR_ENGINE.with(|runtime| match runtime.try_borrow() {
        Ok(runtime) => {
            if let Err(e) = runtime.clear_config() {
                return tarantool_error(&format!(
                    "Failed to clear the configuration in the coordinator runtime during cache invalidation: {e}"
                ));
            }
            if let Err(e) = runtime.clear_cache() {
                return tarantool_error(&format!(
                    "Failed to clear the IR cache on router: {e:?}"
                ));
            }
            ctx.return_mp(&true).unwrap();
            0
        }
        Err(e) => tarantool_error(&format!(
            "Failed to borrow the runtime while clearing cached configuration on router: {e}"
        )),
    })
}

/// Flush cached configuration in the Rust memory of the segment runtime.
/// This function should be invoked in the Lua cartridge application with `apply_config()`.
#[no_mangle]
pub extern "C" fn invalidate_segment_cache(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    SEGMENT_ENGINE.with(|runtime| match runtime.try_borrow() {
        Ok(runtime) => {
            if let Err(e) = runtime.clear_config() {
                return tarantool_error(&format!(
                    "Failed to clear the storage configuration: {e:?}"
                ));
            }
            ctx.return_mp(&true).unwrap();
            0
        }
        Err(e) => tarantool_error(&format!(
            "Failed to borrow the runtime while clearing cached configuration on a storage: {e}"
        )),
    })
}
