use std::cell::RefCell;
use std::os::raw::c_int;
use std::thread::LocalKey;
use tarantool::error::TarantoolErrorCode;
use tarantool::log::{say, SayLevel};
use tarantool::tuple::{FunctionArgs, FunctionCtx};

use crate::api::{COORDINATOR_ENGINE, SEGMENT_ENGINE};
use crate::executor::engine::Configuration;

fn clear_cached_config<Runtime>(
    runtime: &'static LocalKey<RefCell<Runtime>>,
    ctx: &FunctionCtx,
    _: FunctionArgs,
) -> c_int
where
    Runtime: Configuration,
{
    runtime.with(|s| {
        if let Ok(mut runtime) = s.try_borrow_mut() {
            runtime.clear_config();
        } else {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                None,
                &format!("{:?}", "Failed to borrow runtime: already in use"),
            );
            tarantool::set_error!(
                TarantoolErrorCode::ProcC,
                "{}",
                format!("{:?}", "Failed to borrow runtime: already in use")
            );
        }
    });

    ctx.return_mp(&true).unwrap();
    0
}

/// Flush cached configuration in the Rust memory of the coordinator runtime.
/// This function should be invoked in the Lua cartridge application with `apply_config()`.
#[no_mangle]
pub extern "C" fn invalidate_coordinator_cache(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    clear_cached_config(&COORDINATOR_ENGINE, &ctx, args)
}

/// Flush cached configuration in the Rust memory of the segment runtime.
/// This function should be invoked in the Lua cartridge application with `apply_config()`.
#[no_mangle]
pub extern "C" fn invalidate_segment_cache(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    clear_cached_config(&SEGMENT_ENGINE, &ctx, args)
}
