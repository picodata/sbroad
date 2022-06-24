use crate::executor::engine::cartridge::router::RouterRuntime;
use std::cell::RefCell;

thread_local!(static COORDINATOR_ENGINE: RefCell<RouterRuntime> = RefCell::new(RouterRuntime::new().unwrap()));

pub mod calculate_bucket_id;
pub mod calculate_bucket_id_by_dict;
pub mod exec_query;
pub mod explain;
mod helper;
pub mod invalidate_cached_schema;
pub mod load_lua_extra_function;
