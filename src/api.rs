use crate::executor::engine::cartridge::router::RouterRuntime;
use crate::executor::engine::cartridge::storage::StorageRuntime;
use std::cell::RefCell;

thread_local!(static COORDINATOR_ENGINE: RefCell<RouterRuntime> = RefCell::new(RouterRuntime::new().unwrap()));
thread_local!(static SEGMENT_ENGINE: RefCell<StorageRuntime> = RefCell::new(StorageRuntime::new().unwrap()));

pub mod calculate_bucket_id;
pub mod calculate_bucket_id_by_dict;
pub mod calculate_bucket_id_by_tuple;
pub mod exec_query;
pub mod explain;
mod helper;
pub mod invalidate_cached_schema;
pub mod load_lua_extra_function;
