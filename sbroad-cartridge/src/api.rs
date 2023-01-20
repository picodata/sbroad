use crate::cartridge::router::RouterRuntime;
use crate::cartridge::storage::StorageRuntime;
use std::cell::RefCell;

#[derive(Default)]
pub struct AsyncCommands {
    pub invalidate_router_cache: bool,
    pub invalidate_segment_cache: bool,
}

thread_local!(static COORDINATOR_ENGINE: RefCell<RouterRuntime> = RefCell::new(RouterRuntime::new().unwrap()));
thread_local!(static SEGMENT_ENGINE: RefCell<StorageRuntime> = RefCell::new(StorageRuntime::new().unwrap()));
thread_local!(static ASYNC_COMMANDS: RefCell<AsyncCommands> = RefCell::new(AsyncCommands::default()));

pub mod calculate_bucket_id;
pub mod exec_query;
mod helper;
pub mod invalidate_cached_schema;
pub mod statistics;
