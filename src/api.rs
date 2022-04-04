use std::cell::RefCell;

use crate::executor::engine::cartridge;

thread_local!(static QUERY_ENGINE: RefCell<cartridge::Runtime> = RefCell::new(cartridge::Runtime::new().unwrap()));

pub mod calculate_bucket_id;
pub mod calculate_bucket_id_by_dict;
pub mod exec_query;
pub mod explain;
mod helper;
pub mod invalidate_cached_schema;
pub mod load_lua_extra_function;
