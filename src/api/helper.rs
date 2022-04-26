use std::os::raw::c_int;

use tarantool::error::TarantoolErrorCode;

use crate::api::QUERY_ENGINE;
use crate::executor::engine::{Engine, LocalMetadata};

pub fn load_metadata() -> c_int {
    // Tarantool can yield in the middle of a current closure,
    // so we can hold only an immutable reference to the engine.
    let mut metadata: Option<LocalMetadata> = None;
    QUERY_ENGINE.with(|e| {
        let engine = &*e.borrow();
        match engine.get_metadata() {
            Ok(meta) => {
                metadata = meta;
                0
            }
            Err(e) => {
                return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string());
            }
        }
    });

    let mut is_metadata_empty = false;
    QUERY_ENGINE.with(|e| {
        let engine = &*e.borrow();
        if engine.is_metadata_empty() {
            is_metadata_empty = true;
        }
    });
    // Tarantool never yields here, so it is possible to hold
    // a mutable reference to the engine.
    if is_metadata_empty {
        if let Some(metadata) = metadata {
            QUERY_ENGINE.with(|e| {
                let engine = &mut *e.borrow_mut();
                if let Err(e) = engine.update_metadata(metadata) {
                    return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string());
                }
                0
            });
        }
    }

    0
}
