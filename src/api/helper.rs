use crate::executor::engine::Configuration;
use std::cell::RefCell;
use std::os::raw::c_int;
use std::thread::LocalKey;
use tarantool::error::TarantoolErrorCode;

pub fn load_config<Runtime>(engine: &'static LocalKey<RefCell<Runtime>>) -> c_int
where
    Runtime: Configuration,
{
    // Tarantool can yield in the middle of a current closure,
    // so we can hold only an immutable reference to the engine.
    let mut config: Option<_> = None;
    (*engine).with(|engine| {
        let runtime = match engine.try_borrow() {
            Ok(runtime) => runtime,
            Err(e) => {
                return tarantool::set_error!(
                    TarantoolErrorCode::ProcC,
                    "Failed to borrow the runtime while loading configuration: {}",
                    e.to_string()
                );
            }
        };
        match runtime.get_config() {
            Ok(conf) => {
                config = conf;
                0
            }
            Err(e) => {
                return tarantool::set_error!(
                    TarantoolErrorCode::ProcC,
                    "Failed to get configuration: {}",
                    e.to_string()
                );
            }
        }
    });

    // Tarantool never yields here, so it is possible to hold
    // a mutable reference to the engine.
    if let Some(config) = config {
        (*engine).with(|runtime| {
            let mut runtime = match runtime.try_borrow_mut() {
                Ok(runtime) => runtime,
                Err(e) => {
                    return tarantool::set_error!(
                        TarantoolErrorCode::ProcC,
                        "Failed to borrow the runtime while updating configuration: {}",
                        e.to_string()
                    );
                }
            };
            runtime.update_config(config);
            0
        });
    }

    0
}
