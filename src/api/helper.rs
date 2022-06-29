use crate::executor::engine::Configuration;
use std::cell::RefCell;
use std::os::raw::c_int;
use std::thread::LocalKey;
use tarantool::error::TarantoolErrorCode;

pub fn load_config<Runtime>(runtime: &'static LocalKey<RefCell<Runtime>>) -> c_int
where
    Runtime: Configuration,
{
    // Tarantool can yield in the middle of a current closure,
    // so we can hold only an immutable reference to the engine.
    let mut config: Option<_> = None;
    (*runtime).with(|engine| match engine.borrow().get_config() {
        Ok(conf) => {
            config = conf;
            0
        }
        Err(e) => {
            return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string());
        }
    });

    let mut is_empty = false;
    (*runtime).with(|engine| {
        if engine.borrow().is_config_empty() {
            is_empty = true;
        }
    });
    // Tarantool never yields here, so it is possible to hold
    // a mutable reference to the engine.
    if is_empty {
        if let Some(config) = config {
            (*runtime).with(|engine| {
                engine.borrow_mut().update_config(config);
                0
            });
        }
    }

    0
}
