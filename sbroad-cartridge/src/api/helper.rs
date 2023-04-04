use sbroad::error;
use sbroad::log::tarantool_error;
use std::cell::RefCell;
use std::os::raw::c_int;
use std::thread::LocalKey;

use crate::cartridge::Configuration;

pub fn load_config<Runtime>(engine: &'static LocalKey<RefCell<Runtime>>) -> c_int
where
    Runtime: Configuration,
{
    // Tarantool can yield in the middle of a current closure,
    // so we can hold only an immutable reference to the engine.
    let mut config: Option<_> = None;
    let mut code = (*engine).with(|engine| {
        let runtime = match engine.try_borrow() {
            Ok(runtime) => runtime,
            Err(e) => {
                return tarantool_error(&format!(
                    "Failed to borrow the runtime while loading configuration: {e}"
                ));
            }
        };
        match runtime.retrieve_config() {
            Ok(conf) => {
                config = conf;
                0
            }
            Err(e) => {
                error!(Option::from("get config"), &format!("{e:?}"));
                tarantool_error(&format!("Failed to get configuration: {e}"))
            }
        }
    });

    if code != 0 {
        return code;
    }

    // Tarantool never yields here, so it is possible to hold
    // a mutable reference to the engine.
    if let Some(config) = config {
        code = (*engine).with(|runtime| {
            let runtime = match runtime.try_borrow() {
                Ok(runtime) => runtime,
                Err(e) => {
                    return tarantool_error(&format!(
                        "Failed to borrow the runtime while updating configuration: {e}"
                    ));
                }
            };
            if let Err(e) = runtime.update_config(config) {
                return tarantool_error(&format!(
                    "Failed to update the configuration in the runtime during configuration loading: {e}"
                ));
            }
            0
        });
    }

    code
}
