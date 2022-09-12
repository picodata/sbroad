use std::os::raw::c_int;

use serde::{Deserialize, Serialize};
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

use crate::api::helper::load_config;
use crate::api::COORDINATOR_ENGINE;
use crate::error;
use crate::errors::QueryPlannerError;
use crate::executor::Query;
use crate::log::tarantool_error;

#[derive(Serialize, Deserialize)]
/// Lua function params
struct Args {
    /// Target sql query
    query: String,
}

impl TryFrom<FunctionArgs> for Args {
    type Error = QueryPlannerError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        Tuple::from(value)
            .decode::<Args>()
            .map_err(|e| QueryPlannerError::CustomError(format!("Parsing args error: {:?}", e)))
    }
}

/// Print query explain.
#[no_mangle]
pub extern "C" fn explain(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let lua_params = match Args::try_from(args) {
        Ok(param) => param,
        Err(e) => return tarantool_error(&e.to_string()),
    };

    let ret_code = load_config(&COORDINATOR_ENGINE);
    if ret_code != 0 {
        return ret_code;
    }
    COORDINATOR_ENGINE.with(|engine| {
        let runtime = match engine.try_borrow() {
            Ok(runtime) => runtime,
            Err(e) => {
                return tarantool_error(&format!(
                    "Failed to borrow runtime while explaining the query: {}",
                    e
                ));
            }
        };
        let query = match Query::new(&*runtime, &lua_params.query, vec![]) {
            Ok(q) => q,
            Err(e) => {
                error!(Option::from("explain"), &format!("{:?}", e));
                return tarantool_error(&e.to_string());
            }
        };

        match query.explain() {
            Ok(q) => {
                ctx.return_mp(&q).unwrap();
                0
            }
            Err(e) => tarantool_error(&e.to_string()),
        }
    })
}
