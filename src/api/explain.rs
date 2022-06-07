use std::os::raw::c_int;

use serde::{Deserialize, Serialize};
use tarantool::error::TarantoolErrorCode;
use tarantool::log::{say, SayLevel};
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

use crate::api::helper::load_metadata;
use crate::api::QUERY_ENGINE;
use crate::errors::QueryPlannerError;
use crate::executor::Query;

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
            .into_struct::<Args>()
            .map_err(|e| QueryPlannerError::CustomError(format!("Parsing args error: {:?}", e)))
    }
}

/// Print query explain.
#[no_mangle]
pub extern "C" fn explain(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let lua_params = match Args::try_from(args) {
        Ok(param) => param,
        Err(e) => return tarantool::set_error!(TarantoolErrorCode::ProcC, "{:?}", e),
    };

    let ret_code = load_metadata();
    if ret_code != 0 {
        return ret_code;
    }
    QUERY_ENGINE.with(|e| {
        let engine = &*e.borrow();
        let query = match Query::new(engine, &lua_params.query, &[]) {
            Ok(q) => q,
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    None,
                    &format!("{:?}", e),
                );
                return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", format!("{:?}", e));
            }
        };

        match query.explain() {
            Ok(q) => {
                ctx.return_mp(&q).unwrap();
                0
            }
            Err(e) => tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string()),
        }
    })
}