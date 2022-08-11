use serde::{Deserialize, Deserializer};
use std::os::raw::c_int;
use tarantool::error::TarantoolErrorCode;
use tarantool::log::{say, SayLevel};
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

use crate::api::helper::load_config;
use crate::api::{COORDINATOR_ENGINE, SEGMENT_ENGINE};
use crate::errors::QueryPlannerError;
use crate::executor::engine::cartridge::backend::sql::ir::PatternWithParams;
use crate::executor::Query;
use crate::ir::value::Value;

/// Dispatch parameterized SQL query from coordinator to the segments.
#[no_mangle]
pub extern "C" fn dispatch_query(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let lua_params = match PatternWithParams::try_from(args) {
        Ok(param) => param,
        Err(e) => return tarantool::set_error!(TarantoolErrorCode::ProcC, "{:?}", e),
    };

    let ret_code = load_config(&COORDINATOR_ENGINE);
    if ret_code != 0 {
        return ret_code;
    }
    COORDINATOR_ENGINE.with(|engine| {
        let runtime = match engine.try_borrow() {
            Ok(runtime) => runtime,
            Err(e) => {
                return tarantool::set_error!(
                    TarantoolErrorCode::ProcC,
                    "Failed to borrow the runtime while dispatching the query: {}",
                    e.to_string()
                );
            }
        };
        let mut query = match Query::new(&*runtime, &lua_params.pattern, &lua_params.params) {
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

        match query.dispatch() {
            Ok(result) => {
                if let Some(tuple) = (&*result).downcast_ref::<Tuple>() {
                    ctx.return_tuple(tuple).unwrap();
                    0
                } else {
                    tarantool::set_error!(
                        TarantoolErrorCode::ProcC,
                        "{}",
                        "Unsupported result type"
                    )
                }
            }
            Err(e) => tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string()),
        }
    })
}

#[derive(Debug)]
struct DispatchedQuery {
    pattern: String,
    params: Vec<Value>,
    is_data_modifier: bool,
}

impl TryFrom<FunctionArgs> for DispatchedQuery {
    type Error = QueryPlannerError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        Tuple::from(value).decode::<DispatchedQuery>().map_err(|e| {
            QueryPlannerError::CustomError(format!("Parsing error (dispatched query): {:?}", e))
        })
    }
}

impl<'de> Deserialize<'de> for DispatchedQuery {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "DispatchedQuery")]
        struct StructHelper(String, Vec<Value>, bool);

        let struct_helper = StructHelper::deserialize(deserializer)?;

        Ok(DispatchedQuery {
            pattern: struct_helper.0,
            params: struct_helper.1,
            is_data_modifier: struct_helper.2,
        })
    }
}

#[no_mangle]
pub extern "C" fn execute_query(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let lua_params = match DispatchedQuery::try_from(args) {
        Ok(param) => param,
        Err(e) => return tarantool::set_error!(TarantoolErrorCode::ProcC, "{:?}", e),
    };

    let ret_code = load_config(&SEGMENT_ENGINE);
    if ret_code != 0 {
        return ret_code;
    }
    SEGMENT_ENGINE.with(|engine| {
        let runtime = match engine.try_borrow() {
            Ok(runtime) => runtime,
            Err(e) => {
                return tarantool::set_error!(
                    TarantoolErrorCode::ProcC,
                    "Failed to borrow the runtime while executing the query: {}",
                    e.to_string()
                );
            }
        };
        match runtime.execute(
            lua_params.pattern.as_str(),
            &lua_params.params,
            lua_params.is_data_modifier,
        ) {
            Ok(result) => {
                if let Some(tuple) = (&*result).downcast_ref::<Tuple>() {
                    ctx.return_tuple(tuple).unwrap();
                    0
                } else {
                    tarantool::set_error!(
                        TarantoolErrorCode::ProcC,
                        "{}",
                        "Unsupported result type"
                    )
                }
            }
            Err(e) => tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", format!("{:?}", e)),
        }
    })
}
