use std::os::raw::c_int;

use serde::{Deserialize, Deserializer, Serialize};
use tarantool::error::TarantoolErrorCode;
use tarantool::log::{say, SayLevel};
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

use crate::api::helper::load_metadata;
use crate::api::QUERY_ENGINE;
use crate::errors::QueryPlannerError;
use crate::executor::result::{ConsumerResult, ProducerResult};
use crate::executor::Query;
use crate::ir::value::Value;

#[derive(Serialize)]
/// Lua function params
struct Args {
    /// Target sql query
    query: String,
    /// Query parameters
    params: Vec<Value>,
}

impl TryFrom<FunctionArgs> for Args {
    type Error = QueryPlannerError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        Tuple::from(value)
            .into_struct::<Args>()
            .map_err(|e| QueryPlannerError::CustomError(format!("Parsing args error: {:?}", e)))
    }
}

/// Custom deserializer of the input function arguments
impl<'de> Deserialize<'de> for Args {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "Args")]
        struct StructHelper(String, Vec<Value>);

        let struct_helper = StructHelper::deserialize(deserializer)?;

        Ok(Args {
            query: struct_helper.0,
            params: struct_helper.1,
        })
    }
}

/// Execute parameterized SQL query.
#[no_mangle]
pub extern "C" fn execute_query(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
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
        let mut query = match Query::new(engine, &lua_params.query, &lua_params.params) {
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

        match query.exec() {
            Ok(result) => {
                if let Some(producer_result) = (&*result).downcast_ref::<ProducerResult>() {
                    ctx.return_mp(&producer_result).unwrap();
                    0
                } else if let Some(consumer_result) = (&*result).downcast_ref::<ConsumerResult>() {
                    ctx.return_mp(&consumer_result).unwrap();
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
