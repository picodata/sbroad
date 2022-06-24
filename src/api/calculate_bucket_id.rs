use std::os::raw::c_int;

use serde::{Deserialize, Serialize};
use tarantool::error::TarantoolErrorCode;
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

use crate::api::COORDINATOR_ENGINE;
use crate::errors::QueryPlannerError;
use crate::executor::engine::Coordinator;
use crate::ir::value::Value;

#[derive(Serialize, Deserialize)]
/// Lua function params
struct Args {
    /// The input string for calculating bucket
    rec: String,
}

impl TryFrom<FunctionArgs> for Args {
    type Error = QueryPlannerError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        Tuple::from(value)
            .into_struct::<Args>()
            .map_err(|e| QueryPlannerError::CustomError(format!("Parsing args error: {:?}", e)))
    }
}

/// Calculate the target bucket by a string of the joined column values.
#[no_mangle]
pub extern "C" fn calculate_bucket_id(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let bucket_str = match Args::try_from(args) {
        Ok(param) => Value::from(param.rec),
        Err(e) => return tarantool::set_error!(TarantoolErrorCode::ProcC, "{:?}", e),
    };

    COORDINATOR_ENGINE.with(|e| {
        let engine = &mut *e.borrow_mut();

        let result = engine.determine_bucket_id(&[&bucket_str]);

        ctx.return_mp(&result).unwrap();
        0
    })
}
