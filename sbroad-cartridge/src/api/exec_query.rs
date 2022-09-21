use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use std::os::raw::c_int;
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple};

use crate::api::helper::load_config;
use crate::api::{COORDINATOR_ENGINE, SEGMENT_ENGINE};
use sbroad::backend::sql::ir::PatternWithParams;
use sbroad::errors::QueryPlannerError;
use sbroad::executor::Query;
use sbroad::ir::value::Value;
use sbroad::log::tarantool_error;
use sbroad::otm::{child_span, extract_params, query_span};
use sbroad::{debug, error};

/// Dispatch parameterized SQL query from coordinator to the segments.
#[no_mangle]
pub extern "C" fn dispatch_query(f_ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let lua_params = match PatternWithParams::try_from(args) {
        Ok(params) => params,
        Err(e) => return tarantool_error(&e.to_string()),
    };

    // We initialize the global tracer on every configuration update.
    // As a side effect, we can't trace load_config() call itself (sic!).
    let ret_code = load_config(&COORDINATOR_ENGINE);

    let (id, ctx, tracer) = extract_params(lua_params.context, lua_params.id);

    query_span("api.router", &id, tracer, &ctx, &lua_params.pattern, || {
        if ret_code != 0 {
            return ret_code;
        }
        COORDINATOR_ENGINE.with(|engine| {
            let runtime = match engine.try_borrow() {
                Ok(runtime) => runtime,
                Err(e) => {
                    return tarantool_error(&format!(
                        "Failed to borrow the runtime while dispatching the query: {}",
                        e
                    ));
                }
            };
            let mut query = match Query::new(&*runtime, &lua_params.pattern, lua_params.params) {
                Ok(q) => q,
                Err(e) => {
                    error!(Option::from("query dispatch"), &format!("{:?}", e));
                    return tarantool_error(&e.to_string());
                }
            };

            match query.dispatch() {
                Ok(result) => child_span("tarantool.tuple.return", || {
                    if let Some(tuple) = (&*result).downcast_ref::<Tuple>() {
                        f_ctx.return_tuple(tuple).unwrap();
                        0
                    } else {
                        tarantool_error("Unsupported result type")
                    }
                }),
                Err(e) => tarantool_error(&e.to_string()),
            }
        })
    })
}

#[derive(Debug)]
struct ExecuteQueryParams {
    pattern: String,
    params: Vec<Value>,
    context: Option<HashMap<String, String>>,
    id: Option<String>,
    is_data_modifier: bool,
}

impl TryFrom<FunctionArgs> for ExecuteQueryParams {
    type Error = QueryPlannerError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        debug!(
            Option::from("argument parsing"),
            &format!("Execute query parameters: {:?}", value),
        );
        Tuple::from(value)
            .decode::<ExecuteQueryParams>()
            .map_err(|e| {
                QueryPlannerError::CustomError(format!("Parsing error (dispatched query): {:?}", e))
            })
    }
}

impl<'de> Deserialize<'de> for ExecuteQueryParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "ExecuteQueryParams")]
        struct StructHelper(
            String,
            Vec<Value>,
            Option<HashMap<String, String>>,
            Option<String>,
            bool,
        );

        let struct_helper = StructHelper::deserialize(deserializer)?;

        Ok(ExecuteQueryParams {
            pattern: struct_helper.0,
            params: struct_helper.1,
            context: struct_helper.2,
            id: struct_helper.3,
            is_data_modifier: struct_helper.4,
        })
    }
}

#[no_mangle]
pub extern "C" fn execute_query(f_ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let lua_params = match ExecuteQueryParams::try_from(args) {
        Ok(param) => param,
        Err(e) => return tarantool_error(&e.to_string()),
    };

    let ret_code = load_config(&SEGMENT_ENGINE);
    if ret_code != 0 {
        return ret_code;
    }

    let (id, ctx, tracer) = extract_params(lua_params.context, lua_params.id);
    query_span(
        "api.storage",
        &id,
        tracer,
        &ctx,
        &lua_params.pattern,
        || {
            SEGMENT_ENGINE.with(|engine| {
                let runtime = match engine.try_borrow() {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        return tarantool_error(&format!(
                            "Failed to borrow the runtime while executing the query: {}",
                            e
                        ));
                    }
                };
                match runtime.execute(
                    lua_params.pattern.as_str(),
                    &lua_params.params,
                    lua_params.is_data_modifier,
                ) {
                    Ok(result) => {
                        if let Some(tuple) = (&*result).downcast_ref::<Tuple>() {
                            f_ctx.return_tuple(tuple).unwrap();
                            0
                        } else {
                            tarantool_error("Unsupported result type")
                        }
                    }
                    Err(e) => tarantool_error(&e.to_string()),
                }
            })
        },
    )
}
