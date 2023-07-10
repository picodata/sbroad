use sbroad::executor::engine::helpers::decode_msgpack;
use std::os::raw::c_int;
use tarantool::tuple::{FunctionArgs, FunctionCtx, Tuple, TupleBuffer};

use crate::api::helper::load_config;
use crate::api::{COORDINATOR_ENGINE, SEGMENT_ENGINE};
use sbroad::backend::sql::ir::PatternWithParams;
use sbroad::executor::protocol::{EncodedRequiredData, RequiredData};
use sbroad::executor::Query;
use sbroad::log::tarantool_error;
use sbroad::otm::{child_span, query_span};
use sbroad::{debug, error};

/// Dispatch parameterized SQL query from coordinator to the segments.
#[no_mangle]
pub extern "C" fn dispatch_query(f_ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let mut lua_params = match PatternWithParams::try_from(args) {
        Ok(params) => params,
        Err(e) => {
            error!(Option::from("dispatch_query"), &format!("Error: {e}"));
            return tarantool_error(&e.to_string());
        }
    };

    // We initialize the global tracer on every configuration update.
    // As a side effect, we can't trace load_config() call itself (sic!).
    let ret_code = load_config(&COORDINATOR_ENGINE);

    let id = lua_params.clone_id();
    let ctx = lua_params.extract_context();
    let tracer = lua_params.get_tracer();

    query_span(
        "\"api.router\"",
        &id,
        &tracer,
        &ctx,
        &lua_params.pattern,
        || {
            if ret_code != 0 {
                return ret_code;
            }
            COORDINATOR_ENGINE.with(|engine| {
                let runtime = match engine.try_borrow() {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        return tarantool_error(&format!(
                            "Failed to borrow the runtime while dispatching the query: {e}"
                        ));
                    }
                };
                let mut query = match Query::new(&*runtime, &lua_params.pattern, lua_params.params)
                {
                    Ok(q) => q,
                    Err(e) => {
                        error!(Option::from("query dispatch"), &format!("{e:?}"));
                        return tarantool_error(&e.to_string());
                    }
                };
                if let Ok(true) = query.is_ddl() {
                    return tarantool_error("DDL queries are not supported");
                }

                match query.dispatch() {
                    Ok(result) => child_span("\"tarantool.tuple.return\"", || {
                        if let Some(tuple) = (*result).downcast_ref::<Tuple>() {
                            debug!(
                                Option::from("query dispatch"),
                                &format!("Returning tuple: {tuple:?}")
                            );
                            f_ctx.return_tuple(tuple).unwrap();
                            0
                        } else {
                            error!(
                                Option::from("query dispatch"),
                                &format!("Failed to downcast result: {result:?}")
                            );
                            tarantool_error("Unsupported result type")
                        }
                    }),
                    Err(e) => tarantool_error(&e.to_string()),
                }
            })
        },
    )
}

#[no_mangle]
pub extern "C" fn execute(f_ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    debug!(Option::from("decode_msgpack"), &format!("args: {args:?}"));
    let tuple_buf: Vec<u8> = TupleBuffer::from(Tuple::from(args)).into();
    let (raw_required, mut raw_optional) = match decode_msgpack(tuple_buf.as_slice()) {
        Ok(raw_data) => raw_data,
        Err(e) => {
            let err = format!("Failed to decode dispatched data: {e:?}");
            error!(Option::from("execute"), &err);
            return tarantool_error(&err);
        }
    };

    let ret_code = load_config(&SEGMENT_ENGINE);
    if ret_code != 0 {
        return ret_code;
    }

    let mut required = match RequiredData::try_from(EncodedRequiredData::from(raw_required)) {
        Ok(data) => data,
        Err(e) => {
            let err = format!("Failed to decode required data: {e:?}");
            error!(Option::from("execute"), &err);
            return tarantool_error(&err);
        }
    };

    let id: String = required.id().into();
    let ctx = required.extract_context();
    let tracer = required.tracer();

    query_span("\"api.storage\"", &id, &tracer, &ctx, "", || {
        SEGMENT_ENGINE.with(|engine| {
            let runtime = match engine.try_borrow() {
                Ok(runtime) => runtime,
                Err(e) => {
                    return tarantool_error(&format!(
                        "Failed to borrow the runtime while executing the query: {e}"
                    ));
                }
            };
            match runtime.execute_plan(&mut required, &mut raw_optional) {
                Ok(result) => {
                    if let Some(tuple) = (*result).downcast_ref::<Tuple>() {
                        f_ctx.return_tuple(tuple).unwrap();
                        0
                    } else if let Some(mp) = (*result).downcast_ref::<Vec<u8>>() {
                        f_ctx.return_mp(mp.as_slice()).unwrap();
                        0
                    } else {
                        error!(
                            Option::from("execute"),
                            &format!("Failed to downcast result: {result:?}")
                        );
                        tarantool_error("Unsupported result type")
                    }
                }
                Err(e) => {
                    let error = format!("Failed to execute the query: {e}");
                    error!(Option::from("execute"), &error);
                    tarantool_error(&error)
                }
            }
        })
    })
}
