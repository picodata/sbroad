mod extargs;

use std::cell::RefCell;
use std::convert::TryInto;
use std::os::raw::c_int;

use serde::{Deserialize, Serialize};
use tarantool::error::TarantoolErrorCode;
use tarantool::log::{say, SayLevel};
use tarantool::tuple::{AsTuple, FunctionArgs, FunctionCtx, Tuple};

use crate::errors::QueryPlannerError;
use crate::executor::engine::{cartridge, Engine};
use crate::executor::Query;

use self::extargs::{BucketCalcArgs, BucketCalcArgsDict};

thread_local!(static QUERY_ENGINE: RefCell<cartridge::Runtime> = RefCell::new(cartridge::Runtime::new().unwrap()));

#[derive(Serialize, Deserialize)]
struct Args {
    pub query: String,
}

/**
Function invalidates schema cache, then it updates schema before next query.
It must be called in function `apply_config()` in lua cartridge application.
 */
#[no_mangle]
pub extern "C" fn invalidate_caching_schema(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    QUERY_ENGINE.with(|s| {
        let v = &mut *s.borrow_mut();
        v.clear_metadata();
    });

    ctx.return_mp(&true).unwrap();
    0
}

#[no_mangle]
pub extern "C" fn calculate_bucket_id(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<BucketCalcArgs>().unwrap();

    QUERY_ENGINE.with(|e| {
        let result = e.clone().into_inner().determine_bucket_id(&args.rec);
        ctx.return_mp(&result).unwrap();
        0
    })
}

#[no_mangle]
pub extern "C" fn calculate_bucket_id_by_dict(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    QUERY_ENGINE.with(|e| {
        let mut engine = e.clone().into_inner();
        // Update cartridge schema after cache invalidation by calling `apply_config()` in lua code.
        if engine.has_metadata() {
            match engine.load_metadata() {
                Ok(_) => *e.borrow_mut() = engine.clone(),
                Err(e) => {
                    return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string());
                }
            };
        }

        // Closure for more concise error propagation from calls nested in the bucket calculation
        let propagate_err = || -> Result<u64, QueryPlannerError> {
            // Deserialization error
            let bca = BucketCalcArgsDict::try_from(args)?;
            // Error in filtering bucket calculation arguments by sharding keys
            let fk = engine
                .extract_sharding_keys(bca.space, bca.rec)?
                .into_iter()
                .fold(String::new(), |mut acc, v| {
                    let s: String = v.into();
                    acc.push_str(s.as_str());
                    acc
                });
            Ok(e.clone().into_inner().determine_bucket_id(fk.as_str()))
        };

        match propagate_err() {
            Ok(bucket_id) => {
                ctx.return_mp(&bucket_id).unwrap();
                0
            }
            Err(e) => tarantool::set_error!(TarantoolErrorCode::ProcC, "{:?}", e),
        }
    })
}

#[derive(Debug, Serialize, Deserialize)]
struct ExecQueryArgs {
    pub query: String,
}

impl AsTuple for ExecQueryArgs {}

#[no_mangle]
pub extern "C" fn execute_query(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<Args>().unwrap();

    QUERY_ENGINE.with(|s| {
        let mut engine = s.clone().into_inner();
        // Update cartridge schema after cache invalidation by calling `apply_config()` in lua code.
        if engine.has_metadata() {
            match engine.load_metadata() {
                Ok(_) => *s.borrow_mut() = engine.clone(),
                Err(e) => {
                    return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string());
                }
            };
        }

        let mut query = match Query::new(engine, args.query.as_str()) {
            Ok(q) => q,
            Err(e) => {
                say(
                    SayLevel::Error,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    None,
                    &format!("{:?}", e),
                );
                // Temporary error for parsing ast error because someone query types aren't implemented
                return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", format!("{:?}", e));
            }
        };

        match query.exec() {
            Ok(q) => {
                ctx.return_mp(&q).unwrap();
                0
            }
            Err(e) => tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string()),
        }
    })
}
