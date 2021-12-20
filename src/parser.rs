use std::cell::RefCell;
use std::convert::TryInto;
use std::fmt;
use std::os::raw::c_int;

use serde::{Deserialize, Serialize};
use sqlparser::ast::Select;
use tarantool::error::TarantoolErrorCode;
use tarantool::log::{say, SayLevel};
use tarantool::tuple::{AsTuple, FunctionArgs, FunctionCtx, Tuple};

use crate::bucket::str_to_bucket_id;
use crate::cache::Metadata;
use crate::errors::QueryPlannerError;
use crate::lua_bridge::{bucket_count, exec_query, get_cluster_schema};
use crate::query::ParsedTree;

thread_local!(static CARTRIDGE_SCHEMA: RefCell<Metadata> = RefCell::new(Metadata::new()));

#[derive(Serialize, Deserialize)]
struct Args {
    pub query: String,
}

#[derive(Debug, Serialize)]
struct TarantoolResponse(Vec<QueryResult>, String);

impl AsTuple for Args {}

#[no_mangle]
pub extern "C" fn parse_sql(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<Args>().unwrap();

    CARTRIDGE_SCHEMA.with(|s| {
        let mut schema = s.clone().into_inner();
        // Update cartridge schema after cache invalidation by calling `apply_config()` in lua code.
        if schema.is_empty() {
            let text_schema = match get_cluster_schema() {
                Ok(s) => s,
                Err(e) => {
                    say(
                        SayLevel::Error,
                        "parser.rs",
                        40,
                        Option::from("get cluster schema error"),
                        &format!("{:?}", e),
                    );
                    return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string());
                }
            };
            match schema.load(&text_schema) {
                Ok(_) => *s.borrow_mut() = schema.clone(),
                Err(e) => {
                    return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string())
                }
            };
        }

        let bucket_count = match bucket_count() {
            Ok(c) => c,
            Err(e) => return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string()),
        };

        let q = ParsedTree::new(args.query.as_str(), schema, bucket_count).unwrap();
        let result = match q.transform() {
            Ok(p) => {
                ctx.return_mp(&p).unwrap();
                0
            }
            Err(e) => tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string()),
        };

        result
    })
}

/**
Function invalidates schema cache, then it updates schema before next query.
It must be called in function `apply_config()` in lua cartridge application.
 */
#[no_mangle]
pub extern "C" fn invalidate_caching_schema(ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    CARTRIDGE_SCHEMA.with(|s| {
        *s.borrow_mut() = Metadata::new();
    });

    ctx.return_mp(&true).unwrap();
    0
}

#[derive(Serialize, Deserialize)]
struct BucketCalcArgs {
    pub val: String,
}

impl AsTuple for BucketCalcArgs {}

#[no_mangle]
pub extern "C" fn calculate_bucket_id(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<BucketCalcArgs>().unwrap();
    let bucket_count = match bucket_count() {
        Ok(c) => c,
        Err(e) => return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string()),
    };
    let result = str_to_bucket_id(&args.val, bucket_count.try_into().unwrap());
    ctx.return_mp(&result).unwrap();
    0
}

#[derive(Debug, Serialize, Deserialize)]
struct ExecQueryArgs {
    pub bucket_id: u64,
    pub query: String,
}

impl AsTuple for ExecQueryArgs {}

#[no_mangle]
pub extern "C" fn execute_query(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<ExecQueryArgs>().unwrap();

    match exec_query(args.bucket_id, &args.query) {
        Ok(p) => {
            ctx.return_mp(&p).unwrap();
            0
        }
        Err(e) => return tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string()),
    }
}

#[derive(Serialize, Debug, Eq, PartialEq)]
pub struct QueryResult {
    pub bucket_id: usize,
    pub node_query: String,
}

impl QueryResult {
    pub fn new() -> QueryResult {
        QueryResult {
            bucket_id: 0,
            node_query: String::new(),
        }
    }
}

impl fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Bucket: {}. Query: {}\r\n",
            self.bucket_id, self.node_query
        )
    }
}

pub trait QueryPlaner {
    fn parse(&self) -> Result<Vec<Box<Select>>, QueryPlannerError>;
}
