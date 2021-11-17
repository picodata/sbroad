use crate::bucket::str_to_bucket_id;
use crate::cluster_lua::{execute_sql, get_cluster_schema, init_cluster_functions};
use crate::errors::QueryPlannerError;
use crate::query::ParsedTree;
use crate::schema::Cluster;
use serde::{Deserialize, Serialize};
use sqlparser::ast::Select;
use std::cell::RefCell;
use std::fmt;
use std::os::raw::c_int;
use tarantool::error::TarantoolErrorCode;
use tarantool::ffi::lua::{luaT_state, lua_State};
use tarantool::tuple::{AsTuple, FunctionArgs, FunctionCtx, Tuple};

thread_local!(static CARTRIDGE_SCHEMA: RefCell<Cluster> = RefCell::new(Cluster::new()));
thread_local!(static LUA_STATE: RefCell<*mut lua_State> = RefCell::new( unsafe { luaT_state() }));

#[derive(Serialize, Deserialize)]
struct Args {
    pub query: String,
    pub bucket_count: u64,
}

#[derive(Debug, Serialize)]
struct TarantoolResponse(Vec<QueryResult>, String);

impl AsTuple for Args {}

#[no_mangle]
pub extern "C" fn parse_sql(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<Args>().unwrap();

    let l = LUA_STATE.try_with(|s| s.clone().into_inner()).unwrap();

    CARTRIDGE_SCHEMA.with(|s| {
        let mut schema = s.clone().into_inner();
        // Update cartridge schema after cache invalidation by calling `apply_config()` in lua code.
        if schema.is_empty() {
            let text_schema = get_cluster_schema(l);
            schema = Cluster::from(text_schema);

            *s.borrow_mut() = schema.clone();
        }

        let q = ParsedTree::new(args.query.as_str(), schema, args.bucket_count).unwrap();
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
        *s.borrow_mut() = Cluster::new();
    });

    ctx.return_mp(&true).unwrap();
    0
}

#[derive(Serialize, Deserialize)]
struct BucketCalcArgs {
    pub val: String,
    pub bucket_count: u64,
}

impl AsTuple for BucketCalcArgs {}

#[no_mangle]
pub extern "C" fn calculate_bucket_id(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<BucketCalcArgs>().unwrap();

    let result = str_to_bucket_id(&args.val, args.bucket_count);
    ctx.return_mp(&result).unwrap();
    0
}

#[derive(Debug, Serialize, Deserialize)]
struct ExecQueryArgs {
    pub bucket_id: isize,
    pub query: String,
}

impl AsTuple for ExecQueryArgs {}

#[no_mangle]
pub extern "C" fn execute_query(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<ExecQueryArgs>().unwrap();

    let l = LUA_STATE.try_with(|s| s.clone().into_inner()).unwrap();
    let result = execute_sql(l, args.bucket_id, &args.query);
    ctx.return_mp(&result).unwrap();
    0
}

#[derive(Debug, Serialize, Deserialize)]
struct InitArgs {
    pub login: String,
    pub password: String,
}

impl AsTuple for InitArgs {}

/**
Function must be called in router and storage roles of cartridge application.
*/
#[no_mangle]
pub extern "C" fn init(_ctx: FunctionCtx, _: FunctionArgs) -> c_int {
    LUA_STATE
        .try_with(|s| {
            let l = s.clone().into_inner();
            init_cluster_functions(l);
        })
        .unwrap();
    0
}

#[derive(Serialize, Debug, Eq, PartialEq)]
pub struct QueryResult {
    pub bucket_id: u64,
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
