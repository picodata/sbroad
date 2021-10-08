use serde::{Deserialize, Serialize};
use std;
use std::os::raw::c_int;
use tarantool::tuple::{AsTuple, FunctionArgs, FunctionCtx, Tuple};
use tarantool::error::{TarantoolErrorCode};
use std::fmt;
use crate::query::UserQuery;
use crate::schema::ClusterSchema;
use sqlparser::ast::{Select};
use crate::errors::QueryPlannerError;

#[derive(Serialize, Deserialize)]
struct Args {
    pub query: String,
    pub schema: String,
    pub bucket_count: u64,
}

#[derive(Debug, Serialize)]
struct TarantoolResponse(Vec<QueryResult>, String);

impl AsTuple for Args {}

#[no_mangle]
pub extern "C" fn parse_sql(ctx: FunctionCtx, args: FunctionArgs) -> c_int {
    let args: Tuple = args.into();
    let args = args.into_struct::<Args>().unwrap();

    let schema = ClusterSchema::from(args.schema.to_string());
    let q = UserQuery::new(args.query.as_str(), schema, args.bucket_count).unwrap();
    let result = match q.transform() {
        Ok(p) => {
            ctx.return_mp(&p).unwrap();
            0
        }
        Err(e) => tarantool::set_error!(TarantoolErrorCode::ProcC, "{}", e.to_string())
    };

    result
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
        write!(f, "Bucket: {}. Query: {}\r\n", self.bucket_id, self.node_query)
    }
}

pub trait QueryPlaner {
    fn parse(&self) -> Result<Vec<Box<Select>>, QueryPlannerError>;
}
