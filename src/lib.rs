//! Tarantool planner for distributed SQL.
#[macro_use]
extern crate pest_derive;

mod backend;
mod bucket;
mod cache;
mod errors;
mod executor;
mod frontend;
pub mod ir;
mod lua_bridge;
mod parser;
