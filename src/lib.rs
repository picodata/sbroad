//! Tarantool planner for distributed SQL.
#[macro_use]
extern crate pest_derive;

mod errors;
mod executor;
mod frontend;
pub mod ir;
mod parser;
