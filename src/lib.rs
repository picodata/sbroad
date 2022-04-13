//! Tarantool planner and executor for a distributed SQL.
#[macro_use]
extern crate pest_derive;

pub mod errors;
pub mod executor;
pub mod frontend;
pub mod ir;
mod parser;
