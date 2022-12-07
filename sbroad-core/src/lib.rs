//! Tarantool planner and executor for a distributed SQL.
#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate pest_derive;
extern crate core;

pub mod backend;
pub mod errors;
pub mod executor;
pub mod frontend;
pub mod ir;
pub mod log;
pub mod otm;
