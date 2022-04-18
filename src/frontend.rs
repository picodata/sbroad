//! Frontend module.
//!
//! A list of different frontend implementations
//! to build the intermediate representation (IR).

use crate::errors::QueryPlannerError;
use crate::executor::engine::Metadata;
use crate::ir::Plan;

pub trait Ast {
    fn empty() -> Self
    where
        Self: Sized;

    /// Builds abstract syntax tree (AST) from SQL query.
    ///
    /// # Errors
    /// - SQL query is not valid or not supported.
    fn new(query: &str) -> Result<Self, QueryPlannerError>
    where
        Self: Sized;

    /// AST is empty.
    fn is_empty(&self) -> bool;

    /// Builds the intermediate representation (IR) from the AST.
    ///
    /// # Errors
    /// - The AST doesn't represent a valid SQL query.
    /// - AST contains objects not present in the metadata.
    fn to_ir<M>(&self, metadata: &M) -> Result<Plan, QueryPlannerError>
    where
        M: Metadata;
}

pub mod sql;
