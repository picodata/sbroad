//! Frontend module.
//!
//! A list of different frontend implementations
//! to build the intermediate representation (IR).

use crate::errors::QueryPlannerError;
use crate::executor::engine::CoordinatorMetadata;
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

    /// Build a plan from the AST with parameters as placeholders for the values.
    ///
    /// # Errors
    /// - Failed to resolve AST nodes with cluster metadata.
    fn resolve_metadata<M>(&self, metadata: &M) -> Result<Plan, QueryPlannerError>
    where
        M: CoordinatorMetadata;
}

pub mod sql;
