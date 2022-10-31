//! IR tree traversal module.

use super::{Nodes, Plan};
use std::cell::RefCell;

trait TreeIterator<'nodes> {
    fn get_current(&self) -> &'nodes usize;
    fn get_child(&self) -> &RefCell<usize>;
    fn get_nodes(&self) -> &'nodes Nodes;
}

trait PlanTreeIterator<'plan>: TreeIterator<'plan> {
    fn get_plan(&self) -> &'plan Plan;
}

/// A snapshot describes the version of the plan
/// subtree to iterate over.
#[derive(Debug, Clone)]
pub enum Snapshot {
    Latest,
    Oldest,
}

pub mod and;
pub mod eq_class;
pub mod expression;
pub mod relation;
pub mod subtree;

#[cfg(test)]
mod tests;
