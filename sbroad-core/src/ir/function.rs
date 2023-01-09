use crate::errors::{Entity, SbroadError};
use crate::ir::expression::Expression;
use crate::ir::{Node, Plan};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum Behavior {
    /// The function is a stable function, it does not have any side effects.
    /// It cannot modify the database, and that within a single table scan it
    /// will consistently return the same result for the same argument values,
    /// but that its result could change across SQL statements.
    /// This type of functions can be executed on any node.
    Stable,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Function {
    pub name: String,
    pub behavior: Behavior,
}

impl Function {
    #[must_use]
    pub fn new(name: String, behavior: Behavior) -> Self {
        Self { name, behavior }
    }

    #[must_use]
    pub fn new_stable(name: String) -> Self {
        Self::new(name, Behavior::Stable)
    }

    #[must_use]
    pub fn is_stable(&self) -> bool {
        matches!(self.behavior, Behavior::Stable)
    }
}

impl Plan {
    /// Adds a stable function to the plan.
    ///
    /// # Errors
    /// - Function is not stable.
    /// - Function is not found in the plan.
    pub fn add_stable_function(
        &mut self,
        function: &Function,
        children: Vec<usize>,
    ) -> Result<usize, SbroadError> {
        if !function.is_stable() {
            return Err(SbroadError::Invalid(
                Entity::SQLFunction,
                Some(format!("function {} is not stable", function.name)),
            ));
        }
        let func_expr = Expression::StableFunction {
            name: function.name.to_string(),
            children,
        };
        let func_id = self.nodes.push(Node::Expression(func_expr));
        Ok(func_id)
    }
}
