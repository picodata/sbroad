use crate::errors::{Entity, SbroadError};
use crate::ir::aggregates::AggregateKind;
use crate::ir::expression::Expression;
use crate::ir::relation::Type;
use crate::ir::{Node, Plan};
use serde::{Deserialize, Serialize};

use super::expression::TrimKind;

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
    pub func_type: Type,
}

impl Function {
    #[must_use]
    pub fn new(name: String, behavior: Behavior, func_type: Type) -> Self {
        Self {
            name,
            behavior,
            func_type,
        }
    }

    #[must_use]
    pub fn new_stable(name: String, func_type: Type) -> Self {
        Self::new(name, Behavior::Stable, func_type)
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
        trim_kind: Option<TrimKind>,
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
            is_distinct: false,
            func_type: function.func_type.clone(),
            trim_kind,
        };
        let func_id = self.nodes.push(Node::Expression(func_expr));
        Ok(func_id)
    }

    /// Add aggregate function to plan
    ///
    /// # Errors
    /// - Invalid arguments for given aggregate function
    pub fn add_aggregate_function(
        &mut self,
        function: &str,
        kind: AggregateKind,
        children: Vec<usize>,
        is_distinct: bool,
    ) -> Result<usize, SbroadError> {
        match kind {
            AggregateKind::GRCONCAT => {
                if children.len() > 2 || children.is_empty() {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format!(
                            "GROUP_CONCAT aggregate function can have one or two arguments at most. Got: {} arguments", children.len()
                        )),
                    ));
                }
                if is_distinct && children.len() == 2 {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format!(
                            "distinct GROUP_CONCAT aggregate function has only one argument. Got: {} arguments", children.len()
                        )),
                    ));
                }
            }
            _ => {
                if children.len() != 1 {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format!("Expected one argument for aggregate: {function}.")),
                    ));
                }
            }
        }
        let func_expr = Expression::StableFunction {
            name: function.to_lowercase(),
            children,
            is_distinct,
            func_type: Type::from(kind),
            trim_kind: None,
        };
        let id = self.nodes.push(Node::Expression(func_expr));
        Ok(id)
    }
}
