//! Expressions are the building blocks of the tuple.
//!
//! They provide us information about:
//! - what input tuple's columns where used to build our tuple
//! - the order of the columns (and we can get their types as well)
//! - distribution of the data in the tuple

use super::operator;
use super::value::Value;
use super::{Node, Plan};
use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tuple data chunk distribution policy in the cluster.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Distribution {
    /// Each segment contains a random portion of data.
    ///
    /// Example: "erased" distribution key by projection operator.
    Random,
    /// Each segment contains a full copy of data.
    ///
    /// Example: constant expressions.
    Replicated,
    /// Each segment contains a portion of data,
    /// determined by the distribution key rule.
    ///
    /// Example: segmented table.
    Segment {
        /// A list of column positions in the output tuple,
        /// that for a distribution key.
        key: Vec<usize>,
    },
    /// Only a single segment contains all the data.
    ///
    /// Example: insertion to the virtual table on coordinator.
    Single,
}

/// Tree branch.
///
/// Reference expressions point to the position in the incoming
/// tuple. But union and join nodes have multiple incoming tuples
/// and we need a way to detect which branch the tuple came from.
/// So, branches act like additional dimension (among with position)
/// to refer incoming data.
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum Branch {
    /// Output reference points both to the left and right branches.
    ///
    /// Example: union operator.
    Both,
    /// Left branch is also the default value for a single-child operator.
    ///
    /// Example: join, selection operator.
    Left,
    /// Right branch.
    ///
    /// Example: join operator.
    Right,
}

/// Tuple tree build blocks.
///
/// Tuple describes a single portion of data moved among the cluster.
/// It consists of the ordered, strictly typed expressions with names
/// (columns) with additional information about data distribution policy.
///
/// Tuple is a tree with a `Row` top (level 0) and a list of the named
/// `Alias` columns (level 1). This convention is used among the code
/// and should not be changed. Thanks to this fact we always know the
/// name of any column in the tuple that should simplify AST
/// deserialization.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Expression {
    /// Expression name.
    ///
    /// Example: `42 as a`.
    Alias {
        /// Alias name.
        name: String,
        /// Child expression node index in the plan node arena (left branch).
        child: usize,
    },
    /// Binary expression returning boolean result.
    ///
    /// Example: `a > 42`, `b in (select c from ...)`.
    Bool {
        /// Left branch expression node index in the plan node arena.
        left: usize,
        /// Boolean operator.
        op: operator::Bool,
        /// Right branch expression node index in the plan node arena.
        right: usize,
    },
    /// Constant expressions.
    ///
    // Example: `42`.
    Constant {
        /// Contained value (boolean, number, string or null)
        value: Value,
    },
    /// Reference to the position in the incoming tuple(s).
    /// Uses child branch and incoming column position as
    /// a coordinate system.
    ///
    // Example: &0 (left)
    Reference {
        /// Branch of the input tuple.
        branch: Branch,
        /// Expression position in the input tuple.
        position: usize,
    },
    /// Top of the tuple tree with a list of aliases.
    ///
    ///  Example: (a, b, 1).
    Row {
        /// A list of the alias expression node indexes in the plan node arena.
        list: Vec<usize>,
        /// Resulting data distribution of the tuple.
        distribution: Distribution,
    },
}

#[allow(dead_code)]
impl Expression {
    /// Suggest possible distribution to the parent tuple.
    ///
    /// When a parent tuple deduces its distribution, it builds a reference map for each
    /// branch and asks the corresponding child for a suggestion about distribution.
    /// This function is executed on the child's side.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when aliases are invalid.
    pub fn suggested_distribution(
        &self,
        my_branch: &Branch,
        aliases: &[usize],
        plan: &Plan,
    ) -> Result<Distribution, QueryPlannerError> {
        if let Expression::Row {
            ref distribution, ..
        } = self
        {
            match *distribution {
                Distribution::Random => return Ok(Distribution::Random),
                Distribution::Replicated => return Ok(Distribution::Replicated),
                Distribution::Segment { ref key } => {
                    // When expression is a Row, it has a three level structure:
                    //
                    // level 0: row itself
                    // level 1: list of aliases
                    // level 2: arbitrary expressions (references as well)
                    // ...
                    // Now we traverse the level 2 for the reference expressions, as only
                    // they can contain positions of the distribution key from the input row, then
                    // build a map <input row list position, self row list position of the reference>.
                    let mut map: HashMap<usize, usize> = HashMap::new();

                    for (self_pos, alias_id) in aliases.iter().enumerate() {
                        if let Node::Expression(Expression::Alias { child, .. }) =
                            plan.get_node(*alias_id)?
                        {
                            if let Node::Expression(Expression::Reference {
                                branch: ref_branch,
                                position: ref_pos,
                            }) = plan.get_node(*child)?
                            {
                                if ((*ref_branch == *my_branch)
                                    || (*my_branch == Branch::Both)
                                    || (*ref_branch == Branch::Both))
                                    && map.insert(*ref_pos, self_pos).is_some()
                                {
                                    return Err(QueryPlannerError::InvalidPlan);
                                }
                            }
                        } else {
                            return Err(QueryPlannerError::InvalidRow);
                        }
                    }

                    let mut new_key: Vec<usize> = Vec::new();
                    let all_found = key.iter().all(|pos| {
                        if let Some(new_pos) = map.get(pos) {
                            new_key.push(*new_pos);
                            return true;
                        }
                        false
                    });
                    if all_found {
                        return Ok(Distribution::Segment { key: new_key });
                    }
                    return Ok(Distribution::Random);
                }
                Distribution::Single => return Ok(Distribution::Single),
            }
        }
        Err(QueryPlannerError::InvalidRow)
    }

    /// Get current row distribution.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the function is called on
    /// expression other than `Row`.
    pub fn distribution(&self) -> Result<&Distribution, QueryPlannerError> {
        if let Expression::Row { distribution, .. } = self {
            return Ok(distribution);
        }
        Err(QueryPlannerError::InvalidRow)
    }

    /// Alias expression constructor.
    #[must_use]
    pub fn new_alias(name: &str, child: usize) -> Self {
        Expression::Alias {
            name: String::from(name),
            child,
        }
    }

    /// Constant expression constructor.
    #[must_use]
    pub fn new_const(value: Value) -> Self {
        Expression::Constant { value }
    }

    /// Reference expression constructor.
    #[must_use]
    pub fn new_ref(branch: Branch, position: usize) -> Self {
        Expression::Reference { branch, position }
    }

    // TODO: check that doesn't contain top-level aliases with the same names
    /// Row expression constructor.
    #[must_use]
    pub fn new_row(list: Vec<usize>, distribution: Distribution) -> Self {
        Expression::Row { list, distribution }
    }

    /// Boolean expression constructor.
    #[must_use]
    pub fn new_bool(left: usize, op: operator::Bool, right: usize) -> Self {
        Expression::Bool { left, op, right }
    }
}

#[cfg(test)]
mod tests;
