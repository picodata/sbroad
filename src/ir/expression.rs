//! Expressions are the building blocks of the tuple.
//!
//! They provide us information about:
//! - what input tuple's columns where used to build our tuple
//! - the order of the columns (and we can get their types as well)
//! - distribution of the data in the tuple

use super::distribution::Distribution;
use super::operator;
use super::value::Value;
use super::{vec_alloc, Node, Plan};
use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};

/// Tuple tree build blocks.
///
/// Tuple describes a single portion of data moved among the cluster.
/// It consists of the ordered, strictly typed expressions with names
/// (columns) and additional information about data distribution policy.
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
        /// Child expression node index in the plan node arena.
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
    /// Uses a relative pointer as a coordinate system:
    /// - relational node (containing this reference)
    /// - target(s) in the relational nodes list of children
    /// - column position in the child(ren) output tuple
    Reference {
        /// Targets in the relational node children list.
        /// - Leaf nodes (relation scans): None.
        /// - Union nodes: two elements (left and right).
        /// - Other: single element.
        targets: Option<Vec<usize>>,
        /// Expression position in the input tuple.
        position: usize,
        /// Relational node ID, that contains current reference
        parent: usize,
    },
    /// Top of the tuple tree with a list of aliases.
    ///
    ///  Example: (a, b, 1).
    Row {
        /// A list of the alias expression node indexes in the plan node arena.
        list: Vec<usize>,
        /// Resulting data distribution of the tuple. Should be filled as a part
        /// of the last "add Motion" transformation.
        distribution: Option<Distribution>,
    },
}

#[allow(dead_code)]
impl Expression {
    /// Get current row distribution.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the function is called on expression
    /// other than `Row` or a node doesn't know its distribution yet.
    pub fn distribution(&self) -> Result<&Distribution, QueryPlannerError> {
        if let Expression::Row { distribution, .. } = self {
            let dist = match distribution {
                Some(d) => d,
                None => return Err(QueryPlannerError::UninitializedDistribution),
            };
            return Ok(dist);
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
    pub fn new_ref(parent: usize, targets: Option<Vec<usize>>, position: usize) -> Self {
        Expression::Reference {
            parent,
            targets,
            position,
        }
    }

    // TODO: check that doesn't contain top-level aliases with the same names
    /// Row expression constructor.
    #[must_use]
    pub fn new_row(list: Vec<usize>, distribution: Option<Distribution>) -> Self {
        Expression::Row { list, distribution }
    }

    /// Boolean expression constructor.
    #[must_use]
    pub fn new_bool(left: usize, op: operator::Bool, right: usize) -> Self {
        Expression::Bool { left, op, right }
    }
}

impl Plan {
    /// Create a new output tuple from the children nodes output, containing
    /// a specified list of column names. If the column list is empty then
    /// just copy all the columns to a new tuple.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - relation node contains invalid `Row` in the output
    /// - targets and children are inconsistent
    /// - column names don't exits
    pub fn add_output_row(
        &mut self,
        rel_node_id: usize,
        children: &[usize],
        targets: &[usize],
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        // We can pass two target children nodes only in a case
        // of `UnionAll`. Even for a `NaturalJoin` we work with
        // each child independently. In fact, we need only the
        // first child in a `UnionAll` operator to get correct
        // column names for a new tuple (second child aliases
        // would be shadowed). But each reference should point
        // to both children to give us additional information
        // during transformations.
        if (targets.len() > 2) || targets.is_empty() {
            return Err(QueryPlannerError::InvalidRow);
        }

        if let Some(max) = targets.iter().max() {
            if *max >= children.len() {
                return Err(QueryPlannerError::InvalidRow);
            }
        }

        let target_child: usize = if let Some(target) = targets.get(0) {
            *target
        } else {
            return Err(QueryPlannerError::InvalidRow);
        };
        let child_node: usize = if let Some(child) = children.get(target_child) {
            *child
        } else {
            return Err(QueryPlannerError::InvalidRow);
        };

        if col_names.is_empty() {
            let child_row_list: Vec<usize> =
                if let Node::Relational(relational_op) = self.get_node(child_node)? {
                    if let Node::Expression(Expression::Row { list, .. }) =
                        self.get_node(relational_op.output())?
                    {
                        list.clone()
                    } else {
                        return Err(QueryPlannerError::InvalidRow);
                    }
                } else {
                    return Err(QueryPlannerError::InvalidNode);
                };

            let mut aliases: Vec<usize> = Vec::new();

            for (pos, alias_node) in child_row_list.iter().enumerate() {
                let name: String = if let Node::Expression(Expression::Alias { ref name, .. }) =
                    self.get_node(*alias_node)?
                {
                    String::from(name)
                } else {
                    return Err(QueryPlannerError::InvalidRow);
                };
                let new_targets: Vec<usize> = targets.iter().copied().collect();
                // Create new references and aliases. Save them to the plan nodes arena.
                let r_id = vec_alloc(
                    &mut self.nodes,
                    Node::Expression(Expression::new_ref(rel_node_id, Some(new_targets), pos)),
                );
                let a_id = vec_alloc(
                    &mut self.nodes,
                    Node::Expression(Expression::new_alias(&name, r_id)),
                );
                aliases.push(a_id);
            }

            let row_node = vec_alloc(
                &mut self.nodes,
                Node::Expression(Expression::new_row(aliases, None)),
            );
            return Ok(row_node);
        }

        let map = if let Node::Relational(relational_op) = self.get_node(child_node)? {
            relational_op.output_alias_position_map(self)?
        } else {
            return Err(QueryPlannerError::InvalidNode);
        };

        let mut aliases: Vec<usize> = Vec::new();

        let all_found = col_names.iter().all(|col| {
            map.get(*col).map_or(false, |pos| {
                let new_targets: Vec<usize> = targets.iter().copied().collect();
                // Create new references and aliases. Save them to the plan nodes arena.
                let r_id = vec_alloc(
                    &mut self.nodes,
                    Node::Expression(Expression::new_ref(rel_node_id, Some(new_targets), *pos)),
                );
                let a_id = vec_alloc(
                    &mut self.nodes,
                    Node::Expression(Expression::new_alias(col, r_id)),
                );
                aliases.push(a_id);
                true
            })
        });

        if all_found {
            let row_node = vec_alloc(
                &mut self.nodes,
                Node::Expression(Expression::new_row(aliases, None)),
            );
            return Ok(row_node);
        }
        Err(QueryPlannerError::InvalidRow)
    }
}
