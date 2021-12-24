//! Expressions are the building blocks of the tuple.
//!
//! They provide us information about:
//! - what input tuple's columns where used to build our tuple
//! - the order of the columns (and we can get their types as well)
//! - distribution of the data in the tuple

use super::distribution::Distribution;
use super::operator;
use super::value::Value;
use super::{Node, Nodes, Plan};
use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

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
    /// Top of the tuple tree.
    ///
    /// If current tuple is the output for some relational operator it should
    /// consist from the list of aliases. Otherwise (rows in selection filter
    /// or in join condition) we don't insist on the aliases in the list.
    ///
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
}

impl Nodes {
    /// Add alias node.
    ///
    /// # Errors
    /// - child node is invalid
    /// - name is empty
    pub fn add_alias(&mut self, name: &str, child: usize) -> Result<usize, QueryPlannerError> {
        self.arena
            .get(child)
            .ok_or(QueryPlannerError::InvalidNode)?;
        if name.is_empty() {
            return Err(QueryPlannerError::InvalidName);
        }
        let alias = Expression::Alias {
            name: String::from(name),
            child,
        };
        Ok(self.push(Node::Expression(alias)))
    }

    /// Add boolean node.
    ///
    /// # Errors
    /// - when left or right nodes are invalid
    pub fn add_bool(
        &mut self,
        left: usize,
        op: operator::Bool,
        right: usize,
    ) -> Result<usize, QueryPlannerError> {
        self.arena.get(left).ok_or(QueryPlannerError::InvalidNode)?;
        self.arena
            .get(right)
            .ok_or(QueryPlannerError::InvalidNode)?;
        Ok(self.push(Node::Expression(Expression::Bool { left, op, right })))
    }

    /// Add constant node.
    pub fn add_const(&mut self, value: Value) -> usize {
        self.push(Node::Expression(Expression::Constant { value }))
    }

    /// Add reference node.
    pub fn add_ref(
        &mut self,
        parent: usize,
        targets: Option<Vec<usize>>,
        position: usize,
    ) -> usize {
        let r = Expression::Reference {
            parent,
            targets,
            position,
        };
        self.push(Node::Expression(r))
    }

    /// Add row node.
    pub fn add_row(&mut self, list: Vec<usize>, distribution: Option<Distribution>) -> usize {
        self.push(Node::Expression(Expression::Row { list, distribution }))
    }

    /// Add row node, where every column has an alias.
    /// Mostly used for relational node output.
    ///
    /// # Errors
    /// - nodes in a list are invalid
    /// - nodes in a list are not aliases
    /// - aliases in a list have duplicate names
    pub fn add_row_of_aliases(
        &mut self,
        list: Vec<usize>,
        distribution: Option<Distribution>,
    ) -> Result<usize, QueryPlannerError> {
        let mut names: HashSet<String> = HashSet::new();

        for alias_node in &list {
            if let Node::Expression(Expression::Alias { name, .. }) = self
                .arena
                .get(*alias_node)
                .ok_or(QueryPlannerError::InvalidNode)?
            {
                if !names.insert(String::from(name)) {
                    return Err(QueryPlannerError::DuplicateColumn);
                }
            } else {
                return Err(QueryPlannerError::InvalidRow);
            }
        }
        Ok(self.add_row(list, distribution))
    }
}

impl Plan {
    /// Returns a list of columns from the child node outputs.
    /// If the column list is empty then copy all the columns to a new tuple.
    ///
    /// `is_join` option "on" builds an output tuple for the left child and
    /// appends the right child's one to it. Otherwise we build an output tuple
    /// only from the first child (left).
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - relation node contains invalid `Row` in the output
    /// - targets and children are inconsistent
    /// - column names don't exits
    pub fn new_columns(
        &mut self,
        rel_node_id: usize,
        children: &[usize],
        is_join: bool,
        targets: &[usize],
        col_names: &[&str],
        need_aliases: bool,
    ) -> Result<Vec<usize>, QueryPlannerError> {
        // We can pass two target children nodes only in a case
        // of `UnionAll` and `InnerlJoin`.
        // - For the `UnionAll` operator we need only the first
        // child to get correct column names for a new tuple
        // (second child aliases would be shadowed). But each reference should point
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

        let mut result: Vec<usize> = Vec::new();

        if col_names.is_empty() {
            let required_targets = if is_join { targets } else { &targets[0..1] };
            for target_idx in required_targets {
                let target_child: usize = if let Some(target) = targets.get(*target_idx) {
                    *target
                } else {
                    return Err(QueryPlannerError::InvalidRow);
                };
                let child_node: usize = if let Some(child) = children.get(target_child) {
                    *child
                } else {
                    return Err(QueryPlannerError::InvalidRow);
                };

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

                for (pos, alias_node) in child_row_list.iter().enumerate() {
                    let name: String =
                        if let Node::Expression(Expression::Alias { ref name, .. }) =
                            self.get_node(*alias_node)?
                        {
                            String::from(name)
                        } else {
                            return Err(QueryPlannerError::InvalidRow);
                        };
                    let new_targets: Vec<usize> = if is_join {
                        // Reference in a join tuple points to at first to the left,
                        // then to the right child.
                        vec![*target_idx]
                    } else {
                        // Reference in union tuple points to **both** left and right children.
                        targets.iter().copied().collect()
                    };
                    // Add new references and aliases to arena (if we need them).
                    let r_id = self.nodes.add_ref(rel_node_id, Some(new_targets), pos);
                    if need_aliases {
                        let a_id = self.nodes.add_alias(&name, r_id)?;
                        result.push(a_id);
                    } else {
                        result.push(r_id);
                    }
                }
            }

            return Ok(result);
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

        let map = if let Node::Relational(relational_op) = self.get_node(child_node)? {
            relational_op.output_alias_position_map(&self.nodes)?
        } else {
            return Err(QueryPlannerError::InvalidNode);
        };

        let mut result: Vec<usize> = Vec::new();

        let all_found = col_names.iter().all(|col| {
            map.get(*col).map_or(false, |pos| {
                let new_targets: Vec<usize> = targets.iter().copied().collect();
                // Add new references and aliases to arena (if we need them).
                let r_id = self.nodes.add_ref(rel_node_id, Some(new_targets), *pos);
                if need_aliases {
                    if let Ok(a_id) = self.nodes.add_alias(col, r_id) {
                        result.push(a_id);
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
            })
        });

        if all_found {
            return Ok(result);
        }
        Err(QueryPlannerError::InvalidRow)
    }

    /// New output for a single child node (have aliases).
    ///
    /// If column names are empty, copy all the columns from the child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - child is an inconsistent relational node
    /// - column names don't exits
    pub fn add_row_for_output(
        &mut self,
        id: usize,
        child: usize,
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(id, &[child], false, &[0], col_names, true)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// New output row for union node.
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_union(
        &mut self,
        id: usize,
        left: usize,
        right: usize,
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(id, &[left, right], false, &[0, 1], &[], true)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// New output row for join node.
    ///
    /// Contains all the columns from left and right children.
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_join(
        &mut self,
        id: usize,
        left: usize,
        right: usize,
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(id, &[left, right], true, &[0, 1], &[], true)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// Project columns from the child node.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - child is an inconsistent relational node
    /// - column names don't exits
    pub fn add_row_from_child(
        &mut self,
        id: usize,
        child: usize,
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(id, &[child], false, &[0], col_names, false)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the join's left branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the left child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exits
    pub fn add_row_from_left_branch(
        &mut self,
        id: usize,
        left: usize,
        right: usize,
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(id, &[left, right], true, &[0], col_names, false)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the join's right branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the right child.
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exits
    pub fn add_row_from_right_branch(
        &mut self,
        id: usize,
        left: usize,
        right: usize,
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let list = self.new_columns(id, &[left, right], true, &[1], col_names, false)?;
        Ok(self.nodes.add_row(list, None))
    }
}

#[cfg(test)]
mod tests;
