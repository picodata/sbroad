//! Operators for expression transformations.

use super::expression::{Branch, Expression};
use super::relation::Table;
use super::{vec_alloc, Node, Plan};
use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Binary operator returning Bool expression.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Bool {
    /// `&&`
    And,
    /// `=`
    Eq,
    /// `=all` (also named `in`)
    EqAll,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `!=`
    NotEq,
    /// `||`
    Or,
}

/// Relational algebra operator returning a new tuple.
///
/// Transforms input tuple(s) into the output one using
/// relation algebra logic.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Relational {
    /// Inner Join
    InnerJoin {
        /// Left and right tuple comparison condition.
        /// In fact - an expression tree top index in plan node arena.
        condition: usize,
        /// Left branch tuple node index in the plan node arena.
        left: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
        /// Right branch tuple node index in the plan node arena.
        right: usize,
    },
    Motion {
        /// Child tuple node index in the plan node arena (left branch).
        child: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    Projection {
        /// Child tuple node index in the plan node arena (left branch).
        child: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    ScanRelation {
        /// Output tuple node index in the plan node arena.
        output: usize,
        /// Relation name.
        relation: String,
    },
    ScanSubQuery {
        /// SubQuery name
        alias: String,
        /// Child tuple node index in the plan node arena (left branch).
        child: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    Selection {
        /// Child tuple node index in the plan node arena (left branch).
        child: usize,
        /// Filter expression node index in the plan node arena.
        filter: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    UnionAll {
        /// Left branch tuple node index in the plan node arena.
        left: usize,
        /// Right branch tuple node index in the plan node arena.
        right: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
}

/// Returns a list of new alias nodes.
/// Helpful, when construct a new row from the child node
/// and we have only column names. We can feed this function
/// with column names and child node pointer to create a new
/// alias node list.
///
/// # Errors
/// Returns `QueryPlannerError` when child node is not a
/// relational operator or some of the alias names are
/// absent in the child node's output.  
pub fn new_alias_nodes(
    plan: &mut Plan,
    node: usize,
    col_names: &[&str],
    branch: &Branch,
) -> Result<Vec<usize>, QueryPlannerError> {
    if col_names.is_empty() {
        return Err(QueryPlannerError::InvalidRow);
    }

    if let Node::Relational(relation_node) = plan.get_node(node)? {
        let map = relation_node.output_alias_position_map(plan)?;
        let mut aliases: Vec<usize> = Vec::new();

        let all_found = col_names.iter().all(|col| {
            map.get(*col).map_or(false, |pos| {
                // Create new references and aliases. Save them to the plan nodes arena.
                let r_id = vec_alloc(
                    &mut plan.nodes,
                    Node::Expression(Expression::new_ref(branch.clone(), *pos)),
                );
                let a_id = vec_alloc(
                    &mut plan.nodes,
                    Node::Expression(Expression::new_alias(col, r_id)),
                );
                aliases.push(a_id);
                true
            })
        });

        if all_found {
            return Ok(aliases);
        }

        return Err(QueryPlannerError::InvalidRow);
    }
    Err(QueryPlannerError::InvalidPlan)
}

#[allow(dead_code)]
impl Relational {
    /// Get an <column name - position> map from the output tuple.
    ///
    /// We expect that the top level of the node's expression tree
    /// is a row of aliases with unique names.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the output tuple is invalid.
    pub fn output_alias_position_map(
        &self,
        plan: &Plan,
    ) -> Result<HashMap<String, usize>, QueryPlannerError> {
        let mut map: HashMap<String, usize> = HashMap::new();

        if let Some(Node::Expression(Expression::Row { list, .. })) = plan.nodes.get(self.output())
        {
            let valid = list.iter().enumerate().all(|(pos, item)| {
                // Check that expressions in the row list are all aliases
                if let Some(Node::Expression(Expression::Alias { ref name, .. })) =
                    plan.nodes.get(*item)
                {
                    // Populate the map and check duplicate absence
                    if map.insert(String::from(name), pos).is_none() {
                        return true;
                    }
                }
                false
            });
            if valid {
                return Ok(map);
            }
            return Err(QueryPlannerError::InvalidPlan);
        }
        Err(QueryPlannerError::ValueOutOfRange)
    }

    /// Get output tuple node index in plan node arena.
    #[must_use]
    pub fn output(&self) -> usize {
        match self {
            Relational::InnerJoin { output, .. }
            | Relational::Motion { output, .. }
            | Relational::Projection { output, .. }
            | Relational::ScanRelation { output, .. }
            | Relational::ScanSubQuery { output, .. }
            | Relational::Selection { output, .. }
            | Relational::UnionAll { output, .. } => *output,
        }
    }

    /// Return a list of column names from the output tuple.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` if the tuple is invalid.
    pub fn output_alias_names(&self, nodes: &[Node]) -> Result<Vec<String>, QueryPlannerError> {
        let mut names: Vec<String> = Vec::new();

        if let Some(Node::Expression(Expression::Row { list, .. })) = nodes.get(self.output()) {
            let valid = list.iter().all(|item| {
                if let Some(Node::Expression(Expression::Alias { ref name, .. })) = nodes.get(*item)
                {
                    names.push(name.clone());
                    true
                } else {
                    false
                }
            });
            if valid {
                return Ok(names);
            }
            return Err(QueryPlannerError::InvalidPlan);
        }
        Err(QueryPlannerError::ValueOutOfRange)
    }

    /// New `ScanRelation` constructor.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when relation is invalid.
    pub fn new_scan(table_name: &str, plan: &mut Plan) -> Result<Self, QueryPlannerError> {
        let nodes = &mut plan.nodes;
        if let Some(relations) = &plan.relations {
            if let Some(rel) = relations.get(table_name) {
                match rel {
                    Table::Segment { ref columns, .. } => {
                        let refs = columns
                            .iter()
                            .enumerate()
                            .map(|(pos, col)| {
                                let r = Expression::new_ref(Branch::Left, pos);
                                let r_id = vec_alloc(nodes, Node::Expression(r));
                                vec_alloc(
                                    nodes,
                                    Node::Expression(Expression::new_alias(&col.name, r_id)),
                                )
                            })
                            .collect();

                        return Ok(Relational::ScanRelation {
                            output: vec_alloc(
                                nodes,
                                Node::Expression(Expression::new_row(refs, None)),
                            ),
                            relation: String::from(table_name),
                        });
                    }
                    //TODO: implement virtual tables as well
                    _ => return Err(QueryPlannerError::InvalidRelation),
                }
            }
        }
        Err(QueryPlannerError::InvalidRelation)
    }

    /// New `Projection` constructor.
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - child node is not relational
    /// - child output tuple is invalid
    /// - column name do not match the ones in the child output tuple
    pub fn new_proj(
        plan: &mut Plan,
        child: usize,
        output: &[&str],
    ) -> Result<Self, QueryPlannerError> {
        let aliases = new_alias_nodes(plan, child, output, &Branch::Left)?;

        let new_output = vec_alloc(
            &mut plan.nodes,
            Node::Expression(Expression::new_row(aliases, None)),
        );

        Ok(Relational::Projection {
            child,
            output: new_output,
        })
    }

    /// New `Selection` constructor
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - filter expression is not boolean
    /// - child node is not relational
    /// - child output tuple is not valid
    pub fn new_select(
        plan: &mut Plan,
        child: usize,
        filter: usize,
    ) -> Result<Self, QueryPlannerError> {
        if let Node::Expression(Expression::Bool { .. }) = plan.get_node(filter)? {
        } else {
            return Err(QueryPlannerError::InvalidBool);
        }

        let names: Vec<String> = if let Node::Relational(rel_op) = plan.get_node(child)? {
            rel_op.output_alias_names(&plan.nodes)?
        } else {
            return Err(QueryPlannerError::InvalidRow);
        };
        let output: Vec<&str> = names.iter().map(|s| s as &str).collect();
        let aliases = new_alias_nodes(plan, child, &output, &Branch::Left)?;

        let new_output = vec_alloc(
            &mut plan.nodes,
            Node::Expression(Expression::new_row(aliases, None)),
        );

        Ok(Relational::Selection {
            child,
            filter,
            output: new_output,
        })
    }

    /// New `UnionAll` constructor.
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - children nodes are not relational
    /// - children tuples are invalid
    /// - children tuples have mismatching structure
    pub fn new_union_all(
        plan: &mut Plan,
        left: usize,
        right: usize,
    ) -> Result<Self, QueryPlannerError> {
        let left_names: Vec<String> = if let Node::Relational(rel_op) = plan.get_node(left)? {
            rel_op.output_alias_names(&plan.nodes)?
        } else {
            return Err(QueryPlannerError::InvalidRow);
        };

        let right_names: Vec<String> = if let Node::Relational(rel_op) = plan.get_node(right)? {
            rel_op.output_alias_names(&plan.nodes)?
        } else {
            return Err(QueryPlannerError::InvalidRow);
        };

        let equal = (left_names.len() == right_names.len())
            && left_names.iter().zip(right_names).all(|(l, r)| l.eq(&r));

        if !equal {
            return Err(QueryPlannerError::NotEqualRows);
        }

        // Generate new output columns.
        let col_names: Vec<&str> = left_names.iter().map(|s| s as &str).collect();
        let aliases = new_alias_nodes(plan, left, &col_names, &Branch::Both)?;

        let output = vec_alloc(
            &mut plan.nodes,
            Node::Expression(Expression::new_row(aliases, None)),
        );

        Ok(Relational::UnionAll {
            left,
            right,
            output,
        })
    }

    /// New `ScanSubQuery` constructor.
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - child node is not relational
    /// - child node output is not a correct tuple
    /// - `SubQuery` name is empty
    pub fn new_sub_query(
        plan: &mut Plan,
        child: usize,
        alias: &str,
    ) -> Result<Self, QueryPlannerError> {
        let names: Vec<String> = if let Node::Relational(rel_op) = plan.get_node(child)? {
            rel_op.output_alias_names(&plan.nodes)?
        } else {
            return Err(QueryPlannerError::InvalidRow);
        };
        if alias.is_empty() {
            return Err(QueryPlannerError::InvalidName);
        }

        let col_names: Vec<&str> = names.iter().map(|s| s as &str).collect();
        let aliases = new_alias_nodes(plan, child, &col_names, &Branch::Both)?;

        let output = vec_alloc(
            &mut plan.nodes,
            Node::Expression(Expression::new_row(aliases, None)),
        );

        Ok(Relational::ScanSubQuery {
            alias: String::from(alias),
            child,
            output,
        })
    }
}

#[cfg(test)]
mod tests;
