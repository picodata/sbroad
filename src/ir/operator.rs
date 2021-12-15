//! Operators for expression transformations.

use super::expression::Expression;
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
        /// Contains exactly two elements: left and right node indexes
        /// from the plan node arena.
        children: Vec<usize>,
        /// Left and right tuple comparison condition.
        /// In fact - an expression tree top index from the plan node arena.
        condition: usize,
        /// Logical node ID
        id: usize,
        /// Output tuple node index from the plan node arena.
        output: usize,
    },
    Motion {
        /// Contains exactly a single element: child node index
        /// from the plan node arena.
        children: Vec<usize>,
        /// Logical node ID
        id: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    Projection {
        /// Contains at least a single element: child node index
        /// from the plan node arena. Every element other that the
        /// first one should be treated as a `SubQuery` node from
        /// the output tuple tree.
        children: Vec<usize>,
        /// Logical node ID
        id: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    ScanRelation {
        /// Output tuple node index in the plan node arena.
        output: usize,
        /// Logical node ID
        id: usize,
        /// Relation name.
        relation: String,
    },
    ScanSubQuery {
        /// SubQuery name
        alias: String,
        /// Contains exactly a single element: child node index
        /// from the plan node arena.
        children: Vec<usize>,
        /// Logical node ID
        id: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    Selection {
        /// Contains at least a single element: child node index
        /// from the plan node arena. Every element other that the
        /// first one should be treated as a `SubQuery` node from
        /// the filter tree.
        children: Vec<usize>,
        /// Filter expression node index in the plan node arena.
        filter: usize,
        /// Logical node ID
        id: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    UnionAll {
        /// Contains exactly two elements: left and right node indexes
        /// from the plan node arena.
        children: Vec<usize>,
        /// Logical node ID
        id: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
}

/// Create a new tuple from the children nodes output, containing only
/// a specified list of column names. If the column list is empty then
/// just copy all the columns to a new tuple.
/// # Errors
/// Returns `QueryPlannerError`:
/// - relation node contains invalid `Row` in the output
/// - targets and children are inconsistent
/// - column names don't exits
pub fn new_row_node(
    plan: &mut Plan,
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
            if let Node::Relational(relational_op) = plan.get_node(child_node)? {
                if let Node::Expression(Expression::Row { list, .. }) =
                    plan.get_node(relational_op.output())?
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
                plan.get_node(*alias_node)?
            {
                String::from(name)
            } else {
                return Err(QueryPlannerError::InvalidRow);
            };
            let new_targets: Vec<usize> = targets.iter().copied().collect();
            // Create new references and aliases. Save them to the plan nodes arena.
            let r_id = vec_alloc(
                &mut plan.nodes,
                Node::Expression(Expression::new_ref(rel_node_id, Some(new_targets), pos)),
            );
            let a_id = vec_alloc(
                &mut plan.nodes,
                Node::Expression(Expression::new_alias(&name, r_id)),
            );
            aliases.push(a_id);
        }

        let row_node = vec_alloc(
            &mut plan.nodes,
            Node::Expression(Expression::new_row(aliases, None)),
        );
        return Ok(row_node);
    }

    let map = if let Node::Relational(relational_op) = plan.get_node(child_node)? {
        relational_op.output_alias_position_map(plan)?
    } else {
        return Err(QueryPlannerError::InvalidNode);
    };

    let mut aliases: Vec<usize> = Vec::new();

    let all_found = col_names.iter().all(|col| {
        map.get(*col).map_or(false, |pos| {
            let new_targets: Vec<usize> = targets.iter().copied().collect();
            // Create new references and aliases. Save them to the plan nodes arena.
            let r_id = vec_alloc(
                &mut plan.nodes,
                Node::Expression(Expression::new_ref(rel_node_id, Some(new_targets), *pos)),
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
        let row_node = vec_alloc(
            &mut plan.nodes,
            Node::Expression(Expression::new_row(aliases, None)),
        );
        return Ok(row_node);
    }
    Err(QueryPlannerError::InvalidRow)
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

    /// Get logical id of the relational node.
    #[must_use]
    pub fn logical_id(&self) -> usize {
        match self {
            Relational::InnerJoin { id, .. }
            | Relational::Motion { id, .. }
            | Relational::Projection { id, .. }
            | Relational::ScanRelation { id, .. }
            | Relational::ScanSubQuery { id, .. }
            | Relational::Selection { id, .. }
            | Relational::UnionAll { id, .. } => *id,
        }
    }

    // Get a copy of the children nodes.
    #[must_use]
    pub fn children(&self) -> Option<Vec<usize>> {
        match self {
            Relational::InnerJoin { children, .. }
            | Relational::Motion { children, .. }
            | Relational::Projection { children, .. }
            | Relational::ScanSubQuery { children, .. }
            | Relational::Selection { children, .. }
            | Relational::UnionAll { children, .. } => Some(children.clone()),
            Relational::ScanRelation { .. } => None,
        }
    }

    /// New `ScanRelation` constructor.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when relation is invalid.
    pub fn new_scan(table_name: &str, plan: &mut Plan) -> Result<Self, QueryPlannerError> {
        let scan_id = plan.next_node_id();
        let nodes = &mut plan.nodes;

        if let Some(relations) = &plan.relations {
            if let Some(rel) = relations.get(table_name) {
                match rel {
                    Table::Segment { ref columns, .. } => {
                        let refs = columns
                            .iter()
                            .enumerate()
                            .map(|(pos, col)| {
                                let r = Expression::new_ref(scan_id, None, pos);
                                let r_id = vec_alloc(nodes, Node::Expression(r));
                                vec_alloc(
                                    nodes,
                                    Node::Expression(Expression::new_alias(&col.name, r_id)),
                                )
                            })
                            .collect();

                        return Ok(Relational::ScanRelation {
                            id: scan_id,
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

    // TODO: we need a more flexible projection constructor (constants, etc)

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
        col_names: &[&str],
    ) -> Result<Self, QueryPlannerError> {
        let id = plan.next_node_id();
        let children: Vec<usize> = vec![child];
        let output = new_row_node(plan, id, &children, &[0], col_names)?;

        Ok(Relational::Projection {
            children,
            id,
            output,
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

        let id = plan.next_node_id();
        let children: Vec<usize> = vec![child];
        let output = new_row_node(plan, id, &children, &[0], &[])?;

        Ok(Relational::Selection {
            children,
            filter,
            id,
            output,
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
        let child_row_len = |child: usize, plan: &Plan| -> Result<usize, QueryPlannerError> {
            if let Node::Relational(relational_op) = plan.get_node(child)? {
                match plan.get_node(relational_op.output())? {
                    Node::Expression(Expression::Row { ref list, .. }) => Ok(list.len()),
                    _ => Err(QueryPlannerError::InvalidRow),
                }
            } else {
                Err(QueryPlannerError::InvalidRow)
            }
        };

        if child_row_len(left, plan) != child_row_len(right, plan) {
            return Err(QueryPlannerError::NotEqualRows);
        }

        let id = plan.next_node_id();
        let children: Vec<usize> = vec![left, right];
        let output = new_row_node(plan, id, &children, &[0, 1], &[])?;

        Ok(Relational::UnionAll {
            children,
            id,
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
        if alias.is_empty() {
            return Err(QueryPlannerError::InvalidName);
        }
        let id = plan.next_node_id();
        let children: Vec<usize> = vec![child];
        let output = new_row_node(plan, id, &children, &[0], &[])?;

        Ok(Relational::ScanSubQuery {
            alias: String::from(alias),
            children,
            id,
            output,
        })
    }
}

#[cfg(test)]
mod tests;
