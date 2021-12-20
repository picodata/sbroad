//! Operators for expression transformations.

use super::expression::Expression;
use super::relation::Table;
use super::{Node, Nodes, Plan};
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
        nodes: &Nodes,
    ) -> Result<HashMap<String, usize>, QueryPlannerError> {
        let mut map: HashMap<String, usize> = HashMap::new();

        if let Some(Node::Expression(Expression::Row { list, .. })) = nodes.arena.get(self.output())
        {
            let valid = list.iter().enumerate().all(|(pos, item)| {
                // Check that expressions in the row list are all aliases
                if let Some(Node::Expression(Expression::Alias { ref name, .. })) =
                    nodes.arena.get(*item)
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
}

impl Plan {
    /// Add a scan node.
    ///
    /// # Errors
    /// - relation is invalid
    pub fn add_scan(&mut self, table: &str) -> Result<usize, QueryPlannerError> {
        let logical_id = self.nodes.next_id();
        let nodes = &mut self.nodes;

        if let Some(relations) = &self.relations {
            if let Some(rel) = relations.get(table) {
                match rel {
                    Table::Segment { ref columns, .. } => {
                        let mut refs: Vec<usize> = Vec::new();

                        for (pos, col) in columns.iter().enumerate() {
                            let r_id = nodes.add_ref(logical_id, None, pos);
                            let alias_id = nodes.add_alias(&col.name, r_id)?;
                            refs.push(alias_id);
                        }

                        let scan = Relational::ScanRelation {
                            id: logical_id,
                            output: nodes.add_row(refs, None)?,
                            relation: String::from(table),
                        };

                        return Ok(nodes.push(Node::Relational(scan)));
                    }
                    //TODO: implement virtual tables as well
                    _ => return Err(QueryPlannerError::InvalidRelation),
                }
            }
        }
        Err(QueryPlannerError::InvalidRelation)
    }

    // TODO: we need a more flexible projection constructor (constants, etc)

    /// Add projection node.
    ///
    /// # Errors
    /// - child node is not relational
    /// - child output tuple is invalid
    /// - column name do not match the ones in the child output tuple
    pub fn add_proj(
        &mut self,
        child: usize,
        col_names: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let id = self.nodes.next_id();
        let children: Vec<usize> = vec![child];
        let output = self.add_output_row(id, &children, &[0], col_names)?;

        let proj = Relational::Projection {
            children,
            id,
            output,
        };
        Ok(self.nodes.push(Node::Relational(proj)))
    }

    /// Add selection node
    ///
    /// # Errors
    /// - filter expression is not boolean
    /// - child node is not relational
    /// - child output tuple is not valid
    pub fn add_select(
        &mut self,
        child: usize,
        filter: usize,
        id: usize,
    ) -> Result<usize, QueryPlannerError> {
        if let Node::Expression(Expression::Bool { .. }) = self.get_node(filter)? {
        } else {
            return Err(QueryPlannerError::InvalidBool);
        }

        let children: Vec<usize> = vec![child];
        let output = self.add_output_row(id, &children, &[0], &[])?;

        let select = Relational::Selection {
            children,
            filter,
            id,
            output,
        };

        Ok(self.nodes.push(Node::Relational(select)))
    }

    /// Add sub query scan node.
    ///
    /// # Errors
    /// - child node is not relational
    /// - child node output is not a correct tuple
    /// - sub query name is empty
    pub fn add_sub_query(&mut self, child: usize, alias: &str) -> Result<usize, QueryPlannerError> {
        if alias.is_empty() {
            return Err(QueryPlannerError::InvalidName);
        }
        let id = self.nodes.next_id();
        let children: Vec<usize> = vec![child];
        let output = self.add_output_row(id, &children, &[0], &[])?;

        let sq = Relational::ScanSubQuery {
            alias: String::from(alias),
            children,
            id,
            output,
        };

        Ok(self.nodes.push(Node::Relational(sq)))
    }

    /// Add union all node.
    ///
    /// # Errors
    /// - children nodes are not relational
    /// - children tuples are invalid
    /// - children tuples have mismatching structure
    pub fn add_union_all(&mut self, left: usize, right: usize) -> Result<usize, QueryPlannerError> {
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

        if child_row_len(left, self) != child_row_len(right, self) {
            return Err(QueryPlannerError::NotEqualRows);
        }

        let id = self.nodes.next_id();
        let children: Vec<usize> = vec![left, right];
        let output = self.add_output_row(id, &children, &[0, 1], &[])?;

        let union_all = Relational::UnionAll {
            children,
            id,
            output,
        };

        Ok(self.nodes.push(Node::Relational(union_all)))
    }
}

#[cfg(test)]
mod tests;
