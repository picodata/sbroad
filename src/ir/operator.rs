//! Operators for expression transformations.

use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::errors::QueryPlannerError;

use super::expression::Expression;
use super::transformation::redistribution::MotionPolicy;
use super::{Node, Nodes, Plan};

/// Binary operator returning Bool expression.
#[derive(Serialize, Deserialize, PartialEq, Debug, Eq, Hash, Clone)]
pub enum Bool {
    /// `&&`
    And,
    /// `=`
    Eq,
    /// `in`
    In,
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

impl Bool {
    /// Create `Bool` from operator string.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the operator is invalid.
    pub fn from(s: &str) -> Result<Self, QueryPlannerError> {
        match s.to_lowercase().as_str() {
            "and" => Ok(Bool::And),
            "or" => Ok(Bool::Or),
            "=" => Ok(Bool::Eq),
            "in" => Ok(Bool::In),
            ">" => Ok(Bool::Gt),
            ">=" => Ok(Bool::GtEq),
            "<" => Ok(Bool::Lt),
            "<=" => Ok(Bool::LtEq),
            "!=" | "<>" => Ok(Bool::NotEq),
            _ => Err(QueryPlannerError::InvalidBool),
        }
    }
}

impl Display for Bool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = match &self {
            Bool::And => "and",
            Bool::Or => "or",
            Bool::Eq => "=",
            Bool::In => "in",
            Bool::Gt => ">",
            Bool::GtEq => ">=",
            Bool::Lt => "<",
            Bool::LtEq => "<=",
            Bool::NotEq => "<>",
        };

        write!(f, "{}", op)
    }
}

/// Relational algebra operator returning a new tuple.
///
/// Transforms input tuple(s) into the output one using
/// relation algebra logic.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Relational {
    /// Inner Join
    InnerJoin {
        /// Contains at least two elements: left and right node indexes
        /// from the plan node arena. Every element other that the
        /// first two should be treated as a `SubQuery` node.
        children: Vec<usize>,
        /// Left and right tuple comparison condition.
        /// In fact - an expression tree top index from the plan node arena.
        condition: usize,
        /// Output tuple node index from the plan node arena.
        output: usize,
    },
    Motion {
        /// Contains exactly a single element: child node index
        /// from the plan node arena.
        children: Vec<usize>,
        policy: MotionPolicy,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    Projection {
        /// Contains at least a single element: child node index
        /// from the plan node arena. Every element other that the
        /// first one should be treated as a `SubQuery` node from
        /// the output tree.
        children: Vec<usize>,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    ScanRelation {
        // Scan name.
        alias: Option<String>,
        /// Output tuple node index in the plan node arena.
        output: usize,
        /// Relation name.
        relation: String,
    },
    ScanSubQuery {
        /// SubQuery name.
        alias: Option<String>,
        /// Contains exactly a single element: child node index
        /// from the plan node arena.
        children: Vec<usize>,
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
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    UnionAll {
        /// Contains exactly two elements: left and right node indexes
        /// from the plan node arena.
        children: Vec<usize>,
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

    /// Check node is a motion.
    #[must_use]
    pub fn is_motion(&self) -> bool {
        matches!(self, &Relational::Motion { .. })
    }

    /// Check node is a sub-query scan.
    #[must_use]
    pub fn is_subquery(&self) -> bool {
        matches!(self, &Relational::ScanSubQuery { .. })
    }

    /// Set new children to relational node.
    ///
    /// # Errors
    /// - try to set children for the scan node (it is always a leaf node)
    pub fn set_children(&mut self, children: Vec<usize>) -> Result<(), QueryPlannerError> {
        match self {
            Relational::InnerJoin {
                children: ref mut old,
                ..
            }
            | Relational::Motion {
                children: ref mut old,
                ..
            }
            | Relational::Projection {
                children: ref mut old,
                ..
            }
            | Relational::ScanSubQuery {
                children: ref mut old,
                ..
            }
            | Relational::Selection {
                children: ref mut old,
                ..
            }
            | Relational::UnionAll {
                children: ref mut old,
                ..
            } => {
                *old = children;
                Ok(())
            }
            Relational::ScanRelation { .. } => Err(QueryPlannerError::CustomError(String::from(
                "Scan is a leaf node",
            ))),
        }
    }

    /// Get relational scan name if it exists.
    ///
    /// # Errors
    /// - plan tree is invalid (failed to retrieve child nodes)
    pub fn scan_name<'n>(
        &'n self,
        plan: &'n Plan,
        position: usize,
    ) -> Result<Option<&'n str>, QueryPlannerError> {
        match self {
            Relational::ScanRelation {
                alias, relation, ..
            } => Ok(alias.as_deref().or(Some(relation.as_str()))),
            Relational::ScanSubQuery { alias, .. } => Ok(alias.as_deref()),
            Relational::Projection { .. }
            | Relational::Selection { .. }
            | Relational::InnerJoin { .. } => {
                let output_row = plan.get_expression_node(self.output())?;
                let list = output_row.extract_row_list()?;
                let col_id = *list.get(position).ok_or_else(|| {
                    QueryPlannerError::CustomError(String::from("Row has no such alias"))
                })?;
                let col_node = plan.get_expression_node(col_id)?;
                if let Expression::Alias { child, .. } = col_node {
                    let child_node = plan.get_expression_node(*child)?;
                    if let Expression::Reference { position: pos, .. } = child_node {
                        let rel_ids = plan.get_relational_from_reference_node(*child)?;
                        let rel_node = plan.get_relation_node(
                            rel_ids.into_iter().next().ok_or_else(|| {
                                QueryPlannerError::CustomError(String::from("No relational node"))
                            })?,
                        )?;
                        return rel_node.scan_name(plan, *pos);
                    }
                } else {
                    return Err(QueryPlannerError::CustomError(String::from(
                        "Expected an alias in the output row",
                    )));
                }
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Set new scan name to relational node.
    ///
    /// # Errors
    /// - relational node is not a scan.
    pub fn set_scan_name(&mut self, name: Option<String>) -> Result<(), QueryPlannerError> {
        match self {
            Relational::ScanRelation { ref mut alias, .. }
            | Relational::ScanSubQuery { ref mut alias, .. } => {
                *alias = name;
                Ok(())
            }
            _ => Err(QueryPlannerError::CustomError(
                "Relational node is not a scan.".into(),
            )),
        }
    }
}

impl Plan {
    /// Add a scan node.
    ///
    /// # Errors
    /// - relation is invalid
    pub fn add_scan(
        &mut self,
        table: &str,
        alias: Option<&str>,
    ) -> Result<usize, QueryPlannerError> {
        let nodes = &mut self.nodes;

        if let Some(relations) = &self.relations {
            if let Some(rel) = relations.get(table) {
                let mut refs: Vec<usize> = Vec::new();
                for (pos, col) in rel.columns.iter().enumerate() {
                    let r_id = nodes.add_ref(None, None, pos);
                    let col_alias_id = nodes.add_alias(&col.name, r_id)?;
                    refs.push(col_alias_id);
                }

                let output_id = nodes.add_row_of_aliases(refs, None)?;
                let scan = Relational::ScanRelation {
                    output: output_id,
                    relation: String::from(table),
                    alias: alias.map(String::from),
                };

                let scan_id = nodes.push(Node::Relational(scan));
                self.replace_parent_in_subtree(output_id, None, Some(scan_id))?;
                return Ok(scan_id);
            }
        }
        Err(QueryPlannerError::InvalidRelation)
    }

    /// Add inner join node.
    ///
    /// # Errors
    /// - condition is not a boolean expression
    /// - children nodes are not relational
    /// - children output tuples are invalid
    pub fn add_join(
        &mut self,
        left: usize,
        right: usize,
        condition: usize,
    ) -> Result<usize, QueryPlannerError> {
        if !self.is_trivalent(condition)? {
            return Err(QueryPlannerError::CustomError(String::from(
                "Condition is not a trivalent expression",
            )));
        }

        let output = self.add_row_for_join(left, right)?;
        let join = Relational::InnerJoin {
            children: vec![left, right],
            condition,
            output,
        };

        let join_id = self.nodes.push(Node::Relational(join));
        self.replace_parent_in_subtree(condition, None, Some(join_id))?;
        self.replace_parent_in_subtree(output, None, Some(join_id))?;
        Ok(join_id)
    }

    /// Add motion node.
    ///
    /// # Errors
    /// - child node is not relational
    /// - child output tuple is invalid
    pub fn add_motion(
        &mut self,
        child_id: usize,
        policy: &MotionPolicy,
    ) -> Result<usize, QueryPlannerError> {
        if let Node::Relational(_) = self.get_node(child_id)? {
        } else {
            return Err(QueryPlannerError::InvalidRelation);
        }

        let output = self.add_row_for_output(child_id, &[])?;
        let motion = Relational::Motion {
            children: vec![child_id],
            policy: policy.clone(),
            output,
        };
        let motion_id = self.nodes.push(Node::Relational(motion));
        self.replace_parent_in_subtree(output, None, Some(motion_id))?;
        Ok(motion_id)
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
        let output = self.add_row_for_output(child, col_names)?;
        let proj = Relational::Projection {
            children: vec![child],
            output,
        };

        let proj_id = self.nodes.push(Node::Relational(proj));
        self.replace_parent_in_subtree(output, None, Some(proj_id))?;
        Ok(proj_id)
    }

    /// Add projection node (use a list of expressions instead of alias names).
    ///
    /// # Errors
    /// - child node is not relational
    /// - child output tuple is invalid
    /// - columns are not aliases or have duplicate names
    pub fn add_proj_internal(
        &mut self,
        child: usize,
        columns: &[usize],
    ) -> Result<usize, QueryPlannerError> {
        let output = self.nodes.add_row_of_aliases(columns.to_vec(), None)?;
        let proj = Relational::Projection {
            children: vec![child],
            output,
        };

        let proj_id = self.nodes.push(Node::Relational(proj));
        self.replace_parent_in_subtree(output, None, Some(proj_id))?;
        Ok(proj_id)
    }

    /// Add selection node
    ///
    /// # Errors
    /// - children list is empty
    /// - filter expression is not boolean
    /// - children nodes are not relational
    /// - first child output tuple is not valid
    pub fn add_select(
        &mut self,
        children: &[usize],
        filter: usize,
    ) -> Result<usize, QueryPlannerError> {
        let first_child: usize = match children.len() {
            0 => return Err(QueryPlannerError::InvalidInput),
            _ => children[0],
        };

        if !self.is_trivalent(filter)? {
            return Err(QueryPlannerError::CustomError(
                "Filter expression is not a trivalent expression.".into(),
            ));
        }

        for child in children {
            if let Node::Relational(_) = self.get_node(*child)? {
            } else {
                return Err(QueryPlannerError::InvalidRelation);
            }
        }

        let output = self.add_row_for_output(first_child, &[])?;
        let select = Relational::Selection {
            children: children.into(),
            filter,
            output,
        };

        let select_id = self.nodes.push(Node::Relational(select));
        self.replace_parent_in_subtree(filter, None, Some(select_id))?;
        self.replace_parent_in_subtree(output, None, Some(select_id))?;
        Ok(select_id)
    }

    /// Add sub query scan node.
    ///
    /// # Errors
    /// - child node is not relational
    /// - child node output is not a correct tuple
    pub fn add_sub_query(
        &mut self,
        child: usize,
        alias: Option<&str>,
    ) -> Result<usize, QueryPlannerError> {
        let name: Option<String> = if let Some(name) = alias {
            if name.is_empty() {
                return Err(QueryPlannerError::InvalidName);
            }
            Some(String::from(name))
        } else {
            None
        };

        let output = self.add_row_for_output(child, &[])?;
        let sq = Relational::ScanSubQuery {
            alias: name,
            children: vec![child],
            output,
        };

        let sq_id = self.nodes.push(Node::Relational(sq));
        self.replace_parent_in_subtree(output, None, Some(sq_id))?;
        Ok(sq_id)
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

        let output = self.add_row_for_union(left, right)?;
        let union_all = Relational::UnionAll {
            children: vec![left, right],
            output,
        };

        let union_all_id = self.nodes.push(Node::Relational(union_all));
        self.replace_parent_in_subtree(output, None, Some(union_all_id))?;
        Ok(union_all_id)
    }

    /// Get an output tuple from relational node id
    ///
    /// # Errors
    /// - node is not relational
    pub fn get_relational_output(&self, rel_id: usize) -> Result<usize, QueryPlannerError> {
        if let Node::Relational(rel) = self.get_node(rel_id)? {
            Ok(rel.output())
        } else {
            Err(QueryPlannerError::InvalidRelation)
        }
    }

    /// Get children from relational node.
    ///
    /// # Errors
    /// - node is not relational
    pub fn get_relational_children(
        &self,
        rel_id: usize,
    ) -> Result<Option<Vec<usize>>, QueryPlannerError> {
        if let Node::Relational(rel) = self.get_node(rel_id)? {
            Ok(rel.children())
        } else {
            Err(QueryPlannerError::CustomError(
                "Invalid relational node".into(),
            ))
        }
    }

    /// Set children for relational node
    ///
    /// # Errors
    /// - node is not relational
    /// - node is scan (always a leaf node)
    pub fn set_relational_children(
        &mut self,
        rel_id: usize,
        children: Vec<usize>,
    ) -> Result<(), QueryPlannerError> {
        if let Node::Relational(ref mut rel) = self
            .nodes
            .arena
            .get_mut(rel_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?
        {
            rel.set_children(children)?;
            Ok(())
        } else {
            Err(QueryPlannerError::InvalidRelation)
        }
    }
}

#[cfg(test)]
mod tests;
