//! Tuple operators module.
//!
//! Contains operator nodes that transform the tuples in IR tree.

use ahash::RandomState;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::errors::QueryPlannerError;

use super::expression::Expression;
use super::transformation::redistribution::{DataGeneration, MotionPolicy};
use super::{Node, Nodes, Plan};
use crate::collection;
use crate::ir::distribution::Distribution;
use crate::ir::relation::ColumnRole;
use traversal::Bft;

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
    /// Creates `Bool` from the operator string.
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
/// Transforms input tuple(s) into the output one using the
/// relation algebra logic.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Relational {
    Insert {
        /// Relation name.
        relation: String,
        /// Target column positions for data insertion from
        /// the child's tuple.
        columns: Vec<usize>,
        /// Contains exactly one single element.
        children: Vec<usize>,
        /// The output tuple (need for `insert returning`).
        output: usize,
    },
    InnerJoin {
        /// Contains at least two elements: left and right node indexes
        /// from the plan node arena. Every element other than those
        /// two should be treated as a `SubQuery` node.
        children: Vec<usize>,
        /// Left and right tuple comparison condition.
        /// In fact it is an expression tree top index from the plan node arena.
        condition: usize,
        /// Outputs tuple node index from the plan node arena.
        output: usize,
    },
    Motion {
        /// Contains exactly one single element: child node index
        /// from the plan node arena.
        children: Vec<usize>,
        /// Motion policy - the amount of data to be moved.
        policy: MotionPolicy,
        /// Data generation strategy - what data to be generated.
        generation: DataGeneration,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
    },
    Projection {
        /// Contains at least one single element: child node index
        /// from the plan node arena. Every element other than the
        /// first one should be treated as a `SubQuery` node from
        /// the output tree.
        children: Vec<usize>,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
    },
    ScanRelation {
        // Scan name.
        alias: Option<String>,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
        /// Relation name.
        relation: String,
    },
    ScanSubQuery {
        /// SubQuery name.
        alias: Option<String>,
        /// Contains exactly one single element: child node index
        /// from the plan node arena.
        children: Vec<usize>,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
    },
    Selection {
        /// Contains at least one single element: child node index
        /// from the plan node arena. Every element other than the
        /// first one should be treated as a `SubQuery` node from
        /// the filter tree.
        children: Vec<usize>,
        /// Filters expression node index in the plan node arena.
        filter: usize,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
    },
    UnionAll {
        /// Contains exactly two elements: left and right node indexes
        /// from the plan node arena.
        children: Vec<usize>,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
    },
    Values {
        /// Output tuple.
        output: usize,
        /// Тon-empty list of value rows.
        children: Vec<usize>,
    },
    ValuesRow {
        /// Output tuple of aliases.
        output: usize,
        /// The data tuple.
        data: usize,
        /// A list of children is required for the rows containing
        /// sub-queries. For example, the row `(1, (select a from t))`
        /// requires `children` to keep projection node. If the row
        /// contains only constants (i.e. `(1, 2)`), then `children`
        /// should be empty.
        children: Vec<usize>,
    },
}

#[allow(dead_code)]
impl Relational {
    /// Gets a <column name - position> map from the output tuple.
    ///
    /// We expect that the top level of the node's expression tree
    /// is a row of aliases with unique names.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the output tuple is invalid.
    pub fn output_alias_position_map<'rel_op, 'nodes>(
        &'rel_op self,
        nodes: &'nodes Nodes,
    ) -> Result<HashMap<&'nodes str, usize, RandomState>, QueryPlannerError> {
        if let Some(Node::Expression(Expression::Row { list, .. })) = nodes.arena.get(self.output())
        {
            let state = RandomState::new();
            let mut map: HashMap<&str, usize, RandomState> =
                HashMap::with_capacity_and_hasher(list.len(), state);
            let valid = list.iter().enumerate().all(|(pos, item)| {
                // Checks that expressions in the row list are all aliases
                if let Some(Node::Expression(Expression::Alias { ref name, .. })) =
                    nodes.arena.get(*item)
                {
                    // Populates the map and checks for duplicates
                    if map.insert(name, pos).is_none() {
                        return true;
                    }
                }
                false
            });
            if valid {
                return Ok(map);
            }
            return Err(QueryPlannerError::CustomError(
                "Invalid output tuple".to_string(),
            ));
        }
        Err(QueryPlannerError::CustomError(
            "Failed to find an output tuple node in the arena".to_string(),
        ))
    }

    /// Gets output tuple node index in plan node arena.
    #[must_use]
    pub fn output(&self) -> usize {
        match self {
            Relational::InnerJoin { output, .. }
            | Relational::Insert { output, .. }
            | Relational::Motion { output, .. }
            | Relational::Projection { output, .. }
            | Relational::ScanRelation { output, .. }
            | Relational::ScanSubQuery { output, .. }
            | Relational::Selection { output, .. }
            | Relational::UnionAll { output, .. }
            | Relational::Values { output, .. }
            | Relational::ValuesRow { output, .. } => *output,
        }
    }

    // Gets a copy of the children nodes.
    #[must_use]
    pub fn children(&self) -> Option<&[usize]> {
        match self {
            Relational::InnerJoin { children, .. }
            | Relational::Insert { children, .. }
            | Relational::Motion { children, .. }
            | Relational::Projection { children, .. }
            | Relational::ScanSubQuery { children, .. }
            | Relational::Selection { children, .. }
            | Relational::UnionAll { children, .. }
            | Relational::ValuesRow { children, .. }
            | Relational::Values { children, .. } => Some(children),
            Relational::ScanRelation { .. } => None,
        }
    }

    /// Checks if the node is an insertion.
    #[must_use]
    pub fn is_insert(&self) -> bool {
        matches!(self, Relational::Insert { .. })
    }

    /// Checks that the node is a motion.
    #[must_use]
    pub fn is_motion(&self) -> bool {
        matches!(self, &Relational::Motion { .. })
    }

    /// Checks that the node is a sub-query scan.
    #[must_use]
    pub fn is_subquery(&self) -> bool {
        matches!(self, &Relational::ScanSubQuery { .. })
    }

    /// Sets new children to relational node.
    ///
    /// # Errors
    /// - try to set children for the scan node (it is always a leaf node)
    pub fn set_children(&mut self, children: Vec<usize>) -> Result<(), QueryPlannerError> {
        match self {
            Relational::InnerJoin {
                children: ref mut old,
                ..
            }
            | Relational::Insert {
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
            }
            | Relational::Values {
                children: ref mut old,
                ..
            }
            | Relational::ValuesRow {
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

    /// Gets relational scan name if it exists.
    ///
    /// # Errors
    /// - plan tree is invalid (failed to retrieve child nodes)
    pub fn scan_name<'n>(
        &'n self,
        plan: &'n Plan,
        position: usize,
    ) -> Result<Option<&'n str>, QueryPlannerError> {
        match self {
            Relational::Insert { relation, .. } => Ok(Some(relation.as_str())),
            Relational::ScanRelation {
                alias, relation, ..
            } => Ok(alias.as_deref().or(Some(relation.as_str()))),
            Relational::ScanSubQuery { alias, .. } => Ok(alias.as_deref()),
            Relational::Projection { .. }
            | Relational::Selection { .. }
            | Relational::InnerJoin { .. }
            | Relational::Motion { .. } => {
                let output_row = plan.get_expression_node(self.output())?;
                let list = output_row.get_row_list()?;
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
            Relational::UnionAll { .. }
            | Relational::Values { .. }
            | Relational::ValuesRow { .. } => Ok(None),
        }
    }

    /// Sets new scan name to relational node.
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
    /// Adds insert node.
    ///
    /// # Errors
    /// - Failed to find a target relation.
    pub fn add_insert(
        &mut self,
        relation: &str,
        child: usize,
        columns: &[&str],
    ) -> Result<usize, QueryPlannerError> {
        let rel_map = self.relations.as_ref().ok_or_else(|| {
            QueryPlannerError::CustomError("Plan doesn't contain any relations".to_string())
        })?;
        let rel = rel_map.get(relation).ok_or_else(|| {
            QueryPlannerError::CustomError(format!("Invalid relation: {}", relation))
        })?;
        let columns: Vec<usize> = if columns.is_empty() {
            rel.columns
                .iter()
                .enumerate()
                .filter(|(_, c)| ColumnRole::User.eq(c.get_role()))
                .map(|(i, _)| i)
                .collect()
        } else {
            let mut names: HashMap<&str, (&ColumnRole, usize), RandomState> =
                HashMap::with_capacity_and_hasher(columns.len(), RandomState::new());
            rel.columns.iter().enumerate().for_each(|(i, c)| {
                names.insert(c.name.as_str(), (c.get_role(), i));
            });
            let mut cols: Vec<usize> = Vec::with_capacity(names.len());
            for name in columns {
                match names.get(name) {
                    Some((&ColumnRole::User, pos)) => cols.push(*pos),
                    Some((&ColumnRole::Sharding, _)) => {
                        return Err(QueryPlannerError::CustomError(format!(
                            "System column {} cannot be inserted",
                            name
                        )))
                    }
                    None => {
                        return Err(QueryPlannerError::CustomError(format!(
                            "Column {} does not exist",
                            name
                        )))
                    }
                }
            }
            cols
        };
        let child_rel = self.get_relation_node(child)?;
        let child_output = self.get_expression_node(child_rel.output())?;
        let child_output_list_len = if let Expression::Row { list, .. } = child_output {
            list.len()
        } else {
            return Err(QueryPlannerError::CustomError(String::from(
                "Child output is not a row.",
            )));
        };
        if child_output_list_len != columns.len() {
            return Err(QueryPlannerError::CustomError(format!(
                "Invalid number of values: {}. Table {} expects {} column(s).",
                child_output_list_len,
                relation,
                columns.len()
            )));
        }

        let mut refs: Vec<usize> = Vec::with_capacity(rel.columns.len());
        for (pos, col) in rel.columns.iter().enumerate() {
            let r_id = self.nodes.add_ref(None, None, pos);
            let col_alias_id = self.nodes.add_alias(&col.name, r_id)?;
            refs.push(col_alias_id);
        }
        let dist = Distribution::Segment {
            keys: collection! { rel.key.clone() },
        };
        let output = self.nodes.add_row_of_aliases(refs, Some(dist))?;
        let insert = Node::Relational(Relational::Insert {
            relation: relation.into(),
            columns,
            children: vec![child],
            output,
        });
        let insert_id = self.nodes.push(insert);
        self.replace_parent_in_subtree(output, None, Some(insert_id))?;
        Ok(insert_id)
    }

    /// Adds a scan node.
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
                let mut refs: Vec<usize> = Vec::with_capacity(rel.columns.len());
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

    /// Adds inner join node.
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

        // For any child in a relational scan, we need to
        // remove a sharding column from its output with a
        // projection node and wrap the result with a sub-query
        // scan.
        let mut children: Vec<usize> = Vec::with_capacity(2);
        for child in &[left, right] {
            let child_node = self.get_relation_node(*child)?;
            let chid_id = if let Relational::ScanRelation {
                relation, alias, ..
            } = child_node
            {
                let scan_name = if let Some(alias_name) = alias {
                    alias_name.clone()
                } else {
                    relation.clone()
                };
                let proj_id = self.add_proj(*child, &[])?;
                self.add_sub_query(proj_id, Some(&scan_name))?
            } else {
                *child
            };
            children.push(chid_id);
        }
        if let (Some(left_id), Some(right_id)) = (children.get(0), children.get(1)) {
            let output = self.add_row_for_join(*left_id, *right_id)?;
            let join = Relational::InnerJoin {
                children: vec![*left_id, *right_id],
                condition,
                output,
            };

            let join_id = self.nodes.push(Node::Relational(join));
            self.replace_parent_in_subtree(condition, None, Some(join_id))?;
            self.replace_parent_in_subtree(output, None, Some(join_id))?;
            return Ok(join_id);
        }
        Err(QueryPlannerError::CustomError(
            "Invalid children for join".to_string(),
        ))
    }

    /// Adds motion node.
    ///
    /// # Errors
    /// - child node is not relational
    /// - child output tuple is invalid
    pub fn add_motion(
        &mut self,
        child_id: usize,
        policy: &MotionPolicy,
        generation: &DataGeneration,
    ) -> Result<usize, QueryPlannerError> {
        if let Node::Relational(_) = self.get_node(child_id)? {
        } else {
            return Err(QueryPlannerError::InvalidRelation);
        }

        let output = self.add_row_for_output(child_id, &[], true)?;
        self.set_const_dist(output)?;
        let motion = Relational::Motion {
            children: vec![child_id],
            policy: policy.clone(),
            generation: generation.clone(),
            output,
        };
        let motion_id = self.nodes.push(Node::Relational(motion));
        self.replace_parent_in_subtree(output, None, Some(motion_id))?;
        Ok(motion_id)
    }

    // TODO: we need a more flexible projection constructor (constants, etc)

    /// Adds projection node.
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
        let output = self.add_row_for_output(child, col_names, false)?;
        let proj = Relational::Projection {
            children: vec![child],
            output,
        };

        let proj_id = self.nodes.push(Node::Relational(proj));
        self.replace_parent_in_subtree(output, None, Some(proj_id))?;
        Ok(proj_id)
    }

    /// Adds projection node (use a list of expressions instead of alias names).
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

    /// Adds selection node
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

        let output = self.add_row_for_output(first_child, &[], true)?;
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

    /// Adds sub query scan node.
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

        let output = self.add_row_for_output(child, &[], true)?;
        let sq = Relational::ScanSubQuery {
            alias: name,
            children: vec![child],
            output,
        };

        let sq_id = self.nodes.push(Node::Relational(sq));
        self.replace_parent_in_subtree(output, None, Some(sq_id))?;
        Ok(sq_id)
    }

    /// Adds union all node.
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

    /// Adds a values row node
    ///
    /// # Errors
    /// - Row node is not of a row type
    pub fn add_values_row(
        &mut self,
        row_id: usize,
        col_idx: &mut usize,
    ) -> Result<usize, QueryPlannerError> {
        let row = self.get_expression_node(row_id)?;
        let columns = if let Expression::Row { list, .. } = row {
            list.clone()
        } else {
            return Err(QueryPlannerError::CustomError(format!(
                "Expression {} is not a row.",
                row_id
            )));
        };
        let mut aliases: Vec<usize> = Vec::with_capacity(columns.len());
        for col_id in columns {
            // Generate a row of aliases for the incoming row.
            *col_idx += 1;
            let name = format!("COLUMN_{}", col_idx);
            let alias_id = self.nodes.add_alias(&name, col_id)?;
            aliases.push(alias_id);
        }
        let output = self.nodes.add_row(aliases, None);

        let values_row = Relational::ValuesRow {
            output,
            data: row_id,
            children: vec![],
        };
        let values_row_id = self.nodes.push(Node::Relational(values_row));
        self.replace_parent_in_subtree(row_id, None, Some(values_row_id))?;
        Ok(values_row_id)
    }

    /// Adds values node.
    ///
    /// # Errors
    /// - No child nodes
    /// - Child node is not relational
    pub fn add_values(&mut self, children: Vec<usize>) -> Result<usize, QueryPlannerError> {
        // Get the last row of the children list. We need it to
        // get the correct anonymous column names.
        let last_id = if let Some(last_id) = children.last() {
            *last_id
        } else {
            return Err(QueryPlannerError::CustomError(
                "Values node must have at least one child.".into(),
            ));
        };
        let child_last = self.get_relation_node(last_id)?;
        let last_output_id = if let Relational::ValuesRow { output, .. } = child_last {
            *output
        } else {
            return Err(QueryPlannerError::CustomError(
                "Values node must have at least one child row.".into(),
            ));
        };
        let last_output = self.get_expression_node(last_output_id)?;
        let names = if let Expression::Row { list, .. } = last_output {
            let mut aliases: Vec<String> = Vec::with_capacity(list.len());
            for alias_id in list {
                let alias = self.get_expression_node(*alias_id)?;
                if let Expression::Alias { name, .. } = alias {
                    aliases.push(name.clone());
                } else {
                    return Err(QueryPlannerError::CustomError("Expected an alias".into()));
                }
            }
            aliases
        } else {
            return Err(QueryPlannerError::CustomError(
                "Values node must have at least one child row.".into(),
            ));
        };

        // Generate a row of aliases referencing all the children.
        let mut aliases: Vec<usize> = Vec::with_capacity(names.len());
        for (pos, name) in names.iter().enumerate() {
            let ref_id =
                self.nodes
                    .add_ref(None, Some((0..children.len()).collect::<Vec<usize>>()), pos);
            let alias_id = self.nodes.add_alias(name, ref_id)?;
            aliases.push(alias_id);
        }
        let output = self.nodes.add_row(aliases, None);

        let values = Relational::Values { output, children };
        let values_id = self.nodes.push(Node::Relational(values));
        self.replace_parent_in_subtree(output, None, Some(values_id))?;
        Ok(values_id)
    }

    /// Gets an output tuple from relational node id
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

    /// Gets children from relational node.
    ///
    /// # Errors
    /// - node is not relational
    pub fn get_relational_children(
        &self,
        rel_id: usize,
    ) -> Result<Option<&[usize]>, QueryPlannerError> {
        if let Node::Relational(rel) = self.get_node(rel_id)? {
            Ok(rel.children())
        } else {
            Err(QueryPlannerError::CustomError(
                "Invalid relational node".into(),
            ))
        }
    }

    /// Synchronize values row output with the data tuple after parameter binding.
    ///
    /// # Errors
    /// - Node is not values row
    /// - Output and data tuples have different number of columns
    /// - Output is not a row of aliases
    pub fn update_values_row(&mut self, id: usize) -> Result<(), QueryPlannerError> {
        let values_row = self.get_node(id)?;
        let (output_id, data_id) =
            if let Node::Relational(Relational::ValuesRow { output, data, .. }) = values_row {
                (*output, *data)
            } else {
                return Err(QueryPlannerError::CustomError(format!(
                    "Expected a values row: {:?}",
                    values_row
                )));
            };
        let data = self.get_expression_node(data_id)?;
        let data_list = data.clone_row_list()?;
        let output = self.get_expression_node(output_id)?;
        let output_list = output.clone_row_list()?;
        for (pos, alias_id) in output_list.iter().enumerate() {
            let new_child_id = *data_list.get(pos).ok_or_else(|| {
                QueryPlannerError::CustomError(format!("Expected a child at position {}", pos))
            })?;
            let alias = self.get_mut_expression_node(*alias_id)?;
            if let Expression::Alias { ref mut child, .. } = alias {
                *child = new_child_id;
            } else {
                return Err(QueryPlannerError::CustomError(format!(
                    "Expected an alias: {:?}",
                    alias
                )));
            }
        }
        Ok(())
    }

    /// Sets children for relational node
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

    /// Checks if the node is an additional child of some relational node.
    ///
    /// # Errors
    /// - Failed to get plan top
    /// - Node returned by the relational iterator is not relational (bug)
    pub fn is_additional_child(&self, node_id: usize) -> Result<bool, QueryPlannerError> {
        let top_id = self.get_top()?;
        let rel_tree = Bft::new(&top_id, |node| self.nodes.rel_iter(node));
        for (_, id) in rel_tree {
            let rel = self.get_relation_node(*id)?;
            match rel {
                Relational::Selection { children, .. } => {
                    if children.iter().skip(1).any(|&c| c == node_id) {
                        return Ok(true);
                    }
                }
                Relational::InnerJoin { children, .. } => {
                    if children.iter().skip(2).any(|&c| c == node_id) {
                        return Ok(true);
                    }
                }
                _ => continue,
            }
        }
        Ok(false)
    }

    /// Checks that the sub-query is an additional child of the parent relational node.
    ///
    /// # Errors
    /// - If the node is not relational.
    pub fn is_additional_child_of_rel(
        &self,
        rel_id: usize,
        sq_id: usize,
    ) -> Result<bool, QueryPlannerError> {
        let children = if let Some(children) = self.get_relational_children(rel_id)? {
            children
        } else {
            return Ok(false);
        };
        match self.get_relation_node(rel_id)? {
            Relational::Selection { .. } | Relational::Projection { .. } => {
                Ok(children.get(0) != Some(&sq_id))
            }
            Relational::InnerJoin { .. } => {
                Ok(children.get(0) != Some(&sq_id) && children.get(1) != Some(&sq_id))
            }
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests;
