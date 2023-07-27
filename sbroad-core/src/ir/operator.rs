//! Tuple operators module.
//!
//! Contains operator nodes that transform the tuples in IR tree.

use ahash::RandomState;

use crate::collection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use crate::errors::{Action, Entity, SbroadError};

use super::expression::Expression;
use super::transformation::redistribution::MotionPolicy;
use super::tree::traversal::{BreadthFirst, EXPR_CAPACITY, REL_CAPACITY};
use super::{Node, Nodes, Plan};
use crate::ir::distribution::{Distribution, KeySet};
use crate::ir::helpers::RepeatableState;
use crate::ir::relation::ColumnRole;
use crate::ir::transformation::redistribution::JoinChild;

/// Binary operator returning Bool expression.
#[derive(Serialize, Deserialize, PartialEq, Debug, Eq, Hash, Clone)]
#[serde(rename_all = "lowercase")]
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
    /// `not in`
    NotIn,
    /// `||`
    Or,
}

impl Bool {
    /// Creates `Bool` from the operator string.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    pub fn from(s: &str) -> Result<Self, SbroadError> {
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
            "not in" => Ok(Bool::NotIn),
            _ => Err(SbroadError::Unsupported(Entity::Operator, None)),
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
            Bool::NotIn => "not in",
        };

        write!(f, "{op}")
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Eq, Hash, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Arithmetic {
    /// `*`
    Multiply,
    /// `/`
    Divide,
    /// `+`
    Add,
    /// `-`
    Subtract,
}

impl Arithmetic {
    /// Creates `Arithmetic` from the operator string.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    pub fn from(s: &str) -> Result<Self, SbroadError> {
        match s.to_lowercase().as_str() {
            "*" => Ok(Arithmetic::Multiply),
            "/" => Ok(Arithmetic::Divide),
            "+" => Ok(Arithmetic::Add),
            "-" => Ok(Arithmetic::Subtract),
            _ => Err(SbroadError::Unsupported(Entity::Operator, None)),
        }
    }
}

impl Display for Arithmetic {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = match &self {
            Arithmetic::Multiply => "*",
            Arithmetic::Divide => "/",
            Arithmetic::Add => "+",
            Arithmetic::Subtract => "-",
        };

        write!(f, "{op}")
    }
}

/// Unary operator returning Bool expression.
#[derive(Serialize, Deserialize, PartialEq, Debug, Eq, Hash, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Unary {
    /// `is null`
    IsNull,
    /// `is not null`
    IsNotNull,
    /// `exists`
    Exists,
    /// `not exists`
    NotExists,
}

impl Unary {
    /// Creates `Unary` from the operator string.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    pub fn from(s: &str) -> Result<Self, SbroadError> {
        match s.to_lowercase().as_str() {
            "is null" => Ok(Unary::IsNull),
            "is not null" => Ok(Unary::IsNotNull),
            "exists" => Ok(Unary::Exists),
            "not exists" => Ok(Unary::NotExists),
            _ => Err(SbroadError::Invalid(
                Entity::Operator,
                Some(format!(
                    "expected `is null` or `is not null`, got unary operator `{s}`"
                )),
            )),
        }
    }
}

impl Display for Unary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = match &self {
            Unary::IsNull => "is null",
            Unary::IsNotNull => "is not null",
            Unary::Exists => "exists",
            Unary::NotExists => "not exists",
        };

        write!(f, "{op}")
    }
}

/// Specifies what kind of join user specified in query
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum JoinKind {
    LeftOuter,
    Inner,
}

impl Display for JoinKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            JoinKind::LeftOuter => "left",
            JoinKind::Inner => "inner",
        };
        write!(f, "{kind}")
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum ConflictStrategy {
    /// Swallow the error, do not insert the conflicting tuple
    DoNothing,
    /// Replace the conflicting tuple with the new one
    DoReplace,
    /// Throw the error, no tuples will be inserted for this
    /// storage. But for other storages the insertion may be successful.
    DoFail,
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        ConflictStrategy::DoFail
    }
}

impl Display for ConflictStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            ConflictStrategy::DoNothing => "nothing",
            ConflictStrategy::DoReplace => "replace",
            ConflictStrategy::DoFail => "fail",
        };
        write!(f, "{s}")
    }
}

/// Relational algebra operator returning a new tuple.
///
/// Transforms input tuple(s) into the output one using the
/// relation algebra logic.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Relational {
    Except {
        /// Contains exactly two elements: left and right node indexes
        /// from the plan node arena.
        children: Vec<usize>,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
    },
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
        /// What to do in case there is a conflict during insert on storage
        conflict_strategy: ConflictStrategy,
    },
    Join {
        /// Contains at least two elements: left and right node indexes
        /// from the plan node arena. Every element other than those
        /// two should be treated as a `SubQuery` node.
        children: Vec<usize>,
        /// Left and right tuple comparison condition.
        /// In fact it is an expression tree top index from the plan node arena.
        condition: usize,
        /// Outputs tuple node index from the plan node arena.
        output: usize,
        /// inner or left
        kind: JoinKind,
    },
    Motion {
        // Scan name.
        alias: Option<String>,
        /// Contains exactly one single element: child node index
        /// from the plan node arena.
        children: Vec<usize>,
        /// Motion policy - the amount of data to be moved.
        policy: MotionPolicy,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
        /// A helper field indicating whether first element of
        /// `children` vec is a `Relational::SubQuery`.
        /// We need it on the stage of translating Plan to SQL, because
        /// by that moment we've already erased `children` information using
        /// `unlink_motion_subtree` function.
        is_child_subquery: bool,
    },
    Projection {
        /// Contains at least one single element: child node index
        /// from the plan node arena. Every element other than the
        /// first one should be treated as a `SubQuery` node from
        /// the output tree.
        children: Vec<usize>,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
        /// Wheter the select was marked with `distinct` keyword
        is_distinct: bool,
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
    GroupBy {
        /// The first child is a relational operator before group by
        children: Vec<usize>,
        gr_cols: Vec<usize>,
        output: usize,
        is_final: bool,
    },
    Having {
        children: Vec<usize>,
        output: usize,
        filter: usize,
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
        /// Non-empty list of value rows.
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
    /// Returns `SbroadError` when the output tuple is invalid.
    pub fn output_alias_position_map<'nodes>(
        &self,
        nodes: &'nodes Nodes,
    ) -> Result<HashMap<&'nodes str, usize, RandomState>, SbroadError> {
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
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("invalid output tuple".to_string()),
            ));
        }
        Err(SbroadError::NotFound(
            Entity::Node,
            "(an output tuple) from the arena".to_string(),
        ))
    }

    /// Gets an immutable id of the output tuple node of the plan's arena.
    #[must_use]
    pub fn output(&self) -> usize {
        match self {
            Relational::Except { output, .. }
            | Relational::GroupBy { output, .. }
            | Relational::Having { output, .. }
            | Relational::Join { output, .. }
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

    /// Gets an immutable reference to the output tuple node id.
    #[must_use]
    pub fn mut_output(&mut self) -> &mut usize {
        match self {
            Relational::Except { output, .. }
            | Relational::GroupBy { output, .. }
            | Relational::Having { output, .. }
            | Relational::Join { output, .. }
            | Relational::Insert { output, .. }
            | Relational::Motion { output, .. }
            | Relational::Projection { output, .. }
            | Relational::ScanRelation { output, .. }
            | Relational::ScanSubQuery { output, .. }
            | Relational::Selection { output, .. }
            | Relational::UnionAll { output, .. }
            | Relational::Values { output, .. }
            | Relational::ValuesRow { output, .. } => output,
        }
    }

    // Gets an immutable reference to the children nodes.
    #[must_use]
    pub fn children(&self) -> Option<&[usize]> {
        match self {
            Relational::Except { children, .. }
            | Relational::GroupBy { children, .. }
            | Relational::Join { children, .. }
            | Relational::Having { children, .. }
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

    // Gets a mutable reference to the children nodes.
    #[must_use]
    pub fn mut_children(&mut self) -> Option<&mut [usize]> {
        match self {
            Relational::Except {
                ref mut children, ..
            }
            | Relational::GroupBy {
                ref mut children, ..
            }
            | Relational::Having {
                ref mut children, ..
            }
            | Relational::Join {
                ref mut children, ..
            }
            | Relational::Insert {
                ref mut children, ..
            }
            | Relational::Motion {
                ref mut children, ..
            }
            | Relational::Projection {
                ref mut children, ..
            }
            | Relational::ScanSubQuery {
                ref mut children, ..
            }
            | Relational::Selection {
                ref mut children, ..
            }
            | Relational::UnionAll {
                ref mut children, ..
            }
            | Relational::ValuesRow {
                ref mut children, ..
            }
            | Relational::Values {
                ref mut children, ..
            } => Some(children),
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
    pub fn set_children(&mut self, children: Vec<usize>) -> Result<(), SbroadError> {
        match self {
            Relational::Except {
                children: ref mut old,
                ..
            }
            | Relational::Join {
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
            | Relational::GroupBy {
                children: ref mut old,
                ..
            }
            | Relational::Having {
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
            Relational::ScanRelation { .. } => Err(SbroadError::Invalid(
                Entity::Relational,
                Some("Scan is a leaf node".into()),
            )),
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
    ) -> Result<Option<&'n str>, SbroadError> {
        match self {
            Relational::Insert { relation, .. } => Ok(Some(relation.as_str())),
            Relational::ScanRelation {
                alias, relation, ..
            } => Ok(alias.as_deref().or(Some(relation.as_str()))),
            Relational::Projection { .. }
            | Relational::GroupBy { .. }
            | Relational::Having { .. }
            | Relational::Selection { .. }
            | Relational::Join { .. } => {
                let output_row = plan.get_expression_node(self.output())?;
                let list = output_row.get_row_list()?;
                let col_id = *list.get(position).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Column,
                        format!("at position {position} of Row, {self:?}, {output_row:?}"),
                    )
                })?;
                let col_node = plan.get_expression_node(col_id)?;
                if let Expression::Alias { child, .. } = col_node {
                    let child_node = plan.get_expression_node(*child)?;
                    if let Expression::Reference { position: pos, .. } = child_node {
                        let rel_id = *plan.get_relational_from_reference_node(*child)?;
                        let rel_node = plan.get_relation_node(rel_id)?;
                        if rel_node == self {
                            return Err(SbroadError::DuplicatedValue(format!(
                                "Reference to the same node {rel_node:?} at position {position}"
                            )));
                        }
                        return rel_node.scan_name(plan, *pos);
                    }
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("expected an alias in the output row".into()),
                    ));
                }
                Ok(None)
            }
            Relational::ScanSubQuery { alias, .. } | Relational::Motion { alias, .. } => {
                if let Some(name) = alias.as_ref() {
                    if !name.is_empty() {
                        return Ok(alias.as_deref());
                    }
                }
                Ok(None)
            }
            Relational::Except { .. }
            | Relational::UnionAll { .. }
            | Relational::Values { .. }
            | Relational::ValuesRow { .. } => Ok(None),
        }
    }

    /// Sets new scan name to relational node.
    ///
    /// # Errors
    /// - relational node is not a scan.
    pub fn set_scan_name(&mut self, name: Option<String>) -> Result<(), SbroadError> {
        match self {
            Relational::ScanRelation { ref mut alias, .. }
            | Relational::ScanSubQuery { ref mut alias, .. } => {
                *alias = name;
                Ok(())
            }
            _ => Err(SbroadError::Invalid(
                Entity::Relational,
                Some("Relational node is not a Scan.".into()),
            )),
        }
    }
}

impl Plan {
    /// Adds except node.
    ///
    /// # Errors
    /// - children nodes are not relational
    /// - children tuples are invalid
    /// - children tuples have mismatching structure
    pub fn add_except(&mut self, left: usize, right: usize) -> Result<usize, SbroadError> {
        let child_row_len = |child: usize, plan: &Plan| -> Result<usize, SbroadError> {
            let child_output = plan.get_relation_node(child)?.output();
            Ok(plan
                .get_expression_node(child_output)?
                .get_row_list()?
                .len())
        };

        let left_row_len = child_row_len(left, self)?;
        let right_row_len = child_row_len(right, self)?;
        if left_row_len != right_row_len {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "children tuples have mismatching amount of columns in except node: left {left_row_len}, right {right_row_len}"
            )));
        }

        let output = self.add_row_for_union_except(left, right)?;
        let except = Relational::Except {
            children: vec![left, right],
            output,
        };

        let except_id = self.nodes.push(Node::Relational(except));
        self.replace_parent_in_subtree(output, None, Some(except_id))?;
        Ok(except_id)
    }

    /// Adds insert node.
    ///
    /// # Errors
    /// - Failed to find a target relation.
    pub fn add_insert(
        &mut self,
        relation: &str,
        child: usize,
        columns: &[&str],
        conflict_strategy: ConflictStrategy,
    ) -> Result<usize, SbroadError> {
        let rel = self.relations.get(relation).ok_or_else(|| {
            SbroadError::NotFound(Entity::Table, format!("{relation} among plan relations"))
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
                        return Err(SbroadError::FailedTo(
                            Action::Insert,
                            Some(Entity::Column),
                            format!("system column {name} cannot be inserted"),
                        ))
                    }
                    None => return Err(SbroadError::NotFound(Entity::Column, (*name).to_string())),
                }
            }
            cols
        };
        let child_rel = self.get_relation_node(child)?;
        let child_output = self.get_expression_node(child_rel.output())?;
        let child_output_list_len = if let Expression::Row { list, .. } = child_output {
            list.len()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("child output is not a Row.".into()),
            ));
        };
        if child_output_list_len != columns.len() {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "invalid number of values: {}. Table {} expects {} column(s).",
                child_output_list_len,
                relation,
                columns.len()
            )));
        }

        let mut refs: Vec<usize> = Vec::with_capacity(rel.columns.len());
        for (pos, col) in rel.columns.iter().enumerate() {
            let r_id = self.nodes.add_ref(None, None, pos, col.r#type.clone());
            let col_alias_id = self.nodes.add_alias(&col.name, r_id)?;
            refs.push(col_alias_id);
        }
        let keys: HashSet<_, RepeatableState> = collection! { rel.shard_key.clone() };
        let dist = Distribution::Segment {
            keys: KeySet::from(keys),
        };
        let output = self.nodes.add_row_of_aliases(refs, Some(dist))?;
        let insert = Node::Relational(Relational::Insert {
            relation: relation.into(),
            columns,
            children: vec![child],
            output,
            conflict_strategy,
        });
        let insert_id = self.nodes.push(insert);
        self.replace_parent_in_subtree(output, None, Some(insert_id))?;
        Ok(insert_id)
    }

    /// Adds a scan node.
    ///
    /// # Errors
    /// - relation is invalid
    pub fn add_scan(&mut self, table: &str, alias: Option<&str>) -> Result<usize, SbroadError> {
        let nodes = &mut self.nodes;

        if let Some(rel) = self.relations.get(table) {
            let mut refs: Vec<usize> = Vec::with_capacity(rel.columns.len());
            for (pos, col) in rel.columns.iter().enumerate() {
                let r_id = nodes.add_ref(None, None, pos, col.r#type.clone());
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
        Err(SbroadError::NotFound(
            Entity::Table,
            format!("{table} among the plan relations"),
        ))
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
        kind: JoinKind,
    ) -> Result<usize, SbroadError> {
        if !self.is_trivalent(condition)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("Condition is not a trivalent expression".into()),
            ));
        }

        // For any child in a relational scan, we need to
        // remove a sharding column from its output with a
        // projection node and wrap the result with a sub-query
        // scan.
        // As a side effect, we also need to update the references
        // to the child's output in the condition expression as
        // we have filtered out the sharding column.
        let mut children: Vec<usize> = Vec::with_capacity(2);
        for (child, join_child) in &[(left, JoinChild::Outer), (right, JoinChild::Inner)] {
            let child_node = self.get_relation_node(*child)?;
            if let Relational::ScanRelation {
                relation, alias, ..
            } = child_node
            {
                // We'll need it later to update the condition expression (borrow checker).
                let table = self.get_relation(relation).ok_or_else(|| {
                    SbroadError::NotFound(Entity::Table, format!("{relation} among plan relations"))
                })?;
                let sharding_column_pos = table.get_bucket_id_position()?;

                // Wrap relation with sub-query scan.
                let scan_name = if let Some(alias_name) = alias {
                    alias_name.clone()
                } else {
                    relation.clone()
                };
                let proj_id = self.add_proj(*child, &[], false)?;
                let sq_id = self.add_sub_query(proj_id, Some(&scan_name))?;
                children.push(sq_id);

                // Update references to the sub-query's output in the condition.
                let mut condition_tree = BreadthFirst::with_capacity(
                    |node| self.nodes.expr_iter(node, false),
                    EXPR_CAPACITY,
                    EXPR_CAPACITY,
                );
                let refs = condition_tree
                    .iter(condition)
                    .filter_map(|(_, id)| {
                        let expr = self.get_expression_node(id).ok();
                        if let Some(Expression::Reference { .. }) = expr {
                            Some(id)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                // we should update ONLY references that refer to current child (left, right)
                let current_target = match join_child {
                    JoinChild::Inner => Some(vec![1_usize]),
                    JoinChild::Outer => Some(vec![0_usize]),
                };
                for ref_id in refs {
                    let expr = self.get_mut_expression_node(ref_id)?;
                    if let Expression::Reference {
                        position, targets, ..
                    } = expr
                    {
                        if current_target == *targets && *position > sharding_column_pos {
                            *position -= 1;
                        }
                    }
                }
            } else {
                children.push(*child);
            }
        }
        if let (Some(left_id), Some(right_id)) = (children.first(), children.get(1)) {
            let output = self.add_row_for_join(*left_id, *right_id)?;
            let join = Relational::Join {
                children: vec![*left_id, *right_id],
                condition,
                output,
                kind,
            };

            let join_id = self.nodes.push(Node::Relational(join));
            self.replace_parent_in_subtree(condition, None, Some(join_id))?;
            self.replace_parent_in_subtree(output, None, Some(join_id))?;
            return Ok(join_id);
        }
        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("invalid children for join".to_string()),
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
    ) -> Result<usize, SbroadError> {
        let alias = if let Node::Relational(rel) = self.get_node(child_id)? {
            rel.scan_name(self, 0)?.map(String::from)
        } else {
            return Err(SbroadError::Invalid(Entity::Relational, None));
        };

        let output = self.add_row_for_output(child_id, &[], true)?;
        match policy {
            MotionPolicy::Local => {
                return Err(SbroadError::Invalid(
                    Entity::Motion,
                    Some(format!(
                        "add_motion: got MotionPolicy::Local for child_id: {child_id}"
                    )),
                ))
            }
            MotionPolicy::Segment(key) | MotionPolicy::LocalSegment(key) => {
                if let Ok(keyset) = KeySet::try_from(key) {
                    self.set_dist(output, Distribution::Segment { keys: keyset })?;
                } else {
                    self.set_dist(output, Distribution::Any)?;
                }
            }
            MotionPolicy::Full => {
                self.set_const_dist(output)?;
            }
        }
        self.set_const_dist(output)?;

        let child = self.get_relation_node(child_id)?;
        let is_child_subquery = matches!(child, Relational::ScanSubQuery { .. });

        let motion = Relational::Motion {
            alias,
            children: vec![child_id],
            policy: policy.clone(),
            output,
            is_child_subquery,
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
        is_distinct: bool,
    ) -> Result<usize, SbroadError> {
        let output = self.add_row_for_output(child, col_names, false)?;
        let proj = Relational::Projection {
            children: vec![child],
            output,
            is_distinct,
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
        is_distinct: bool,
    ) -> Result<usize, SbroadError> {
        let output = self.nodes.add_row_of_aliases(columns.to_vec(), None)?;
        let proj = Relational::Projection {
            children: vec![child],
            output,
            is_distinct,
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
    pub fn add_select(&mut self, children: &[usize], filter: usize) -> Result<usize, SbroadError> {
        let first_child: usize = match children.len() {
            0 => {
                return Err(SbroadError::UnexpectedNumberOfValues(
                    "children list is empty".into(),
                ))
            }
            _ => children[0],
        };

        if !self.is_trivalent(filter)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("filter expression is not a trivalent expression.".into()),
            ));
        }

        for child in children {
            if let Node::Relational(_) = self.get_node(*child)? {
            } else {
                return Err(SbroadError::Invalid(Entity::Relational, None));
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

    /// Adds having node
    ///
    /// # Errors
    /// - children list is empty
    /// - filter expression is not boolean
    /// - children nodes are not relational
    /// - first child output tuple is not valid
    pub fn add_having(&mut self, children: &[usize], filter: usize) -> Result<usize, SbroadError> {
        let first_child: usize = match children.len() {
            0 => {
                return Err(SbroadError::UnexpectedNumberOfValues(
                    "children list is empty".into(),
                ))
            }
            _ => children[0],
        };

        if !self.is_trivalent(filter)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("filter expression is not a trivalent expression.".into()),
            ));
        }

        for child in children {
            if let Node::Relational(_) = self.get_node(*child)? {
            } else {
                return Err(SbroadError::Invalid(Entity::Relational, None));
            }
        }

        let output = self.add_row_for_output(first_child, &[], true)?;
        let having = Relational::Having {
            children: children.into(),
            filter,
            output,
        };

        let having_id = self.nodes.push(Node::Relational(having));
        self.replace_parent_in_subtree(filter, None, Some(having_id))?;
        self.replace_parent_in_subtree(output, None, Some(having_id))?;
        Ok(having_id)
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
    ) -> Result<usize, SbroadError> {
        let name: Option<String> = alias.map(String::from);

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
    pub fn add_union_all(&mut self, left: usize, right: usize) -> Result<usize, SbroadError> {
        let child_row_len = |child: usize, plan: &Plan| -> Result<usize, SbroadError> {
            let child_output = plan.get_relation_node(child)?.output();
            Ok(plan
                .get_expression_node(child_output)?
                .get_row_list()?
                .len())
        };

        let left_row_len = child_row_len(left, self)?;
        let right_row_len = child_row_len(right, self)?;
        if left_row_len != right_row_len {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "children tuples have mismatching amount of columns in union all node: left {left_row_len}, right {right_row_len}"
            )));
        }

        let output = self.add_row_for_union_except(left, right)?;
        let union_all = Relational::UnionAll {
            children: vec![left, right],
            output,
        };

        let union_all_id = self.nodes.push(Node::Relational(union_all));
        self.replace_parent_in_subtree(output, None, Some(union_all_id))?;
        Ok(union_all_id)
    }

    /// Adds a values row node.
    ///
    /// # Errors
    /// - Row node is not of a row type
    pub fn add_values_row(
        &mut self,
        row_id: usize,
        col_idx: &mut usize,
    ) -> Result<usize, SbroadError> {
        let row = self.get_expression_node(row_id)?;
        let columns = row.clone_row_list()?;
        let mut aliases: Vec<usize> = Vec::with_capacity(columns.len());
        for col_id in columns {
            // Generate a row of aliases for the incoming row.
            *col_idx += 1;
            // The column names are generated according to tarantool naming of anonymous columns
            let name = format!("\"COLUMN_{col_idx}\"");
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
    pub fn add_values(&mut self, value_rows: Vec<usize>) -> Result<usize, SbroadError> {
        // In case we have several `ValuesRow` under `Values`
        // (e.g. VALUES (1, "test_1"), (2, "test_2")),
        // the list of alias column names for it will look like:
        // (COLUMN_1, COLUMN_2), (COLUMN_3, COLUMN_4).
        // As soon as we want to assign name for column and not for the specific value,
        // we choose the names of last `ValuesRow` and set them as names of all the columns of `Values`.
        // The assumption always is that the child `ValuesRow` has the same number of elements.
        let last_id = if let Some(last_id) = value_rows.last() {
            *last_id
        } else {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Values node has no children, expected at least one child.".into(),
            ));
        };
        let value_row_last = self.get_relation_node(last_id)?;
        let last_output_id = if let Relational::ValuesRow { output, .. } = value_row_last {
            *output
        } else {
            return Err(SbroadError::UnexpectedNumberOfValues(
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
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("Expected an alias".into()),
                    ));
                }
            }
            aliases
        } else {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Values node must have at least one child row.".into(),
            ));
        };

        // Generate a row of aliases referencing all the children.
        let mut aliases: Vec<usize> = Vec::with_capacity(names.len());
        let columns = last_output.clone_row_list()?;
        for (pos, name) in names.iter().enumerate() {
            let col_id = *columns.get(pos).ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(format!(
                    "Values node has no column at position {pos}"
                ))
            })?;
            let col_expr = self.get_expression_node(col_id)?;
            let col_type = col_expr.calculate_type(self)?;
            let ref_id = self.nodes.add_ref(
                None,
                Some((0..value_rows.len()).collect::<Vec<usize>>()),
                pos,
                col_type,
            );
            let alias_id = self.nodes.add_alias(name, ref_id)?;
            aliases.push(alias_id);
        }
        let output = self.nodes.add_row(aliases, None);

        let values = Relational::Values {
            output,
            children: value_rows,
        };
        let values_id = self.nodes.push(Node::Relational(values));
        self.replace_parent_in_subtree(output, None, Some(values_id))?;
        Ok(values_id)
    }

    /// Gets an output tuple from relational node id
    ///
    /// # Errors
    /// - node is not relational
    pub fn get_relational_output(&self, rel_id: usize) -> Result<usize, SbroadError> {
        if let Node::Relational(rel) = self.get_node(rel_id)? {
            Ok(rel.output())
        } else {
            Err(SbroadError::Invalid(Entity::Relational, None))
        }
    }

    /// Gets list of aliases in output tuple of `rel_id`
    ///
    /// # Errors
    /// - node is not relational
    /// - output is not `Expression::Row`
    /// - any node in the output tuple is not `Expression::Alias`
    pub fn get_relational_aliases(&self, rel_id: usize) -> Result<Vec<String>, SbroadError> {
        let output = self.get_relational_output(rel_id)?;
        if let Expression::Row { list, .. } = self.get_expression_node(output)? {
            return list
                .iter()
                .map(|alias_id| {
                    self.get_expression_node(*alias_id)?
                        .get_alias_name()
                        .map(std::string::ToString::to_string)
                })
                .collect::<Result<Vec<String>, SbroadError>>();
        }
        return Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!(
                "expected output of Relational node {rel_id} to be Row"
            )),
        ));
    }

    /// Gets children from relational node.
    ///
    /// # Errors
    /// - node is not relational
    pub fn get_relational_children(&self, rel_id: usize) -> Result<Option<&[usize]>, SbroadError> {
        if let Node::Relational(rel) = self.get_node(rel_id)? {
            Ok(rel.children())
        } else {
            Err(SbroadError::Invalid(
                Entity::Node,
                Some("invalid relational node".into()),
            ))
        }
    }

    /// Gets child with specified index
    ///
    /// # Errors
    /// - node is not relational
    /// - node does not have child with specified index
    pub fn get_relational_child(
        &self,
        rel_id: usize,
        child_idx: usize,
    ) -> Result<usize, SbroadError> {
        if let Node::Relational(rel) = self.get_node(rel_id)? {
            let children = rel.children();
            return if let Some(children) = children {
                let child_id = *children.get(child_idx).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Relational,
                        Some(format!(
                            "rel node {rel:?} has no child with idx ({child_idx})"
                        )),
                    )
                })?;
                Ok(child_id)
            } else {
                Err(SbroadError::Invalid(
                    Entity::Relational,
                    Some(format!("rel node {rel:?} has no children. Id: ({rel_id})")),
                ))
            };
        }
        Err(SbroadError::NotFound(
            Entity::Relational,
            format!("with id ({rel_id})"),
        ))
    }

    /// Synchronize values row output with the data tuple after parameter binding.
    ///
    /// # Errors
    /// - Node is not values row
    /// - Output and data tuples have different number of columns
    /// - Output is not a row of aliases
    pub fn update_values_row(&mut self, id: usize) -> Result<(), SbroadError> {
        let values_row = self.get_node(id)?;
        let (output_id, data_id) =
            if let Node::Relational(Relational::ValuesRow { output, data, .. }) = values_row {
                (*output, *data)
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some(format!("Expected a values row: {values_row:?}")),
                ));
            };
        let data = self.get_expression_node(data_id)?;
        let data_list = data.clone_row_list()?;
        let output = self.get_expression_node(output_id)?;
        let output_list = output.clone_row_list()?;
        for (pos, alias_id) in output_list.iter().enumerate() {
            let new_child_id = *data_list
                .get(pos)
                .ok_or_else(|| SbroadError::NotFound(Entity::Node, format!("at position {pos}")))?;
            let alias = self.get_mut_expression_node(*alias_id)?;
            if let Expression::Alias { ref mut child, .. } = alias {
                *child = new_child_id;
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some(format!("expected an alias: {alias:?}")),
                ));
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
    ) -> Result<(), SbroadError> {
        if let Node::Relational(ref mut rel) =
            self.nodes.arena.get_mut(rel_id).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    format!("(mutable) from arena with index {rel_id}"),
                )
            })?
        {
            rel.set_children(children)?;
            Ok(())
        } else {
            Err(SbroadError::Invalid(Entity::Relational, None))
        }
    }

    /// Checks if the node is an additional child of some relational node.
    ///
    /// # Errors
    /// - Failed to get plan top
    /// - Node returned by the relational iterator is not relational (bug)
    pub fn is_additional_child(&self, node_id: usize) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        let mut rel_tree = BreadthFirst::with_capacity(
            |node| self.nodes.rel_iter(node),
            REL_CAPACITY,
            REL_CAPACITY,
        );
        for (_, id) in rel_tree.iter(top_id) {
            let rel = self.get_relation_node(id)?;
            match rel {
                Relational::Selection { children, .. } | Relational::Having { children, .. } => {
                    if children.iter().skip(1).any(|&c| c == node_id) {
                        return Ok(true);
                    }
                }
                Relational::Join { children, .. } => {
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
    ) -> Result<bool, SbroadError> {
        let Some(children) = self.get_relational_children(rel_id)? else {
            return Ok(false);
        };
        match self.get_relation_node(rel_id)? {
            Relational::Selection { .. }
            | Relational::Projection { .. }
            | Relational::Having { .. } => Ok(children.first() != Some(&sq_id)),
            Relational::Join { .. } => {
                Ok(children.first() != Some(&sq_id) && children.get(1) != Some(&sq_id))
            }
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests;
