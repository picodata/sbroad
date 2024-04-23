//! Tuple operators module.
//!
//! Contains operator nodes that transform the tuples in IR tree.

use crate::frontend::sql::get_unnamed_column_alias;
use ahash::RandomState;

use crate::collection;
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::borrow::BorrowMut;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use crate::errors::{Action, Entity, SbroadError};

use super::api::children::{Children, MutChildren};
use super::expression::{ColumnPositionMap, Expression};
use super::transformation::redistribution::{MotionPolicy, Program};
use super::tree::traversal::{PostOrderWithFilter, EXPR_CAPACITY};
use super::{Node, NodeId, Plan};
use crate::ir::distribution::{Distribution, Key, KeySet};
use crate::ir::expression::{ExpressionId, PlanExpr};
use crate::ir::helpers::RepeatableState;
use crate::ir::relation::{Column, ColumnRole};
use crate::ir::transformation::redistribution::{ColumnPosition, JoinChild};

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
    /// `||`
    Or,
    /// `between`
    Between,
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
            Bool::Between => unreachable!("Between in op fmt"),
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
    /// `not`
    Not,
    /// `is null`
    IsNull,
    /// `exists`
    Exists,
}

impl Unary {
    /// Creates `Unary` from the operator string.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    pub fn from(s: &str) -> Result<Self, SbroadError> {
        match s.to_lowercase().as_str() {
            "is null" => Ok(Unary::IsNull),
            "exists" => Ok(Unary::Exists),
            _ => Err(SbroadError::Invalid(
                Entity::Operator,
                Some(format_smolstr!(
                    "expected `is null` or `is not null`, got unary operator `{s}`"
                )),
            )),
        }
    }
}

impl Display for Unary {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let op = match &self {
            Unary::Not => "not",
            Unary::IsNull => "is null",
            Unary::Exists => "exists",
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

/// Strategy applied on INSERT execution.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize, Default)]
pub enum ConflictStrategy {
    /// Swallow the error, do not insert the conflicting tuple
    DoNothing,
    /// Replace the conflicting tuple with the new one
    DoReplace,
    /// Throw the error, no tuples will be inserted for this
    /// storage. But for other storages the insertion may be successful.
    #[default]
    DoFail,
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

/// Execution strategy for update node.
///
/// Depending on whether some sharding column
/// is updated or not, the update will be executed
/// differently.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum UpdateStrategy {
    /// Strategy when some sharding column is updated.
    /// When some sharding column is changed, it changes
    /// the bucket_id of the tuple. When this happens
    /// tuple may be needed to relocated on the other
    /// replicaset. This works as first selecting needed
    /// tuples, redistributing them and then deletion and insertion
    /// in the same local transaction on each storage.
    ///
    /// # Details
    /// Update works as follows:
    /// 1. Projection below update selects whole table tuple
    /// with updated columns (but without bucket_id) and
    /// old values for sharding columns.
    /// 2. Then on the router each tuple is transformed into two
    /// tuples (one for insertion and one for deletion):
    ///     * old values of sharding columns are popped out from original tuple
    ///       and used to calculate the bucket_id for deletion tuple. The original
    ///       tuple becomes the tuple for insertion.
    ///     * deletion tuple is created from primary keys of insertion tuple.
    ///     * bucket_id for insertion tuple is calculated using new shard key values.
    /// 3. Because pk key can't be updated and insertion tuple contains
    /// primary key + at least 1 sharding column and deletion tuple consists only from primary key
    /// => len of insertion tuple > len of deletion tuple.
    /// This invariant will be used to distinguish between these tuples on the storage (tuples are
    /// stored in the same table, because currently whole sbroad assumes that motion
    /// produces only one table). The len of deletion tuple is saved in this struct
    /// variant
    /// 4. On storages the table is traversed and in transaction first tuples are deleted,
    /// then insertion tuples are inserted.
    ShardedUpdate { delete_tuple_len: Option<usize> },
    /// Strategy when no sharding column is updated.
    ///
    /// In this case because join/selection does not change
    /// the distribution of data, update can be performed
    /// locally, without any data motion. NB: this may
    /// change if join children are reordered (update table scan
    /// is not inner child) or join conflict resolution changes.
    ///
    /// Projection below update has the following structure:
    /// ```sql
    /// select upd_expr1, .., upd_expr, pk_col1, .., pk_col
    /// ```
    /// If some expressions are the same, the column is reused.
    ///
    /// # Example
    /// ```sql
    /// update t set
    /// a = c + d
    /// b = c + d
    /// c = d
    /// ```
    /// Table t has pk columns `d, e`.
    /// Then the following projection will be created:
    /// ```sql
    /// select c + d, d, e
    /// ```
    LocalUpdate,
}

#[derive(Clone, Deserialize, Debug, PartialEq, Eq, Serialize)]
pub enum OrderByEntity {
    Expression { expr_id: usize },
    Index { value: usize },
}

#[derive(Clone, Deserialize, Debug, PartialEq, Eq, Serialize)]
pub enum OrderByType {
    Asc,
    Desc,
}

impl Display for OrderByType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OrderByType::Asc => write!(f, "asc"),
            OrderByType::Desc => write!(f, "desc"),
        }
    }
}

#[derive(Clone, Deserialize, Debug, PartialEq, Eq, Serialize)]
pub struct OrderByElement {
    pub entity: OrderByEntity,
    pub order_type: Option<OrderByType>,
}

/// Relational algebra operator returning a new tuple.
///
/// Transforms input tuple(s) into the output one using the
/// relation algebra logic.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Relational {
    ScanCte {
        /// CTE's name.
        alias: SmolStr,
        /// Contains exactly one single element (projection node index).
        child: usize,
        /// An output tuple with aliases.
        output: usize,
    },
    Except {
        /// Left child id
        left: usize,
        /// Right child id
        right: usize,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
    },
    Delete {
        /// Relation name.
        relation: SmolStr,
        /// Contains exactly one single element.
        children: Vec<usize>,
        /// The output tuple (reserved for `delete returning`).
        output: usize,
    },
    Insert {
        /// Relation name.
        relation: SmolStr,
        /// Target column positions for data insertion from
        /// the child's tuple.
        columns: Vec<usize>,
        /// Contains exactly one single element.
        children: Vec<usize>,
        /// The output tuple (reserved for `insert returning`).
        output: usize,
        /// What to do in case there is a conflict during insert on storage
        conflict_strategy: ConflictStrategy,
    },
    Intersect {
        left: usize,
        right: usize,
        // id of the output tuple
        output: usize,
    },
    Update {
        /// Relation name.
        relation: SmolStr,
        /// Children ids. Update has exactly one child.
        children: Vec<usize>,
        /// Maps position of column being updated in table to corresponding position
        /// in `Projection` below `Update`.
        ///
        /// For sharded `Update`, it will contain every table column except `bucket_id`
        /// column. For local `Update` it will contain only update table columns.
        update_columns_map: HashMap<ColumnPosition, ColumnPosition, RepeatableState>,
        /// How this update must be executed.
        strategy: UpdateStrategy,
        /// Positions of primary columns in `Projection`
        /// below `Update`.
        pk_positions: Vec<ColumnPosition>,
        /// Output id.
        output: usize,
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
        alias: Option<SmolStr>,
        /// Contains exactly one single element: child node index
        /// from the plan node arena.
        children: Vec<usize>,
        /// Motion policy - the amount of data to be moved.
        policy: MotionPolicy,
        /// A sequence of opcodes that transform the data.
        program: Program,
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
        alias: Option<SmolStr>,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
        /// Relation name.
        relation: SmolStr,
    },
    ScanSubQuery {
        /// SubQuery name.
        alias: Option<SmolStr>,
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
    OrderBy {
        child: usize,
        output: usize,
        order_by_elements: Vec<OrderByElement>,
    },
    UnionAll {
        /// Left child id
        left: usize,
        /// Right child id
        right: usize,
        /// Outputs tuple node index in the plan node arena.
        output: usize,
    },
    Union {
        /// Left child id
        left: usize,
        /// Right child id
        right: usize,
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
    /// Gets an immutable id of the output tuple node of the plan's arena.
    #[must_use]
    pub fn output(&self) -> usize {
        match self {
            Relational::ScanCte { output, .. }
            | Relational::Except { output, .. }
            | Relational::GroupBy { output, .. }
            | Relational::OrderBy { output, .. }
            | Relational::Having { output, .. }
            | Relational::Update { output, .. }
            | Relational::Join { output, .. }
            | Relational::Delete { output, .. }
            | Relational::Insert { output, .. }
            | Relational::Intersect { output, .. }
            | Relational::Motion { output, .. }
            | Relational::Projection { output, .. }
            | Relational::ScanRelation { output, .. }
            | Relational::ScanSubQuery { output, .. }
            | Relational::Selection { output, .. }
            | Relational::Union { output, .. }
            | Relational::UnionAll { output, .. }
            | Relational::Values { output, .. }
            | Relational::ValuesRow { output, .. } => *output,
        }
    }

    /// Gets an immutable reference to the output tuple node id.
    #[must_use]
    pub fn mut_output(&mut self) -> &mut usize {
        match self {
            Relational::ScanCte { output, .. }
            | Relational::Except { output, .. }
            | Relational::GroupBy { output, .. }
            | Relational::OrderBy { output, .. }
            | Relational::Update { output, .. }
            | Relational::Having { output, .. }
            | Relational::Join { output, .. }
            | Relational::Delete { output, .. }
            | Relational::Insert { output, .. }
            | Relational::Intersect { output, .. }
            | Relational::Motion { output, .. }
            | Relational::Projection { output, .. }
            | Relational::ScanRelation { output, .. }
            | Relational::ScanSubQuery { output, .. }
            | Relational::Selection { output, .. }
            | Relational::Union { output, .. }
            | Relational::UnionAll { output, .. }
            | Relational::Values { output, .. }
            | Relational::ValuesRow { output, .. } => output,
        }
    }

    // Gets an immutable reference to the children nodes.
    #[must_use]
    pub fn children(&self) -> Children<'_> {
        match self {
            Relational::OrderBy { child, .. } | Relational::ScanCte { child, .. } => {
                Children::Single(child)
            }
            Relational::Except { left, right, .. }
            | Relational::Intersect { left, right, .. }
            | Relational::UnionAll { left, right, .. }
            | Relational::Union { left, right, .. } => Children::Couple(left, right),
            Relational::GroupBy { children, .. }
            | Relational::Update { children, .. }
            | Relational::Join { children, .. }
            | Relational::Having { children, .. }
            | Relational::Delete { children, .. }
            | Relational::Insert { children, .. }
            | Relational::Motion { children, .. }
            | Relational::Projection { children, .. }
            | Relational::ScanSubQuery { children, .. }
            | Relational::Selection { children, .. }
            | Relational::ValuesRow { children, .. }
            | Relational::Values { children, .. } => Children::Many(children),
            Relational::ScanRelation { .. } => Children::None,
        }
    }

    // Gets a mutable reference to the children nodes.
    #[must_use]
    pub fn mut_children(&mut self) -> MutChildren<'_> {
        // return MutChildren { node: self };
        match self {
            Relational::OrderBy { child, .. } | Relational::ScanCte { child, .. } => {
                MutChildren::Single(child)
            }
            Relational::Except { left, right, .. }
            | Relational::Intersect { left, right, .. }
            | Relational::UnionAll { left, right, .. }
            | Relational::Union { left, right, .. } => MutChildren::Couple(left, right),
            Relational::GroupBy {
                ref mut children, ..
            }
            | Relational::Update {
                ref mut children, ..
            }
            | Relational::Having {
                ref mut children, ..
            }
            | Relational::Join {
                ref mut children, ..
            }
            | Relational::Delete {
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
            | Relational::ValuesRow {
                ref mut children, ..
            }
            | Relational::Values {
                ref mut children, ..
            } => MutChildren::Many(children),
            Relational::ScanRelation { .. } => MutChildren::None,
        }
    }

    /// Checks if the node is deletion.
    #[must_use]
    pub fn is_delete(&self) -> bool {
        matches!(self, Relational::Delete { .. })
    }
    /// Checks if the node is an insertion.
    #[must_use]
    pub fn is_insert(&self) -> bool {
        matches!(self, Relational::Insert { .. })
    }

    /// Checks if the node is dml node
    #[must_use]
    pub fn is_dml(&self) -> bool {
        matches!(
            self,
            Relational::Insert { .. } | Relational::Update { .. } | Relational::Delete { .. }
        )
    }

    /// Checks that the node is a motion.
    #[must_use]
    pub fn is_motion(&self) -> bool {
        matches!(self, &Relational::Motion { .. })
    }

    /// Checks that the node is a sub-query or CTE scan.
    #[must_use]
    pub fn is_subquery_or_cte(&self) -> bool {
        matches!(
            self,
            &Relational::ScanSubQuery { .. } | &Relational::ScanCte { .. }
        )
    }

    /// Sets new children to relational node.
    ///
    /// # Panics
    /// - wrong number of children for the given node
    pub fn set_children(&mut self, children: Vec<usize>) {
        match self {
            Relational::Join {
                children: ref mut old,
                ..
            }
            | Relational::Delete {
                children: ref mut old,
                ..
            }
            | Relational::Update {
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
            }
            Relational::Except { left, right, .. }
            | Relational::UnionAll { left, right, .. }
            | Relational::Intersect { left, right, .. }
            | Relational::Union { left, right, .. } => {
                if children.len() != 2 {
                    unreachable!("Node has only two children!");
                }
                *left = children[0];
                *right = children[1];
            }
            Relational::OrderBy { ref mut child, .. } => {
                if children.len() != 1 {
                    unreachable!("ORDER BY may have only a single relational child");
                }
                // It is safe to unwrap here, because the length is already checked above.
                *child = children[0];
            }
            Relational::ScanCte { ref mut child, .. } => {
                if children.len() != 1 {
                    unreachable!("CTE may have only a single relational child");
                }
                // It is safe to unwrap here, because the length is already checked above.
                *child = children[0];
            }
            Relational::ScanRelation { .. } => {
                assert!(children.is_empty(), "scan must have no children!");
            }
        }
    }

    /// Get relational Scan name that given `output_alias_position` (`Expression::Alias`)
    /// references to.
    ///
    /// # Errors
    /// - plan tree is invalid (failed to retrieve child nodes)
    pub fn scan_name<'n>(
        &'n self,
        plan: &'n Plan,
        output_alias_position: usize,
    ) -> Result<Option<&'n str>, SbroadError> {
        match self {
            Relational::Insert { relation, .. } | Relational::Delete { relation, .. } => {
                Ok(Some(relation.as_str()))
            }
            Relational::ScanRelation {
                alias, relation, ..
            } => Ok(alias.as_deref().or(Some(relation.as_str()))),
            Relational::Projection { .. }
            | Relational::GroupBy { .. }
            | Relational::OrderBy { .. }
            | Relational::Intersect { .. }
            | Relational::Having { .. }
            | Relational::Selection { .. }
            | Relational::Update { .. }
            | Relational::Join { .. } => {
                let output_row = plan.get_expression_node(self.output())?;
                let list = output_row.get_row_list()?;
                let col_id = *list.get(output_alias_position).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Column,
                        format_smolstr!(
                            "at position {output_alias_position} of Row, {self:?}, {output_row:?}"
                        ),
                    )
                })?;
                let col_node = plan.get_expression_node(col_id)?;
                if let Expression::Alias { child, .. } = col_node {
                    let child_node = plan.get_expression_node(*child)?;
                    if let Expression::Reference { position: pos, .. } = child_node {
                        let rel_id = *plan.get_relational_from_reference_node(*child)?;
                        let rel_node = plan.get_relation_node(rel_id)?;
                        if rel_node == self {
                            return Err(SbroadError::DuplicatedValue(format_smolstr!(
                                "Reference to the same node {rel_node:?} at position {output_alias_position}"
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
            Relational::ScanCte { alias, .. } => Ok(Some(alias)),
            Relational::ScanSubQuery { alias, .. } | Relational::Motion { alias, .. } => {
                if let Some(name) = alias.as_ref() {
                    if !name.is_empty() {
                        return Ok(alias.as_deref());
                    }
                }
                Ok(None)
            }
            Relational::Except { .. }
            | Relational::Union { .. }
            | Relational::UnionAll { .. }
            | Relational::Values { .. }
            | Relational::ValuesRow { .. } => Ok(None),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Relational::Except { .. } => "Except",
            Relational::Delete { .. } => "Delete",
            Relational::Insert { .. } => "Insert",
            Relational::Intersect { .. } => "Intersect",
            Relational::Update { .. } => "Update",
            Relational::Join { .. } => "Join",
            Relational::Motion { .. } => "Motion",
            Relational::Projection { .. } => "Projection",
            Relational::ScanCte { .. } => "CTE",
            Relational::ScanRelation { .. } => "Scan",
            Relational::ScanSubQuery { .. } => "Subquery",
            Relational::Selection { .. } => "Selection",
            Relational::GroupBy { .. } => "GroupBy",
            Relational::OrderBy { .. } => "OrderBy",
            Relational::Having { .. } => "Having",
            Relational::Union { .. } => "Union",
            Relational::UnionAll { .. } => "UnionAll",
            Relational::Values { .. } => "Values",
            Relational::ValuesRow { .. } => "ValuesRow",
        }
    }

    /// Sets new scan name to relational node.
    ///
    /// # Errors
    /// - relational node is not a scan.
    ///
    /// # Panics
    /// - CTE must have a name.
    pub fn set_scan_name(&mut self, name: Option<SmolStr>) -> Result<(), SbroadError> {
        match self {
            Relational::ScanRelation { ref mut alias, .. }
            | Relational::ScanSubQuery { ref mut alias, .. } => {
                *alias = name;
                Ok(())
            }
            Relational::ScanCte { ref mut alias, .. } => {
                let name = name.expect("CTE must have a name");
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
    /// Add relational node to plan arena and update shard columns info.
    ///
    /// # Errors
    /// - failed to oupdate shard columns info due to invalid plan subtree
    pub fn add_relational(&mut self, node: Relational) -> Result<usize, SbroadError> {
        let rel_id = self.nodes.push(Node::Relational(node));
        let mut context = self.context_mut();
        context.shard_col_info.update_node(rel_id, self)?;
        Ok(rel_id)
    }

    /// Adds delete node.
    ///
    /// # Errors
    /// - child id pointes to non-existing or non-relational node.
    pub fn add_delete(&mut self, table: SmolStr, child_id: usize) -> Result<usize, SbroadError> {
        let output = self.add_row_for_output(child_id, &[], true)?;
        let delete = Relational::Delete {
            relation: table,
            children: vec![child_id],
            output,
        };
        let delete_id = self.add_relational(delete)?;
        self.replace_parent_in_subtree(output, None, Some(delete_id))?;
        Ok(delete_id)
    }

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
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "children tuples have mismatching amount of columns in except node: left {left_row_len}, right {right_row_len}"
            )));
        }

        let output = self.add_row_for_union_except(left, right)?;
        let except = Relational::Except {
            left,
            right,
            output,
        };

        let except_id = self.add_relational(except)?;
        self.replace_parent_in_subtree(output, None, Some(except_id))?;
        Ok(except_id)
    }

    /// Add `Update` relational node.
    ///
    /// This function first looks whether some sharding column is
    /// updated and then creates `Projection` and `Update` nodes
    /// according to that. For details, see [`UpdateStrategy`].
    ///
    /// # Projection format
    /// For sharded update:
    /// ```text
    /// table_tuple, old_shard_key
    /// ```
    /// `table_tuple` consists from table columns in the same order but:
    /// 1. If column is updated, it is replaced with corresponding update expression
    /// 2. `bucket_id` column is skipped
    /// For example:
    /// ```text
    /// t: a b bucket_id c
    /// shard_key: b c
    /// pk: a b
    ///
    /// update t set c = 20
    /// projection: a, b, 20, b, c
    /// ```
    ///
    /// For local update:
    /// ```text
    /// update_exprs, pk_exprs
    /// ```
    /// All expressions are unique. Therefore `pk_exprs`
    /// contains expression not present in `update_exprs`.
    /// Example:
    /// ```text
    /// t: a b c d bucket_id
    /// shard_key: a
    /// pk: a
    /// update t set
    /// c = a,
    /// b = a + 1
    /// c = a + 1
    ///             upd_exprs pk_exprs
    /// projection: a,   a+1
    /// ```
    /// Note that here `pk_expr` are empty, because all pk columns are already
    /// present in `upd_exps`.
    ///
    ///
    /// # Arguments
    /// * `update_defs` - mapping between column position in table
    /// and corresponding update expression.
    /// * `relation` - name of the table being updated.
    /// * `rel_child_id` - id of `Update` child
    ///
    /// # Errors
    /// - invalid update table
    /// - invalid table columns positions in `update_defs`
    #[allow(clippy::too_many_lines)]
    pub fn add_update(
        &mut self,
        relation: &str,
        update_defs: &HashMap<ColumnPosition, ExpressionId, RepeatableState>,
        rel_child_id: usize,
    ) -> Result<usize, SbroadError> {
        // Create Reference node from given table column.
        fn create_ref_from_column(
            plan: &mut Plan,
            relation: &str,
            table_position_map: &HashMap<ColumnPosition, ColumnPosition>,
            col_pos: usize,
        ) -> Result<usize, SbroadError> {
            let table = plan.get_relation_or_error(relation)?;
            let col: &Column = table.columns.get(col_pos).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Table,
                    Some(format_smolstr!(
                        "expected to have at least {} columns",
                        col_pos + 1
                    )),
                )
            })?;
            let output_pos = *table_position_map.get(&col_pos).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "Expected {} column in update child output",
                        &col.name
                    )),
                )
            })?;
            let col_type = col.r#type.clone();
            let node = Expression::Reference {
                // It will be updated using `replace_parent_in_subtree`
                // in the end of the function.
                parent: None,
                targets: Some(vec![0]),
                position: output_pos,
                col_type,
            };
            let id = plan.nodes.push(Node::Expression(node));
            Ok(id)
        }

        let table = self.get_relation_or_error(relation)?;
        // is shard key column updated
        let is_sharded_update = !table.is_global()
            && table
                .get_sk()?
                .iter()
                .any(|col| update_defs.contains_key(col));
        // Columns of Projection that will be created
        let mut projection_cols: Vec<usize> = Vec::with_capacity(update_defs.len());
        // Positions of columns in Projection that constitute the primary key
        let mut primary_key_positions: Vec<usize> =
            Vec::with_capacity(table.primary_key.positions.len());
        // Maps position in table of column being updated to corresponding position in Projection
        let mut update_columns_map =
            HashMap::with_capacity_and_hasher(update_defs.len(), RepeatableState);
        // Helper map between table column position and corresponding column position in child's output
        let child_map = self.table_position_map(relation, rel_child_id)?;

        let update_kind = if is_sharded_update {
            // For sharded Update Projection has the following format:
            // table_tuple , sharding_key_columns
            // table_tuple is without bucket_id column

            // Calculate primary key positions in table_tuple
            if let Some(bucket_id_pos) = table.get_bucket_id_position()? {
                table.primary_key.positions.iter().for_each(|pos| {
                    if *pos < bucket_id_pos {
                        primary_key_positions.push(*pos);
                    } else {
                        primary_key_positions.push(*pos - 1);
                    }
                });
            } else {
                primary_key_positions.extend(table.primary_key.positions.iter());
            }

            // Create projection columns for table_tuple
            let cols_len = table.columns.len();
            // current projection column position
            let mut proj_pos = 0;
            for table_col in 0..cols_len {
                let column = self.get_relation_column(relation, table_col)?;
                // skip bucket_id
                if let ColumnRole::Sharding = column.role {
                    continue;
                }
                update_columns_map.insert(table_col, proj_pos);
                let expr_id = if let Some(id) = update_defs.get(&table_col) {
                    *id
                } else {
                    create_ref_from_column(self, relation, &child_map, table_col)?
                };
                projection_cols.push(expr_id);
                proj_pos += 1;
            }

            // Create projection columns for sharding_key_columns
            // todo(ars): some sharding column maybe already present in projection_cols
            let table = self.get_relation_or_error(relation)?;
            let shard_key_len = table.get_sk()?.len();
            for i in 0..shard_key_len {
                let table = self.get_relation_or_error(relation)?;
                let col_pos = *table.get_sk()?.get(i).ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid shard col position {i}"
                    ))
                })?;
                let shard_col_expr_id =
                    create_ref_from_column(self, relation, &child_map, col_pos)?;
                projection_cols.push(shard_col_expr_id);
            }
            UpdateStrategy::ShardedUpdate {
                delete_tuple_len: None,
            }
        } else {
            // For local Update, projection has the following format:
            // update_expressions, pk_expressions (not present in update_expressions)

            // Helper map between expression and its position in projection.
            // The logic is that we want to reuse expression calculation in case it's used twice
            // (e.g. `update T set a = some_expr, b = some_expr`).
            let mut expr_to_col_pos: HashMap<PlanExpr, ColumnPosition> =
                HashMap::with_capacity(update_defs.len());
            // Expressions that form primary key of updated table.
            let pk_expressions = table
                .primary_key
                .positions
                .clone()
                .iter()
                .map(|pos| create_ref_from_column(self, relation, &child_map, *pos))
                .collect::<Result<Vec<usize>, SbroadError>>()?;

            let mut pos = 0;
            // Add update_expressions
            for (table_col, expr_id) in update_defs {
                let expr = PlanExpr::new(*expr_id, self);
                match expr_to_col_pos.entry(expr) {
                    Entry::Occupied(o) => {
                        let column_pos = *o.get();
                        update_columns_map.insert(*table_col, column_pos);
                    }
                    Entry::Vacant(v) => {
                        projection_cols.push(*expr_id);
                        update_columns_map.insert(*table_col, pos);
                        v.insert(pos);
                        pos += 1;
                    }
                }
            }

            // Add pk_expressions
            for expr_id in pk_expressions {
                let expr = PlanExpr::new(expr_id, self);
                match expr_to_col_pos.entry(expr) {
                    Entry::Vacant(e) => {
                        projection_cols.push(expr_id);
                        primary_key_positions.push(pos);
                        e.insert(pos);
                        pos += 1;
                    }
                    Entry::Occupied(o) => {
                        let column_pos = *o.get();
                        primary_key_positions.push(column_pos);
                    }
                }
            }
            UpdateStrategy::LocalUpdate
        };

        // Generate aliases to projection expressions, because
        // it is assumed that any projection column always has an alias.
        for (pos, expr_id) in projection_cols.iter_mut().enumerate() {
            let alias = get_unnamed_column_alias(pos);
            let alias_id = self.nodes.push(Node::Expression(Expression::Alias {
                child: *expr_id,
                name: alias,
            }));
            *expr_id = alias_id;
        }
        let proj_output = self.nodes.add_row(projection_cols, None);
        let proj_node = Relational::Projection {
            children: vec![rel_child_id],
            output: proj_output,
            is_distinct: false,
        };
        let proj_id = self.add_relational(proj_node)?;
        self.replace_parent_in_subtree(proj_output, None, Some(proj_id))?;
        let upd_output = self.add_row_for_output(proj_id, &[], false)?;
        let update_node = Relational::Update {
            relation: relation.to_smolstr(),
            pk_positions: primary_key_positions,
            children: vec![proj_id],
            update_columns_map,
            output: upd_output,
            strategy: update_kind,
        };
        let update_id = self.add_relational(update_node)?;
        self.replace_parent_in_subtree(upd_output, None, Some(update_id))?;

        Ok(update_id)
    }

    /// Adds insert node.
    ///
    /// # Errors
    /// - Failed to find a target relation.
    pub fn add_insert(
        &mut self,
        relation: &str,
        child: usize,
        columns: &[SmolStr],
        conflict_strategy: ConflictStrategy,
    ) -> Result<usize, SbroadError> {
        let rel = self.relations.get(relation).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Table,
                format_smolstr!("{relation} among plan relations"),
            )
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
                match names.get(name.as_str()) {
                    Some((&ColumnRole::User, pos)) => cols.push(*pos),
                    Some((&ColumnRole::Sharding, _)) => {
                        return Err(SbroadError::FailedTo(
                            Action::Insert,
                            Some(Entity::Column),
                            format_smolstr!("system column {name} cannot be inserted"),
                        ))
                    }
                    None => {
                        return Err(SbroadError::NotFound(Entity::Column, (*name).to_smolstr()))
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
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("child output is not a Row.".into()),
            ));
        };
        if child_output_list_len != columns.len() {
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
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
        let dist = if rel.is_global() {
            Distribution::Global
        } else {
            let keys: HashSet<_, RepeatableState> =
                collection! { Key::new(rel.get_sk()?.to_vec()) };
            Distribution::Segment {
                keys: KeySet::from(keys),
            }
        };
        let output = self.nodes.add_row(refs, Some(dist));
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

            let output_id = nodes.add_row(refs, None);
            let scan = Relational::ScanRelation {
                output: output_id,
                relation: SmolStr::from(table),
                alias: alias.map(SmolStr::from),
            };

            let scan_id = self.add_relational(scan)?;
            self.replace_parent_in_subtree(output_id, None, Some(scan_id))?;
            return Ok(scan_id);
        }
        Err(SbroadError::NotFound(
            Entity::Table,
            format_smolstr!("{table} among the plan relations"),
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
                let table = self.get_relation_or_error(relation)?;
                let sharding_column_pos = table.get_bucket_id_position()?;
                let mut needs_bucket_id_column = false;

                // Update references to the sub-query's output in the condition.
                let condition_nodes = {
                    let filter = |id| -> bool {
                        matches!(
                            self.get_expression_node(id),
                            Ok(Expression::Reference { .. })
                        )
                    };
                    let mut condition_tree = PostOrderWithFilter::with_capacity(
                        |node| self.nodes.expr_iter(node, false),
                        EXPR_CAPACITY,
                        Box::new(filter),
                    );
                    condition_tree.populate_nodes(condition);
                    condition_tree.take_nodes()
                };
                // We should update ONLY references that refer to current child (left, right)
                let current_target = match join_child {
                    JoinChild::Inner => Some(vec![1_usize]),
                    JoinChild::Outer => Some(vec![0_usize]),
                };
                let mut refs = Vec::with_capacity(condition_nodes.len());
                for (_, id) in condition_nodes {
                    let expr = self.get_expression_node(id)?;
                    if let Expression::Reference {
                        position, targets, ..
                    } = expr
                    {
                        if *targets == current_target {
                            if Some(*position) == sharding_column_pos {
                                needs_bucket_id_column = true;
                            }
                            refs.push(id);
                        }
                    }
                }

                // Wrap relation with sub-query scan.
                let scan_name = if let Some(alias_name) = alias {
                    alias_name.clone()
                } else {
                    relation.clone()
                };
                let proj_id = self.add_proj(*child, &[], false, needs_bucket_id_column)?;
                let sq_id = self.add_sub_query(proj_id, Some(&scan_name))?;
                children.push(sq_id);

                if needs_bucket_id_column {
                    continue;
                }

                if let Some(sharding_column_pos) = sharding_column_pos {
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

            let join_id = self.add_relational(join)?;
            self.replace_parent_in_subtree(condition, None, Some(join_id))?;
            self.replace_parent_in_subtree(output, None, Some(join_id))?;
            return Ok(join_id);
        }
        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("invalid children for join".to_smolstr()),
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
        program: Program,
    ) -> Result<usize, SbroadError> {
        let alias = if let Node::Relational(rel) = self.get_node(child_id)? {
            rel.scan_name(self, 0)?.map(SmolStr::from)
        } else {
            return Err(SbroadError::Invalid(Entity::Relational, None));
        };

        let output = self.add_row_for_output(child_id, &[], true)?;
        match policy {
            MotionPolicy::None => {
                return Err(SbroadError::Invalid(
                    Entity::Motion,
                    Some(format_smolstr!(
                        "add_motion: got MotionPolicy::None for child_id: {child_id}"
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
                self.set_dist(output, Distribution::Global)?;
            }
            MotionPolicy::Local => {
                self.set_dist(output, Distribution::Any)?;
            }
        }

        let child = self.get_relation_node(child_id)?;
        let is_child_subquery = matches!(child, Relational::ScanSubQuery { .. });

        let motion = Relational::Motion {
            alias,
            children: vec![child_id],
            policy: policy.clone(),
            program,
            output,
            is_child_subquery,
        };
        let motion_id = self.add_relational(motion)?;
        self.replace_parent_in_subtree(output, None, Some(motion_id))?;
        let mut context = self.context_mut();
        context
            .shard_col_info
            .borrow_mut()
            .handle_node_insertion(motion_id, self)?;
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
        needs_shard_col: bool,
    ) -> Result<usize, SbroadError> {
        let output = self.add_row_for_output(child, col_names, needs_shard_col)?;
        let proj = Relational::Projection {
            children: vec![child],
            output,
            is_distinct,
        };

        let proj_id = self.add_relational(proj)?;
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
        let output = self.nodes.add_row(columns.to_vec(), None);
        let proj = Relational::Projection {
            children: vec![child],
            output,
            is_distinct,
        };

        let proj_id = self.add_relational(proj)?;
        self.replace_parent_in_subtree(output, None, Some(proj_id))?;
        Ok(proj_id)
    }

    /// Adds `Select` node.
    ///
    /// # Errors
    /// - children list is empty
    /// - filter expression is not boolean
    /// - children nodes are not relational
    /// - first child output tuple is not valid
    ///
    /// # Panics
    /// - `children` is empty
    pub fn add_select(&mut self, children: &[usize], filter: usize) -> Result<usize, SbroadError> {
        let first_child: usize = *children
            .first()
            .expect("No children passed to `add_select`");

        if !self.is_trivalent(filter)? {
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some("filter expression is not a trivalent expression.".into()),
            ));
        }

        let output = self.add_row_for_output(first_child, &[], true)?;
        let select = Relational::Selection {
            children: children.into(),
            filter,
            output,
        };

        let select_id = self.add_relational(select)?;
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

        let having_id = self.add_relational(having)?;
        self.replace_parent_in_subtree(filter, None, Some(having_id))?;
        self.replace_parent_in_subtree(output, None, Some(having_id))?;
        Ok(having_id)
    }

    /// Add `OrderBy` node into the plan.
    ///
    /// # Errors
    /// - Unable to add output row from child.
    /// - Unable to replace parent in subtree.
    ///
    /// # Panics
    /// - Relational node child not found.
    pub fn add_order_by(
        &mut self,
        child: usize,
        order_by_elements: Vec<OrderByElement>,
    ) -> Result<usize, SbroadError> {
        let output = self.add_row_for_output(child, &[], true)?;
        let order_by = Relational::OrderBy {
            child,
            output,
            order_by_elements: order_by_elements.clone(),
        };

        let plan_order_by_id = self.add_relational(order_by)?;
        for order_by_element in order_by_elements {
            if let OrderByElement {
                entity: OrderByEntity::Expression { expr_id },
                ..
            } = order_by_element
            {
                self.replace_parent_in_subtree(expr_id, None, Some(plan_order_by_id))?;
            }
        }
        self.replace_parent_in_subtree(output, None, Some(plan_order_by_id))?;
        let top_proj_id = self.add_proj(plan_order_by_id, &[], false, true)?;
        Ok(top_proj_id)
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
        let name: Option<SmolStr> = alias.map(SmolStr::from);

        let output = self.add_row_for_output(child, &[], true)?;
        let sq = Relational::ScanSubQuery {
            alias: name,
            children: vec![child],
            output,
        };

        let sq_id = self.add_relational(sq)?;
        self.replace_parent_in_subtree(output, None, Some(sq_id))?;
        Ok(sq_id)
    }

    /// Appends a new CTE node to the plan arena.
    ///
    /// # Errors
    /// - CTE has incorrect amount of columns;
    ///
    /// # Panics
    /// - child node is not a valid relational node.
    pub fn add_cte(
        &mut self,
        child: NodeId,
        alias: SmolStr,
        columns: Vec<SmolStr>,
    ) -> Result<NodeId, SbroadError> {
        let child_node = self
            .get_relation_node(child)
            .expect("CTE child node is not a relational node");
        let mut child_id = child;

        if !columns.is_empty() {
            let mut child_output_id = child_node.output();
            // If the child node is VALUES, we need to wrap it with a subquery
            // to change names in projection.
            if matches!(child_node, Relational::Values { .. }) {
                let sq_id = self
                    .add_sub_query(child_id, Some(&alias))
                    .expect("add subquery for values");
                child_id = self
                    .add_proj(sq_id, &[], false, false)
                    .expect("add projection for values");
                child_output_id = self
                    .get_relational_output(child_id)
                    .expect("projection has an output tuple");
            }

            // If CTE has explicit column names, let's rename the columns in the child projection.
            let child_columns = self
                .get_expression_node(child_output_id)
                .expect("output row")
                .clone_row_list()?;
            if child_columns.len() != columns.len() {
                return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                    "expected {} columns in CTE, got {}",
                    child_columns.len(),
                    columns.len()
                )));
            }
            for (col_id, col_name) in child_columns.into_iter().zip(columns.into_iter()) {
                let col_alias = self
                    .get_mut_expression_node(col_id)
                    .expect("column expression");
                if let Expression::Alias { name, .. } = col_alias {
                    *name = col_name;
                } else {
                    panic!("Expected a row of aliases in the output tuple");
                };
            }
        }

        let output = self
            .add_row_for_output(child_id, &[], true)
            .expect("output row for CTE");
        let cte = Relational::ScanCte {
            alias,
            child: child_id,
            output,
        };
        let cte_id = self.add_relational(cte)?;
        Ok(cte_id)
    }

    /// Adds union all node.
    ///
    /// # Errors
    /// - children nodes are not relational
    /// - children tuples are invalid
    /// - children tuples have mismatching structure
    pub fn add_union(
        &mut self,
        left: usize,
        right: usize,
        remove_duplicates: bool,
    ) -> Result<usize, SbroadError> {
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
            return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                "children tuples have mismatching amount of columns in union all node: left {left_row_len}, right {right_row_len}"
            )));
        }

        let output = self.add_row_for_union_except(left, right)?;
        let union_all = if remove_duplicates {
            Relational::Union {
                left,
                right,
                output,
            }
        } else {
            Relational::UnionAll {
                left,
                right,
                output,
            }
        };

        let union_id = self.add_relational(union_all)?;
        self.replace_parent_in_subtree(output, None, Some(union_id))?;
        Ok(union_id)
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
        let values_row_id = self.add_relational(values_row)?;
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
            let mut aliases: Vec<SmolStr> = Vec::with_capacity(list.len());
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
                SbroadError::UnexpectedNumberOfValues(format_smolstr!(
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
        let values_id = self.add_relational(values)?;
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
    pub fn get_relational_aliases(&self, rel_id: usize) -> Result<Vec<SmolStr>, SbroadError> {
        let output = self.get_relational_output(rel_id)?;
        if let Expression::Row { list, .. } = self.get_expression_node(output)? {
            return list
                .iter()
                .map(|alias_id| {
                    self.get_expression_node(*alias_id)?
                        .get_alias_name()
                        .map(smol_str::ToSmolStr::to_smolstr)
                })
                .collect::<Result<Vec<SmolStr>, SbroadError>>();
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "expected output of Relational node {rel_id} to be Row"
            )),
        ))
    }

    /// Gets children from relational node.
    ///
    /// # Errors
    /// - node is not relational
    pub fn get_relational_children(&self, rel_id: usize) -> Result<Children<'_>, SbroadError> {
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
            return Ok(*rel.children().get(child_idx).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Relational,
                    Some(format_smolstr!(
                        "rel node {rel:?} has no child with idx ({child_idx})"
                    )),
                )
            })?);
        }
        Err(SbroadError::NotFound(
            Entity::Relational,
            format_smolstr!("with id ({rel_id})"),
        ))
    }

    /// Some nodes (like Having, Selection, Join),
    /// may have 0 or more subqueries in addition to
    /// their required children. This is a helper method
    /// to return the number of required children.
    ///
    /// # Errors
    /// - Node is not Join, Having, Selection
    pub fn get_required_children_len(&self, node_id: usize) -> Result<usize, SbroadError> {
        let len = match self.get_relation_node(node_id)? {
            Relational::Join { .. } => 2,
            Relational::Having { .. } | Relational::Selection { .. } => 1,
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "expected Join, Having or Selection on id ({node_id})"
                    )),
                ))
            }
        };
        Ok(len)
    }

    /// Finds the parent of the given relational node.
    ///
    /// # Errors
    /// - node is not relational
    /// - Plan has no top
    pub fn find_parent_rel(&self, target_id: usize) -> Result<Option<usize>, SbroadError> {
        for (id, node) in self.nodes.iter().enumerate() {
            if !matches!(node, Node::Relational(_)) {
                continue;
            }
            for child_id in self.nodes.rel_iter(id) {
                if child_id == target_id {
                    return Ok(Some(id));
                }
            }
        }
        Ok(None)
    }

    /// Change child of relational node.
    ///
    /// # Errors
    /// - node is not relational
    /// - node does not have child with specified id
    pub fn change_child(
        &mut self,
        parent_id: usize,
        old_child_id: usize,
        new_child_id: usize,
    ) -> Result<(), SbroadError> {
        let node = self.get_mut_relation_node(parent_id)?;
        let children = node.mut_children();
        for child_id in children {
            if *child_id == old_child_id {
                *child_id = new_child_id;
                return Ok(());
            }
        }

        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "node ({parent_id}) has no child with id ({old_child_id})"
            )),
        ))
    }

    /// Create a mapping between column positions
    /// in table and corresponding positions in
    /// relational node's output. Sharding
    /// column is skipped.
    ///
    /// # Errors
    /// - Node is not relational
    /// - Output tuple is invalid
    /// - Some table column is not found among output columns
    pub fn table_position_map(
        &self,
        table_name: &str,
        rel_id: usize,
    ) -> Result<HashMap<ColumnPosition, ColumnPosition>, SbroadError> {
        let table = self.get_relation_or_error(table_name)?;
        let alias_to_pos = ColumnPositionMap::new(self, rel_id)?;
        let mut map: HashMap<ColumnPosition, ColumnPosition> =
            HashMap::with_capacity(table.columns.len());
        for (table_pos, col) in table.columns.iter().enumerate() {
            if let ColumnRole::Sharding = col.role {
                continue;
            }
            let output_pos = alias_to_pos.get(col.name.as_str())?;
            map.insert(table_pos, output_pos);
        }
        Ok(map)
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
        if let Node::Relational(ref mut rel) = self.nodes.get_mut(rel_id)? {
            rel.set_children(children);
            return Ok(());
        }
        Err(SbroadError::Invalid(Entity::Relational, None))
    }

    /// Checks if the node is an additional child of some relational node.
    /// We can use a simple node scan instead of the tree traversal as we are interested
    /// only in relational nodes that can't be unlinked from the tree by our transformations.
    /// This is done for performance reasons.
    ///
    /// # Errors
    /// - Failed to get plan top
    /// - Node returned by the relational iterator is not relational (bug)
    pub fn is_additional_child(&self, node_id: usize) -> Result<bool, SbroadError> {
        for node in &self.nodes {
            match node {
                Node::Relational(
                    Relational::Selection { children, .. } | Relational::Having { children, .. },
                ) => {
                    if children.iter().skip(1).any(|&c| c == node_id) {
                        return Ok(true);
                    }
                }
                Node::Relational(Relational::Join { children, .. }) => {
                    if children.iter().skip(2).any(|&c| c == node_id) {
                        return Ok(true);
                    }
                }
                _ => {}
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
        let children = self.get_relational_children(rel_id)?;
        match self.get_relation_node(rel_id)? {
            Relational::Selection { .. }
            | Relational::Projection { .. }
            | Relational::Having { .. } => Ok(children.get(0) != Some(&sq_id)),
            Relational::Join { .. } => {
                Ok(children.get(0) != Some(&sq_id) && children.get(1) != Some(&sq_id))
            }
            _ => Ok(false),
        }
    }

    /// Get `Motion`'s node policy
    ///
    /// # Errors
    /// - node is not motion
    pub fn get_motion_policy(&self, motion_id: usize) -> Result<&MotionPolicy, SbroadError> {
        let node = self.get_relation_node(motion_id)?;
        if let Relational::Motion { policy, .. } = node {
            return Ok(policy);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!("expected Motion, got: {node:?}")),
        ))
    }
}

#[cfg(test)]
mod tests;
