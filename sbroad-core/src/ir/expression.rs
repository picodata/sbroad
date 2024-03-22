//! Expression module.
//!
//! Expressions are the building blocks of the tuple.
//! They provide information about:
//! - what input tuple's columns where used to build our tuple
//! - the order of the columns (and we can get their types as well)
//! - distribution of the data in the tuple

use ahash::RandomState;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::{BTreeMap, HashSet};
use std::hash::{Hash, Hasher};
use std::ops::Bound::Included;

use crate::errors::{Entity, SbroadError};
use crate::executor::engine::helpers::is_sharding_column_name;
use crate::ir::aggregates::AggregateKind;
use crate::ir::operator::{Bool, Relational};
use crate::ir::relation::Type;

use super::distribution::Distribution;
use super::tree::traversal::{PostOrderWithFilter, EXPR_CAPACITY};
use super::value::Value;
use super::{operator, Node, Nodes, Plan};

pub mod cast;
pub mod concat;
pub mod types;

pub(crate) type ExpressionId = usize;

/// Tuple tree build blocks.
///
/// A tuple describes a single portion of data moved among cluster nodes.
/// It consists of the ordered, strictly typed expressions with names
/// (columns) and additional information about data distribution policy.
///
/// Tuple is a tree with a `Row` top (level 0) and a list of the named
/// `Alias` columns (level 1). This convention is used across the code
/// and should not be changed. It ensures that we always know the
/// name of any column in the tuple and therefore simplifies AST
/// deserialization.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
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
    /// Binary expression returning row result.
    ///
    /// Example: `a + b > 42`, `a + b < c + 1`, `1 + 2 != 2 * 2`.
    ///
    /// TODO: always cover children with parentheses (in to_sql).
    Arithmetic {
        /// Left branch expression node index in the plan node arena.
        left: usize,
        /// Arithmetic operator.
        op: operator::Arithmetic,
        /// Right branch expression node index in the plan node arena.
        right: usize,
    },
    /// Type cast expression.
    ///
    /// Example: `cast(a as text)`.
    Cast {
        /// Target expression that must be casted to another type.
        child: usize,
        /// Cast type.
        to: cast::Type,
    },
    /// String concatenation expression.
    ///
    /// Example: `a || 'hello'`.
    Concat {
        /// Left expression node id.
        left: usize,
        /// Right expression node id.
        right: usize,
    },
    /// Constant expressions.
    ///
    /// Example: `42`.
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
        /// Relational node ID that contains current reference.
        parent: Option<usize>,
        /// Targets in the relational node children list.
        /// - Leaf nodes (relation scans): None.
        /// - Union nodes: two elements (left and right).
        /// - Other: single element.
        targets: Option<Vec<usize>>,
        /// Expression position in the input tuple (i.e. `Alias` column).
        position: usize,
        /// Referred column type in the input tuple.
        col_type: Type,
    },
    /// Top of the tuple tree.
    ///
    /// If the current tuple is the output for some relational operator, it should
    /// consist of the list of aliases. Otherwise (rows in selection filter
    /// or in join condition) we don't require aliases in the list.
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
    /// Stable function cannot modify the database and
    /// is guaranteed to return the same results given
    /// the same arguments for all rows within a single
    /// statement.
    ///
    /// Example: `bucket_id("1")` (the number of buckets can be
    /// changed only after restarting the cluster).
    StableFunction {
        /// Function name.
        name: String,
        /// Function arguments.
        children: Vec<usize>,
        /// If this function is an aggregate function: whether it is marked DISTINCT or not
        is_distinct: bool,
        /// Function return type.
        func_type: Type,
    },
    /// Unary expression returning boolean result.
    Unary {
        /// Unary operator.
        op: operator::Unary,
        /// Child expression node index in the plan node arena.
        child: usize,
    },
    /// Argument of `count` aggregate in `count(*)` expression
    CountAsterisk,
    ExprInParentheses {
        child: usize,
    },
}

#[allow(dead_code)]
impl Expression {
    /// Gets current row distribution.
    ///
    /// # Errors
    /// Returns `SbroadError` when the function is called on expression
    /// other than `Row` or a node doesn't know its distribution yet.
    pub fn distribution(&self) -> Result<&Distribution, SbroadError> {
        if let Expression::Row { distribution, .. } = self {
            let Some(dist) = distribution else {
                return Err(SbroadError::Invalid(
                    Entity::Distribution,
                    Some("distribution is uninitialized".into()),
                ));
            };
            return Ok(dist);
        }
        Err(SbroadError::Invalid(Entity::Expression, None))
    }

    /// Clone the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn clone_row_list(&self) -> Result<Vec<usize>, SbroadError> {
        match self {
            Expression::Row { list, .. } => Ok(list.clone()),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    #[must_use]
    pub fn is_aggregate_name(name: &str) -> bool {
        // currently we support only simple aggregates
        AggregateKind::new(name).is_some()
    }

    #[must_use]
    pub fn is_aggregate_fun(&self) -> bool {
        match self {
            Expression::StableFunction { name, .. } => Expression::is_aggregate_name(name),
            _ => false,
        }
    }

    /// Get a reference to the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn get_row_list(&self) -> Result<&[usize], SbroadError> {
        match self {
            Expression::Row { ref list, .. } => Ok(list),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    /// Get a mut reference to the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn get_mut_row_list(&mut self) -> Result<&mut Vec<usize>, SbroadError> {
        match self {
            Expression::Row { ref mut list, .. } => Ok(list),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    /// Get a mutable reference to the row children list.
    ///
    /// # Errors
    /// - node isn't `Row`
    pub fn get_row_list_mut(&mut self) -> Result<&mut Vec<usize>, SbroadError> {
        match self {
            Expression::Row { ref mut list, .. } => Ok(list),
            _ => Err(SbroadError::Invalid(
                Entity::Expression,
                Some("node isn't Row type".into()),
            )),
        }
    }

    /// Gets alias node name.
    ///
    /// # Errors
    /// - node isn't `Alias`
    pub fn get_alias_name(&self) -> Result<&str, SbroadError> {
        match self {
            Expression::Alias { name, .. } => Ok(name.as_str()),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some("node is not Alias type".into()),
            )),
        }
    }

    /// Checks for distribution determination
    ///
    /// # Errors
    /// - distribution isn't set
    pub fn has_unknown_distribution(&self) -> Result<bool, SbroadError> {
        let d = self.distribution()?;
        Ok(d.is_unknown())
    }

    /// Gets relational node id containing the reference.
    ///
    /// # Errors
    /// - node isn't reference type
    /// - reference doesn't have a parent
    pub fn get_parent(&self) -> Result<usize, SbroadError> {
        if let Expression::Reference { parent, .. } = self {
            return parent.ok_or_else(|| {
                SbroadError::Invalid(Entity::Expression, Some("Reference has no parent".into()))
            });
        }
        Err(SbroadError::Invalid(
            Entity::Expression,
            Some("node is not Reference type".into()),
        ))
    }

    /// The node is a row expression.
    #[must_use]
    pub fn is_row(&self) -> bool {
        matches!(self, Expression::Row { .. })
    }
    #[must_use]
    pub fn is_arithmetic(&self) -> bool {
        matches!(self, Expression::Arithmetic { .. })
    }

    /// Replaces parent in the reference node with the new one.
    pub fn replace_parent_in_reference(&mut self, from_id: Option<usize>, to_id: Option<usize>) {
        if let Expression::Reference { parent, .. } = self {
            if *parent == from_id {
                *parent = to_id;
            }
        }
    }

    /// Flushes parent in the reference node.
    pub fn flush_parent_in_reference(&mut self) {
        if let Expression::Reference { parent, .. } = self {
            *parent = None;
        }
    }
}

impl Nodes {
    /// Adds exression covered with parentheses node.
    ///
    /// # Errors
    /// - child node is invalid
    pub(crate) fn add_covered_with_parentheses(&mut self, child: usize) -> usize {
        let covered_with_parentheses = Expression::ExprInParentheses { child };
        self.push(Node::Expression(covered_with_parentheses))
    }

    /// Adds alias node.
    ///
    /// # Errors
    /// - child node is invalid
    /// - name is empty
    pub fn add_alias(&mut self, name: &str, child: usize) -> Result<usize, SbroadError> {
        let alias = Expression::Alias {
            name: String::from(name),
            child,
        };
        Ok(self.push(Node::Expression(alias)))
    }

    /// Adds boolean node.
    ///
    /// # Errors
    /// - when left or right nodes are invalid
    pub fn add_bool(
        &mut self,
        left: usize,
        op: operator::Bool,
        right: usize,
    ) -> Result<usize, SbroadError> {
        self.arena.get(left).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(left child of boolean node) from arena with index {left}"),
            )
        })?;
        self.arena.get(right).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(right child of boolean node) from arena with index {right}"),
            )
        })?;
        Ok(self.push(Node::Expression(Expression::Bool { left, op, right })))
    }

    /// Adds arithmetic node.
    ///
    /// # Errors
    /// - when left or right nodes are invalid
    pub fn add_arithmetic_node(
        &mut self,
        left: usize,
        op: operator::Arithmetic,
        right: usize,
    ) -> Result<usize, SbroadError> {
        self.arena.get(left).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(left child of Arithmetic node) from arena with index {left}"),
            )
        })?;
        self.arena.get(right).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(right child of Arithmetic node) from arena with index {right}"),
            )
        })?;
        Ok(self.push(Node::Expression(Expression::Arithmetic { left, op, right })))
    }

    /// Adds reference node.
    pub fn add_ref(
        &mut self,
        parent: Option<usize>,
        targets: Option<Vec<usize>>,
        position: usize,
        col_type: Type,
    ) -> usize {
        let r = Expression::Reference {
            parent,
            targets,
            position,
            col_type,
        };
        self.push(Node::Expression(r))
    }

    /// Adds row node.
    pub fn add_row(&mut self, list: Vec<usize>, distribution: Option<Distribution>) -> usize {
        self.push(Node::Expression(Expression::Row { list, distribution }))
    }

    /// Adds row node, where every column has an alias.
    /// Mostly used for relational node output.
    ///
    /// # Errors
    /// - nodes in the list do not exist in the arena
    /// - some nodes in the list are not aliases
    pub fn add_row_of_aliases(
        &mut self,
        list: Vec<usize>,
        distribution: Option<Distribution>,
    ) -> Result<usize, SbroadError> {
        for alias_id in &list {
            let node = self.arena.get(*alias_id).ok_or_else(|| {
                SbroadError::NotFound(Entity::Node, format!("from arena with index {alias_id}"))
            })?;
            if let Node::Expression(Expression::Alias { .. }) = node {
                continue;
            }
            return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(format!("expected {alias_id}  to be alias, got {node:?}")),
            ));
        }
        Ok(self.add_row(list, distribution))
    }

    /// Adds unary boolean node.
    ///
    /// # Errors
    /// - child node is invalid
    pub fn add_unary_bool(
        &mut self,
        op: operator::Unary,
        child: usize,
    ) -> Result<usize, SbroadError> {
        self.arena.get(child).ok_or_else(|| {
            SbroadError::NotFound(Entity::Node, format!("from arena with index {child}"))
        })?;
        Ok(self.push(Node::Expression(Expression::Unary { op, child })))
    }
}

// todo(ars): think how to refactor, ideally we must not store
// plan for PlanExpression, try to put it into hasher? but what do
// with equality?
pub struct PlanExpr<'plan> {
    pub id: usize,
    pub plan: &'plan Plan,
}

impl<'plan> PlanExpr<'plan> {
    #[must_use]
    pub fn new(id: usize, plan: &'plan Plan) -> Self {
        PlanExpr { id, plan }
    }
}

impl<'plan> Hash for PlanExpr<'plan> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let comp = Comparator::new(ReferencePolicy::ByFields, self.plan);
        comp.hash_for_expr(self.id, state, EXPR_HASH_DEPTH);
    }
}

impl<'plan> PartialEq for PlanExpr<'plan> {
    fn eq(&self, other: &Self) -> bool {
        let comp = Comparator::new(ReferencePolicy::ByFields, self.plan);
        comp.are_subtrees_equal(self.id, other.id).unwrap_or(false)
    }
}

impl<'plan> Eq for PlanExpr<'plan> {}

/// Policy of comparing and hashing expressions under `Reference` nodes.
pub enum ReferencePolicy {
    /// References are considered equal,
    /// if their subfields are equal (parent, position, target)
    ///
    /// Reference's hash is computed by hashing all subfields.
    /// E.g. used in `Update` logic for hashing expression in case we assigning several columns to
    /// the same value (e.g. `b+c` expression will be hashed using `ByFields` policy in query
    /// like `update T set a = b+c, d = b+c`).
    ByFields,
    /// References are considered equal,
    /// if they refer to the same alias
    ///
    /// Reference's hash is computed by hashing alias.
    /// E.g. used in `GroupBy` logic for comparing expression in projection with one used in the
    /// grouping expression (e.g. `"a"*2` expression will be hashed using `ByFields` policy in query
    /// like `select "a"*2 from T group by "a"*2`).
    ByAliases,
}

pub struct Comparator<'plan> {
    policy: ReferencePolicy,
    plan: &'plan Plan,
}

pub const EXPR_HASH_DEPTH: usize = 5;

impl<'plan> Comparator<'plan> {
    #[must_use]
    pub fn new(policy: ReferencePolicy, plan: &'plan Plan) -> Self {
        Comparator { policy, plan }
    }

    /// Checks whether expression subtrees `lhs` and `rhs` are equal.
    /// This function traverses both trees comparing their nodes.
    ///
    /// # Errors
    /// - invalid [`Expression::Reference`]s in either of subtrees
    /// - invalid children in some expression
    #[allow(clippy::too_many_lines)]
    pub fn are_subtrees_equal(&self, lhs: usize, rhs: usize) -> Result<bool, SbroadError> {
        let l = self.plan.get_node(lhs)?;
        let r = self.plan.get_node(rhs)?;
        if let Node::Expression(left) = l {
            if let Node::Expression(right) = r {
                match left {
                    Expression::Alias { .. } => {}
                    Expression::CountAsterisk => {
                        return Ok(matches!(right, Expression::CountAsterisk))
                    }
                    Expression::ExprInParentheses { child: l_child } => {
                        if let Expression::ExprInParentheses { child: r_child } = right {
                            return self.are_subtrees_equal(*l_child, *r_child);
                        }
                    }
                    Expression::Bool {
                        left: left_left,
                        op: op_left,
                        right: right_left,
                    } => {
                        if let Expression::Bool {
                            left: left_right,
                            op: op_right,
                            right: right_right,
                        } = right
                        {
                            return Ok(*op_left == *op_right
                                && self.are_subtrees_equal(*left_left, *left_right)?
                                && self.are_subtrees_equal(*right_left, *right_right)?);
                        }
                    }
                    Expression::Arithmetic {
                        op: op_left,
                        left: l_left,
                        right: r_left,
                    } => {
                        if let Expression::Arithmetic {
                            op: op_right,
                            left: l_right,
                            right: r_right,
                        } = right
                        {
                            return Ok(*op_left == *op_right
                                && self.are_subtrees_equal(*l_left, *l_right)?
                                && self.are_subtrees_equal(*r_left, *r_right)?);
                        }
                    }
                    Expression::Cast {
                        child: child_left,
                        to: to_left,
                    } => {
                        if let Expression::Cast {
                            child: child_right,
                            to: to_right,
                        } = right
                        {
                            return Ok(*to_left == *to_right
                                && self.are_subtrees_equal(*child_left, *child_right)?);
                        }
                    }
                    Expression::Concat {
                        left: left_left,
                        right: right_left,
                    } => {
                        if let Expression::Concat {
                            left: left_right,
                            right: right_right,
                        } = right
                        {
                            return Ok(self.are_subtrees_equal(*left_left, *left_right)?
                                && self.are_subtrees_equal(*right_left, *right_right)?);
                        }
                    }
                    Expression::Constant { value: value_left } => {
                        if let Expression::Constant { value: value_right } = right {
                            return Ok(*value_left == *value_right);
                        }
                    }
                    Expression::Reference { .. } => {
                        if let Expression::Reference { .. } = right {
                            return match self.policy {
                                ReferencePolicy::ByAliases => {
                                    let alias_left =
                                        self.plan.get_alias_from_reference_node(left)?;
                                    let alias_right =
                                        self.plan.get_alias_from_reference_node(right)?;
                                    Ok(alias_left == alias_right)
                                }
                                ReferencePolicy::ByFields => Ok(left == right),
                            };
                        }
                    }
                    Expression::Row {
                        list: list_left, ..
                    } => {
                        if let Expression::Row {
                            list: list_right, ..
                        } = right
                        {
                            return Ok(list_left
                                .iter()
                                .zip(list_right.iter())
                                .all(|(l, r)| self.are_subtrees_equal(*l, *r).unwrap_or(false)));
                        }
                    }
                    Expression::StableFunction {
                        name: name_left,
                        children: children_left,
                        is_distinct: distinct_left,
                        func_type: func_type_left,
                    } => {
                        if let Expression::StableFunction {
                            name: name_right,
                            children: children_right,
                            is_distinct: distinct_right,
                            func_type: func_type_right,
                        } = right
                        {
                            return Ok(name_left == name_right
                                && distinct_left == distinct_right
                                && func_type_left == func_type_right
                                && children_left.iter().zip(children_right.iter()).all(
                                    |(l, r)| self.are_subtrees_equal(*l, *r).unwrap_or(false),
                                ));
                        }
                    }
                    Expression::Unary {
                        op: op_left,
                        child: child_left,
                    } => {
                        if let Expression::Unary {
                            op: op_right,
                            child: child_right,
                        } = right
                        {
                            return Ok(*op_left == *op_right
                                && self.are_subtrees_equal(*child_left, *child_right)?);
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    pub fn hash_for_expr<H: Hasher>(&self, top: usize, state: &mut H, depth: usize) {
        if depth == 0 {
            return;
        }
        let Ok(node) = self.plan.get_expression_node(top) else {
            return;
        };
        match node {
            Expression::ExprInParentheses { child } => {
                self.hash_for_expr(*child, state, depth - 1);
            }
            Expression::Alias { child, name } => {
                name.hash(state);
                self.hash_for_expr(*child, state, depth - 1);
            }
            Expression::Bool { op, left, right } => {
                op.hash(state);
                self.hash_for_expr(*left, state, depth - 1);
                self.hash_for_expr(*right, state, depth - 1);
            }
            Expression::Arithmetic { op, left, right } => {
                op.hash(state);
                self.hash_for_expr(*left, state, depth - 1);
                self.hash_for_expr(*right, state, depth - 1);
            }
            Expression::Cast { child, to } => {
                to.hash(state);
                self.hash_for_expr(*child, state, depth - 1);
            }
            Expression::Concat { left, right } => {
                self.hash_for_expr(*left, state, depth - 1);
                self.hash_for_expr(*right, state, depth - 1);
            }
            Expression::Constant { value } => {
                value.hash(state);
            }
            Expression::Reference {
                parent,
                position,
                targets,
                col_type,
            } => match self.policy {
                ReferencePolicy::ByAliases => {
                    self.plan
                        .get_alias_from_reference_node(node)
                        .unwrap_or("")
                        .hash(state);
                }
                ReferencePolicy::ByFields => {
                    parent.hash(state);
                    position.hash(state);
                    targets.hash(state);
                    col_type.hash(state);
                }
            },
            Expression::Row { list, .. } => {
                for child in list {
                    self.hash_for_expr(*child, state, depth - 1);
                }
            }
            Expression::StableFunction {
                name,
                children,
                is_distinct,
                func_type,
            } => {
                is_distinct.hash(state);
                func_type.hash(state);
                name.hash(state);
                for child in children {
                    self.hash_for_expr(*child, state, depth - 1);
                }
            }
            Expression::Unary { child, op } => {
                op.hash(state);
                self.hash_for_expr(*child, state, depth - 1);
            }
            Expression::CountAsterisk => {
                "CountAsterisk".hash(state);
            }
        }
    }
}

pub(crate) type Position = usize;

/// Identifier of how many times column (with specific name) was met in relational output.
#[derive(Debug, PartialEq)]
pub(crate) enum Positions {
    /// Init state.
    Empty,
    /// Column with such name was met in the output only once on a given `Position`.
    Single(Position),
    /// Several columns were met with the same name in the output.
    Multiple,
}

impl Positions {
    pub(crate) fn new() -> Self {
        Positions::Empty
    }

    pub(crate) fn push(&mut self, pos: Position) {
        if Positions::Empty == *self {
            *self = Positions::Single(pos);
        } else {
            *self = Positions::Multiple;
        }
    }
}

/// Pair of (Column name, Option(Scan name)).
pub(crate) type ColumnScanName = (SmolStr, Option<SmolStr>);

/// Map of { column name (with optional scan name) -> on which positions of relational node it's met }.
/// Built for concrete relational node. Every column from its (relational node) output is
/// presented as a key in `map`.
#[derive(Debug)]
pub(crate) struct ColumnPositionMap {
    /// Binary tree map.
    map: BTreeMap<ColumnScanName, Positions>,
    /// Max Scan name (in alphabetical order) that some of the columns in output can reference to.
    /// E.g. we have Join node that references to Scan nodes "aa" and "ab". The `max_scan_name` will
    /// be "ab".
    ///
    /// Used for querying binary tree `map` by ranges (see `ColumnPositionMap` `get` method below).
    max_scan_name: Option<SmolStr>,
}

impl ColumnPositionMap {
    pub(crate) fn new(plan: &Plan, rel_id: usize) -> Result<Self, SbroadError> {
        let rel_node = plan.get_relation_node(rel_id)?;
        let output = plan.get_expression_node(rel_node.output())?;
        let alias_ids = output.get_row_list()?;

        let mut map = BTreeMap::new();
        let mut max_name = None;
        for (pos, alias_id) in alias_ids.iter().enumerate() {
            let alias = plan.get_expression_node(*alias_id)?;
            let alias_name = SmolStr::from(alias.get_alias_name()?);
            let scan_name = rel_node.scan_name(plan, pos)?.map(SmolStr::from);
            // For query `select "a", "b" as "a" from (select "a", "b" from t)`
            // column entry "a" will have `Position::Multiple` so that if parent operator will
            // reference "a" we won't be able to identify which of these two columns
            // will it reference.
            map.entry((alias_name, scan_name.clone()))
                .or_insert_with(Positions::new)
                .push(pos);
            if max_name < scan_name {
                max_name = scan_name;
            }
        }
        Ok(Self {
            map,
            max_scan_name: max_name,
        })
    }

    /// Get position of relational output that corresponds to given `column`.
    /// Note that we don't specify a Scan name here (see `get_with_scan` below for that logic).
    pub(crate) fn get(&self, column: &str) -> Result<Position, SbroadError> {
        let from_key = (SmolStr::from(column), None);
        let to_key = (SmolStr::from(column), self.max_scan_name.clone());
        let mut iter = self.map.range((Included(from_key), Included(to_key)));
        match (iter.next(), iter.next()) {
            // Map contains several values for the same `column`.
            // e.g. in the query
            // `select "t2"."a", "t1"."a" from (select "a" from "t1") join (select "a" from "t2")
            // for the column "a" there will be two results: {
            // * Some(("a", "t2"), _),
            // * Some(("a", "t1"), _)
            // }
            //
            // So that given just a column name we can't say what column to refer to.
            (Some(..), Some(..)) => Err(SbroadError::DuplicatedValue(format!(
                "column name {column} is ambiguous"
            ))),
            // Map contains single value for the given `column`.
            (Some((_, position)), None) => {
                if let Positions::Single(pos) = position {
                    return Ok(*pos);
                }
                // In case we have query like
                // `select "a", "a" from (select "a" from t)`
                // where single column is met on several positions.
                Err(SbroadError::DuplicatedValue(format!(
                    "column name {column} is ambiguous"
                )))
            }
            _ => Err(SbroadError::NotFound(
                Entity::Column,
                format!("with name {column}"),
            )),
        }
    }

    pub(crate) fn get_with_scan(
        &self,
        column: &str,
        scan: Option<&str>,
    ) -> Result<Position, SbroadError> {
        let key = &(SmolStr::from(column), scan.map(SmolStr::from));
        if let Some(position) = self.map.get(key) {
            if let Positions::Single(pos) = position {
                return Ok(*pos);
            }
            // In case we have query like
            // `select "a", "a" from (select "a" from t)`
            // where single column is met on several positions.
            //
            // Even given `scan` we can't identify which of these two columns do we need to
            // refer to.
            return Err(SbroadError::DuplicatedValue(format!(
                "column name {column} is ambiguous"
            )));
        }
        Err(SbroadError::NotFound(
            Entity::Column,
            format!("with name {column} and scan {scan:?}"),
        ))
    }
}

impl Plan {
    /// Add `Row` to plan.
    pub fn add_row(&mut self, list: Vec<usize>, distribution: Option<Distribution>) -> usize {
        self.nodes.add_row(list, distribution)
    }

    /// Returns a list of columns from the children relational nodes outputs.
    ///
    /// * If `col_names` is empty then copies all the columns
    ///   from the child node to a new tuple.`need_sharding_column` indicates whether we want
    ///   to copy sharding columns from the child relational node
    /// * If `col_names` is not empty then copies only child output columns that correspond
    ///   to that names.
    ///
    /// `need_aliases` indicates whether we'd like to copy aliases (their names) from the child
    ///  node or whether we'd like to build raw References list.
    ///
    /// The `is_join` option "on" builds an output tuple for the left child and
    /// appends the right child's one to it. Otherwise we build an output tuple
    /// only from the first (left) child.
    ///
    /// # Errors
    /// Returns `SbroadError`:
    /// - relation node contains invalid `Row` in the output
    /// - targets and children are inconsistent
    /// - column names don't exist
    ///
    /// # Panics
    /// - `targets` has unreachable values
    #[allow(clippy::too_many_lines)]
    pub fn new_columns(
        &mut self,
        child_rel_nodes: &[usize],
        is_join: bool,
        targets: &[usize],
        col_names: &[&str],
        need_aliases: bool,
        need_sharding_column: bool,
    ) -> Result<Vec<usize>, SbroadError> {
        // We can pass two target children nodes only in case
        // of `UnionAll` and `InnerJoin`.
        // - For the `UnionAll` operator we need only the first
        // child to get correct column names for a new tuple
        // (the second child aliases would be shadowed). But each reference should point
        // to both children to give us additional information
        // during transformations.
        if (targets.len() > 2) || targets.is_empty() {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "invalid target length: {}",
                targets.len()
            )));
        }
        if let Some(max) = targets.iter().max() {
            if *max >= child_rel_nodes.len() {
                return Err(SbroadError::UnexpectedNumberOfValues(format!(
                    "invalid children length: {}",
                    child_rel_nodes.len()
                )));
            }
        }

        // List of columns to be passed into `Expression::Row`.
        let mut result_row_list: Vec<usize> = Vec::new();

        if col_names.is_empty() {
            let required_targets = if is_join { targets } else { &targets[0..1] };
            for target_idx in required_targets {
                let target_rel_child_id = targets
                    .get(*target_idx)
                    .expect("target child not found among required");
                let child_node_id = child_rel_nodes
                    .get(*target_rel_child_id)
                    .expect("child rel node not found");
                let rel_node = self.get_relation_node(*child_node_id)?;
                let child_row_list: Vec<(usize, usize)> = if let Expression::Row { list, .. } =
                    self.get_expression_node(rel_node.output())?
                {
                    if need_sharding_column {
                        list.iter()
                            .enumerate()
                            .map(|(pos, id)| (pos, *id))
                            .collect()
                    } else {
                        let mut new_list = Vec::with_capacity(list.len());
                        for (pos, expr_id) in list.iter().enumerate() {
                            if let Expression::Alias { name, .. } =
                                self.get_expression_node(*expr_id)?
                            {
                                if is_sharding_column_name(name.as_str()) {
                                    continue;
                                }
                            }
                            new_list.push((pos, *expr_id));
                        }
                        new_list
                    }
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("child node is not a row".into()),
                    ));
                };

                result_row_list.reserve(child_row_list.len());
                for (pos, alias_node) in child_row_list {
                    let expr = self.get_expression_node(alias_node)?;
                    let name: String = if let Expression::Alias { ref name, .. } = expr {
                        String::from(name)
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some(format!("expression {expr:?} is not an Alias")),
                        ));
                    };
                    let new_targets: Vec<usize> = if is_join {
                        // Reference in a join tuple first points to the left,
                        // then to the right child.
                        vec![*target_idx]
                    } else {
                        // Reference in union tuple points to **both** left and right children.
                        targets.to_vec()
                    };
                    let col_type = expr.calculate_type(self)?;
                    // Adds new references and aliases to arena (if we need them).
                    let r_id = self.nodes.add_ref(None, Some(new_targets), pos, col_type);
                    if need_aliases {
                        let a_id = self.nodes.add_alias(&name, r_id)?;
                        result_row_list.push(a_id);
                    } else {
                        result_row_list.push(r_id);
                    }
                }
            }

            return Ok(result_row_list);
        }

        result_row_list.reserve(col_names.len());
        let target_child = targets.first().expect("targets are empty");
        let child_node = child_rel_nodes
            .get(*target_child)
            .expect("child_rel_nodes doesn't contain needed target");

        let mut col_names_set: HashSet<&str, RandomState> =
            HashSet::with_capacity_and_hasher(col_names.len(), RandomState::new());
        for col_name in col_names {
            col_names_set.insert(col_name);
        }

        // Map of { column name (aliased) from child output -> its index in output }
        let map = ColumnPositionMap::new(self, *child_node)?;

        // Vec of { `map` key (column name), targets, `map` value (column index in child output) }
        let mut refs: Vec<(&str, Vec<usize>, usize)> = Vec::with_capacity(col_names.len());
        for col in col_names {
            let pos = map.get(col)?;
            refs.push((col, targets.to_vec(), pos));
        }

        let relational_op = self.get_relation_node(*child_node)?;
        let output_id = relational_op.output();
        let output = self.get_expression_node(output_id)?;
        let columns = output.clone_row_list()?;
        for (col, new_targets, pos) in refs {
            let col_id = *columns
                .get(pos)
                .expect("Column id not found under relational child output");
            let col_expr = self.get_expression_node(col_id)?;
            let col_type = col_expr.calculate_type(self)?;
            let r_id = self.nodes.add_ref(None, Some(new_targets), pos, col_type);
            if need_aliases {
                let a_id = self.nodes.add_alias(col, r_id)?;
                result_row_list.push(a_id);
            } else {
                result_row_list.push(r_id);
            }
        }

        Ok(result_row_list)
    }

    /// New output for a single child node (with aliases).
    ///
    /// If column names are empty, copy all the columns from the child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - child is an inconsistent relational node
    /// - column names don't exist
    pub fn add_row_for_output(
        &mut self,
        rel_node: usize,
        col_names: &[&str],
        need_sharding_column: bool,
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(
            &[rel_node],
            false,
            &[0],
            col_names,
            true,
            need_sharding_column,
        )?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// New output row for union node.
    ///
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_union_except(
        &mut self,
        left: usize,
        right: usize,
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[left, right], false, &[0, 1], &[], true, true)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// New output row for join node.
    ///
    /// Contains all the columns from left and right children.
    ///
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    pub fn add_row_for_join(&mut self, left: usize, right: usize) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[left, right], true, &[0, 1], &[], true, true)?;
        self.nodes.add_row_of_aliases(list, None)
    }

    /// Project columns from the child node.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - child is an inconsistent relational node
    /// - column names don't exist
    pub fn add_row_from_child(
        &mut self,
        child: usize,
        col_names: &[&str],
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[child], false, &[0], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project all the columns from the child's subquery node.
    ///
    /// New columns don't have aliases.
    ///
    /// # Errors
    /// - children nodes are inconsistent with the target position
    pub(crate) fn add_row_from_subquery(
        &mut self,
        children: &[usize],
        target: usize,
        parent: Option<usize>,
    ) -> Result<usize, SbroadError> {
        let sq_id = *children.get(target).ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues(format!(
                "invalid target index: {target} (children: {children:?})",
            ))
        })?;
        let sq_rel = self.get_relation_node(sq_id)?;
        let sq_output_id = sq_rel.output();
        let sq_alias_ids_len = self.get_row_list(sq_output_id)?.len();
        let mut new_refs = Vec::with_capacity(sq_alias_ids_len);
        for pos in 0..sq_alias_ids_len {
            let alias_id = *self
                .get_row_list(sq_output_id)?
                .get(pos)
                .expect("subquery output row already checked");
            let alias_type = self.get_expression_node(alias_id)?.calculate_type(self)?;
            let ref_id = self
                .nodes
                .add_ref(parent, Some(vec![target]), pos, alias_type);
            new_refs.push(ref_id);
        }
        let row_id = self.nodes.add_row(new_refs, None);
        Ok(row_id)
    }

    /// Project columns from the join's left branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the left child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_row_from_left_branch(
        &mut self,
        left: usize,
        right: usize,
        col_names: &[&str],
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[left, right], true, &[0], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// Project columns from the join's right branch.
    ///
    /// New columns don't have aliases. If column names are empty,
    /// copy all the columns from the right child.
    /// # Errors
    /// Returns `SbroadError`:
    /// - children are inconsistent relational nodes
    /// - column names don't exist
    pub fn add_row_from_right_branch(
        &mut self,
        left: usize,
        right: usize,
        col_names: &[&str],
    ) -> Result<usize, SbroadError> {
        let list = self.new_columns(&[left, right], true, &[1], col_names, false, true)?;
        Ok(self.nodes.add_row(list, None))
    }

    /// A relational node pointed by the reference.
    /// In a case of a reference in the Motion node
    /// within a dispatched IR to the storage, returns
    /// the Motion node itself.
    ///
    /// # Errors
    /// - reference is invalid
    pub fn get_relational_from_reference_node(&self, ref_id: usize) -> Result<&usize, SbroadError> {
        if let Node::Expression(Expression::Reference {
            targets, parent, ..
        }) = self.get_node(ref_id)?
        {
            let Some(referred_rel_id) = parent else {
                return Err(SbroadError::NotFound(
                    Entity::Node,
                    format!("that is Reference ({ref_id}) parent"),
                ));
            };
            let rel = self.get_relation_node(*referred_rel_id)?;
            if let Relational::Insert { .. } = rel {
                return Ok(referred_rel_id);
            } else if let Some(children) = rel.children() {
                match targets {
                    None => {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Reference node has no targets".into(),
                        ))
                    }
                    Some(positions) => match (positions.first(), positions.get(1)) {
                        (Some(first), None) => {
                            if let Some(child_id) = children.get(*first) {
                                return Ok(child_id);
                            }
                            // When we dispatch IR to the storage, we truncate the
                            // subtree below the Motion node. So, the references in
                            // the Motion's output row are broken. We treat them in
                            // a special way: we return the Motion node itself. Be
                            // aware of the circular references in the tree!
                            if let Relational::Motion { .. } = rel {
                                return Ok(referred_rel_id);
                            }
                            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                                "Relational node {rel:?} has no children"
                            )));
                        }
                        _ => {
                            return Err(SbroadError::UnexpectedNumberOfValues(
                                "Reference expected to point exactly a single relational node"
                                    .into(),
                            ))
                        }
                    },
                }
            }
        }
        Err(SbroadError::Invalid(Entity::Expression, None))
    }

    /// Get relational nodes referenced in the row.
    ///
    /// # Errors
    /// - node is not a row
    /// - row is invalid
    /// - `relational_map` is not initialized
    pub fn get_relational_nodes_from_row(
        &self,
        row_id: usize,
    ) -> Result<HashSet<usize, RandomState>, SbroadError> {
        let row = self.get_expression_node(row_id)?;
        let capacity = if let Expression::Row { list, .. } = row {
            list.len()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some("Node is not a row".into()),
            ));
        };
        let filter = |node_id: usize| -> bool {
            if let Ok(Node::Expression(Expression::Reference { .. })) = self.get_node(node_id) {
                return true;
            }
            false
        };
        let mut post_tree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            capacity,
            Box::new(filter),
        );
        post_tree.populate_nodes(row_id);
        let nodes = post_tree.take_nodes();
        // We don't expect much relational references in a row (5 is a reasonable number).
        let mut rel_nodes: HashSet<usize, RandomState> =
            HashSet::with_capacity_and_hasher(5, RandomState::new());
        for (_, id) in nodes {
            let reference = self.get_expression_node(id)?;
            if let Expression::Reference {
                targets, parent, ..
            } = reference
            {
                let referred_rel_id = parent.ok_or_else(|| {
                    SbroadError::NotFound(Entity::Node, format!("that is Reference ({id}) parent"))
                })?;
                let rel = self.get_relation_node(referred_rel_id)?;
                if let Some(children) = rel.children() {
                    if let Some(positions) = targets {
                        for pos in positions {
                            if let Some(child) = children.get(*pos) {
                                rel_nodes.insert(*child);
                            }
                        }
                    }
                }
            }
        }
        Ok(rel_nodes)
    }

    /// Check that the node is a boolean equality and its children are both rows.
    #[must_use]
    pub fn is_bool_eq_with_rows(&self, node_id: usize) -> bool {
        let Ok(node) = self.get_expression_node(node_id) else {
            return false;
        };
        if let Expression::Bool { left, op, right } = node {
            if *op != Bool::Eq {
                return false;
            }

            let Ok(left_node) = self.get_expression_node(*left) else {
                return false;
            };

            let Ok(right_node) = self.get_expression_node(*right) else {
                return false;
            };

            if left_node.is_row() && right_node.is_row() {
                return true;
            }
        }

        false
    }

    /// The node is a trivalent (boolean or NULL).
    ///
    /// # Errors
    /// - If node is not an expression.
    pub fn is_trivalent(&self, expr_id: usize) -> Result<bool, SbroadError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Bool { .. }
            | Expression::Arithmetic { .. }
            | Expression::Unary { .. }
            | Expression::Constant {
                value: Value::Boolean(_) | Value::Null,
                ..
            } => return Ok(true),
            Expression::ExprInParentheses { child } => return self.is_trivalent(*child),
            Expression::Row { list, .. } => {
                if let (Some(inner_id), None) = (list.first(), list.get(1)) {
                    return self.is_trivalent(*inner_id);
                }
            }
            _ => {}
        }
        Ok(false)
    }

    /// The node is a reference (or a row of a single reference column).
    ///
    /// # Errors
    /// - If node is not an expression.
    pub fn is_ref(&self, expr_id: usize) -> Result<bool, SbroadError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Reference { .. } => return Ok(true),
            Expression::Row { list, .. } => {
                if let (Some(inner_id), None) = (list.first(), list.get(1)) {
                    return self.is_ref(*inner_id);
                }
            }
            _ => {}
        }
        Ok(false)
    }

    /// Extract `Const` value from `Row` by index
    ///
    /// # Errors
    /// - node is not a row
    /// - row doesn't have const
    /// - const value is invalid
    #[allow(dead_code)]
    pub fn get_child_const_from_row(
        &self,
        row_id: usize,
        child_num: usize,
    ) -> Result<Value, SbroadError> {
        let node = self.get_expression_node(row_id)?;
        if let Expression::Row { list, .. } = node {
            let const_node_id = list
                .get(child_num)
                .ok_or_else(|| SbroadError::NotFound(Entity::Node, format!("{child_num}")))?;

            let v = self.get_expression_node(*const_node_id)?.as_const_value()?;

            return Ok(v);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some("node is not Row type".into()),
        ))
    }

    /// Replace parent for all references in the expression subtree of the current node.
    ///
    /// # Errors
    /// - node is invalid
    /// - node is not an expression
    pub fn replace_parent_in_subtree(
        &mut self,
        node_id: usize,
        from_id: Option<usize>,
        to_id: Option<usize>,
    ) -> Result<(), SbroadError> {
        let filter = |node_id: usize| -> bool {
            if let Ok(Node::Expression(Expression::Reference { .. })) = self.get_node(node_id) {
                return true;
            }
            false
        };
        let mut subtree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        subtree.populate_nodes(node_id);
        let references = subtree.take_nodes();
        drop(subtree);
        for (_, id) in references {
            let node = self.get_mut_expression_node(id)?;
            node.replace_parent_in_reference(from_id, to_id);
        }
        Ok(())
    }

    /// Flush parent to `None` for all references in the expression subtree of the current node.
    ///
    /// # Errors
    /// - node is invalid
    /// - node is not an expression
    pub fn flush_parent_in_subtree(&mut self, node_id: usize) -> Result<(), SbroadError> {
        let filter = |node_id: usize| -> bool {
            if let Ok(Node::Expression(Expression::Reference { .. })) = self.get_node(node_id) {
                return true;
            }
            false
        };
        let mut subtree = PostOrderWithFilter::with_capacity(
            |node| self.nodes.expr_iter(node, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        subtree.populate_nodes(node_id);
        let references = subtree.take_nodes();
        drop(subtree);
        for (_, id) in references {
            let node = self.get_mut_expression_node(id)?;
            node.flush_parent_in_reference();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
