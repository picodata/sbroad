//! Intermediate representation (IR) module.
//!
//! Contains the logical plan tree and helpers.

use base64ct::{Base64, Encoding};
use serde::{Deserialize, Serialize};
use smol_str::ToSmolStr;
use std::collections::hash_map::IntoIter;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

use std::slice::Iter;
use tarantool::tlua;

use acl::Acl;
use block::Block;
use ddl::Ddl;
use expression::Expression;
use operator::{Arithmetic, Relational};
use relation::Table;

use crate::errors::Entity::Query;
use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::TableVersionMap;
use crate::ir::expression::Expression::StableFunction;
use crate::ir::helpers::RepeatableState;
use crate::ir::operator::Bool;
use crate::ir::relation::Column;
use crate::ir::tree::traversal::{
    BreadthFirst, PostOrder, PostOrderWithFilter, EXPR_CAPACITY, REL_CAPACITY,
};
use crate::ir::undo::TransformationLog;
use crate::ir::value::Value;
use crate::{collection, error, warn};

use self::expression::Position;
use self::parameters::Parameters;
use self::relation::Relations;
use self::transformation::redistribution::MotionPolicy;

// TODO: remove when rust version in bumped in module
#[allow(elided_lifetimes_in_associated_constant)]
pub mod acl;
pub mod aggregates;
pub mod block;
pub mod ddl;
pub mod distribution;
pub mod expression;
pub mod function;
pub mod helpers;
pub mod operator;
pub mod parameters;
pub mod relation;
pub mod transformation;
pub mod tree;
pub mod undo;
pub mod value;

const DEFAULT_VTABLE_MAX_ROWS: u64 = 5000;
const DEFAULT_VDBE_MAX_STEPS: u64 = 45000;

/// Plan tree node.
///
/// There are two kinds of node variants: expressions and relational
/// operators. Both of them can easily refer each other in the
/// tree as they are stored in the same node arena. The reasons
/// to separate them are:
///
/// - they should be treated with quite different logic
/// - we don't want to have a single huge enum
///
/// Enum was chosen as we don't want to mess with dynamic
/// dispatching and its performance penalties.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Node {
    Acl(Acl),
    Block(Block),
    Ddl(Ddl),
    Expression(Expression),
    Relational(Relational),
    Parameter,
}

/// Plan nodes storage.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Nodes {
    /// We don't want to mess with the borrow checker and RefCell/Rc,
    /// so all nodes are stored in the single arena ("nodes" array).
    /// The positions in the array act like pointers, so it is possible
    /// only to add nodes to the plan, but never remove them.
    arena: Vec<Node>,
}

impl Nodes {
    pub(crate) fn get(&self, id: usize) -> Result<&Node, SbroadError> {
        match self.arena.get(id) {
            None => Err(SbroadError::NotFound(
                Entity::Node,
                format!("from arena with index {id}").into(),
            )),
            Some(node) => Ok(node),
        }
    }

    pub(crate) fn get_mut(&mut self, id: usize) -> Result<&mut Node, SbroadError> {
        match self.arena.get_mut(id) {
            None => Err(SbroadError::NotFound(
                Entity::Node,
                format!("from arena with index {id}").into(),
            )),
            Some(node) => Ok(node),
        }
    }

    /// Get the amount of relational nodes in the plan.
    #[must_use]
    pub fn relation_node_amount(&self) -> usize {
        self.arena
            .iter()
            .filter(|node| matches!(node, Node::Relational(_)))
            .count()
    }

    /// Get the amount of expression nodes in the plan.
    #[must_use]
    pub fn expression_node_amount(&self) -> usize {
        self.arena
            .iter()
            .filter(|node| matches!(node, Node::Expression(_)))
            .count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.arena.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.arena.len()
    }

    pub fn iter(&self) -> Iter<'_, Node> {
        self.arena.iter()
    }

    /// Add new node to arena.
    ///
    /// Inserts a new node to the arena and returns its position,
    /// that is treated as a pointer.
    pub fn push(&mut self, node: Node) -> usize {
        let position = self.arena.len();
        self.arena.push(node);
        position
    }

    /// Returns the next node position
    #[must_use]
    pub fn next_id(&self) -> usize {
        self.arena.len()
    }

    /// Replace a node in arena with another one.
    ///
    /// # Errors
    /// - The node with the given position doesn't exist.
    pub fn replace(&mut self, id: usize, node: Node) -> Result<Node, SbroadError> {
        if id >= self.arena.len() {
            return Err(SbroadError::UnexpectedNumberOfValues(
                format!("can't replace node with id {id} as it is out of arena bounds").into(),
            ));
        }
        let old_node = std::mem::replace(&mut self.arena[id], node);
        Ok(old_node)
    }

    pub fn reserve(&mut self, capacity: usize) {
        self.arena.reserve(capacity);
    }

    pub fn shrink_to_fit(&mut self) {
        self.arena.shrink_to_fit();
    }
}

impl<'nodes> IntoIterator for &'nodes Nodes {
    type Item = &'nodes Node;
    type IntoIter = Iter<'nodes, Node>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// One level of `Slices`.
/// Element of `slice` vec is a `motion_id` to execute.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Slice {
    slice: Vec<usize>,
}

impl From<Vec<usize>> for Slice {
    fn from(vec: Vec<usize>) -> Self {
        Self { slice: vec }
    }
}

impl Slice {
    #[must_use]
    pub fn position(&self, index: usize) -> Option<&usize> {
        self.slice.get(index)
    }

    #[must_use]
    pub fn positions(&self) -> &[usize] {
        &self.slice
    }
}

/// Vec of `motion_id` levels (see `slices` field of `Plan` structure for more information).
/// Element of `slices` vec is one level containing several `motion_id`s to execute.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Slices {
    pub slices: Vec<Slice>,
}

impl From<Vec<Slice>> for Slices {
    fn from(vec: Vec<Slice>) -> Self {
        Self { slices: vec }
    }
}

impl From<Vec<Vec<usize>>> for Slices {
    fn from(vec: Vec<Vec<usize>>) -> Self {
        Self {
            slices: vec.into_iter().map(Slice::from).collect(),
        }
    }
}

impl Slices {
    #[must_use]
    pub fn slice(&self, index: usize) -> Option<&Slice> {
        self.slices.get(index)
    }

    #[must_use]
    pub fn slices(&self) -> &[Slice] {
        self.slices.as_ref()
    }

    #[must_use]
    pub fn empty() -> Self {
        Self { slices: vec![] }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Deserialize, Serialize)]
pub enum OptionParamValue {
    Value { val: Value },
    Parameter { plan_id: usize },
}

#[derive(PartialEq, Eq, Debug, Clone, Deserialize, Serialize)]
pub struct OptionSpec {
    pub kind: OptionKind,
    pub val: OptionParamValue,
}

#[derive(Clone, PartialEq, Eq, Debug, Hash, Deserialize, Serialize)]
pub enum OptionKind {
    SqlVdbeMaxSteps,
    VTableMaxRows,
}

impl Display for OptionKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            OptionKind::SqlVdbeMaxSteps => "sql_vdbe_max_steps",
            OptionKind::VTableMaxRows => "vtable_max_rows",
        };
        write!(f, "{s}")
    }
}

/// Options passed to `box.execute`
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Eq)]
pub struct ExecuteOptions(HashMap<OptionKind, Value, RepeatableState>);

impl ExecuteOptions {
    #[must_use]
    pub fn new(opts: HashMap<OptionKind, Value, RepeatableState>) -> Self {
        ExecuteOptions(opts)
    }

    #[must_use]
    pub fn to_iter(self) -> IntoIter<OptionKind, Value> {
        self.0.into_iter()
    }

    pub fn insert(&mut self, kind: OptionKind, value: Value) -> Option<Value> {
        self.0.insert(kind, value)
    }
}

impl<L> tlua::PushInto<L> for ExecuteOptions
where
    L: tlua::AsLua,
{
    type Err = String;

    #[allow(unreachable_code)]
    fn push_into_lua(self, lua: L) -> Result<tlua::PushGuard<L>, (Self::Err, L)> {
        let to_push: Vec<Vec<(String, Value)>> = if self.0.is_empty() {
            vec![]
        } else {
            vec![self
                .0
                .into_iter()
                .map(|(kind, value)| (kind.to_string(), value))
                .collect()]
        };
        match to_push.push_into_lua(lua) {
            Ok(r) => Ok(r),
            Err(e) => {
                error!(
                    Option::from("push ExecuteOptions into lua"),
                    &format!("{:?}", e.0),
                );
                Err((e.0.to_string(), e.1))
            }
        }
    }
}

impl Default for ExecuteOptions {
    fn default() -> Self {
        let exec_opts: HashMap<OptionKind, Value, RepeatableState> = collection!((
            OptionKind::SqlVdbeMaxSteps,
            Value::Unsigned(DEFAULT_VDBE_MAX_STEPS)
        ));
        ExecuteOptions(exec_opts)
    }
}

/// SQL options specified by user in `option(..)` clause.
///
/// Note: ddl options are handled separately.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Options {
    /// Maximum size of the virtual table that this query can produce or use during
    /// query execution. This limit is checked on storage before sending a result table,
    /// and on router before appending the result from one storage to results from other
    /// storages. Value of `0` indicates that this limit is disabled.
    ///
    /// Note: this limit allows the out of memory error for query execution in the following
    /// scenario: if already received vtable has `X` rows and `X + a` causes the OOM, then
    /// if one of the storages returns `a` or more rows, the OOM will occur.
    pub vtable_max_rows: u64,
    /// Options passed to `box.execute` function on storages. Currently there is only one option
    /// `sql_vdbe_max_steps`.
    pub execute_options: ExecuteOptions,
}

impl Default for Options {
    fn default() -> Self {
        Options::new(DEFAULT_VTABLE_MAX_ROWS, ExecuteOptions::default())
    }
}

impl Options {
    #[must_use]
    pub fn new(vtable_max_rows: u64, execute_options: ExecuteOptions) -> Self {
        Options {
            vtable_max_rows,
            execute_options,
        }
    }
}

pub type NodeId = usize;
pub type ValueIdx = usize;

/// Logical plan tree structure.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct Plan {
    /// Append only arena for the plan nodes.
    pub(crate) nodes: Nodes,
    /// Relations are stored in a hash-map, with a table name acting as a
    /// key to guarantee its uniqueness across the plan.
    pub relations: Relations,
    /// Slice is a plan subtree under Motion node, that can be executed
    /// on a single db instance without data distribution problems (we add
    /// Motions to resolve them). Them we traverse the plan tree and collect
    /// Motions level by level in a bottom-up manner to the "slices" array
    /// of arrays. All the slices on the same level can be executed in parallel.
    /// In fact, "slices" is a prepared set of commands for the executor.
    pub(crate) slices: Slices,
    /// The plan top is marked as optional for tree creation convenience.
    /// We build the plan tree in a bottom-up manner, so the top would
    /// be added last. The plan without a top should be treated as invalid.
    top: Option<usize>,
    /// The flag is enabled if user wants to get a query plan only.
    /// In this case we don't need to execute query.
    is_explain: bool,
    /// The undo log keeps the history of the plan transformations. It can
    /// be used to revert the plan subtree to some previous snapshot if needed.
    pub(crate) undo: TransformationLog,
    /// Maps parameter to the corresponding constant node.
    pub(crate) constants: Parameters,
    /// Options that were passed by user in `Option` clause. Does not include
    /// options for DDL as those are handled separately. This field is used only
    /// for storing the order of options in `Option` clause, after `bind_params` is
    /// called this field is not used and becomes empty.
    pub raw_options: Vec<OptionSpec>,
    /// Mapping between parameter plan id and corresponding value position in
    /// in values list. This is needed only for handling PG-like parameters
    /// in `bind_params` after which it becomes `None`.
    /// If query uses tnt-like params, then the map is empty.
    pub pg_params_map: HashMap<NodeId, ValueIdx>,
    /// SQL options. Initiliazed to defaults upon IR creation. Then bound to actual
    /// values after `bind_params` these options are set to their actual values.
    /// See `apply_options`.
    pub options: Options,
    pub version_map: TableVersionMap,
}

impl Default for Plan {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl Plan {
    /// Add relation to the plan.
    ///
    /// If relation already exists, do nothing.
    pub fn add_rel(&mut self, table: Table) {
        self.relations.insert(table);
    }

    /// Check that plan tree is valid.
    ///
    /// # Errors
    /// Returns `SbroadError` when the plan tree check fails.
    pub fn check(&self) -> Result<(), SbroadError> {
        match self.top {
            None => {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some("plan tree top is None".into()),
                ))
            }
            Some(top) => {
                let _ = self.nodes.get(top)?;
            }
        }

        //TODO: additional consistency checks

        Ok(())
    }

    /// Constructor for an empty plan structure.
    #[must_use]
    pub fn new() -> Self {
        Plan {
            nodes: Nodes { arena: Vec::new() },
            relations: Relations::new(),
            slices: Slices { slices: vec![] },
            top: None,
            is_explain: false,
            undo: TransformationLog::new(),
            constants: Parameters::new(),
            raw_options: vec![],
            options: Options::default(),
            version_map: TableVersionMap::new(),
            pg_params_map: HashMap::new(),
        }
    }

    /// Validate options stored in `Plan.raw_options` and initialize
    /// `Plan`'s fields for corresponding options
    ///
    /// # Errors
    /// - Invalid parameter value for given option
    /// - The same option used more than once in `Plan.raw_options`
    /// - Option value already violated in current `Plan`
    /// - The given option does not work for this specific query
    #[allow(clippy::uninlined_format_args)]
    pub fn apply_options(&mut self) -> Result<(), SbroadError> {
        let mut used_options: HashSet<OptionKind> = HashSet::new();
        let options = std::mem::take(&mut self.raw_options);
        let values_count = {
            let mut values_count: Option<usize> = None;
            let mut bfs =
                BreadthFirst::with_capacity(|x| self.nodes.rel_iter(x), REL_CAPACITY, REL_CAPACITY);
            bfs.populate_nodes(self.get_top()?);
            let nodes = bfs.take_nodes();
            for (_, id) in nodes {
                if let Relational::Insert { .. } = self.get_relation_node(id)? {
                    let child_id = self.get_relational_child(id, 0)?;
                    if let Relational::Values { children, .. } = self.get_relation_node(child_id)? {
                        values_count = Some(children.len());
                    }
                }
            }
            values_count
        };
        for opt in options {
            if !used_options.insert(opt.kind.clone()) {
                return Err(SbroadError::Invalid(
                    Query,
                    Some(format!("option {} specified more than once!", opt.kind).into()),
                ));
            }
            let OptionParamValue::Value { val } = opt.val else {
                return Err(SbroadError::Invalid(Entity::OptionSpec, None));
            };
            match opt.kind {
                OptionKind::SqlVdbeMaxSteps => {
                    if values_count.is_some() {
                        warn!(
                            Option::from("apply_options"),
                            &format!("Option {} does not apply for insert with values", opt.kind)
                        );
                    }
                    if let Value::Unsigned(_) = val {
                        self.options.execute_options.insert(opt.kind, val);
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::OptionSpec,
                            Some(
                                format!("expected option {} to be unsigned got: {val:?}", opt.kind)
                                    .into(),
                            ),
                        ));
                    }
                }
                OptionKind::VTableMaxRows => {
                    if let Value::Unsigned(limit) = val {
                        if let Some(vtable_rows_count) = values_count {
                            if limit < vtable_rows_count as u64 {
                                return Err(SbroadError::UnexpectedNumberOfValues(format!(
                                    "Exceeded maximum number of rows ({limit}) in virtual table: {}",
                                    vtable_rows_count
                                ).into()));
                            }
                        }
                        self.options.vtable_max_rows = limit;
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::OptionSpec,
                            Some(
                                format!("expected option {} to be unsigned got: {val:?}", opt.kind)
                                    .into(),
                            ),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Check if the plan arena is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.arena.is_empty()
    }

    /// Get a node by its pointer (position in the node arena).
    ///
    /// # Errors
    /// Returns `SbroadError` when the node with requested index
    /// doesn't exist.
    pub fn get_node(&self, id: usize) -> Result<&Node, SbroadError> {
        match self.nodes.arena.get(id) {
            None => Err(SbroadError::NotFound(
                Entity::Node,
                format!("from arena with index {id}").into(),
            )),
            Some(node) => Ok(node),
        }
    }

    /// Get a mutable node by its pointer (position in the node arena).
    ///
    /// # Errors
    /// Returns `SbroadError` when the node with requested index
    /// doesn't exist.
    pub fn get_mut_node(&mut self, id: usize) -> Result<&mut Node, SbroadError> {
        match self.nodes.arena.get_mut(id) {
            None => Err(SbroadError::NotFound(
                Entity::Node,
                format!("(mutable) from arena with index {id}").into(),
            )),
            Some(node) => Ok(node),
        }
    }

    /// Get a top node of the plan tree.
    ///
    /// # Errors
    /// - top node is None (i.e. invalid plan)
    pub fn get_top(&self) -> Result<usize, SbroadError> {
        self.top
            .ok_or_else(|| SbroadError::Invalid(Entity::Plan, Some("plan tree top is None".into())))
    }

    /// Clone plan slices.
    #[must_use]
    pub fn clone_slices(&self) -> Slices {
        self.slices.clone()
    }

    /// Get relation in the plan by its name or returns error.
    ///
    /// # Errors
    /// - no relation with given name
    pub fn get_relation_or_error(&self, name: &str) -> Result<&Table, SbroadError> {
        self.relations
            .get(name)
            .ok_or_else(|| SbroadError::NotFound(Entity::Table, format!("with name {name}").into()))
    }

    /// Get relation of a scan node
    ///
    /// # Errors
    /// - Given node is not a scan
    pub fn get_scan_relation(&self, scan_id: usize) -> Result<&str, SbroadError> {
        let node = self.get_relation_node(scan_id)?;
        if let Relational::ScanRelation { relation, .. } = node {
            return Ok(relation.as_str());
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("expected scan node, got: {node:?}").into()),
        ))
    }

    /// Get relation in the plan by its name or returns error.
    ///
    /// # Errors
    /// - invalid table name
    /// - invalid column index
    pub fn get_relation_column(
        &self,
        table_name: &str,
        col_idx: usize,
    ) -> Result<&Column, SbroadError> {
        self.get_relation_or_error(table_name)?
            .columns
            .get(col_idx)
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Column,
                    Some(
                        format!("invalid column position {col_idx} for table {table_name}").into(),
                    ),
                )
            })
    }

    /// Check whether given expression contains aggregates.
    /// If `check_top` is false, the root expression node is not
    /// checked.
    ///
    /// # Errors
    /// - node is not an expression
    /// - invalid expression tree
    pub fn contains_aggregates(
        &self,
        expr_id: usize,
        check_top: bool,
    ) -> Result<bool, SbroadError> {
        let filter = |id: usize| -> bool {
            matches!(
                self.get_node(id),
                Ok(Node::Expression(Expression::StableFunction { .. }))
            )
        };
        let mut dfs = PostOrderWithFilter::with_capacity(
            |x| self.nodes.expr_iter(x, false),
            EXPR_CAPACITY,
            Box::new(filter),
        );
        for (_, id) in dfs.iter(expr_id) {
            if !check_top && id == expr_id {
                continue;
            }
            if let Node::Expression(Expression::StableFunction { name, .. }) = self.get_node(id)? {
                if Expression::is_aggregate_name(name) {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Construct a plan from the YAML file.
    ///
    /// # Errors
    /// Returns `SbroadError` when the YAML plan is invalid.
    pub fn from_yaml(s: &str) -> Result<Self, SbroadError> {
        let plan: Plan = match serde_yaml::from_str(s) {
            Ok(p) => p,
            Err(e) => return Err(SbroadError::Invalid(Entity::Plan, Some(e.to_smolstr()))),
        };
        plan.check()?;
        Ok(plan)
    }

    /// Helper function for writing tests with yaml
    ///
    /// # Errors
    /// Returns `SbroadError` when serde failed to serialize the plan.
    pub fn to_yaml(&self) -> Result<String, SbroadError> {
        let s = match serde_yaml::to_string(self) {
            Ok(s) => s,
            Err(e) => return Err(SbroadError::Invalid(Entity::Plan, Some(e.to_smolstr()))),
        };
        Ok(s)
    }

    /// Get relational node and produce a new row without aliases from its output (row with aliases).
    ///
    /// # Errors
    /// - node is not relational
    /// - node's output is not a row of aliases
    pub fn get_row_from_rel_node(&mut self, node: usize) -> Result<usize, SbroadError> {
        let n = self.get_node(node)?;
        if let Node::Relational(rel) = n {
            if let Node::Expression(Expression::Row { list, .. }) = self.get_node(rel.output())? {
                let mut cols: Vec<usize> = Vec::with_capacity(list.len());
                for alias in list {
                    if let Node::Expression(Expression::Alias { child, .. }) =
                        self.get_node(*alias)?
                    {
                        cols.push(*child);
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some("node's output is not a row of aliases".into()),
                        ));
                    }
                }
                return Ok(self.nodes.add_row(cols, None));
            }
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("node is not Relational type: {n:?}").into()),
        ))
    }

    #[must_use]
    pub fn next_id(&self) -> usize {
        self.nodes.next_id()
    }

    /// Add condition node to the plan.
    ///
    /// # Errors
    /// Returns `SbroadError` when the condition node can't append'.
    pub fn add_cond(
        &mut self,
        left: usize,
        op: operator::Bool,
        right: usize,
    ) -> Result<usize, SbroadError> {
        self.nodes.add_bool(left, op, right)
    }

    /// Add node covered with parentheses to the plan.
    ///
    /// # Errors
    /// Returns `SbroadError` when the condition node can't append'.
    pub fn add_covered_with_parentheses(&mut self, child: usize) -> usize {
        self.nodes.add_covered_with_parentheses(child)
    }

    /// Add arithmetic node to the plan.
    ///
    /// # Errors
    /// Returns `SbroadError` when the condition node can't append'.
    pub fn add_arithmetic_to_plan(
        &mut self,
        left: usize,
        op: Arithmetic,
        right: usize,
    ) -> Result<usize, SbroadError> {
        self.nodes.add_arithmetic_node(left, op, right)
    }

    /// Add unary operator node to the plan.
    ///
    /// # Errors
    /// - Child node is invalid
    pub fn add_unary(&mut self, op: operator::Unary, child: usize) -> Result<usize, SbroadError> {
        self.nodes.add_unary_bool(op, child)
    }

    /// Add bool operator node to the plan.
    ///
    /// # Errors
    /// - Children node are invalid
    pub fn add_bool(&mut self, left: usize, op: Bool, right: usize) -> Result<usize, SbroadError> {
        self.nodes.add_bool(left, op, right)
    }

    /// Marks plan as query explain
    pub fn mark_as_explain(&mut self) {
        self.is_explain = true;
    }

    /// Checks that plan is explain query
    #[must_use]
    pub fn is_explain(&self) -> bool {
        self.is_explain
    }

    /// Checks that plan is a block of queries.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_block(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        Ok(matches!(self.get_node(top_id)?, Node::Block(..)))
    }

    /// Checks that plan is DDL query
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_ddl(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        Ok(matches!(self.get_node(top_id)?, Node::Ddl(..)))
    }

    /// Checks that plan is ACL query.
    ///
    /// # Errors
    /// - top node doesn't exist in the plan or is invalid.
    pub fn is_acl(&self) -> Result<bool, SbroadError> {
        let top_id = self.get_top()?;
        Ok(matches!(self.get_node(top_id)?, Node::Acl(..)))
    }

    /// Set top node of plan
    /// # Errors
    /// - top node doesn't exist in the plan.
    pub fn set_top(&mut self, top: usize) -> Result<(), SbroadError> {
        self.get_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get relation type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not a relational type
    pub fn get_relation_node(&self, node_id: usize) -> Result<&Relational, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Relational(rel) => Ok(rel),
            Node::Expression(_)
            | Node::Parameter
            | Node::Ddl(..)
            | Node::Acl(..)
            | Node::Block(..) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not Relational type: {node:?}").into()),
            )),
        }
    }

    /// Get mutable relation type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not a relational type
    pub fn get_mut_relation_node(
        &mut self,
        node_id: usize,
    ) -> Result<&mut Relational, SbroadError> {
        match self.get_mut_node(node_id)? {
            Node::Relational(rel) => Ok(rel),
            Node::Expression(_)
            | Node::Parameter
            | Node::Ddl(..)
            | Node::Acl(..)
            | Node::Block(..) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("Node is not relational".into()),
            )),
        }
    }

    /// Get expression type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not expression type
    pub fn get_expression_node(&self, node_id: usize) -> Result<&Expression, SbroadError> {
        match self.get_node(node_id)? {
            Node::Expression(exp) => Ok(exp),
            Node::Parameter => {
                let node = self.constants.get(node_id);
                if let Some(Node::Expression(exp)) = node {
                    Ok(exp)
                } else {
                    Err(SbroadError::Invalid(
                        Entity::Node,
                        Some("parameter node does not refer to an expression".into()),
                    ))
                }
            }
            Node::Relational(_) | Node::Ddl(..) | Node::Acl(..) | Node::Block(..) => Err(
                SbroadError::Invalid(Entity::Node, Some("node is not Expression type".into())),
            ),
        }
    }

    /// Get mutable expression type node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not expression type
    pub fn get_mut_expression_node(
        &mut self,
        node_id: usize,
    ) -> Result<&mut Expression, SbroadError> {
        let node = self.get_mut_node(node_id)?;
        match node {
            Node::Expression(exp) => Ok(exp),
            Node::Relational(_)
            | Node::Parameter
            | Node::Ddl(..)
            | Node::Acl(..)
            | Node::Block(..) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node ({node_id}) is not expression type: {node:?}").into()),
            )),
        }
    }

    /// Gets list of `Row` children ids
    ///
    /// # Errors
    /// - supplied id does not correspond to `Row` node
    pub fn get_row_list(&self, row_id: usize) -> Result<&[usize], SbroadError> {
        self.get_expression_node(row_id)?.get_row_list()
    }

    /// Helper function to get id of node under alias node,
    /// or return the given id if node is not an alias.
    ///
    /// # Errors
    /// - node is not an expression node
    pub fn get_child_under_alias(&self, child_id: usize) -> Result<usize, SbroadError> {
        match self.get_expression_node(child_id)? {
            Expression::Alias {
                child: alias_child, ..
            } => Ok(*alias_child),
            _ => Ok(child_id),
        }
    }

    /// Gets mut list of `Row` children ids
    ///
    /// # Errors
    /// - supplied id does not correspond to `Row` node
    pub fn get_mut_row_list(&mut self, row_id: usize) -> Result<&mut Vec<usize>, SbroadError> {
        self.get_mut_expression_node(row_id)?.get_mut_row_list()
    }

    /// Replace expression that is not root of the tree (== has parent)
    ///
    /// # Arguments
    /// * `parent_id` - id of the expression that is parent to expression being replaced
    /// * `old_id` - child of `parent_id` expression that is being replaced
    /// * `new_id` - id of expression that replaces `old_id` expression
    ///
    /// # Errors
    /// - invalid parent id
    /// - parent expression does not have specified child expression
    ///
    /// # Note
    /// This function assumes that parent expression does NOT have two or more
    /// children with the same id. So, if this happens, only one child will be replaced.
    pub fn replace_expression(
        &mut self,
        parent_id: usize,
        old_id: usize,
        new_id: usize,
    ) -> Result<(), SbroadError> {
        match self.get_mut_expression_node(parent_id)? {
            Expression::Unary { child, .. }
            | Expression::ExprInParentheses { child }
            | Expression::Alias { child, .. }
            | Expression::Cast { child, .. } => {
                if *child == old_id {
                    *child = new_id;
                    return Ok(());
                }
            }
            Expression::Bool { left, right, .. }
            | Expression::Arithmetic { left, right, .. }
            | Expression::Concat { left, right, .. } => {
                if *left == old_id {
                    *left = new_id;
                    return Ok(());
                }
                if *right == old_id {
                    *right = new_id;
                    return Ok(());
                }
            }
            Expression::Row { list: arr, .. } | StableFunction { children: arr, .. } => {
                for child in arr.iter_mut() {
                    if *child == old_id {
                        *child = new_id;
                        return Ok(());
                    }
                }
            }
            Expression::Constant { .. }
            | Expression::Reference { .. }
            | Expression::CountAsterisk => {}
        }
        Err(SbroadError::FailedTo(
            Action::Replace,
            Some(Entity::Expression),
            format!("parent expression ({parent_id}) has no child with id {old_id}").into(),
        ))
    }

    /// Gets `GroupBy` column by idx
    ///
    /// # Errors
    /// - supplied index is out of range
    /// - node is not `GroupBy`
    pub fn get_groupby_col(&self, groupby_id: usize, col_idx: usize) -> Result<usize, SbroadError> {
        let node = self.get_relation_node(groupby_id)?;
        if let Relational::GroupBy { gr_cols, .. } = node {
            let col_id = gr_cols.get(col_idx).ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(
                    format!("groupby column index out of range. Node: {node:?}").into(),
                )
            })?;
            return Ok(*col_id);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("Expected GroupBy node. Got: {node:?}").into()),
        ))
    }

    /// Gets `Projection` column by idx
    ///
    /// # Errors
    /// - supplied index is out of range
    /// - node is not `Projection`
    pub fn get_proj_col(&self, proj_id: usize, col_idx: usize) -> Result<usize, SbroadError> {
        let node = self.get_relation_node(proj_id)?;
        if let Relational::Projection { output, .. } = node {
            let col_id = self.get_row_list(*output)?.get(col_idx).ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(
                    format!("projection column index out of range. Node: {node:?}").into(),
                )
            })?;
            return Ok(*col_id);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("Expected Projection node. Got: {node:?}").into()),
        ))
    }

    /// Gets `GroupBy` columns
    ///
    /// # Errors
    /// - node is not `GroupBy`
    pub fn get_grouping_cols(&self, groupby_id: usize) -> Result<&[usize], SbroadError> {
        let node = self.get_relation_node(groupby_id)?;
        if let Relational::GroupBy { gr_cols, .. } = node {
            return Ok(gr_cols);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("Expected GroupBy node. Got: {node:?}").into()),
        ))
    }

    /// Gets `GroupBy` columns to specified columns
    ///
    /// # Errors
    /// - node is not `GroupBy`
    pub fn set_grouping_cols(
        &mut self,
        groupby_id: usize,
        new_cols: Vec<usize>,
    ) -> Result<(), SbroadError> {
        let node = self.get_mut_relation_node(groupby_id)?;
        if let Relational::GroupBy { gr_cols, .. } = node {
            *gr_cols = new_cols;
            return Ok(());
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("Expected GroupBy node. Got: {node:?}").into()),
        ))
    }

    /// Get alias string for `Reference` node
    ///
    /// # Errors
    /// - node doesn't exist in the plan
    /// - node is not `Reference`
    /// - invalid references between nodes
    pub fn get_alias_from_reference_node(&self, node: &Expression) -> Result<&str, SbroadError> {
        if let Expression::Reference {
            targets,
            position,
            parent,
            ..
        } = node
        {
            let ref_node = if let Some(parent) = parent {
                self.get_relation_node(*parent)?
            } else {
                return Err(SbroadError::UnexpectedNumberOfValues(
                    "Reference node has no parent".into(),
                ));
            };

            // In a case of insert we don't inspect children output tuple
            // but rather use target relation columns.
            if let Relational::Insert { ref relation, .. } = ref_node {
                let rel = self
                    .relations
                    .get(relation)
                    .ok_or_else(|| SbroadError::NotFound(Entity::Table, relation.to_smolstr()))?;
                let col_name = rel
                    .columns
                    .get(*position)
                    .ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Table,
                            "{relation}'s column {position}".into(),
                        )
                    })?
                    .name
                    .as_str();
                return Ok(col_name);
            }

            if let Some(list_of_column_nodes) = ref_node.children() {
                let child_ids = targets.as_ref().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Target,
                        Some("node refs to scan node, not alias".into()),
                    )
                })?;
                let column_index_in_list = child_ids.first().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues("Target has no children".into())
                })?;
                let col_idx_in_rel =
                    list_of_column_nodes
                        .get(*column_index_in_list)
                        .ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Node,
                                format!("type Column with index {column_index_in_list}").into(),
                            )
                        })?;

                let column_rel_node = self.get_relation_node(*col_idx_in_rel)?;
                let column_expr_node = self.get_expression_node(column_rel_node.output())?;

                let col_alias_idx =
                    column_expr_node
                        .get_row_list()?
                        .get(*position)
                        .ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Column,
                                format!("at position {position} in row list").into(),
                            )
                        })?;

                let col_alias_node = self.get_expression_node(*col_alias_idx)?;
                match col_alias_node {
                    Expression::Alias { name, .. } => return Ok(name),
                    _ => {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some("expected alias node".into()),
                        ))
                    }
                }
            }

            return Err(SbroadError::FailedTo(
                Action::Get,
                None,
                "a referred relational node".into(),
            ));
        }

        Err(SbroadError::Invalid(
            Entity::Node,
            Some("node is not of a reference type".into()),
        ))
    }

    /// Set slices of the plan.
    pub fn set_slices(&mut self, slices: Vec<Vec<usize>>) {
        self.slices = slices.into();
    }

    /// # Errors
    /// - serialization error (to binary)
    pub fn pattern_id(&self, top_id: usize) -> Result<String, SbroadError> {
        let mut dfs =
            PostOrder::with_capacity(|x| self.subtree_iter(x, false), self.nodes.next_id());
        dfs.populate_nodes(top_id);
        let nodes = dfs.take_nodes();
        let mut plan_nodes: Vec<&Node> = Vec::with_capacity(nodes.len());
        for (_, id) in nodes {
            plan_nodes.push(self.get_node(id)?);
        }
        let bytes: Vec<u8> = bincode::serialize(&plan_nodes).map_err(|e| {
            SbroadError::FailedTo(
                Action::Serialize,
                None,
                format!("plan nodes to binary: {e:?}").into(),
            )
        })?;
        let hash = Base64::encode_string(blake3::hash(&bytes).to_hex().as_bytes());
        Ok(hash)
    }
}

/// Relational node id -> positions of columns in output that refer to sharding column.
pub type ShardColInfo = ahash::AHashMap<NodeId, Vec<Position>>;

impl Plan {
    /// Helper function to track position of the sharding column
    /// for any relational node in the subtree defined by `top_id`.
    ///
    /// # Errors
    /// - invalid references in the plan subtree
    pub fn track_shard_column_pos(&self, top_id: usize) -> Result<ShardColInfo, SbroadError> {
        let mut memo = ShardColInfo::with_capacity(REL_CAPACITY);
        let mut dfs = PostOrder::with_capacity(|x| self.nodes.rel_iter(x), REL_CAPACITY);

        for (_, node_id) in dfs.iter(top_id) {
            let node = self.get_relation_node(node_id)?;

            match node {
                Relational::ScanRelation { relation, .. } => {
                    let table = self.get_relation_or_error(relation)?;
                    if let Ok(Some(pos)) = table.get_bucket_id_position() {
                        memo.insert(node_id, vec![pos]);
                    }
                    continue;
                }
                Relational::Motion { policy, .. } => {
                    // Any motion node that moves data invalidates
                    // bucket_id column selected from that space.
                    // Even Segment policy is no help, because it only
                    // creates index on virtual table but does not actually
                    // add or update bucket_id column.
                    if !matches!(policy, MotionPolicy::Local | MotionPolicy::LocalSegment(_)) {
                        continue;
                    }
                }
                _ => {}
            }

            let Some(children) = node.children() else {
                continue;
            };

            let output = self.get_row_list(node.output())?;
            for (pos, alias_id) in output.iter().enumerate() {
                let ref_id = self.get_child_under_alias(*alias_id)?;
                // If there is a parameter under alias
                // and we haven't bound parameters yet,
                // we will get an error.
                let Ok(Expression::Reference {
                    targets, position, ..
                }) = self.get_expression_node(ref_id)
                else {
                    continue;
                };
                let Some(targets) = targets else {
                    continue;
                };

                // For node with multiple targets (Union, Except, Intersect)
                // we need that ALL targets would refer to the shard column.
                let mut refers_to_shard_col = true;
                for target in targets {
                    let child_id = children.get(*target).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Plan,
                            Some(format!(
                                "invalid target ({target}) in reference with id: {ref_id}"
                            ).into()),
                        )
                    })?;
                    let Some(candidates) = memo.get(child_id) else {
                        refers_to_shard_col = false;
                        break;
                    };
                    if !candidates.contains(position) {
                        refers_to_shard_col = false;
                        break;
                    }
                }

                if refers_to_shard_col {
                    memo.entry(node_id)
                        .and_modify(|v| v.push(pos))
                        .or_insert(vec![pos]);
                }
            }
        }

        Ok(memo)
    }
}

pub mod api;
mod explain;
#[cfg(test)]
pub mod tests;
