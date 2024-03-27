use ahash::RandomState;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use std::collections::HashMap;
use std::mem::take;

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::ir::ExecutionPlan;
use crate::ir::expression::{Expression, FunctionFeature, TrimKind};
use crate::ir::operator::{Bool, Relational, Unary};
use crate::ir::transformation::redistribution::{MotionOpcode, MotionPolicy};
use crate::ir::tree::traversal::PostOrder;
use crate::ir::tree::Snapshot;
use crate::ir::Node;
use crate::otm::child_span;
use sbroad_proc::otm_child_span;

/// Payload of the syntax tree node.
#[derive(Clone, Deserialize, Debug, PartialEq, Eq, Serialize)]
pub enum SyntaxData {
    /// "as alias_name"
    Alias(SmolStr),
    /// "cast"
    Cast,
    /// ")"
    CloseParenthesis,
    /// "||"
    Concat,
    /// ","
    Comma,
    /// "on"
    Condition,
    /// "distinct"
    Distinct,
    /// Inline sql string
    Inline(String),
    /// "from"
    From,
    /// "leading"
    Leading,
    /// "both"
    Both,
    /// "trailing"
    Trailing,
    /// "("
    OpenParenthesis,
    /// "=, >, <, and, or, ..."
    Operator(String),
    /// plan node id
    PlanId(usize),
    /// parameter (a wrapper over a plan constants)
    Parameter(usize),
    /// virtual table (the key is a motion node id
    /// pointing to the execution plan's virtual table)
    VTable(usize),
}

/// A syntax tree node.
///
/// In order to understand the process of `left` (and `right`) fields filling
/// see `add_plan_node` function.
#[derive(Clone, Deserialize, Debug, PartialEq, Eq, Serialize)]
pub struct SyntaxNode {
    /// Payload
    pub(crate) data: SyntaxData,
    /// Pointer to the left node in the syntax tree. We keep it separate
    /// from "other" right nodes as we sometimes need it to be None, while
    /// other nodes have values (all children should be on the right of the
    /// current node in a case of in-order traversal - row or sub-query as
    /// an example).
    ///
    /// Literally the left node if we look at SQL query representation.
    /// It's `None` in case:
    /// * It's a first token in an SQL query.
    /// * `OrderedSyntaxNodes` `try_from` method made it so during traversal.
    pub(crate) left: Option<usize>,
    /// Pointers to the right children.
    ///
    /// Literally the right node if we look at SQL query representation.
    /// Sometimes this field may contain the node itself but converted from `Node` to `SyntaxNode` representation. E.g. see how
    /// `Expression::Bool` operator is added to `right` being transformed to `SyntaxNode::Operator` in `add_plan_node` function).
    pub(crate) right: Vec<usize>,
}

impl SyntaxNode {
    fn new_alias(name: SmolStr) -> Self {
        SyntaxNode {
            data: SyntaxData::Alias(name),
            left: None,
            right: Vec::new(),
        }
    }

    fn new_cast() -> Self {
        SyntaxNode {
            data: SyntaxData::Cast,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_close() -> Self {
        SyntaxNode {
            data: SyntaxData::CloseParenthesis,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_concat() -> Self {
        SyntaxNode {
            data: SyntaxData::Concat,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_comma() -> Self {
        SyntaxNode {
            data: SyntaxData::Comma,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_condition() -> Self {
        SyntaxNode {
            data: SyntaxData::Condition,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_distinct() -> Self {
        SyntaxNode {
            data: SyntaxData::Distinct,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_inline(value: &str) -> Self {
        SyntaxNode {
            data: SyntaxData::Inline(value.into()),
            left: None,
            right: Vec::new(),
        }
    }

    fn new_from() -> Self {
        SyntaxNode {
            data: SyntaxData::From,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_leading() -> Self {
        SyntaxNode {
            data: SyntaxData::Leading,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_both() -> Self {
        SyntaxNode {
            data: SyntaxData::Both,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_trailing() -> Self {
        SyntaxNode {
            data: SyntaxData::Trailing,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_operator(value: &str) -> Self {
        SyntaxNode {
            data: SyntaxData::Operator(value.into()),
            left: None,
            right: Vec::new(),
        }
    }

    fn new_open() -> Self {
        SyntaxNode {
            data: SyntaxData::OpenParenthesis,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_pointer(id: usize, left: Option<usize>, right: Vec<usize>) -> Self {
        SyntaxNode {
            data: SyntaxData::PlanId(id),
            left,
            right,
        }
    }

    fn new_parameter(id: usize) -> Self {
        SyntaxNode {
            data: SyntaxData::Parameter(id),
            left: None,
            right: Vec::new(),
        }
    }

    fn left_id_or_err(&self) -> Result<usize, SbroadError> {
        match self.left {
            Some(id) => Ok(id),
            None => Err(SbroadError::Invalid(
                Entity::Node,
                Some("left node is not set.".into()),
            )),
        }
    }

    fn new_vtable(motion_id: usize) -> Self {
        SyntaxNode {
            data: SyntaxData::VTable(motion_id),
            left: None,
            right: Vec::new(),
        }
    }
}

/// Storage for the syntax nodes.
#[derive(Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SyntaxNodes {
    pub(crate) arena: Vec<SyntaxNode>,
    /// Map of { node_id from `Plan` arena -> node_id from `SyntaxNodes`(Self) arena }.
    map: HashMap<usize, usize, RandomState>,
}

#[derive(Debug)]
pub struct SyntaxIterator<'n> {
    current: usize,
    child: usize,
    nodes: &'n SyntaxNodes,
}

impl<'n> SyntaxNodes {
    #[must_use]
    pub fn iter(&'n self, current: usize) -> SyntaxIterator<'n> {
        SyntaxIterator {
            current,
            child: 0,
            nodes: self,
        }
    }
}

impl<'n> Iterator for SyntaxIterator<'n> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        syntax_next(self).copied()
    }
}

fn syntax_next<'nodes>(iter: &mut SyntaxIterator<'nodes>) -> Option<&'nodes usize> {
    match iter.nodes.arena.get(iter.current) {
        Some(SyntaxNode { left, right, .. }) => {
            if iter.child == 0 {
                iter.child += 1;
                if let Some(left_id) = left {
                    return Some(left_id);
                }
            }
            let right_idx = iter.child - 1;
            if right_idx < right.len() {
                iter.child += 1;
                return Some(&right[right_idx]);
            }
            None
        }
        None => None,
    }
}

impl SyntaxNodes {
    /// Add sub-query syntax node
    ///
    /// # Errors
    /// - sub-query in plan tree is invalid
    fn add_sq(&mut self, rel: &Relational, id: usize) -> Result<usize, SbroadError> {
        if let Relational::ScanSubQuery {
            children, alias, ..
        } = rel
        {
            let right_id = *children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues("Sub-query has no children.".into())
            })?;
            let mut children: Vec<usize> = vec![
                self.push_syntax_node(SyntaxNode::new_open()),
                self.get_syntax_node_id(right_id)?,
                self.push_syntax_node(SyntaxNode::new_close()),
            ];
            if let Some(name) = alias {
                children.push(self.push_syntax_node(SyntaxNode::new_alias(name.clone())));
            }
            let sn = SyntaxNode::new_pointer(id, None, children);
            Ok(self.push_syntax_node(sn))
        } else {
            Err(SbroadError::Invalid(
                Entity::SyntaxNode,
                Some("current node is not a sub-query".into()),
            ))
        }
    }

    /// Construct syntax nodes from the YAML file.
    ///
    /// # Errors
    /// Returns `SbroadError` when the YAML nodes arena is invalid.
    #[allow(dead_code)]
    pub fn from_yaml(s: &str) -> Result<Self, SbroadError> {
        let nodes: SyntaxNodes = match serde_yaml::from_str(s) {
            Ok(p) => p,
            Err(e) => {
                return Err(SbroadError::FailedTo(
                    Action::Serialize,
                    Some(Entity::SyntaxNodes),
                    format!("{e:?}").into(),
                ))
            }
        };
        Ok(nodes)
    }

    /// Get a syntax node from arena
    ///
    /// # Errors
    /// - current node is invalid (doesn't exist in arena)
    pub fn get_syntax_node(&self, id: usize) -> Result<&SyntaxNode, SbroadError> {
        self.arena.get(id).ok_or_else(|| {
            SbroadError::NotFound(Entity::Node, format!("from arena with index {id}").into())
        })
    }

    /// Get a mutable syntax node from arena
    ///
    /// # Errors
    /// - current node is invalid (doesn't exist in arena)
    pub fn get_mut_syntax_node(&mut self, id: usize) -> Result<&mut SyntaxNode, SbroadError> {
        self.arena.get_mut(id).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(mutable) from arena with index {id}").into(),
            )
        })
    }

    /// Get syntax node id by the plan node's one
    ///
    /// # Errors
    /// - nothing was found
    fn get_syntax_node_id(&self, plan_id: usize) -> Result<usize, SbroadError> {
        self.map.get(&plan_id).copied().ok_or_else(|| {
            SbroadError::NotFound(Entity::Node, format!("({plan_id}) in the map").into())
        })
    }

    /// Push a new syntax node to arena
    pub fn push_syntax_node(&mut self, node: SyntaxNode) -> usize {
        let id = self.next_id();
        match node.data {
            SyntaxData::PlanId(plan_id) | SyntaxData::Parameter(plan_id) => {
                self.map.insert(plan_id, id);
            }
            _ => {}
        }
        self.arena.push(node);
        id
    }

    /// Get next node id
    #[must_use]
    pub fn next_id(&self) -> usize {
        self.arena.len()
    }

    /// Constructor with pre-allocated memory
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        SyntaxNodes {
            arena: Vec::with_capacity(capacity),
            map: HashMap::with_capacity_and_hasher(capacity, RandomState::new()),
        }
    }
}

/// Helper for `Selection` structure.
#[derive(Debug)]
enum Branch {
    Left,
    Right,
}

/// Keeps syntax node chain of the `SELECT` command:
/// projection, selection, scan and the upper node over
/// them all (parent).
#[derive(Debug)]
struct Select {
    /// The node over projection
    parent: Option<usize>,
    /// Parent's branch where projection was found
    branch: Option<Branch>,
    /// Projection syntax node
    proj: usize,
    /// Scan syntax node
    scan: usize,
}

impl Select {
    /// `Select` node constructor.
    ///
    /// There are several valid combinations of the `SELECT` command.
    /// The general view of all such commands is:
    /// `Projection` -> set of additional relational operators (possibly empty) -> `Scan`
    /// The main goal of this function is to find `Projection` and `Scan` nodes for
    /// further reordering of the syntax tree.
    fn new(
        sp: &SyntaxPlan,
        parent: Option<usize>,
        branch: Option<Branch>,
        id: usize,
    ) -> Result<Option<Select>, SbroadError> {
        let sn = sp.nodes.get_syntax_node(id)?;
        if let Some(Node::Relational(Relational::Projection { .. })) = sp.get_plan_node(&sn.data)? {
            let mut select = Select {
                parent,
                branch,
                proj: id,
                scan: 0,
            };

            // Iterate over the left branch subtree of the projection node to find
            // the scan node (leaf node without children).
            let mut node = sp.nodes.get_syntax_node(id)?;
            loop {
                let left_id = node.left_id_or_err()?;
                let sn_left = sp.nodes.get_syntax_node(left_id)?;
                let plan_node_left = sp.plan_node_or_err(&sn_left.data)?;
                if let Node::Relational(
                    Relational::ScanRelation { .. }
                    | Relational::ScanSubQuery { .. }
                    | Relational::Motion { .. },
                ) = plan_node_left
                {
                    select.scan = left_id;
                    break;
                }
                node = sn_left;
            }

            if select.scan != 0 {
                return Ok(Some(select));
            }
        }
        Ok(None)
    }
}

/// A wrapper over original plan tree.
/// We can modify it as we wish without any influence
/// on the original plan tree.
///
/// Example:
/// - Query: `SELECT "id" FROM "test_space"`
/// - `SyntaxPlan` (syntax node id -> plan node id):
///   5 -> 11 (`ScanRelation` (`"test_space"`))   <- `top` = 5
///   ├── 7 -> 15 (`Projection` (child = 11))     <- left
///   │   ├── None                                <- left
///   │   ├── 1 -> 13 (`Alias` (`"id"`))
///   │   │   ├── None                            <- left
///   │   │   └── 0 -> 12 (`Reference` (target = 15, position = 0))
///   │   │       ├── None                        <- left
///   │   │       └── []
///   │   └── 6 -> From
///   │       ├── None                            <- left
///   │       └── []
///   └── []
#[derive(Debug)]
pub struct SyntaxPlan<'p> {
    pub(crate) nodes: SyntaxNodes,
    /// Id of top `SyntaxNode`.
    pub(crate) top: Option<usize>,
    plan: &'p ExecutionPlan,
    snapshot: Snapshot,
}

#[allow(dead_code)]
impl<'p> SyntaxPlan<'p> {
    /// Add an IR plan node to the syntax tree.
    ///
    /// # Errors
    /// - Failed to translate an IR plan node to a syntax node.
    #[allow(clippy::too_many_lines)]
    #[allow(unused_variables)]
    pub fn add_plan_node(&mut self, id: usize) -> Result<usize, SbroadError> {
        let ir_plan = self.plan.get_ir_plan();
        let node = ir_plan.get_node(id)?;
        match node {
            Node::Ddl(..) => Err(SbroadError::Invalid(
                Entity::SyntaxPlan,
                Some(format!("DDL node {node:?} is not supported in the syntax plan").into()),
            )),
            Node::Acl(..) => Err(SbroadError::Invalid(
                Entity::SyntaxPlan,
                Some(format!("ACL node {node:?} is not supported in the syntax plan").into()),
            )),
            Node::Block(..) => Err(SbroadError::Invalid(
                Entity::SyntaxPlan,
                Some(format!("Block node {node:?} is not supported in the syntax plan").into()),
            )),
            Node::Parameter => {
                let sn = SyntaxNode::new_parameter(id);
                Ok(self.nodes.push_syntax_node(sn))
            }
            Node::Relational(rel) => match rel {
                Relational::Insert { .. }
                | Relational::Delete { .. }
                | Relational::Update { .. } => Err(SbroadError::Invalid(
                    Entity::SyntaxPlan,
                    Some(format!("DML node {node:?} is not supported in the syntax plan").into()),
                )),
                Relational::Join {
                    children,
                    condition,
                    ..
                } => {
                    let left_id = *children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Inner Join has no children.".into())
                    })?;
                    let right_id = *children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "that is Inner Join right child.".into(),
                        )
                    })?;
                    let condition_id = match self.snapshot {
                        Snapshot::Latest => *condition,
                        Snapshot::Oldest => *ir_plan
                            .undo
                            .get_oldest(condition)
                            .map_or_else(|| condition, |id| id),
                    };

                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(left_id)?),
                        vec![
                            self.nodes.get_syntax_node_id(right_id)?,
                            self.nodes.push_syntax_node(SyntaxNode::new_condition()),
                            self.nodes.get_syntax_node_id(condition_id)?,
                        ],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::Projection {
                    children, output, ..
                } => {
                    let left_id = *children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Projection has no children.".into())
                    })?;
                    // We don't need the row node itself, only its children.
                    // Otherwise we'll produce redundant parentheses between
                    // `SELECT` and `FROM`.
                    let expr = ir_plan.get_expression_node(*output)?;
                    if let Expression::Row { list, .. } = expr {
                        let mut nodes: Vec<usize> = Vec::with_capacity(list.len() * 2);
                        if let Some((last, elements)) = list.split_last() {
                            for elem in elements {
                                nodes.push(self.nodes.get_syntax_node_id(*elem)?);
                                nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_comma()));
                            }
                            nodes.push(self.nodes.get_syntax_node_id(*last)?);
                            nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_from()));
                            let sn = SyntaxNode::new_pointer(
                                id,
                                Some(self.nodes.get_syntax_node_id(left_id)?),
                                nodes,
                            );
                            return Ok(self.nodes.push_syntax_node(sn));
                        }
                    }
                    Err(SbroadError::Invalid(Entity::Node, None))
                }
                Relational::ScanSubQuery { .. } => self.nodes.add_sq(rel, id),
                Relational::GroupBy {
                    children, gr_cols, ..
                } => {
                    let left_id = *children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("GroupBy has no children.".into())
                    })?;
                    let mut right: Vec<usize> = Vec::with_capacity(gr_cols.len() * 2);
                    if let Some((last, other)) = gr_cols.split_last() {
                        for col_id in other {
                            right.push(self.nodes.get_syntax_node_id(*col_id)?);
                            right.push(self.nodes.push_syntax_node(SyntaxNode::new_comma()));
                        }
                        right.push(self.nodes.get_syntax_node_id(*last)?);
                    }
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(left_id)?),
                        right,
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::Selection {
                    children, filter, ..
                }
                | Relational::Having {
                    children, filter, ..
                } => {
                    let left_id = *children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            format!("{node:?} has no children.").into(),
                        )
                    })?;
                    let filter_id = match self.snapshot {
                        Snapshot::Latest => *filter,
                        Snapshot::Oldest => *ir_plan
                            .undo
                            .get_oldest(filter)
                            .map_or_else(|| filter, |id| id),
                    };
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(left_id)?),
                        vec![self.nodes.get_syntax_node_id(filter_id)?],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::Except { children, .. }
                | Relational::UnionAll { children, .. }
                | Relational::Intersect { children, .. } => {
                    let left_id = *children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Union/Except/Intersect has no children.".into(),
                        )
                    })?;
                    let right_id = *children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "that is Union/Except/Intersect right child.".into(),
                        )
                    })?;
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(left_id)?),
                        vec![self.nodes.get_syntax_node_id(right_id)?],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::ScanRelation { alias, .. } => {
                    let children: Vec<usize> = if let Some(name) = alias {
                        vec![self
                            .nodes
                            .push_syntax_node(SyntaxNode::new_alias(name.clone()))]
                    } else {
                        Vec::new()
                    };
                    let sn = SyntaxNode::new_pointer(id, None, children);
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::Motion {
                    policy,
                    children,
                    is_child_subquery,
                    program,
                    output,
                    ..
                } => {
                    if let MotionPolicy::LocalSegment { .. } = policy {
                        #[cfg(feature = "mock")]
                        {
                            // We should materialize the subquery on the storage.
                            // Honestly, this SQL is not valid and should never be
                            // generated in runtime, but let's leave it for testing.
                            if let Some(child_id) = children.first() {
                                let sn = SyntaxNode::new_pointer(
                                    id,
                                    Some(self.nodes.get_syntax_node_id(*child_id)?),
                                    vec![],
                                );
                                return Ok(self.nodes.push_syntax_node(sn));
                            }
                        }
                        #[cfg(not(feature = "mock"))]
                        {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some(
                                    "LocalSegment motion policy is not supported in the syntax plan."
                                        .into(),
                                ),
                            ));
                        }
                    }
                    if let Some(op) = program
                        .0
                        .iter()
                        .find(|op| matches!(op, MotionOpcode::SerializeAsEmptyTable(_)))
                    {
                        let is_enabled = matches!(op, MotionOpcode::SerializeAsEmptyTable(true));
                        if is_enabled {
                            let output_len = self.plan.get_ir_plan().get_row_list(*output)?.len();
                            let empty_select = format!(
                                "select {}null where false",
                                "null, ".repeat(output_len - 1)
                            );
                            let inline_id = self
                                .nodes
                                .push_syntax_node(SyntaxNode::new_inline(&empty_select));
                            let pointer_id = self.nodes.push_syntax_node(SyntaxNode::new_pointer(
                                id,
                                None,
                                vec![inline_id],
                            ));
                            return Ok(pointer_id);
                        }
                        let child_plan_id = self.plan.get_ir_plan().get_relational_child(id, 0)?;
                        let child_sp_id = *self.nodes.map.get(&child_plan_id).ok_or_else(|| {
                            SbroadError::Invalid(
                                Entity::SyntaxPlan,
                                Some(
                                    format!("motion child {child_plan_id} is not found in map")
                                        .into(),
                                ),
                            )
                        })?;
                        self.nodes.map.insert(id, child_sp_id);
                        return Ok(child_sp_id);
                    }
                    let vtable = self.plan.get_motion_vtable(id)?;
                    let vtable_alias = vtable.get_alias();

                    // There are some cases when motion child is not a `SubQuery` and when
                    // it has an alias. E.g. in case of a `INSERT ... SELECT ...` its child
                    // may be a `Projection.
                    let children = if *is_child_subquery || vtable_alias.is_some() {
                        let mut children: Vec<usize> = vec![
                            self.nodes.push_syntax_node(SyntaxNode::new_open()),
                            self.nodes.push_syntax_node(SyntaxNode::new_vtable(id)),
                            self.nodes.push_syntax_node(SyntaxNode::new_close()),
                        ];

                        if let Some(name) = vtable_alias {
                            if name.is_empty() {
                                return Err(SbroadError::Invalid(
                                    Entity::VirtualTable,
                                    Some(
                                        format!("Vtable {vtable:?} has an empty alias name").into(),
                                    ),
                                ));
                            }
                            children.push(
                                self.nodes
                                    .push_syntax_node(SyntaxNode::new_alias(name.clone())),
                            );
                        }
                        children
                    } else {
                        vec![self.nodes.push_syntax_node(SyntaxNode::new_vtable(id))]
                    };

                    let sn = SyntaxNode::new_pointer(id, None, children);
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::ValuesRow { data, .. } => {
                    let sn = SyntaxNode::new_pointer(
                        id,
                        None,
                        vec![self.nodes.get_syntax_node_id(*data)?],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Relational::Values { children, .. } => {
                    let mut sn_children: Vec<usize> = Vec::with_capacity(children.len() * 2);
                    if let Some((last_id, other)) = children.split_last() {
                        for child_id in other {
                            sn_children.push(self.nodes.get_syntax_node_id(*child_id)?);
                            sn_children.push(self.nodes.push_syntax_node(SyntaxNode::new_comma()));
                        }
                        sn_children.push(self.nodes.get_syntax_node_id(*last_id)?);
                    }

                    let sn = SyntaxNode::new_pointer(id, None, sn_children);
                    Ok(self.nodes.push_syntax_node(sn))
                }
            },
            Node::Expression(expr) => match expr {
                Expression::ExprInParentheses { child } => {
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.push_syntax_node(SyntaxNode::new_open())),
                        vec![
                            self.nodes.get_syntax_node_id(*child)?,
                            self.nodes.push_syntax_node(SyntaxNode::new_close()),
                        ],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Cast { child, to } => {
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.push_syntax_node(SyntaxNode::new_cast())),
                        vec![
                            self.nodes.push_syntax_node(SyntaxNode::new_open()),
                            self.nodes.get_syntax_node_id(*child)?,
                            self.nodes
                                .push_syntax_node(SyntaxNode::new_alias(SmolStr::from(to))),
                            self.nodes.push_syntax_node(SyntaxNode::new_close()),
                        ],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Concat { left, right } => {
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(*left)?),
                        vec![
                            self.nodes.push_syntax_node(SyntaxNode::new_concat()),
                            self.nodes.get_syntax_node_id(*right)?,
                        ],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Constant { .. } => {
                    let sn = SyntaxNode::new_parameter(id);
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Reference { .. } | Expression::CountAsterisk => {
                    let sn = SyntaxNode::new_pointer(id, None, vec![]);
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Alias { child, name, .. } => {
                    // Do not generate an alias in SQL when a column has exactly the same name.
                    let child_expr = ir_plan.get_expression_node(*child)?;
                    if let Expression::Reference { .. } = child_expr {
                        let alias = &ir_plan.get_alias_from_reference_node(child_expr)?;
                        if alias == name {
                            let sn = SyntaxNode::new_pointer(
                                id,
                                None,
                                vec![self.nodes.get_syntax_node_id(*child)?],
                            );
                            return Ok(self.nodes.push_syntax_node(sn));
                        }
                    }
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(*child)?),
                        vec![self
                            .nodes
                            .push_syntax_node(SyntaxNode::new_alias(name.clone()))],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Row { list, .. } => {
                    // In projections with a huge amount of columns it can be
                    // very expensive to retrieve corresponding relational nodes.
                    let rel_ids = ir_plan.get_relational_nodes_from_row(id)?;

                    if let Some(motion_id) = ir_plan.get_motion_among_rel_nodes(&rel_ids)? {
                        // Logic of replacing row child with vtable (corresponding to motion) is
                        // applicable only in case the child is Reference appeared from transformed
                        // SubQuery (Like in case `Exists` or `In` operator or in expression like
                        // `select * from t where b = (select a from t)`).
                        // There are other cases of row containing references to `Motion` nodes when
                        // we shouldn't replace them with vtable (e.g. aggregates' stable functions
                        // which arguments may point to `Motion` node).
                        let first_child_id = *list.first().ok_or_else(|| {
                            SbroadError::Invalid(
                                Entity::Expression,
                                Some(SmolStr::from("Row node should have child list node")),
                            )
                        })?;
                        let first_child = ir_plan.get_expression_node(first_child_id)?;
                        let first_child_is_ref =
                            matches!(first_child, Expression::Reference { .. });

                        // Replace motion node to virtual table node
                        let vtable = self.plan.get_motion_vtable(motion_id)?;
                        let needs_replacement = vtable.get_alias().is_none()
                            && first_child_is_ref
                            && ir_plan.is_additional_child(motion_id)?;
                        if needs_replacement {
                            let sn = SyntaxNode::new_pointer(
                                id,
                                None,
                                vec![
                                    self.nodes.push_syntax_node(SyntaxNode::new_open()),
                                    self.nodes
                                        .push_syntax_node(SyntaxNode::new_vtable(motion_id)),
                                    self.nodes.push_syntax_node(SyntaxNode::new_close()),
                                ],
                            );

                            return Ok(self.nodes.push_syntax_node(sn));
                        }
                    }

                    if let Some(sq_id) = ir_plan.get_sub_query_among_rel_nodes(&rel_ids)? {
                        // Replace current row with the referred sub-query
                        // (except the case when sub-query is located in the FROM clause).
                        if ir_plan.is_additional_child(sq_id)? {
                            let rel = ir_plan.get_relation_node(sq_id)?;
                            return self.nodes.add_sq(rel, id);
                        }
                    }
                    let mut nodes: Vec<usize> =
                        vec![self.nodes.push_syntax_node(SyntaxNode::new_open())];
                    if let Some((last, elements)) = list.split_last() {
                        nodes.reserve(list.len() * 2);
                        for elem in elements {
                            nodes.push(self.nodes.get_syntax_node_id(*elem)?);
                            nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_comma()));
                        }
                        nodes.push(self.nodes.get_syntax_node_id(*last)?);
                        nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_close()));
                        let sn = SyntaxNode::new_pointer(id, None, nodes);
                        return Ok(self.nodes.push_syntax_node(sn));
                    }
                    Err(SbroadError::Invalid(Entity::Expression, None))
                }
                Expression::Bool {
                    left, right, op, ..
                } => {
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(*left)?),
                        vec![
                            self.nodes
                                .push_syntax_node(SyntaxNode::new_operator(&format!("{op}"))),
                            self.nodes.get_syntax_node_id(*right)?,
                        ],
                    );
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Arithmetic { left, right, op } => {
                    let sn = SyntaxNode::new_pointer(
                        id,
                        Some(self.nodes.get_syntax_node_id(*left)?),
                        vec![
                            self.nodes
                                .push_syntax_node(SyntaxNode::new_operator(&format!("{op}"))),
                            self.nodes.get_syntax_node_id(*right)?,
                        ],
                    );

                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::Unary { child, op, .. } => {
                    let operator_node_id = self
                        .nodes
                        .push_syntax_node(SyntaxNode::new_operator(&format!("{op}")));
                    let child_node_id = self.nodes.get_syntax_node_id(*child)?;
                    let child_node = ir_plan.get_expression_node(*child)?;
                    // Bool::Or operator already covers itself with parentheses, that's why we
                    // don't have to cover it here.
                    let sn = if *op == Unary::Not
                        && matches!(*child_node, Expression::Bool { op: Bool::And, .. })
                    {
                        SyntaxNode::new_pointer(
                            id,
                            Some(operator_node_id),
                            vec![
                                self.nodes.push_syntax_node(SyntaxNode::new_open()),
                                child_node_id,
                                self.nodes.push_syntax_node(SyntaxNode::new_close()),
                            ],
                        )
                    } else {
                        let (left, right) = match op {
                            Unary::IsNull => (child_node_id, operator_node_id),
                            Unary::Exists | Unary::Not => (operator_node_id, child_node_id),
                        };
                        SyntaxNode::new_pointer(id, Some(left), vec![right])
                    };
                    Ok(self.nodes.push_syntax_node(sn))
                }
                Expression::StableFunction {
                    children, feature, ..
                } => {
                    let mut nodes: Vec<usize> =
                        vec![self.nodes.push_syntax_node(SyntaxNode::new_open())];
                    if let Some(FunctionFeature::Trim(kind)) = feature {
                        // `trim` function has a special format. For instance, here how we can
                        // call it: trim(leading 'a' from 'ab').
                        match kind {
                            TrimKind::Leading => {
                                nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_leading()));
                            }
                            TrimKind::Trailing => {
                                nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_trailing()));
                            }
                            TrimKind::Both => {
                                nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_both()));
                            }
                        }

                        if let Some((string, removal_chars)) = children.split_last() {
                            for child in removal_chars {
                                nodes.push(self.nodes.get_syntax_node_id(*child)?);
                            }
                            nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_from()));
                            nodes.push(self.nodes.get_syntax_node_id(*string)?);
                        }
                    } else {
                        if let Some(FunctionFeature::Distinct) = feature {
                            nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_distinct()));
                        }
                        if let Some((last, others)) = children.split_last() {
                            for child in others {
                                nodes.push(self.nodes.get_syntax_node_id(*child)?);
                                nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_comma()));
                            }
                            nodes.push(self.nodes.get_syntax_node_id(*last)?);
                        }
                    }

                    nodes.push(self.nodes.push_syntax_node(SyntaxNode::new_close()));
                    let sn = SyntaxNode::new_pointer(id, None, nodes);
                    Ok(self.nodes.push_syntax_node(sn))
                }
            },
        }
    }

    /// Get the plan node from the syntax tree node.
    ///
    /// # Errors
    /// - plan node is invalid
    pub fn get_plan_node(&self, data: &SyntaxData) -> Result<Option<&Node>, SbroadError> {
        if let SyntaxData::PlanId(id) = data {
            Ok(Some(self.plan.get_ir_plan().get_node(*id)?))
        } else {
            Ok(None)
        }
    }

    /// Get the plan node from the syntax tree node or fail.
    ///
    /// # Errors
    /// - plan node is invalid
    /// - syntax tree node doesn't have a plan node
    pub fn plan_node_or_err(&self, data: &SyntaxData) -> Result<&Node, SbroadError> {
        self.get_plan_node(data)?.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::SyntaxPlan,
                Some("Plan node is not found in syntax tree".into()),
            )
        })
    }

    /// Set top of the tree.
    ///
    /// # Errors
    /// - top is invalid node
    pub fn set_top(&mut self, top: usize) -> Result<(), SbroadError> {
        self.nodes.get_syntax_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get the top of the syntax tree.
    ///
    /// # Errors
    /// - top is not set
    /// - top is not a valid node
    pub fn get_top(&self) -> Result<usize, SbroadError> {
        if let Some(top) = self.top {
            self.nodes.get_syntax_node(top)?;
            Ok(top)
        } else {
            Err(SbroadError::Invalid(
                Entity::SyntaxPlan,
                Some("Syntax tree has an invalid top.".into()),
            ))
        }
    }

    /// Gather all projections with auxiliary nodes (scan, selection, parent)
    /// among the syntax tree.
    ///
    /// # Errors
    /// - got unexpected nodes under projection
    fn gather_selects(&self) -> Result<Option<Vec<Select>>, SbroadError> {
        let mut selects: Vec<Select> = Vec::new();
        let top = self.get_top()?;
        let mut dfs = PostOrder::with_capacity(
            |node| self.nodes.iter(node),
            self.plan.get_ir_plan().nodes.len(),
        );
        dfs.populate_nodes(top);
        let nodes = dfs.take_nodes();
        for (_, pos) in nodes {
            let node = self.nodes.get_syntax_node(pos)?;
            if pos == top {
                let select = Select::new(self, None, None, pos)?;
                if let Some(s) = select {
                    selects.push(s);
                }
            }
            if let Some(left) = node.left {
                let select = Select::new(self, Some(pos), Some(Branch::Left), left)?;
                if let Some(s) = select {
                    selects.push(s);
                }
            }
            for right in &node.right {
                let select = Select::new(self, Some(pos), Some(Branch::Right), *right)?;
                if let Some(s) = select {
                    selects.push(s);
                }
            }
        }

        if selects.is_empty() {
            Ok(None)
        } else {
            Ok(Some(selects))
        }
    }

    /// Move projection nodes under their scans
    ///
    /// # Errors
    /// - got unexpected nodes under some projection
    fn move_proj_under_scan(&mut self) -> Result<(), SbroadError> {
        let selects = self.gather_selects()?;
        if let Some(selects) = selects {
            for select in &selects {
                self.reorder(select)?;
            }
        }
        Ok(())
    }

    pub(crate) fn empty(plan: &'p ExecutionPlan) -> Self {
        SyntaxPlan {
            nodes: SyntaxNodes::with_capacity(plan.get_ir_plan().next_id() * 2),
            top: None,
            plan,
            snapshot: Snapshot::Latest,
        }
    }

    /// Build a new syntax tree from the execution plan.
    ///
    /// # Errors
    /// - Failed to ad an IR plan to the syntax tree
    /// - Failed to get to the top of the syntax tree
    /// - Failed to move projection nodes under their scans
    #[otm_child_span("syntax.new")]
    pub fn new(
        plan: &'p ExecutionPlan,
        top: usize,
        snapshot: Snapshot,
    ) -> Result<Self, SbroadError> {
        let mut sp = SyntaxPlan::empty(plan);
        sp.snapshot = snapshot.clone();
        let ir_plan = plan.get_ir_plan();

        // Wrap plan's nodes and preserve their ids.
        let capacity = ir_plan.next_id();
        match snapshot {
            Snapshot::Latest => {
                let mut dft_post =
                    PostOrder::with_capacity(|node| ir_plan.subtree_iter(node, false), capacity);
                for (_, id) in dft_post.iter(top) {
                    // it works only for post-order traversal
                    let sn_id = sp.add_plan_node(id)?;
                    if id == top {
                        sp.set_top(sn_id)?;
                    }
                }
            }
            Snapshot::Oldest => {
                let mut dft_post =
                    PostOrder::with_capacity(|node| ir_plan.flashback_subtree_iter(node), capacity);
                for (_, id) in dft_post.iter(top) {
                    // it works only for post-order traversal
                    let sn_id = sp.add_plan_node(id)?;
                    if id == top {
                        sp.set_top(sn_id)?;
                    }
                }
            }
        }
        sp.move_proj_under_scan()?;
        Ok(sp)
    }

    fn reorder(&mut self, select: &Select) -> Result<(), SbroadError> {
        // Move projection under scan.
        let proj = self.nodes.get_mut_syntax_node(select.proj)?;
        let new_top = proj.left.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::SyntaxPlan,
                Some("Proj syntax node does not have left child!".into()),
            )
        })?;
        proj.left = None;
        let scan = self.nodes.get_mut_syntax_node(select.scan)?;
        scan.left = Some(select.proj);

        // Try to move new top under parent.
        if let Some(id) = select.parent {
            let parent = self.nodes.get_mut_syntax_node(id)?;
            match select.branch {
                Some(Branch::Left) => {
                    parent.left = Some(new_top);
                }
                Some(Branch::Right) => {
                    let mut found: bool = false;
                    for child in &mut parent.right {
                        if child == &select.proj {
                            *child = new_top;
                            found = true;
                        }
                    }
                    if !found {
                        return Err(SbroadError::Invalid(
                            Entity::SyntaxNode,
                            Some(
                                "Parent node doesn't contain projection in its right children"
                                    .into(),
                            ),
                        ));
                    }
                }
                None => {
                    return Err(SbroadError::Invalid(
                        Entity::SyntaxNode,
                        Some("Selection structure is in inconsistent state.".into()),
                    ))
                }
            }
        }

        // Update the syntax plan top if it was current projection
        if self.get_top()? == select.proj {
            self.set_top(new_top)?;
        }

        Ok(())
    }
}

/// Wrapper over `SyntaxNode` `arena` that is used for converting it to SQL.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct OrderedSyntaxNodes {
    arena: Vec<SyntaxNode>,
    /// Indices of nodes from `arena`. During the conversion to SQL the order of nodes from
    /// `positions` is the order they will appear in SQL string representation.
    positions: Vec<usize>,
}

impl OrderedSyntaxNodes {
    /// Constructs a vector of the syntax node pointers in an order, suitable for building
    /// an SQL query (in-order traversal).
    ///
    /// # Errors
    /// - internal error (positions point to invalid nodes in the arena)
    pub fn to_syntax_data(&self) -> Result<Vec<&SyntaxData>, SbroadError> {
        let mut result: Vec<&SyntaxData> = Vec::with_capacity(self.positions.len());
        for id in &self.positions {
            result.push(
                &self
                    .arena
                    .get(*id)
                    .ok_or_else(|| {
                        SbroadError::NotFound(Entity::SyntaxNode, format!("(id {id})").into())
                    })?
                    .data,
            );
        }
        Ok(result)
    }

    #[must_use]
    pub fn empty() -> Self {
        OrderedSyntaxNodes {
            arena: Vec::new(),
            positions: Vec::new(),
        }
    }
}

impl TryFrom<SyntaxPlan<'_>> for OrderedSyntaxNodes {
    type Error = SbroadError;

    #[otm_child_span("syntax.ordered")]
    fn try_from(mut sp: SyntaxPlan) -> Result<Self, Self::Error> {
        // Result with plan node ids.
        let mut positions: Vec<usize> = Vec::with_capacity(sp.nodes.arena.len());
        // Stack to keep syntax node data.
        let mut stack: Vec<usize> = Vec::with_capacity(sp.nodes.arena.len());

        // Make a destructive in-order traversal over the syntax plan
        // nodes (left and right pointers for any wrapped node become
        // None or removed). It seems to be the fastest traversal
        // approach in Rust (`take()` and `pop()`).
        stack.push(sp.get_top()?);
        while let Some(id) = stack.last() {
            let sn = sp.nodes.get_mut_syntax_node(*id)?;
            // Note that in case `left` is a `Some(...)`, call of `take` will make it None.
            if let Some(left_id) = sn.left.take() {
                stack.push(left_id);
            } else if let Some(id) = stack.pop() {
                positions.push(id);
                let sn_next = sp.nodes.get_mut_syntax_node(id)?;
                while let Some(right_id) = sn_next.right.pop() {
                    stack.push(right_id);
                }
            }
        }

        let arena: Vec<SyntaxNode> = take(&mut sp.nodes.arena);
        Ok(Self { arena, positions })
    }
}

#[cfg(test)]
mod tests;
