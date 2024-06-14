use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr};
use std::mem::take;

use crate::errors::{Entity, SbroadError};
use crate::executor::ir::ExecutionPlan;
use crate::ir::expression::{Expression, FunctionFeature, TrimKind};
use crate::ir::operator::{Bool, OrderByElement, OrderByEntity, OrderByType, Relational, Unary};
use crate::ir::transformation::redistribution::{MotionOpcode, MotionPolicy};
use crate::ir::tree::traversal::PostOrder;
use crate::ir::tree::Snapshot;
use crate::ir::{Node, Plan};
use crate::otm::child_span;
use sbroad_proc::otm_child_span;

/// Payload of the syntax tree node.
#[derive(Clone, Deserialize, Debug, PartialEq, Eq, Serialize)]
pub enum SyntaxData {
    /// "as \"alias_name\""
    Alias(SmolStr),
    /// "as alias_name"
    UnquotedAlias(SmolStr),
    /// "cast"
    Cast,
    // "case"
    Case,
    // "when"
    When,
    // "then"
    Then,
    // "else"
    Else,
    // "end"
    End,
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
    /// ORDER BY `position`.
    /// The reason it's represented as a standalone node (and not `Expression::Constant`)
    /// is that we don't want it to be replaced as parameter in pattern later
    /// Order by index is semantically different from usual constants and the following
    /// local SQL queries are not equal:
    /// `select * from t order by 1`
    /// !=
    /// `select * from t order by ?` with {1} params (1 is perceived as expression and not position).
    OrderByPosition(usize),
    /// "asc" or "desc"
    OrderByType(OrderByType),
    /// Inline sql string
    Inline(SmolStr),
    /// "from"
    From,
    /// "leading"
    Leading,
    /// "limit"
    Limit(u64),
    /// "both"
    Both,
    /// "trailing"
    Trailing,
    /// "trim"
    Trim,
    /// "("
    OpenParenthesis,
    /// "=, >, <, and, or, ..."
    Operator(SmolStr),
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

    fn new_unquoted_alias(name: SmolStr) -> Self {
        SyntaxNode {
            data: SyntaxData::UnquotedAlias(name),
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

    fn new_case() -> Self {
        SyntaxNode {
            data: SyntaxData::Case,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_end() -> Self {
        SyntaxNode {
            data: SyntaxData::End,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_when() -> Self {
        SyntaxNode {
            data: SyntaxData::When,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_then() -> Self {
        SyntaxNode {
            data: SyntaxData::Then,
            left: None,
            right: Vec::new(),
        }
    }

    fn new_else() -> Self {
        SyntaxNode {
            data: SyntaxData::Else,
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

    fn new_order_index(index: usize) -> Self {
        SyntaxNode {
            data: SyntaxData::OrderByPosition(index),
            left: None,
            right: Vec::new(),
        }
    }

    fn new_order_type(order_type: &OrderByType) -> Self {
        SyntaxNode {
            data: SyntaxData::OrderByType(order_type.clone()),
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

    fn new_limit(limit: u64) -> Self {
        SyntaxNode {
            data: SyntaxData::Limit(limit),
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

    fn new_trim() -> Self {
        SyntaxNode {
            data: SyntaxData::Trim,
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
    /// Syntax node tree (positions in the vector act like identifiers).
    pub(crate) arena: Vec<SyntaxNode>,
    /// A stack with syntax node identifiers (required on the tree building step).
    stack: Vec<usize>,
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
    fn get_sn(&self, id: usize) -> &SyntaxNode {
        self.arena
            .get(id)
            .unwrap_or_else(|| panic!("syntax node with id {id} must exist"))
    }

    fn get_mut_sn(&mut self, id: usize) -> &mut SyntaxNode {
        self.arena
            .get_mut(id)
            .unwrap_or_else(|| panic!("syntax node with id {id} must exist"))
    }

    fn push_sn_plan(&mut self, node: SyntaxNode) -> usize {
        let id = self.next_id();
        match node.data {
            SyntaxData::PlanId(_) | SyntaxData::Parameter(_) => self.stack.push(id),
            _ => {
                unreachable!("Expected a plan node wrapper.");
            }
        }
        self.arena.push(node);
        id
    }

    fn push_sn_non_plan(&mut self, node: SyntaxNode) -> usize {
        let id = self.next_id();
        assert!(!matches!(
            node.data,
            SyntaxData::PlanId(_) | SyntaxData::Parameter(_)
        ));
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
        let depth = capacity.ilog2() as usize;
        SyntaxNodes {
            arena: Vec::with_capacity(capacity),
            stack: Vec::with_capacity(depth),
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
        let sn = sp.nodes.get_sn(id);
        if let Some(Node::Relational(Relational::Projection { .. })) = sp.get_plan_node(&sn.data)? {
            let mut select = Select {
                parent,
                branch,
                proj: id,
                scan: 0,
            };

            // Iterate over the left branch subtree of the projection node to find
            // the scan node (leaf node without children).
            let mut node = sp.nodes.get_sn(id);
            loop {
                let left_id = node.left_id_or_err()?;
                let sn_left = sp.nodes.get_sn(left_id);
                let plan_node_left = sp.plan_node_or_err(&sn_left.data)?;
                if let Node::Relational(
                    Relational::ScanRelation { .. }
                    | Relational::ScanCte { .. }
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
    /// Checks that the syntax node wraps an expected plan node identifier.
    ///
    /// # Panics
    /// - syntax node wraps non-plan node;
    /// - plan node is not the one we expect;
    fn check_plan_node(&self, sn_id: usize, plan_id: usize) {
        let sn = self.nodes.get_sn(sn_id);
        let SyntaxNode {
            data: SyntaxData::PlanId(id) | SyntaxData::Parameter(id),
            ..
        } = sn
        else {
            panic!("Expected plan syntax node");
        };
        if *id != plan_id {
            // If it is a motion and we need its child instead.
            let plan = self.plan.get_ir_plan();
            let node = plan.get_node(plan_id).expect("node in the plan must exist");
            match node {
                Node::Relational(Relational::Motion { children, .. }) => {
                    let child_id = *children.first().expect("MOTION child must exist");
                    if *id == child_id {
                        return;
                    }
                }
                Node::Expression(Expression::Row { .. }) => {
                    let rel_ids = plan
                        .get_relational_nodes_from_row(plan_id)
                        .expect("row relational nodes");
                    if rel_ids.contains(id) {
                        return;
                    }
                }
                _ => {}
            }
            panic!("Expected plan node {plan_id} but got {id}");
        }
    }

    /// Push a syntax node identifier on the top of the stack.
    fn push_on_stack(&mut self, sn_id: usize) {
        self.nodes.stack.push(sn_id);
    }

    /// Pop syntax node identifier from the stack with a check, that the popped syntax
    /// node wraps an expected plan node.
    fn pop_from_stack(&mut self, plan_id: usize) -> usize {
        let sn_id = self.nodes.stack.pop().expect("stack must be non-empty");
        self.check_plan_node(sn_id, plan_id);
        sn_id
    }

    /// Add an IR plan node to the syntax tree. Keep in mind, that this function expects
    /// that the plan node's children are iterated with `subtree_iter()` on traversal.
    /// As a result, the syntax node children on the stack should be popped in the reverse
    /// order.
    ///
    /// # Errors
    /// - Failed to translate an IR plan node to a syntax node.
    ///
    /// # Panics
    /// - Failed to find a node in the plan.
    #[allow(clippy::too_many_lines)]
    #[allow(unused_variables)]
    pub fn add_plan_node(&mut self, id: usize) {
        let ir_plan = self.plan.get_ir_plan();
        let node = ir_plan
            .get_node(id)
            .expect("node {id} must exist in the plan");
        match node {
            Node::Ddl(..) => panic!("DDL node {node:?} is not supported in the syntax plan"),
            Node::Acl(..) => panic!("ACL node {node:?} is not supported in the syntax plan"),
            Node::Block(..) => panic!("Block node {node:?} is not supported in the syntax plan"),
            Node::Parameter(..) => {
                let sn = SyntaxNode::new_parameter(id);
                self.nodes.push_sn_plan(sn);
            }
            Node::Relational(rel) => match rel {
                Relational::Insert { .. }
                | Relational::Delete { .. }
                | Relational::Update { .. } => {
                    panic!("DML node {node:?} is not supported in the syntax plan")
                }
                Relational::Join { .. } => self.add_join(id),
                Relational::Projection { .. } => self.add_proj(id),
                Relational::OrderBy { .. } => self.add_order_by(id),
                Relational::ScanCte { .. } => self.add_cte(id),
                Relational::ScanSubQuery { .. } => self.add_sq(id),
                Relational::GroupBy { .. } => self.add_group_by(id),
                Relational::Selection { .. } | Relational::Having { .. } => self.add_filter(id),
                Relational::Except { .. }
                | Relational::Union { .. }
                | Relational::UnionAll { .. }
                | Relational::Intersect { .. } => self.add_set(id),
                Relational::ScanRelation { .. } => self.add_scan_relation(id),
                Relational::Motion { .. } => self.add_motion(id),
                Relational::ValuesRow { .. } => self.add_values_row(id),
                Relational::Values { .. } => self.add_values(id),
                Relational::Limit { .. } => self.add_limit(id),
            },
            Node::Expression(expr) => match expr {
                Expression::ExprInParentheses { .. } => self.add_expr_in_parentheses(id),
                Expression::Cast { .. } => self.add_cast(id),
                Expression::Case { .. } => self.add_case(id),
                Expression::Concat { .. } => self.add_concat(id),
                Expression::Constant { .. } => {
                    let sn = SyntaxNode::new_parameter(id);
                    self.nodes.push_sn_plan(sn);
                }
                Expression::Reference { .. } | Expression::CountAsterisk => {
                    let sn = SyntaxNode::new_pointer(id, None, vec![]);
                    self.nodes.push_sn_plan(sn);
                }
                Expression::Alias { .. } => self.add_alias(id),
                Expression::Row { .. } => self.add_row(id),
                Expression::Bool { .. } | Expression::Arithmetic { .. } => self.add_binary_op(id),
                Expression::Unary { .. } => self.add_unary_op(id),
                Expression::StableFunction { .. } => self.add_stable_func(id),
                Expression::Trim { .. } => self.add_trim(id),
            },
        }
    }

    fn prologue_rel(&self, id: usize) -> (&Plan, &Relational) {
        let plan = self.plan.get_ir_plan();
        let rel = plan
            .get_relation_node(id)
            .expect("node {id} must exist in the plan");
        (plan, rel)
    }

    fn prologue_expr(&self, id: usize) -> (&Plan, &Expression) {
        let plan = self.plan.get_ir_plan();
        let expr = plan
            .get_expression_node(id)
            .expect("node {id} must exist in the plan");
        (plan, expr)
    }

    // Relational nodes.

    fn add_cte(&mut self, id: usize) {
        let (_, cte) = self.prologue_rel(id);
        let Relational::ScanCte { alias, child, .. } = cte else {
            panic!("expected CTE node");
        };
        let (child, alias) = (*child, alias.clone());
        let child_sn_id = self.pop_from_stack(child);
        let arena = &mut self.nodes;
        let children: Vec<usize> = vec![
            arena.push_sn_non_plan(SyntaxNode::new_open()),
            child_sn_id,
            arena.push_sn_non_plan(SyntaxNode::new_close()),
            arena.push_sn_non_plan(SyntaxNode::new_alias(alias)),
        ];
        let sn = SyntaxNode::new_pointer(id, None, children);
        arena.push_sn_plan(sn);
    }

    fn add_filter(&mut self, id: usize) {
        let (plan, rel) = self.prologue_rel(id);
        let (Relational::Selection {
            children, filter, ..
        }
        | Relational::Having {
            children, filter, ..
        }) = rel
        else {
            panic!("Expected FILTER node");
        };
        let filter_id = match self.snapshot {
            Snapshot::Latest => *filter,
            Snapshot::Oldest => *plan.undo.get_oldest(filter).map_or_else(|| filter, |id| id),
        };
        let child_plan_id = *children.first().expect("FILTER child");
        let filter_sn_id = self.pop_from_stack(filter_id);
        let child_sn_id = self.pop_from_stack(child_plan_id);
        let sn = SyntaxNode::new_pointer(id, Some(child_sn_id), vec![filter_sn_id]);
        self.nodes.push_sn_plan(sn);
    }

    fn add_group_by(&mut self, id: usize) {
        let (_, gb) = self.prologue_rel(id);
        let Relational::GroupBy {
            children, gr_cols, ..
        } = gb
        else {
            panic!("Expected GROUP BY node");
        };
        let child_plan_id = *children.first().expect("GROUP BY child");
        // The columns on the stack are in reverse order.
        let mut sn_gr_cols = gr_cols.iter().rev().copied().collect::<Vec<_>>();
        // Reuse the same vector to avoid extra allocations
        // (replace plan node ids with syntax node ids).
        for col_id in &mut sn_gr_cols {
            *col_id = self.pop_from_stack(*col_id);
        }
        let child_sn_id = self.pop_from_stack(child_plan_id);
        let mut sn_children = Vec::with_capacity(sn_gr_cols.len() * 2 - 1);
        // The columns are in reverse order, so we need to reverse them back.
        if let Some((first, others)) = sn_gr_cols.split_first() {
            for id in others.iter().rev() {
                sn_children.push(*id);
                sn_children.push(self.nodes.push_sn_non_plan(SyntaxNode::new_comma()));
            }
            sn_children.push(*first);
        }
        let sn = SyntaxNode::new_pointer(id, Some(child_sn_id), sn_children);
        self.nodes.push_sn_plan(sn);
    }

    fn add_join(&mut self, id: usize) {
        let (plan, join) = self.prologue_rel(id);
        let Relational::Join {
            children,
            condition,
            ..
        } = join
        else {
            panic!("Expected JOIN node");
        };
        let cond_plan_id = match self.snapshot {
            Snapshot::Latest => *condition,
            Snapshot::Oldest => *plan
                .undo
                .get_oldest(condition)
                .map_or_else(|| condition, |id| id),
        };
        let inner_plan_id = *children.get(1).expect("JOIN inner child");
        let outer_plan_id = *children.first().expect("JOIN outer child");
        let cond_sn_id = self.pop_from_stack(cond_plan_id);
        let inner_sn_id = self.pop_from_stack(inner_plan_id);
        let outer_sn_id = self.pop_from_stack(outer_plan_id);
        let arena = &mut self.nodes;
        let sn = SyntaxNode::new_pointer(
            id,
            Some(outer_sn_id),
            vec![
                inner_sn_id,
                arena.push_sn_non_plan(SyntaxNode::new_condition()),
                cond_sn_id,
            ],
        );
        arena.push_sn_plan(sn);
    }

    fn add_motion(&mut self, id: usize) {
        let (plan, motion) = self.prologue_rel(id);
        let Relational::Motion {
            policy,
            children,
            is_child_subquery,
            program,
            output,
            ..
        } = motion
        else {
            panic!("Expected MOTION node");
        };

        let first_child = children.first().copied();
        if let MotionPolicy::LocalSegment { .. } = policy {
            #[cfg(feature = "mock")]
            {
                // We should materialize the subquery on the storage. Honestly, this SQL
                // is not valid and should never be generated in runtime, but let's leave
                // it for testing.
                let child_plan_id = first_child.expect("MOTION child");
                let child_sn_id = self.pop_from_stack(child_plan_id);
                let sn = SyntaxNode::new_pointer(id, Some(child_sn_id), vec![]);
                self.nodes.push_sn_plan(sn);
                return;
            }
            #[cfg(not(feature = "mock"))]
            {
                panic!("Local segment notion policy is not supported in the syntax plan");
            }
        }

        let empty_table_op = program
            .0
            .iter()
            .find(|op| matches!(op, MotionOpcode::SerializeAsEmptyTable(_)));
        if let Some(op) = empty_table_op {
            let is_enabled = matches!(op, MotionOpcode::SerializeAsEmptyTable(true));
            if is_enabled {
                let output_cols = plan.get_row_list(*output).expect("row aliases");
                // We need to preserve types when doing `select null`,
                // otherwise tarantool will cast it to `scalar`.
                let mut select_columns = Vec::with_capacity(output_cols.len());
                for col in output_cols {
                    let ref_id = plan
                        .get_child_under_alias(*col)
                        .expect("motion output must be a row of aliases!");
                    let ref_expr = plan.get_expression_node(ref_id).expect("reference node");
                    let Expression::Reference { col_type, .. } = ref_expr else {
                        panic!("expected Reference under Alias in Motion output");
                    };
                    select_columns.push(format_smolstr!("cast(null as {col_type})"));
                }
                let empty_select =
                    format_smolstr!("select {} where false", select_columns.join(","));
                let inline = SyntaxNode::new_inline(&empty_select);
                let inline_sn_id = self.nodes.push_sn_non_plan(inline);
                let sn = SyntaxNode::new_pointer(id, None, vec![inline_sn_id]);
                self.nodes.push_sn_plan(sn);
            }
            return;
        }
        let vtable = self
            .plan
            .get_motion_vtable(id)
            .expect("motion virtual table");

        let vtable_alias = vtable.get_alias().cloned();
        let is_child_subquery = *is_child_subquery;

        // Remove motion's child from the stack (if any).
        if let Some(child_id) = first_child {
            let _ = self.pop_from_stack(child_id);
        }

        let arena = &mut self.nodes;

        // There are some cases when motion child is not a `SubQuery` and when
        // it has an alias. E.g. in case of a `INSERT ... SELECT ...` its child
        // may be a `Projection`.
        let sn_children = if is_child_subquery || vtable_alias.is_some() {
            let mut children: Vec<usize> = vec![
                arena.push_sn_non_plan(SyntaxNode::new_open()),
                arena.push_sn_non_plan(SyntaxNode::new_vtable(id)),
                arena.push_sn_non_plan(SyntaxNode::new_close()),
            ];

            if let Some(name) = vtable_alias {
                assert!(
                    !name.is_empty(),
                    "Virtual table {vtable:?} has an empty alias name"
                );
                let alias = SyntaxNode::new_alias(name);
                children.push(arena.push_sn_non_plan(alias));
            }
            children
        } else {
            vec![arena.push_sn_non_plan(SyntaxNode::new_vtable(id))]
        };

        let sn = SyntaxNode::new_pointer(id, None, sn_children);
        arena.push_sn_plan(sn);
    }

    fn add_order_by(&mut self, id: usize) {
        let (_, order_by) = self.prologue_rel(id);
        let Relational::OrderBy {
            order_by_elements,
            child,
            ..
        } = order_by
        else {
            panic!("expect ORDER BY node");
        };
        let child_plan_id = *child;
        let mut elems = order_by_elements.clone();

        let mut children: Vec<usize> = Vec::with_capacity(elems.len() * 3 - 1);
        let mut wrapped_syntax_nodes =
            |elem: &OrderByElement, need_comma: bool| -> [Option<usize>; 3] {
                let mut nodes = [None, None, None];
                match elem.entity {
                    OrderByEntity::Expression { expr_id } => {
                        let expr_sn_id = self.pop_from_stack(expr_id);
                        nodes[2] = Some(expr_sn_id);
                    }
                    OrderByEntity::Index { value } => {
                        let sn = SyntaxNode::new_order_index(value);
                        nodes[2] = Some(self.nodes.push_sn_non_plan(sn));
                    }
                }
                if let Some(order_type) = &elem.order_type {
                    let sn = SyntaxNode::new_order_type(order_type);
                    nodes[1] = Some(self.nodes.push_sn_non_plan(sn));
                }
                if need_comma {
                    nodes[0] = Some(self.nodes.push_sn_non_plan(SyntaxNode::new_comma()));
                }
                nodes
            };

        // The elements on the stack are in the reverse order.
        let first = elems.pop().expect("at least one column in ORDER BY");
        for id in wrapped_syntax_nodes(&first, false).into_iter().flatten() {
            children.push(id);
        }
        while let Some(elem) = elems.pop() {
            for id in wrapped_syntax_nodes(&elem, true).into_iter().flatten() {
                children.push(id);
            }
        }
        // Reverse the order of the children back.
        children.reverse();

        let child_sn_id = self.pop_from_stack(child_plan_id);

        let sn = SyntaxNode::new_pointer(id, Some(child_sn_id), children);
        self.nodes.push_sn_plan(sn);
    }

    fn add_proj(&mut self, id: usize) {
        let (_, proj) = self.prologue_rel(id);
        let Relational::Projection {
            children, output, ..
        } = proj
        else {
            panic!("Expected PROJECTION node");
        };
        let child_plan_id = *children.first().expect("PROJECTION child");
        let output = *output;
        let child_sn_id = self.pop_from_stack(child_plan_id);
        let row_sn_id = self.pop_from_stack(output);
        // We don't need the row node itself, only its children. Otherwise we'll produce
        // redundant parentheses between `SELECT` and `FROM`.
        let sn_from_id = self.nodes.push_sn_non_plan(SyntaxNode::new_from());
        let row_sn = self.nodes.get_mut_sn(row_sn_id);
        let col_len = row_sn.right.len();
        let mut children = Vec::with_capacity(col_len - 1);
        // Remove the open and close parentheses.
        for (pos, id) in row_sn.right.iter().enumerate() {
            if pos == 0 {
                continue;
            }
            if pos == col_len - 1 {
                children.push(sn_from_id);
                break;
            }
            children.push(*id);
        }
        let sn = SyntaxNode::new_pointer(id, Some(child_sn_id), children);
        self.nodes.push_sn_plan(sn);
    }

    fn add_scan_relation(&mut self, id: usize) {
        let (_, scan) = self.prologue_rel(id);
        let Relational::ScanRelation { alias, .. } = scan else {
            panic!("Expected SCAN node");
        };
        let scan_alias = alias.clone();
        let arena = &mut self.nodes;
        let children = if let Some(name) = scan_alias {
            vec![arena.push_sn_non_plan(SyntaxNode::new_alias(name))]
        } else {
            Vec::new()
        };
        let sn = SyntaxNode::new_pointer(id, None, children);
        arena.push_sn_plan(sn);
    }

    fn add_set(&mut self, id: usize) {
        let (_, set) = self.prologue_rel(id);
        let (Relational::Except { left, right, .. }
        | Relational::Union { left, right, .. }
        | Relational::UnionAll { left, right, .. }
        | Relational::Intersect { left, right, .. }) = set
        else {
            panic!("Expected SET node");
        };
        let (left, right) = (*left, *right);
        let right_sn_id = self.pop_from_stack(right);
        let left_sn_id = self.pop_from_stack(left);
        let sn = SyntaxNode::new_pointer(id, Some(left_sn_id), vec![right_sn_id]);
        self.nodes.push_sn_plan(sn);
    }

    fn add_sq(&mut self, id: usize) {
        let (_, sq) = self.prologue_rel(id);
        let Relational::ScanSubQuery {
            children, alias, ..
        } = sq
        else {
            panic!("Expected SUBQUERY node");
        };
        let child_plan_id = *children.first().expect("SUBQUERY child");
        let sq_alias = alias.clone();
        let child_sn_id = self.pop_from_stack(child_plan_id);

        let arena = &mut self.nodes;
        let mut children: Vec<usize> = vec![
            arena.push_sn_non_plan(SyntaxNode::new_open()),
            child_sn_id,
            arena.push_sn_non_plan(SyntaxNode::new_close()),
        ];
        if let Some(name) = sq_alias {
            children.push(arena.push_sn_non_plan(SyntaxNode::new_alias(name)));
        }
        let sn = SyntaxNode::new_pointer(id, None, children);
        arena.push_sn_plan(sn);
    }

    fn add_values_row(&mut self, id: usize) {
        let (_, row) = self.prologue_rel(id);
        let Relational::ValuesRow { data, .. } = row else {
            panic!("Expected VALUES ROW node");
        };
        let data_sn_id = self.pop_from_stack(*data);
        let sn = SyntaxNode::new_pointer(id, None, vec![data_sn_id]);
        self.nodes.push_sn_plan(sn);
    }

    fn add_values(&mut self, id: usize) {
        let (_, values) = self.prologue_rel(id);
        let Relational::Values {
            children, output, ..
        } = values
        else {
            panic!("Expected VALUES node");
        };
        let output_plan_id = *output;
        // The syntax nodes on the stack are in the reverse order.
        let mut sn_children = children.iter().rev().copied().collect::<Vec<_>>();
        // Reuse the same vector to avoid extra allocations (replace plan node ids with syntax node ids).
        for child_id in &mut sn_children {
            *child_id = self.pop_from_stack(*child_id);
        }

        // Consume the output from the stack.
        let _ = self.pop_from_stack(output_plan_id);

        let mut nodes = Vec::with_capacity(sn_children.len() * 2 - 1);
        // Reverse the order of the children back.
        let arena = &mut self.nodes;
        for child_id in sn_children.iter().skip(1).rev() {
            nodes.push(*child_id);
            nodes.push(arena.push_sn_non_plan(SyntaxNode::new_comma()));
        }
        nodes.push(
            *sn_children
                .first()
                .expect("values must have at least one child"),
        );
        let sn = SyntaxNode::new_pointer(id, None, nodes);
        arena.push_sn_plan(sn);
    }

    fn add_limit(&mut self, id: usize) {
        let (_, limit) = self.prologue_rel(id);
        let Relational::Limit { limit, child, .. } = limit else {
            panic!("expected LIMIT node");
        };
        let (limit, child) = (*limit, *child);
        let child_sn_id = self.pop_from_stack(child);
        let arena = &mut self.nodes;
        let children: Vec<usize> = vec![
            child_sn_id,
            arena.push_sn_non_plan(SyntaxNode::new_limit(limit)),
        ];
        let sn = SyntaxNode::new_pointer(id, None, children);
        arena.push_sn_plan(sn);
    }

    // Expression nodes.

    fn add_alias(&mut self, id: usize) {
        let (_, expr) = self.prologue_expr(id);
        let Expression::Alias { child, name } = expr else {
            panic!("Expected ALIAS node");
        };
        let (child, name) = (*child, name.clone());
        let child_sn_id = self.pop_from_stack(child);
        let plan = self.plan.get_ir_plan();
        // Do not generate an alias in SQL when a column has exactly the same name.
        let child_expr = plan
            .get_expression_node(child)
            .expect("alias child expression");
        if let Expression::Reference { .. } = child_expr {
            let alias = plan
                .get_alias_from_reference_node(child_expr)
                .expect("alias name");
            if alias == name {
                let sn = SyntaxNode::new_pointer(id, None, vec![child_sn_id]);
                self.nodes.push_sn_plan(sn);
                return;
            }
        }
        let alias_sn_id = self.nodes.push_sn_non_plan(SyntaxNode::new_alias(name));
        let sn = SyntaxNode::new_pointer(id, Some(child_sn_id), vec![alias_sn_id]);
        self.nodes.push_sn_plan(sn);
    }

    fn add_binary_op(&mut self, id: usize) {
        let (_, expr) = self.prologue_expr(id);
        let (left_plan_id, right_plan_id, op_sn_id) = match expr {
            Expression::Bool {
                left, right, op, ..
            } => {
                let (op, left, right) = (op.clone(), *left, *right);
                let op_sn_id = self
                    .nodes
                    .push_sn_non_plan(SyntaxNode::new_operator(&format!("{op}")));
                (left, right, op_sn_id)
            }
            Expression::Arithmetic {
                left, right, op, ..
            } => {
                let (op, left, right) = (op.clone(), *left, *right);
                let op_sn_id = self
                    .nodes
                    .push_sn_non_plan(SyntaxNode::new_operator(&format!("{op}")));
                (left, right, op_sn_id)
            }
            _ => panic!("Expected binary expression node"),
        };
        let right_sn_id = self.pop_from_stack(right_plan_id);
        let left_sn_id = self.pop_from_stack(left_plan_id);
        let children = vec![op_sn_id, right_sn_id];
        let sn = SyntaxNode::new_pointer(id, Some(left_sn_id), children);
        self.nodes.push_sn_plan(sn);
    }

    fn add_case(&mut self, id: usize) {
        let (_, expr) = self.prologue_expr(id);
        let Expression::Case {
            search_expr,
            when_blocks,
            else_expr,
        } = expr
        else {
            panic!("Expected CASE node");
        };
        let search_expr = *search_expr;
        let when_blocks: Vec<(usize, usize)> = when_blocks.clone();
        let else_expr = *else_expr;

        let mut right_vec = Vec::with_capacity(1 + when_blocks.len() * 4 + 1);
        right_vec.push(self.nodes.push_sn_non_plan(SyntaxNode::new_end()));
        if let Some(else_expr_id) = else_expr {
            right_vec.push(self.pop_from_stack(else_expr_id));
            right_vec.push(self.nodes.push_sn_non_plan(SyntaxNode::new_else()));
        }
        for (cond_expr_id, res_expr_id) in when_blocks.iter().rev() {
            right_vec.push(self.pop_from_stack(*res_expr_id));
            right_vec.push(self.nodes.push_sn_non_plan(SyntaxNode::new_then()));
            right_vec.push(self.pop_from_stack(*cond_expr_id));
            right_vec.push(self.nodes.push_sn_non_plan(SyntaxNode::new_when()));
        }
        if let Some(search_expr_id) = search_expr {
            right_vec.push(self.pop_from_stack(search_expr_id));
        }
        right_vec.reverse();
        let sn = SyntaxNode::new_pointer(
            id,
            Some(self.nodes.push_sn_non_plan(SyntaxNode::new_case())),
            right_vec,
        );
        self.nodes.push_sn_plan(sn);
    }

    fn add_cast(&mut self, id: usize) {
        let (_, expr) = self.prologue_expr(id);
        let Expression::Cast { child, to } = expr else {
            panic!("Expected CAST node");
        };
        let to_alias = SmolStr::from(to);
        let child_plan_id = *child;
        let child_sn_id = self.pop_from_stack(child_plan_id);
        let arena = &mut self.nodes;
        let children = vec![
            arena.push_sn_non_plan(SyntaxNode::new_open()),
            child_sn_id,
            arena.push_sn_non_plan(SyntaxNode::new_unquoted_alias(to_alias)),
            arena.push_sn_non_plan(SyntaxNode::new_close()),
        ];
        let cast_sn_id = arena.push_sn_non_plan(SyntaxNode::new_cast());
        let sn = SyntaxNode::new_pointer(id, Some(cast_sn_id), children);
        arena.push_sn_plan(sn);
    }

    fn add_concat(&mut self, id: usize) {
        let (_, expr) = self.prologue_expr(id);
        let Expression::Concat { left, right } = expr else {
            panic!("Expected CONCAT node");
        };
        let (left, right) = (*left, *right);
        let right_sn_id = self.pop_from_stack(right);
        let left_sn_id = self.pop_from_stack(left);
        let children = vec![
            self.nodes.push_sn_non_plan(SyntaxNode::new_concat()),
            right_sn_id,
        ];
        let sn = SyntaxNode::new_pointer(id, Some(left_sn_id), children);
        self.nodes.push_sn_plan(sn);
    }

    fn add_expr_in_parentheses(&mut self, id: usize) {
        let (_, expr) = self.prologue_expr(id);
        let Expression::ExprInParentheses { child } = expr else {
            panic!("Expected expression in parentheses node");
        };
        let child_sn_id = self.pop_from_stack(*child);
        let arena = &mut self.nodes;
        let children = vec![child_sn_id, arena.push_sn_non_plan(SyntaxNode::new_close())];
        let sn = SyntaxNode::new_pointer(
            id,
            Some(arena.push_sn_non_plan(SyntaxNode::new_open())),
            children,
        );
        arena.push_sn_plan(sn);
    }

    fn add_row(&mut self, id: usize) {
        let plan = self.plan.get_ir_plan();
        let expr = plan
            .get_expression_node(id)
            .expect("node {id} must exist in the plan");
        let Expression::Row { list, .. } = expr else {
            panic!("Expected ROW node");
        };

        // In projections with a huge amount of columns it can be
        // very expensive to retrieve corresponding relational nodes.
        let rel_ids = plan
            .get_relational_nodes_from_row(id)
            .expect("row relational nodes");

        if let Some(motion_id) = plan
            .get_motion_among_rel_nodes(&rel_ids)
            .expect("motion lookup")
        {
            // Logic of replacing row child with vtable (corresponding to motion) is
            // applicable only in case the child is Reference appeared from transformed
            // SubQuery (Like in case `Exists` or `In` operator or in expression like
            // `select * from t where b = (select a from t)`).
            // There are other cases of row containing references to `Motion` nodes when
            // we shouldn't replace them with vtable (e.g. aggregates' stable functions
            // which arguments may point to `Motion` node).
            let first_child_id = *list.first().expect("row should have at least one child");
            let first_child = plan
                .get_expression_node(first_child_id)
                .expect("row child is expression");
            let first_child_is_ref = matches!(first_child, Expression::Reference { .. });

            // Replace motion node to virtual table node.
            let vtable = self
                .plan
                .get_motion_vtable(motion_id)
                .expect("motion virtual table");
            let needs_replacement = vtable.get_alias().is_none()
                && first_child_is_ref
                && plan
                    .is_additional_child(motion_id)
                    .expect("motion id is valid");
            if needs_replacement {
                // Remove columns from the stack.
                for child_id in list.iter().rev() {
                    let _ = self.pop_from_stack(*child_id);

                    // Remove the referred motion from the stack (if any).
                    let expr = plan
                        .get_expression_node(*child_id)
                        .expect("row child is expression");
                    if matches!(expr, Expression::Reference { .. }) {
                        let referred_id = *plan
                            .get_relational_from_reference_node(*child_id)
                            .expect("referred id");
                        self.pop_from_stack(referred_id);
                    }
                }

                // Add virtual table node to the stack.
                let arena = &mut self.nodes;
                let children = vec![
                    arena.push_sn_non_plan(SyntaxNode::new_open()),
                    arena.push_sn_non_plan(SyntaxNode::new_vtable(motion_id)),
                    arena.push_sn_non_plan(SyntaxNode::new_close()),
                ];
                let sn = SyntaxNode::new_pointer(id, None, children);
                arena.push_sn_plan(sn);
                return;
            }
        }

        if let Some(sq_id) = plan
            .get_sub_query_among_rel_nodes(&rel_ids)
            .expect("subquery id")
        {
            // Replace current row with the referred sub-query
            // (except the case when sub-query is located in the FROM clause).
            if plan
                .is_additional_child(sq_id)
                .expect("subquery id is valid")
            {
                let mut sq_sn_id = None;

                // Remove columns from the stack.
                for child_id in list.iter().rev() {
                    let _ = self.pop_from_stack(*child_id);

                    // Remove the referred sub-query from the stack (if any).
                    let expr = plan
                        .get_expression_node(*child_id)
                        .expect("row child is expression");
                    if matches!(expr, Expression::Reference { .. }) {
                        let referred_id = *plan
                            .get_relational_from_reference_node(*child_id)
                            .expect("referred id");
                        sq_sn_id = Some(self.pop_from_stack(referred_id));
                    }
                }

                // Restore the sub-query node on the top of the stack.
                self.push_on_stack(sq_sn_id.expect("sub-query id"));

                return;
            }
        }

        let mut children = Vec::with_capacity(list.len() * 2 + 3);
        // The nodes on the stack are in the reverse order.
        children.push(self.nodes.push_sn_non_plan(SyntaxNode::new_close()));
        if let Some((first, others)) = list.split_first() {
            for child_id in others.iter().rev() {
                children.push(self.pop_from_stack(*child_id));
                children.push(self.nodes.push_sn_non_plan(SyntaxNode::new_comma()));
            }
            children.push(self.pop_from_stack(*first));
        }
        children.push(self.nodes.push_sn_non_plan(SyntaxNode::new_open()));
        // Need to reverse the order of the children back.
        children.reverse();
        let sn = SyntaxNode::new_pointer(id, None, children);
        self.nodes.push_sn_plan(sn);
    }

    fn add_stable_func(&mut self, id: usize) {
        let plan = self.plan.get_ir_plan();
        let expr = plan
            .get_expression_node(id)
            .expect("node {id} must exist in the plan");
        let Expression::StableFunction {
            children: args,
            feature,
            ..
        } = expr
        else {
            panic!("Expected stable function node");
        };
        // The arguments on the stack are in the reverse order.
        let mut nodes = Vec::with_capacity(args.len() * 2 + 2);
        nodes.push(self.nodes.push_sn_non_plan(SyntaxNode::new_close()));
        if let Some((first, others)) = args.split_first() {
            for child_id in others.iter().rev() {
                nodes.push(self.pop_from_stack(*child_id));
                nodes.push(self.nodes.push_sn_non_plan(SyntaxNode::new_comma()));
            }
            nodes.push(self.pop_from_stack(*first));
        }
        if let Some(FunctionFeature::Distinct) = feature {
            nodes.push(self.nodes.push_sn_non_plan(SyntaxNode::new_distinct()));
        }
        nodes.push(self.nodes.push_sn_non_plan(SyntaxNode::new_open()));
        // Need to reverse the order of the children back.
        nodes.reverse();
        let sn = SyntaxNode::new_pointer(id, None, nodes);
        self.nodes.push_sn_plan(sn);
    }

    fn add_trim(&mut self, id: usize) {
        let (_, expr) = self.prologue_expr(id);
        let Expression::Trim {
            kind,
            pattern,
            target,
        } = expr
        else {
            panic!("Expected TRIM node");
        };
        let (kind, pattern, target) = (kind.clone(), *pattern, *target);
        let mut need_from = false;

        // Syntax nodes on the stack are in the reverse order.
        let target_sn_id = self.pop_from_stack(target);
        let mut pattern_sn_id = None;
        if let Some(pattern) = pattern {
            pattern_sn_id = Some(self.pop_from_stack(pattern));
            need_from = true;
        }

        // Populate trim children.
        let sn_kind = match kind {
            Some(TrimKind::Leading) => Some(SyntaxNode::new_leading()),
            Some(TrimKind::Trailing) => Some(SyntaxNode::new_trailing()),
            Some(TrimKind::Both) => Some(SyntaxNode::new_both()),
            None => None,
        };
        let mut nodes = Vec::with_capacity(6);
        nodes.push(self.nodes.push_sn_non_plan(SyntaxNode::new_open()));
        if let Some(kind) = sn_kind {
            nodes.push(self.nodes.push_sn_non_plan(kind));
            need_from = true;
        }
        if let Some(pattern) = pattern_sn_id {
            nodes.push(pattern);
        }
        if need_from {
            nodes.push(self.nodes.push_sn_non_plan(SyntaxNode::new_from()));
        }
        nodes.push(target_sn_id);
        nodes.push(self.nodes.push_sn_non_plan(SyntaxNode::new_close()));

        let trim_id = self.nodes.push_sn_non_plan(SyntaxNode::new_trim());
        let sn = SyntaxNode::new_pointer(id, Some(trim_id), nodes);
        self.nodes.push_sn_plan(sn);
    }

    fn add_unary_op(&mut self, id: usize) {
        let (plan, expr) = self.prologue_expr(id);
        let Expression::Unary { child, op } = expr else {
            panic!("Expected unary expression node");
        };
        let (child, op) = (*child, op.clone());
        let child_node = plan.get_expression_node(child).expect("child expression");
        let is_and = matches!(child_node, Expression::Bool { op: Bool::And, .. });
        let operator_node_id = self
            .nodes
            .push_sn_non_plan(SyntaxNode::new_operator(&format!("{op}")));
        let child_sn_id = self.pop_from_stack(child);
        // Bool::Or operator already covers itself with parentheses, that's why we
        // don't have to cover it here.
        let sn = if op == Unary::Not && is_and {
            SyntaxNode::new_pointer(
                id,
                Some(operator_node_id),
                vec![
                    self.nodes.push_sn_non_plan(SyntaxNode::new_open()),
                    child_sn_id,
                    self.nodes.push_sn_non_plan(SyntaxNode::new_close()),
                ],
            )
        } else {
            let (left, right) = match op {
                Unary::IsNull => (child_sn_id, operator_node_id),
                Unary::Exists | Unary::Not => (operator_node_id, child_sn_id),
            };
            SyntaxNode::new_pointer(id, Some(left), vec![right])
        };
        self.nodes.push_sn_plan(sn);
    }

    /// Get the plan node if any.
    ///
    /// # Errors
    /// - syntax node wraps an invalid plan node
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
        self.nodes.get_sn(top);
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
            self.nodes.get_sn(top);
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
            let node = self.nodes.get_sn(pos);
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
                    sp.add_plan_node(id);
                    let sn_id = sp.nodes.next_id() - 1;
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
                    sp.add_plan_node(id);
                    let sn_id = sp.nodes.next_id() - 1;
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
        let proj = self.nodes.get_mut_sn(select.proj);
        let new_top = proj.left.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::SyntaxPlan,
                Some("Proj syntax node does not have left child!".into()),
            )
        })?;
        proj.left = None;
        let scan = self.nodes.get_mut_sn(select.scan);
        scan.left = Some(select.proj);

        // Try to move new top under parent.
        if let Some(id) = select.parent {
            let parent = self.nodes.get_mut_sn(id);
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
                        SbroadError::NotFound(Entity::SyntaxNode, format_smolstr!("(id {id})"))
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
            let sn = sp.nodes.get_mut_sn(*id);
            // Note that in case `left` is a `Some(...)`, call of `take` will make it None.
            if let Some(left_id) = sn.left.take() {
                stack.push(left_id);
            } else if let Some(id) = stack.pop() {
                positions.push(id);
                let sn_next = sp.nodes.get_mut_sn(id);
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
