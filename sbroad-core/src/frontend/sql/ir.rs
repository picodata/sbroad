use std::collections::{HashMap, HashSet};

use ahash::AHashMap;
use pest::iterators::Pair;
use smol_str::format_smolstr;
use tarantool::decimal::Decimal;

use crate::errors::{Action, Entity, SbroadError};
use crate::frontend::sql::ast::Rule;
use crate::ir::helpers::RepeatableState;
use crate::ir::node::expression::{ExprOwned, Expression, MutExpression};
use crate::ir::node::relational::{MutRelational, RelOwned, Relational};
use crate::ir::node::{
    Alias, ArenaType, ArithmeticExpr, BoolExpr, Case, Cast, Concat, Constant, Delete, Except,
    ExprInParentheses, GroupBy, Having, Insert, Intersect, Join, Limit, Motion, MutNode, Node,
    NodeAligned, NodeId, OrderBy, Projection, Reference, Row, ScanCte, ScanRelation, ScanSubQuery,
    Selection, StableFunction, Trim, UnaryExpr, Union, UnionAll, Update, Values, ValuesRow,
};
use crate::ir::operator::{OrderByElement, OrderByEntity};
use crate::ir::transformation::redistribution::MotionOpcode;
use crate::ir::tree::traversal::{LevelNode, PostOrder, EXPR_CAPACITY};
use crate::ir::value::double::Double;
use crate::ir::value::Value;
use crate::ir::Plan;

use super::Between;

impl Value {
    /// Creates `Value` from pest pair.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    #[allow(dead_code)]
    pub(super) fn from_node(pair: &Pair<Rule>) -> Result<Self, SbroadError> {
        let pair_string = pair.as_str();

        match pair.as_rule() {
            Rule::False => Ok(false.into()),
            Rule::True => Ok(true.into()),
            Rule::Null => Ok(Value::Null),
            Rule::Integer => Ok(pair_string
                .parse::<i64>()
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("i64 parsing error {e}"),
                    )
                })?
                .into()),
            Rule::Decimal => Ok(pair_string
                .parse::<Decimal>()
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("decimal parsing error {e:?}"),
                    )
                })?
                .into()),
            Rule::Double => Ok(pair_string
                .parse::<Double>()
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("double parsing error {e}"),
                    )
                })?
                .into()),
            Rule::Unsigned => Ok(pair_string
                .parse::<u64>()
                .map_err(|e| {
                    SbroadError::ParsingError(
                        Entity::Value,
                        format_smolstr!("u64 parsing error {e}"),
                    )
                })?
                .into()),
            Rule::SingleQuotedString => {
                let pair_str = pair.as_str();
                Ok(pair_str[1..pair_str.len() - 1].into())
            }
            _ => Err(SbroadError::Unsupported(
                Entity::Type,
                Some("can not create Value from ParseNode".into()),
            )),
        }
    }
}

#[derive(Debug)]
/// Helper struct representing map of { `ParseNode` id -> `Node` id }
pub(super) struct Translation {
    map: HashMap<usize, NodeId>,
}

impl Translation {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Translation {
            map: HashMap::with_capacity(capacity),
        }
    }

    pub(super) fn add(&mut self, parse_id: usize, plan_id: NodeId) {
        self.map.insert(parse_id, plan_id);
    }

    pub(super) fn get(&self, old: usize) -> Result<NodeId, SbroadError> {
        self.map.get(&old).copied().ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format_smolstr!("(parse node) [{old}] in translation map"),
            )
        })
    }
}

/// Helper struct used for `SubQuery` -> Reference replacement in `gather_sq_for_replacement`.
#[derive(Hash, PartialEq, Debug)]
struct SubQuery {
    /// Relational operator that is a parent of current SubQuery.
    /// E.g. Selection (in case SubQuery is met in WHERE expression).
    relational: NodeId,
    /// Expression operator in which this SubQuery is met.
    /// E.g. `Exists`.
    operator: NodeId,
    /// SubQuery id in plan.
    sq: NodeId,
}
impl Eq for SubQuery {}

impl SubQuery {
    fn new(relational: NodeId, operator: NodeId, sq: NodeId) -> SubQuery {
        SubQuery {
            relational,
            operator,
            sq,
        }
    }
}

struct CloneExprSubtreeMap {
    // Map of { old_node_id -> new_node_id } for cloning nodes.
    inner: AHashMap<NodeId, NodeId>,
}

impl CloneExprSubtreeMap {
    fn with_capacity(capacity: usize) -> Self {
        CloneExprSubtreeMap {
            inner: AHashMap::with_capacity(capacity),
        }
    }

    fn insert(&mut self, old_id: NodeId, new_id: NodeId) {
        self.inner.insert(old_id, new_id);
    }

    fn replace(&self, id: &mut NodeId) {
        let new_id = self.get(*id);
        *id = new_id;
    }

    fn get(&self, id: NodeId) -> NodeId {
        *self
            .inner
            .get(&id)
            .unwrap_or_else(|| panic!("Node with id {id:?} not found in the cloning subtree map."))
    }
}

impl Plan {
    fn gather_sq_for_replacement(&self) -> Result<HashSet<SubQuery, RepeatableState>, SbroadError> {
        let mut set: HashSet<SubQuery, RepeatableState> = HashSet::with_hasher(RepeatableState);
        // Traverse expression trees of the selection and join nodes.
        // Gather all sub-queries in the boolean expressions there.
        for (relational_id, _node) in self.nodes.iter64().enumerate() {
            let node = self.get_node(NodeId {
                offset: u32::try_from(relational_id).unwrap(),
                arena_type: ArenaType::Arena64,
            })?;
            match node {
                Node::Relational(
                    Relational::Selection(Selection { filter: tree, .. })
                    | Relational::Join(Join {
                        condition: tree, ..
                    })
                    | Relational::Having(Having { filter: tree, .. }),
                ) => {
                    let capacity = self.nodes.len();
                    let mut expr_post = PostOrder::with_capacity(
                        |node| self.nodes.expr_iter(node, false),
                        capacity,
                    );
                    for LevelNode(_, op_id) in expr_post.iter(*tree) {
                        let expression_node = self.get_node(op_id)?;
                        if let Node::Expression(Expression::Bool(BoolExpr {
                            left, right, ..
                        })) = expression_node
                        {
                            let children = &[*left, *right];
                            for child in children {
                                if let Node::Relational(Relational::ScanSubQuery { .. }) =
                                    self.get_node(*child)?
                                {
                                    let relational_id = NodeId {
                                        offset: u32::try_from(relational_id).unwrap(),
                                        arena_type: ArenaType::Arena64,
                                    };
                                    set.insert(SubQuery::new(relational_id, op_id, *child));
                                }
                            }
                        } else if let Node::Expression(Expression::Unary(UnaryExpr {
                            child, ..
                        })) = expression_node
                        {
                            if let Node::Relational(Relational::ScanSubQuery { .. }) =
                                self.get_node(*child)?
                            {
                                let relational_id = NodeId {
                                    offset: u32::try_from(relational_id).unwrap(),
                                    arena_type: ArenaType::Arena64,
                                };
                                set.insert(SubQuery::new(relational_id, op_id, *child));
                            }
                        }
                    }
                }
                _ => continue,
            }
        }
        Ok(set)
    }

    /// Replace sub-queries with references to the sub-query.
    pub(super) fn replace_sq_with_references(
        &mut self,
    ) -> Result<AHashMap<NodeId, NodeId>, SbroadError> {
        let set = self.gather_sq_for_replacement()?;
        let mut replaces: AHashMap<NodeId, NodeId> = AHashMap::with_capacity(set.len());
        for sq in set {
            // Append sub-query to relational node if it is not already there (can happen with BETWEEN).
            match self.get_mut_node(sq.relational)? {
                MutNode::Relational(
                    MutRelational::Selection(Selection { children, .. })
                    | MutRelational::Join(Join { children, .. })
                    | MutRelational::Having(Having { children, .. }),
                ) => {
                    // O(n) can become a problem.
                    if !children.contains(&sq.sq) {
                        children.push(sq.sq);
                    }
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Relational,
                        Some("Sub-query is not in selection or join node".into()),
                    ))
                }
            }

            // Generate a reference to the sub-query.
            let rel = self.get_relation_node(sq.relational)?;
            let children = match rel {
                Relational::Join(Join { children, .. })
                | Relational::Having(Having { children, .. })
                | Relational::Selection(Selection { children, .. }) => children.clone(),
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Relational,
                        Some("Sub-query is not in selection, group by or join node".into()),
                    ))
                }
            };
            let position: usize = children.iter().position(|&x| x == sq.sq).ok_or_else(|| {
                SbroadError::FailedTo(Action::Build, None, "a reference to the sub-query".into())
            })?;
            let row_id = self.add_row_from_subquery(&children, position, Some(sq.relational))?;

            // Replace sub-query with reference.
            let op = self.get_mut_expression_node(sq.operator)?;
            if let MutExpression::Bool(BoolExpr {
                ref mut left,
                ref mut right,
                ..
            }) = op
            {
                if *left == sq.sq {
                    *left = row_id;
                } else if *right == sq.sq {
                    *right = row_id;
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("Sub-query is not a left or right operand".into()),
                    ));
                }
                replaces.insert(sq.sq, row_id);
            } else if let MutExpression::Unary(UnaryExpr { child, .. }) = op {
                *child = row_id;
                replaces.insert(sq.sq, row_id);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("Sub-query is not in a boolean expression".into()),
                ));
            }
        }
        Ok(replaces)
    }

    /// Resolve the double linking problem in BETWEEN operator. On the AST to IR step
    /// we transform `left BETWEEN center AND right` construction into
    /// `left >= center AND left <= right`, where the same `left` expression is reused
    /// twice. So, We need to copy the 'left' expression tree from `left >= center` to the
    /// `left <= right` expression.
    ///
    /// Otherwise we'll have problems on the dispatch stage while taking nodes from the original
    /// plan to build a sub-plan for the storage. If the same `left` subtree is used twice in
    /// the plan, these nodes are taken while traversing the `left >= center` expression and
    /// nothing is left for the `left <= right` sutree.
    pub(super) fn fix_betweens(
        &mut self,
        betweens: &[Between],
        replaces: &AHashMap<NodeId, NodeId>,
    ) -> Result<(), SbroadError> {
        for between in betweens {
            let left_id: NodeId = if let Some(id) = replaces.get(&between.left_id) {
                self.clone_expr_subtree(*id)?
            } else {
                self.clone_expr_subtree(between.left_id)?
            };
            let less_eq_expr = self.get_mut_expression_node(between.less_eq_id)?;
            if let MutExpression::Bool(BoolExpr { ref mut left, .. }) = less_eq_expr {
                *left = left_id;
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("expected a boolean Expression".into()),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn clone_expr_subtree(&mut self, top_id: NodeId) -> Result<NodeId, SbroadError> {
        let mut subtree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, false), EXPR_CAPACITY);
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();
        let mut map = CloneExprSubtreeMap::with_capacity(nodes.len());
        for LevelNode(_, id) in nodes {
            let mut expr = self.get_expression_node(id)?.get_expr_owned();
            match expr {
                ExprOwned::Constant { .. }
                | ExprOwned::Reference { .. }
                | ExprOwned::CountAsterisk { .. } => {}
                ExprOwned::Alias(Alias { ref mut child, .. })
                | ExprOwned::ExprInParentheses(ExprInParentheses { ref mut child })
                | ExprOwned::Cast(Cast { ref mut child, .. })
                | ExprOwned::Unary(UnaryExpr { ref mut child, .. }) => map.replace(child),
                ExprOwned::Bool(BoolExpr {
                    ref mut left,
                    ref mut right,
                    ..
                })
                | ExprOwned::Arithmetic(ArithmeticExpr {
                    ref mut left,
                    ref mut right,
                    ..
                })
                | ExprOwned::Concat(Concat {
                    ref mut left,
                    ref mut right,
                    ..
                }) => {
                    map.replace(left);
                    map.replace(right);
                }
                ExprOwned::Trim(Trim {
                    ref mut pattern,
                    ref mut target,
                    ..
                }) => {
                    if let Some(pattern) = pattern {
                        map.replace(pattern);
                    }
                    map.replace(target);
                }
                ExprOwned::Row(Row {
                    list: ref mut children,
                    ..
                })
                | ExprOwned::StableFunction(StableFunction {
                    ref mut children, ..
                }) => {
                    for child in children {
                        map.replace(child);
                    }
                }
                ExprOwned::Case(Case {
                    ref mut search_expr,
                    ref mut when_blocks,
                    ref mut else_expr,
                }) => {
                    if let Some(search_expr) = search_expr {
                        map.replace(search_expr);
                    }
                    for (cond_expr, res_expr) in when_blocks {
                        map.replace(cond_expr);
                        map.replace(res_expr);
                    }
                    if let Some(else_expr) = else_expr {
                        map.replace(else_expr);
                    }
                }
            }
            let next_id = self.nodes.push(expr.into());
            map.insert(id, next_id);
        }
        Ok(map.get(top_id))
    }
}

/// Helper struct to clone plan's subtree.
/// Assumes that all parameters are bound.
pub struct SubtreeCloner {
    old_new: AHashMap<NodeId, NodeId>,
    nodes_with_backward_references: Vec<NodeId>,
}

impl SubtreeCloner {
    fn new(capacity: usize) -> Self {
        SubtreeCloner {
            old_new: AHashMap::with_capacity(capacity),
            nodes_with_backward_references: Vec::new(),
        }
    }

    fn get_new_id(&self, old_id: NodeId) -> Result<NodeId, SbroadError> {
        self.old_new
            .get(&old_id)
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!("new node not found for old id: {old_id:?}")),
                )
            })
            .copied()
    }

    fn copy_list(&self, list: &[NodeId]) -> Result<Vec<NodeId>, SbroadError> {
        let mut new_list = Vec::with_capacity(list.len());
        for id in list {
            new_list.push(self.get_new_id(*id)?);
        }
        Ok(new_list)
    }

    fn clone_expression(&mut self, expr: &Expression) -> Result<ExprOwned, SbroadError> {
        let mut copied = expr.get_expr_owned();

        // note: all struct fields are listed explicitly (instead of `..`), so that
        // when a new field is added to a struct, this match must
        // be updated, or compilation will fail.
        match &mut copied {
            ExprOwned::Constant(Constant { value: _ })
            | ExprOwned::Reference(Reference {
                parent: _,
                targets: _,
                position: _,
                col_type: _,
            })
            | ExprOwned::CountAsterisk { .. } => {}
            ExprOwned::Alias(Alias {
                ref mut child,
                name: _,
            })
            | ExprOwned::ExprInParentheses(ExprInParentheses { ref mut child })
            | ExprOwned::Cast(Cast {
                ref mut child,
                to: _,
            })
            | ExprOwned::Unary(UnaryExpr {
                ref mut child,
                op: _,
            }) => {
                *child = self.get_new_id(*child)?;
            }
            ExprOwned::Case(Case {
                ref mut search_expr,
                ref mut when_blocks,
                ref mut else_expr,
            }) => {
                if let Some(search_expr) = search_expr {
                    *search_expr = self.get_new_id(*search_expr)?;
                }
                for (cond_expr, res_expr) in when_blocks {
                    *cond_expr = self.get_new_id(*cond_expr)?;
                    *res_expr = self.get_new_id(*res_expr)?;
                }
                if let Some(else_expr) = else_expr {
                    *else_expr = self.get_new_id(*else_expr)?;
                }
            }
            ExprOwned::Bool(BoolExpr {
                ref mut left,
                ref mut right,
                op: _,
            })
            | ExprOwned::Arithmetic(ArithmeticExpr {
                ref mut left,
                ref mut right,
                op: _,
            })
            | ExprOwned::Concat(Concat {
                ref mut left,
                ref mut right,
            }) => {
                *left = self.get_new_id(*left)?;
                *right = self.get_new_id(*right)?;
            }
            ExprOwned::Trim(Trim {
                ref mut pattern,
                ref mut target,
                ..
            }) => {
                if let Some(pattern) = pattern {
                    *pattern = self.get_new_id(*pattern)?;
                }
                *target = self.get_new_id(*target)?;
            }
            ExprOwned::Row(Row {
                list: ref mut children,
                distribution: _,
            })
            | ExprOwned::StableFunction(StableFunction {
                ref mut children, ..
            }) => {
                *children = self.copy_list(&*children)?;
            }
        }

        Ok(copied)
    }

    #[allow(clippy::too_many_lines)]
    fn clone_relational(
        &mut self,
        old_relational: &Relational,
        id: NodeId,
    ) -> Result<RelOwned, SbroadError> {
        let mut copied: RelOwned = old_relational.get_rel_owned();

        // all relational nodes have output and children list,
        // which must be copied.
        let children = old_relational.children().to_vec();
        let new_children = self.copy_list(&children)?;
        copied.set_children(new_children);
        let new_output_id = self.get_new_id(old_relational.output())?;
        *copied.mut_output() = new_output_id;

        // copy node specific fields, that reference other plan nodes

        // note: all struct fields are listed explicitly (instead of `..`), so that
        // when a new field is added to a struct, this match must
        // be updated, or compilation will fail.
        match &mut copied {
            RelOwned::Values(Values {
                output: _,
                children: _,
            })
            | RelOwned::Projection(Projection {
                children: _,
                output: _,
                is_distinct: _,
            })
            | RelOwned::Insert(Insert {
                relation: _,
                columns: _,
                children: _,
                output: _,
                conflict_strategy: _,
            })
            | RelOwned::Update(Update {
                relation: _,
                children: _,
                update_columns_map: _,
                strategy: _,
                pk_positions: _,
                output: _,
            })
            | RelOwned::Delete(Delete {
                relation: _,
                children: _,
                output: _,
            })
            | RelOwned::ScanRelation(ScanRelation {
                alias: _,
                output: _,
                relation: _,
            })
            | RelOwned::ScanCte(ScanCte {
                alias: _,
                output: _,
                child: _,
            })
            | RelOwned::ScanSubQuery(ScanSubQuery {
                alias: _,
                children: _,
                output: _,
            })
            | RelOwned::Except(Except {
                left: _,
                right: _,
                output: _,
            })
            | RelOwned::Intersect(Intersect {
                left: _,
                right: _,
                output: _,
            })
            | RelOwned::Union(Union {
                left: _,
                right: _,
                output: _,
            })
            | RelOwned::UnionAll(UnionAll {
                left: _,
                right: _,
                output: _,
            })
            | RelOwned::Limit(Limit {
                limit: _,
                child: _,
                output: _,
            }) => {}
            RelOwned::Having(Having {
                children: _,
                output: _,
                filter,
            })
            | RelOwned::Selection(Selection {
                children: _,
                filter,
                output: _,
            })
            | RelOwned::Join(Join {
                children: _,
                condition: filter,
                output: _,
                kind: _,
            }) => {
                *filter = self.get_new_id(*filter)?;
            }
            RelOwned::Motion(Motion {
                alias: _,
                children: _,
                policy: _,
                program,
                output: _,
                is_child_subquery: _,
            }) => {
                for op in &mut program.0 {
                    match op {
                        MotionOpcode::RearrangeForShardedUpdate {
                            update_id: _,
                            old_shard_columns_len: _,
                            new_shard_columns_positions: _,
                        } => {
                            // Update -> Motion -> ...
                            // Update is not copied yet.
                            self.nodes_with_backward_references.push(id);
                        }
                        MotionOpcode::AddMissingRowsForLeftJoin { motion_id } => {
                            // Projection -> THIS Motion -> Projection -> InnerJoin -> Motion (== motion_id)
                            // so it is safe to look up motion_id in map
                            *motion_id = self.get_new_id(*motion_id)?;
                        }
                        MotionOpcode::PrimaryKey(_)
                        | MotionOpcode::RemoveDuplicates
                        | MotionOpcode::ReshardIfNeeded
                        | MotionOpcode::SerializeAsEmptyTable(_) => {}
                    }
                }
            }
            RelOwned::GroupBy(GroupBy {
                children: _,
                gr_cols,
                output: _,
                is_final: _,
            }) => {
                *gr_cols = self.copy_list(gr_cols)?;
            }
            RelOwned::OrderBy(OrderBy {
                child: _,
                order_by_elements,
                output: _,
            }) => {
                let mut new_order_by_elements = Vec::with_capacity(order_by_elements.len());
                for element in &mut *order_by_elements {
                    let new_entity = match element.entity {
                        OrderByEntity::Expression { expr_id } => OrderByEntity::Expression {
                            expr_id: self.get_new_id(expr_id)?,
                        },
                        OrderByEntity::Index { value } => OrderByEntity::Index { value },
                    };
                    new_order_by_elements.push(OrderByElement {
                        entity: new_entity,
                        order_type: element.order_type.clone(),
                    });
                }
                *order_by_elements = new_order_by_elements;
            }
            RelOwned::ValuesRow(ValuesRow {
                output: _,
                data,
                children: _,
            }) => {
                *data = self.get_new_id(*data)?;
            }
        }

        Ok(copied)
    }

    // Some nodes contain references to nodes above in the tree
    // This function replaces those references to new nodes.
    fn replace_backward_refs(&self, plan: &mut Plan) -> Result<(), SbroadError> {
        for old_id in &self.nodes_with_backward_references {
            if let Node::Relational(Relational::Motion(Motion { program, .. })) =
                plan.get_node(*old_id)?
            {
                let op_cnt = program.0.len();
                for idx in 0..op_cnt {
                    let op = plan.get_motion_opcode(*old_id, idx)?;
                    if let MotionOpcode::RearrangeForShardedUpdate { update_id, .. } = op {
                        let new_motion_id = self.get_new_id(*old_id)?;
                        let new_update_id = self.get_new_id(*update_id)?;

                        if let MutRelational::Motion(Motion {
                            program: new_program,
                            ..
                        }) = plan.get_mut_relation_node(new_motion_id)?
                        {
                            if let Some(MotionOpcode::RearrangeForShardedUpdate {
                                update_id: new_node_update_id,
                                ..
                            }) = new_program.0.get_mut(idx)
                            {
                                *new_node_update_id = new_update_id;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn clone(
        &mut self,
        plan: &mut Plan,
        top_id: NodeId,
        capacity: usize,
    ) -> Result<NodeId, SbroadError> {
        let mut dfs = PostOrder::with_capacity(|x| plan.subtree_iter(x, true), capacity);
        dfs.populate_nodes(top_id);
        let nodes = dfs.take_nodes();
        drop(dfs);
        for LevelNode(_, id) in nodes {
            let node = plan.get_node(id)?;
            let new_node: NodeAligned = match node {
                Node::Relational(rel) => self.clone_relational(&rel, id)?.into(),
                Node::Expression(expr) => self.clone_expression(&expr)?.into(),
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Node,
                        Some(format_smolstr!(
                            "clone: expected relational or expression on id: {id}"
                        )),
                    ))
                }
            };
            let new_id = plan.nodes.push(new_node);
            let old = self.old_new.insert(id, new_id);
            if let Some(old_new_id) = old {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "clone: node with id {id} was mapped twice: {old_new_id}, {new_id}"
                    )),
                ));
            }
        }

        self.replace_backward_refs(plan)?;

        let new_top_id = self
            .old_new
            .get(&top_id)
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "invalid subtree traversal with top: {top_id}"
                    )),
                )
            })
            .copied()?;
        Ok(new_top_id)
    }

    /// Clones the given subtree to the plan arena and returns new `top_id`.
    /// Assumes that all parameters are bound and there are no parameters
    /// in the subtree.
    ///
    /// # Errors
    /// - invalid plan subtree, e.g some node is met twice in the plan
    /// - parameters/ddl/acl nodes are found in subtree
    pub fn clone_subtree(
        plan: &mut Plan,
        top_id: NodeId,
        subtree_capacity: usize,
    ) -> Result<NodeId, SbroadError> {
        let mut helper = Self::new(subtree_capacity);
        helper.clone(plan, top_id, subtree_capacity)
    }
}

#[cfg(test)]
mod tests;
