use std::collections::{HashMap, HashSet};

use ahash::AHashMap;
use pest::iterators::Pair;
use smol_str::format_smolstr;
use tarantool::decimal::Decimal;

use crate::errors::{Action, Entity, SbroadError};
use crate::frontend::sql::ast::Rule;
use crate::ir::expression::Expression;
use crate::ir::helpers::RepeatableState;
use crate::ir::operator::{OrderByElement, OrderByEntity, Relational};
use crate::ir::transformation::redistribution::MotionOpcode;
use crate::ir::tree::traversal::{PostOrder, EXPR_CAPACITY};
use crate::ir::value::double::Double;
use crate::ir::value::Value;
use crate::ir::{Node, Plan};

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
    map: HashMap<usize, usize>,
}

impl Translation {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Translation {
            map: HashMap::with_capacity(capacity),
        }
    }

    pub(super) fn add(&mut self, parse_id: usize, plan_id: usize) {
        self.map.insert(parse_id, plan_id);
    }

    pub(super) fn get(&self, old: usize) -> Result<usize, SbroadError> {
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
    relational: usize,
    /// Expression operator in which this SubQuery is met.
    /// E.g. `Exists`.
    operator: usize,
    /// SubQuery id in plan.
    sq: usize,
}
impl Eq for SubQuery {}

impl SubQuery {
    fn new(relational: usize, operator: usize, sq: usize) -> SubQuery {
        SubQuery {
            relational,
            operator,
            sq,
        }
    }
}

impl Plan {
    fn gather_sq_for_replacement(&self) -> Result<HashSet<SubQuery, RepeatableState>, SbroadError> {
        let mut set: HashSet<SubQuery, RepeatableState> = HashSet::with_hasher(RepeatableState);
        // Traverse expression trees of the selection and join nodes.
        // Gather all sub-queries in the boolean expressions there.
        for (relational_id, node) in self.nodes.iter().enumerate() {
            match node {
                Node::Relational(
                    Relational::Selection { filter: tree, .. }
                    | Relational::Join {
                        condition: tree, ..
                    }
                    | Relational::Having { filter: tree, .. },
                ) => {
                    let capacity = self.nodes.len();
                    let mut expr_post = PostOrder::with_capacity(
                        |node| self.nodes.expr_iter(node, false),
                        capacity,
                    );
                    for (_, op_id) in expr_post.iter(*tree) {
                        let expression_node = self.get_node(op_id)?;
                        if let Node::Expression(Expression::Bool { left, right, .. }) =
                            expression_node
                        {
                            let children = &[*left, *right];
                            for child in children {
                                if let Node::Relational(Relational::ScanSubQuery { .. }) =
                                    self.get_node(*child)?
                                {
                                    set.insert(SubQuery::new(relational_id, op_id, *child));
                                }
                            }
                        } else if let Node::Expression(Expression::Unary { child, .. }) =
                            expression_node
                        {
                            if let Node::Relational(Relational::ScanSubQuery { .. }) =
                                self.get_node(*child)?
                            {
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
    ) -> Result<AHashMap<usize, usize>, SbroadError> {
        let set = self.gather_sq_for_replacement()?;
        let mut replaces: AHashMap<usize, usize> = AHashMap::with_capacity(set.len());
        for sq in set {
            // Append sub-query to relational node if it is not already there (can happen with BETWEEN).
            match self.get_mut_node(sq.relational)? {
                Node::Relational(
                    Relational::Selection { children, .. }
                    | Relational::Join { children, .. }
                    | Relational::Having { children, .. },
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
                Relational::Join { children, .. }
                | Relational::Having { children, .. }
                | Relational::Selection { children, .. } => children.clone(),
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
            if let Expression::Bool {
                ref mut left,
                ref mut right,
                ..
            } = op
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
            } else if let Expression::Unary { child, .. } = op {
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
        replaces: &AHashMap<usize, usize>,
    ) -> Result<(), SbroadError> {
        for between in betweens {
            let left_id: usize = if let Some(id) = replaces.get(&between.left_id) {
                self.clone_expr_subtree(*id)?
            } else {
                self.clone_expr_subtree(between.left_id)?
            };
            let less_eq_expr = self.get_mut_expression_node(between.less_eq_id)?;
            if let Expression::Bool { ref mut left, .. } = less_eq_expr {
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

    pub(crate) fn clone_expr_subtree(&mut self, top_id: usize) -> Result<usize, SbroadError> {
        let mut map = HashMap::new();
        let mut subtree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, false), EXPR_CAPACITY);
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();
        for (_, id) in nodes {
            let next_id = self.nodes.next_id();
            let mut expr = self.get_expression_node(id)?.clone();
            match expr {
                Expression::Constant { .. }
                | Expression::Reference { .. }
                | Expression::CountAsterisk => {}
                Expression::Alias { ref mut child, .. }
                | Expression::ExprInParentheses { ref mut child }
                | Expression::Cast { ref mut child, .. }
                | Expression::Unary { ref mut child, .. } => {
                    *child = *map.get(child).ok_or_else(|| {
                        SbroadError::NotFound(Entity::SubTree, format_smolstr!("(id {id})"))
                    })?;
                }
                Expression::Bool {
                    ref mut left,
                    ref mut right,
                    ..
                }
                | Expression::Arithmetic {
                    ref mut left,
                    ref mut right,
                    ..
                }
                | Expression::Concat {
                    ref mut left,
                    ref mut right,
                    ..
                } => {
                    *left = *map.get(left).ok_or_else(|| {
                        SbroadError::NotFound(Entity::SubTree, format_smolstr!("(id {id})"))
                    })?;
                    *right = *map.get(right).ok_or_else(|| {
                        SbroadError::NotFound(Entity::SubTree, format_smolstr!("(id {id})"))
                    })?;
                }
                Expression::Trim {
                    ref mut pattern,
                    ref mut target,
                    ..
                } => {
                    if let Some(pattern) = pattern {
                        *pattern = *map.get(pattern).ok_or_else(|| {
                            SbroadError::NotFound(Entity::SubTree, format_smolstr!("(id {id})"))
                        })?;
                    }
                    *target = *map.get(target).ok_or_else(|| {
                        SbroadError::NotFound(Entity::SubTree, format_smolstr!("(id {id})"))
                    })?;
                }
                Expression::Row {
                    list: ref mut children,
                    ..
                }
                | Expression::StableFunction {
                    ref mut children, ..
                } => {
                    for child in children {
                        *child = *map.get(child).ok_or_else(|| {
                            SbroadError::NotFound(Entity::SubTree, format_smolstr!("(id {id})"))
                        })?;
                    }
                }
                Expression::Case {
                    ref mut search_expr,
                    ref mut when_blocks,
                    ref mut else_expr,
                } => {
                    if let Some(search_expr) = search_expr {
                        *search_expr = *map.get(search_expr).unwrap_or_else(|| {
                            panic!("Search expression not found for subtree cloning.")
                        });
                    }
                    for (cond_expr, res_expr) in when_blocks {
                        *cond_expr = *map.get(cond_expr).unwrap_or_else(|| {
                            panic!("Condition expression not found for subtree cloning.")
                        });
                        *res_expr = *map.get(res_expr).unwrap_or_else(|| {
                            panic!("Result expression not found for subtree cloning.")
                        });
                    }
                    if let Some(else_expr) = else_expr {
                        *else_expr = *map.get(else_expr).unwrap_or_else(|| {
                            panic!("Else expression not found for subtree cloning.")
                        });
                    }
                }
            }
            self.nodes.push(Node::Expression(expr));
            map.insert(id, next_id);
        }
        Ok(self.nodes.next_id() - 1)
    }
}

/// Helper struct to clone plan's subtree.
/// Assumes that all parameters are bound.
pub struct SubtreeCloner {
    old_new: AHashMap<usize, usize>,
    nodes_with_backward_references: Vec<usize>,
}

impl SubtreeCloner {
    fn new(capacity: usize) -> Self {
        SubtreeCloner {
            old_new: AHashMap::with_capacity(capacity),
            nodes_with_backward_references: Vec::new(),
        }
    }

    fn get_new_id(&self, old_id: usize) -> Result<usize, SbroadError> {
        self.old_new
            .get(&old_id)
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!("new node not found for old id: {old_id}")),
                )
            })
            .copied()
    }

    fn copy_list(&self, list: &[usize]) -> Result<Vec<usize>, SbroadError> {
        let mut new_list = Vec::with_capacity(list.len());
        for id in list {
            new_list.push(self.get_new_id(*id)?);
        }
        Ok(new_list)
    }

    fn clone_expression(&mut self, expr: &Expression) -> Result<Expression, SbroadError> {
        let mut copied = expr.clone();

        // note: all struct fields are listed explicitly (instead of `..`), so that
        // when a new field is added to a struct, this match must
        // be updated, or compilation will fail.
        match &mut copied {
            Expression::Constant { value: _ }
            | Expression::Reference {
                parent: _,
                targets: _,
                position: _,
                col_type: _,
            }
            | Expression::CountAsterisk => {}
            Expression::Alias {
                ref mut child,
                name: _,
            }
            | Expression::ExprInParentheses { ref mut child }
            | Expression::Cast {
                ref mut child,
                to: _,
            }
            | Expression::Unary {
                ref mut child,
                op: _,
            } => {
                *child = self.get_new_id(*child)?;
            }
            Expression::Case {
                ref mut search_expr,
                ref mut when_blocks,
                ref mut else_expr,
            } => {
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
            Expression::Bool {
                ref mut left,
                ref mut right,
                op: _,
            }
            | Expression::Arithmetic {
                ref mut left,
                ref mut right,
                op: _,
            }
            | Expression::Concat {
                ref mut left,
                ref mut right,
            } => {
                *left = self.get_new_id(*left)?;
                *right = self.get_new_id(*right)?;
            }
            Expression::Trim {
                ref mut pattern,
                ref mut target,
                ..
            } => {
                if let Some(pattern) = pattern {
                    *pattern = self.get_new_id(*pattern)?;
                }
                *target = self.get_new_id(*target)?;
            }
            Expression::Row {
                list: ref mut children,
                distribution: _,
            }
            | Expression::StableFunction {
                ref mut children, ..
            } => {
                *children = self.copy_list(&*children)?;
            }
        }

        Ok(copied)
    }

    #[allow(clippy::too_many_lines)]
    fn clone_relational(
        &mut self,
        old_relational: &Relational,
        id: usize,
    ) -> Result<Relational, SbroadError> {
        let mut copied = old_relational.clone();

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
            Relational::Values {
                output: _,
                children: _,
            }
            | Relational::Projection {
                children: _,
                output: _,
                is_distinct: _,
            }
            | Relational::Insert {
                relation: _,
                columns: _,
                children: _,
                output: _,
                conflict_strategy: _,
            }
            | Relational::Update {
                relation: _,
                children: _,
                update_columns_map: _,
                strategy: _,
                pk_positions: _,
                output: _,
            }
            | Relational::Delete {
                relation: _,
                children: _,
                output: _,
            }
            | Relational::ScanRelation {
                alias: _,
                output: _,
                relation: _,
            }
            | Relational::ScanCte {
                alias: _,
                output: _,
                child: _,
            }
            | Relational::ScanSubQuery {
                alias: _,
                children: _,
                output: _,
            }
            | Relational::Except {
                left: _,
                right: _,
                output: _,
            }
            | Relational::Intersect {
                left: _,
                right: _,
                output: _,
            }
            | Relational::Union {
                left: _,
                right: _,
                output: _,
            }
            | Relational::UnionAll {
                left: _,
                right: _,
                output: _,
            } => {}
            Relational::Having {
                children: _,
                output: _,
                filter,
            }
            | Relational::Selection {
                children: _,
                filter,
                output: _,
            }
            | Relational::Join {
                children: _,
                condition: filter,
                output: _,
                kind: _,
            } => {
                *filter = self.get_new_id(*filter)?;
            }
            Relational::Motion {
                alias: _,
                children: _,
                policy: _,
                program,
                output: _,
                is_child_subquery: _,
            } => {
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
            Relational::GroupBy {
                children: _,
                gr_cols,
                output: _,
                is_final: _,
            } => {
                *gr_cols = self.copy_list(gr_cols)?;
            }
            Relational::OrderBy {
                child: _,
                order_by_elements,
                output: _,
            } => {
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
            Relational::ValuesRow {
                output: _,
                data,
                children: _,
            } => {
                *data = self.get_new_id(*data)?;
            }
        }

        Ok(copied)
    }

    // Some nodes contain references to nodes above in the tree
    // This function replaces those references to new nodes.
    fn replace_backward_refs(&self, plan: &mut Plan) -> Result<(), SbroadError> {
        for old_id in &self.nodes_with_backward_references {
            if let Node::Relational(Relational::Motion { program, .. }) = plan.get_node(*old_id)? {
                let op_cnt = program.0.len();
                for idx in 0..op_cnt {
                    let op = plan.get_motion_opcode(*old_id, idx)?;
                    if let MotionOpcode::RearrangeForShardedUpdate { update_id, .. } = op {
                        let new_motion_id = self.get_new_id(*old_id)?;
                        let new_update_id = self.get_new_id(*update_id)?;

                        if let Relational::Motion {
                            program: new_program,
                            ..
                        } = plan.get_mut_relation_node(new_motion_id)?
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
        top_id: usize,
        capacity: usize,
    ) -> Result<usize, SbroadError> {
        let mut dfs = PostOrder::with_capacity(|x| plan.subtree_iter(x, true), capacity);
        dfs.populate_nodes(top_id);
        let nodes = dfs.take_nodes();
        drop(dfs);
        for (_, id) in nodes {
            let node = plan.get_node(id)?;
            let new_node = match node {
                Node::Relational(rel) => Node::Relational(self.clone_relational(rel, id)?),
                Node::Expression(expr) => Node::Expression(self.clone_expression(expr)?),
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
        top_id: usize,
        subtree_capacity: usize,
    ) -> Result<usize, SbroadError> {
        let mut helper = Self::new(subtree_capacity);
        helper.clone(plan, top_id, subtree_capacity)
    }
}

#[cfg(test)]
mod tests;
