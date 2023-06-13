use crate::errors::{Entity, SbroadError};
use crate::ir::aggregates::{generate_local_alias_for_aggr, AggregateKind, SimpleAggregate};
use crate::ir::distribution::Distribution;
use crate::ir::expression::Expression;
use crate::ir::expression::Expression::StableFunction;
use crate::ir::operator::Relational;
use crate::ir::transformation::redistribution::{MotionKey, MotionPolicy, Strategy, Target};
use crate::ir::tree::traversal::{PostOrder, EXPR_CAPACITY};
use crate::ir::{Node, Plan};
use std::collections::{HashMap, HashSet};

use crate::ir::function::{Behavior, Function};
use crate::ir::helpers::RepeatableState;
use itertools::Itertools;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

const AGGR_CAPACITY: usize = 10;
const EXPR_HASH_DEPTH: usize = 5;

/// Helper struct to store metadata about aggregates
#[derive(Clone, Debug)]
struct AggrInfo {
    /// id of Relational node in which this aggregate is located.
    /// It can be located in `Projection`, `Having`, `OrderBy`
    parent_rel: usize,
    /// id of parent expression of aggregate function,
    /// if there is no parent it's `None`
    parent_expr: Option<usize>,
    /// info about what aggregate it is: sum, count, ...
    aggr: SimpleAggregate,
    /// whether this aggregate was marked distinct in original user query
    is_distinct: bool,
}

/// Helper struct to find aggregates in expressions of finals
struct AggrCollector<'plan> {
    /// id of final node in which matches are searched
    parent_rel: Option<usize>,
    /// collected aggregates
    infos: Vec<AggrInfo>,
    plan: &'plan Plan,
}

/// Helper struct to hold information about
/// location of grouping expressions used in
/// nodes other than `GroupBy`.
///
/// For example grouping expressions can appear
/// in `Projection`, `Having`, `OrderBy`
struct ExpressionLocation {
    pub parent_expr_id: Option<usize>,
    pub expr_id: usize,
    pub rel_id: usize,
}

impl ExpressionLocation {
    pub fn new(expr_id: usize, parent_expr_id: Option<usize>, rel_id: usize) -> Self {
        ExpressionLocation {
            parent_expr_id,
            expr_id,
            rel_id,
        }
    }
}

/// Helper struct to filter duplicate aggregates in local stage.
///
/// Consider user query: `select sum(a), avg(a) from t`
/// at local stage we need to compute `sum(a)` only once.
///
/// This struct contains info needed to compute hash and compare aggregates
/// used at local stage.
struct AggregateSignature<'plan, 'args> {
    pub kind: AggregateKind,
    /// ids of expressions used as arguments to aggregate
    pub arguments: &'args Vec<usize>,
    pub plan: &'plan Plan,
    /// reference to local alias of this local aggregate
    pub local_alias: Option<Rc<String>>,
}

impl<'plan, 'args> AggregateSignature<'plan, 'args> {
    pub fn get_alias(&self) -> Result<Rc<String>, SbroadError> {
        let r = self
            .local_alias
            .as_ref()
            .ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::AggregateSignature,
                    Some("missing local alias".into()),
                )
            })?
            .clone();
        Ok(r)
    }
}

struct GroupingExpression<'plan> {
    pub id: usize,
    pub plan: &'plan Plan,
}

impl<'plan> GroupingExpression<'plan> {
    pub fn new(id: usize, plan: &'plan Plan) -> Self {
        GroupingExpression { id, plan }
    }
}

impl<'plan> Hash for GroupingExpression<'plan> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.plan.hash_for_expr(self.id, state, EXPR_HASH_DEPTH);
    }
}

impl<'plan> PartialEq for GroupingExpression<'plan> {
    fn eq(&self, other: &Self) -> bool {
        self.plan
            .are_subtrees_equal(self.id, other.id)
            .unwrap_or(false)
    }
}

impl<'plan> Eq for GroupingExpression<'plan> {}

impl Plan {
    /// Compute hash from plan expression
    ///
    /// # Note
    /// This function assumes that `Reference`s may come from different relational nodes,
    /// and therefore hash for `Reference` is computed by hash from corresponding alias.
    ///
    /// This behavior is needed to filter duplicate `GroupBy` expressions/aggregates that may
    /// come from different relational nodes. For example, you may have an aggregate that comes
    /// from projection and the same aggregate from having:
    /// `select sum(a) from t having sum(a) > 1`
    fn hash_for_expr<H: Hasher>(&self, top: usize, state: &mut H, depth: usize) {
        if depth == 0 {
            return;
        }
        let Ok(node) = self.get_expression_node(top) else {
            return;
        };
        match node {
            Expression::Alias { child, name } => {
                name.hash(state);
                self.hash_for_expr(*child, state, depth - 1);
            }
            Expression::Bool { op, left, right } => {
                op.hash(state);
                self.hash_for_expr(*left, state, depth - 1);
                self.hash_for_expr(*right, state, depth - 1);
            }
            Expression::Arithmetic {
                op,
                left,
                right,
                with_parentheses,
            } => {
                op.hash(state);
                with_parentheses.hash(state);
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
            Expression::Reference { .. } => {
                self.get_alias_from_reference_node(node)
                    .unwrap_or("")
                    .hash(state);
            }
            Expression::Row { list, .. } => {
                for child in list {
                    self.hash_for_expr(*child, state, depth - 1);
                }
            }
            StableFunction {
                name,
                children,
                is_distinct,
            } => {
                is_distinct.hash(state);
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

impl<'plan, 'args> Hash for AggregateSignature<'plan, 'args> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        for arg in self.arguments {
            self.plan.hash_for_expr(*arg, state, EXPR_HASH_DEPTH);
        }
    }
}

impl<'plan, 'args> PartialEq<Self> for AggregateSignature<'plan, 'args> {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self
                .arguments
                .iter()
                .zip(other.arguments.iter())
                .all(|(l, r)| self.plan.are_subtrees_equal(*l, *r).unwrap_or(false))
    }
}

impl<'plan, 'args> Eq for AggregateSignature<'plan, 'args> {}

impl<'plan> AggrCollector<'plan> {
    pub fn with_capacity(plan: &'plan Plan, capacity: usize) -> AggrCollector<'plan> {
        AggrCollector {
            infos: Vec::with_capacity(capacity),
            parent_rel: None,
            plan,
        }
    }

    pub fn take_aggregates(&mut self) -> Vec<AggrInfo> {
        std::mem::take(&mut self.infos)
    }

    /// Collect aggregates in internal field by traversing expression tree `top`
    ///
    /// # Arguments
    /// * `top` - id of expression root in which to look for aggregates
    /// * `parent_rel` - id of parent relational node, where `top` is located. It is used to
    /// create `AggrInfo`
    ///
    /// # Errors
    /// - invalid expression tree pointed by `top`
    pub fn collect_aggregates(&mut self, top: usize, parent_rel: usize) -> Result<(), SbroadError> {
        self.parent_rel = Some(parent_rel);
        self.find(top, None)?;
        self.parent_rel = None;
        Ok(())
    }

    fn find(&mut self, current: usize, parent: Option<usize>) -> Result<(), SbroadError> {
        let expr = self.plan.get_expression_node(current)?;
        if let StableFunction {
            name, is_distinct, ..
        } = expr
        {
            if let Some(aggr) = SimpleAggregate::new(name, current) {
                let Some(parent_rel) = self.parent_rel else {
                    return Err(SbroadError::Invalid(Entity::AggregateCollector, None))
                };
                let info = AggrInfo {
                    parent_rel,
                    parent_expr: parent,
                    aggr,
                    is_distinct: *is_distinct,
                };
                self.infos.push(info);
                return Ok(());
            };
        }
        for child in self.plan.nodes.expr_iter(current, false) {
            self.find(child, Some(current))?;
        }
        Ok(())
    }
}

type GroupbyExpressionsMap = HashMap<usize, Vec<ExpressionLocation>>;
type LocalAliasesMap = HashMap<usize, Rc<String>>;
type LocalAggrInfo = (AggregateKind, Vec<usize>, Rc<String>);

/// Helper struct to map expressions used in `GroupBy` to
/// expressions used in some other node (`Projection`, `Having`, `OrderBy`)
struct ExpressionMapper<'plan> {
    /// List of expressions ids of `GroupBy`
    gr_exprs: &'plan Vec<usize>,
    /// Maps `GroupBy` expression to expressions used `Projection`
    /// First element in pair is `Projection` column id,
    /// second one is the id of expression
    map: GroupbyExpressionsMap,
    plan: &'plan Plan,
    node_id: Option<usize>,
}

impl<'plan> ExpressionMapper<'plan> {
    fn new(gr_expressions: &'plan Vec<usize>, plan: &'plan Plan) -> ExpressionMapper<'plan> {
        let map: GroupbyExpressionsMap = HashMap::new();
        ExpressionMapper {
            gr_exprs: gr_expressions,
            map,
            plan,
            node_id: None,
        }
    }

    /// Traverses given expression from top to bottom, trying
    /// to find subexpressions that match expressions located in `GroupBy`,
    /// when match is found it is stored in map passed to [`ExpressionMapper`]'s
    /// constructor.
    ///
    /// # Errors
    /// - invalid references in any expression (`GroupBy`'s or node's one)
    /// - invalid query: node expression contains references that are not
    /// found in `GroupBy` expression. The reason is that user specified expression in
    /// node that does not match any expression in `GroupBy`
    fn find_matches(&mut self, expr_root: usize, node_id: usize) -> Result<(), SbroadError> {
        self.node_id = Some(node_id);
        self.find(expr_root, None)?;
        self.node_id = None;
        Ok(())
    }

    /// Helper function for `find_matches` which compares current node to `GroupBy` expressions
    /// and if no match is found recursively calls itself.
    fn find(&mut self, current: usize, parent: Option<usize>) -> Result<(), SbroadError> {
        if let Some(gr_expr) = self
            .gr_exprs
            .iter()
            .find(|gr_expr| {
                self.plan
                    .are_subtrees_equal(current, **gr_expr)
                    .unwrap_or(false)
            })
            .copied()
        {
            let Some(node_id) = self.node_id else {
                return Err(SbroadError::Invalid(Entity::ExpressionMapper, None))
            };
            let location = ExpressionLocation::new(current, parent, node_id);
            if let Some(v) = self.map.get_mut(&gr_expr) {
                v.push(location);
            } else {
                self.map.insert(gr_expr, vec![location]);
            }
            return Ok(());
        }
        let node = self.plan.get_expression_node(current)?;
        if let Expression::Reference { .. } = node {
            // We found a column which is not inside aggregate function
            // and it is not a grouping expression:
            // select a from t group by b - is invalid
            let column_name = {
                self.plan
                    .get_alias_from_reference_node(node)
                    .unwrap_or("'failed to get column name'")
            };
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some(format!(
                    "column {column_name} is not found in grouping expressions!"
                )),
            ));
        }
        for child in self.plan.nodes.aggregate_iter(current, false) {
            self.find(child, Some(current))?;
        }
        Ok(())
    }

    pub fn get_matches(&mut self) -> GroupbyExpressionsMap {
        std::mem::take(&mut self.map)
    }
}

pub struct AggregateInfo {
    pub expression_top: usize,
    pub aggregate: SimpleAggregate,
    pub is_distinct: bool,
}

impl Plan {
    #[allow(unreachable_code)]
    fn generate_local_alias(id: usize) -> String {
        #[cfg(feature = "mock")]
        {
            return format!("\"column_{id}\"");
        }
        format!("\"{}_{id}\"", uuid::Uuid::new_v4().as_simple())
    }

    #[allow(clippy::too_many_lines)]
    /// Checks whether subtrees [`lhs`] and [`rhs`] are equal.
    /// This function traverses both trees comparing their nodes.
    ///
    /// # References Equality
    /// Note: references are considered equal if the columns names to which
    /// they refer are equal. This is because this function is used to find
    /// common expression between `GroupBy` and nodes in Reduce stage of
    /// 2-stage aggregation (`Projection`, `Having`, `OrderBy`). These nodes
    /// handle tuples of the same tables and do not introduce new aliases, so it is safe to compare
    /// references this way.
    ///
    /// It would be wrong to use this function for comparing expressions that
    /// come from different tables:
    /// ```text
    /// select a + b from t1
    /// where c in (select a + b from t2)
    /// ```
    /// Here this function would say that expressions `a+b` in projection and
    /// selection are the same, which is wrong.
    ///
    /// # Errors
    /// - invalid [`Expression::Reference`]s in either of subtrees
    pub fn are_subtrees_equal(&self, lhs: usize, rhs: usize) -> Result<bool, SbroadError> {
        let l = self.get_node(lhs)?;
        let r = self.get_node(rhs)?;
        if let Node::Expression(left) = l {
            if let Node::Expression(right) = r {
                match left {
                    Expression::Alias { .. } => {}
                    Expression::CountAsterisk => {
                        return Ok(matches!(right, Expression::CountAsterisk))
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
                        with_parentheses: parens_left,
                    } => {
                        if let Expression::Arithmetic {
                            op: op_right,
                            left: l_right,
                            right: r_right,
                            with_parentheses: parens_right,
                        } = right
                        {
                            return Ok(*op_left == *op_right
                                && *parens_left == *parens_right
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
                            let alias_left = self.get_alias_from_reference_node(left)?;
                            let alias_right = self.get_alias_from_reference_node(right)?;
                            return Ok(alias_left == alias_right);
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
                    } => {
                        if let Expression::StableFunction {
                            name: name_right,
                            children: children_right,
                            is_distinct: distinct_right,
                        } = right
                        {
                            return Ok(name_left == name_right
                                && distinct_left == distinct_right
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

    /// Used to create a `GroupBy` IR node from AST.
    /// The added `GroupBy` node is local - meaning
    /// that it is part of local stage in 2-stage
    /// aggregation. For more info, see `add_two_stage_aggregation`.
    ///
    /// # Arguments
    /// * `children` - plan's ids of `group by` children from AST
    ///
    /// # Errors
    /// - invalid children count
    /// - failed to create output for `GroupBy`
    pub fn add_groupby_from_ast(&mut self, children: &[usize]) -> Result<usize, SbroadError> {
        if children.len() < 2 {
            return Err(SbroadError::Invalid(
                Entity::Relational,
                Some("Expected GroupBy to have at least one child".into()),
            ));
        }

        let Some((first_child, other)) = children.split_first() else {
            return Err(SbroadError::UnexpectedNumberOfValues("GroupBy ast has no children".into()))
        };

        // Check grouping expression:
        // 1) aggregates are not allowed
        // 2) must contain at least one column (group by 1 - is not valid)
        for (pos, grouping_expr_id) in other.iter().enumerate() {
            let mut dfs =
                PostOrder::with_capacity(|x| self.nodes.expr_iter(x, false), EXPR_CAPACITY);
            let mut contains_at_least_one_col = false;
            for (_, node_id) in dfs.iter(*grouping_expr_id) {
                let node = self.get_node(node_id)?;
                match node {
                    Node::Expression(Expression::Reference { .. }) => {
                        contains_at_least_one_col = true;
                    }
                    Node::Expression(Expression::StableFunction { name, .. }) => {
                        if Expression::is_aggregate_name(name) {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(format!("aggregate functions are not allowed inside grouping expression. Got aggregate: {name}"))
                            ));
                        }
                    }
                    _ => {}
                }
            }
            if !contains_at_least_one_col {
                return Err(SbroadError::Invalid(
                    Entity::Query,
                    Some(format!("grouping expression must contain at least one column. Invalid expression number: {pos}"))
                ));
            }
        }

        let groupby_id = self.add_groupby(*first_child, other, false)?;
        Ok(groupby_id)
    }

    /// Helper function to add `group by` to IR
    ///
    /// # Errors
    /// - `child_id` - invalid `Relational` node
    /// - `grouping_exprs` - contains non-expr id
    pub fn add_groupby(
        &mut self,
        child_id: usize,
        grouping_exprs: &[usize],
        is_final: bool,
    ) -> Result<usize, SbroadError> {
        let final_output = self.add_row_for_output(child_id, &[], true)?;
        let groupby = Relational::GroupBy {
            children: [child_id].to_vec(),
            gr_cols: grouping_exprs.to_vec(),
            output: final_output,
            is_final,
        };

        let groupby_id = self.nodes.push(Node::Relational(groupby));

        self.replace_parent_in_subtree(final_output, None, Some(groupby_id))?;
        for expr in grouping_exprs.iter() {
            self.replace_parent_in_subtree(*expr, None, Some(groupby_id))?;
        }

        Ok(groupby_id)
    }

    /// Collect information about aggregates
    ///
    /// Aggregates can appear in `Projection`, `Having`, `OrderBy`
    ///
    /// # Arguments
    /// [`finals`] - ids of nodes in final (reduce stage) before adding two stage aggregation.
    /// It may contain ids of `Projection`, `Having`, `Limit`, `OrderBy`.
    /// Note: final `GroupBy` is not present because it will be added later in 2-stage pipeline.
    fn collect_aggregates(&self, finals: &Vec<usize>) -> Result<Vec<AggrInfo>, SbroadError> {
        let mut collector = AggrCollector::with_capacity(self, AGGR_CAPACITY);
        for node_id in finals {
            let node = self.get_relation_node(*node_id)?;
            match node {
                Relational::Projection { output, .. } => {
                    for col in self.get_row_list(*output)? {
                        collector.collect_aggregates(*col, *node_id)?;
                    }
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format!(
                            "collect_aggregates: unexpected relational node ({node_id}): {node:?}"
                        )),
                    ))
                }
            }
        }

        let aggr_infos = collector.take_aggregates();
        self.validate_aggregates(&aggr_infos)?;

        Ok(aggr_infos)
    }

    /// Validates expressions used in aggregates
    ///
    /// Currently we only check that there is no aggregates inside aggregates
    fn validate_aggregates(&self, aggr_infos: &Vec<AggrInfo>) -> Result<(), SbroadError> {
        for info in aggr_infos {
            let top = info.aggr.fun_id;
            let mut dfs =
                PostOrder::with_capacity(|x| self.nodes.expr_iter(x, false), EXPR_CAPACITY);
            for (_, id) in dfs.iter(top) {
                if id == top {
                    continue;
                }
                if let Node::Expression(Expression::StableFunction { name, .. }) =
                    self.get_node(id)?
                {
                    if Expression::is_aggregate_name(name) {
                        return Err(SbroadError::Invalid(
                            Entity::Query,
                            Some(format!("aggregate function inside aggregate function is not allowed. Got `{name}` inside `{}`", info.aggr.kind))
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Get ids of nodes in Reduce stage (finals) and id of the top node in Map stage.
    ///
    /// Finals are nodes in Reduce stage without final `GroupBy`.
    ///
    /// # Example
    /// original query: `select sum(a), b from t group by by t having sum(a) > 1`
    /// Approximate plan before adding 2-stage aggregation:
    /// ```txt
    /// Projection (1)
    ///     Having (2)
    ///         GroupBy (3)
    ///             Scan (4)
    /// ```
    /// Then this function will return `([1, 2], 4)`
    fn split_reduce_stage(&self, final_proj_id: usize) -> Result<(Vec<usize>, usize), SbroadError> {
        let mut finals: Vec<usize> = vec![];
        let get_first_child = |rel_id: usize| -> Result<usize, SbroadError> {
            let c = *self
                .get_relational_children(rel_id)?
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format!(
                        "split_reduce_stage: expected relation node ({rel_id}) to have children!"
                    ))
                })?
                .first()
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format!(
                        "split_reduce_stage: expected relation node ({rel_id}) to have children!"
                    ))
                })?;
            Ok(c)
        };
        let mut next: usize = final_proj_id;
        // Currently in Reduce stage only Projection is possible.
        // When any Having, OrderBy, Limit will be added this will change.
        let max_reduce_nodes = 1;
        for _ in 0..=max_reduce_nodes {
            match self.get_relation_node(next)? {
                Relational::Projection { .. } => {
                    finals.push(next);
                    next = get_first_child(next)?;
                }
                _ => return Ok((finals, next)),
            }
        }
        return Err(SbroadError::Invalid(
            Entity::Plan,
            Some(format!("too many nodes ({}) in Reduce stage", finals.len())),
        ));
    }

    /// Collects information about grouping expressions for future use.
    /// This function also does all the validation of incorrect usage of
    /// expressions used outside of aggregate functions.
    ///
    ///
    /// # Returns
    /// - list of ids of expressions used in `GroupBy`. Duplicate expressions are removed.
    /// - mapping between `GroupBy` expressions and corresponding expressions in final nodes
    /// (`Projection`, `Having`, `GroupBy`, `OrderBy`).
    ///
    /// # Arguments
    /// * `upper` - id of the top node in reduce stage, if `GroupBy` is present in the query
    /// the top node in Reduce stage will be `GroupBy`.
    /// * `finals` - ids of nodes in final stage starting from `Projection`
    ///
    /// # Errors
    /// - invalid references in `GroupBy`
    /// - invalid query with `GroupBy`: some expression in some final node wasn't matched to
    /// some `GroupBy` expression
    /// - invalid query without `GroupBy` and with aggregates: there are column references outside
    /// aggregate functions
    /// - invalid query with `Having`: in case there's no `GroupBy`, `Having` may contain
    /// only expressions with constants and aggregates. References outside of aggregate functions
    /// are illegal.
    fn collect_grouping_expressions(
        &mut self,
        upper: usize,
        finals: &Vec<usize>,
        has_aggregates: bool,
    ) -> Result<(Vec<usize>, GroupbyExpressionsMap), SbroadError> {
        let mut grouping_expr = vec![];
        let mut gr_expr_map: GroupbyExpressionsMap = HashMap::new();

        let has_groupby = matches!(self.get_relation_node(upper)?, Relational::GroupBy { .. });
        if has_groupby {
            let old_gr_cols = self.get_grouping_cols(upper)?;
            // remove duplicate expressions
            let mut unique_grouping_exprs: HashSet<GroupingExpression, _> =
                HashSet::with_capacity_and_hasher(old_gr_cols.len(), RepeatableState);
            for gr_expr in old_gr_cols {
                unique_grouping_exprs.insert(GroupingExpression::new(*gr_expr, self));
            }
            let grouping_exprs = unique_grouping_exprs
                .into_iter()
                .map(|x| x.id)
                .collect::<Vec<usize>>();
            grouping_expr.extend(grouping_exprs.iter());
            self.set_grouping_cols(upper, grouping_exprs)?;

            let mut mapper = ExpressionMapper::new(&grouping_expr, self);
            for node_id in finals {
                if let Relational::Projection { output, .. } = self.get_relation_node(*node_id)? {
                    for col in self.get_row_list(*output)? {
                        mapper.find_matches(*col, *node_id)?;
                    }
                }
            }
            gr_expr_map = mapper.get_matches();
        }

        if has_aggregates && !has_groupby {
            // check that all column references are inside aggregate functions
            for id in finals {
                let node = self.get_relation_node(*id)?;
                if let Relational::Projection { output, .. } = node {
                    for col in self.get_row_list(*output)? {
                        let mut dfs = PostOrder::with_capacity(
                            |x| self.nodes.aggregate_iter(x, false),
                            EXPR_CAPACITY,
                        );
                        dfs.populate_nodes(*col);
                        let nodes = dfs.take_nodes();
                        for (_, id) in nodes {
                            let n = self.get_expression_node(id)?;
                            if let Expression::Reference { .. } = n {
                                let alias = match self.get_alias_from_reference_node(n) {
                                    Ok(v) => v.to_string(),
                                    Err(e) => e.to_string(),
                                };
                                return Err(SbroadError::Invalid(Entity::Query,
                                                                Some(format!("found column reference ({alias}) outside aggregate function"))));
                            }
                        }
                    }
                }
            }
        }

        Ok((grouping_expr, gr_expr_map))
    }

    /// Add expressions used as arguments to distinct aggregates to `GroupBy` in reduce stage
    ///
    /// E.g: For query below, this func should add b*b to reduce `GroupBy`
    /// `select a, sum(distinct b*b), count(c) from t group by a`
    /// Map: `select a as l1, b*b as l2, count(c) as l3 from t group by a, b`
    /// Reduce: `select l1, sum(distinct l2), sum(l3) from tmp_space group by l1`
    fn add_distinct_aggregates_to_local_groupby(
        &mut self,
        upper: usize,
        additional_grouping_exprs: Vec<usize>,
    ) -> Result<usize, SbroadError> {
        let mut local_proj_child_id = upper;
        if !additional_grouping_exprs.is_empty() {
            if let Relational::GroupBy { gr_cols, .. } =
                self.get_mut_relation_node(local_proj_child_id)?
            {
                gr_cols.extend(additional_grouping_exprs.into_iter());
            } else {
                local_proj_child_id = self.add_groupby(upper, &additional_grouping_exprs, true)?;
                self.set_distribution(self.get_relational_output(local_proj_child_id)?)?;
            }
        }
        Ok(local_proj_child_id)
    }

    /// Create Projection node for Map(local) stage of 2-stage aggregation
    ///
    /// # Arguments
    ///
    /// * `child_id` - id of child for Projection node to be created.
    /// * `aggr_infos` - vector of metadata for each aggregate function that was found in final
    /// projection. Each info specifies what kind of aggregate it is (sum, avg, etc) and location
    /// in final projection.
    /// * `grouping_exprs` - ids of grouping expressions from local `GroupBy`, empty if there is
    /// no `GroupBy` in original query.
    /// * `finals` - ids of nodes from final stage, starting from `Projection`.
    /// Final stage may contain `Projection`, `Limit`, `OrderBy`, `Having` nodes.
    ///
    /// Local Projection is created by creating columns for grouping expressions and columns
    /// for local aggregates. If there is no `GroupBy` in the original query then `child_id` refers
    /// to other node and in case there are distinct aggregates, `GroupBy` node will be created
    /// to contain expressions from distinct aggregates:
    /// ```text
    /// select sum(distinct a + b) from t
    /// // Plan before calling this function:
    /// Projection sum(distinct a + b) from t
    ///     Scan t
    /// // After calling this function
    /// Projection sum(distinct a + b) from t <- did not changed
    ///     Projection a + b as l1 <- created local Projection
    ///         GroupBy a <- created a GroupBy node for distinct aggregate
    ///             Scan t
    /// ```
    ///
    /// If there is `GroupBy` in the original query, then distinct expressions will be added
    /// to that.
    ///
    /// # Local aliases
    /// For each column in local `Projection` alias is created. It is generated
    /// as `{uuid}_{node_id}`, for the purpose of not matching to some of the user aliases.
    /// Aggregates encapsulate this logic in themselves, see [`create_columns_for_local_proj`]
    /// of [`SimpleAggregate`]. For grouping expressions it is done manually using
    /// [`generate_local_alias`].
    /// These local aliases are used later in 2-stage aggregation pipeline to replace
    /// original expressions in nodes like `Projection`, `Having`, `GroupBy`. For example:
    /// ```text
    /// // initially final Projection
    /// Projection count(expr)
    /// // when we create local Projection, we take expr from final Projection,
    /// // and later(not in this function) replace expression in final
    /// // Projection with corresponding local alias:
    /// Projection sum(l1)
    ///     ...
    ///         Projection count(expr) as l1 // l1 - is generated local alias
    /// ```
    /// The same logic must be applied to any node in final stage of 2-stage aggregation:
    /// `Having`, `GroupBy`, `OrderBy`. See [`add_two_stage_aggregation`] for more details.
    ///
    /// # Returns
    /// - id of local `Projection` that was created.
    /// - vector of positions by which `GroupBy` is done. Positions are relative to local `Projection`
    /// output.
    /// - map between `GroupBy` expression and corresponding local alias.
    fn add_local_projection(
        &mut self,
        child_id: usize,
        aggr_infos: &mut Vec<AggrInfo>,
        grouping_exprs: &Vec<usize>,
    ) -> Result<(usize, Vec<usize>, LocalAliasesMap), SbroadError> {
        let (child_id, proj_output_cols, groupby_local_aliases, grouping_positions) =
            self.create_columns_for_local_proj(aggr_infos, child_id, grouping_exprs)?;
        let proj_output = self.nodes.add_row(proj_output_cols, None);
        let proj = Relational::Projection {
            output: proj_output,
            children: vec![child_id],
        };
        let proj_id = self.nodes.push(Node::Relational(proj));
        for info in aggr_infos {
            // We take expressions inside aggregate functions from Final projection,
            // so we need to update parent
            self.replace_parent_in_subtree(info.aggr.fun_id, Some(info.parent_rel), Some(proj_id))?;
        }
        self.set_distribution(proj_output)?;

        Ok((proj_id, grouping_positions, groupby_local_aliases))
    }

    fn create_local_aggregate(
        &mut self,
        kind: AggregateKind,
        arguments: &[usize],
        local_alias: &str,
    ) -> Result<usize, SbroadError> {
        // Currently all supported aggregate functions take only one argument
        let aggregate_expression = *arguments.first().ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues(format!(
                "create_local_aggregate: Aggregate function has no children: {kind}, {local_alias}"
            ))
        })?;
        let fun = Function {
            name: kind.to_string(),
            behavior: Behavior::Stable,
        };
        // We can reuse `aggregate_expression` between local aggregates, because
        // all local aggregates are located inside the same motion subtree and we
        // assume that each local aggregate does not need to modify its expression
        let local_fun_id = self.add_stable_function(&fun, vec![aggregate_expression])?;
        let alias_id = self.nodes.add_alias(local_alias, local_fun_id)?;
        Ok(alias_id)
    }

    /// Creates columns for local projection
    ///
    /// local projection contains groupby columns + local aggregates,
    /// this function removes duplicated among them and creates the list for output
    /// `Row` for local projection.
    ///
    /// In case we have distinct aggregates and no groupby in original query,
    /// local `GroupBy` node will created.
    ///
    /// # Returns
    /// - id of local Projection child.
    /// - created list of columns
    /// - mapping between `GroupBy` expressions and local aliases
    /// - grouping positions: positions of columns by which `GroupBy` is done
    fn create_columns_for_local_proj(
        &mut self,
        aggr_infos: &mut Vec<AggrInfo>,
        upper_id: usize,
        grouping_exprs: &Vec<usize>,
    ) -> Result<(usize, Vec<usize>, LocalAliasesMap, Vec<usize>), SbroadError> {
        let mut output_cols: Vec<usize> = vec![];
        let (local_aliases, child_id, grouping_positions) =
            self.add_grouping_exprs(aggr_infos, upper_id, grouping_exprs, &mut output_cols)?;
        self.add_local_aggregates(aggr_infos, &mut output_cols)?;

        Ok((child_id, output_cols, local_aliases, grouping_positions))
    }

    /// Adds grouping expressions to columns of local projection
    ///
    /// # Arguments
    /// * `aggr_infos` - list of metadata info for each aggregate
    /// * `upper_id` - first node in local stage, if `GroupBy` was
    /// present in the original user query, then it is the id of that
    /// `GroupBy`
    /// * `grouping_exprs` - ids of grouping expressions from local
    /// `GroupBy`. It is assumed that there are no duplicate expressions
    /// among them.
    /// * `output_cols` - list of projection columns, where to push grouping
    /// expressions.
    ///
    /// # Returns
    /// - map between grouping expression id and corresponding local alias
    /// - id of a Projection child, in case there are distinct aggregates and
    /// no local `GroupBy` node, this node will be created
    /// - list of positions in projection columns by which `GroupBy` is done. These
    /// positions are later used to create Motion node and they include only positions
    /// from original `GroupBy`. Grouping expressions from distinct aggregates are not
    /// included in this list as they shouldn't be used for Motion node.
    fn add_grouping_exprs(
        &mut self,
        aggr_infos: &mut [AggrInfo],
        upper_id: usize,
        grouping_exprs: &Vec<usize>,
        output_cols: &mut Vec<usize>,
    ) -> Result<(LocalAliasesMap, usize, Vec<usize>), SbroadError> {
        let mut unique_grouping_exprs_for_local_stage: HashMap<
            GroupingExpression,
            Rc<String>,
            RepeatableState,
        > = HashMap::with_hasher(RepeatableState);
        for gr_expr in grouping_exprs {
            unique_grouping_exprs_for_local_stage.insert(
                GroupingExpression::new(*gr_expr, self),
                Rc::new(Self::generate_local_alias(*gr_expr)),
            );
        }

        // add grouping expressions found from distinct aggregates to local groupby
        let mut grouping_exprs_from_aggregates: Vec<usize> = vec![];
        for info in aggr_infos.iter_mut().filter(|x| x.is_distinct) {
            let argument = {
                let args = self
                    .nodes
                    .expr_iter(info.aggr.fun_id, false)
                    .collect::<Vec<usize>>();
                if args.len() > 1 {
                    return Err(SbroadError::UnexpectedNumberOfValues(format!(
                        "aggregate ({info:?}) have more than one argument"
                    )));
                }
                *args.first().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format!(
                        "Aggregate function has no children: {info:?}"
                    ))
                })?
            };
            let expr = GroupingExpression::new(argument, self);
            if let Some(local_alias) = unique_grouping_exprs_for_local_stage.get(&expr) {
                info.aggr
                    .lagg_alias
                    .insert(info.aggr.kind, local_alias.clone());
            } else {
                grouping_exprs_from_aggregates.push(argument);
                let local_alias = Rc::new(Self::generate_local_alias(argument));
                unique_grouping_exprs_for_local_stage.insert(expr, local_alias.clone());
                info.aggr.lagg_alias.insert(info.aggr.kind, local_alias);
            }
        }

        // Because of borrow checker we need to remove references to Plan from map
        let mut unique_grouping_exprs_for_local_stage: HashMap<usize, Rc<String>, RepeatableState> =
            unique_grouping_exprs_for_local_stage
                .into_iter()
                .map(|(e, s)| (e.id, s))
                .collect();

        let mut alias_to_pos: HashMap<Rc<String>, usize> = HashMap::new();
        // add grouping expressions to local projection
        for (pos, (gr_expr, local_alias)) in
            unique_grouping_exprs_for_local_stage.iter().enumerate()
        {
            let new_alias = self.nodes.add_alias(local_alias, *gr_expr)?;
            output_cols.push(new_alias);
            alias_to_pos.insert(local_alias.clone(), pos);
        }

        let mut local_aliases: LocalAliasesMap =
            HashMap::with_capacity(unique_grouping_exprs_for_local_stage.len());
        let mut grouping_positions: Vec<usize> = Vec::with_capacity(grouping_exprs.len());

        // Note: we need to iterate only over grouping expressions that were present
        // in original user query here. We must not use the grouping expressions
        // that come from distinct aggregates. This is because they are handled separately:
        // local aliases map is needed only for GroupBy expressions in the original query and
        // grouping positions are used to create a Motion later, which should take into account
        // only positions from GroupBy expressions in the original user query.
        for expr_id in grouping_exprs {
            if let Some(local_alias) = unique_grouping_exprs_for_local_stage.remove(expr_id) {
                local_aliases.insert(*expr_id, local_alias.clone());
                if let Some(pos) = alias_to_pos.get(&local_alias) {
                    grouping_positions.push(*pos);
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format!("missing position for local GroupBy column with local alias: {local_alias}"))
                    ));
                }
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!("invalid map with unique grouping expressions. Could not find grouping expression with id: {expr_id}"))));
            }
        }
        let child_id = self
            .add_distinct_aggregates_to_local_groupby(upper_id, grouping_exprs_from_aggregates)?;
        Ok((local_aliases, child_id, grouping_positions))
    }

    /// Adds aggregates columns in `output_cols` for local `Projection`
    ///
    /// This function collects local aggregates from each `AggrInfo`,
    /// then it removes duplicates from them using `AggregateSignature`.
    /// Next, it creates for each unique aggregate local alias and column.
    fn add_local_aggregates(
        &mut self,
        aggr_infos: &mut Vec<AggrInfo>,
        output_cols: &mut Vec<usize>,
    ) -> Result<(), SbroadError> {
        // Aggregate expressions can appear in `Projection`, `Having`, `OrderBy`, if the
        // same expression appears in different places, we must not calculate it separately:
        // `select sum(a) from t group by b having sum(a) > 10`
        // Here `sum(a)` appears both in projection and having, so we need to calculate it only once.
        let mut unique_local_aggregates: HashSet<AggregateSignature, RepeatableState> =
            HashSet::with_hasher(RepeatableState);
        for pos in 0..aggr_infos.len() {
            let info = aggr_infos.get(pos).ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(format!(
                    "invalid idx of aggregate infos ({pos})"
                ))
            })?;
            if info.is_distinct {
                continue;
            }
            let arguments = {
                if let StableFunction { children, .. } =
                    self.get_expression_node(info.aggr.fun_id)?
                {
                    children
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Aggregate,
                        Some(format!("invalid fun_id: {}", info.aggr.fun_id)),
                    ));
                }
            };
            for kind in info.aggr.kind.get_local_aggregates_kinds() {
                let mut signature = AggregateSignature {
                    kind,
                    arguments,
                    plan: self,
                    local_alias: None,
                };
                if let Some(sig) = unique_local_aggregates.get(&signature) {
                    if let Some(alias) = &sig.local_alias {
                        let info = aggr_infos.get_mut(pos).ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(format!(
                                "invalid idx of aggregate infos ({pos})"
                            ))
                        })?;
                        info.aggr.lagg_alias.insert(kind, alias.clone());
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::AggregateSignature,
                            Some("no local alias".into()),
                        ));
                    }
                } else {
                    let info = aggr_infos.get_mut(pos).ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(format!(
                            "invalid idx of aggregate infos ({pos})"
                        ))
                    })?;
                    let alias = Rc::new(generate_local_alias_for_aggr(
                        &kind,
                        &info.aggr.fun_id.to_string(),
                    ));
                    info.aggr.lagg_alias.insert(kind, alias.clone());
                    signature.local_alias = Some(alias);
                    unique_local_aggregates.insert(signature);
                }
            }
        }

        // add non-distinct aggregates to local projection
        let local_aggregates: Result<Vec<LocalAggrInfo>, SbroadError> = unique_local_aggregates
            .into_iter()
            .map(
                |x| -> Result<(AggregateKind, Vec<usize>, Rc<String>), SbroadError> {
                    match x.get_alias() {
                        Ok(s) => Ok((x.kind, x.arguments.clone(), s)),
                        Err(e) => Err(e),
                    }
                },
            )
            .collect();
        for (kind, arguments, local_alias) in local_aggregates? {
            let alias_id = self.create_local_aggregate(kind, &arguments, local_alias.as_str())?;
            output_cols.push(alias_id);
        }

        Ok(())
    }

    fn add_final_groupby(
        &mut self,
        child_id: usize,
        grouping_exprs: &Vec<usize>,
        local_aliases_map: &LocalAliasesMap,
    ) -> Result<usize, SbroadError> {
        if grouping_exprs.is_empty() {
            // no GroupBy in the original query nothing to do
            return Ok(child_id);
        }
        let mut gr_cols: Vec<usize> = Vec::with_capacity(grouping_exprs.len());
        let child_map = self
            .get_relation_node(child_id)?
            .output_alias_position_map(&self.nodes)?
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<HashMap<String, usize>>();
        for expr_id in grouping_exprs {
            let Some(local_alias) = local_aliases_map.get(expr_id) else {
                return Err(SbroadError::Invalid(Entity::Plan,
                Some(format!("add_final_groupby: could not find local alias for GroupBy expr ({expr_id})"))))
            };
            let Some(position) = child_map.get(&*(*local_alias)) else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!("add_final_groupby: did not find alias: {local_alias} in child ({child_id}) output!")))
                )
            };
            let new_col = Expression::Reference {
                position: *position,
                parent: None,
                targets: Some(vec![0]),
            };
            let new_col_id = self.nodes.push(Node::Expression(new_col));
            gr_cols.push(new_col_id);
        }
        let output = self.add_row_for_output(child_id, &[], true)?;
        let final_id = self.nodes.next_id();
        for col in &gr_cols {
            self.replace_parent_in_subtree(*col, None, Some(final_id))?;
        }
        let final_groupby = Relational::GroupBy {
            gr_cols,
            children: vec![child_id],
            is_final: true,
            output,
        };
        self.replace_parent_in_subtree(output, None, Some(final_id))?;
        self.nodes.push(Node::Relational(final_groupby));

        Ok(final_id)
    }

    /// Replace grouping expressions in finals with corresponding
    /// references to local aliases.
    ///
    /// For example:
    /// original query: `select a + b from t group by a + b`
    /// map query: `select a + b as l1 from t group by a + b` `l1` is local alias
    /// reduce query: `select l1 as user_alias from tmp_space group by l1`
    /// In above example this function will replace `a+b` expression in final `Projection`
    fn patch_grouping_expressions(
        &mut self,
        local_aliases_map: &LocalAliasesMap,
        map: GroupbyExpressionsMap,
    ) -> Result<(), SbroadError> {
        type RelationalID = usize;
        type GroupByExpressionID = usize;
        type ExpressionID = usize;
        type ExpressionParent = Option<usize>;
        type ParentExpressionMap =
            HashMap<RelationalID, Vec<(GroupByExpressionID, ExpressionID, ExpressionParent)>>;
        let map: ParentExpressionMap = {
            let mut new_map: ParentExpressionMap = HashMap::with_capacity(map.len());
            for (k, v) in map {
                for location in v {
                    let rec = (k, location.expr_id, location.parent_expr_id);
                    if let Some(u) = new_map.get_mut(&location.rel_id) {
                        u.push(rec);
                    } else {
                        new_map.insert(location.rel_id, vec![rec]);
                    }
                }
            }
            new_map
        };

        for (rel_id, group) in map {
            let child_id = *self
                .get_relational_children(rel_id)?
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format!(
                        "patch_grouping_exprs: expected relation node ({rel_id}) to have children!"
                    ))
                })?
                .first()
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format!(
                        "patch_grouping_exprs: expected relation node ({rel_id}) to have children!"
                    ))
                })?;
            let alias_to_pos_map = self
                .get_relation_node(child_id)?
                .output_alias_position_map(&self.nodes)?
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<HashMap<String, usize>>();
            for (gr_expr_id, expr_id, parent) in group {
                let Some(local_alias) = local_aliases_map.get(&gr_expr_id) else {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format!("patch_finals: failed to find local alias for groupby expression {gr_expr_id}"))))
                };
                let Some(pos) = alias_to_pos_map.get(&*(*local_alias)).copied() else {
                    return Err(SbroadError::Invalid(Entity::Plan,
                                                    Some(format!("patch_finals: failed to find alias '{local_alias}' in ({child_id}). Aliases: {}", alias_to_pos_map.keys().join(" ")))))
                };
                let new_ref = Expression::Reference {
                    parent: Some(rel_id),
                    targets: Some(vec![0]),
                    position: pos,
                };
                let ref_id = self.nodes.push(Node::Expression(new_ref));
                if let Some(parent_expr_id) = parent {
                    self.replace_expression(parent_expr_id, expr_id, ref_id)?;
                } else {
                    match self.get_mut_relation_node(rel_id)? {
                        Relational::Projection { .. } => {
                            return Err(SbroadError::Invalid(
                                Entity::Plan,
                                Some(format!("patch_finals: invalid mapping between groupby expression {gr_expr_id} and projection one: expression {expr_id} has no parent"))
                            ))
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Plan,
                                Some(format!("patch_finals: unexpected node in Reduce stage: {rel_id}"))
                            ))
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Make finals nodes in 2-stage aggregation valid, after local stage was created.
    ///
    /// After reduce stage was created, finals nodes contain invalid
    /// references and aggregate functions. This function replaces
    /// grouping expressions with corresponding local aliases and
    /// replaces old aggregate functions with final aggregates.
    ///
    /// For example:
    /// original query: `select a, sum(b) from t group by a having count(distinct b) > 3`
    /// map: `select a as l1, b as l2, sum(b) as l3 from t group by a, b`
    /// reduce:  `select l1 as a, sum(l3) from t group by l1 having count(distinct l2) > 3`
    ///
    /// This function replaces `a` to `l1`, `sum(b)` to `sum(l3)`,
    /// `count(distinct b)` to `count(distinct l2)`
    ///
    /// # Arguments
    /// * `finals` - ids of nodes to be patched
    /// * `finals_child_id` - id of a relational node right after `finals` in the plan. In case
    /// original query had `GroupBy`, this will be final `GroupBy` id.
    /// * `local_aliases_map` - map between grouping expressions ids and corresponding local aliases.
    /// * `aggr_infos` - list of metadata about aggregates
    /// * `gr_expr_map` - map between grouping expressions in `GroupBy` and grouping expressions
    /// used in `finals`.
    fn patch_finals(
        &mut self,
        finals: &Vec<usize>,
        finals_child_id: usize,
        local_aliases_map: &LocalAliasesMap,
        aggr_infos: &Vec<AggrInfo>,
        gr_expr_map: GroupbyExpressionsMap,
    ) -> Result<(), SbroadError> {
        // After we added a Map stage, we need to update output
        // of nodes in Reduce stage
        if let Some(last) = finals.last() {
            self.get_mut_relation_node(*last)?
                .set_children(vec![finals_child_id])?;
        }
        for node_id in finals.iter().rev() {
            let node = self.get_relation_node(*node_id)?;
            match node {
                Relational::Projection { .. } => {}
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format!(
                            "patch_finals: Unexpected node in reduce stage: {node:?}"
                        )),
                    ))
                }
            }
        }

        self.patch_grouping_expressions(local_aliases_map, gr_expr_map)?;
        let mut parent_to_infos: HashMap<usize, Vec<AggrInfo>> =
            HashMap::with_capacity(finals.len());
        for info in aggr_infos {
            if let Some(v) = parent_to_infos.get_mut(&info.parent_rel) {
                v.push(info.clone());
            } else {
                parent_to_infos.insert(info.parent_rel, vec![info.clone()]);
            }
        }
        for (parent, infos) in parent_to_infos {
            let child_id = if let Some(children) = self.get_relation_node(parent)?.children() {
                *children.first().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Node,
                        Some(format!(
                            "patch aggregates: rel node ({parent}) has no children!"
                        )),
                    )
                })?
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format!(
                        "patch aggregates: rel node on id: {parent} has no children!"
                    )),
                ));
            };
            let alias_to_pos_map = self
                .get_relation_node(child_id)?
                .output_alias_position_map(&self.nodes)?
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<HashMap<String, usize>>();
            for info in infos {
                let final_expr = info.aggr.create_column_for_final_projection(
                    parent,
                    self,
                    &alias_to_pos_map,
                    info.is_distinct,
                )?;
                if let Some(parent_expr) = info.parent_expr {
                    self.replace_expression(parent_expr, info.aggr.fun_id, final_expr)?;
                } else if let Relational::Projection { .. } = self.get_mut_relation_node(parent)? {
                    // currently final stage may contain only Projection node,
                    // when Having will be added, here will be the logic of replacing
                    // its filter
                    return Err(SbroadError::Invalid(
                        Entity::Aggregate,
                        Some(format!(
                            "aggregate info from Projection has no parent! Info: {info:?}"
                        )),
                    ));
                }
            }
        }
        Ok(())
    }

    fn add_motion_to_2stage(
        &mut self,
        grouping_positions: &Vec<usize>,
        motion_parent: usize,
        finals: &[usize],
    ) -> Result<(), SbroadError> {
        let proj_id = *finals.first().ok_or_else(|| {
            SbroadError::Invalid(Entity::Plan, Some("no nodes in Reduce stage!".into()))
        })?;
        if let Relational::Projection { .. } = self.get_relation_node(proj_id)? {
        } else {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("expected Projection as first node in reduce stage!".into()),
            ));
        }
        if grouping_positions.is_empty() {
            // no GroupBy
            let last_final_id = *finals.last().ok_or_else(|| {
                SbroadError::Invalid(Entity::Plan, Some("Reduce stage has no nodes!".into()))
            })?;
            let mut strategy = Strategy::new(last_final_id);
            strategy.add_child(motion_parent, MotionPolicy::Full);
            self.create_motion_nodes(&strategy)?;

            self.set_dist(self.get_relational_output(proj_id)?, Distribution::Single)?;
        } else {
            // we have GroupBy, then finals_child_id is final GroupBy
            let child_id = if let Relational::GroupBy { children, .. } =
                self.get_relation_node(motion_parent)?
            {
                *children.first().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Node,
                        Some(format!("final GroupBy ({motion_parent}) has no children!")),
                    )
                })?
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format!(
                        "expected to have GroupBy under reduce nodes on id: {motion_parent}"
                    )),
                ));
            };
            let mut strategy = Strategy::new(motion_parent);
            strategy.add_child(
                child_id,
                MotionPolicy::Segment(MotionKey {
                    targets: grouping_positions
                        .iter()
                        .map(|x| Target::Reference(*x))
                        .collect::<Vec<Target>>(),
                }),
            );
            self.create_motion_nodes(&strategy)?;

            // When we created final GroupBy we didn't set its distribution, because its
            // actual child (Motion) wasn't created yet.
            self.set_distribution(self.get_relational_output(motion_parent)?)?;
        }
        Ok(())
    }

    /// Adds 2-stage aggregation and returns `true` if there are any aggregate
    /// functions or `GroupBy` is present. Otherwise, returns `false` and
    /// does nothing.
    ///
    /// # Errors
    /// - failed to create local `GroupBy` node
    /// - failed to create local `Projection` node
    /// - failed to create `SQ` node
    /// - failed to change final `GroupBy` child to `SQ`
    /// - failed to update expressions in final `Projection`
    pub fn add_two_stage_aggregation(&mut self, final_proj_id: usize) -> Result<bool, SbroadError> {
        let (finals, upper) = self.split_reduce_stage(final_proj_id)?;
        let mut aggr_infos = self.collect_aggregates(&finals)?;
        let has_aggregates = !aggr_infos.is_empty();
        let (grouping_exprs, gr_expr_map) =
            self.collect_grouping_expressions(upper, &finals, has_aggregates)?;
        if grouping_exprs.is_empty() && aggr_infos.is_empty() {
            return Ok(false);
        }
        let (local_proj_id, grouping_positions, local_aliases_map) =
            self.add_local_projection(upper, &mut aggr_infos, &grouping_exprs)?;
        let sq_id = self.add_sub_query(local_proj_id, Some(""))?;
        self.set_distribution(self.get_relational_output(sq_id)?)?;
        let finals_child_id = self.add_final_groupby(sq_id, &grouping_exprs, &local_aliases_map)?;
        self.patch_finals(
            &finals,
            finals_child_id,
            &local_aliases_map,
            &aggr_infos,
            gr_expr_map,
        )?;
        self.add_motion_to_2stage(&grouping_positions, finals_child_id, &finals)?;

        // skip Projection
        for node_id in finals.iter().skip(1).rev() {
            self.set_distribution(self.get_relational_output(*node_id)?)?;
        }

        if matches!(
            self.get_relation_node(finals_child_id)?,
            Relational::GroupBy { .. }
        ) {
            self.set_distribution(self.get_relational_output(final_proj_id)?)?;
        } else {
            self.set_dist(
                self.get_relational_output(final_proj_id)?,
                Distribution::Single,
            )?;
        }

        Ok(true)
    }
}