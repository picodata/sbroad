use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::AggregateInfo;
use crate::ir::aggregates::SimpleAggregate;
use crate::ir::distribution::Distribution;
use crate::ir::expression::Expression;
use crate::ir::expression::Expression::StableFunction;
use crate::ir::operator::Relational;
use crate::ir::transformation::redistribution::{MotionKey, MotionPolicy, Strategy, Target};
use crate::ir::tree::traversal::{BreadthFirst, PostOrder, EXPR_CAPACITY};
use crate::ir::{Node, Plan};
use std::collections::{HashMap, HashSet};

/// Helper struct to map expressions used in `GroupBy` to
/// expressions used in `Projection`
struct ExpressionMapper<'plan> {
    /// List of expressions ids of `GroupBy`
    gr_exprs: &'plan Vec<usize>,
    /// Maps `GroupBy` expression to expressions used `Projection`
    /// First element in pair is `Projection` column id,
    /// second one is the id of expression
    map: &'plan mut HashMap<usize, Vec<(usize, usize)>>,
    plan: &'plan Plan,
    /// Root of projection expression, for which `find` was called
    top_id: Option<usize>,
}

impl<'plan> ExpressionMapper<'plan> {
    fn new(
        gr_id: usize,
        map: &'plan mut HashMap<usize, Vec<(usize, usize)>>,
        plan: &'plan Plan,
    ) -> Result<ExpressionMapper<'plan>, SbroadError> {
        let Relational::GroupBy { gr_cols: gr_exprs, .. } = plan.get_relation_node(gr_id)? else {
            return Err(SbroadError::Invalid(
                Entity::Node,
            Some(format!("ExpressionMapper: expected GroupBy on id: {gr_id}"))))  
        };
        let res = ExpressionMapper {
            gr_exprs,
            map,
            plan,
            top_id: None,
        };
        Ok(res)
    }

    /// Traverses given projection expression from top to bottom, trying
    /// to find subexpressions that match expressions located in `GroupBy`,
    /// when match is found it is stored in map passed to [`ExpressionMapper`]'s
    /// constructor.
    ///
    /// # Errors
    /// - invalid references in any expression (`GroupBy`'s or `Projection`'s one)
    /// - invalid query: `Projection` expression contains references that are not
    /// found in `GroupBy` expression. The reason is that user specified expression in
    /// `Projection` that does not match any expression in `GroupBy`
    fn find_matches(&mut self, proj_expr: usize) -> Result<(), SbroadError> {
        let top_id = match self.plan.get_expression_node(proj_expr)? {
            Expression::Alias { child, .. } => *child,
            _ => proj_expr,
        };
        // here we use proj_expr, because later when updating columns in final Projection
        // we will need the id of column with alias
        self.top_id = Some(proj_expr);
        self.find(top_id)?;
        self.top_id = None;
        Ok(())
    }

    /// Try to match expression pointed by `current` to some expression
    /// in `GroupBy`, if match is found it is stored in map. Otherwise
    /// this function recursively tries to find matches in `current` children.
    ///
    /// # Errors
    /// - invalid references in expressions
    /// - failed to match leaf expression (`Reference`) to any of the `GroupBy` expressions.
    /// This error means that user specified some expression in projection that does not match to
    /// any expression in `GroupBy`.
    fn find(&mut self, current: usize) -> Result<(), SbroadError> {
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
            let top_id = self
                .top_id
                .ok_or(SbroadError::Invalid(Entity::ExpressionMapper, None))?;
            if let Some(v) = self.map.get_mut(&gr_expr) {
                v.push((current, top_id));
            } else {
                self.map.insert(gr_expr, vec![(current, top_id)]);
            }
            return Ok(());
        }
        let node = self.plan.get_expression_node(current)?;
        if let Expression::Reference { .. } = node {
            // We found a column which is not inside aggregate function
            // and it is not a grouping expression:
            // select a from t group by b - is invalid
            let column_name = match self.plan.get_alias_from_reference_node(node) {
                Ok(alias) => alias.to_string(),
                Err(e) => format!("`failed to retrieve column name: {e}`"),
            };
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some(format!("Invalid projection with GROUP BY clause: column {column_name} is not found in grouping expressions!")
                )));
        }
        for child in self.plan.nodes.aggregate_iter(current, false) {
            self.find(child)?;
        }
        Ok(())
    }
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

    /// Create Projection node for Map(local) stage of 2-stage aggregation
    ///
    /// # Arguments
    ///
    /// * `final_proj_id` - id of Projection that was created during creation of Plan
    /// from AST. It is a Projection in Reduce (final) stage. It contains columns with aggregates
    /// and grouping expressions.
    /// * `child_id` - id of child for Projection node to be created.
    /// * `infos` - vector of metadata for each aggregate function that was found in final
    /// projection. Each info specifies what kind of aggregate it is (sum, avg, etc) and location
    /// in final projection.
    /// * `grouping_positions` - positions by which local `GroupBy` will be done, excluding positions
    /// of grouping expressions from distinct aggregates.
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
    /// Aggregates encapsulate this logic in themselves, see [`create_columns_for_local_projection`]
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
    /// - map between `GroupBy` expression and corresponding local alias.
    fn add_local_projection(
        &mut self,
        final_proj_id: usize,
        child_id: usize,
        infos: &Vec<AggregateInfo>,
        grouping_positions: &mut Vec<usize>,
    ) -> Result<(usize, Option<HashMap<usize, String>>), SbroadError> {
        let mut proj_output_cols: Vec<usize> = Vec::with_capacity(infos.len());
        // Map expression id in GroupBy to alias used in local Projection for future needs
        // If there is no GroupBy, it's None
        let mut map: Option<HashMap<usize, String>> = None;
        if let Relational::GroupBy { gr_cols, .. } = self.get_relation_node(child_id)? {
            let gr_cols_len = gr_cols.len();
            let mut gr_expr_to_alias: HashMap<usize, String> =
                HashMap::with_capacity(gr_cols_len * 2);
            proj_output_cols.reserve(gr_cols_len + proj_output_cols.capacity());
            for col_idx in 0..gr_cols_len {
                let new_col = self.get_groupby_col(child_id, col_idx)?;
                let local_alias = Self::generate_local_alias(new_col);
                let new_alias = self.nodes.add_alias(&local_alias, new_col)?;
                gr_expr_to_alias.insert(new_col, local_alias);
                proj_output_cols.push(new_alias);
                grouping_positions.push(col_idx);
            }
            map = Some(gr_expr_to_alias);
        }
        let local_proj_child_id = {
            let mut local_proj_child_id = child_id;
            let has_distinct_aggregates = infos.iter().any(|x| x.is_distinct);
            if has_distinct_aggregates {
                let mut grouping_exprs: Vec<usize> = vec![];
                for info in infos.iter().filter(|x| x.is_distinct) {
                    for expr_id in self.nodes.expr_iter(info.aggregate.fun_id, false) {
                        grouping_exprs.push(expr_id);
                    }
                }
                if let Relational::GroupBy { gr_cols, .. } = self.get_mut_relation_node(child_id)? {
                    gr_cols.extend(grouping_exprs.into_iter());
                } else {
                    // In case original query didn't have GroupBy, we didn't create local GroupBy node.
                    // So we need to create it
                    local_proj_child_id = self.add_groupby(child_id, &grouping_exprs, false)?;
                    self.set_distribution(self.get_relational_output(local_proj_child_id)?)?;
                }
            }
            local_proj_child_id
        };
        for info in infos {
            // we take whole expression tree inside aggregate function and reuse it here, the
            // References' positions are valid because local and final GroupBy have the same
            // output
            let locals = info
                .aggregate
                .create_columns_for_local_projection(self, info.is_distinct)?;
            proj_output_cols.extend(locals.into_iter());
        }
        let proj_output = self.nodes.add_row(proj_output_cols, None);
        let proj = Relational::Projection {
            output: proj_output,
            children: vec![local_proj_child_id],
        };
        let proj_id = self.nodes.push(Node::Relational(proj));
        for info in infos {
            // We take expressions inside aggregate functions from Final projection,
            // so we need to update parent
            self.replace_parent_in_subtree(
                info.aggregate.fun_id,
                Some(final_proj_id),
                Some(proj_id),
            )?;
        }
        self.set_distribution(proj_output)?;
        Ok((proj_id, map))
    }

    #[allow(clippy::too_many_lines)]
    fn update_final_proj_columns(
        &mut self,
        final_proj_id: usize,
        final_proj_child_id: usize,
        columns_with_aggregates: &HashSet<usize>,
        infos: &Vec<AggregateInfo>,
        gr_proj: Option<HashMap<usize, Vec<(usize, usize)>>>,
        gr_expr_to_alias: Option<HashMap<usize, String>>,
    ) -> Result<(), SbroadError> {
        // Maps previous aggregate top to new top in final stage of aggregation
        let mut top_to_final_aggr: HashMap<usize, usize> = HashMap::with_capacity(infos.len());
        let alias_to_pos_map = self
            .get_relation_node(final_proj_child_id)?
            .output_alias_position_map(&self.nodes)?
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<HashMap<String, usize>>();
        for info in infos {
            let final_aggr_id = info.aggregate.create_column_for_final_projection(
                final_proj_id,
                self,
                &alias_to_pos_map,
                info.is_distinct,
            )?;
            top_to_final_aggr.insert(info.aggregate.fun_id, final_aggr_id);
        }

        // Add final aggregate expressions inside expression trees with aggregates
        // todo: remember parent of aggregate function instead of searching for it
        for column_top in columns_with_aggregates {
            let mut dfs = BreadthFirst::with_capacity(
                |x| self.nodes.aggregate_iter(x, false),
                EXPR_CAPACITY,
                EXPR_CAPACITY,
            );
            dfs.populate_nodes(*column_top);
            let nodes = dfs.take_nodes();
            for (_, n_id) in nodes {
                let expr = self.get_mut_expression_node(n_id)?;
                match expr {
                    Expression::Cast { child, .. }
                    | Expression::Unary { child, .. }
                    | Expression::Alias { child, .. } => {
                        if let Some(new_top) = top_to_final_aggr.get(child) {
                            *child = *new_top;
                        }
                    }
                    Expression::Bool { left, right, .. }
                    | Expression::Arithmetic { left, right, .. }
                    | Expression::Concat { left, right, .. } => {
                        if let Some(new_top) = top_to_final_aggr.get(left) {
                            *left = *new_top;
                        }
                        if let Some(new_top) = top_to_final_aggr.get(right) {
                            *right = *new_top;
                        }
                    }
                    Expression::Row { list, .. }
                    | Expression::StableFunction { children: list, .. } => {
                        for child in list {
                            if let Some(new_top) = top_to_final_aggr.get(child) {
                                *child = *new_top;
                            }
                        }
                    }
                    Expression::Constant { .. } | Expression::Reference { .. } => {}
                }
            }
        }

        //  gr_proj is None in case we have aggregates without GroupBy
        let Some(gr_proj) = gr_proj else {
            return Ok(())
        };
        let Some(gr_expr_to_alias) = gr_expr_to_alias else {
            return Err(SbroadError::Invalid(Entity::Node,
            Some("missing map from GroupBy expression to local alias".into())))
        };

        // Replace grouping expressions in projection columns with corresponding aliases
        for (gr_expr, proj_exprs) in gr_proj {
            for (expr_id, col_id) in proj_exprs {
                let Some(alias) = gr_expr_to_alias.get(&gr_expr) else {
                    continue;
                };
                let Some(position) = alias_to_pos_map.get(alias) else {
                    return Err(SbroadError::Invalid(
                        Entity::Node,
                        Some(format!("update_final_proj_columns: could not find alias ({alias}) in child output: {final_proj_child_id}")))
                    )
                };
                let new_ref = Expression::Reference {
                    position: *position,
                    parent: Some(final_proj_id),
                    targets: Some(vec![0]),
                };
                let ref_id = self.nodes.push(Node::Expression(new_ref));
                let mut bfs = BreadthFirst::with_capacity(
                    |x| self.nodes.aggregate_iter(x, false),
                    EXPR_CAPACITY,
                    EXPR_CAPACITY,
                );
                // we assume that projection columns have alias
                let column_node = self.get_node(col_id)?;
                if !matches!(column_node, Node::Expression(Expression::Alias { .. })) {
                    return Err(SbroadError::Invalid(
                        Entity::Column,
                        Some(format!(
                            "expected id of column expression with alias, got: {column_node:?}"
                        )),
                    ));
                }
                bfs.populate_nodes(col_id);
                let nodes = bfs.take_nodes();
                for (_, n_id) in nodes {
                    let expr = self.get_mut_expression_node(n_id)?;
                    match expr {
                        Expression::Cast { child, .. }
                        | Expression::Unary { child, .. }
                        | Expression::Alias { child, .. } => {
                            if *child == expr_id {
                                *child = ref_id;
                            }
                        }
                        Expression::Bool { left, right, .. }
                        | Expression::Arithmetic { left, right, .. }
                        | Expression::Concat { left, right, .. } => {
                            if *left == expr_id {
                                *left = ref_id;
                            }
                            if *right == expr_id {
                                *right = ref_id;
                            }
                        }
                        Expression::Row { list, .. }
                        | Expression::StableFunction { children: list, .. } => {
                            for child in list {
                                if *child == expr_id {
                                    *child = ref_id;
                                    break;
                                }
                            }
                        }
                        Expression::Constant { .. } | Expression::Reference { .. } => {}
                    }
                }
            }
        }

        Ok(())
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
                    Expression::Alias { .. } => {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some(format!("are_subtrees_equal: unexpected alias: {lhs}")),
                        ));
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
    #[allow(clippy::too_many_lines)]
    pub fn add_two_stage_aggregation(&mut self, final_proj_id: usize) -> Result<bool, SbroadError> {
        let (proj_child_id, proj_output_id, proj_cols) =
            if let Relational::Projection {
                children, output, ..
            } = self.get_relation_node(final_proj_id)?
            {
                let child_id = *children.first().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Node,
                        Some(format!("Projection node {final_proj_id} has no children!")),
                    )
                })?;
                let cols = self.get_row_list(*output)?;
                (child_id, *output, cols)
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!("expected projection on id: {final_proj_id}")),
                ));
            };
        let mut infos: Vec<AggregateInfo> = Vec::with_capacity(proj_cols.len());
        let mut columns_with_aggregates: HashSet<usize> = HashSet::with_capacity(proj_cols.len());

        for plan_alias_id in proj_cols {
            // Collect aggregate functions inside column expression
            let mut bfs = BreadthFirst::with_capacity(
                |x| self.nodes.aggregate_iter(x, false),
                EXPR_CAPACITY,
                EXPR_CAPACITY,
            );
            for (_, id) in bfs.iter(*plan_alias_id) {
                let expr = self.get_expression_node(id)?;
                if let StableFunction {
                    name, is_distinct, ..
                } = expr
                {
                    let Some(aggr) = SimpleAggregate::new(name, id) else {
                        continue
                    };
                    let info = AggregateInfo {
                        aggregate: aggr,
                        expression_top: *plan_alias_id,
                        is_distinct: *is_distinct,
                    };
                    infos.push(info);
                    columns_with_aggregates.insert(*plan_alias_id);
                }
            }
        }

        // Check that we don't have aggregates inside aggregates:
        // count(sum(b)) - makes no sense
        for info in &infos {
            let top = info.aggregate.fun_id;
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
                            Some(format!("aggregate function inside aggregate function is not allowed. Got `{name}` inside `{}`", info.aggregate.kind))
                        ));
                    }
                }
            }
        }

        let has_groupby: bool = matches!(
            self.get_relation_node(proj_child_id)?,
            Relational::GroupBy { .. }
        );

        if !has_groupby && infos.is_empty() {
            return Ok(false);
        }

        // Mapping between group by and projection expressions.
        // group by expression -> (projection column id, projection expression id)
        let mut gr_proj: Option<HashMap<usize, Vec<(usize, usize)>>> = None;
        if has_groupby {
            // map final grouping expression id to pos of column in projection and expression ids that match this grouping expression in this column
            let gr_cols_len: usize = if let Relational::GroupBy { gr_cols, .. } =
                self.get_relation_node(proj_child_id)?
            {
                gr_cols.len()
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!(
                        "add_two_stage_aggregation: expected GroupBy node on id: {proj_child_id}!"
                    )),
                ));
            };
            let mut gr_proj_map: HashMap<usize, Vec<(usize, usize)>> =
                HashMap::with_capacity(gr_cols_len * 2);
            let mut mapper = ExpressionMapper::new(proj_child_id, &mut gr_proj_map, self)?;
            for col in proj_cols {
                mapper.find_matches(*col)?;
            }
            gr_proj = Some(gr_proj_map);
        }

        let mut grouping_positions: Vec<usize> = Vec::new();
        let (proj_id, gr_expr_to_alias) = self.add_local_projection(
            final_proj_id,
            proj_child_id,
            &infos,
            &mut grouping_positions,
        )?;

        // If we generate an alias using uuid (like we do for tmp spaces) the penalty would be  redundant
        // verbosity in the column names. We can't set an alias `None` here as well, because then the frontend
        // would not generate parentheses for a subquery while building sql.
        let sq_id = self.add_sub_query(proj_id, Some(""))?;
        self.set_distribution(self.get_relational_output(sq_id)?)?;

        let mut final_proj_child_id = sq_id;
        if has_groupby {
            let Some(gr_expr_to_alias) = gr_expr_to_alias.as_ref() else {
                return Err(SbroadError::Invalid(Entity::Node,
                Some(format!("missing map between GroupBy expression and local aliases. Local projection id: {proj_id}"))))
            };
            final_proj_child_id = self.add_final_groupby(
                proj_child_id,
                sq_id,
                gr_expr_to_alias,
                &grouping_positions,
            )?;
        }

        self.update_final_proj_columns(
            final_proj_id,
            final_proj_child_id,
            &columns_with_aggregates,
            &infos,
            gr_proj,
            gr_expr_to_alias,
        )?;

        self.set_relational_children(final_proj_id, vec![final_proj_child_id])?;
        if has_groupby {
            self.set_distribution(proj_output_id)?;
        } else {
            let mut strategy = Strategy::new(final_proj_id);
            strategy.add_child(final_proj_child_id, MotionPolicy::Full);
            self.create_motion_nodes(&strategy)?;
            self.set_dist(proj_output_id, Distribution::Single)?;
        }

        Ok(true)
    }

    fn add_final_groupby(
        &mut self,
        local_id: usize,
        child_id: usize,
        gr_exp_to_alias: &HashMap<usize, String>,
        grouping_positions: &Vec<usize>,
    ) -> Result<usize, SbroadError> {
        let grouping_cols_len =
            if let Relational::GroupBy { gr_cols, .. } = self.get_relation_node(local_id)? {
                gr_cols.len()
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!(
                        "add_final_groupby: expected GroupBy on id: {local_id}"
                    )),
                ));
            };
        let mut gr_cols: Vec<usize> = Vec::with_capacity(grouping_cols_len);
        let child_map = self
            .get_relation_node(child_id)?
            .output_alias_position_map(&self.nodes)?
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<HashMap<String, usize>>();
        for i in 0..grouping_cols_len {
            let col_id = self.get_groupby_col(local_id, i)?;
            let Some(local_alias) = gr_exp_to_alias.get(&col_id) else {
                continue
            };
            let Some(position) = child_map.get(local_alias) else {
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

        // Create motion node
        {
            let mut strategy = Strategy::new(final_id);
            // Use group by columns as the motion key.
            let mut targets: Vec<Target> = Vec::with_capacity(grouping_cols_len);
            for pos in grouping_positions {
                targets.push(Target::Reference(*pos));
            }
            strategy.add_child(child_id, MotionPolicy::Segment(MotionKey { targets }));
            self.create_motion_nodes(&strategy)?;
        }

        self.set_distribution(output)?;
        Ok(final_id)
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
}
