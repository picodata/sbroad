use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::AggregateInfo;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::tree::traversal::{BreadthFirst, PostOrder, EXPR_CAPACITY};
use crate::ir::{Node, Plan};
use std::collections::{HashMap, HashSet};

struct ExpressionMapper<'plan> {
    gr_exprs: &'plan Vec<usize>,
    map: &'plan mut HashMap<usize, Vec<(usize, usize)>>,
    plan: &'plan Plan,
    left_child_id: usize,
    top_id: Option<usize>, // root of projection expression, for which `find` was called
}

impl<'plan> ExpressionMapper<'plan> {
    fn new(
        gr_id: usize,
        map: &'plan mut HashMap<usize, Vec<(usize, usize)>>,
        plan: &'plan Plan,
        left_child_id: usize,
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
            left_child_id,
            top_id: None,
        };
        Ok(res)
    }

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

    fn find(&mut self, current: usize) -> Result<(), SbroadError> {
        if let Some(gr_expr) = self
            .gr_exprs
            .iter()
            .find(|gr_expr| {
                self.plan
                    .are_subtrees_equal(current, **gr_expr, self.left_child_id)
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
        if let Node::Expression(Expression::Reference { position, .. }) =
            self.plan.get_node(current)?
        {
            // We found a column which is not inside aggregate function
            // and it is not a grouping expression:
            // select a from t group by b - is invalid
            let column_name = {
                let alias_id = self
                    .plan
                    .get_row_list(self.plan.get_relational_output(self.left_child_id)?)?
                    .get(*position)
                    .ok_or_else(|| SbroadError::UnexpectedNumberOfValues(String::new()))?;
                if let Node::Expression(Expression::Alias { name, .. }) =
                    self.plan.get_node(*alias_id)?
                {
                    Ok::<&str, SbroadError>(name.as_str())
                } else {
                    Ok::<&str, SbroadError>("")
                }
            }
            .unwrap_or("");
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

    fn add_local_projection(
        &mut self,
        local_groupby_id: usize,
        infos: &Vec<AggregateInfo>,
        gr_proj: &HashMap<usize, Vec<(usize, usize)>>,
        gr_expr_to_alias: &mut HashMap<usize, String>,
    ) -> Result<usize, SbroadError> {
        {
            // Check input node
            let node = self.get_relation_node(local_groupby_id)?;
            if !matches!(node, Relational::GroupBy { .. }) {
                return Err(SbroadError::Invalid(Entity::Node, Some(
                    format!("add_local_projection: expected Relational::GroupBy node on id: {local_groupby_id}, got: {node:?}"))));
            }
        }

        let gr_cols_len = if let Relational::GroupBy { gr_cols, .. } =
            self.get_relation_node(local_groupby_id)?
        {
            gr_cols.len()
        } else {
            return Err(SbroadError::Invalid(Entity::Node, None));
        };
        let mut proj_output_cols: Vec<usize> = Vec::with_capacity(gr_cols_len + infos.len());
        for col_idx in 0..gr_cols_len {
            let new_col = self.get_groupby_col(local_groupby_id, col_idx)?;
            // if some grouping expression is not found among projection expressions, then we don't need it
            // E.g select a from t group by a, b
            // map: select a from t group by a, b
            if let Some(proj_expr_ids) = gr_proj.get(&new_col) {
                if proj_expr_ids.is_empty() {
                    continue;
                }
                let local_alias = Self::generate_local_alias(new_col);
                let new_alias = self.nodes.add_alias(&local_alias, new_col)?;
                gr_expr_to_alias.insert(new_col, local_alias);
                proj_output_cols.push(new_alias);
            }
        }
        for info in infos {
            // we take whole expression tree inside aggregate function and reuse it here, the
            // References' positions are valid because local and final GroupBy have the same
            // output
            let locals = info.aggregate.create_columns_for_local_projection(
                self,
                local_groupby_id,
                info.is_distinct,
            )?;
            proj_output_cols.extend(locals.into_iter());
        }
        let proj_output = self.nodes.add_row(proj_output_cols, None);
        let proj = Relational::Projection {
            output: proj_output,
            children: vec![local_groupby_id],
        };
        let proj_id = self.nodes.push(Node::Relational(proj));
        for info in infos {
            self.replace_parent_in_subtree(info.aggregate.fun_id, None, Some(proj_id))?;
        }
        Ok(proj_id)
    }

    #[allow(clippy::too_many_lines)]
    fn update_final_proj_columns(
        &mut self,
        final_id: usize,
        columns_with_aggregates: &HashSet<usize>,
        infos: &Vec<AggregateInfo>,
        gr_proj: &HashMap<usize, Vec<(usize, usize)>>,
        gr_expr_to_alias: &HashMap<usize, String>,
    ) -> Result<(), SbroadError> {
        // Maps previous aggregate top to new top in final stage of aggregation
        let mut top_to_final_aggr: HashMap<usize, usize> = HashMap::with_capacity(infos.len());
        let alias_to_pos_map = self
            .get_relation_node(final_id)?
            .output_alias_position_map(&self.nodes)?
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<HashMap<String, usize>>();
        for info in infos {
            let final_aggr_id = info.aggregate.create_column_for_final_projection(
                self,
                &alias_to_pos_map,
                info.is_distinct,
            )?;
            top_to_final_aggr.insert(info.aggregate.fun_id, final_aggr_id);
        }

        // Add final aggregate expressions inside expression trees with aggregates
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

        // Replace grouping expressions in projection columns with corresponding aliases
        for (gr_expr, proj_exprs) in gr_proj {
            for (expr_id, col_id) in proj_exprs {
                let Some(alias) = gr_expr_to_alias.get(gr_expr) else {
                    continue;
                };
                let Some(position) = alias_to_pos_map.get(alias) else {
                    return Err(SbroadError::Invalid(
                        Entity::Node,
                        Some(format!("update_final_proj_cols: could not find alias ({alias}) in child output: {final_id}")))
                    )
                };
                let new_ref = Expression::Reference {
                    position: *position,
                    parent: None,
                    targets: Some(vec![0]),
                };
                let ref_id = self.nodes.push(Node::Expression(new_ref));
                let mut bfs = BreadthFirst::with_capacity(
                    |x| self.nodes.aggregate_iter(x, false),
                    EXPR_CAPACITY,
                    EXPR_CAPACITY,
                );
                // we assume that projection columns have alias
                let column_node = self.get_node(*col_id)?;
                if !matches!(column_node, Node::Expression(Expression::Alias { .. })) {
                    return Err(SbroadError::Invalid(
                        Entity::Column,
                        Some(format!(
                            "expected id of column expression with alias, got: {column_node:?}"
                        )),
                    ));
                }
                bfs.populate_nodes(*col_id);
                let nodes = bfs.take_nodes();
                for (_, n_id) in nodes {
                    let expr = self.get_mut_expression_node(n_id)?;
                    match expr {
                        Expression::Cast { child, .. }
                        | Expression::Unary { child, .. }
                        | Expression::Alias { child, .. } => {
                            if *child == *expr_id {
                                *child = ref_id;
                            }
                        }
                        Expression::Bool { left, right, .. }
                        | Expression::Arithmetic { left, right, .. }
                        | Expression::Concat { left, right, .. } => {
                            if *left == *expr_id {
                                *left = ref_id;
                            }
                            if *right == *expr_id {
                                *right = ref_id;
                            }
                        }
                        Expression::Row { list, .. }
                        | Expression::StableFunction { children: list, .. } => {
                            for child in list {
                                if *child == *expr_id {
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
    ///
    /// This function assumes that [`lhs`] may have uninitialized references:
    /// references without parent, so it is needed to pass [`left_child_id`]
    /// to resolve such references.
    ///
    /// # Errors
    /// - [`error_on_alias`] is true, and [`Expression::Alias`] was encountered
    /// - invalid [`Expression::Reference`]s in either of subtrees
    pub fn are_subtrees_equal(
        &self,
        lhs: usize,
        rhs: usize,
        left_child_id: usize,
    ) -> Result<bool, SbroadError> {
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
                                && self.are_subtrees_equal(
                                    *left_left,
                                    *left_right,
                                    left_child_id,
                                )?
                                && self.are_subtrees_equal(
                                    *right_left,
                                    *right_right,
                                    left_child_id,
                                )?);
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
                            with_parentheses: parends_right,
                        } = right
                        {
                            return Ok(*op_left == *op_right
                                && *parens_left == *parends_right
                                && self.are_subtrees_equal(*l_left, *l_right, left_child_id)?
                                && self.are_subtrees_equal(*r_left, *r_right, left_child_id)?);
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
                                && self.are_subtrees_equal(
                                    *child_left,
                                    *child_right,
                                    left_child_id,
                                )?);
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
                            return Ok(self.are_subtrees_equal(
                                *left_left,
                                *left_right,
                                left_child_id,
                            )? && self.are_subtrees_equal(
                                *right_left,
                                *right_right,
                                left_child_id,
                            )?);
                        }
                    }
                    Expression::Constant { value: value_left } => {
                        if let Expression::Constant { value: value_right } = right {
                            return Ok(*value_left == *value_right);
                        }
                    }
                    Expression::Reference { position, .. } => {
                        if let Expression::Reference { .. } = right {
                            let alias_left = if let Some(alias_id) = self
                                .get_expression_node(self.get_relational_output(left_child_id)?)?
                                .get_row_list()?
                                .get(*position)
                            {
                                self.get_expression_node(*alias_id)?.get_alias_name()?
                            } else {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format!("are_subtrees_equal: invalid reference position of left side node: {lhs}"))
                                ));
                            };
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
                            return Ok(list_left.iter().zip(list_right.iter()).all(|(l, r)| {
                                self.are_subtrees_equal(*l, *r, left_child_id)
                                    .unwrap_or(false)
                            }));
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
                                    |(l, r)| {
                                        self.are_subtrees_equal(*l, *r, left_child_id)
                                            .unwrap_or(false)
                                    },
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
                                && self.are_subtrees_equal(
                                    *child_left,
                                    *child_right,
                                    left_child_id,
                                )?);
                        }
                    }
                }
            }
        }
        Ok(false)
    }

    /// Adds local stage for aggregation
    ///
    /// # Errors
    /// - failed to create local `GroupBy` node
    /// - failed to create local `Projection` node
    /// - failed to create `SQ` node
    /// - failed to change final `GroupBy` child to `SQ`
    /// - failed to update expressions in final `Projection`
    #[allow(clippy::too_many_lines)]
    pub fn add_two_stage_aggregation(
        &mut self,
        local_id: usize,
        proj_cols: &Vec<usize>,
        columns_with_aggregates: &HashSet<usize>,
        infos: &Vec<AggregateInfo>,
    ) -> Result<usize, SbroadError> {
        // Check that we don't have aggregates inside aggregates:
        // count(sum(b)) - makes no sense
        for info in infos {
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
        let gr_cols_len: usize =
            if let Relational::GroupBy { gr_cols, .. } = self.get_relation_node(local_id)? {
                gr_cols.len()
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!(
                        "add_two_stage_aggregation: expected GroupBy node on id: {local_id}!"
                    )),
                ));
            };
        // map final grouping expression id to pos of column in projection and expression ids that match this grouping expression in this column
        let mut gr_proj: HashMap<usize, Vec<(usize, usize)>> =
            HashMap::with_capacity(gr_cols_len * 2);
        let mut mapper = ExpressionMapper::new(local_id, &mut gr_proj, self, local_id)?;
        for col in proj_cols {
            mapper.find_matches(*col)?;
        }

        // After we change final GroupBy output, references in proj columns will
        // become invalid, to update references we need to know
        // which alias this reference had before
        let mut ref_to_alias: HashMap<usize, String> =
            HashMap::with_capacity(columns_with_aggregates.len());
        for col in proj_cols {
            let mut dfs =
                PostOrder::with_capacity(|x| self.nodes.expr_iter(x, false), EXPR_CAPACITY);
            for (_, expr_id) in dfs.iter(*col) {
                let node = self.get_expression_node(expr_id)?;
                if let Expression::Reference { position, .. } = node {
                    let alias: String = self.get_relational_aliases(local_id)?
                        .get(*position)
                        .ok_or_else(|| SbroadError::Invalid(
                            Entity::Node,
                            Some(format!("Reference in projection column has invalid position. {node:?}, id: {expr_id}"))))?.clone();
                    ref_to_alias.insert(expr_id, alias);
                }
            }
        }

        let mut gr_expr_to_alias: HashMap<usize, String> = HashMap::with_capacity(gr_cols_len * 2);
        let proj_id =
            self.add_local_projection(local_id, infos, &gr_proj, &mut gr_expr_to_alias)?;
        // If we generate an alias using uuid (like we do for tmp spaces) the penalty would be  redundant
        // verbosity in the column names. We can't set an alias `None` here as well, because then the frontend
        // would not generate parentheses for a subquery while building sql.
        let sq_id = self.add_sub_query(proj_id, Some(""))?;
        let final_gr_id = self.add_final_groupby(local_id, sq_id, &gr_expr_to_alias)?;

        self.update_final_proj_columns(
            final_gr_id,
            columns_with_aggregates,
            infos,
            &gr_proj,
            &gr_expr_to_alias,
        )?;

        Ok(final_gr_id)
    }

    fn add_final_groupby(
        &mut self,
        local_id: usize,
        child_id: usize,
        gr_exp_to_alias: &HashMap<usize, String>,
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

        Ok(final_id)
    }

    /// Adds final `GroupBy` node to `Plan`
    ///
    /// # Errors
    /// - invalid children count
    /// - failed to create output for `GroupBy`
    pub fn add_groupby(&mut self, children: &[usize]) -> Result<usize, SbroadError> {
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

        let final_output = self.add_row_for_output(*first_child, &[], true)?;
        let groupby = Relational::GroupBy {
            children: [*first_child].to_vec(),
            gr_cols: other.to_vec(),
            output: final_output,
            is_final: false,
        };

        let groupby_id = self.nodes.push(Node::Relational(groupby));

        self.replace_parent_in_subtree(final_output, None, Some(groupby_id))?;
        for col in children.iter().skip(1) {
            self.replace_parent_in_subtree(*col, None, Some(groupby_id))?;
        }

        Ok(groupby_id)
    }
}
