use crate::errors::SbroadError;
use crate::ir::block::Block;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::tree::traversal::{LevelNode, PostOrder};
use crate::ir::value::Value;
use crate::ir::{Node, NodeId, OptionParamValue, Plan, ValueIdx};
use crate::otm::child_span;
use sbroad_proc::otm_child_span;

use crate::ir::relation::Type;
use ahash::{AHashMap, AHashSet, RandomState};
use std::collections::HashMap;

struct ParamsBinder<'binder> {
    plan: &'binder mut Plan,
    /// Plan nodes to traverse during binding.
    nodes: Vec<LevelNode>,
    /// Number of parameters met in the OPTIONs.
    binded_options_counter: usize,
    /// Flag indicating whether we use Tarantool parameters notation.
    tnt_params_style: bool,
    /// Map of { plan param_id -> corresponding value }.
    pg_params_map: HashMap<NodeId, ValueIdx>,
    /// Plan nodes that correspond to Parameters.
    param_node_ids: AHashSet<NodeId>,
    /// Params transformed into constant Values.
    value_ids: Vec<NodeId>,
    /// Values that should be bind.
    values: Vec<Value>,
    /// We need to use rows instead of values in some cases (AST can solve
    /// this problem for non-parameterized queries, but for parameterized
    /// queries it is IR responsibility).
    ///
    /// Map of { param_id -> corresponding row }.
    row_map: AHashMap<usize, usize, RandomState>,
}

fn get_param_value(
    tnt_params_style: bool,
    param_id: usize,
    param_index: usize,
    value_ids: &[usize],
    pg_params_map: &HashMap<NodeId, ValueIdx>,
) -> usize {
    let value_index = if tnt_params_style {
        // In case non-pg params are used, index is the correct position
        param_index
    } else {
        value_ids.len()
            - 1
            - *pg_params_map.get(&param_id).unwrap_or_else(|| {
                panic!("Value index not found for parameter with id: {param_id}.")
            })
    };
    let val_id = value_ids
        .get(value_index)
        .unwrap_or_else(|| panic!("Parameter not found in position {value_index}."));
    *val_id
}

impl<'binder> ParamsBinder<'binder> {
    fn new(plan: &'binder mut Plan, mut values: Vec<Value>) -> Result<Self, SbroadError> {
        let capacity = plan.next_id();
        let mut tree = PostOrder::with_capacity(|node| plan.subtree_iter(node, false), capacity);
        let top_id = plan.get_top()?;
        tree.populate_nodes(top_id);
        let nodes = tree.take_nodes();

        let mut binded_options_counter = 0;
        if !plan.raw_options.is_empty() {
            binded_options_counter = plan.bind_option_params(&mut values);
        }

        let param_node_ids = plan.get_param_set();
        let tnt_params_style = plan.pg_params_map.is_empty();
        let pg_params_map = std::mem::take(&mut plan.pg_params_map);

        let binder = ParamsBinder {
            plan,
            nodes,
            binded_options_counter,
            tnt_params_style,
            pg_params_map,
            param_node_ids,
            value_ids: Vec::new(),
            values,
            row_map: AHashMap::new(),
        };
        Ok(binder)
    }

    /// Copy values to bind for Postgres-style parameters.
    fn handle_pg_parameters(&mut self) -> Result<(), SbroadError> {
        if !self.tnt_params_style {
            // Due to how we calculate hash for plan subtree and the
            // fact that pg parameters can refer to same value multiple
            // times we currently copy params that are referred more
            // than once in order to get the same hash.
            // See https://git.picodata.io/picodata/picodata/sbroad/-/issues/583
            let mut used_values = vec![false; self.values.len()];
            let invalid_idx = |param_id: usize, value_idx: usize| {
                panic!("Out of bounds value index {value_idx} for pg parameter {param_id}.");
            };

            // NB: we can't use `param_node_ids`, we need to traverse
            // parameters in the same order they will be bound,
            // otherwise we may get different hashes for plans
            // with tnt and pg parameters. See `subtree_hash*` tests,
            for (_, param_id) in &self.nodes {
                if !matches!(self.plan.get_node(*param_id)?, Node::Parameter) {
                    continue;
                }
                let value_idx = *self.pg_params_map.get(param_id).unwrap_or_else(|| {
                    panic!("Value index not found for parameter with id: {param_id}.");
                });
                if used_values.get(value_idx).copied().unwrap_or(true) {
                    let Some(value) = self.values.get(value_idx) else {
                        invalid_idx(*param_id, value_idx)
                    };
                    self.values.push(value.clone());
                    self.pg_params_map
                        .entry(*param_id)
                        .and_modify(|value_idx| *value_idx = self.values.len() - 1);
                } else if let Some(used) = used_values.get_mut(value_idx) {
                    *used = true;
                } else {
                    invalid_idx(*param_id, value_idx)
                }
            }
        }
        Ok(())
    }

    /// Transform parameters (passed by user) to values (plan constants).
    /// The result values are stored in the opposite to parameters order.
    ///
    /// In case some redundant params were passed, they'll
    /// be ignored (just not popped from the `value_ids` stack later).
    fn create_parameter_constants(&mut self) {
        self.value_ids = Vec::with_capacity(self.values.len());
        while let Some(param) = self.values.pop() {
            self.value_ids.push(self.plan.add_const(param));
        }
    }

    /// Check that number of user passed params equal to the params nodes we have to bind.
    fn check_params_count(&self) {
        let non_binded_params_len = self.param_node_ids.len() - self.binded_options_counter;
        assert!(
            !(self.tnt_params_style && non_binded_params_len > self.value_ids.len()),
            "Expected at least {} values for parameters. Got {}.",
            non_binded_params_len,
            self.value_ids.len()
        );
    }

    /// Retrieve a corresponding value (plan constant node) for a parameter node.
    fn get_param_value(&self, param_id: usize, param_index: usize) -> usize {
        get_param_value(
            self.tnt_params_style,
            param_id,
            param_index,
            &self.value_ids,
            &self.pg_params_map,
        )
    }

    /// 1.) Increase binding param index.
    /// 2.) In case `cover_with_row` is set to true, cover the param node with a row.
    fn cover_param_with_row(
        &self,
        param_id: usize,
        cover_with_row: bool,
        param_index: &mut usize,
        row_ids: &mut HashMap<usize, usize, RandomState>,
    ) {
        if self.param_node_ids.contains(&param_id) {
            if row_ids.contains_key(&param_id) {
                return;
            }
            *param_index = param_index.saturating_sub(1);
            if cover_with_row {
                let val_id = self.get_param_value(param_id, *param_index);
                row_ids.insert(param_id, val_id);
            }
        }
    }

    /// Traverse the plan nodes tree and cover parameter nodes with rows if needed.
    #[allow(clippy::too_many_lines)]
    fn cover_params_with_rows(&mut self) -> Result<(), SbroadError> {
        // Len of `value_ids` - `param_index` = param index we are currently binding.
        let mut param_index = self.value_ids.len();

        let mut row_ids = HashMap::with_hasher(RandomState::new());

        for (_, id) in &self.nodes {
            let node = self.plan.get_node(*id)?;
            match node {
                // Note: Parameter may not be met at the top of relational operators' expression
                //       trees such as OrderBy and GroupBy, because it won't influence ordering and
                //       grouping correspondingly. These cases are handled during parsing stage.
                Node::Relational(rel) => match rel {
                    Relational::Having {
                        filter: ref param_id,
                        ..
                    }
                    | Relational::Selection {
                        filter: ref param_id,
                        ..
                    }
                    | Relational::Join {
                        condition: ref param_id,
                        ..
                    } => {
                        self.cover_param_with_row(*param_id, true, &mut param_index, &mut row_ids);
                    }
                    _ => {}
                },
                Node::Expression(expr) => match expr {
                    Expression::Alias {
                        child: ref param_id,
                        ..
                    }
                    | Expression::ExprInParentheses {
                        child: ref param_id,
                    }
                    | Expression::Cast {
                        child: ref param_id,
                        ..
                    }
                    | Expression::Unary {
                        child: ref param_id,
                        ..
                    } => {
                        self.cover_param_with_row(*param_id, false, &mut param_index, &mut row_ids);
                    }
                    Expression::Bool {
                        ref left,
                        ref right,
                        ..
                    }
                    | Expression::Arithmetic {
                        ref left,
                        ref right,
                        ..
                    }
                    | Expression::Concat {
                        ref left,
                        ref right,
                    } => {
                        for param_id in &[*left, *right] {
                            self.cover_param_with_row(
                                *param_id,
                                true,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                    Expression::Trim {
                        ref pattern,
                        ref target,
                        ..
                    } => {
                        let params = match pattern {
                            Some(p) => [Some(*p), Some(*target)],
                            None => [None, Some(*target)],
                        };
                        for param_id in params.into_iter().flatten() {
                            self.cover_param_with_row(
                                param_id,
                                true,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                    Expression::Row { ref list, .. }
                    | Expression::StableFunction {
                        children: ref list, ..
                    } => {
                        for param_id in list {
                            // Parameter is already under row/function so that we don't
                            // have to cover it with `add_row` call.
                            self.cover_param_with_row(
                                *param_id,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                    Expression::Case {
                        ref search_expr,
                        ref when_blocks,
                        ref else_expr,
                    } => {
                        if let Some(search_expr) = search_expr {
                            self.cover_param_with_row(
                                *search_expr,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                        for (cond_expr, res_expr) in when_blocks {
                            self.cover_param_with_row(
                                *cond_expr,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                            self.cover_param_with_row(
                                *res_expr,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                        if let Some(else_expr) = else_expr {
                            self.cover_param_with_row(
                                *else_expr,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                    Expression::Reference { .. }
                    | Expression::Constant { .. }
                    | Expression::CountAsterisk => {}
                },
                Node::Block(block) => match block {
                    Block::Procedure { ref values, .. } => {
                        for param_id in values {
                            // We don't need to wrap arguments, passed into the
                            // procedure call, into the rows.
                            self.cover_param_with_row(
                                *param_id,
                                false,
                                &mut param_index,
                                &mut row_ids,
                            );
                        }
                    }
                },
                Node::Parameter | Node::Ddl(..) | Node::Acl(..) => {}
            }
        }

        let fixed_row_ids: AHashMap<usize, usize, RandomState> = row_ids
            .iter()
            .map(|(param_id, val_id)| {
                let row_cover = self.plan.nodes.add_row(vec![*val_id], None);
                (*param_id, row_cover)
            })
            .collect();
        self.row_map = fixed_row_ids;

        Ok(())
    }

    /// Replace parameters in the plan.
    #[allow(clippy::too_many_lines)]
    fn bind_params(&mut self) -> Result<(), SbroadError> {
        let mut exprs_to_set_ref_type: HashMap<usize, Type> = HashMap::new();

        for (_, id) in &self.nodes {
            // Before binding, references that referred to
            // parameters had scalar type (by default),
            // but in fact they may refer to different stuff.
            if let Node::Expression(expr) = self.plan.get_node(*id)? {
                if let Expression::Reference { .. } = expr {
                    exprs_to_set_ref_type.insert(*id, expr.recalculate_type(self.plan)?);
                    continue;
                }
            }
        }
        for (id, new_type) in exprs_to_set_ref_type {
            let expr = self.plan.get_mut_expression_node(id)?;
            expr.set_ref_type(new_type);
        }

        // Len of `value_ids` - `param_index` = param index we are currently binding.
        let mut param_index = self.value_ids.len();

        let tnt_params_style = self.tnt_params_style;
        let row_ids = std::mem::take(&mut self.row_map);
        let value_ids = std::mem::take(&mut self.value_ids);
        let pg_params_map = std::mem::take(&mut self.pg_params_map);

        let bind_param = |param_id: &mut usize, is_row: bool, param_index: &mut usize| {
            *param_id = if self.param_node_ids.contains(param_id) {
                *param_index = param_index.saturating_sub(1);
                let binding_node_id = if is_row {
                    *row_ids
                        .get(param_id)
                        .unwrap_or_else(|| panic!("Row not found at position {param_id}"))
                } else {
                    get_param_value(
                        tnt_params_style,
                        *param_id,
                        *param_index,
                        &value_ids,
                        &pg_params_map,
                    )
                };
                binding_node_id
            } else {
                *param_id
            }
        };

        for (_, id) in &self.nodes {
            let node = self.plan.get_mut_node(*id)?;
            match node {
                Node::Relational(rel) => match rel {
                    Relational::Having {
                        filter: ref mut param_id,
                        ..
                    }
                    | Relational::Selection {
                        filter: ref mut param_id,
                        ..
                    }
                    | Relational::Join {
                        condition: ref mut param_id,
                        ..
                    } => {
                        bind_param(param_id, true, &mut param_index);
                    }
                    _ => {}
                },
                Node::Expression(expr) => match expr {
                    Expression::Alias {
                        child: ref mut param_id,
                        ..
                    }
                    | Expression::ExprInParentheses {
                        child: ref mut param_id,
                    }
                    | Expression::Cast {
                        child: ref mut param_id,
                        ..
                    }
                    | Expression::Unary {
                        child: ref mut param_id,
                        ..
                    } => {
                        bind_param(param_id, false, &mut param_index);
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
                    } => {
                        for param_id in [left, right] {
                            bind_param(param_id, true, &mut param_index);
                        }
                    }
                    Expression::Trim {
                        ref mut pattern,
                        ref mut target,
                        ..
                    } => {
                        let params = match pattern {
                            Some(p) => [Some(p), Some(target)],
                            None => [None, Some(target)],
                        };
                        for param_id in params.into_iter().flatten() {
                            bind_param(param_id, true, &mut param_index);
                        }
                    }
                    Expression::Row { ref mut list, .. }
                    | Expression::StableFunction {
                        children: ref mut list,
                        ..
                    } => {
                        for param_id in list {
                            bind_param(param_id, false, &mut param_index);
                        }
                    }
                    Expression::Case {
                        ref mut search_expr,
                        ref mut when_blocks,
                        ref mut else_expr,
                    } => {
                        if let Some(param_id) = search_expr {
                            bind_param(param_id, false, &mut param_index);
                        }
                        for (param_id_1, param_id_2) in when_blocks {
                            bind_param(param_id_1, false, &mut param_index);
                            bind_param(param_id_2, false, &mut param_index);
                        }
                        if let Some(param_id) = else_expr {
                            bind_param(param_id, false, &mut param_index);
                        }
                    }
                    Expression::Reference { .. }
                    | Expression::Constant { .. }
                    | Expression::CountAsterisk => {}
                },
                Node::Block(block) => match block {
                    Block::Procedure { ref mut values, .. } => {
                        for param_id in values {
                            bind_param(param_id, false, &mut param_index);
                        }
                    }
                },
                Node::Parameter | Node::Ddl(..) | Node::Acl(..) => {}
            }
        }

        Ok(())
    }

    fn update_value_rows(&mut self) -> Result<(), SbroadError> {
        for (_, id) in &self.nodes {
            if let Ok(Node::Relational(Relational::ValuesRow { .. })) = self.plan.get_node(*id) {
                self.plan.update_values_row(*id)?;
            }
        }
        Ok(())
    }
}

impl Plan {
    pub fn add_param(&mut self) -> usize {
        self.nodes.push(Node::Parameter)
    }

    /// Bind params related to `Option` clause.
    /// Returns the number of params binded to options.
    ///
    /// # Errors
    /// - User didn't provide parameter value for corresponding option parameter
    ///
    /// # Panics
    /// - Plan is inconsistent state
    pub fn bind_option_params(&mut self, values: &mut Vec<Value>) -> usize {
        // Bind parameters in options to values.
        // Because the Option clause is the last clause in the
        // query the parameters are located in the end of params list.
        let mut binded_params_counter = 0usize;
        for opt in self.raw_options.iter_mut().rev() {
            if let OptionParamValue::Parameter { plan_id: param_id } = opt.val {
                if !self.pg_params_map.is_empty() {
                    // PG-like params syntax
                    let value_idx = *self.pg_params_map.get(&param_id).unwrap_or_else(|| {
                        panic!("No value idx in map for option parameter: {opt:?}.");
                    });
                    let value = values.get(value_idx).unwrap_or_else(|| {
                        panic!("Invalid value idx {value_idx}, for option: {opt:?}.");
                    });
                    opt.val = OptionParamValue::Value { val: value.clone() };
                } else if let Some(v) = values.pop() {
                    binded_params_counter += 1;
                    opt.val = OptionParamValue::Value { val: v };
                } else {
                    panic!("No parameter value specified for option: {}", opt.kind);
                }
            }
        }
        binded_params_counter
    }

    // Gather all parameter nodes from the tree to a hash set.
    #[must_use]
    pub fn get_param_set(&self) -> AHashSet<usize> {
        let param_set: AHashSet<usize> = self
            .nodes
            .arena
            .iter()
            .enumerate()
            .filter_map(|(id, node)| {
                if let Node::Parameter = node {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();
        param_set
    }

    /// Synchronize values row output with the data tuple after parameter binding.
    ///
    /// # Errors
    /// - Node is not values row
    /// - Output and data tuples have different number of columns
    /// - Output is not a row of aliases
    ///
    /// # Panics
    /// - Plan is inconsistent state
    pub fn update_values_row(&mut self, id: usize) -> Result<(), SbroadError> {
        let values_row = self.get_node(id)?;
        let (output_id, data_id) =
            if let Node::Relational(Relational::ValuesRow { output, data, .. }) = values_row {
                (*output, *data)
            } else {
                panic!("Expected a values row: {values_row:?}")
            };
        let data = self.get_expression_node(data_id)?;
        let data_list = data.clone_row_list()?;
        let output = self.get_expression_node(output_id)?;
        let output_list = output.clone_row_list()?;
        for (pos, alias_id) in output_list.iter().enumerate() {
            let new_child_id = *data_list
                .get(pos)
                .unwrap_or_else(|| panic!("Node not found at position {pos}"));
            let alias = self.get_mut_expression_node(*alias_id)?;
            if let Expression::Alias { ref mut child, .. } = alias {
                *child = new_child_id;
            } else {
                panic!("Expected an alias: {alias:?}")
            }
        }
        Ok(())
    }

    /// Substitute parameters to the plan.
    /// The purpose of this function is to find every `Parameter` node and replace it
    /// with `Expression::Constant` (under the row).
    ///
    /// # Errors
    /// - Invalid amount of parameters.
    /// - Internal errors.
    #[allow(clippy::too_many_lines)]
    #[otm_child_span("plan.bind")]
    pub fn bind_params(&mut self, values: Vec<Value>) -> Result<(), SbroadError> {
        // Nothing to do here.
        if values.is_empty() {
            return Ok(());
        }

        let mut binder = ParamsBinder::new(self, values)?;
        binder.handle_pg_parameters()?;
        binder.create_parameter_constants();
        binder.check_params_count();
        binder.cover_params_with_rows()?;
        binder.bind_params()?;
        binder.update_value_rows()?;

        Ok(())
    }
}
