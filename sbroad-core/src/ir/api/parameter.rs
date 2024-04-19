use crate::errors::{Entity, SbroadError};
use crate::ir::block::Block;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::tree::traversal::PostOrder;
use crate::ir::value::Value;
use crate::ir::{Node, OptionParamValue, Plan};
use crate::otm::child_span;
use sbroad_proc::otm_child_span;

use ahash::RandomState;
use smol_str::format_smolstr;
use std::collections::{HashMap, HashSet};

impl Plan {
    pub fn add_param(&mut self) -> usize {
        self.nodes.push(Node::Parameter)
    }

    // Gather all parameter nodes from the tree to a hash set.
    #[must_use]
    pub fn get_param_set(&self) -> HashSet<usize> {
        let param_set: HashSet<usize> = self
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

    /// Substitute parameters to the plan.
    /// The purpose of this function is to find every `Parameter` node and replace it
    /// with `Expression::Constant` (under the row).
    ///
    /// # Errors
    /// - Invalid amount of parameters.
    /// - Internal errors.
    #[allow(clippy::too_many_lines)]
    #[otm_child_span("plan.bind")]
    pub fn bind_params(&mut self, mut values: Vec<Value>) -> Result<(), SbroadError> {
        // Nothing to do here.
        if values.is_empty() {
            return Ok(());
        }

        let capacity = self.next_id();
        let mut tree = PostOrder::with_capacity(|node| self.subtree_iter(node, false), capacity);
        let top_id = self.get_top()?;
        tree.populate_nodes(top_id);
        let nodes = tree.take_nodes();

        let mut binded_params_counter = 0;
        if !self.raw_options.is_empty() {
            binded_params_counter = self.bind_option_params(&mut values)?;
        }

        // Gather all parameter nodes from the tree to a hash set.
        // `param_node_ids` is used during first plan traversal (`row_ids` populating).
        // `param_node_ids_cloned` is used during second plan traversal (nodes transformation).
        let mut param_node_ids = self.get_param_set();
        let mut param_node_ids_cloned = param_node_ids.clone();

        let tnt_params_style = self.pg_params_map.is_empty();

        let mut pg_params_map = std::mem::take(&mut self.pg_params_map);

        if !tnt_params_style {
            // Due to how we calculate hash for plan subtree and the
            // fact that pg parameters can refer to same value multiple
            // times we currently copy params that are referred more
            // than once in order to get the same hash.
            // See https://git.picodata.io/picodata/picodata/sbroad/-/issues/583
            let mut used_values = vec![false; values.len()];
            let invalid_idx = |param_id: usize, value_idx: usize| {
                SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "out of bounds value index {value_idx} for pg parameter {param_id}"
                    )),
                )
            };

            // NB: we can't use `param_node_ids`, we need to traverse
            // parameters in the same order they will be bound,
            // otherwise we may get different hashes for plans
            // with tnt and pg parameters. See `subtree_hash*` tests,
            for (_, param_id) in &nodes {
                if !matches!(self.get_node(*param_id)?, Node::Parameter) {
                    continue;
                }
                let value_idx = *pg_params_map.get(param_id).ok_or(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "value index not found for parameter with id: {param_id}",
                    )),
                ))?;
                if used_values.get(value_idx).copied().unwrap_or(true) {
                    let Some(value) = values.get(value_idx) else {
                        return Err(invalid_idx(*param_id, value_idx));
                    };
                    values.push(value.clone());
                    pg_params_map
                        .entry(*param_id)
                        .and_modify(|value_idx| *value_idx = values.len() - 1);
                } else if let Some(used) = used_values.get_mut(value_idx) {
                    *used = true;
                } else {
                    return Err(invalid_idx(*param_id, value_idx));
                }
            }
        }

        // Transform parameters to values (plan constants). The result values are stored in the
        // opposite to parameters order.
        let mut value_ids: Vec<usize> = Vec::with_capacity(values.len());
        while let Some(param) = values.pop() {
            value_ids.push(self.add_const(param));
        }

        // We need to use rows instead of values in some cases (AST can solve
        // this problem for non-parameterized queries, but for parameterized
        // queries it is IR responsibility).
        let mut row_ids: HashMap<usize, usize, RandomState> =
            HashMap::with_hasher(RandomState::new());

        let non_binded_params_len = param_node_ids.len() - binded_params_counter;
        if tnt_params_style && non_binded_params_len > value_ids.len() {
            return Err(SbroadError::Invalid(
                Entity::Value,
                Some(format_smolstr!(
                    "Expected at least {} values for parameters. Got {}.",
                    non_binded_params_len,
                    value_ids.len()
                )),
            ));
        }

        // Populate rows.
        // Number of parameters - `idx` - 1 = index in params we are currently binding.
        // Initially pointing to nowhere.
        let mut idx = value_ids.len();

        let get_value = |param_id: usize, idx: usize| -> Result<usize, SbroadError> {
            let value_idx = if tnt_params_style {
                // in case non-pg params are used,
                // idx is the correct position
                idx
            } else {
                value_ids.len()
                    - 1
                    - *pg_params_map.get(&param_id).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Plan,
                            Some(format_smolstr!(
                                "value index not found for parameter with id: {param_id}",
                            )),
                        )
                    })?
            };
            let val_id = value_ids.get(value_idx).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    format_smolstr!("(Parameter) in position {value_idx}"),
                )
            })?;
            Ok(*val_id)
        };

        for (_, id) in &nodes {
            let node = self.get_node(*id)?;
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
                        if param_node_ids.take(param_id).is_some() {
                            idx = idx.saturating_sub(1);
                            let val_id = get_value(*param_id, idx)?;
                            row_ids.insert(*param_id, self.nodes.add_row(vec![val_id], None));
                        }
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
                        if param_node_ids.take(param_id).is_some() {
                            idx = idx.saturating_sub(1);
                        }
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
                            if param_node_ids.take(param_id).is_some() {
                                idx = idx.saturating_sub(1);
                                let val_id = get_value(*param_id, idx)?;
                                row_ids.insert(*param_id, self.nodes.add_row(vec![val_id], None));
                            }
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
                            if param_node_ids.take(&param_id).is_some() {
                                idx = idx.saturating_sub(1);
                                let val_id = get_value(param_id, idx)?;
                                row_ids.insert(param_id, self.nodes.add_row(vec![val_id], None));
                            }
                        }
                    }
                    Expression::Row { ref list, .. }
                    | Expression::StableFunction {
                        children: ref list, ..
                    } => {
                        for param_id in list {
                            if param_node_ids.take(param_id).is_some() {
                                // Parameter is already under row/function so that we don't
                                // have to cover it with `add_row` call.
                                idx = idx.saturating_sub(1);
                            }
                        }
                    }
                    Expression::Reference { .. }
                    | Expression::Constant { .. }
                    | Expression::CountAsterisk => {}
                },
                Node::Block(block) => match block {
                    Block::Procedure { ref values, .. } => {
                        for param_id in values {
                            if param_node_ids.take(param_id).is_some() {
                                // We don't need to wrap arguments, passed into the
                                // procedure call, into the rows.
                                idx = idx.saturating_sub(1);
                            }
                        }
                    }
                },
                Node::Parameter | Node::Ddl(..) | Node::Acl(..) => {}
            }
        }

        // Closure to retrieve a corresponding row for a parameter node.
        let get_row = |param_id: usize| -> Result<usize, SbroadError> {
            let row_id = row_ids.get(&param_id).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    format_smolstr!("(Row) at position {param_id}"),
                )
            })?;
            Ok(*row_id)
        };

        // Replace parameters in the plan.
        idx = value_ids.len();
        for (_, id) in &nodes {
            // Before binding, references that referred to
            // parameters had scalar type (by default),
            // but in fact they may refer to different stuff.
            {
                let mut new_type = None;
                if let Node::Expression(expr) = self.get_node(*id)? {
                    if let Expression::Reference { .. } = expr {
                        new_type = Some(expr.recalculate_type(self)?);
                    }
                }
                if let Some(new_type) = new_type {
                    let expr = self.get_mut_expression_node(*id)?;
                    expr.set_ref_type(new_type);
                    continue;
                }
            }

            let node = self.get_mut_node(*id)?;
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
                        if param_node_ids_cloned.take(param_id).is_some() {
                            idx = idx.saturating_sub(1);
                            let row_id = get_row(*param_id)?;
                            *param_id = row_id;
                        }
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
                        if param_node_ids_cloned.take(param_id).is_some() {
                            idx = idx.saturating_sub(1);
                            let val_id = get_value(*param_id, idx)?;
                            *param_id = val_id;
                        }
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
                        for param_id in &mut [left, right].iter_mut() {
                            if param_node_ids_cloned.take(param_id).is_some() {
                                idx = idx.saturating_sub(1);
                                let row_id = get_row(**param_id)?;
                                **param_id = row_id;
                            }
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
                            if param_node_ids_cloned.take(param_id).is_some() {
                                idx = idx.saturating_sub(1);
                                let row_id = get_row(*param_id)?;
                                *param_id = row_id;
                            }
                        }
                    }
                    Expression::Row { ref mut list, .. }
                    | Expression::StableFunction {
                        children: ref mut list,
                        ..
                    } => {
                        for param_id in list {
                            if param_node_ids_cloned.take(param_id).is_some() {
                                idx = idx.saturating_sub(1);
                                let val_id = get_value(*param_id, idx)?;
                                *param_id = val_id;
                            }
                        }
                    }
                    Expression::Reference { .. }
                    | Expression::Constant { .. }
                    | Expression::CountAsterisk => {}
                },
                Node::Block(block) => match block {
                    Block::Procedure { ref mut values, .. } => {
                        for param_id in values {
                            if param_node_ids_cloned.take(param_id).is_some() {
                                idx = idx.saturating_sub(1);
                                let val_id = get_value(*param_id, idx)?;
                                *param_id = val_id;
                            }
                        }
                    }
                },
                Node::Parameter | Node::Ddl(..) | Node::Acl(..) => {}
            }
        }

        // Update values row output.
        for (_, id) in nodes {
            if let Ok(Node::Relational(Relational::ValuesRow { .. })) = self.get_node(id) {
                self.update_values_row(id)?;
            }
        }

        Ok(())
    }

    /// Bind params related to `Option` clause.
    /// Returns the number of params binded to options.
    ///
    /// # Errors
    /// - User didn't provide parameter value for corresponding option parameter
    pub fn bind_option_params(&mut self, values: &mut Vec<Value>) -> Result<usize, SbroadError> {
        // Bind parameters in options to values.
        // Because the Option clause is the last clause in the
        // query the parameters are located in the end of params list.
        let mut binded_params_counter = 0usize;
        for opt in self.raw_options.iter_mut().rev() {
            if let OptionParamValue::Parameter { plan_id: param_id } = opt.val {
                if !self.pg_params_map.is_empty() {
                    // PG-like params syntax
                    let value_idx = *self.pg_params_map.get(&param_id).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Plan,
                            Some(format_smolstr!(
                                "no value idx in map for option parameter: {opt:?}"
                            )),
                        )
                    })?;
                    let value = values.get(value_idx).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Plan,
                            Some(format_smolstr!(
                                "invalid value idx {value_idx}, for option: {opt:?}"
                            )),
                        )
                    })?;
                    opt.val = OptionParamValue::Value { val: value.clone() };
                } else if let Some(v) = values.pop() {
                    binded_params_counter += 1;
                    opt.val = OptionParamValue::Value { val: v };
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format_smolstr!(
                            "no parameter value specified for option: {}",
                            opt.kind
                        )),
                    ));
                }
            }
        }
        Ok(binded_params_counter)
    }
}
