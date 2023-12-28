use crate::errors::{Entity, SbroadError};
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::tree::traversal::PostOrder;
use crate::ir::value::Value;
use crate::ir::{Node, Plan};
use crate::otm::child_span;
use sbroad_proc::otm_child_span;

use ahash::RandomState;
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
    pub fn bind_params(&mut self, mut params: Vec<Value>) -> Result<(), SbroadError> {
        // Nothing to do here.
        if params.is_empty() {
            return Ok(());
        }

        let capacity = self.next_id();
        let mut tree = PostOrder::with_capacity(|node| self.subtree_iter(node, false), capacity);
        let top_id = self.get_top()?;
        tree.populate_nodes(top_id);
        let nodes = tree.take_nodes();

        let mut binded_params_counter = 0;
        if !self.raw_options.is_empty() {
            binded_params_counter = self.bind_option_params(&mut params)?;
        }

        // Transform parameters to values (plan constants). The result values are stored in the
        // opposite to parameters order.
        let mut value_ids: Vec<usize> = Vec::with_capacity(params.len());
        while let Some(param) = params.pop() {
            value_ids.push(self.add_const(param));
        }

        // We need to use rows instead of values in some cases (AST can solve
        // this problem for non-parameterized queries, but for parameterized
        // queries it is IR responsibility).
        let mut row_ids: HashMap<usize, usize, RandomState> =
            HashMap::with_hasher(RandomState::new());

        // Gather all parameter nodes from the tree to a hash set.
        // `param_node_ids` is used during first plan traversal (`row_ids` populating).
        // `param_set_params` is used during second plan traversal (nodes transformation).
        let mut param_node_ids = self.get_param_set();
        let mut param_node_ids_cloned = param_node_ids.clone();

        if param_node_ids.len() - binded_params_counter > value_ids.len() {
            return Err(SbroadError::Invalid(
                Entity::Value,
                Some(format!(
                    "Expected at least {} values for parameters. Got {}.",
                    param_node_ids.len() - binded_params_counter,
                    value_ids.len()
                )),
            ));
        }

        // Closure to retrieve a corresponding value for a parameter node.
        let get_value = |pos: usize| -> Result<usize, SbroadError> {
            let val_id = value_ids.get(pos).ok_or_else(|| {
                SbroadError::NotFound(Entity::Node, format!("(Parameter) in position {pos}"))
            })?;
            Ok(*val_id)
        };

        // After binding parameters we need to recalculate expression types.
        let mut new_types = HashMap::with_capacity_and_hasher(nodes.len(), RandomState::new());

        // Populate rows.
        // Number of parameters - `idx` - 1 = index in params we are currently binding.
        // Initially pointing to nowhere.
        let mut idx = value_ids.len();
        for (_, id) in &nodes {
            let node = self.get_node(*id)?;
            match node {
                Node::Relational(rel) => match rel {
                    Relational::Selection {
                        filter: ref param_id,
                        ..
                    }
                    | Relational::Join {
                        condition: ref param_id,
                        ..
                    }
                    | Relational::Projection {
                        output: ref param_id,
                        ..
                    } => {
                        if param_node_ids.take(param_id).is_some() {
                            idx -= 1;
                            let val_id = get_value(idx)?;
                            row_ids.insert(idx, self.nodes.add_row(vec![val_id], None));
                        }
                    }
                    _ => {}
                },
                Node::Expression(expr) => match expr {
                    Expression::Alias {
                        child: ref param_id,
                        ..
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
                            idx -= 1;
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
                                idx -= 1;
                                let val_id = get_value(idx)?;
                                row_ids.insert(idx, self.nodes.add_row(vec![val_id], None));
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
                                idx -= 1;
                            }
                        }
                    }
                    Expression::Reference { .. } => {
                        println!("expr: {expr:?}, id: {id}");
                        // Remember to recalculate type.
                        new_types.insert(id, expr.recalculate_type(self)?);
                    }
                    Expression::Constant { .. } | Expression::CountAsterisk => {}
                },
                Node::Parameter | Node::Ddl(..) | Node::Acl(..) => {}
            }
        }

        // Closure to retrieve a corresponding row for a parameter node.
        let get_row = |idx: usize| -> Result<usize, SbroadError> {
            let row_id = row_ids.get(&idx).ok_or_else(|| {
                SbroadError::NotFound(Entity::Node, format!("(Row) at position {idx}"))
            })?;
            Ok(*row_id)
        };

        // Replace parameters in the plan.
        idx = value_ids.len();
        for (_, id) in &nodes {
            let node = self.get_mut_node(*id)?;
            match node {
                Node::Relational(rel) => match rel {
                    Relational::Selection {
                        filter: ref mut param_id,
                        ..
                    }
                    | Relational::Join {
                        condition: ref mut param_id,
                        ..
                    }
                    | Relational::Projection {
                        output: ref mut param_id,
                        ..
                    } => {
                        if param_node_ids_cloned.take(param_id).is_some() {
                            idx -= 1;
                            let row_id = get_row(idx)?;
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
                    | Expression::Cast {
                        child: ref mut param_id,
                        ..
                    }
                    | Expression::Unary {
                        child: ref mut param_id,
                        ..
                    } => {
                        if param_node_ids_cloned.take(param_id).is_some() {
                            idx -= 1;
                            let val_id = get_value(idx)?;
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
                                idx -= 1;
                                let row_id = get_row(idx)?;
                                **param_id = row_id;
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
                                idx -= 1;
                                let val_id = get_value(idx)?;
                                *param_id = val_id;
                            }
                        }
                    }
                    Expression::Reference { .. } => {
                        // Update type.
                        let new_type = new_types
                            .get(id)
                            .ok_or_else(|| {
                                SbroadError::NotFound(
                                    Entity::Node,
                                    format!("failed to get type for node {id}"),
                                )
                            })?
                            .clone();
                        expr.set_ref_type(new_type);
                    }
                    Expression::Constant { .. } | Expression::CountAsterisk => {}
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
    pub fn bind_option_params(&mut self, params: &mut Vec<Value>) -> Result<usize, SbroadError> {
        // Bind parameters in options to values.
        // Because the Option clause is the last clause in the
        // query the parameters are located in the end of params list.
        let mut binded_params_counter = 0usize;
        for opt in self.raw_options.iter_mut().rev() {
            if opt.val.is_none() {
                if let Some(v) = params.pop() {
                    binded_params_counter += 1;
                    opt.val = Some(v);
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some(format!(
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
