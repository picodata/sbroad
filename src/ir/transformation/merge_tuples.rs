use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::Bool;
use crate::ir::Plan;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use traversal::Bft;

fn call_expr_tree_merge_tuples(plan: &mut Plan, top_id: usize) -> Result<usize, QueryPlannerError> {
    plan.expr_tree_merge_tuples(top_id)
}

#[derive(Debug)]
struct Chain {
    // Left and right sides of the boolean expression
    // grouped by the operator.
    grouped: HashMap<Bool, (Vec<usize>, Vec<usize>)>,
    // Non-boolean expressions in the AND chain (true, false, null).
    other: Vec<usize>,
}

impl Chain {
    fn new() -> Self {
        Self {
            grouped: HashMap::new(),
            other: Vec::new(),
        }
    }

    fn insert(&mut self, plan: &mut Plan, expr_id: usize) -> Result<(), QueryPlannerError> {
        let bool_expr = plan.get_expression_node(expr_id)?;
        if let Expression::Bool { left, op, right } = bool_expr {
            let (left_id, right_id, group_op) = match *op {
                Bool::And | Bool::Or => {
                    // We can only merge tuples in non AND/OR expressions.
                    return Err(QueryPlannerError::CustomError(format!(
                        "AND/OR expressions are not supported: {:?}",
                        bool_expr
                    )));
                }
                Bool::Eq | Bool::NotEq => {
                    // Try to put expressions with references to the left side.
                    match (plan.is_ref(*left)?, plan.is_ref(*right)?) {
                        (false, true) => (*right, *left, op.clone()),
                        _ => (*left, *right, op.clone()),
                    }
                }
                Bool::Gt | Bool::GtEq | Bool::In => (*left, *right, op.clone()),
                // Invert operator to unite expressions with Gt and GtEq.
                Bool::Lt => (*right, *left, Bool::Gt),
                Bool::LtEq => (*right, *left, Bool::GtEq),
            };
            match self.grouped.entry(group_op) {
                Entry::Occupied(mut entry) => {
                    let (left, right) = entry.get_mut();
                    let new_left_id = plan.expr_clone(left_id)?;
                    let new_right_id = plan.expr_clone(right_id)?;
                    plan.get_columns_or_self(new_left_id)?
                        .iter()
                        .for_each(|id| {
                            left.push(*id);
                        });
                    plan.get_columns_or_self(new_right_id)?
                        .iter()
                        .for_each(|id| {
                            right.push(*id);
                        });
                }
                Entry::Vacant(entry) => {
                    let new_left_id = plan.expr_clone(left_id)?;
                    let new_right_id = plan.expr_clone(right_id)?;
                    entry.insert((
                        plan.get_columns_or_self(new_left_id)?,
                        plan.get_columns_or_self(new_right_id)?,
                    ));
                }
            }
        } else {
            let new_expr_id = plan.expr_clone(expr_id)?;
            self.other.push(new_expr_id);
        }
        Ok(())
    }

    fn as_plan(&self, plan: &mut Plan) -> Result<usize, QueryPlannerError> {
        let other_top_id = match self.other.split_first() {
            Some((first, other)) => {
                let mut top_id = *first;
                for id in other {
                    top_id = plan.add_cond(*id, Bool::And, top_id)?;
                }
                Some(top_id)
            }
            None => None,
        };

        // Chain is grouped by the operators in the hash map.
        // To make serialization non-flaky, we extract operators
        // in a deterministic order.
        let mut grouped_top_id: Option<usize> = None;
        // No need for "And" and "Or" operators.
        let ordered_ops = &[
            Bool::Eq,
            Bool::Gt,
            Bool::GtEq,
            Bool::In,
            Bool::Lt,
            Bool::LtEq,
            Bool::NotEq,
        ];
        for op in ordered_ops {
            if let Some((left, right)) = self.grouped.get(op) {
                let left_row_id = plan.nodes.add_row(left.clone(), None);
                let right_row_id = plan.nodes.add_row(right.clone(), None);
                let cond_id = plan.add_cond(left_row_id, op.clone(), right_row_id)?;
                match grouped_top_id {
                    None => {
                        grouped_top_id = Some(cond_id);
                    }
                    Some(top_id) => {
                        grouped_top_id = Some(plan.add_cond(top_id, Bool::And, cond_id)?);
                    }
                }
            }
        }
        match (grouped_top_id, other_top_id) {
            (Some(grouped_top_id), Some(other_top_id)) => {
                Ok(plan.add_cond(grouped_top_id, Bool::And, other_top_id)?)
            }
            (Some(grouped_top_id), None) => Ok(grouped_top_id),
            (None, Some(other_top_id)) => Ok(other_top_id),
            (None, None) => Err(QueryPlannerError::CustomError(
                "No expressions to merge".to_string(),
            )),
        }
    }

    fn is_empty(&self) -> bool {
        self.grouped.is_empty() && self.other.is_empty()
    }
}

impl Plan {
    fn get_columns_or_self(&self, expr_id: usize) -> Result<Vec<usize>, QueryPlannerError> {
        let expr = self.get_expression_node(expr_id)?;
        match expr {
            Expression::Row { list, .. } => Ok(list.clone()),
            _ => Ok(vec![expr_id]),
        }
    }

    fn populate_and_chains(
        &mut self,
        nodes: &[usize],
    ) -> Result<HashMap<usize, Chain>, QueryPlannerError> {
        let mut visited: HashSet<usize> = HashSet::new();
        let mut chains: HashMap<usize, Chain> = HashMap::new();

        for id in nodes {
            if visited.contains(id) {
                continue;
            }
            visited.insert(*id);

            let tree_and = Bft::new(id, |node| self.nodes.and_iter(node));
            let mut nodes_and: Vec<usize> = Vec::new();
            for (_, and_id) in tree_and {
                nodes_and.push(*and_id);
            }
            let mut chain = Chain::new();
            let mut nodes_for_chain: Vec<usize> = Vec::new();
            for and_id in nodes_and {
                let expr = self.get_expression_node(and_id)?;
                if let Expression::Bool {
                    left,
                    op: Bool::And,
                    right,
                    ..
                } = expr
                {
                    let children = vec![*left, *right];
                    for child_id in children {
                        visited.insert(child_id);
                        let child_expr = self.get_expression_node(child_id)?;
                        if let Expression::Bool {
                            op: Bool::And | Bool::Or,
                            ..
                        } = child_expr
                        {
                            continue;
                        }
                        nodes_for_chain.push(child_id);
                    }
                }
            }
            for node_id in nodes_for_chain {
                chain.insert(self, node_id)?;
            }

            if !chain.is_empty() {
                chains.insert(*id, chain);
            }
        }
        Ok(chains)
    }

    fn expr_tree_merge_tuples(&mut self, expr_id: usize) -> Result<usize, QueryPlannerError> {
        let mut nodes: Vec<usize> = Vec::new();
        let tree = Bft::new(&expr_id, |node| self.nodes.expr_iter(node, false));
        for (_, id) in tree {
            nodes.push(*id);
        }
        let chains = self.populate_and_chains(&nodes)?;

        // Replace nodes' children with the merged tuples.
        for id in nodes {
            let expr = self.get_expression_node(id)?;
            match expr {
                Expression::Alias { child, .. } => {
                    let chain = chains.get(child);
                    if let Some(chain) = chain {
                        let new_child_id = chain.as_plan(self)?;
                        let expr_mut = self.get_mut_expression_node(id)?;
                        if let Expression::Alias {
                            child: ref mut child_id,
                            ..
                        } = expr_mut
                        {
                            *child_id = new_child_id;
                        } else {
                            return Err(QueryPlannerError::CustomError(format!(
                                "Expected alias expression: {:?}",
                                expr_mut
                            )));
                        }
                    }
                }
                Expression::Bool { left, right, .. } => {
                    let children = vec![*left, *right];
                    for (pos, child) in children.iter().enumerate() {
                        let chain = chains.get(child);
                        if let Some(chain) = chain {
                            let new_child_id = chain.as_plan(self)?;
                            let expr_mut = self.get_mut_expression_node(id)?;
                            if let Expression::Bool {
                                left: ref mut left_id,
                                right: ref mut right_id,
                                ..
                            } = expr_mut
                            {
                                if pos == 0 {
                                    *left_id = new_child_id;
                                } else {
                                    *right_id = new_child_id;
                                }
                            } else {
                                return Err(QueryPlannerError::CustomError(format!(
                                    "Expected boolean expression: {:?}",
                                    expr_mut
                                )));
                            }
                        }
                    }
                }
                Expression::Row { list, .. } => {
                    let children = list.clone();
                    for (pos, child) in children.iter().enumerate() {
                        let chain = chains.get(child);
                        if let Some(chain) = chain {
                            let new_child_id = chain.as_plan(self)?;
                            let expr_mut = self.get_mut_expression_node(id)?;
                            if let Expression::Row { ref mut list, .. } = expr_mut {
                                if let Some(child_id) = list.get_mut(pos) {
                                    *child_id = new_child_id;
                                } else {
                                    return Err(QueryPlannerError::CustomError(format!(
                                        "Expected a column at position {} in the row {:?}",
                                        pos, expr_mut
                                    )));
                                }
                            } else {
                                return Err(QueryPlannerError::CustomError(format!(
                                    "Expected row expression: {:?}",
                                    expr_mut
                                )));
                            }
                        }
                    }
                }
                _ => continue,
            }
        }

        // Try to replace the subtree top node (if it is also AND).
        if let Some(top_chain) = chains.get(&expr_id) {
            let new_expr_id = top_chain.as_plan(self)?;
            return Ok(new_expr_id);
        }

        Ok(expr_id)
    }

    /// Group boolean operators in the AND-ed chain by operator type and merge
    /// them into a single boolean operator.
    ///
    /// # Errors
    /// - If the plan tree is invalid (doesn't contain correct nodes where we expect it to).
    pub fn merge_tuples(&mut self) -> Result<(), QueryPlannerError> {
        self.transform_expr_trees(&call_expr_tree_merge_tuples)
    }
}

#[cfg(test)]
mod tests;
