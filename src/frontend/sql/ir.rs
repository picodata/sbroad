use traversal::DftPost;

use crate::cache::Metadata;
use crate::errors::QueryPlannerError;
use crate::frontend::sql::ast::{AbstractSyntaxTree, Node, Type};
use crate::ir::operator::Bool;
use crate::ir::value::Value;
use crate::ir::Plan;

impl Bool {
    /// Create `Bool` from ast node type.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the operator is invalid.
    #[allow(dead_code)]
    fn from_node_type(s: &Type) -> Result<Self, QueryPlannerError> {
        match s {
            Type::And => Ok(Bool::And),
            Type::Or => Ok(Bool::Or),
            Type::Eq => Ok(Bool::Eq),
            Type::In => Ok(Bool::In),
            Type::Gt => Ok(Bool::Gt),
            Type::GtEq => Ok(Bool::GtEq),
            Type::Lt => Ok(Bool::Lt),
            Type::LtEq => Ok(Bool::LtEq),
            Type::NotEq => Ok(Bool::NotEq),
            _ => Err(QueryPlannerError::InvalidBool),
        }
    }
}

impl Value {
    /// Create `Value` from ast node type and text.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the operator is invalid.
    #[allow(dead_code)]
    fn from_node(s: &Node) -> Result<Self, QueryPlannerError> {
        let val = match s.clone().value {
            Some(v) => v,
            None => "".into(),
        };

        match s.rule {
            Type::Number => Ok(Value::number_from_str(val.as_str())?),
            Type::String => Ok(Value::string_from_str(val.as_str())),
            Type::Null => Ok(Value::Null),
            _ => Err(QueryPlannerError::UnsupportedIrValueType),
        }
    }
}

/// Temporary stack for traversing ast
struct Stack {
    store: Vec<usize>,
}

impl Stack {
    /// Create empty stack instance
    fn new() -> Self {
        Stack { store: vec![] }
    }

    /// Function push element on the top of stack
    fn push(&mut self, node_id: usize) {
        self.store.push(node_id);
    }

    /// Extract a top stack element or return an error from the `err` parameter
    ///
    /// # Errors
    /// - failed to extract an element from the stack.
    fn pop_or_err(&mut self, err: QueryPlannerError) -> Result<usize, QueryPlannerError> {
        match self.store.pop() {
            Some(v) => Ok(v),
            None => Err(err),
        }
    }
}

impl AbstractSyntaxTree {
    /// Transform ast to ir plan tree.
    ///
    /// Creates a plan from AST (based on AST node type).
    /// Traverse the tree in Post-Order with [Depth-First Traversal](https://en.wikipedia.org/wiki/Tree_traversal).
    ///
    /// # Errors
    /// - IR plan can't be built.
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    pub fn to_ir(&self, metadata: &Metadata) -> Result<Plan, QueryPlannerError> {
        let mut plan = Plan::new();

        let top = match self.top {
            Some(t) => t,
            None => return Err(QueryPlannerError::InvalidAst),
        };
        let dft_pre = DftPost::new(&top, |node| self.nodes.tree_iter(node));

        let mut current_scan_id = 0;
        let mut current_logical_id = 0;
        let mut stack = Stack::new();
        let mut projection = vec![];
        let mut subquery_alias = "";

        for (_, node_id) in dft_pre {
            let node = self.nodes.get_node(*node_id)?;

            match node.clone().rule {
                Type::Table => {
                    // add scan node to plan

                    if let Some(node_val) = node.clone().value {
                        let table = node_val.as_str().trim_matches('\"');
                        let t = metadata.get_table_segment(table)?;
                        plan.add_rel(t);
                        stack.push(plan.add_scan(table)?);
                    }
                }
                Type::Name => {
                    if let Some(name) = &node.value {
                        let col = name.trim_matches('\"');
                        stack.push(plan.add_row_from_child(
                            current_logical_id,
                            current_scan_id,
                            &[col],
                        )?);
                    }
                }
                Type::Number | Type::String => {
                    let val = Value::from_node(node)?;
                    stack.push(plan.add_const(val));
                }
                Type::And
                | Type::Or
                | Type::Eq
                | Type::In
                | Type::Gt
                | Type::GtEq
                | Type::Lt
                | Type::LtEq
                | Type::NotEq => {
                    let op = Bool::from_node_type(&node.rule)?;

                    // Extract left and right nodes id from stack for creation condition plan node
                    let right_id = stack.pop_or_err(QueryPlannerError::CustomError(
                        "Incorrect right part of condition".to_string(),
                    ))?;

                    let left_id = stack.pop_or_err(QueryPlannerError::CustomError(
                        "Incorrect left part of condition".to_string(),
                    ))?;

                    stack.push(plan.add_cond(left_id, op, right_id)?);
                }
                Type::Selection => {
                    // extract from stack condition node id for creating selection plan node
                    let selection_id =
                        stack.pop_or_err(QueryPlannerError::InvalidAstConditionNode)?;

                    stack.push(plan.add_select(
                        &[current_scan_id],
                        selection_id,
                        current_logical_id,
                    )?);
                }
                Type::ProjectedName => {
                    // save projection column for append it later

                    if let Some(col) = &node.value {
                        projection.push(col.as_str().trim_matches('\"'));
                    }
                }
                Type::Projection => {
                    // extract from stack selection node id for creating projection plan node

                    let selection_id =
                        stack.pop_or_err(QueryPlannerError::InvalidAstSelectionNode)?;

                    stack.push(plan.add_proj(selection_id, &projection)?);

                    // clear projection columns for next tables
                    projection.clear();
                }
                Type::UnionAll => {
                    // extract from stack left and right union subtree node for creating union plan node

                    let right_id = stack.pop_or_err(QueryPlannerError::CustomError(
                        "Invalid right union part".to_string(),
                    ))?;

                    let left_id = stack.pop_or_err(QueryPlannerError::CustomError(
                        "Invalid left union part".to_string(),
                    ))?;

                    stack.push(plan.add_union_all(left_id, right_id)?);
                }
                Type::SubQueryName => {
                    // save alias name for using in the next step
                    if let Some(v) = &node.value {
                        subquery_alias = v.as_str().trim_matches('\"');
                    }
                }
                Type::SubQuery => {
                    // add subquery node for it stack must have scan (or union) node id

                    let sq_id = stack.pop_or_err(QueryPlannerError::InvalidAstSubQueryNode)?;

                    stack.push(plan.add_sub_query(sq_id, Some(subquery_alias))?);
                }
                Type::Asterisk => {
                    projection.clear();
                }
                Type::Scan => {
                    // extract current scan node id from stack and getting logical id for appending other node
                    current_scan_id = stack.pop_or_err(QueryPlannerError::InvalidAstScanNode)?;
                    current_logical_id = plan.next_id();
                }
                Type::Column | Type::Select => {}
                rule => {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Not implements type: {:?}",
                        rule
                    )));
                }
            }
        }

        // get root node id
        let top_id = stack.pop_or_err(QueryPlannerError::InvalidAstTopNode)?;
        plan.set_top(top_id)?;

        Ok(plan)
    }
}

#[cfg(test)]
mod tests;
