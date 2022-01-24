use traversal::DftPost;

use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::{Node, Plan};

impl Plan {
    #[allow(dead_code)]
    fn subtree_as_sql(&self, node_id: usize) -> Result<String, QueryPlannerError> {
        let dft_post = DftPost::new(&node_id, |node| self.nodes.subtree_iter(node));
        let mut result = String::from("SELECT ");

        let mut stack = vec![];
        for (_, next_node_id) in dft_post {
            let current_node = self.get_node(*next_node_id)?;

            match current_node {
                Node::Expression(e) => match e {
                    Expression::Alias { name, .. } => stack.push(format!("\"{}\"", name)),
                    Expression::Bool { op, .. } => {
                        let r = stack.pop().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Invalid right node id for transform to sql".into(),
                            )
                        })?;
                        let l = stack.pop().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Invalid left node id for transform to sql".into(),
                            )
                        })?;

                        stack.push(format!("\"{}\" {} {}", l, op, r));
                    }
                    Expression::Constant { value } => {
                        stack.push(value.to_string());
                    }
                    Expression::Reference { .. } => {
                        stack.push(self.get_alias_from_reference_node(e)?);
                    }
                    Expression::Row { .. } => {}
                },
                Node::Relational(r) => match r {
                    Relational::InnerJoin { .. }
                    | Relational::Motion { .. }
                    | Relational::ScanSubQuery { .. }
                    | Relational::UnionAll { .. } => {
                        return Err(QueryPlannerError::QueryNotImplemented)
                    }
                    Relational::Projection { .. } => {
                        let selection = stack.pop().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Invalid selection node id for transform to sql".into(),
                            )
                        })?;

                        let mut cols = vec![];
                        while let Some(col) = stack.pop() {
                            cols.insert(0, col);
                        }

                        stack.push(format!("{} {}", cols.join(", "), selection));
                    }
                    Relational::ScanRelation { relation, .. } => {
                        stack.push(format!("\"{}\"", relation));
                    }
                    Relational::Selection { .. } => {
                        let cond = stack.pop().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Invalid condition id for transform to sql".into(),
                            )
                        })?;
                        let table = stack.pop().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Invalid table node id for transform to sql".into(),
                            )
                        })?;

                        stack.push(format!("FROM {} WHERE {}", table, cond));
                    }
                },
            }
        }

        let q = stack.pop().ok_or_else(|| {
            QueryPlannerError::CustomError("Invalid top node id for transform to sql".into())
        })?;
        result.push_str(&q);

        Ok(result)
    }
}

#[cfg(test)]
mod tests;
