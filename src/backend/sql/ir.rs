use super::tree::{SyntaxData, SyntaxPlan};
use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::{Node, Plan};

impl Plan {
    /// Traverse plan sub-tree (pointed by top) in the order
    /// convenient for SQL serialization.
    ///
    /// # Panics
    /// - the amount of nodes exceeds `isize::MAX / usize` bytes
    ///
    /// # Errors
    /// - top node is invalid
    /// - plan is invalid
    #[allow(dead_code)]
    pub fn get_sql_order(&self, top: usize) -> Result<Vec<SyntaxData>, QueryPlannerError> {
        let mut sp = SyntaxPlan::new(self, top)?;
        // Result with plan node ids.
        let mut result: Vec<SyntaxData> = Vec::with_capacity(sp.nodes.arena.len());
        // Stack to keep syntax node data.
        let mut stack: Vec<usize> = Vec::with_capacity(sp.nodes.arena.len());

        // Make a destructive in-order traversal over the syntax plan
        // nodes (left and right pointers for any wrapped node become
        // None or removed). It seems to be the fastest traversal
        // approach in Rust (`take()` and `pop()`).
        stack.push(sp.get_top()?);
        while let Some(id) = stack.last() {
            let sn = sp.nodes.get_mut_syntax_node(*id)?;
            if let Some(left_id) = sn.left.take() {
                stack.push(left_id);
            } else if let Some(id) = stack.pop() {
                let sn_next = sp.nodes.get_mut_syntax_node(id)?;
                result.push(sn_next.data.clone());
                while let Some(right_id) = sn_next.right.pop() {
                    stack.push(right_id);
                }
            }
        }

        Ok(result)
    }

    /// Check that node is an additional child of some relational operator.
    ///
    /// # Errors
    /// - node is invalid
    pub fn is_additional_child(&self, node_id: usize) -> Result<bool, QueryPlannerError> {
        for id in 0..self.nodes.next_id() {
            let node = self.get_node(id)?;
            match node {
                Node::Relational(rel) => match rel {
                    Relational::Projection { children, .. }
                    | Relational::Selection { children, .. } => {
                        if children.first() == Some(&node_id) {
                            return Ok(false);
                        }
                    }
                    Relational::InnerJoin { children, .. }
                    | Relational::UnionAll { children, .. } => {
                        if children.first() == Some(&node_id) || children.get(1) == Some(&node_id) {
                            return Ok(false);
                        }
                    }
                    _ => continue,
                },
                Node::Expression(_) => continue,
            }
        }
        Ok(true)
    }

    /// Transform plan sub-tree (pointed by top) to sql string
    ///
    /// # Errors
    /// - plan is invalid and can't be transformed
    #[allow(dead_code)]
    pub fn subtree_as_sql(&self, node_id: usize) -> Result<String, QueryPlannerError> {
        let mut sql = String::new();
        let nodes = self.get_sql_order(node_id)?;
        let delim = " ";

        let need_delim_after = |id: usize| -> bool {
            let mut result: bool = true;
            if id > 0 {
                if let Some(SyntaxData::OpenParenthesis) = nodes.get(id - 1) {
                    result = false;
                }
            }
            if let Some(SyntaxData::Comma | SyntaxData::CloseParenthesis) = nodes.get(id) {
                result = false;
            }
            if id == 0 {
                result = false;
            }
            result
        };

        for (id, data) in nodes.iter().enumerate() {
            if let Some(' ' | '(') = sql.chars().last() {
            } else if need_delim_after(id) {
                sql.push_str(delim);
            }
            match data {
                // TODO: should we care about plans without projections?
                // Or they should be treated as invalid?
                SyntaxData::Alias(s) => sql.push_str(&format!("as \"{}\"", s.as_str())),
                SyntaxData::CloseParenthesis => sql.push(')'),
                SyntaxData::Comma => sql.push(','),
                SyntaxData::Condition => sql.push_str("on"),
                SyntaxData::From => sql.push_str("FROM"),
                SyntaxData::Operator(s) => sql.push_str(s.as_str()),
                SyntaxData::OpenParenthesis => sql.push('('),
                SyntaxData::PlanId(id) => match self.get_node(*id)? {
                    Node::Relational(rel) => match rel {
                        Relational::InnerJoin { .. } => sql.push_str("INNER JOIN"),
                        Relational::Motion { .. } => {
                            return Err(QueryPlannerError::CustomError(
                                "Motion nodes can't be converted to SQL.".into(),
                            ))
                        }
                        Relational::Projection { .. } => sql.push_str("SELECT"),
                        Relational::ScanRelation { relation, .. } => {
                            sql.push_str(&format!("\"{}\"", relation));
                        }
                        Relational::ScanSubQuery { .. } => {}
                        Relational::Selection { .. } => sql.push_str("WHERE"),
                        Relational::UnionAll { .. } => sql.push_str("UNION ALL"),
                    },
                    Node::Expression(expr) => match expr {
                        Expression::Alias { .. }
                        | Expression::Bool { .. }
                        | Expression::Row { .. } => {}
                        Expression::Constant { value, .. } => sql.push_str(&format!("{}", value)),
                        Expression::Reference { .. } => {
                            sql.push_str(&format!(
                                "\"{}\"",
                                &self.get_alias_from_reference_node(expr)?
                            ));
                        }
                    },
                },
            }
        }

        Ok(sql)
    }
}

#[cfg(test)]
mod tests;
