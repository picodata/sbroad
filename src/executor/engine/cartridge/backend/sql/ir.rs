use itertools::Itertools;

use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::executor::vtable::VTableTuple;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::Node;

use super::tree::{SyntaxData, SyntaxPlan};

impl ExecutionPlan {
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

    /// Transform plan sub-tree (pointed by top) to sql string
    ///
    /// # Errors
    /// - plan is invalid and can't be transformed
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    pub fn syntax_nodes_as_sql(
        &self,
        nodes: &[SyntaxData],
        buckets: &Buckets,
    ) -> Result<String, QueryPlannerError> {
        let mut sql = String::new();
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

        let ir_plan = self.get_ir_plan();

        // The starting value of the counter for generating anonymous column names.
        // It emulates Tarantool parser (generated by lemon parser generator).
        // It traverses the parse tree bottom-up from left to right and increments
        // the global counter with the columns that need an auto-generated name.
        // As a result each such column would be named like "COLUMN_%d", where "%d"
        // is the new global counter value.
        let mut anonymous_col_idx_base = 0;
        for (id, data) in nodes.iter().enumerate() {
            if let Some(' ' | '(') = sql.chars().last() {
            } else if need_delim_after(id) {
                sql.push_str(delim);
            }

            match data {
                // TODO: should we care about plans without projections?
                // Or they should be treated as invalid?
                SyntaxData::Alias(s) => {
                    sql.push_str("as ");
                    sql.push_str(s);
                }
                SyntaxData::CloseParenthesis => sql.push(')'),
                SyntaxData::Comma => sql.push(','),
                SyntaxData::Condition => sql.push_str("ON"),
                SyntaxData::From => sql.push_str("FROM"),
                SyntaxData::Operator(s) => sql.push_str(s.as_str()),
                SyntaxData::OpenParenthesis => sql.push('('),
                SyntaxData::PlanId(id) => {
                    let node = ir_plan.get_node(*id)?;
                    match node {
                        Node::Parameter => {
                            return Err(QueryPlannerError::CustomError(
                                "Parameters are not supported in the generated SQL".into(),
                            ));
                        }
                        Node::Relational(rel) => match rel {
                            Relational::Insert { relation, .. } => {
                                sql.push_str("INSERT INTO ");
                                sql.push_str(relation.as_str());
                            }
                            Relational::InnerJoin { .. } => sql.push_str("INNER JOIN"),
                            Relational::Projection { .. } => sql.push_str("SELECT"),
                            Relational::ScanRelation { relation, .. } => {
                                sql.push_str(relation);
                            }
                            Relational::ScanSubQuery { .. }
                            | Relational::Motion { .. }
                            | Relational::ValuesRow { .. } => {}
                            Relational::Selection { .. } => sql.push_str("WHERE"),
                            Relational::UnionAll { .. } => sql.push_str("UNION ALL"),
                            Relational::Values { .. } => sql.push_str("VALUES"),
                        },
                        Node::Expression(expr) => match expr {
                            Expression::Alias { .. }
                            | Expression::Bool { .. }
                            | Expression::Row { .. } => {}
                            Expression::Constant { value, .. } => {
                                sql.push_str(&format!("{}", value));
                            }
                            Expression::Reference { position, .. } => {
                                let rel_id: usize = ir_plan
                                    .get_relational_from_reference_node(*id)?
                                    .into_iter()
                                    .next()
                                    .ok_or_else(|| {
                                        QueryPlannerError::CustomError(
                                            "Reference points to a non-relational node.".into(),
                                        )
                                    })?;
                                let rel_node = ir_plan.get_relation_node(rel_id)?;
                                let alias = &ir_plan.get_alias_from_reference_node(expr)?;

                                if rel_node.is_insert() {
                                    // We expect `INSERT INTO t(a, b) VALUES(1, 2)`
                                    // rather then `INSERT INTO t(t.a, t.b) VALUES(1, 2)`.
                                    sql.push_str(alias);
                                } else if let Some(name) = rel_node.scan_name(ir_plan, *position)? {
                                    sql.push_str(name);
                                    sql.push('.');
                                    sql.push_str(alias);
                                } else {
                                    sql.push_str(alias);
                                }
                            }
                        },
                    }
                }
                SyntaxData::VTable(vtable) => {
                    let cols_count = vtable.get_columns().len();

                    let cols = |base_idx| {
                        vtable
                            .get_columns()
                            .iter()
                            .enumerate()
                            .map(|(i, c)| format!("COLUMN_{} as \"{}\"", base_idx + i, c.name))
                            .collect::<Vec<String>>()
                            .join(",")
                    };

                    let tuples: Vec<&VTableTuple> = match buckets {
                        Buckets::All => vtable.get_tuples().iter().collect(),
                        Buckets::Filtered(bucket_ids) => {
                            if vtable.get_index().is_empty() {
                                // TODO: Implement selection push-down (join_linker3_test).
                                vtable.get_tuples().iter().collect()
                            } else {
                                bucket_ids
                                    .iter()
                                    .filter_map(|bucket_id| vtable.get_index().get(bucket_id))
                                    .flatten()
                                    .filter_map(|pos| vtable.get_tuples().get(*pos))
                                    .collect()
                            }
                        }
                    };

                    if tuples.is_empty() {
                        anonymous_col_idx_base += 1;

                        let values = (0..cols_count)
                            .map(|_| "null")
                            .collect::<Vec<&str>>()
                            .join(",");

                        sql.push_str(&format!(
                            "SELECT {} FROM (VALUES ({})) WHERE FALSE",
                            cols(anonymous_col_idx_base),
                            values
                        ));
                    } else {
                        let values = tuples
                            .iter()
                            .map(|t| format!("({})", (t.iter().map(ToString::to_string)).join(",")))
                            .collect::<Vec<String>>()
                            .join(",");

                        anonymous_col_idx_base += cols_count * tuples.len() - (cols_count - 1);

                        sql.push_str(&format!(
                            "SELECT {} FROM (VALUES {})",
                            cols(anonymous_col_idx_base),
                            values
                        ));
                    }
                }
            }
        }
        Ok(sql)
    }

    /// Checks if the given query subtree modifies data or not.
    ///
    /// # Errors
    /// - If the subtree top is not a relational node.
    pub fn subtree_modifies_data(&self, top_id: usize) -> Result<bool, QueryPlannerError> {
        // Tarantool doesn't support `INSERT`, `UPDATE` and `DELETE` statements
        // with `RETURNING` clause. That is why it is enough to check if the top
        // node is a data modification statement or not.
        let top = self.get_ir_plan().get_relation_node(top_id)?;
        Ok(top.is_insert())
    }
}

#[cfg(test)]
mod tests;
