use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt::Write as _;
use tarantool::tlua;
use tarantool::tuple::{FunctionArgs, Tuple};

use crate::debug;
use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::executor::vtable::VTableTuple;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::value::Value;
use crate::ir::Node;
use crate::otm::{child_span, current_id, force_trace, inject_context};

use super::tree::SyntaxData;

#[derive(Debug, Eq, Serialize, tlua::Push)]
pub struct PatternWithParams {
    pub pattern: String,
    pub params: Vec<Value>,
    pub context: Option<HashMap<String, String>>,
    pub id: Option<String>,
    pub force_trace: bool,
}

impl PartialEq for PatternWithParams {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.params == other.params
    }
}

impl TryFrom<FunctionArgs> for PatternWithParams {
    type Error = QueryPlannerError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        debug!(
            Option::from("argument parsing"),
            &format!("Query parameters: {:?}", value),
        );
        Tuple::from(value)
            .decode::<PatternWithParams>()
            .map_err(|e| {
                QueryPlannerError::CustomError(format!(
                    "Parsing error (pattern with parameters): {:?}",
                    e
                ))
            })
    }
}

impl<'de> Deserialize<'de> for PatternWithParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "PatternWithParams")]
        struct StructHelper(
            String,
            Vec<Value>,
            Option<HashMap<String, String>>,
            Option<String>,
            bool,
        );

        let struct_helper = StructHelper::deserialize(deserializer)?;

        Ok(PatternWithParams {
            pattern: struct_helper.0,
            params: struct_helper.1,
            context: struct_helper.2,
            id: struct_helper.3,
            force_trace: struct_helper.4,
        })
    }
}

impl PatternWithParams {
    #[must_use]
    pub fn new(pattern: String, params: Vec<Value>) -> Self {
        let mut carrier = HashMap::new();
        inject_context(&mut carrier);
        let force_trace = force_trace();
        if carrier.is_empty() {
            PatternWithParams {
                pattern,
                params,
                context: None,
                id: None,
                force_trace,
            }
        } else {
            PatternWithParams {
                pattern,
                params,
                context: Some(carrier),
                id: Some(current_id()),
                force_trace,
            }
        }
    }
}

impl From<PatternWithParams> for String {
    fn from(p: PatternWithParams) -> Self {
        format!("pattern: {}, parameters: {:?}", p.pattern, p.params)
    }
}

impl ExecutionPlan {
    /// Transform plan sub-tree (pointed by top) to sql string
    ///
    /// # Errors
    /// - plan is invalid and can't be transformed
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    pub fn to_sql(
        &self,
        nodes: &[&SyntaxData],
        buckets: &Buckets,
    ) -> Result<PatternWithParams, QueryPlannerError> {
        let (sql, params) = child_span("\"syntax.ordered.sql\"", || {
            let mut params: Vec<Value> = Vec::new();

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
                                Relational::Except { .. } => sql.push_str("EXCEPT"),
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
                                | Expression::Row { .. }
                                | Expression::Unary { .. } => {}
                                Expression::Constant { value, .. } => {
                                    write!(sql, "{}", value).map_err(|e| {
                                        QueryPlannerError::CustomError(format!(
                                            "Failed to write constant value to SQL: {}",
                                            e
                                        ))
                                    })?;
                                }
                                Expression::Reference { position, .. } => {
                                    let rel_id: usize =
                                        *ir_plan.get_relational_from_reference_node(*id)?;
                                    let rel_node = ir_plan.get_relation_node(rel_id)?;
                                    let alias = &ir_plan.get_alias_from_reference_node(expr)?;

                                    if rel_node.is_insert() {
                                        // We expect `INSERT INTO t(a, b) VALUES(1, 2)`
                                        // rather then `INSERT INTO t(t.a, t.b) VALUES(1, 2)`.
                                        sql.push_str(alias);
                                    } else if let Some(name) =
                                        rel_node.scan_name(ir_plan, *position)?
                                    {
                                        sql.push_str(name);
                                        sql.push('.');
                                        sql.push_str(alias);
                                    } else {
                                        sql.push_str(alias);
                                    }
                                }
                                Expression::StableFunction { name, .. } => {
                                    sql.push_str(name.as_str());
                                }
                            },
                        }
                    }
                    SyntaxData::Parameter(id) => {
                        sql.push('?');
                        let value = ir_plan.get_expression_node(*id)?;
                        if let Expression::Constant { value, .. } = value {
                            params.push(value.clone());
                        } else {
                            return Err(QueryPlannerError::CustomError(format!(
                                "Parameter {:?} is not a constant",
                                value
                            )));
                        }
                    }
                    SyntaxData::VTable(vtable) => {
                        let cols_count = vtable.get_columns().len();

                        let cols = |base_idx: &mut usize| {
                            vtable
                                .get_columns()
                                .iter()
                                .map(|c| {
                                    *base_idx += 1;
                                    format!("COLUMN_{} as \"{}\"", base_idx, c.name)
                                })
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
                            let values = (0..cols_count)
                                .map(|_| "null")
                                .collect::<Vec<&str>>()
                                .join(",");

                            write!(
                                sql,
                                "SELECT {} FROM (VALUES ({})) WHERE FALSE",
                                cols(&mut anonymous_col_idx_base),
                                values
                            )
                            .map_err(|e| QueryPlannerError::CustomError(e.to_string()))?;
                        } else {
                            let values = tuples
                                .iter()
                                .map(|t| format!("({})", (t.iter().map(|_| "?")).join(",")))
                                .collect::<Vec<String>>()
                                .join(",");

                            anonymous_col_idx_base += cols_count * (tuples.len() - 1);

                            write!(
                                sql,
                                "SELECT {} FROM (VALUES {})",
                                cols(&mut anonymous_col_idx_base),
                                values
                            )
                            .map_err(|e| {
                                QueryPlannerError::CustomError(format!(
                                    "Failed to generate SQL for VTable: {}",
                                    e
                                ))
                            })?;

                            for t in tuples {
                                for v in t {
                                    params.push(v.clone());
                                }
                            }
                        }
                    }
                }
            }
            Ok((sql, params))
        })?;
        // MUST be constructed out of the `syntax.ordered.sql` context scope.
        Ok(PatternWithParams::new(sql, params))
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
