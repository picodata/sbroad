use crate::debug;
use ahash::AHashMap;
use opentelemetry::Context;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::IntoIter;
use std::collections::HashMap;
use std::fmt::Write as _;
use tarantool::tlua::{self, Push};
use tarantool::tuple::{FunctionArgs, Tuple};

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::ir::ExecutionPlan;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::value::{LuaValue, Value};
use crate::ir::Node;
use crate::otm::{child_span, current_id, deserialize_context, inject_context, query_id};

use super::space::TmpSpace;
use super::tree::SyntaxData;

#[derive(Debug, Eq, Deserialize, Serialize, Push, Clone)]
pub struct PatternWithParams {
    pub pattern: String,
    pub params: Vec<Value>,
    pub context: Option<HashMap<String, String>>,
    pub id: Option<String>,
    // Name of the tracer to use
    pub tracer: Option<String>,
}

impl PartialEq for PatternWithParams {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern && self.params == other.params
    }
}

impl TryFrom<FunctionArgs> for PatternWithParams {
    type Error = SbroadError;

    fn try_from(value: FunctionArgs) -> Result<Self, Self::Error> {
        debug!(
            Option::from("argument parsing"),
            &format!("Query parameters: {value:?}"),
        );
        match Tuple::from(value).decode::<EncodedPatternWithParams>() {
            Ok(encoded) => Ok(PatternWithParams::try_from(encoded)?),
            Err(e) => Err(SbroadError::ParsingError(
                Entity::PatternWithParams,
                format!("{e:?}"),
            )),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct EncodedPatternWithParams(
    String,
    Option<Vec<LuaValue>>,
    Option<HashMap<String, String>>,
    Option<String>,
    Option<String>,
);

impl From<PatternWithParams> for EncodedPatternWithParams {
    fn from(mut value: PatternWithParams) -> Self {
        let encoded_params: Vec<LuaValue> = value.params.drain(..).map(LuaValue::from).collect();
        EncodedPatternWithParams(
            value.pattern,
            Some(encoded_params),
            value.context,
            value.id,
            value.tracer,
        )
    }
}

impl TryFrom<EncodedPatternWithParams> for PatternWithParams {
    type Error = SbroadError;
    fn try_from(mut value: EncodedPatternWithParams) -> Result<Self, Self::Error> {
        let params: Vec<Value> = match value.1.take() {
            Some(mut encoded_params) => encoded_params
                .drain(..)
                .map(Value::from)
                .collect::<Vec<Value>>(),
            None => Vec::new(),
        };
        let res = PatternWithParams {
            pattern: value.0,
            params,
            context: value.2,
            id: value.3,
            tracer: value.4,
        };
        Ok(res)
    }
}

impl PatternWithParams {
    #[must_use]
    pub fn new(pattern: String, params: Vec<Value>) -> Self {
        let mut carrier = HashMap::new();
        inject_context(&mut carrier);
        if carrier.is_empty() {
            PatternWithParams {
                pattern,
                params,
                context: None,
                id: None,
                tracer: None,
            }
        } else {
            PatternWithParams {
                pattern,
                params,
                context: Some(carrier),
                id: current_id(),
                tracer: None,
            }
        }
    }

    #[must_use]
    pub fn clone_id(&self) -> String {
        self.id.clone().unwrap_or_else(|| query_id(&self.pattern))
    }

    pub fn extract_context(&mut self) -> Context {
        deserialize_context(self.context.take())
    }
}

impl From<PatternWithParams> for String {
    fn from(p: PatternWithParams) -> Self {
        format!("pattern: {}, parameters: {:?}", p.pattern, p.params)
    }
}

#[derive(Debug)]
pub struct TmpSpaceMap {
    inner: AHashMap<String, TmpSpace>,
}

impl IntoIterator for TmpSpaceMap {
    type Item = (String, TmpSpace);
    type IntoIter = IntoIter<String, TmpSpace>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl TmpSpaceMap {
    fn with_capacity(capacity: usize) -> Self {
        TmpSpaceMap {
            inner: AHashMap::with_capacity(capacity),
        }
    }

    fn get(&self, name: &str) -> Option<&TmpSpace> {
        self.inner.get(name)
    }

    fn insert(&mut self, name: String, space: TmpSpace) -> Result<(), SbroadError> {
        if self.inner.contains_key(&name) {
            return Err(SbroadError::FailedTo(
                Action::Insert,
                Some(Entity::Space),
                format!("in the temporary space map ({name})"),
            ));
        }
        self.inner.insert(name, space);
        Ok(())
    }
}

impl ExecutionPlan {
    /// # Errors
    /// - IR plan is invalid
    pub fn to_params(
        &self,
        nodes: &[&SyntaxData],
        buckets: &Buckets,
    ) -> Result<Vec<Value>, SbroadError> {
        let ir_plan = self.get_ir_plan();
        let mut params: Vec<Value> = Vec::new();

        for data in nodes {
            match data {
                SyntaxData::Parameter(id) => {
                    let value = ir_plan.get_expression_node(*id)?;
                    if let Expression::Constant { value, .. } = value {
                        params.push(value.clone());
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some(format!("parameter {value:?} is not a constant")),
                        ));
                    }
                }
                SyntaxData::VTable(motion_id) => {
                    let vtable = self.get_motion_vtable(*motion_id)?;
                    let tuples = (*vtable).get_tuples_with_buckets(buckets);
                    for t in tuples {
                        for v in t {
                            params.push(v.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(params)
    }

    /// Transform plan sub-tree (pointed by top) to sql string pattern with parameters.
    ///
    /// # Errors
    /// - plan is invalid and can't be transformed
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    pub fn to_sql(
        &self,
        nodes: &[&SyntaxData],
        buckets: &Buckets,
        name_base: &str,
    ) -> Result<(PatternWithParams, TmpSpaceMap), SbroadError> {
        let vtable_engine = self.vtable_engine()?;
        let capacity = self.get_vtables().map_or(1, HashMap::len);
        let mut tmp_spaces = TmpSpaceMap::with_capacity(capacity);
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
                    SyntaxData::Cast => sql.push_str("CAST"),
                    SyntaxData::CloseParenthesis => sql.push(')'),
                    SyntaxData::Concat => sql.push_str("||"),
                    SyntaxData::Comma => sql.push(','),
                    SyntaxData::Condition => sql.push_str("ON"),
                    SyntaxData::Distinct => sql.push_str("DISTINCT"),
                    SyntaxData::Inline(content) => sql.push_str(content),
                    SyntaxData::From => sql.push_str("FROM"),
                    SyntaxData::Operator(s) => sql.push_str(s.as_str()),
                    SyntaxData::OpenParenthesis => sql.push('('),
                    SyntaxData::PlanId(id) => {
                        let node = ir_plan.get_node(*id)?;
                        match node {
                            Node::Ddl(_) => {
                                return Err(SbroadError::Unsupported(
                                    Entity::Node,
                                    Some("DDL nodes are not supported in the generated SQL".into()),
                                ));
                            }
                            Node::Acl(_) => {
                                return Err(SbroadError::Unsupported(
                                    Entity::Node,
                                    Some("ACL nodes are not supported in the generated SQL".into()),
                                ));
                            }
                            Node::Block(_) => {
                                return Err(SbroadError::Unsupported(
                                    Entity::Node,
                                    Some(
                                        "Code block nodes are not supported in the generated SQL"
                                            .into(),
                                    ),
                                ));
                            }
                            Node::Parameter => {
                                return Err(SbroadError::Unsupported(
                                    Entity::Node,
                                    Some(
                                        "Parameters are not supported in the generated SQL".into(),
                                    ),
                                ));
                            }
                            Node::Relational(rel) => match rel {
                                Relational::Except { .. } => sql.push_str("EXCEPT"),
                                Relational::GroupBy { .. } => sql.push_str("GROUP BY"),
                                Relational::Intersect { .. } => sql.push_str("INTERSECT"),
                                Relational::Having { .. } => sql.push_str("HAVING"),
                                Relational::Delete { .. } => {
                                    return Err(SbroadError::Unsupported(
                                        Entity::Node,
                                        Some(
                                            "DML nodes are not supported in the generated SQL"
                                                .into(),
                                        ),
                                    ));
                                }
                                Relational::Insert { relation, .. } => {
                                    sql.push_str("INSERT INTO ");
                                    sql.push_str(relation.as_str());
                                }
                                Relational::Join { kind, .. } => sql.push_str(
                                    format!("{} JOIN", kind.to_string().to_uppercase()).as_str(),
                                ),
                                Relational::Projection { .. } => sql.push_str("SELECT"),
                                Relational::ScanRelation { relation, .. } => {
                                    sql.push_str(relation);
                                }
                                Relational::ScanSubQuery { .. }
                                | Relational::Motion { .. }
                                | Relational::ValuesRow { .. } => {}
                                Relational::Selection { .. } => sql.push_str("WHERE"),
                                Relational::UnionAll { .. } => sql.push_str("UNION ALL"),
                                Relational::Update { .. } => sql.push_str("UPDATE"),
                                Relational::Values { .. } => sql.push_str("VALUES"),
                            },
                            Node::Expression(expr) => {
                                match expr {
                                    Expression::Alias { .. }
                                    | Expression::ExprInParentheses { .. }
                                    | Expression::Bool { .. }
                                    | Expression::Arithmetic { .. }
                                    | Expression::Cast { .. }
                                    | Expression::Concat { .. }
                                    | Expression::Row { .. }
                                    | Expression::Unary { .. } => {}
                                    Expression::Constant { value, .. } => {
                                        write!(sql, "{value}").map_err(|e| {
                                            SbroadError::FailedTo(
                                                Action::Put,
                                                Some(Entity::Value),
                                                format!("constant value to SQL: {e}"),
                                            )
                                        })?;
                                    }
                                    Expression::Reference { position, .. } => {
                                        let rel_id: usize =
                                            *ir_plan.get_relational_from_reference_node(*id)?;
                                        let rel_node = ir_plan.get_relation_node(rel_id)?;

                                        if rel_node.is_motion() {
                                            if let Ok(vt) = self.get_motion_vtable(*id) {
                                                let alias = (*vt)
                                                    .get_columns()
                                                    .get(*position)
                                                    .map(|column| &column.name)
                                                    .ok_or_else(|| {
                                                        SbroadError::NotFound(
                                                            Entity::Name,
                                                            format!(
                                                                "for column at position {position}"
                                                            ),
                                                        )
                                                    })?;
                                                if let Some(name) = (*vt).get_alias() {
                                                    sql.push_str(name);
                                                    sql.push('.');
                                                    sql.push_str(alias);
                                                    continue;
                                                }
                                            }
                                        }

                                        let alias = &ir_plan.get_alias_from_reference_node(expr)?;
                                        if rel_node.is_insert() {
                                            // We expect `INSERT INTO t(a, b) VALUES(1, 2)`
                                            // rather then `INSERT INTO t(t.a, t.b) VALUES(1, 2)`.
                                            sql.push_str(alias);
                                            continue;
                                        }
                                        if let Some(name) =
                                            rel_node.scan_name(ir_plan, *position)?
                                        {
                                            sql.push_str(name);
                                            sql.push('.');
                                            sql.push_str(alias);
                                            continue;
                                        }
                                        sql.push_str(alias);
                                    }
                                    Expression::StableFunction { name, .. } => {
                                        sql.push_str(name.as_str());
                                    }
                                    Expression::CountAsterisk => {
                                        sql.push('*');
                                    }
                                }
                            }
                        }
                    }
                    SyntaxData::Parameter(id) => {
                        sql.push('?');
                        let value = ir_plan.get_expression_node(*id)?;
                        if let Expression::Constant { value, .. } = value {
                            params.push(value.clone());
                        } else {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(format!("parameter {value:?} is not a constant")),
                            ));
                        }
                    }
                    SyntaxData::VTable(motion_id) => {
                        let name = TmpSpace::generate_space_name(name_base, *motion_id);
                        let vtable = self.get_motion_vtable(*motion_id)?;
                        let col_names = vtable
                            .get_columns()
                            .iter()
                            .map(|c| {
                                if let (Some('"'), Some('"')) =
                                    (c.name.chars().next(), c.name.chars().last())
                                {
                                    c.name.clone()
                                } else {
                                    format!("\"{}\"", c.name)
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        write!(sql, "SELECT {col_names} FROM \"{name}\"").map_err(|e| {
                            SbroadError::FailedTo(
                                Action::Serialize,
                                Some(Entity::VirtualTable),
                                format!("SQL builder: {e}"),
                            )
                        })?;
                        // BETWEEN can refer to the same virtual table multiple times.
                        if tmp_spaces.get(&name).is_none() {
                            let space = TmpSpace::initialize(
                                self,
                                name_base,
                                *motion_id,
                                buckets,
                                &vtable_engine,
                            )?;
                            tmp_spaces.insert(name, space)?;
                        }
                    }
                }
            }
            Ok((sql, params))
        })?;
        // MUST be constructed out of the `syntax.ordered.sql` context scope.
        Ok((PatternWithParams::new(sql, params), tmp_spaces))
    }

    /// Checks if the given query subtree modifies data or not.
    ///
    /// # Errors
    /// - If the subtree top is not a relational node.
    pub fn subtree_modifies_data(&self, top_id: usize) -> Result<bool, SbroadError> {
        // Tarantool doesn't support `INSERT`, `UPDATE` and `DELETE` statements
        // with `RETURNING` clause. That is why it is enough to check if the top
        // node is a data modification statement or not.
        let top = self.get_ir_plan().get_relation_node(top_id)?;
        Ok(top.is_dml())
    }
}

#[cfg(test)]
mod tests;
