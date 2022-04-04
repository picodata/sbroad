use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use itertools::Itertools;
use serde::Serialize;
use traversal::DftPost;

use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::Plan;

use super::operator::Bool;
use super::value::Value;

#[derive(Debug, Serialize, Default)]
struct Col {
    /// Column alias from sql query
    alias: Option<String>,

    /// Column name
    col: String,

    /// Column table
    scan: Option<String>,
}

impl Col {
    #[allow(dead_code)]
    fn new(plan: &Plan, subtree_top: usize) -> Result<Self, QueryPlannerError> {
        let mut column = Col::default();

        let dft_post = DftPost::new(&subtree_top, |node| plan.nodes.expr_iter(node, true));
        for (_, id) in dft_post {
            let current_node = plan.get_expression_node(*id)?;

            match &current_node {
                Expression::Alias { name, .. } => {
                    column.alias = Some(name.to_string());
                }
                Expression::Bool { .. } | Expression::Row { .. } | Expression::Constant { .. } => {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Expression node [{:?}] is not supported for explain yet",
                        current_node
                    )));
                }
                Expression::Reference { position, .. } => {
                    let rel_id: usize = plan
                        .get_relational_from_reference_node(*id)?
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Reference doesn't have child relation node".into(),
                            )
                        })?;

                    let rel_node = plan.get_relation_node(rel_id)?;
                    let alias = plan.get_alias_from_reference_node(current_node)?;

                    column.col.push_str(alias);

                    if let Some(name) = rel_node.scan_name(plan, *position)? {
                        column.scan = Some(name.to_string());
                    }
                }
            }
        }

        Ok(column)
    }
}

impl Display for Col {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        if let Some(tbl) = &self.scan {
            s.push_str(&format!("{}.", tbl));
        }

        s.push_str(&self.col);

        if let Some(a) = &self.alias {
            s.push_str(&format!(" -> {}", a));
        }

        write!(f, "{}", s)
    }
}

#[derive(Debug, Serialize)]
struct Projection {
    /// List of colums in sql query
    cols: Vec<Col>,
}

impl Projection {
    #[allow(dead_code)]
    fn new(plan: &Plan, output_id: usize) -> Result<Self, QueryPlannerError> {
        let mut result = Projection { cols: vec![] };

        let alias_list = plan.get_expression_node(output_id)?;

        for col_node_id in alias_list.get_row_list()? {
            let col = Col::new(plan, *col_node_id)?;

            result.cols.push(col);
        }
        Ok(result)
    }
}

impl Display for Projection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("projection ");

        let cols = &self
            .cols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        s.push_str(&format!("({})", cols));
        write!(f, "{}", s)
    }
}

#[derive(Debug, Serialize)]
struct Scan {
    /// Table name
    table: String,

    /// Table alias
    alias: Option<String>,
}

impl Scan {
    #[allow(dead_code)]
    fn new(table: String, alias: Option<String>) -> Self {
        Scan { table, alias }
    }
}

impl Display for Scan {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("scan ");

        s.push_str(&self.table);

        if let Some(a) = &self.alias {
            s.push_str(&format!(" -> {}", a));
        }

        write!(f, "{}", s)
    }
}

#[derive(Debug, Serialize)]
struct Ref {
    /// Reference to subquery index in `FullExplain` parts
    number: usize,
}

impl Ref {
    #[allow(dead_code)]
    fn new(number: usize) -> Self {
        Ref { number }
    }
}

impl Display for Ref {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.number)
    }
}

#[derive(Debug, Serialize)]
enum RowVal {
    Const(Value),
    Column(Col),
    SqRef(Ref),
}

impl Display for RowVal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            RowVal::Const(c) => c.to_string(),
            RowVal::Column(c) => c.to_string(),
            RowVal::SqRef(r) => r.to_string(),
        };

        write!(f, "{}", s)
    }
}

#[derive(Debug, Serialize)]
struct Row {
    /// List of sql values in `WHERE` cause
    cols: Vec<RowVal>,
}

impl Row {
    #[allow(dead_code)]
    fn new() -> Self {
        Row { cols: vec![] }
    }

    fn add_col(&mut self, row: RowVal) {
        self.cols.push(row);
    }

    fn from_ir_nodes(
        plan: &Plan,
        node_ids: &[usize],
        ref_map: &HashMap<usize, usize>,
    ) -> Result<Self, QueryPlannerError> {
        let mut row = Row::new();

        for child in node_ids {
            let current_node = plan.get_expression_node(*child)?;

            match &current_node {
                Expression::Constant { value, .. } => {
                    row.add_col(RowVal::Const(value.clone()));
                }
                Expression::Bool { .. } | Expression::Row { .. } | Expression::Alias { .. } => {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Expression node [{:?}] hasn't supported in selection expalin node yet",
                        current_node
                    )));
                }
                Expression::Reference { position, .. } => {
                    let rel_id: usize = plan
                        .get_relational_from_reference_node(*child)?
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Reference doesn't have child relation node.".into(),
                            )
                        })?;

                    let rel_node = plan.get_relation_node(rel_id)?;

                    // if reletation node (as rule is subquery) doesn't have alias name it means
                    // that this node is part of where cause
                    if rel_node.scan_name(plan, *position)?.is_some() {
                        let col = Col::new(plan, *child)?;
                        row.add_col(RowVal::Column(col));
                    } else {
                        let sq_num = ref_map.get(&rel_id).ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Relation subquery number not found in the map".into(),
                            )
                        })?;

                        row.add_col(RowVal::SqRef(Ref::new(*sq_num)));
                    }
                }
            }
        }

        Ok(row)
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let cols = &self
            .cols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "ROW({})", cols)
    }
}

/// Recursive type which describe `WHERE` cause in eplain
#[derive(Debug, Serialize)]
enum Selection {
    Row(Row),
    BinaryOp {
        left: Box<Selection>,
        op: Bool,
        right: Box<Selection>,
    },
}

impl Selection {
    #[allow(dead_code)]
    fn new(
        plan: &Plan,
        subtree_node_id: usize,
        ref_map: &HashMap<usize, usize>,
    ) -> Result<Self, QueryPlannerError> {
        let current_node = plan.get_expression_node(subtree_node_id)?;

        let result = match current_node {
            Expression::Bool { left, op, right } => Selection::BinaryOp {
                left: Box::new(Selection::new(plan, *left, ref_map)?),
                op: op.clone(),
                right: Box::new(Selection::new(plan, *right, ref_map)?),
            },
            Expression::Row { list, .. } => {
                let row = Row::from_ir_nodes(plan, list, ref_map)?;
                Selection::Row(row)
            }
            Expression::Reference { .. }
            | Expression::Constant { .. }
            | Expression::Alias { .. } => {
                return Err(QueryPlannerError::CustomError(
                    "Unsupport expr in selection explain node".into(),
                ));
            }
        };

        Ok(result)
    }
}

impl Display for Selection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            Selection::Row(r) => r.to_string(),
            Selection::BinaryOp { left, op, right } => {
                format!("{} {} {}", left, op, right)
            }
        };

        write!(f, "{}", s)
    }
}

#[derive(Debug, Serialize)]
struct SubQuery {
    /// Subquery alias. For subquery in `WHERE` cause alias is `None`.
    alias: Option<String>,
}

impl SubQuery {
    #[allow(dead_code)]
    fn new(alias: Option<String>) -> Self {
        SubQuery { alias }
    }
}

impl Display for SubQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("scan");

        if let Some(a) = &self.alias {
            s.push_str(&format!(" {}", a));
        }
        write!(f, "{}", s)
    }
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
enum ExplainNode {
    Projection(Projection),
    Scan(Scan),
    Selection(Selection),
    UnionAll,
    SubQuery(SubQuery),
}

impl Display for ExplainNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            ExplainNode::Projection(e) => e.to_string(),
            ExplainNode::Scan(s) => s.to_string(),
            ExplainNode::Selection(s) => format!("selection {}", s),
            ExplainNode::UnionAll => "union all".to_string(),
            ExplainNode::SubQuery(s) => s.to_string(),
        };

        write!(f, "{}", s)
    }
}

/// Describe sql query (or subquery) as recursive type
#[derive(Debug, Serialize)]
struct ExplainTreePart {
    /// Level hepls to detect count of idents
    #[serde(skip_serializing)]
    level: usize,

    /// Current node of sql query
    current: Option<ExplainNode>,

    /// Children nodes of current sql node
    children: Vec<ExplainTreePart>,
}

impl Display for ExplainTreePart {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        if let Some(c) = &self.current {
            s.push_str(&format!("{}\n", &c.to_string()));
        }

        let ident = (0..=self.level).map(|_| "    ").collect::<String>();

        for child in &self.children {
            s.push_str(&ident);
            s.push_str(&child.to_string());
        }

        write!(f, "{}", s)
    }
}

impl Default for ExplainTreePart {
    fn default() -> Self {
        Self {
            level: 0,
            current: None,
            children: Vec::with_capacity(200),
        }
    }
}

impl ExplainTreePart {
    fn with_level(level: usize) -> Self {
        ExplainTreePart {
            level,
            current: None,
            children: Vec::with_capacity(100),
        }
    }
}

#[derive(Debug, Serialize, Default)]
struct FullExplain {
    /// Main sql subtree
    main_query: ExplainTreePart,
    /// Related part of query which describe as `WHERE` cause subqueries
    subqueries: HashMap<usize, ExplainTreePart>,
}

impl Display for FullExplain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = self.main_query.to_string();

        for k in self.subqueries.keys().sorted() {
            s.push_str(&format!("subquery ${}:\n", k));
            s.push_str(&self.subqueries[k].to_string());
        }
        write!(f, "{}", s)
    }
}

impl FullExplain {
    #[allow(dead_code)]
    pub fn new(ir: &Plan, top_id: usize) -> Result<Self, QueryPlannerError> {
        let mut buffer: Vec<ExplainTreePart> = Vec::with_capacity(100);

        let mut prev_level = 0;
        let mut result = FullExplain::default();

        let dft_post = DftPost::new(&top_id, |node| ir.nodes.rel_iter(node));
        for (level, id) in dft_post {
            let mut current_node = ExplainTreePart::with_level(level);

            let node = ir.get_relation_node(*id)?;
            current_node.current = match &node {
                Relational::Projection { output, .. } => {
                    let p = Projection::new(ir, *output)?;
                    Some(ExplainNode::Projection(p))
                }
                Relational::ScanRelation {
                    relation, alias, ..
                } => {
                    let s = Scan::new(
                        relation.to_string(),
                        alias.as_ref().map(ToString::to_string),
                    );
                    Some(ExplainNode::Scan(s))
                }
                Relational::Selection {
                    children, filter, ..
                } => {
                    let mut sq_ref_map = HashMap::new();

                    if children.len() > 1 {
                        let mut sq_set = children.get(0..).unwrap().to_vec();
                        sq_set.sort_unstable();

                        let mut sq_count = children.len() - 1;
                        let actual_refs_offset = result.subqueries.len();
                        while sq_count > 0 {
                            if let Some(p) = buffer.pop() {
                                let sq_id = match children.get(sq_count) {
                                    Some(v) => *v,
                                    None => continue,
                                };

                                let ref_num = match sq_set.iter().position(|&r| r == sq_id) {
                                    Some(v) => v,
                                    None => continue,
                                };

                                sq_ref_map.insert(sq_id, ref_num);
                                result.subqueries.insert(actual_refs_offset + ref_num, p);
                            }

                            sq_count -= 1;
                        }
                    }

                    let s = Selection::new(ir, *filter, &sq_ref_map)?;
                    Some(ExplainNode::Selection(s))
                }
                Relational::UnionAll { .. } => {
                    if let Some(n) = buffer.pop() {
                        current_node.children.push(n);
                    }

                    Some(ExplainNode::UnionAll)
                }
                Relational::ScanSubQuery { alias, .. } => {
                    let s = SubQuery::new(alias.as_ref().map(ToString::to_string));

                    Some(ExplainNode::SubQuery(s))
                }

                Relational::InnerJoin { .. }
                | Relational::Motion { .. }
                | Relational::Insert { .. }
                | Relational::Values { .. }
                | Relational::ValuesRow { .. } => {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Explain hasn't supported node {:?} yet",
                        node
                    )))
                }
            };

            if level < prev_level {
                if let Some(n) = buffer.pop() {
                    current_node.children.push(n);
                }
            }

            buffer.push(current_node);

            prev_level = level;
        }

        result.main_query = buffer
            .pop()
            .ok_or_else(|| QueryPlannerError::CustomError("Invalid explain top node.".into()))?;

        Ok(result)
    }
}

impl Plan {
    /// Display ir explain
    ///
    /// # Errors
    /// - Failed to get top node
    /// - Failed to build explain
    pub fn as_explain(&self) -> Result<String, QueryPlannerError> {
        let top_id = self.get_top()?;
        let explain = FullExplain::new(self, top_id)?;
        Ok(explain.to_string())
    }
}

#[cfg(test)]
mod tests;
