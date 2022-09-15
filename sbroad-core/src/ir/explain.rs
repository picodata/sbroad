use std::collections::HashMap;
use std::fmt::{Display, Formatter, Write as _};

use serde::Serialize;
use traversal::DftPost;

use crate::errors::QueryPlannerError;
use crate::ir::expression::Expression;
use crate::ir::operator::Relational;
use crate::ir::transformation::redistribution::{
    DataGeneration, MotionPolicy as IrMotionPolicy, Target as IrTarget,
};
use crate::ir::Plan;

use super::operator::{Bool, Unary};
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
                Expression::Bool { .. }
                | Expression::Row { .. }
                | Expression::Constant { .. }
                | Expression::Unary { .. } => {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Expression node [{:?}] is not supported for explain yet",
                        current_node
                    )));
                }
                Expression::Reference { position, .. } => {
                    let rel_id: usize = *plan.get_relational_from_reference_node(*id)?;

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
            write!(s, "{}.", tbl)?;
        }

        s.push_str(&self.col);

        if let Some(a) = &self.alias {
            write!(s, " -> {}", a)?;
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

        write!(s, "({})", cols)?;
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
            write!(s, " -> {}", a)?;
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
                Expression::Bool { .. }
                | Expression::Row { .. }
                | Expression::Alias { .. }
                | Expression::Unary { .. } => {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Expression node [{:?}] is not supported for selection's explain node yet",
                        current_node
                    )));
                }
                Expression::Reference { position, .. } => {
                    let rel_id: usize = *plan.get_relational_from_reference_node(*child)?;

                    let rel_node = plan.get_relation_node(rel_id)?;

                    // If relation node doesn't have alias name it means
                    // that this node is a sub-query and acts as a part of
                    // the WHERE cause.
                    if rel_node.scan_name(plan, *position)?.is_some() {
                        let col = Col::new(plan, *child)?;
                        row.add_col(RowVal::Column(col));
                    } else {
                        let sq_offset = ref_map.get(&rel_id).ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "The sub-query was not found in the map".into(),
                            )
                        })?;

                        row.add_col(RowVal::SqRef(Ref::new(*sq_offset)));
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
    UnaryOp {
        op: Unary,
        child: Box<Selection>,
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
                    "Unsupported expression in selection's explain node".into(),
                ));
            }
            Expression::Unary { op, child } => Selection::UnaryOp {
                op: op.clone(),
                child: Box::new(Selection::new(plan, *child, ref_map)?),
            },
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
            Selection::UnaryOp { op, child } => match op {
                Unary::IsNull | Unary::IsNotNull => format!("{} {}", child, op),
            },
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
            write!(s, " {}", a)?;
        }
        write!(f, "{}", s)
    }
}

#[derive(Debug, Serialize)]
struct Motion {
    policy: MotionPolicy,
    generation: DataGeneration,
}

impl Motion {
    fn new(policy: MotionPolicy, generation: DataGeneration) -> Self {
        Motion { policy, generation }
    }
}

impl Display for Motion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "motion [policy: {}, generation: {}]",
            &self.policy, &self.generation
        )
    }
}

#[derive(Debug, Serialize)]
enum MotionPolicy {
    Full,
    Segment(MotionKey),
    Local,
}

impl Display for MotionPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            MotionPolicy::Full => write!(f, "full"),
            MotionPolicy::Segment(mk) => write!(f, "segment({})", mk),
            MotionPolicy::Local => write!(f, "local"),
        }
    }
}

#[derive(Debug, Serialize)]
struct MotionKey {
    pub targets: Vec<Target>,
}

impl Display for MotionKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let targets = &self
            .targets
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        write!(f, "[{}]", targets)
    }
}

#[derive(Debug, Serialize)]
enum Target {
    Reference(String),
    Value(Value),
}

impl Display for Target {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Target::Reference(s) => write!(f, "ref({})", s),
            Target::Value(v) => write!(f, "value({})", v),
        }
    }
}

#[derive(Debug, Serialize)]
struct InnerJoin {
    condition: Selection,
}

impl Display for InnerJoin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "join on {}", self.condition)
    }
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
enum ExplainNode {
    Except,
    InnerJoin(InnerJoin),
    ValueRow(Row),
    Value,
    Insert(String),
    Projection(Projection),
    Scan(Scan),
    Selection(Selection),
    UnionAll,
    SubQuery(SubQuery),
    Motion(Motion),
}

impl Display for ExplainNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            ExplainNode::Except => "except".to_string(),
            ExplainNode::InnerJoin(i) => i.to_string(),
            ExplainNode::ValueRow(r) => format!("value row (data={})", r),
            ExplainNode::Value => "values".to_string(),
            ExplainNode::Insert(s) => format!("insert {}", s),
            ExplainNode::Projection(e) => e.to_string(),
            ExplainNode::Scan(s) => s.to_string(),
            ExplainNode::Selection(s) => format!("selection {}", s),
            ExplainNode::UnionAll => "union all".to_string(),
            ExplainNode::SubQuery(s) => s.to_string(),
            ExplainNode::Motion(m) => m.to_string(),
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
            writeln!(s, "{}", &c.to_string())?;
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
    subqueries: Vec<ExplainTreePart>,
}

impl Display for FullExplain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = self.main_query.to_string();

        for (pos, sq) in self.subqueries.iter().enumerate() {
            writeln!(s, "subquery ${}:", pos)?;
            s.push_str(&sq.to_string());
        }
        write!(f, "{}", s)
    }
}

impl FullExplain {
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    pub fn new(ir: &Plan, top_id: usize) -> Result<Self, QueryPlannerError> {
        let mut stack: Vec<ExplainTreePart> = Vec::with_capacity(ir.nodes.relation_node_amount());
        let mut result = FullExplain::default();

        let dft_post = DftPost::new(&top_id, |node| ir.nodes.rel_iter(node));
        for (level, id) in dft_post {
            let mut current_node = ExplainTreePart::with_level(level);
            let node = ir.get_relation_node(*id)?;
            current_node.current = match &node {
                Relational::Except { .. } => {
                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        current_node.children.push(left);
                        current_node.children.push(right);
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Exception node must have exactly two children".into(),
                        ));
                    }
                    Some(ExplainNode::Except)
                }
                Relational::Projection { output, .. } => {
                    // TODO: change this logic when we'll enable sub-queries in projection
                    let child = stack.pop().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Projection node must have exactly one child".into(),
                        )
                    })?;
                    current_node.children.push(child);
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
                    let mut sq_ref_map: HashMap<usize, usize> =
                        HashMap::with_capacity(children.len() - 1);
                    if let Some((_, other)) = children.split_first() {
                        for sq_id in other.iter().rev() {
                            let sq_node = stack.pop().ok_or_else(|| {
                                QueryPlannerError::CustomError(
                                    "Selection node failed to get a sub-query.".into(),
                                )
                            })?;
                            result.subqueries.push(sq_node);
                            let offset = result.subqueries.len() - 1;
                            sq_ref_map.insert(*sq_id, offset);
                        }
                        let child = stack.pop().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Selection node must have exactly one child".into(),
                            )
                        })?;
                        current_node.children.push(child);
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Selection node doesn't have any children".into(),
                        ));
                    }
                    let s = Selection::new(ir, *filter, &sq_ref_map)?;
                    Some(ExplainNode::Selection(s))
                }
                Relational::UnionAll { .. } => {
                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        current_node.children.push(left);
                        current_node.children.push(right);
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Union all node must have exactly two children".into(),
                        ));
                    }
                    Some(ExplainNode::UnionAll)
                }
                Relational::ScanSubQuery { alias, .. } => {
                    let child = stack.pop().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "ScanSubQuery node must have exactly one child".into(),
                        )
                    })?;
                    current_node.children.push(child);
                    let s = SubQuery::new(alias.as_ref().map(ToString::to_string));
                    Some(ExplainNode::SubQuery(s))
                }
                Relational::Motion {
                    children,
                    policy,
                    generation,
                    ..
                } => {
                    let child = stack.pop().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Motion node must have exactly one child".into(),
                        )
                    })?;
                    current_node.children.push(child);

                    let p = match policy {
                        IrMotionPolicy::Segment(s) => {
                            let child_id = children.first().ok_or_else(|| {
                                QueryPlannerError::CustomError(
                                    "Current node should have exactly one child".to_string(),
                                )
                            })?;

                            let child_output_id = ir.get_relation_node(*child_id)?.output();
                            let col_list =
                                ir.get_expression_node(child_output_id)?.get_row_list()?;

                            let targets = (&s.targets)
                                .iter()
                                .map(|r| match r {
                                    IrTarget::Reference(pos) => {
                                        let col_id = *col_list.get(*pos).ok_or_else(|| {
                                            QueryPlannerError::CustomError(String::from(
                                                "Invalid position in list",
                                            ))
                                        })?;
                                        let col_name = ir
                                            .get_expression_node(col_id)?
                                            .get_alias_name()?
                                            .to_string();

                                        Ok(Target::Reference(col_name))
                                    }
                                    IrTarget::Value(v) => Ok(Target::Value(v.clone())),
                                })
                                .collect::<Result<Vec<Target>, _>>()?;

                            MotionPolicy::Segment(MotionKey { targets })
                        }
                        IrMotionPolicy::Full => MotionPolicy::Full,
                        IrMotionPolicy::Local => MotionPolicy::Local,
                    };
                    let m = Motion::new(p, generation.clone());

                    Some(ExplainNode::Motion(m))
                }
                Relational::InnerJoin {
                    children,
                    condition,
                    ..
                } => {
                    if children.len() < 2 {
                        return Err(QueryPlannerError::CustomError(
                            "Join must have at least two children".into(),
                        ));
                    }
                    let (_, subquery_ids) = children.split_at(2);
                    let mut sq_ref_map: HashMap<usize, usize> =
                        HashMap::with_capacity(children.len() - 2);

                    for sq_id in subquery_ids.iter().rev() {
                        let sq_node = stack.pop().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Join node failed to get a sub-query.".into(),
                            )
                        })?;
                        result.subqueries.push(sq_node);
                        let offset = result.subqueries.len() - 1;
                        sq_ref_map.insert(*sq_id, offset);
                    }

                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        current_node.children.push(left);
                        current_node.children.push(right);
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Join node must have exactly two children".into(),
                        ));
                    }

                    let condition = Selection::new(ir, *condition, &sq_ref_map)?;
                    Some(ExplainNode::InnerJoin(InnerJoin { condition }))
                }
                Relational::ValuesRow { data, children, .. } => {
                    let mut sq_ref_map: HashMap<usize, usize> =
                        HashMap::with_capacity(children.len());

                    for sq_id in children.iter().rev() {
                        let sq_node = stack.pop().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Insert node failed to get a sub-query.".into(),
                            )
                        })?;

                        result.subqueries.push(sq_node);
                        let offset = result.subqueries.len() - 1;
                        sq_ref_map.insert(*sq_id, offset);
                    }

                    let values = ir.get_expression_node(*data)?.get_row_list()?;
                    let row = Row::from_ir_nodes(ir, values, &sq_ref_map)?;

                    Some(ExplainNode::ValueRow(row))
                }
                Relational::Values { children, .. } => {
                    let mut amount_values = children.len();

                    while amount_values > 0 {
                        let value_row = stack.pop().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Insert node failed to get a value row.".into(),
                            )
                        })?;

                        current_node.children.insert(0, value_row);
                        amount_values -= 1;
                    }
                    Some(ExplainNode::Value)
                }
                Relational::Insert { relation, .. } => {
                    let values = stack.pop().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Insert node failed to get a value row.".into(),
                        )
                    })?;

                    current_node.children.push(values);

                    Some(ExplainNode::Insert(relation.into()))
                }
            };
            stack.push(current_node);
        }
        result.main_query = stack
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