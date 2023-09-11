use std::collections::HashMap;
use std::fmt::{Display, Formatter, Write as _};
use std::mem::take;

use itertools::Itertools;
use serde::Serialize;

use crate::errors::{Entity, SbroadError};
use crate::ir::expression::cast::Type as CastType;
use crate::ir::expression::Expression;
use crate::ir::operator::{ConflictStrategy, JoinKind, Relational};
use crate::ir::relation::Type;
use crate::ir::transformation::redistribution::{
    MotionKey as IrMotionKey, MotionPolicy as IrMotionPolicy, Target as IrTarget,
};
use crate::ir::{OptionKind, Plan};

use super::operator::{Arithmetic, Bool, Unary};
use super::tree::traversal::{PostOrder, EXPR_CAPACITY, REL_CAPACITY};
use super::value::Value;

#[derive(Debug, Serialize)]
enum ColExpr {
    Alias(Box<ColExpr>, String),
    Arithmetic(Box<ColExpr>, BinaryOp, Box<ColExpr>, bool),
    Bool(Box<ColExpr>, BinaryOp, Box<ColExpr>),
    Unary(Unary, Box<ColExpr>),
    Column(String, Type),
    Cast(Box<ColExpr>, CastType),
    Concat(Box<ColExpr>, Box<ColExpr>),
    StableFunction(String, Vec<ColExpr>, bool, Type),
    Row(Row),
    None,
}

impl Display for ColExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            ColExpr::Alias(expr, name) => format!("{expr} -> {name}"),
            ColExpr::Arithmetic(left, op, right, with_parentheses) => match with_parentheses {
                false => format!("{left} {op} {right}"),
                true => format!("({left} {op} {right})"),
            },
            ColExpr::Bool(left, op, right) => {
                if let BinaryOp::BoolOp(Bool::Or) = op {
                    format!("({left} {op} {right})")
                } else {
                    format!("{left} {op} {right}")
                }
            }
            ColExpr::Unary(op, expr) => match op {
                Unary::IsNull | Unary::IsNotNull => format!("{expr} {op}"),
                Unary::Exists | Unary::NotExists => format!("{op} {expr}"),
            },
            ColExpr::Column(c, col_type) => format!("{c}::{col_type}"),
            ColExpr::Cast(v, t) => format!("{v}::{t}"),
            ColExpr::Concat(l, r) => format!("{l} || {r}"),
            ColExpr::StableFunction(name, args, is_distinct, func_type) => {
                let formatted_args = format!("({})", args.iter().format(", "));
                format!(
                    "{name}({}{formatted_args})::{func_type}",
                    if *is_distinct { "distinct " } else { "" }
                )
            }
            ColExpr::Row(row) => row.to_string(),
            ColExpr::None => String::new(),
        };

        write!(f, "{s}")
    }
}

impl From<&ColExpr> for String {
    fn from(s: &ColExpr) -> Self {
        s.to_string()
    }
}

impl Default for ColExpr {
    fn default() -> Self {
        Self::None
    }
}

impl ColExpr {
    #[allow(dead_code, clippy::too_many_lines)]
    fn new(
        plan: &Plan,
        subtree_top: usize,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut stack: Vec<(ColExpr, usize)> = Vec::new();
        let mut dft_post =
            PostOrder::with_capacity(|node| plan.nodes.expr_iter(node, false), EXPR_CAPACITY);

        for (_, id) in dft_post.iter(subtree_top) {
            let current_node = plan.get_expression_node(id)?;

            match &current_node {
                Expression::Cast { to, .. } => {
                    let (expr, _) = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing CAST expression".to_string(),
                        )
                    })?;
                    let cast_expr = ColExpr::Cast(Box::new(expr), to.clone());
                    stack.push((cast_expr, id));
                }
                Expression::CountAsterisk => {
                    let count_asterisk_expr =
                        ColExpr::Column("*".to_string(), current_node.calculate_type(plan)?);
                    stack.push((count_asterisk_expr, id));
                }
                Expression::Reference { position, .. } => {
                    let mut col_name = String::new();

                    let rel_id: usize = *plan.get_relational_from_reference_node(id)?;
                    let rel_node = plan.get_relation_node(rel_id)?;

                    if let Some(name) = rel_node.scan_name(plan, *position)? {
                        col_name.push_str(name);
                        col_name.push('.');
                    }

                    let alias = plan.get_alias_from_reference_node(current_node)?;
                    col_name.push_str(alias);

                    let ref_expr = ColExpr::Column(col_name, current_node.calculate_type(plan)?);
                    stack.push((ref_expr, id));
                }
                Expression::Concat { .. } => {
                    let (right, _) = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing CONCAT expression".to_string(),
                        )
                    })?;
                    let (left, _) = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing CONCAT expression".to_string(),
                        )
                    })?;
                    let concat_expr = ColExpr::Concat(Box::new(left), Box::new(right));
                    stack.push((concat_expr, id));
                }
                Expression::Constant { value } => {
                    let expr =
                        ColExpr::Column(value.to_string(), current_node.calculate_type(plan)?);
                    stack.push((expr, id));
                }
                Expression::StableFunction {
                    name,
                    children,
                    is_distinct,
                    func_type,
                } => {
                    let mut len = children.len();
                    let mut args: Vec<ColExpr> = Vec::with_capacity(len);
                    while len > 0 {
                        let (arg, _) = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                format!("stack is empty, expected to pop {len} element while processing STABLE FUNCTION expression"),
                            )
                        })?;
                        args.push(arg);
                        len -= 1;
                    }
                    args.reverse();
                    let func_expr = ColExpr::StableFunction(
                        name.clone(),
                        args,
                        *is_distinct,
                        func_type.clone(),
                    );
                    stack.push((func_expr, id));
                }
                Expression::Row { list, .. } => {
                    let mut len = list.len();
                    let mut row: Vec<(ColExpr, usize)> = Vec::with_capacity(len);
                    while len > 0 {
                        let expr = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                format!("stack is empty, expected to pop {len} element while processing ROW expression"),
                            )
                        })?;
                        row.push(expr);
                        len -= 1;
                    }
                    row.reverse();
                    let row = Row::from_col_exprs_with_ids(plan, &mut row, sq_ref_map)?;
                    let row_expr = ColExpr::Row(row);
                    stack.push((row_expr, id));
                }
                Expression::Arithmetic {
                    left: _,
                    op,
                    right: _,
                    with_parentheses,
                } => {
                    let (right, _) = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing ARITHMETIC expression".to_string(),
                        )
                    })?;

                    let (left, _) = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing ARITHMETIC expression".to_string(),
                        )
                    })?;

                    let ar_expr = ColExpr::Arithmetic(
                        Box::new(left),
                        BinaryOp::ArithOp(op.clone()),
                        Box::new(right),
                        *with_parentheses,
                    );

                    stack.push((ar_expr, id));
                }
                Expression::Alias { name, .. } => {
                    let (expr, _) = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing ALIAS expression".to_string(),
                        )
                    })?;
                    let alias_expr = ColExpr::Alias(Box::new(expr), name.clone());
                    stack.push((alias_expr, id));
                }
                Expression::Bool { op, .. } => {
                    let (right, _) = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing BOOL expression".to_string(),
                        )
                    })?;

                    let (left, _) = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing BOOL expression".to_string(),
                        )
                    })?;

                    let bool_expr = ColExpr::Bool(
                        Box::new(left),
                        BinaryOp::BoolOp(op.clone()),
                        Box::new(right),
                    );

                    stack.push((bool_expr, id));
                }
                Expression::Unary { op, .. } => {
                    let (expr, _) = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing UNARY expression".to_string(),
                        )
                    })?;
                    let alias_expr = ColExpr::Unary(op.clone(), Box::new(expr));
                    stack.push((alias_expr, id));
                }
            }
        }

        let (expr, _) = stack
            .pop()
            .ok_or_else(|| SbroadError::UnexpectedNumberOfValues("stack is empty".to_string()))?;
        Ok(expr)
    }
}

/// Alias for map of (`SubQuery` id -> it's offset).
/// Offset = `SubQuery` index (e.g. in case there are several `SubQueries` in Selection WHERE condition
/// index will indicate to which of them Reference is pointing).
type SubQueryRefMap = HashMap<usize, usize>;

#[derive(Debug, Serialize)]
struct Projection {
    /// List of colums in sql query
    cols: Vec<ColExpr>,
}

impl Projection {
    #[allow(dead_code)]
    fn new(
        plan: &Plan,
        output_id: usize,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut result = Projection { cols: vec![] };

        let alias_list = plan.get_expression_node(output_id)?;

        for col_node_id in alias_list.get_row_list()? {
            let col = ColExpr::new(plan, *col_node_id, sq_ref_map)?;

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

        write!(s, "({cols})")?;
        write!(f, "{s}")
    }
}

#[derive(Debug, Serialize)]
struct GroupBy {
    /// List of colums in sql query
    gr_cols: Vec<ColExpr>,
    output_cols: Vec<ColExpr>,
}

impl GroupBy {
    #[allow(dead_code)]
    fn new(
        plan: &Plan,
        gr_cols: &Vec<usize>,
        output_id: usize,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut result = GroupBy {
            gr_cols: vec![],
            output_cols: vec![],
        };

        for col_node_id in gr_cols {
            let col = ColExpr::new(plan, *col_node_id, sq_ref_map)?;
            result.gr_cols.push(col);
        }
        let alias_list = plan.get_expression_node(output_id)?;
        for col_node_id in alias_list.get_row_list()? {
            let col = ColExpr::new(plan, *col_node_id, sq_ref_map)?;
            result.output_cols.push(col);
        }
        Ok(result)
    }
}

impl Display for GroupBy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = "group by ".to_string();

        let gr_cols = &self
            .gr_cols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        let output_cols = &self
            .output_cols
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join(", ");

        write!(s, "({gr_cols})")?;
        write!(s, " output: ({output_cols})")?;
        write!(f, "{s}")
    }
}

#[derive(Debug, Serialize)]
struct Update {
    /// List of colums in sql query
    table: String,
    update_statements: Vec<(String, String)>,
}

impl Update {
    #[allow(dead_code)]
    fn new(plan: &Plan, update_id: usize) -> Result<Self, SbroadError> {
        if let Relational::Update {
            relation: ref rel,
            update_columns_map,
            output: ref output_id,
            ..
        } = plan.get_relation_node(update_id)?
        {
            let mut update_statements: Vec<(String, String)> =
                Vec::with_capacity(update_columns_map.len());
            let table = plan.relations.get(rel).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Node,
                    Some(format!("invalid table {rel} in Update node")),
                )
            })?;
            let output_list = plan.get_row_list(*output_id)?;
            for (col_idx, proj_col) in update_columns_map {
                let col_name = table
                    .columns
                    .get(*col_idx)
                    .map(|c| c.name.clone())
                    .ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Node,
                            Some(format!("invalid column index {col_idx} in Update node")),
                        )
                    })?;
                let proj_alias = {
                    let alias_id = *output_list.get(*proj_col).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Node,
                            Some(format!(
                                "invalid update projection position {proj_col} in Update node"
                            )),
                        )
                    })?;
                    let node = plan.get_expression_node(alias_id)?;
                    if let Expression::Alias { name, .. } = node {
                        name.clone()
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some(format!(
                                "expected alias as top in Update output, got: {node:?}"
                            )),
                        ));
                    }
                };
                update_statements.push((col_name, proj_alias));
            }
            let result = Update {
                table: rel.to_string(),
                update_statements,
            };
            return Ok(result);
        }
        Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!("explain: expected Update node on id: {update_id}")),
        ))
    }
}

impl Display for Update {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = "update ".to_string();

        write!(s, "{}", &self.table)?;
        let update_statements = self
            .update_statements
            .iter()
            .map(|(col, alias)| format!("{col} = {alias}"))
            .join("\n");
        write!(s, "\n{update_statements}")?;
        write!(f, "{s}")
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
            write!(s, " -> {a}")?;
        }

        write!(f, "{s}")
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
    ColumnExpr(ColExpr),
    SqRef(Ref),
}

impl Display for RowVal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            RowVal::ColumnExpr(c) => c.to_string(),
            RowVal::SqRef(r) => r.to_string(),
        };

        write!(f, "{s}")
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

    fn from_col_exprs_with_ids(
        plan: &Plan,
        exprs_with_ids: &mut Vec<(ColExpr, usize)>,
        sq_ref_map: &SubQueryRefMap,
    ) -> Result<Self, SbroadError> {
        let mut row = Row::new();

        for (col_expr, expr_id) in take(exprs_with_ids) {
            let current_node = plan.get_expression_node(expr_id)?;

            match &current_node {
                Expression::Reference { .. } => {
                    let rel_id: usize = *plan.get_relational_from_reference_node(expr_id)?;

                    let rel_node = plan.get_relation_node(rel_id)?;
                    if plan.is_additional_child(rel_id)? {
                        if let Relational::ScanSubQuery { .. } | Relational::Motion { .. } =
                            rel_node
                        {
                            let sq_offset = sq_ref_map.get(&rel_id).ok_or_else(|| {
                                SbroadError::NotFound(
                                    Entity::SubQuery,
                                    format!("with index {rel_id} in the map"),
                                )
                            })?;
                            row.add_col(RowVal::SqRef(Ref::new(*sq_offset)));
                        } else {
                            return Err(SbroadError::Invalid(
                                Entity::Plan,
                                Some(format!(
                                    "additional child ({rel_id}) is not SQ or Motion: {rel_node:?}"
                                )),
                            ));
                        }
                    } else {
                        let col = ColExpr::new(plan, expr_id, sq_ref_map)?;
                        row.add_col(RowVal::ColumnExpr(col));
                    }
                }
                _ => row.add_col(RowVal::ColumnExpr(col_expr)),
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

        write!(f, "ROW({cols})")
    }
}

#[derive(Debug, Serialize)]
enum BinaryOp {
    ArithOp(Arithmetic),
    BoolOp(Bool),
}

impl Display for BinaryOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            BinaryOp::ArithOp(a) => a.to_string(),
            BinaryOp::BoolOp(b) => b.to_string(),
        };

        write!(f, "{s}")
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
            write!(s, " {a}")?;
        }
        write!(f, "{s}")
    }
}

#[derive(Debug, Serialize)]
struct Motion {
    policy: MotionPolicy,
}

impl Motion {
    fn new(policy: MotionPolicy) -> Self {
        Motion { policy }
    }
}

impl Display for Motion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "motion [policy: {}]", &self.policy)
    }
}

#[derive(Debug, Serialize)]
enum MotionPolicy {
    None,
    Full,
    Segment(MotionKey),
    Local,
    LocalSegment(MotionKey),
}

impl Display for MotionPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            MotionPolicy::None => write!(f, "none"),
            MotionPolicy::Full => write!(f, "full"),
            MotionPolicy::Segment(mk) => write!(f, "segment({mk})"),
            MotionPolicy::Local => write!(f, "local"),
            MotionPolicy::LocalSegment(mk) => write!(f, "local segment({mk})"),
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

        write!(f, "[{targets}]")
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
            Target::Reference(s) => write!(f, "ref({s})"),
            Target::Value(v) => write!(f, "value({v})"),
        }
    }
}

#[derive(Debug, Serialize)]
struct InnerJoin {
    condition: ColExpr,
    kind: JoinKind,
}

impl Display for InnerJoin {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let kind = match self.kind {
            JoinKind::LeftOuter => {
                let mut s = self.kind.to_string();
                s.push(' ');
                s
            }
            JoinKind::Inner => String::new(),
        };
        write!(f, "{kind}join on {0}", self.condition)
    }
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
enum ExplainNode {
    Delete(String),
    Except,
    GroupBy(GroupBy),
    InnerJoin(InnerJoin),
    ValueRow(ColExpr),
    Value,
    Insert(String, ConflictStrategy),
    Projection(Projection),
    Scan(Scan),
    Selection(ColExpr),
    Having(ColExpr),
    UnionAll,
    Update(Update),
    SubQuery(SubQuery),
    Motion(Motion),
}

impl Display for ExplainNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            ExplainNode::Delete(s) => format!("delete {s}"),
            ExplainNode::Except => "except".to_string(),
            ExplainNode::InnerJoin(i) => i.to_string(),
            ExplainNode::ValueRow(r) => format!("value row (data={r})"),
            ExplainNode::Value => "values".to_string(),
            ExplainNode::Insert(s, conflict) => format!("insert {s} on conflict: {conflict}"),
            ExplainNode::Projection(e) => e.to_string(),
            ExplainNode::GroupBy(p) => p.to_string(),
            ExplainNode::Scan(s) => s.to_string(),
            ExplainNode::Selection(s) => format!("selection {s}"),
            ExplainNode::Having(s) => format!("having {s}"),
            ExplainNode::UnionAll => "union all".to_string(),
            ExplainNode::Update(u) => u.to_string(),
            ExplainNode::SubQuery(s) => s.to_string(),
            ExplainNode::Motion(m) => m.to_string(),
        };

        write!(f, "{s}")
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

        write!(f, "{s}")
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
    /// Options imposed during query execution
    exec_options: Vec<(OptionKind, Value)>,
}

impl Display for FullExplain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = self.main_query.to_string();

        for (pos, sq) in self.subqueries.iter().enumerate() {
            writeln!(s, "subquery ${pos}:")?;
            s.push_str(&sq.to_string());
        }
        if !self.exec_options.is_empty() {
            writeln!(s, "execution options:")?;
            for opt in &self.exec_options {
                writeln!(s, "{} = {}", opt.0, opt.1)?;
            }
        }
        write!(f, "{s}")
    }
}

impl FullExplain {
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    pub fn new(ir: &Plan, top_id: usize) -> Result<Self, SbroadError> {
        let mut stack: Vec<ExplainTreePart> = Vec::with_capacity(ir.nodes.relation_node_amount());
        let mut result = FullExplain::default();
        result
            .exec_options
            .extend(ir.options.execute_options.clone().to_iter());
        result.exec_options.push((
            OptionKind::VTableMaxRows,
            Value::Unsigned(ir.options.vtable_max_rows),
        ));

        let mut dft_post = PostOrder::with_capacity(|node| ir.nodes.rel_iter(node), REL_CAPACITY);
        for (level, id) in dft_post.iter(top_id) {
            let mut current_node = ExplainTreePart::with_level(level);
            let node = ir.get_relation_node(id)?;
            current_node.current = match &node {
                Relational::Except { .. } => {
                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        current_node.children.push(left);
                        current_node.children.push(right);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Exception node must have exactly two children".into(),
                        ));
                    }
                    Some(ExplainNode::Except)
                }
                Relational::GroupBy {
                    gr_cols, output, ..
                } => {
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Groupby node must have at least one child".into(),
                        )
                    })?;
                    current_node.children.push(child);
                    let p = GroupBy::new(ir, gr_cols, *output, &HashMap::new())?;
                    Some(ExplainNode::GroupBy(p))
                }
                Relational::Projection { output, .. } => {
                    // TODO: change this logic when we'll enable sub-queries in projection
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Projection node must have exactly one child".into(),
                        )
                    })?;
                    current_node.children.push(child);
                    let p = Projection::new(ir, *output, &HashMap::new())?;
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
                }
                | Relational::Having {
                    children, filter, ..
                } => {
                    let mut sq_ref_map: SubQueryRefMap = HashMap::with_capacity(children.len() - 1);
                    if let Some((_, other)) = children.split_first() {
                        for sq_id in other.iter().rev() {
                            let sq_node = stack.pop().ok_or_else(|| {
                                SbroadError::UnexpectedNumberOfValues(
                                    "Selection node failed to pop a sub-query.".into(),
                                )
                            })?;
                            result.subqueries.push(sq_node);
                            let offset = result.subqueries.len() - 1;
                            sq_ref_map.insert(*sq_id, offset);
                        }
                        let child = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "Selection node must have exactly one child".into(),
                            )
                        })?;
                        current_node.children.push(child);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Selection node doesn't have any children".into(),
                        ));
                    }
                    let filter_id = ir.undo.get_oldest(filter).map_or_else(|| *filter, |id| *id);
                    let selection = ColExpr::new(ir, filter_id, &sq_ref_map)?;
                    let explain_node = match &node {
                        Relational::Selection { .. } => ExplainNode::Selection(selection),
                        Relational::Having { .. } => ExplainNode::Having(selection),
                        _ => return Err(SbroadError::DoSkip),
                    };
                    Some(explain_node)
                }
                Relational::UnionAll { .. } => {
                    if let (Some(right), Some(left)) = (stack.pop(), stack.pop()) {
                        current_node.children.push(left);
                        current_node.children.push(right);
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Union all node must have exactly two children".into(),
                        ));
                    }
                    Some(ExplainNode::UnionAll)
                }
                Relational::ScanSubQuery { alias, .. } => {
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "ScanSubQuery node must have exactly one child".into(),
                        )
                    })?;
                    current_node.children.push(child);
                    let s = SubQuery::new(alias.as_ref().map(ToString::to_string));
                    Some(ExplainNode::SubQuery(s))
                }
                Relational::Motion {
                    children, policy, ..
                } => {
                    let child = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Motion node must have exactly one child".into(),
                        )
                    })?;
                    current_node.children.push(child);

                    let collect_targets = |s: &IrMotionKey| -> Result<Vec<Target>, SbroadError> {
                        let child_id = children.first().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "current node should have exactly one child".to_string(),
                            )
                        })?;

                        let child_output_id = ir.get_relation_node(*child_id)?.output();
                        let col_list = ir.get_expression_node(child_output_id)?.get_row_list()?;

                        let targets = (s.targets)
                            .iter()
                            .map(|r| match r {
                                IrTarget::Reference(pos) => {
                                    let col_id = *col_list.get(*pos).ok_or_else(|| {
                                        SbroadError::NotFound(
                                            Entity::Target,
                                            format!("reference with position {pos}"),
                                        )
                                    })?;
                                    let col_name = ir
                                        .get_expression_node(col_id)?
                                        .get_alias_name()?
                                        .to_string();

                                    Ok::<Target, SbroadError>(Target::Reference(col_name))
                                }
                                IrTarget::Value(v) => Ok(Target::Value(v.clone())),
                            })
                            .collect::<Result<Vec<Target>, _>>()?;
                        Ok(targets)
                    };

                    let p = match policy {
                        IrMotionPolicy::None => MotionPolicy::None,
                        IrMotionPolicy::Segment(s) => {
                            let targets = collect_targets(s)?;
                            MotionPolicy::Segment(MotionKey { targets })
                        }
                        IrMotionPolicy::Full => MotionPolicy::Full,
                        IrMotionPolicy::Local => MotionPolicy::Local,
                        IrMotionPolicy::LocalSegment(s) => {
                            let targets = collect_targets(s)?;
                            MotionPolicy::LocalSegment(MotionKey { targets })
                        }
                    };
                    let m = Motion::new(p);

                    Some(ExplainNode::Motion(m))
                }
                Relational::Join {
                    children,
                    condition,
                    kind,
                    ..
                } => {
                    if children.len() < 2 {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Join must have at least two children".into(),
                        ));
                    }
                    let (_, subquery_ids) = children.split_at(2);
                    let mut sq_ref_map: SubQueryRefMap = HashMap::with_capacity(children.len() - 2);

                    for sq_id in subquery_ids.iter().rev() {
                        let sq_node = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "Join node failed to pop a sub-query.".into(),
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
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Join node must have exactly two children".into(),
                        ));
                    }

                    let condition = ColExpr::new(ir, *condition, &sq_ref_map)?;
                    Some(ExplainNode::InnerJoin(InnerJoin {
                        condition,
                        kind: kind.clone(),
                    }))
                }
                Relational::ValuesRow { data, children, .. } => {
                    let mut sq_ref_map: SubQueryRefMap = HashMap::with_capacity(children.len());

                    for sq_id in children.iter().rev() {
                        let sq_node = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "Insert node failed to pop a sub-query.".into(),
                            )
                        })?;

                        result.subqueries.push(sq_node);
                        let offset = result.subqueries.len() - 1;
                        sq_ref_map.insert(*sq_id, offset);
                    }

                    let row = ColExpr::new(ir, *data, &sq_ref_map)?;

                    Some(ExplainNode::ValueRow(row))
                }
                Relational::Values { children, .. } => {
                    let mut amount_values = children.len();

                    while amount_values > 0 {
                        let value_row = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "Insert node failed to pop a value row.".into(),
                            )
                        })?;

                        current_node.children.insert(0, value_row);
                        amount_values -= 1;
                    }
                    Some(ExplainNode::Value)
                }
                Relational::Insert {
                    relation,
                    conflict_strategy,
                    ..
                } => {
                    let values = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Insert node failed to pop a value row.".into(),
                        )
                    })?;

                    current_node.children.push(values);

                    Some(ExplainNode::Insert(
                        relation.into(),
                        conflict_strategy.clone(),
                    ))
                }
                Relational::Update { .. } => {
                    let values = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Insert node failed to pop a value row.".into(),
                        )
                    })?;

                    current_node.children.push(values);

                    Some(ExplainNode::Update(Update::new(ir, id)?))
                }
                Relational::Delete { relation, .. } => {
                    let values = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Delete node failed to pop a value row.".into(),
                        )
                    })?;

                    current_node.children.push(values);

                    Some(ExplainNode::Delete(relation.into()))
                }
            };
            stack.push(current_node);
        }
        result.main_query = stack
            .pop()
            .ok_or_else(|| SbroadError::NotFound(Entity::Node, "that is explain top".into()))?;
        Ok(result)
    }
}

impl Plan {
    /// Display ir explain
    ///
    /// # Errors
    /// - Failed to get top node
    /// - Failed to build explain
    pub fn as_explain(&self) -> Result<String, SbroadError> {
        let top_id = self.get_top()?;
        let explain = FullExplain::new(self, top_id)?;
        Ok(explain.to_string())
    }
}

#[cfg(test)]
mod tests;
