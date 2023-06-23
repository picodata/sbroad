use std::collections::HashMap;
use std::fmt::{Display, Formatter, Write as _};

use itertools::Itertools;
use serde::Serialize;

use crate::errors::{Entity, SbroadError};
use crate::ir::expression::cast::Type as CastType;
use crate::ir::expression::Expression;
use crate::ir::operator::{JoinKind, Relational};
use crate::ir::relation::Type;
use crate::ir::transformation::redistribution::{
    MotionKey as IrMotionKey, MotionPolicy as IrMotionPolicy, Target as IrTarget,
};
use crate::ir::Plan;

use super::operator::{Arithmetic, Bool, Unary};
use super::tree::traversal::{PostOrder, EXPR_CAPACITY, REL_CAPACITY};
use super::value::Value;

#[derive(Debug, Serialize)]
enum ColExpr {
    Alias(Box<ColExpr>, String),
    Arithmetic(Box<ColExpr>, BinaryOp, Box<ColExpr>, bool),
    Column(String, Type),
    Cast(Box<ColExpr>, CastType),
    Concat(Box<ColExpr>, Box<ColExpr>),
    StableFunction(String, Box<ColExpr>, bool, Type),
    Row(Vec<ColExpr>),
    None,
}

impl Display for ColExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            ColExpr::Alias(expr, name) => format!("({expr} -> {name})"),
            ColExpr::Arithmetic(left, op, right, with_parentheses) => match with_parentheses {
                false => format!("{left} {op} {right}"),
                true => format!("({left} {op} {right})"),
            },
            ColExpr::Column(c, col_type) => format!("{c}::{col_type}"),
            ColExpr::Cast(v, t) => format!("{v}::{t}"),
            ColExpr::Concat(l, r) => format!("{l} || {r}"),
            ColExpr::StableFunction(name, arg, is_distinct, func_type) => format!(
                "{name}({}{arg})::{func_type}",
                if *is_distinct { "distinct " } else { "" }
            ),
            ColExpr::Row(list) => format!("({})", list.iter().format(", ")),
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
    fn new(plan: &Plan, subtree_top: usize) -> Result<Self, SbroadError> {
        let mut stack: Vec<ColExpr> = Vec::new();
        let mut dft_post =
            PostOrder::with_capacity(|node| plan.nodes.expr_iter(node, false), EXPR_CAPACITY);

        for (_, id) in dft_post.iter(subtree_top) {
            let current_node = plan.get_expression_node(id)?;

            match &current_node {
                Expression::Cast { to, .. } => {
                    let expr = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing CAST expression".to_string(),
                        )
                    })?;
                    let cast_expr = ColExpr::Cast(Box::new(expr), to.clone());
                    stack.push(cast_expr);
                }
                Expression::CountAsterisk => {
                    stack.push(ColExpr::Column(
                        "*".to_string(),
                        current_node.get_type(plan)?,
                    ));
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

                    stack.push(ColExpr::Column(col_name, current_node.get_type(plan)?));
                }
                Expression::Concat { .. } => {
                    let right = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing CONCAT expression".to_string(),
                        )
                    })?;
                    let left = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing CONCAT expression".to_string(),
                        )
                    })?;
                    let concat_expr = ColExpr::Concat(Box::new(left), Box::new(right));
                    stack.push(concat_expr);
                }
                Expression::Constant { value } => {
                    let expr = ColExpr::Column(value.to_string(), current_node.get_type(plan)?);
                    stack.push(expr);
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
                        let arg = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                format!("stack is empty, expected to pop {len} element while processing STABLE FUNCTION expression"),
                            )
                        })?;
                        args.push(arg);
                        len -= 1;
                    }
                    args.reverse();
                    let args_expr = ColExpr::Row(args);
                    let func_expr = ColExpr::StableFunction(
                        name.clone(),
                        Box::new(args_expr),
                        *is_distinct,
                        func_type.clone(),
                    );
                    stack.push(func_expr);
                }
                Expression::Row { list, .. } => {
                    let mut len = list.len();
                    let mut row: Vec<ColExpr> = Vec::with_capacity(len);
                    while len > 0 {
                        let expr = stack.pop().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                format!("stack is empty, expected to pop {len} element while processing ROW expression"),
                            )
                        })?;
                        row.push(expr);
                        len -= 1;
                    }
                    let row_expr = ColExpr::Row(row);
                    stack.push(row_expr);
                }
                Expression::Arithmetic {
                    left: _,
                    op,
                    right: _,
                    with_parentheses,
                } => {
                    let right = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing ARITHMETIC expression".to_string(),
                        )
                    })?;

                    let left = stack.pop().ok_or_else(|| {
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

                    stack.push(ar_expr);
                }
                Expression::Alias { name, .. } => {
                    let expr = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "stack is empty while processing ALIAS expression".to_string(),
                        )
                    })?;
                    let alias_expr = ColExpr::Alias(Box::new(expr), name.clone());
                    stack.push(alias_expr);
                }
                Expression::Bool { .. } | Expression::Unary { .. } => {
                    return Err(SbroadError::Unsupported(
                        Entity::Expression,
                        Some(format!(
                            "Column expression node [{current_node:?}] is not supported for yet"
                        )),
                    ));
                }
            }
        }

        stack
            .pop()
            .ok_or_else(|| SbroadError::UnexpectedNumberOfValues("stack is empty".to_string()))
    }
}

#[derive(Debug, Serialize, Default)]
struct Col {
    /// Column alias from sql query
    alias: Option<String>,

    /// Column expression (e.g. column name, function, etc.)
    col: ColExpr,
}

impl Col {
    #[allow(dead_code)]
    fn new(plan: &Plan, subtree_top: usize) -> Result<Self, SbroadError> {
        let mut column = Col::default();

        let mut dft_post =
            PostOrder::with_capacity(|node| plan.nodes.expr_iter(node, true), EXPR_CAPACITY);
        for (_, id) in dft_post.iter(subtree_top) {
            let current_node = plan.get_expression_node(id)?;

            if let Expression::Alias { name, .. } = &current_node {
                column.alias = Some(name.to_string());
            } else {
                column.col = ColExpr::new(plan, id)?;
            }
        }

        Ok(column)
    }
}

impl Display for Col {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from(&self.col);

        if let Some(a) = &self.alias {
            write!(s, " -> {a}")?;
        }

        write!(f, "{s}")
    }
}

#[derive(Debug, Serialize)]
struct Projection {
    /// List of colums in sql query
    cols: Vec<Col>,
}

impl Projection {
    #[allow(dead_code)]
    fn new(plan: &Plan, output_id: usize) -> Result<Self, SbroadError> {
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

        write!(s, "({cols})")?;
        write!(f, "{s}")
    }
}

#[derive(Debug, Serialize)]
struct GroupBy {
    /// List of colums in sql query
    gr_cols: Vec<Col>,
    output_cols: Vec<Col>,
}

impl GroupBy {
    #[allow(dead_code)]
    fn new(plan: &Plan, gr_cols: &Vec<usize>, output_id: usize) -> Result<Self, SbroadError> {
        let mut result = GroupBy {
            gr_cols: vec![],
            output_cols: vec![],
        };

        for col_node_id in gr_cols {
            let col = Col::new(plan, *col_node_id)?;
            result.gr_cols.push(col);
        }
        let alias_list = plan.get_expression_node(output_id)?;
        for col_node_id in alias_list.get_row_list()? {
            let col = Col::new(plan, *col_node_id)?;
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
    Const(Value),
    Column(Col),
    SqRef(Ref),
}

impl Display for RowVal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            RowVal::Const(c) => format!("{c}::{}", c.get_type()),
            RowVal::Column(c) => c.to_string(),
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

    fn from_ir_nodes(
        plan: &Plan,
        node_ids: &[usize],
        ref_map: &HashMap<usize, usize>,
    ) -> Result<Self, SbroadError> {
        let mut row = Row::new();

        for child in node_ids {
            let current_node = plan.get_expression_node(*child)?;

            match &current_node {
                Expression::Constant { value, .. } => {
                    row.add_col(RowVal::Const(value.clone()));
                }
                Expression::Reference { .. } => {
                    let rel_id: usize = *plan.get_relational_from_reference_node(*child)?;

                    let rel_node = plan.get_relation_node(rel_id)?;
                    if plan.is_additional_child(rel_id)? {
                        if let Relational::ScanSubQuery { .. } | Relational::Motion { .. } =
                            rel_node
                        {
                            let sq_offset = ref_map.get(&rel_id).ok_or_else(|| {
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
                        let col = Col::new(plan, *child)?;
                        row.add_col(RowVal::Column(col));
                    }
                }
                Expression::Bool { .. }
                | Expression::Arithmetic { .. }
                | Expression::Cast { .. }
                | Expression::Concat { .. }
                | Expression::StableFunction { .. }
                | Expression::Row { .. }
                | Expression::Alias { .. }
                | Expression::Unary { .. } => {
                    let col = Col::new(plan, *child)?;
                    row.add_col(RowVal::Column(col));
                }
                Expression::CountAsterisk => {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some("CountAsterisk can't be present among Row children!".into()),
                    ))
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

        write!(f, "ROW({cols})")
    }
}

#[derive(Debug, Serialize)]
enum BinaryOp {
    ArithOp(Arithmetic),
    BoolOp(Bool),
}
/// Recursive type which describe `WHERE` cause in explain
#[derive(Debug, Serialize)]
enum Selection {
    Row(Row),
    BinaryOp {
        left: Box<Selection>,
        op: BinaryOp,
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
    ) -> Result<Self, SbroadError> {
        let current_node = plan.get_expression_node(subtree_node_id)?;

        let result = match current_node {
            Expression::Bool { left, op, right } => Selection::BinaryOp {
                left: Box::new(Selection::new(plan, *left, ref_map)?),
                op: BinaryOp::BoolOp(op.clone()),
                right: Box::new(Selection::new(plan, *right, ref_map)?),
            },
            Expression::Arithmetic {
                left, op, right, ..
            } => Selection::BinaryOp {
                left: Box::new(Selection::new(plan, *left, ref_map)?),
                op: BinaryOp::ArithOp(op.clone()),
                right: Box::new(Selection::new(plan, *right, ref_map)?),
            },
            Expression::Row { list, .. } => {
                let row = Row::from_ir_nodes(plan, list, ref_map)?;
                Selection::Row(row)
            }
            Expression::Unary { op, child } => Selection::UnaryOp {
                op: op.clone(),
                child: Box::new(Selection::new(plan, *child, ref_map)?),
            },
            Expression::Reference { .. }
            | Expression::Cast { .. }
            | Expression::StableFunction { .. }
            | Expression::Concat { .. }
            | Expression::Constant { .. }
            | Expression::Alias { .. } => {
                let row = Row::from_ir_nodes(plan, &[subtree_node_id], ref_map)?;
                Selection::Row(row)
            }
            Expression::CountAsterisk => {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some("CountAsterisk can't be present in Selection filter!".into()),
                ))
            }
        };

        Ok(result)
    }
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

impl Display for Selection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            Selection::Row(r) => r.to_string(),
            Selection::BinaryOp { left, op, right } => {
                format!("{left} {op} {right}")
            }
            Selection::UnaryOp { op, child } => match op {
                Unary::IsNull | Unary::IsNotNull => format!("{child} {op}"),
                Unary::Exists | Unary::NotExists => format!("{op} {child}"),
            },
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
    Full,
    Segment(MotionKey),
    Local,
    LocalSegment(MotionKey),
}

impl Display for MotionPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
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
    condition: Selection,
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
    Except,
    GroupBy(GroupBy),
    InnerJoin(InnerJoin),
    ValueRow(Row),
    Value,
    Insert(String),
    Projection(Projection),
    Scan(Scan),
    Selection(Selection),
    Having(Selection),
    UnionAll,
    SubQuery(SubQuery),
    Motion(Motion),
}

impl Display for ExplainNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match &self {
            ExplainNode::Except => "except".to_string(),
            ExplainNode::InnerJoin(i) => i.to_string(),
            ExplainNode::ValueRow(r) => format!("value row (data={r})"),
            ExplainNode::Value => "values".to_string(),
            ExplainNode::Insert(s) => format!("insert {s}"),
            ExplainNode::Projection(e) => e.to_string(),
            ExplainNode::GroupBy(p) => p.to_string(),
            ExplainNode::Scan(s) => s.to_string(),
            ExplainNode::Selection(s) => format!("selection {s}"),
            ExplainNode::Having(s) => format!("having {s}"),
            ExplainNode::UnionAll => "union all".to_string(),
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
}

impl Display for FullExplain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = self.main_query.to_string();

        for (pos, sq) in self.subqueries.iter().enumerate() {
            writeln!(s, "subquery ${pos}:")?;
            s.push_str(&sq.to_string());
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
                    let p = GroupBy::new(ir, gr_cols, *output)?;
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
                }
                | Relational::Having {
                    children, filter, ..
                } => {
                    let mut sq_ref_map: HashMap<usize, usize> =
                        HashMap::with_capacity(children.len() - 1);
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
                    let s = Selection::new(ir, filter_id, &sq_ref_map)?;
                    let explain_node = match &node {
                        Relational::Selection { .. } => ExplainNode::Selection(s),
                        Relational::Having { .. } => ExplainNode::Having(s),
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
                    let mut sq_ref_map: HashMap<usize, usize> =
                        HashMap::with_capacity(children.len() - 2);

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

                    let condition = Selection::new(ir, *condition, &sq_ref_map)?;
                    Some(ExplainNode::InnerJoin(InnerJoin {
                        condition,
                        kind: kind.clone(),
                    }))
                }
                Relational::ValuesRow { data, children, .. } => {
                    let mut sq_ref_map: HashMap<usize, usize> =
                        HashMap::with_capacity(children.len());

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

                    let values = ir.get_expression_node(*data)?.get_row_list()?;
                    let row = Row::from_ir_nodes(ir, values, &sq_ref_map)?;

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
                Relational::Insert { relation, .. } => {
                    let values = stack.pop().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Insert node failed to pop a value row.".into(),
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
