use std::collections::{HashMap, HashSet};

use ahash::AHashMap;
use tarantool::decimal::Decimal;

use crate::errors::{Action, Entity, SbroadError};
use crate::frontend::sql::ast::{ParseNode, Type};
use crate::ir::expression::Expression;
use crate::ir::helpers::RepeatableState;
use crate::ir::operator::{Arithmetic, Bool, Relational, Unary};
use crate::ir::tree::traversal::{PostOrder, EXPR_CAPACITY};
use crate::ir::value::double::Double;
use crate::ir::value::Value;
use crate::ir::{Node, Plan};

use super::Between;

impl Bool {
    /// Creates `Bool` from ast node type.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    #[allow(dead_code)]
    pub(super) fn from_node_type(s: &Type) -> Result<Self, SbroadError> {
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
            Type::NotIn => Ok(Bool::NotIn),
            _ => Err(SbroadError::Invalid(
                Entity::Operator,
                Some(format!("bool operator: {s:?}")),
            )),
        }
    }
}

impl Arithmetic {
    /// Creates `Arithmetic` from ast node type.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    #[allow(dead_code)]
    pub(super) fn from_node_type(s: &Type) -> Result<Self, SbroadError> {
        match s {
            Type::Multiply => Ok(Arithmetic::Multiply),
            Type::Add => Ok(Arithmetic::Add),
            Type::Divide => Ok(Arithmetic::Divide),
            Type::Subtract => Ok(Arithmetic::Subtract),
            _ => Err(SbroadError::Invalid(
                Entity::Operator,
                Some(format!("Arithmetic: {s:?}")),
            )),
        }
    }
}

impl Unary {
    /// Creates `Unary` from ast node type.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    #[allow(dead_code)]
    pub(super) fn from_node_type(s: &Type) -> Result<Self, SbroadError> {
        match s {
            Type::IsNull => Ok(Unary::IsNull),
            Type::IsNotNull => Ok(Unary::IsNotNull),
            Type::Exists => Ok(Unary::Exists),
            Type::NotExists => Ok(Unary::NotExists),
            _ => Err(SbroadError::Invalid(
                Entity::Operator,
                Some(format!("unary operator: {s:?}")),
            )),
        }
    }
}

impl Value {
    /// Creates `Value` from ast node type and text.
    ///
    /// # Errors
    /// Returns `SbroadError` when the operator is invalid.
    #[allow(dead_code)]
    pub(super) fn from_node(s: &ParseNode) -> Result<Self, SbroadError> {
        let val = match &s.value {
            Some(v) => v.clone(),
            None => String::new(),
        };

        match s.rule {
            Type::False => Ok(false.into()),
            Type::Null => Ok(Value::Null),
            Type::Integer => Ok(val
                .parse::<i64>()
                .map_err(|e| {
                    SbroadError::ParsingError(Entity::Value, format!("i64 parsing error {e}"))
                })?
                .into()),
            Type::Decimal => Ok(val
                .parse::<Decimal>()
                .map_err(|e| {
                    SbroadError::ParsingError(Entity::Value, format!("decimal parsing error {e:?}"))
                })?
                .into()),
            Type::Double => Ok(val
                .parse::<Double>()
                .map_err(|e| {
                    SbroadError::ParsingError(Entity::Value, format!("double parsing error {e}"))
                })?
                .into()),
            Type::Unsigned => Ok(val
                .parse::<u64>()
                .map_err(|e| {
                    SbroadError::ParsingError(Entity::Value, format!("u64 parsing error {e}"))
                })?
                .into()),
            Type::String => Ok(val.into()),
            Type::True => Ok(true.into()),
            _ => Err(SbroadError::Unsupported(
                Entity::Type,
                Some("can not create Value from ParseNode".into()),
            )),
        }
    }
}

#[derive(Debug)]
/// Helper struct representing map of { `ParseNode` id -> `Node` id }
pub(super) struct Translation {
    map: HashMap<usize, usize>,
}

impl Translation {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Translation {
            map: HashMap::with_capacity(capacity),
        }
    }

    pub(super) fn add(&mut self, parse_id: usize, plan_id: usize) {
        self.map.insert(parse_id, plan_id);
    }

    pub(super) fn get(&self, old: usize) -> Result<usize, SbroadError> {
        self.map.get(&old).copied().ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(parse node) [{old}] in translation map"),
            )
        })
    }
}

#[derive(Hash, PartialEq, Debug)]
struct SubQuery {
    relational: usize,
    operator: usize,
    sq: usize,
}
impl Eq for SubQuery {}

impl SubQuery {
    fn new(relational: usize, operator: usize, sq: usize) -> SubQuery {
        SubQuery {
            relational,
            operator,
            sq,
        }
    }
}

impl Plan {
    fn gather_sq_for_replacement(&self) -> Result<HashSet<SubQuery, RepeatableState>, SbroadError> {
        let mut set: HashSet<SubQuery, RepeatableState> = HashSet::with_hasher(RepeatableState);
        // Traverse expression trees of the selection and join nodes.
        // Gather all sub-queries in the boolean expressions there.
        for (id, node) in self.nodes.iter().enumerate() {
            match node {
                Node::Relational(
                    Relational::Selection { filter: tree, .. }
                    | Relational::Join {
                        condition: tree, ..
                    }
                    | Relational::Having { filter: tree, .. },
                ) => {
                    let capacity = self.nodes.len();
                    let mut expr_post = PostOrder::with_capacity(
                        |node| self.nodes.expr_iter(node, false),
                        capacity,
                    );
                    for (_, op_id) in expr_post.iter(*tree) {
                        let expression_node = self.get_node(op_id)?;
                        if let Node::Expression(Expression::Bool { left, right, .. }) =
                            expression_node
                        {
                            let children = &[*left, *right];
                            for child in children {
                                if let Node::Relational(Relational::ScanSubQuery { .. }) =
                                    self.get_node(*child)?
                                {
                                    set.insert(SubQuery::new(id, op_id, *child));
                                }
                            }
                        } else if let Node::Expression(Expression::Unary { child, .. }) =
                            expression_node
                        {
                            if let Node::Relational(Relational::ScanSubQuery { .. }) =
                                self.get_node(*child)?
                            {
                                set.insert(SubQuery::new(id, op_id, *child));
                            }
                        }
                    }
                }
                _ => continue,
            }
        }
        Ok(set)
    }

    /// Replace sub-queries with references to the sub-query.
    pub(super) fn replace_sq_with_references(
        &mut self,
    ) -> Result<AHashMap<usize, usize>, SbroadError> {
        let set = self.gather_sq_for_replacement()?;
        let mut replaces: AHashMap<usize, usize> = AHashMap::with_capacity(set.len());
        for sq in set {
            // Append sub-query to relational node if it is not already there (can happen with BETWEEN).
            match self.get_mut_node(sq.relational)? {
                Node::Relational(
                    Relational::Selection { children, .. }
                    | Relational::Join { children, .. }
                    | Relational::Having { children, .. },
                ) => {
                    // O(n) can become a problem.
                    if !children.contains(&sq.sq) {
                        children.push(sq.sq);
                    }
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Relational,
                        Some("Sub-query is not in selection or join node".into()),
                    ))
                }
            }

            // Generate a reference to the sub-query.
            let row_id: usize = match self.get_node(sq.relational)? {
                Node::Relational(
                    Relational::Selection { children, .. }
                    | Relational::Join { children, .. }
                    | Relational::Having { children, .. },
                ) => {
                    let nodes = children.clone();
                    let sq_output = self.get_relation_node(sq.sq)?.output();
                    let row = self.get_expression_node(sq_output)?;
                    if let Expression::Row { list, .. } = row {
                        let mut names: Vec<String> = Vec::new();
                        for col_id in list {
                            names.push(self.get_expression_node(*col_id)?.get_alias_name()?.into());
                        }
                        let names_str: Vec<_> = names.iter().map(String::as_str).collect();
                        // TODO: should we add current row_id to the set of the generated rows?
                        let position: usize =
                            children.iter().position(|&x| x == sq.sq).ok_or_else(|| {
                                SbroadError::FailedTo(
                                    Action::Build,
                                    None,
                                    "a reference to the sub-query".into(),
                                )
                            })?;
                        let row_id = self.add_row_from_sub_query(&nodes, position, &names_str)?;
                        self.replace_parent_in_subtree(row_id, None, Some(sq.relational))?;
                        row_id
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Expression,
                            Some("Sub-query output is not a row".into()),
                        ));
                    }
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Node,
                        Some("Sub-query is not in selection or join node".into()),
                    ))
                }
            };

            // Replace sub-query with reference.
            let op = self.get_mut_expression_node(sq.operator)?;
            if let Expression::Bool {
                ref mut left,
                ref mut right,
                ..
            } = op
            {
                if *left == sq.sq {
                    *left = row_id;
                } else if *right == sq.sq {
                    *right = row_id;
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some("Sub-query is not a left or right operand".into()),
                    ));
                }
                replaces.insert(sq.sq, row_id);
            } else if let Expression::Unary { child, .. } = op {
                *child = row_id;
                replaces.insert(sq.sq, row_id);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("Sub-query is not in a boolean expression".into()),
                ));
            }
        }
        Ok(replaces)
    }

    /// Resolve the double linking problem in BETWEEN operator. On the AST to IR step
    /// we transform `left BETWEEN center AND right` construction into
    /// `left >= center AND left <= right`, where the same `left` expression is reused
    /// twice. So, We need to copy the 'left' expression tree from `left >= center` to the
    /// `left <= right` expression.
    ///
    /// Otherwise we'll have problems on the dispatch stage while taking nodes from the original
    /// plan to build a sub-plan for the storage. If the same `left` subtree is used twice in
    /// the plan, these nodes are taken while traversing the `left >= center` expression and
    /// nothing is left for the `left <= right` sutree.
    pub(super) fn fix_betweens(
        &mut self,
        betweens: &[Between],
        replaces: &AHashMap<usize, usize>,
    ) -> Result<(), SbroadError> {
        for between in betweens {
            let left_id: usize = if let Some(id) = replaces.get(&between.left_id) {
                self.clone_expr_subtree(*id)?
            } else {
                self.clone_expr_subtree(between.left_id)?
            };
            let less_eq_expr = self.get_mut_expression_node(between.less_eq_id)?;
            if let Expression::Bool { ref mut left, .. } = less_eq_expr {
                *left = left_id;
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some("expected a boolean Expression".into()),
                ));
            }
        }
        Ok(())
    }

    // It is necessary to keep parentheses from original sql query
    // We mark this arithmetic expressions to add in the future `()` as a part of SyntaxData
    pub(super) fn fix_arithmetic_parentheses(
        &mut self,
        arithmetic_expression_ids: &[usize],
    ) -> Result<(), SbroadError> {
        for id in arithmetic_expression_ids {
            self.nodes.set_arithmetic_node_parentheses(*id, true)?;
        }
        Ok(())
    }

    pub(crate) fn clone_expr_subtree(&mut self, top_id: usize) -> Result<usize, SbroadError> {
        let mut map = HashMap::new();
        let mut subtree =
            PostOrder::with_capacity(|node| self.nodes.expr_iter(node, false), EXPR_CAPACITY);
        subtree.populate_nodes(top_id);
        let nodes = subtree.take_nodes();
        for (_, id) in nodes {
            let next_id = self.nodes.next_id();
            let mut expr = self.get_expression_node(id)?.clone();
            match expr {
                Expression::Constant { .. }
                | Expression::Reference { .. }
                | Expression::CountAsterisk => {}
                Expression::Alias { ref mut child, .. }
                | Expression::Cast { ref mut child, .. }
                | Expression::Unary { ref mut child, .. } => {
                    *child = *map.get(child).ok_or_else(|| {
                        SbroadError::NotFound(Entity::SubTree, format!("(id {id})"))
                    })?;
                }
                Expression::Bool {
                    ref mut left,
                    ref mut right,
                    ..
                }
                | Expression::Arithmetic {
                    ref mut left,
                    ref mut right,
                    ..
                }
                | Expression::Concat {
                    ref mut left,
                    ref mut right,
                    ..
                } => {
                    *left = *map.get(left).ok_or_else(|| {
                        SbroadError::NotFound(Entity::SubTree, format!("(id {id})"))
                    })?;
                    *right = *map.get(right).ok_or_else(|| {
                        SbroadError::NotFound(Entity::SubTree, format!("(id {id})"))
                    })?;
                }
                Expression::Row {
                    list: ref mut children,
                    ..
                }
                | Expression::StableFunction {
                    ref mut children, ..
                } => {
                    for child in children {
                        *child = *map.get(child).ok_or_else(|| {
                            SbroadError::NotFound(Entity::SubTree, format!("(id {id})"))
                        })?;
                    }
                }
            }
            self.nodes.push(Node::Expression(expr));
            map.insert(id, next_id);
        }
        Ok(self.nodes.next_id() - 1)
    }
}

#[cfg(test)]
mod tests;
