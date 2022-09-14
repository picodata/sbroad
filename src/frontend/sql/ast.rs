//! Abstract syntax tree (AST) module.
//!
//! This module contains a definition of the abstract syntax tree
//! constructed from the nodes of the `pest` tree iterator nodes.

extern crate pest;

use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::mem::swap;

use pest::iterators::Pair;
use serde::{Deserialize, Serialize};
use traversal::DftPost;

use crate::errors::QueryPlannerError;

/// Parse tree
#[derive(Parser)]
#[grammar = "frontend/sql/query.pest"]
pub(super) struct ParseTree;

/// A list of current rules from the actual grammar.
/// When new tokens are added to the grammar they
/// should be also added in the current list.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Type {
    Alias,
    AliasName,
    And,
    Asterisk,
    Between,
    Column,
    ColumnName,
    Condition,
    Decimal,
    Double,
    Eq,
    Except,
    Explain,
    False,
    Gt,
    GtEq,
    In,
    InnerJoin,
    Insert,
    Integer,
    IsNull,
    IsNotNull,
    Lt,
    LtEq,
    Name,
    NotEq,
    NotIn,
    Null,
    Or,
    Parameter,
    Parentheses,
    Primary,
    Projection,
    Reference,
    Row,
    Scan,
    ScanName,
    Select,
    Selection,
    String,
    SubQuery,
    SubQueryName,
    Table,
    TargetColumns,
    True,
    UnionAll,
    Unsigned,
    Value,
    Values,
    ValuesRow,
}

impl Type {
    #[allow(dead_code)]
    fn from_rule(rule: Rule) -> Result<Self, QueryPlannerError> {
        match rule {
            Rule::Alias => Ok(Type::Alias),
            Rule::AliasName => Ok(Type::AliasName),
            Rule::And => Ok(Type::And),
            Rule::Asterisk => Ok(Type::Asterisk),
            Rule::Between => Ok(Type::Between),
            Rule::Column => Ok(Type::Column),
            Rule::ColumnName => Ok(Type::ColumnName),
            Rule::Condition => Ok(Type::Condition),
            Rule::Decimal => Ok(Type::Decimal),
            Rule::Double => Ok(Type::Double),
            Rule::Eq => Ok(Type::Eq),
            Rule::Except => Ok(Type::Except),
            Rule::Explain => Ok(Type::Explain),
            Rule::False => Ok(Type::False),
            Rule::Gt => Ok(Type::Gt),
            Rule::GtEq => Ok(Type::GtEq),
            Rule::In => Ok(Type::In),
            Rule::InnerJoin => Ok(Type::InnerJoin),
            Rule::Integer => Ok(Type::Integer),
            Rule::Insert => Ok(Type::Insert),
            Rule::IsNull => Ok(Type::IsNull),
            Rule::IsNotNull => Ok(Type::IsNotNull),
            Rule::Lt => Ok(Type::Lt),
            Rule::LtEq => Ok(Type::LtEq),
            Rule::Name => Ok(Type::Name),
            Rule::NotEq => Ok(Type::NotEq),
            Rule::NotIn => Ok(Type::NotIn),
            Rule::Null => Ok(Type::Null),
            Rule::Or => Ok(Type::Or),
            Rule::Parameter => Ok(Type::Parameter),
            Rule::Parentheses => Ok(Type::Parentheses),
            Rule::Primary => Ok(Type::Primary),
            Rule::Projection => Ok(Type::Projection),
            Rule::Reference => Ok(Type::Reference),
            Rule::Row => Ok(Type::Row),
            Rule::Scan => Ok(Type::Scan),
            Rule::ScanName => Ok(Type::ScanName),
            Rule::Select => Ok(Type::Select),
            Rule::Selection => Ok(Type::Selection),
            Rule::String => Ok(Type::String),
            Rule::SubQuery => Ok(Type::SubQuery),
            Rule::Table => Ok(Type::Table),
            Rule::TargetColumns => Ok(Type::TargetColumns),
            Rule::True => Ok(Type::True),
            Rule::UnionAll => Ok(Type::UnionAll),
            Rule::Unsigned => Ok(Type::Unsigned),
            Rule::Value => Ok(Type::Value),
            Rule::Values => Ok(Type::Values),
            Rule::ValuesRow => Ok(Type::ValuesRow),
            _ => Err(QueryPlannerError::InvalidAst),
        }
    }
}

/// Parse node is a wrapper over the pest pair.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ParseNode {
    pub(in crate::frontend::sql) children: Vec<usize>,
    pub(in crate::frontend::sql) rule: Type,
    pub(in crate::frontend::sql) value: Option<String>,
}

#[allow(dead_code)]
impl ParseNode {
    pub(super) fn new(rule: Rule, value: Option<String>) -> Result<Self, QueryPlannerError> {
        Ok(ParseNode {
            children: vec![],
            rule: Type::from_rule(rule)?,
            value,
        })
    }
}

/// A storage arena of the parse nodes
/// (a node position in the arena vector acts like a reference).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ParseNodes {
    pub(crate) arena: Vec<ParseNode>,
}

impl Default for ParseNodes {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl ParseNodes {
    /// Get a node from arena
    ///
    /// # Errors
    /// - Failed to get a node from arena.
    pub fn get_node(&self, node: usize) -> Result<&ParseNode, QueryPlannerError> {
        self.arena.get(node).ok_or(QueryPlannerError::InvalidNode)
    }

    /// Get a mutable node from arena
    ///
    /// # Errors
    /// - Failed to get a node from arena.
    pub fn get_mut_node(&mut self, node: usize) -> Result<&mut ParseNode, QueryPlannerError> {
        self.arena
            .get_mut(node)
            .ok_or(QueryPlannerError::InvalidNode)
    }

    /// Push a new node to arena
    pub fn push_node(&mut self, node: ParseNode) -> usize {
        let id = self.next_id();
        self.arena.push(node);
        id
    }

    /// Get next node id
    #[must_use]
    pub fn next_id(&self) -> usize {
        self.arena.len()
    }

    /// Constructor
    #[must_use]
    pub fn new() -> Self {
        ParseNodes { arena: Vec::new() }
    }

    /// Adds children to already existing node.
    /// New elements are added to the beginning of the current list
    /// as we use inverted node order.
    ///
    /// # Errors
    /// - Failed to retrieve node from arena.
    pub fn add_child(
        &mut self,
        node: Option<usize>,
        child: usize,
    ) -> Result<(), QueryPlannerError> {
        if let Some(parent) = node {
            self.get_node(child)?;
            let parent_node = self
                .arena
                .get_mut(parent)
                .ok_or(QueryPlannerError::InvalidNode)?;
            parent_node.children.insert(0, child);
        }
        Ok(())
    }

    /// Update node's value (string from pairs)
    ///
    /// # Errors
    /// - Target node is present in the arena.
    pub fn update_value(
        &mut self,
        node: usize,
        value: Option<String>,
    ) -> Result<(), QueryPlannerError> {
        let mut node = self
            .arena
            .get_mut(node)
            .ok_or(QueryPlannerError::InvalidNode)?;
        node.value = value;
        Ok(())
    }
}

/// A wrapper over the pair to keep its parent as well.
pub(super) struct StackParseNode<'n> {
    pub(super) parent: Option<usize>,
    pub(super) pair: Pair<'n, Rule>,
}

impl<'n> StackParseNode<'n> {
    /// Constructor
    pub(super) fn new(pair: Pair<'n, Rule>, parent: Option<usize>) -> Self {
        StackParseNode { parent, pair }
    }
}

/// AST is a tree build on the top of the parse nodes arena.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AbstractSyntaxTree {
    pub(in crate::frontend::sql) nodes: ParseNodes,
    pub(in crate::frontend::sql) top: Option<usize>,
    pub(super) map: HashMap<usize, Vec<usize>>,
}

impl PartialEq for AbstractSyntaxTree {
    fn eq(&self, other: &Self) -> bool {
        self.nodes == other.nodes && self.top == other.top && self.map == other.map
    }
}

#[allow(dead_code)]
impl AbstractSyntaxTree {
    /// Set the top of AST.
    ///
    /// # Errors
    /// - The new top is not in the arena.
    pub fn set_top(&mut self, top: usize) -> Result<(), QueryPlannerError> {
        self.nodes.get_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get the top of AST.
    ///
    /// # Errors
    /// - AST tree doesn't have a top node.
    pub fn get_top(&self) -> Result<usize, QueryPlannerError> {
        self.top
            .ok_or_else(|| QueryPlannerError::CustomError("No top node found in AST".to_string()))
    }

    /// Serialize AST from YAML.
    ///
    /// # Errors
    /// - Failed to parse YAML.
    pub fn from_yaml(s: &str) -> Result<Self, QueryPlannerError> {
        let ast: AbstractSyntaxTree = match serde_yaml::from_str(s) {
            Ok(p) => p,
            Err(_) => return Err(QueryPlannerError::Serialization),
        };
        Ok(ast)
    }

    /// `Select` node is not IR-friendly as it can have up to five children.
    /// Transform this node in IR-way (to a binary sub-tree).
    pub(super) fn transform_select(&mut self) -> Result<(), QueryPlannerError> {
        let mut selects: HashSet<usize> = HashSet::new();
        for (id, node) in self.nodes.arena.iter().enumerate() {
            if node.rule == Type::Select {
                selects.insert(id);
            }
        }
        for node in &selects {
            let select = self.nodes.get_node(*node)?;
            let children: Vec<usize> = select.children.clone();
            match children.len() {
                2 => self.transform_select_2(*node, &children)?,
                3 => self.transform_select_3(*node, &children)?,
                4 => self.transform_select_4(*node, &children)?,
                5 => self.transform_select_5(*node, &children)?,
                _ => return Err(QueryPlannerError::InvalidAst),
            }
        }

        // Collect select nodes' parents.
        let mut parents: Vec<(usize, usize)> = Vec::new();
        for (id, node) in self.nodes.arena.iter().enumerate() {
            for (children_pos, child_id) in node.children.iter().enumerate() {
                if selects.contains(child_id) {
                    parents.push((id, children_pos));
                }
            }
        }
        // Remove select nodes that have parents.
        for (parent_id, children_pos) in parents {
            let parent = self.nodes.get_node(parent_id)?;
            let child_id = *parent.children.get(children_pos).ok_or_else(|| {
                QueryPlannerError::CustomError(
                    "Parent node doesn't contain a child at expected position.".into(),
                )
            })?;
            let child = self.nodes.get_node(child_id)?;
            let mut node_id = *child.children.first().ok_or_else(|| {
                QueryPlannerError::CustomError(
                    "Selection node doesn't contain any children.".into(),
                )
            })?;
            let parent = self.nodes.get_mut_node(parent_id)?;
            swap(&mut parent.children[children_pos], &mut node_id);
        }
        // Remove select if it is a top node.
        let top_id = self.get_top()?;
        if selects.contains(&top_id) {
            let top = self.nodes.get_node(top_id)?;
            let child_id = *top.children.first().ok_or_else(|| {
                QueryPlannerError::CustomError(
                    "Selection node doesn't contain any children.".into(),
                )
            })?;
            self.set_top(child_id)?;
        }
        Ok(())
    }

    /// Transforms `Select` with `Projection` and `Scan`
    fn transform_select_2(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), QueryPlannerError> {
        if children.len() != 2 {
            return Err(QueryPlannerError::InvalidInput);
        }

        // Check that the second child is `Scan`.
        let scan_id: usize = *children.get(1).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let scan = self.nodes.get_node(scan_id)?;
        if scan.rule != Type::Scan {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Check that the first child is `Projection`.
        let proj_id: usize = *children.first().ok_or(QueryPlannerError::ValueOutOfRange)?;
        let proj = self
            .nodes
            .arena
            .get_mut(proj_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        if proj.rule != Type::Projection {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Append `Scan` to the `Projection` children (zero position)
        proj.children.insert(0, scan_id);

        // Leave `Projection` the only child of `Select`.
        let mut select = self
            .nodes
            .arena
            .get_mut(select_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        select.children = vec![proj_id];

        Ok(())
    }

    /// Transforms `Select` with `Projection`, `Scan` and `Selection`.
    fn transform_select_3(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), QueryPlannerError> {
        if children.len() != 3 {
            return Err(QueryPlannerError::InvalidInput);
        }

        // Check that the second child is `Scan`.
        let scan_id: usize = *children.get(1).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let scan = self.nodes.get_node(scan_id)?;
        if scan.rule != Type::Scan {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Check that the third child is `Selection`.
        let selection_id: usize = *children.get(2).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let selection = self
            .nodes
            .arena
            .get_mut(selection_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        if selection.rule != Type::Selection {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Append `Scan` to the `Selection` children (zero position)
        selection.children.insert(0, scan_id);

        // Check that the first child is `Projection`.
        let proj_id: usize = *children.first().ok_or(QueryPlannerError::ValueOutOfRange)?;
        let proj = self
            .nodes
            .arena
            .get_mut(proj_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        if proj.rule != Type::Projection {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Append `Selection` to the `Projection` children (zero position)
        proj.children.insert(0, selection_id);

        // Leave `Projection` the only child of `Select`.
        let mut select = self
            .nodes
            .arena
            .get_mut(select_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        select.children = vec![proj_id];

        Ok(())
    }

    /// Transforms `Select` with `Projection`, `Scan`, `InnerJoin` and `Condition`
    fn transform_select_4(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), QueryPlannerError> {
        if children.len() != 4 {
            return Err(QueryPlannerError::InvalidInput);
        }

        // Check that the second child is `Scan`.
        let scan_id: usize = *children.get(1).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let scan = self.nodes.get_node(scan_id)?;
        if scan.rule != Type::Scan {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Check that the forth child is `Condition`.
        let cond_id: usize = *children.get(3).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let cond = self.nodes.get_node(cond_id)?;
        if cond.rule != Type::Condition {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Check that the third child is `InnerJoin`.
        let join_id: usize = *children.get(2).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let join = self
            .nodes
            .arena
            .get_mut(join_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        if join.rule != Type::InnerJoin {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Push `Condition` (forth child) to the end of th `InnerJoin` children list.
        join.children.push(cond_id);

        // Append `Scan` to the `InnerJoin` children (zero position)
        join.children.insert(0, scan_id);

        // Check that the first child is `Projection`.
        let proj_id: usize = *children.first().ok_or(QueryPlannerError::ValueOutOfRange)?;
        let proj = self
            .nodes
            .arena
            .get_mut(proj_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        if proj.rule != Type::Projection {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Append `InnerJoin` to the `Projection` children (zero position)
        proj.children.insert(0, join_id);

        // Leave `Projection` the only child of `Select`.
        let mut select = self
            .nodes
            .arena
            .get_mut(select_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        select.children = vec![proj_id];

        Ok(())
    }

    /// Transforms `Select` with `Projection`, `Scan`, `InnerJoin`, `Condition` and `Selection`
    fn transform_select_5(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), QueryPlannerError> {
        if children.len() != 5 {
            return Err(QueryPlannerError::InvalidInput);
        }

        // Check that the second child is `Scan`.
        let scan_id: usize = *children.get(1).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let scan = self.nodes.get_node(scan_id)?;
        if scan.rule != Type::Scan {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Check that the forth child is `Condition`.
        let cond_id: usize = *children.get(3).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let cond = self.nodes.get_node(cond_id)?;
        if cond.rule != Type::Condition {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Check that the third child is `InnerJoin`.
        let join_id: usize = *children.get(2).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let join = self
            .nodes
            .arena
            .get_mut(join_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        if join.rule != Type::InnerJoin {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Push `Condition` (forth child) to the end of the `InnerJoin` children list.
        join.children.push(cond_id);

        // Append `Scan` to the `InnerJoin` children (zero position)
        join.children.insert(0, scan_id);

        // Check that the fifth child is `Selection`.
        let selection_id: usize = *children.get(4).ok_or(QueryPlannerError::ValueOutOfRange)?;
        let selection = self
            .nodes
            .arena
            .get_mut(selection_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        if selection.rule != Type::Selection {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Append `InnerJoin` to the `Selection` children (zero position)
        selection.children.insert(0, join_id);

        // Check that the first child is `Projection`.
        let proj_id: usize = *children.first().ok_or(QueryPlannerError::ValueOutOfRange)?;
        let proj = self
            .nodes
            .arena
            .get_mut(proj_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        if proj.rule != Type::Projection {
            return Err(QueryPlannerError::InvalidAst);
        }

        // Append `Selection` to the `Projection` children (zero position)
        proj.children.insert(0, selection_id);

        // Leave `Projection` the only child of `Select`.
        let mut select = self
            .nodes
            .arena
            .get_mut(select_id)
            .ok_or(QueryPlannerError::ValueOutOfRange)?;
        select.children = vec![proj_id];
        Ok(())
    }

    /// Add aliases to projection columns.
    ///
    /// # Errors
    /// - columns are invalid
    pub(super) fn add_aliases_to_projection(&mut self) -> Result<(), QueryPlannerError> {
        let mut columns: Vec<(usize, Option<String>)> = Vec::new();
        // Collect projection columns and their names.
        for (_, node) in self.nodes.arena.iter().enumerate() {
            if let Type::Projection = node.rule {
                let mut pos = 0;
                for child_id in &node.children {
                    let child = self.nodes.get_node(*child_id)?;
                    if let Type::Column = child.rule {
                        let col_child_id = *child.children.first().ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Column doesn't have any children".into(),
                            )
                        })?;
                        let col_child = self.nodes.get_node(col_child_id)?;
                        match &col_child.rule {
                            Type::Alias => {
                                columns.push((*child_id, None));
                            }
                            Type::Reference => {
                                let col_name_id: usize = if let (Some(_), Some(col_name_id)) =
                                    (col_child.children.first(), col_child.children.get(1))
                                {
                                    *col_name_id
                                } else if let (Some(col_name_id), None) =
                                    (col_child.children.first(), col_child.children.get(1))
                                {
                                    *col_name_id
                                } else {
                                    return Err(QueryPlannerError::CustomError(
                                        "Column doesn't have any children".into(),
                                    ));
                                };
                                let col_name = self.nodes.get_node(col_name_id)?;
                                let name: String = col_name
                                    .value
                                    .as_ref()
                                    .ok_or_else(|| {
                                        QueryPlannerError::CustomError(
                                            "Column name is empty".into(),
                                        )
                                    })?
                                    .clone();
                                columns.push((*child_id, Some(name)));
                            }
                            _ => {
                                pos += 1;
                                columns.push((*child_id, Some(format!("COLUMN_{}", pos))));
                            }
                        }
                    }
                }
            }
        }
        for (id, name) in columns {
            let node = self.nodes.get_node(id)?;
            if node.rule != Type::Column {
                return Err(QueryPlannerError::CustomError(
                    "Parsed node is not a column.".into(),
                ));
            }
            let child_id = *node.children.first().ok_or_else(|| {
                QueryPlannerError::CustomError("Column doesn't have any children".into())
            })?;
            let child = self.nodes.get_node(child_id)?;
            if let Type::Alias | Type::Asterisk = child.rule {
                // Nothing to do here.
                continue;
            }
            let alias_name_id = self.nodes.push_node(ParseNode::new(Rule::AliasName, name)?);
            let alias = ParseNode {
                rule: Type::Alias,
                children: vec![child_id, alias_name_id],
                value: None,
            };
            let alias_id = self.nodes.push_node(alias);
            let column = self.nodes.get_mut_node(id)?;
            column.children = vec![alias_id];
        }
        Ok(())
    }

    /// Map references to the corresponding relational nodes.
    ///
    /// # Errors
    /// - Projection, selection and inner join nodes don't have valid children.
    pub(super) fn build_ref_to_relation_map(&mut self) -> Result<(), QueryPlannerError> {
        let mut map: HashMap<usize, Vec<usize>> = HashMap::new();
        // Traverse relational nodes in Post Order and then enter their subtrees
        // and map expressions to relational nodes.
        let top = self.get_top()?;
        let tree = DftPost::new(&top, |node| self.nodes.ast_iter(node));
        for (_, node_id) in tree {
            let rel_node = self.nodes.get_node(*node_id)?;
            match rel_node.rule {
                Type::Projection => {
                    let rel_id = rel_node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "AST projection doesn't have any children.".into(),
                        )
                    })?;
                    for top in rel_node.children.iter().skip(1) {
                        let subtree = DftPost::new(top, |node| self.nodes.ast_iter(node));
                        for (_, id) in subtree {
                            let node = self.nodes.get_node(*id)?;
                            if let Type::Reference | Type::Asterisk = node.rule {
                                if let Entry::Vacant(entry) = map.entry(*id) {
                                    entry.insert(vec![*rel_id]);
                                }
                            }
                        }
                    }
                }
                Type::Selection => {
                    let rel_id = rel_node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "AST selection doesn't have any children.".into(),
                        )
                    })?;
                    let filter = rel_node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "AST selection doesn't have a filter child.".into(),
                        )
                    })?;
                    let subtree = DftPost::new(filter, |node| self.nodes.ast_iter(node));
                    for (_, id) in subtree {
                        let node = self.nodes.get_node(*id)?;
                        if node.rule == Type::Reference {
                            if let Entry::Vacant(entry) = map.entry(*id) {
                                entry.insert(vec![*rel_id]);
                            }
                        }
                    }
                }
                Type::InnerJoin => {
                    let left_id = rel_node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "AST inner join doesn't have a left child.".into(),
                        )
                    })?;
                    let right_id = rel_node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "AST inner join doesn't have a right child.".into(),
                        )
                    })?;
                    let cond_id = rel_node.children.get(2).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "AST inner join doesn't have a condition child.".into(),
                        )
                    })?;
                    // ast_iter is not working here - we have to ignore sub-queries in the join condition.
                    let subtree = DftPost::new(cond_id, |node| self.nodes.ast_iter(node));
                    for (_, id) in subtree {
                        let node = self.nodes.get_node(*id)?;
                        if node.rule == Type::Reference {
                            if let Entry::Vacant(entry) = map.entry(*id) {
                                entry.insert(vec![*left_id, *right_id]);
                            }
                        }
                    }
                }
                _ => continue,
            }
        }
        self.map = map;
        Ok(())
    }

    /// Get the relational nodes that correspond to a reference.
    ///
    /// # Errors
    /// - Reference is not found.
    pub fn get_referred_relational_nodes(
        &self,
        id: usize,
    ) -> Result<Vec<usize>, QueryPlannerError> {
        self.map
            .get(&id)
            .cloned()
            .ok_or_else(|| QueryPlannerError::CustomError("Reference is not found.".into()))
    }
}

#[cfg(test)]
mod tests;
