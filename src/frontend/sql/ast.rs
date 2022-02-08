extern crate pest;

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::mem::swap;

use pest::iterators::Pair;
use pest::Parser;
use serde::{Deserialize, Serialize};
use traversal::DftPost;

use crate::errors::QueryPlannerError;

/// Parse tree
#[derive(Parser)]
#[grammar = "frontend/sql/grammar.pest"]
pub struct ParseTree;

/// A list of current rules from the actual grammar.
/// When new tokens are added to the grammar they
/// should be also added in the current list.
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Type {
    Alias,
    AliasName,
    And,
    Asterisk,
    Bool,
    Column,
    Condition,
    Gt,
    Eq,
    GtEq,
    In,
    InnerJoin,
    Insert,
    Lt,
    LtEq,
    Name,
    NotEq,
    Null,
    Number,
    Or,
    Parentheses,
    Primary,
    Projection,
    Reference,
    Row,
    Scan,
    Select,
    Selection,
    String,
    SubQuery,
    SubQueryName,
    Table,
    UnionAll,
    Value,
    Values,
}

impl Type {
    #[allow(dead_code)]
    fn from_rule(rule: Rule) -> Result<Self, QueryPlannerError> {
        match rule {
            Rule::Alias => Ok(Type::Alias),
            Rule::AliasName => Ok(Type::AliasName),
            Rule::And => Ok(Type::And),
            Rule::Asterisk => Ok(Type::Asterisk),
            Rule::Bool => Ok(Type::Bool),
            Rule::Column => Ok(Type::Column),
            Rule::Condition => Ok(Type::Condition),
            Rule::Gt => Ok(Type::Gt),
            Rule::Eq => Ok(Type::Eq),
            Rule::GtEq => Ok(Type::GtEq),
            Rule::In => Ok(Type::In),
            Rule::InnerJoin => Ok(Type::InnerJoin),
            Rule::Insert => Ok(Type::Insert),
            Rule::Lt => Ok(Type::Lt),
            Rule::LtEq => Ok(Type::LtEq),
            Rule::Name => Ok(Type::Name),
            Rule::NotEq => Ok(Type::NotEq),
            Rule::Null => Ok(Type::Null),
            Rule::Number => Ok(Type::Number),
            Rule::Or => Ok(Type::Or),
            Rule::Parentheses => Ok(Type::Parentheses),
            Rule::Primary => Ok(Type::Primary),
            Rule::Projection => Ok(Type::Projection),
            Rule::Reference => Ok(Type::Reference),
            Rule::Row => Ok(Type::Row),
            Rule::Scan => Ok(Type::Scan),
            Rule::Select => Ok(Type::Select),
            Rule::Selection => Ok(Type::Selection),
            Rule::String => Ok(Type::String),
            Rule::SubQuery => Ok(Type::SubQuery),
            Rule::SubQueryName => Ok(Type::SubQueryName),
            Rule::Table => Ok(Type::Table),
            Rule::UnionAll => Ok(Type::UnionAll),
            Rule::Value => Ok(Type::Value),
            Rule::Values => Ok(Type::Values),
            _ => Err(QueryPlannerError::InvalidAst),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ParseNode {
    pub(in crate::frontend::sql) children: Vec<usize>,
    pub(in crate::frontend::sql) rule: Type,
    pub(in crate::frontend::sql) value: Option<String>,
}

#[allow(dead_code)]
impl ParseNode {
    fn new(rule: Rule, value: Option<String>) -> Result<Self, QueryPlannerError> {
        Ok(ParseNode {
            children: vec![],
            rule: Type::from_rule(rule)?,
            value,
        })
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ParseNodes {
    arena: Vec<ParseNode>,
}

#[allow(dead_code)]
impl ParseNodes {
    /// Get a node from arena
    pub fn get_node(&self, node: usize) -> Result<&ParseNode, QueryPlannerError> {
        self.arena.get(node).ok_or(QueryPlannerError::InvalidNode)
    }

    /// Get a mutable node from arena
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
    pub fn next_id(&self) -> usize {
        self.arena.len()
    }

    /// Constructor
    pub fn new() -> Self {
        ParseNodes { arena: Vec::new() }
    }

    /// Adds children to already existing node.
    /// New elements are added to the beginning of the current list
    /// as we use inverted node order.
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
struct StackParseNode<'n> {
    parent: Option<usize>,
    pair: Pair<'n, Rule>,
}

impl<'n> StackParseNode<'n> {
    /// Constructor
    fn new(pair: Pair<'n, Rule>, parent: Option<usize>) -> Self {
        StackParseNode { parent, pair }
    }
}

/// AST where all the nodes are kept in a list.
/// Positions in a list act like references.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AbstractSyntaxTree {
    pub(in crate::frontend::sql) nodes: ParseNodes,
    pub(in crate::frontend::sql) top: Option<usize>,
    map: HashMap<usize, Vec<usize>>,
}

#[allow(dead_code)]
impl AbstractSyntaxTree {
    /// Set the top of AST.
    pub fn set_top(&mut self, top: usize) -> Result<(), QueryPlannerError> {
        self.nodes.get_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get the top of AST.
    pub fn get_top(&self) -> Result<usize, QueryPlannerError> {
        self.top
            .ok_or_else(|| QueryPlannerError::CustomError("No top node found in AST".to_string()))
    }

    /// Serialize AST from YAML.
    pub fn from_yaml(s: &str) -> Result<Self, QueryPlannerError> {
        let ast: AbstractSyntaxTree = match serde_yaml::from_str(s) {
            Ok(p) => p,
            Err(_) => return Err(QueryPlannerError::Serialization),
        };
        Ok(ast)
    }

    /// Constructor.
    /// Builds a tree (nodes are in postorder reverse).
    pub fn new(query: &str) -> Result<Self, QueryPlannerError> {
        let mut ast = AbstractSyntaxTree {
            nodes: ParseNodes::new(),
            top: None,
            map: HashMap::new(),
        };

        let mut command_pair = match ParseTree::parse(Rule::Command, query) {
            Ok(p) => p,
            Err(_) => return Err(QueryPlannerError::CustomError("Invalid command.".into())),
        };
        let top_pair = command_pair.next().ok_or_else(|| {
            QueryPlannerError::CustomError("No query found in the parse tree.".to_string())
        })?;
        let top = StackParseNode::new(top_pair, None);

        let mut stack: Vec<StackParseNode> = vec![top];

        while !stack.is_empty() {
            let stack_node: StackParseNode = match stack.pop() {
                Some(n) => n,
                None => break,
            };

            // Save node to AST
            let node = ast.nodes.push_node(ParseNode::new(
                stack_node.pair.as_rule(),
                Some(String::from(stack_node.pair.as_str())),
            )?);

            // Update parent's node children list
            ast.nodes.add_child(stack_node.parent, node)?;
            // Clean parent values (only leafs should contain data)
            if let Some(parent) = stack_node.parent {
                ast.nodes.update_value(parent, None)?;
            }

            for parse_child in stack_node.pair.into_inner() {
                stack.push(StackParseNode::new(parse_child, Some(node)));
            }
        }

        ast.set_top(0)?;

        ast.transform_select()?;
        ast.add_aliases_to_projection()?;
        ast.build_ref_to_relation_map()?;

        Ok(ast)
    }

    /// `Select` node is not IR-friendly as it can have up to five children.
    /// Transform this node in IR-way (to a binary sub-tree).
    fn transform_select(&mut self) -> Result<(), QueryPlannerError> {
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
            let mut node_id = *child.children.get(0).ok_or_else(|| {
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
            let child_id = *top.children.get(0).ok_or_else(|| {
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
        let proj_id: usize = *children.get(0).ok_or(QueryPlannerError::ValueOutOfRange)?;
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
        let proj_id: usize = *children.get(0).ok_or(QueryPlannerError::ValueOutOfRange)?;
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
        let proj_id: usize = *children.get(0).ok_or(QueryPlannerError::ValueOutOfRange)?;
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
        let proj_id: usize = *children.get(0).ok_or(QueryPlannerError::ValueOutOfRange)?;
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
    fn add_aliases_to_projection(&mut self) -> Result<(), QueryPlannerError> {
        let mut columns: Vec<(usize, Option<String>)> = Vec::new();
        // Collect projection columns and their names.
        for (_, node) in self.nodes.arena.iter().enumerate() {
            if let Type::Projection = node.rule {
                let mut pos = 0;
                for child_id in &node.children {
                    let child = self.nodes.get_node(*child_id)?;
                    if let Type::Column = child.rule {
                        let col_child_id = *child.children.get(0).ok_or_else(|| {
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
                                columns.push((*child_id, col_child.value.clone()));
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
            let child_id = *node.children.get(0).ok_or_else(|| {
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
    fn build_ref_to_relation_map(&mut self) -> Result<(), QueryPlannerError> {
        let mut map: HashMap<usize, Vec<usize>> = HashMap::new();
        for rel_node in &self.nodes.arena {
            match rel_node.rule {
                Type::Projection => {
                    let rel_id = rel_node.children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "AST projection doesn't have any children.".into(),
                        )
                    })?;
                    for top in rel_node.children.iter().skip(1) {
                        let subtree = DftPost::new(top, |node| self.nodes.tree_iter(node));
                        for (_, id) in subtree {
                            let node = self.nodes.get_node(*id)?;
                            if let Type::Reference | Type::Asterisk = node.rule {
                                map.insert(*id, vec![*rel_id]);
                            }
                        }
                    }
                }
                Type::Selection => {
                    let rel_id = rel_node.children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "AST selection doesn't have any children.".into(),
                        )
                    })?;
                    let filter = rel_node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "AST selection doesn't have a filter child.".into(),
                        )
                    })?;
                    let subtree = DftPost::new(filter, |node| self.nodes.tree_iter(node));
                    for (_, id) in subtree {
                        let node = self.nodes.get_node(*id)?;
                        if node.rule == Type::Reference {
                            map.insert(*id, vec![*rel_id]);
                        }
                    }
                }
                Type::InnerJoin => {
                    let left_id = rel_node.children.get(0).ok_or_else(|| {
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
                    let subtree = DftPost::new(cond_id, |node| self.nodes.tree_iter(node));
                    for (_, id) in subtree {
                        let node = self.nodes.get_node(*id)?;
                        if node.rule == Type::Reference {
                            map.insert(*id, vec![*left_id, *right_id]);
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

#[derive(Debug)]
pub struct TreeIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n ParseNodes,
}

impl<'n> Iterator for TreeIterator<'n> {
    type Item = &'n usize;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node) = self.nodes.arena.get(*self.current) {
            let step = *self.child.borrow();
            if step < node.children.len() {
                *self.child.borrow_mut() += 1;
                return node.children.get(step);
            }
            None
        } else {
            None
        }
    }
}

impl<'n> ParseNodes {
    #[allow(dead_code)]
    pub fn tree_iter(&'n self, current: &'n usize) -> TreeIterator<'n> {
        TreeIterator {
            current,
            child: RefCell::new(0),
            nodes: self,
        }
    }
}

#[cfg(test)]
mod tests;
