extern crate pest;

use crate::errors::QueryPlannerError;
use pest::iterators::Pair;
use pest::Parser;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;

/// Parse tree
#[derive(Parser)]
#[grammar = "frontend/sql/grammar.pest"]
pub struct ParseTree;

/// A list of current rules from the actual grammar.
/// When new tokens are added to the grammar they
/// should be also added in the current list.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Type {
    Alias,
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
    NotEq,
    Null,
    Number,
    Or,
    Parentheses,
    Primary,
    Projection,
    QuotedName,
    Row,
    Scan,
    Select,
    Selection,
    String,
    SubQuery,
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
            Rule::NotEq => Ok(Type::NotEq),
            Rule::Null => Ok(Type::Null),
            Rule::Number => Ok(Type::Number),
            Rule::Or => Ok(Type::Or),
            Rule::Parentheses => Ok(Type::Parentheses),
            Rule::Primary => Ok(Type::Primary),
            Rule::Projection => Ok(Type::Projection),
            Rule::QuotedName => Ok(Type::QuotedName),
            Rule::Row => Ok(Type::Row),
            Rule::Scan => Ok(Type::Scan),
            Rule::Select => Ok(Type::Select),
            Rule::Selection => Ok(Type::Selection),
            Rule::String => Ok(Type::String),
            Rule::SubQuery => Ok(Type::SubQuery),
            Rule::Table => Ok(Type::Table),
            Rule::UnionAll => Ok(Type::UnionAll),
            Rule::Value => Ok(Type::Value),
            Rule::Values => Ok(Type::Values),
            _ => Err(QueryPlannerError::InvalidAst),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Node {
    children: Vec<usize>,
    rule: Type,
    value: Option<String>,
}

#[allow(dead_code)]
impl Node {
    fn new(rule: Rule, value: Option<String>) -> Result<Self, QueryPlannerError> {
        Ok(Node {
            children: vec![],
            rule: Type::from_rule(rule)?,
            value,
        })
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Nodes {
    arena: Vec<Node>,
}

#[allow(dead_code)]
impl Nodes {
    /// Get a node from arena
    pub fn get_node(&self, node: usize) -> Result<&Node, QueryPlannerError> {
        self.arena.get(node).ok_or(QueryPlannerError::InvalidNode)
    }

    /// Push a new node to arena
    pub fn push_node(&mut self, node: Node) -> usize {
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
        Nodes { arena: Vec::new() }
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
struct StackNode<'n> {
    parent: Option<usize>,
    pair: Pair<'n, Rule>,
}

impl<'n> StackNode<'n> {
    /// Constructor
    fn new(pair: Pair<'n, Rule>, parent: Option<usize>) -> Self {
        StackNode { parent, pair }
    }
}

/// AST where all the nodes are kept in a list.
/// Positions in a list act like references.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AbstractSyntaxTree {
    nodes: Nodes,
    top: Option<usize>,
}

#[allow(dead_code)]
impl AbstractSyntaxTree {
    /// Set the top of AST.
    pub fn set_top(&mut self, top: usize) -> Result<(), QueryPlannerError> {
        self.nodes.get_node(top)?;
        self.top = Some(top);
        Ok(())
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
            nodes: Nodes::new(),
            top: None,
        };

        let top_pair: Pair<Rule> = match ParseTree::parse(Rule::Query, query) {
            Ok(ref mut v) => match v.next() {
                Some(t) => t,
                None => return Err(QueryPlannerError::QueryNotImplemented),
            },
            Err(_) => return Err(QueryPlannerError::QueryNotImplemented),
        };
        let top = StackNode::new(top_pair, None);

        let mut stack: Vec<StackNode> = vec![top];

        while !stack.is_empty() {
            let stack_node: StackNode = match stack.pop() {
                Some(n) => n,
                None => break,
            };

            // Save node to AST
            let node = ast.nodes.push_node(Node::new(
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
                stack.push(StackNode::new(parse_child, Some(node)));
            }
        }

        ast.set_top(0)?;
        Ok(ast)
    }

    /// `Select` node is not IR-friendly as it can have up to five children.
    /// Transform this node in IR-way (to a binary sub-tree).
    pub fn transform_select(&mut self) -> Result<(), QueryPlannerError> {
        let mut selects: Vec<usize> = Vec::new();
        for (pos, node) in self.nodes.arena.iter().enumerate() {
            if node.rule == Type::Select {
                selects.push(pos);
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
        let cond_id: usize = *children.get(3).ok_or(QueryPlannerError::ValueOutOfRange)?;
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
        let cond_id: usize = *children.get(3).ok_or(QueryPlannerError::ValueOutOfRange)?;
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
}

#[derive(Debug)]
pub struct TreeIterator<'n> {
    current: &'n usize,
    child: RefCell<usize>,
    nodes: &'n Nodes,
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

impl<'n> Nodes {
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
