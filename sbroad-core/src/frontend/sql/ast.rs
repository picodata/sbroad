//! Abstract syntax tree (AST) module.
//!
//! This module contains a definition of the abstract syntax tree
//! constructed from the nodes of the `pest` tree iterator nodes.

extern crate pest;

use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::fmt;
use std::mem::swap;

use pest::iterators::Pair;
use serde::{Deserialize, Serialize};

use crate::errors::{Action, Entity, SbroadError};
use crate::ir::tree::traversal::{PostOrder, EXPR_CAPACITY};

/// Parse tree
#[derive(Parser)]
#[grammar = "frontend/sql/query.pest"]
pub(super) struct ParseTree;

/// A list of current rules from the actual grammar.
/// When new tokens are added to the grammar they
/// should be also added in the current list.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Type {
    Add,
    Addition,
    Alias,
    AliasName,
    And,
    ArithmeticExpr,
    ArithParentheses,
    Asterisk,
    Between,
    Cast,
    CreateTable,
    Column,
    ColumnIsNotNull,
    ColumnIsNull,
    Columns,
    ColumnDef,
    ColumnDefName,
    ColumnDefType,
    ColumnDefIsNull,
    ColumnName,
    Concat,
    Condition,
    CountAsterisk,
    Decimal,
    DeletedTable,
    Distinct,
    Distribution,
    Divide,
    Double,
    DropTable,
    Duration,
    Engine,
    Eq,
    Except,
    Exists,
    Explain,
    False,
    Function,
    FunctionName,
    Global,
    Gt,
    GtEq,
    GroupBy,
    GroupingElement,
    Having,
    In,
    InnerJoinKind,
    Join,
    Insert,
    Integer,
    IsNotNull,
    IsNull,
    Length,
    Lt,
    LtEq,
    LeftJoinKind,
    Memtx,
    Multiplication,
    Multiply,
    Name,
    NewTable,
    NotEq,
    NotExists,
    NotIn,
    Null,
    OptionParam,
    Or,
    Parameter,
    Parentheses,
    Primary,
    PrimaryKey,
    PrimaryKeyColumn,
    Projection,
    Reference,
    Row,
    Scan,
    ScanName,
    Select,
    Selection,
    Sharding,
    ShardingColumn,
    String,
    SingleQuotedString,
    SubQuery,
    Subtract,
    Table,
    TargetColumns,
    Timeout,
    True,
    TypeAny,
    TypeBool,
    TypeDecimal,
    TypeDouble,
    TypeInt,
    TypeNumber,
    TypeScalar,
    TypeString,
    TypeText,
    TypeUnsigned,
    TypeVarchar,
    UnionAll,
    Unsigned,
    Value,
    Values,
    ValuesRow,
    Vinyl,
}

impl Type {
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    fn from_rule(rule: Rule) -> Result<Self, SbroadError> {
        match rule {
            Rule::Add => Ok(Type::Add),
            Rule::Addition => Ok(Type::Addition),
            Rule::Alias => Ok(Type::Alias),
            Rule::AliasName => Ok(Type::AliasName),
            Rule::And => Ok(Type::And),
            Rule::ArithmeticExpr => Ok(Type::ArithmeticExpr),
            Rule::ArithParentheses => Ok(Type::ArithParentheses),
            Rule::Asterisk => Ok(Type::Asterisk),
            Rule::Between => Ok(Type::Between),
            Rule::Cast => Ok(Type::Cast),
            Rule::CreateTable => Ok(Type::CreateTable),
            Rule::Column => Ok(Type::Column),
            Rule::ColumnDef => Ok(Type::ColumnDef),
            Rule::ColumnDefName => Ok(Type::ColumnDefName),
            Rule::ColumnDefType => Ok(Type::ColumnDefType),
            Rule::ColumnDefIsNull => Ok(Type::ColumnDefIsNull),
            Rule::ColumnIsNotNull => Ok(Type::ColumnIsNotNull),
            Rule::ColumnIsNull => Ok(Type::ColumnIsNull),
            Rule::ColumnName => Ok(Type::ColumnName),
            Rule::Columns => Ok(Type::Columns),
            Rule::Concat => Ok(Type::Concat),
            Rule::CountAsterisk => Ok(Type::CountAsterisk),
            Rule::Condition => Ok(Type::Condition),
            Rule::Decimal => Ok(Type::Decimal),
            Rule::DeletedTable => Ok(Type::DeletedTable),
            Rule::Divide => Ok(Type::Divide),
            Rule::Double => Ok(Type::Double),
            Rule::DropTable => Ok(Type::DropTable),
            Rule::Duration => Ok(Type::Duration),
            Rule::Eq => Ok(Type::Eq),
            Rule::Except => Ok(Type::Except),
            Rule::Exists => Ok(Type::Exists),
            Rule::Explain => Ok(Type::Explain),
            Rule::False => Ok(Type::False),
            Rule::Function => Ok(Type::Function),
            Rule::Distinct => Ok(Type::Distinct),
            Rule::Distribution => Ok(Type::Distribution),
            Rule::Engine => Ok(Type::Engine),
            Rule::FunctionName => Ok(Type::FunctionName),
            Rule::Global => Ok(Type::Global),
            Rule::GroupBy => Ok(Type::GroupBy),
            Rule::GroupingElement => Ok(Type::GroupingElement),
            Rule::Gt => Ok(Type::Gt),
            Rule::GtEq => Ok(Type::GtEq),
            Rule::Having => Ok(Type::Having),
            Rule::In => Ok(Type::In),
            Rule::InnerJoinKind => Ok(Type::InnerJoinKind),
            Rule::Join => Ok(Type::Join),
            Rule::Insert => Ok(Type::Insert),
            Rule::Integer => Ok(Type::Integer),
            Rule::IsNotNull => Ok(Type::IsNotNull),
            Rule::IsNull => Ok(Type::IsNull),
            Rule::Length => Ok(Type::Length),
            Rule::LeftJoinKind => Ok(Type::LeftJoinKind),
            Rule::Lt => Ok(Type::Lt),
            Rule::LtEq => Ok(Type::LtEq),
            Rule::Memtx => Ok(Type::Memtx),
            Rule::Multiplication => Ok(Type::Multiplication),
            Rule::Multiply => Ok(Type::Multiply),
            Rule::Name => Ok(Type::Name),
            Rule::NewTable => Ok(Type::NewTable),
            Rule::NotEq => Ok(Type::NotEq),
            Rule::NotExists => Ok(Type::NotExists),
            Rule::NotIn => Ok(Type::NotIn),
            Rule::Null => Ok(Type::Null),
            Rule::Or => Ok(Type::Or),
            Rule::OptionParam => Ok(Type::OptionParam),
            Rule::Parameter => Ok(Type::Parameter),
            Rule::Parentheses => Ok(Type::Parentheses),
            Rule::Primary => Ok(Type::Primary),
            Rule::PrimaryKey => Ok(Type::PrimaryKey),
            Rule::PrimaryKeyColumn => Ok(Type::PrimaryKeyColumn),
            Rule::Projection => Ok(Type::Projection),
            Rule::Reference => Ok(Type::Reference),
            Rule::Row => Ok(Type::Row),
            Rule::Scan => Ok(Type::Scan),
            Rule::ScanName => Ok(Type::ScanName),
            Rule::Sharding => Ok(Type::Sharding),
            Rule::ShardingColumn => Ok(Type::ShardingColumn),
            Rule::Select => Ok(Type::Select),
            Rule::Selection => Ok(Type::Selection),
            Rule::String => Ok(Type::String),
            Rule::SingleQuotedString => Ok(Type::SingleQuotedString),
            Rule::SubQuery => Ok(Type::SubQuery),
            Rule::Subtract => Ok(Type::Subtract),
            Rule::Table => Ok(Type::Table),
            Rule::TargetColumns => Ok(Type::TargetColumns),
            Rule::Timeout => Ok(Type::Timeout),
            Rule::True => Ok(Type::True),
            Rule::TypeAny => Ok(Type::TypeAny),
            Rule::TypeBool => Ok(Type::TypeBool),
            Rule::TypeDecimal => Ok(Type::TypeDecimal),
            Rule::TypeDouble => Ok(Type::TypeDouble),
            Rule::TypeInt => Ok(Type::TypeInt),
            Rule::TypeNumber => Ok(Type::TypeNumber),
            Rule::TypeScalar => Ok(Type::TypeScalar),
            Rule::TypeString => Ok(Type::TypeString),
            Rule::TypeText => Ok(Type::TypeText),
            Rule::TypeUnsigned => Ok(Type::TypeUnsigned),
            Rule::TypeVarchar => Ok(Type::TypeVarchar),
            Rule::UnionAll => Ok(Type::UnionAll),
            Rule::Unsigned => Ok(Type::Unsigned),
            Rule::Value => Ok(Type::Value),
            Rule::Values => Ok(Type::Values),
            Rule::ValuesRow => Ok(Type::ValuesRow),
            Rule::Vinyl => Ok(Type::Vinyl),
            _ => Err(SbroadError::Invalid(
                Entity::AST,
                Some(format!("got unexpected rule: {rule:?}")),
            )),
        }
    }
}

impl fmt::Display for Type {
    #[allow(clippy::too_many_lines)]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            Type::Add => "Add".to_string(),
            Type::Addition => "Addition".to_string(),
            Type::Alias => "Alias".to_string(),
            Type::AliasName => "AliasName".to_string(),
            Type::And => "And".to_string(),
            Type::ArithmeticExpr => "ArithmeticExpr".to_string(),
            Type::ArithParentheses => "ArithParentheses".to_string(),
            Type::Asterisk => "Asterisk".to_string(),
            Type::Between => "Between".to_string(),
            Type::Cast => "Cast".to_string(),
            Type::CreateTable => "CreateTable".to_string(),
            Type::Column => "Column".to_string(),
            Type::ColumnDef => "ColumnDef".to_string(),
            Type::ColumnDefIsNull => "ColumnDefIsNull".to_string(),
            Type::ColumnDefName => "ColumnDefName".to_string(),
            Type::ColumnDefType => "ColumnDefType".to_string(),
            Type::Columns => "Columns".to_string(),
            Type::ColumnIsNotNull => "ColumnIsNotNull".to_string(),
            Type::ColumnIsNull => "ColumnIsNull".to_string(),
            Type::ColumnName => "ColumnName".to_string(),
            Type::Concat => "Concat".to_string(),
            Type::CountAsterisk => "CountAsterisk".to_string(),
            Type::Condition => "Condition".to_string(),
            Type::Decimal => "Decimal".to_string(),
            Type::DeletedTable => "DeletedTable".to_string(),
            Type::Distinct => "Distinct".to_string(),
            Type::Distribution => "Distribution".to_string(),
            Type::Divide => "Divide".to_string(),
            Type::Double => "Double".to_string(),
            Type::DropTable => "DropTable".to_string(),
            Type::Duration => "Duration".to_string(),
            Type::Eq => "Eq".to_string(),
            Type::Engine => "Engine".to_string(),
            Type::Except => "Except".to_string(),
            Type::Exists => "Exists".to_string(),
            Type::Explain => "Explain".to_string(),
            Type::False => "False".to_string(),
            Type::Function => "Function".to_string(),
            Type::FunctionName => "FunctionName".to_string(),
            Type::Global => "Global".to_string(),
            Type::Gt => "Gt".to_string(),
            Type::GtEq => "GtEq".to_string(),
            Type::Having => "Having".to_string(),
            Type::InnerJoinKind => "inner".to_string(),
            Type::In => "In".to_string(),
            Type::Join => "Join".to_string(),
            Type::Insert => "Insert".to_string(),
            Type::Integer => "Integer".to_string(),
            Type::IsNotNull => "IsNotNull".to_string(),
            Type::IsNull => "IsNull".to_string(),
            Type::Length => "Length".to_string(),
            Type::LeftJoinKind => "left".to_string(),
            Type::Lt => "Lt".to_string(),
            Type::LtEq => "LtEq".to_string(),
            Type::Memtx => "Memtx".to_string(),
            Type::Multiplication => "Multiplication".to_string(),
            Type::Multiply => "Multiply".to_string(),
            Type::Name => "Name".to_string(),
            Type::NewTable => "NewTable".to_string(),
            Type::NotEq => "NotEq".to_string(),
            Type::NotExists => "NotExists".to_string(),
            Type::NotIn => "NotIn".to_string(),
            Type::Null => "Null".to_string(),
            Type::OptionParam => "OptionParam".to_string(),
            Type::Or => "Or".to_string(),
            Type::Parameter => "Parameter".to_string(),
            Type::Parentheses => "Parentheses".to_string(),
            Type::Primary => "Primary".to_string(),
            Type::PrimaryKey => "PrimaryKey".to_string(),
            Type::PrimaryKeyColumn => "PrimaryKeyColumn".to_string(),
            Type::Projection => "Projection".to_string(),
            Type::Reference => "Reference".to_string(),
            Type::Row => "Row".to_string(),
            Type::Scan => "Scan".to_string(),
            Type::ScanName => "ScanName".to_string(),
            Type::Select => "Select".to_string(),
            Type::Selection => "Selection".to_string(),
            Type::Sharding => "Sharding".to_string(),
            Type::ShardingColumn => "ShardingColumn".to_string(),
            Type::String => "String".to_string(),
            Type::SingleQuotedString => "SingleQuotedString".to_string(),
            Type::SubQuery => "SubQuery".to_string(),
            Type::Subtract => "Subtract".to_string(),
            Type::Table => "Table".to_string(),
            Type::TargetColumns => "TargetColumns".to_string(),
            Type::Timeout => "Timeout".to_string(),
            Type::True => "True".to_string(),
            Type::TypeAny => "TypeAny".to_string(),
            Type::TypeBool => "TypeBool".to_string(),
            Type::TypeDecimal => "TypeDecimal".to_string(),
            Type::TypeDouble => "TypeDouble".to_string(),
            Type::TypeInt => "TypeInt".to_string(),
            Type::TypeNumber => "TypeNumber".to_string(),
            Type::TypeScalar => "TypeScalar".to_string(),
            Type::TypeString => "TypeString".to_string(),
            Type::TypeText => "TypeText".to_string(),
            Type::TypeUnsigned => "TypeUnsigned".to_string(),
            Type::TypeVarchar => "TypeVarchar".to_string(),
            Type::UnionAll => "UnionAll".to_string(),
            Type::Unsigned => "Unsigned".to_string(),
            Type::Value => "Value".to_string(),
            Type::Values => "Values".to_string(),
            Type::Vinyl => "Vinyl".to_string(),
            Type::ValuesRow => "ValuesRow".to_string(),
            Type::GroupBy => "GroupBy".to_string(),
            Type::GroupingElement => "GroupingElement".to_string(),
        };
        write!(f, "{p}")
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
    pub(super) fn new(rule: Rule, value: Option<String>) -> Result<Self, SbroadError> {
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
    pub fn get_node(&self, node: usize) -> Result<&ParseNode, SbroadError> {
        self.arena.get(node).ok_or_else(|| {
            SbroadError::NotFound(Entity::Node, format!("from arena with index {node}"))
        })
    }

    /// Get a mutable node from arena
    ///
    /// # Errors
    /// - Failed to get a node from arena.
    pub fn get_mut_node(&mut self, node: usize) -> Result<&mut ParseNode, SbroadError> {
        self.arena.get_mut(node).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(mutable) from arena with index {node}"),
            )
        })
    }

    /// Push a new node to arena
    pub fn push_node(&mut self, node: ParseNode) -> usize {
        let id = self.next_id();
        self.arena.push(node);
        id
    }

    /// Push `child_id` to the front of `node_id` children
    ///
    /// # Errors
    /// - Failed to get node from arena
    pub fn push_front_child(&mut self, node_id: usize, child_id: usize) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children.insert(0, child_id);
        Ok(())
    }

    /// Push `child_id` to the back of `node_id` children
    ///
    /// # Errors
    /// - Failed to get node from arena
    pub fn push_back_child(&mut self, node_id: usize, child_id: usize) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children.push(child_id);
        Ok(())
    }

    /// Sets node children to given children
    ///
    /// # Errors
    /// - failed to get node from arena
    pub fn set_children(
        &mut self,
        node_id: usize,
        new_children: Vec<usize>,
    ) -> Result<(), SbroadError> {
        let node = self.get_mut_node(node_id)?;
        node.children = new_children;
        Ok(())
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
    pub fn add_child(&mut self, node: Option<usize>, child: usize) -> Result<(), SbroadError> {
        if let Some(parent) = node {
            self.get_node(child)?;
            let parent_node = self.arena.get_mut(parent).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    format!("(mutable) from arena with index {parent}"),
                )
            })?;
            parent_node.children.insert(0, child);
        }
        Ok(())
    }

    /// Update node's value (string from pairs)
    ///
    /// # Errors
    /// - Target node is present in the arena.
    pub fn update_value(&mut self, node: usize, value: Option<String>) -> Result<(), SbroadError> {
        let mut node = self.arena.get_mut(node).ok_or_else(|| {
            SbroadError::NotFound(
                Entity::Node,
                format!("(mutable) from arena with index {node}"),
            )
        })?;
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
    /// Index of top `ParseNode` in `nodes.arena`.
    pub(in crate::frontend::sql) top: Option<usize>,
    /// Map of { reference node_id -> relation node_id it refers to }.
    /// See `build_ref_to_relation_map` to understand how it is filled.
    pub(super) map: HashMap<usize, Vec<usize>>,
}

impl PartialEq for AbstractSyntaxTree {
    fn eq(&self, other: &Self) -> bool {
        self.nodes == other.nodes && self.top == other.top && self.map == other.map
    }
}

/// Helper function to extract i-th element of array, when we sure it is safe
/// But we don't want to panic if future changes break something, so we
/// bubble out with error.
///
/// Supposed to be used only in `transform_select_X` methods!
#[inline]
fn get_or_err(arr: &[usize], idx: usize) -> Result<usize, SbroadError> {
    arr.get(idx)
        .ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues(format!(
                "AST children array: {arr:?}. Requested index: {idx}"
            ))
        })
        .map(|v| *v)
}

#[allow(dead_code)]
impl AbstractSyntaxTree {
    /// Set the top of AST.
    ///
    /// # Errors
    /// - The new top is not in the arena.
    pub fn set_top(&mut self, top: usize) -> Result<(), SbroadError> {
        self.nodes.get_node(top)?;
        self.top = Some(top);
        Ok(())
    }

    /// Get the top of AST.
    ///
    /// # Errors
    /// - AST tree doesn't have a top node.
    pub fn get_top(&self) -> Result<usize, SbroadError> {
        self.top.ok_or_else(|| {
            SbroadError::Invalid(Entity::AST, Some("no top node found in AST".into()))
        })
    }

    /// Serialize AST from YAML.
    ///
    /// # Errors
    /// - Failed to parse YAML.
    pub fn from_yaml(s: &str) -> Result<Self, SbroadError> {
        let ast: AbstractSyntaxTree = match serde_yaml::from_str(s) {
            Ok(p) => p,
            Err(e) => {
                return Err(SbroadError::FailedTo(
                    Action::Serialize,
                    Some(Entity::AST),
                    format!("{e:?}"),
                ))
            }
        };
        Ok(ast)
    }

    /// Bring join AST to expected kind
    ///
    /// Inner join can be specified as `inner join` or `join` in user query,
    /// add `inner` to join if the second form was used
    pub(super) fn normalize_join_ast(&mut self, join_id: usize) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(join_id)?;
        if let Type::Join = node.rule {
            if node.children.len() < 3 {
                let inner_node = ParseNode {
                    children: vec![],
                    rule: Type::InnerJoinKind,
                    value: Some("inner".into()),
                };
                let inner_id = self.nodes.push_node(inner_node);
                let mut_node = self.nodes.get_mut_node(join_id)?;
                mut_node.children.insert(0, inner_id);
            }
        } else {
            return Err(SbroadError::Invalid(
                Entity::ParseNode,
                Some(format!("expected join parse node, got: {node:?}")),
            ));
        }
        Ok(())
    }

    /// `Select` node is not IR-friendly as it can have up to five children.
    /// Transform this node in IR-way (to a binary sub-tree).
    pub(super) fn transform_select(&mut self) -> Result<(), SbroadError> {
        let mut selects: HashSet<usize> = HashSet::new();
        for id in 0..self.nodes.arena.len() {
            let node = self.nodes.get_node(id)?;
            if node.rule == Type::Select {
                selects.insert(id);
            }
            if node.rule == Type::Join {
                self.normalize_join_ast(id)?;
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
                6 => self.transform_select_6(*node, &children)?,
                _ => return Err(SbroadError::Invalid(Entity::AST, None)),
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
                SbroadError::NotFound(Entity::Node, format!("at expected position {children_pos}"))
            })?;
            let child = self.nodes.get_node(child_id)?;
            let mut node_id = *child.children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues("Selection node has no children.".into())
            })?;
            let parent = self.nodes.get_mut_node(parent_id)?;
            swap(&mut parent.children[children_pos], &mut node_id);
        }
        // Remove select if it is a top node.
        let top_id = self.get_top()?;
        if selects.contains(&top_id) {
            let top = self.nodes.get_node(top_id)?;
            let child_id = *top.children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(
                    "Selection node doesn't contain any children.".into(),
                )
            })?;
            self.set_top(child_id)?;
        }
        Ok(())
    }

    fn check<const N: usize, const M: usize>(
        &self,
        allowed: &[[Type; N]; M],
        select_children: &[usize],
    ) -> Result<(), SbroadError> {
        let allowed_len = if let Some(seq) = allowed.first() {
            seq.len()
        } else {
            return Err(SbroadError::UnexpectedNumberOfValues(
                "Expected at least one sequence to check select children".into(),
            ));
        };
        if select_children.len() != allowed_len {
            return Err(SbroadError::UnexpectedNumberOfValues(format!(
                "Expected select {allowed_len} children, got {}",
                select_children.len()
            )));
        }
        let mut is_match = false;
        for seq in allowed {
            let mut all_types_matched = true;
            for (child, expected_type) in select_children.iter().zip(seq) {
                let node = self.nodes.get_node(*child)?;
                if node.rule != *expected_type {
                    all_types_matched = false;
                    break;
                }
            }
            if all_types_matched {
                is_match = true;
                break;
            }
        }
        if !is_match {
            return Err(SbroadError::Invalid(
                Entity::AST,
                Some("Could not match select children to any expected sequence".into()),
            ));
        }
        Ok(())
    }

    fn transform_select_2(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), SbroadError> {
        let allowed = [[Type::Projection, Type::Scan]];
        self.check(&allowed, children)?;
        self.nodes
            .push_front_child(get_or_err(children, 0)?, get_or_err(children, 1)?)?;
        self.nodes.set_children(select_id, vec![children[0]])?;
        Ok(())
    }

    fn transform_select_3(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), SbroadError> {
        let allowed = [
            [Type::Projection, Type::Scan, Type::Join],
            [Type::Projection, Type::Scan, Type::GroupBy],
            [Type::Projection, Type::Scan, Type::Selection],
            [Type::Projection, Type::Scan, Type::Having],
        ];
        self.check(&allowed, children)?;
        self.nodes
            .push_front_child(get_or_err(children, 2)?, get_or_err(children, 1)?)?;
        self.nodes
            .push_front_child(get_or_err(children, 0)?, get_or_err(children, 2)?)?;
        self.nodes
            .set_children(select_id, vec![get_or_err(children, 0)?])?;
        Ok(())
    }

    fn transform_select_4(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), SbroadError> {
        let allowed = [
            [Type::Projection, Type::Scan, Type::Selection, Type::GroupBy],
            [Type::Projection, Type::Scan, Type::Selection, Type::Having],
            [Type::Projection, Type::Scan, Type::GroupBy, Type::Having],
            [Type::Projection, Type::Scan, Type::Join, Type::Selection],
            [Type::Projection, Type::Scan, Type::Join, Type::GroupBy],
            [Type::Projection, Type::Scan, Type::Join, Type::Having],
        ];
        self.check(&allowed, children)?;
        // insert Selection | InnerJoin as first child of GroupBy
        self.nodes
            .push_front_child(get_or_err(children, 3)?, get_or_err(children, 2)?)?;
        // insert Scan as first child of Selection | InnerJoin
        self.nodes
            .push_front_child(get_or_err(children, 2)?, get_or_err(children, 1)?)?;
        // insert GroupBy as first child of Projection
        self.nodes
            .push_front_child(get_or_err(children, 0)?, get_or_err(children, 3)?)?;
        self.nodes.set_children(select_id, vec![children[0]])?;
        Ok(())
    }

    fn transform_select_5(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), SbroadError> {
        let allowed = [
            [
                Type::Projection,
                Type::Scan,
                Type::Join,
                Type::Selection,
                Type::GroupBy,
            ],
            [
                Type::Projection,
                Type::Scan,
                Type::Join,
                Type::Selection,
                Type::Having,
            ],
            [
                Type::Projection,
                Type::Scan,
                Type::Join,
                Type::GroupBy,
                Type::Having,
            ],
            [
                Type::Projection,
                Type::Scan,
                Type::Selection,
                Type::GroupBy,
                Type::Having,
            ],
        ];
        self.check(&allowed, children)?;
        // insert Selection as first child of GroupBy
        self.nodes
            .push_front_child(get_or_err(children, 4)?, get_or_err(children, 3)?)?;
        // insert InnerJoin as first child of Selection
        self.nodes
            .push_front_child(get_or_err(children, 3)?, get_or_err(children, 2)?)?;
        // insert Scan as first child of InnerJoin
        self.nodes
            .push_front_child(get_or_err(children, 2)?, get_or_err(children, 1)?)?;
        // insert GroupBy as first child of Projection
        self.nodes
            .push_front_child(get_or_err(children, 0)?, get_or_err(children, 4)?)?;
        self.nodes.set_children(select_id, vec![children[0]])?;
        Ok(())
    }

    fn transform_select_6(
        &mut self,
        select_id: usize,
        children: &[usize],
    ) -> Result<(), SbroadError> {
        let allowed = [[
            Type::Projection,
            Type::Scan,
            Type::Join,
            Type::Selection,
            Type::GroupBy,
            Type::Having,
        ]];
        self.check(&allowed, children)?;
        // insert GroupBy as first child of Having
        self.nodes
            .push_front_child(get_or_err(children, 5)?, get_or_err(children, 4)?)?;
        // insert Selection as first child of GroupBy
        self.nodes
            .push_front_child(get_or_err(children, 4)?, get_or_err(children, 3)?)?;
        // insert InnerJoin as first child of Selection
        self.nodes
            .push_front_child(get_or_err(children, 3)?, get_or_err(children, 2)?)?;
        // insert Scan as first child of InnerJoin
        self.nodes
            .push_front_child(get_or_err(children, 2)?, get_or_err(children, 1)?)?;
        // insert Having as first child of Projection
        self.nodes
            .push_front_child(get_or_err(children, 0)?, get_or_err(children, 5)?)?;
        self.nodes.set_children(select_id, vec![children[0]])?;
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    /// Add aliases to projection columns.
    ///
    /// # Errors
    /// - columns are invalid
    pub(super) fn add_aliases_to_projection(&mut self) -> Result<(), SbroadError> {
        let mut columns: Vec<(usize, Option<String>)> = Vec::new();
        // Collect projection columns and their names.
        for (_, node) in self.nodes.arena.iter().enumerate() {
            if let Type::Projection = node.rule {
                let mut pos = 0;
                for child_id in &node.children {
                    let child = self.nodes.get_node(*child_id)?;
                    if let Type::Column = child.rule {
                        let col_child_id = *child.children.first().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues("Column has no children".into())
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
                                    return Err(SbroadError::NotFound(
                                        Entity::Node,
                                        "that is first child of the Column".into(),
                                    ));
                                };
                                let col_name = self.nodes.get_node(col_name_id)?;
                                let name: String = col_name
                                    .value
                                    .as_ref()
                                    .ok_or_else(|| {
                                        SbroadError::Invalid(
                                            Entity::Name,
                                            Some("of Column is empty".into()),
                                        )
                                    })?
                                    .clone();
                                columns.push((*child_id, Some(name)));
                            }
                            _ => {
                                pos += 1;
                                columns.push((*child_id, Some(format!("COL_{pos}"))));
                            }
                        }
                    }
                }
            }
        }
        for (id, name) in columns {
            let node = self.nodes.get_node(id)?;
            if node.rule != Type::Column {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some("Parsed node is not a column.".into()),
                ));
            }
            let child_id = *node.children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues("Column has no children".into())
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

    #[allow(clippy::too_many_lines)]
    /// Map references to the corresponding relational nodes.
    ///
    /// # Errors
    /// - Projection, selection and inner join nodes don't have valid children.
    pub(super) fn build_ref_to_relation_map(&mut self) -> Result<(), SbroadError> {
        let mut map: HashMap<usize, Vec<usize>> = HashMap::new();
        // Traverse relational nodes in Post Order and then enter their subtrees
        // and map expressions to relational nodes.
        let top = self.get_top()?;
        let capacity = self.nodes.arena.len();
        let mut tree = PostOrder::with_capacity(|node| self.nodes.ast_iter(node), capacity);
        for (_, node_id) in tree.iter(top) {
            let rel_node = self.nodes.get_node(node_id)?;
            match rel_node.rule {
                Type::Projection => {
                    let rel_id = rel_node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "AST Projection has no children.".into(),
                        )
                    })?;
                    for top in rel_node.children.iter().skip(1) {
                        let capacity = EXPR_CAPACITY * 3;
                        let mut subtree =
                            PostOrder::with_capacity(|node| self.nodes.ast_iter(node), capacity);
                        for (_, id) in subtree.iter(*top) {
                            let node = self.nodes.get_node(id)?;
                            if let Type::Reference | Type::Asterisk = node.rule {
                                if let Entry::Vacant(entry) = map.entry(id) {
                                    entry.insert(vec![*rel_id]);
                                }
                            }
                        }
                    }
                }
                Type::Selection | Type::Having => {
                    let rel_id = rel_node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(format!(
                            "AST {:?} has no children.",
                            rel_node.rule
                        ))
                    })?;
                    let filter = rel_node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "that is AST selection filter child with index 1".into(),
                        )
                    })?;
                    let capacity = EXPR_CAPACITY * 2;
                    let mut subtree =
                        PostOrder::with_capacity(|node| self.nodes.ast_iter(node), capacity);
                    for (_, id) in subtree.iter(*filter) {
                        let node = self.nodes.get_node(id)?;
                        if node.rule == Type::Reference {
                            if let Entry::Vacant(entry) = map.entry(id) {
                                entry.insert(vec![*rel_id]);
                            }
                        }
                    }
                }
                Type::Join => {
                    let left_id = rel_node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "AST inner join has no children.".into(),
                        )
                    })?;
                    let right_id = rel_node.children.get(2).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "that is AST inner join right child with index 1".into(),
                        )
                    })?;
                    let cond_id = rel_node.children.get(3).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "that is AST inner join condition child with index 2".into(),
                        )
                    })?;
                    // ast_iter is not working here - we have to ignore sub-queries in the join condition.
                    let capacity = EXPR_CAPACITY * 4;
                    let mut subtree =
                        PostOrder::with_capacity(|node| self.nodes.ast_iter(node), capacity);
                    for (_, id) in subtree.iter(*cond_id) {
                        let node = self.nodes.get_node(id)?;
                        if node.rule == Type::Reference {
                            if let Entry::Vacant(entry) = map.entry(id) {
                                entry.insert(vec![*left_id, *right_id]);
                            }
                        }
                    }
                }
                Type::GroupBy => {
                    let rel_id = rel_node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "AST group by doesn't have any children.".into(),
                        )
                    })?;
                    for top in rel_node.children.iter().skip(1) {
                        let mut subtree =
                            PostOrder::with_capacity(|node| self.nodes.ast_iter(node), capacity);
                        for (_, id) in subtree.iter(*top) {
                            let node = self.nodes.get_node(id)?;
                            if let Type::Reference = node.rule {
                                if let Entry::Vacant(entry) = map.entry(id) {
                                    entry.insert(vec![*rel_id]);
                                }
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
    pub fn get_referred_relational_nodes(&self, id: usize) -> Result<Vec<usize>, SbroadError> {
        self.map
            .get(&id)
            .cloned()
            .ok_or_else(|| SbroadError::NotFound(Entity::Relational, format!("(id {id})")))
    }
}

#[cfg(test)]
mod tests;
