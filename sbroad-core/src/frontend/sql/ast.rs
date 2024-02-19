//! Abstract syntax tree (AST) module.
//!
//! This module contains a definition of the abstract syntax tree
//! constructed from the nodes of the `pest` tree iterator nodes.

extern crate pest;

use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::mem::swap;

use pest::iterators::Pair;
use serde::{Deserialize, Serialize};

use crate::errors::{Action, Entity, SbroadError};
use crate::frontend::sql::parse_string_value_node;
use crate::ir::tree::traversal::{PostOrder, PostOrderWithFilter, EXPR_CAPACITY};

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
    AlterUser,
    AlterOption,
    AlterLogin,
    AlterNoLogin,
    AlterPassword,
    And,
    ArithmeticExpr,
    ArithParentheses,
    Asterisk,
    AuthMethod,
    Between,
    Cast,
    ChapSha1,
    Cmp,
    CreateProc,
    CreateTable,
    Column,
    Columns,
    ColumnDef,
    ColumnDefName,
    ColumnDefType,
    ColumnDefIsNull,
    ColumnName,
    Concat,
    Condition,
    CountAsterisk,
    CreateRole,
    CreateUser,
    Decimal,
    Delete,
    DeleteFilter,
    DeletedTable,
    Distinct,
    Distribution,
    Divide,
    Double,
    DropRole,
    DropTable,
    DropUser,
    Duration,
    Engine,
    Eq,
    Exists,
    Explain,
    False,
    Function,
    FunctionName,
    Global,
    GrantPrivilege,
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
    IsNull,
    Length,
    DoReplace,
    DoNothing,
    DoFail,
    Ldap,
    LeftJoinKind,
    Lt,
    LtEq,
    Md5,
    Memtx,
    Multiplication,
    Multiply,
    Name,
    NewProc,
    NewTable,
    Not,
    NotEq,
    NotFlag,
    Null,
    Or,
    Password,
    Query,
    SqlVdbeMaxSteps,
    VTableMaxRows,
    Parameter,
    Parentheses,
    PgParameter,
    Primary,
    PrimaryKey,
    PrimaryKeyColumn,
    PrivBlockPrivilege,
    PrivBlockUser,
    PrivBlockSpecificUser,
    PrivBlockRole,
    PrivBlockSpecificRole,
    PrivBlockTable,
    PrivBlockSpecificTable,
    PrivBlockRolePass,
    PrivilegeAlter,
    PrivilegeCreate,
    PrivilegeDrop,
    PrivilegeExecute,
    PrivilegeRead,
    PrivilegeSession,
    PrivilegeUsage,
    PrivilegeWrite,
    ProcBody,
    ProcLanguage,
    ProcParams,
    ProcParamDef,
    Projection,
    Reference,
    RevokePrivilege,
    RoleName,
    Row,
    Scan,
    ScanName,
    ScanTable,
    Select,
    SelectWithOptionalContinuation,
    ExceptContinuation,
    UnionAllContinuation,
    Selection,
    Sharding,
    ShardingColumn,
    String,
    SingleQuotedString,
    SQL,
    SubQuery,
    Subtract,
    Table,
    TargetColumns,
    Timeout,
    TntParameter,
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
    Update,
    UpdateList,
    UpdateItem,
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
            Rule::AliasName => Ok(Type::AliasName),
            Rule::AlterUser => Ok(Type::AlterUser),
            Rule::AlterLogin => Ok(Type::AlterLogin),
            Rule::AlterNoLogin => Ok(Type::AlterNoLogin),
            Rule::AlterPassword => Ok(Type::AlterPassword),
            Rule::And => Ok(Type::And),
            Rule::ArithmeticExpr => Ok(Type::ArithmeticExpr),
            Rule::ArithParentheses => Ok(Type::ArithParentheses),
            Rule::Asterisk => Ok(Type::Asterisk),
            Rule::AuthMethod => Ok(Type::AuthMethod),
            Rule::Between => Ok(Type::Between),
            Rule::Cast => Ok(Type::Cast),
            Rule::ChapSha1 => Ok(Type::ChapSha1),
            Rule::Cmp => Ok(Type::Cmp),
            Rule::CreateProc => Ok(Type::CreateProc),
            Rule::CreateRole => Ok(Type::CreateRole),
            Rule::CreateTable => Ok(Type::CreateTable),
            Rule::CreateUser => Ok(Type::CreateUser),
            Rule::Column => Ok(Type::Column),
            Rule::ColumnDef => Ok(Type::ColumnDef),
            Rule::ColumnDefName => Ok(Type::ColumnDefName),
            Rule::ColumnDefType => Ok(Type::ColumnDefType),
            Rule::ColumnDefIsNull => Ok(Type::ColumnDefIsNull),
            Rule::ColumnName => Ok(Type::ColumnName),
            Rule::Columns => Ok(Type::Columns),
            Rule::Concat => Ok(Type::Concat),
            Rule::CountAsterisk => Ok(Type::CountAsterisk),
            Rule::Condition => Ok(Type::Condition),
            Rule::Decimal => Ok(Type::Decimal),
            Rule::Delete => Ok(Type::Delete),
            Rule::DeleteFilter => Ok(Type::DeleteFilter),
            Rule::DeletedTable => Ok(Type::DeletedTable),
            Rule::Divide => Ok(Type::Divide),
            Rule::Double => Ok(Type::Double),
            Rule::DropRole => Ok(Type::DropRole),
            Rule::DropTable => Ok(Type::DropTable),
            Rule::DropUser => Ok(Type::DropUser),
            Rule::Duration => Ok(Type::Duration),
            Rule::DoReplace => Ok(Type::DoReplace),
            Rule::DoNothing => Ok(Type::DoNothing),
            Rule::DoFail => Ok(Type::DoFail),
            Rule::Eq => Ok(Type::Eq),
            Rule::Exists => Ok(Type::Exists),
            Rule::Explain => Ok(Type::Explain),
            Rule::False => Ok(Type::False),
            Rule::Function => Ok(Type::Function),
            Rule::Distinct => Ok(Type::Distinct),
            Rule::Distribution => Ok(Type::Distribution),
            Rule::Engine => Ok(Type::Engine),
            Rule::FunctionName => Ok(Type::FunctionName),
            Rule::Global => Ok(Type::Global),
            Rule::GrantPrivilege => Ok(Type::GrantPrivilege),
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
            Rule::IsNull => Ok(Type::IsNull),
            Rule::Ldap => Ok(Type::Ldap),
            Rule::Length => Ok(Type::Length),
            Rule::LeftJoinKind => Ok(Type::LeftJoinKind),
            Rule::Lt => Ok(Type::Lt),
            Rule::LtEq => Ok(Type::LtEq),
            Rule::Md5 => Ok(Type::Md5),
            Rule::Memtx => Ok(Type::Memtx),
            Rule::Multiplication => Ok(Type::Multiplication),
            Rule::Multiply => Ok(Type::Multiply),
            Rule::Name => Ok(Type::Name),
            Rule::NewProc => Ok(Type::NewProc),
            Rule::NewTable => Ok(Type::NewTable),
            Rule::Not => Ok(Type::Not),
            Rule::NotEq => Ok(Type::NotEq),
            Rule::NotFlag => Ok(Type::NotFlag),
            Rule::Null => Ok(Type::Null),
            Rule::Or => Ok(Type::Or),
            Rule::Password => Ok(Type::Password),
            Rule::Query => Ok(Type::Query),
            Rule::SqlVdbeMaxSteps => Ok(Type::SqlVdbeMaxSteps),
            Rule::VTableMaxRows => Ok(Type::VTableMaxRows),
            Rule::Parentheses => Ok(Type::Parentheses),
            Rule::Parameter => Ok(Type::Parameter),
            Rule::PgParameter => Ok(Type::PgParameter),
            Rule::Primary => Ok(Type::Primary),
            Rule::PrimaryKey => Ok(Type::PrimaryKey),
            Rule::PrimaryKeyColumn => Ok(Type::PrimaryKeyColumn),
            Rule::PrivBlockPrivilege => Ok(Type::PrivBlockPrivilege),
            Rule::PrivBlockUser => Ok(Type::PrivBlockUser),
            Rule::PrivBlockSpecificUser => Ok(Type::PrivBlockSpecificUser),
            Rule::PrivBlockRole => Ok(Type::PrivBlockRole),
            Rule::PrivBlockSpecificRole => Ok(Type::PrivBlockSpecificRole),
            Rule::PrivBlockTable => Ok(Type::PrivBlockTable),
            Rule::PrivBlockSpecificTable => Ok(Type::PrivBlockSpecificTable),
            Rule::PrivBlockRolePass => Ok(Type::PrivBlockRolePass),
            Rule::PrivilegeAlter => Ok(Type::PrivilegeAlter),
            Rule::PrivilegeCreate => Ok(Type::PrivilegeCreate),
            Rule::PrivilegeDrop => Ok(Type::PrivilegeDrop),
            Rule::PrivilegeExecute => Ok(Type::PrivilegeExecute),
            Rule::PrivilegeRead => Ok(Type::PrivilegeRead),
            Rule::PrivilegeSession => Ok(Type::PrivilegeSession),
            Rule::PrivilegeUsage => Ok(Type::PrivilegeUsage),
            Rule::PrivilegeWrite => Ok(Type::PrivilegeWrite),
            Rule::ProcBody => Ok(Type::ProcBody),
            Rule::ProcLanguage => Ok(Type::ProcLanguage),
            Rule::ProcParams => Ok(Type::ProcParams),
            Rule::ProcParamDef => Ok(Type::ProcParamDef),
            Rule::Projection => Ok(Type::Projection),
            Rule::Reference => Ok(Type::Reference),
            Rule::RevokePrivilege => Ok(Type::RevokePrivilege),
            Rule::RoleName => Ok(Type::RoleName),
            Rule::Row => Ok(Type::Row),
            Rule::Scan => Ok(Type::Scan),
            Rule::ScanName => Ok(Type::ScanName),
            Rule::ScanTable => Ok(Type::ScanTable),
            Rule::Sharding => Ok(Type::Sharding),
            Rule::ShardingColumn => Ok(Type::ShardingColumn),
            Rule::Select => Ok(Type::Select),
            Rule::SelectWithOptionalContinuation => Ok(Type::SelectWithOptionalContinuation),
            Rule::ExceptContinuation => Ok(Type::ExceptContinuation),
            Rule::UnionAllContinuation => Ok(Type::UnionAllContinuation),
            Rule::Selection => Ok(Type::Selection),
            Rule::SQL => Ok(Type::SQL),
            Rule::String => Ok(Type::String),
            Rule::SingleQuotedString => Ok(Type::SingleQuotedString),
            Rule::SubQuery => Ok(Type::SubQuery),
            Rule::Subtract => Ok(Type::Subtract),
            Rule::Table => Ok(Type::Table),
            Rule::TargetColumns => Ok(Type::TargetColumns),
            Rule::Timeout => Ok(Type::Timeout),
            Rule::TntParameter => Ok(Type::TntParameter),
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
            Rule::Update => Ok(Type::Update),
            Rule::UpdateList => Ok(Type::UpdateList),
            Rule::UpdateItem => Ok(Type::UpdateItem),
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
        let node = self.arena.get_mut(node).ok_or_else(|| {
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

    /// Rewrite `Update` AST to IR friendly one.
    ///
    /// `update t .. from s where expr` is transformed into
    /// ```text
    /// update t ..
    ///     join
    ///         scan t
    ///         s
    ///         condition expr
    /// ```
    ///
    /// `update t .. where expr` ->
    /// ```text
    /// update t ..
    ///     where expr
    ///         scan t
    /// ```
    ///
    /// `update t .. from s` ->
    /// ```text
    /// update t ..
    ///     join
    ///         scan t
    ///         s
    ///         condition true
    /// ```
    ///
    /// # Errors
    /// - invalid number of children for Update
    /// - unexpected rule of some child
    #[allow(clippy::too_many_lines)]
    pub(super) fn transform_update(&mut self) -> Result<(), SbroadError> {
        let update_id: usize = {
            let mut update_id: Option<usize> = None;
            for id in 0..self.nodes.arena.len() {
                let node = self.nodes.get_node(id)?;
                if node.rule == Type::Update {
                    update_id = Some(id);
                    break;
                }
            }
            if let Some(id) = update_id {
                id
            } else {
                return Ok(());
            }
        };
        let node = self.nodes.get_node(update_id)?;
        let table_id = *node.children.first().ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues(
                "expected Update ast node to have at least two children!".into(),
            )
        })?;
        let update_list_id = *node.children.get(1).ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues(
                "expected Update ast node to have at least two children!".into(),
            )
        })?;
        let upd_table_scan = ParseNode {
            children: vec![table_id],
            rule: Type::Scan,
            value: None,
        };
        let upd_table_scan_id = self.nodes.push_node(upd_table_scan);
        let node = self.nodes.get_node(update_id)?;
        match node.children.len() {
            // update t set ..
            2 => {
                self.nodes
                    .set_children(update_id, vec![upd_table_scan_id, table_id, update_list_id])?;
            }
            // update t set .. from .. OR update t set .. where ..
            3 => {
                // update t set a = 1 where id = 1
                let child_id = *node.children.get(2).unwrap();
                let is_selection = matches!(self.nodes.get_node(child_id)?.rule, Type::Selection);
                let update_child_id = if is_selection {
                    self.nodes.push_front_child(child_id, upd_table_scan_id)?;
                    child_id
                } else {
                    // update t set a = t.a + t1.b from t1
                    let condition_expr = ParseNode {
                        children: vec![],
                        rule: Type::True,
                        value: Some("true".into()),
                    };
                    let condition_expr_id = self.nodes.push_node(condition_expr);
                    let condition = ParseNode {
                        children: vec![condition_expr_id],
                        rule: Type::Condition,
                        value: None,
                    };
                    let inner_kind_id = self.nodes.push_node(ParseNode {
                        children: vec![],
                        rule: Type::InnerJoinKind,
                        value: None,
                    });
                    let condition_id = self.nodes.push_node(condition);
                    let join_node = ParseNode {
                        children: vec![upd_table_scan_id, inner_kind_id, child_id, condition_id],
                        rule: Type::Join,
                        value: None,
                    };
                    self.nodes.push_node(join_node)
                };
                self.nodes
                    .set_children(update_id, vec![update_child_id, table_id, update_list_id])?;
            }
            4 => {
                // update t set a = t.a + t1.b from t1 where expr
                let condition_id = *node.children.get(3).unwrap();
                let right_scan_id = *node.children.get(2).unwrap();
                let inner_kind_id = self.nodes.push_node(ParseNode {
                    children: vec![],
                    rule: Type::InnerJoinKind,
                    value: None,
                });
                let join_node = ParseNode {
                    children: vec![
                        upd_table_scan_id,
                        inner_kind_id,
                        right_scan_id,
                        condition_id,
                    ],
                    rule: Type::Join,
                    value: None,
                };
                let update_child_id = self.nodes.push_node(join_node);
                self.nodes
                    .set_children(update_id, vec![update_child_id, table_id, update_list_id])?;
            }
            _ => {
                return Err(SbroadError::UnexpectedNumberOfValues(
                    "expected Update ast node to have at most 4 children!".into(),
                ))
            }
        }
        Ok(())
    }

    /// Put delete table under delete filter to support reference resolution.
    pub(super) fn transform_delete(&mut self) -> Result<(), SbroadError> {
        let arena_len = self.nodes.arena.len();
        for id in 0..arena_len {
            let node = self.nodes.get_node(id)?;
            if node.rule != Type::Delete {
                continue;
            }
            let (table_id, filter_id) = if let (Some(table_id), Some(filter_id)) =
                (node.children.first(), node.children.get(1))
            {
                (*table_id, *filter_id)
            } else {
                continue;
            };
            let filter_node = self.nodes.get_node(filter_id)?;
            if filter_node.rule != Type::DeleteFilter {
                return Err(SbroadError::Invalid(
                    Entity::ParseNode,
                    Some(format!(
                        "expected delete filter as a second child, got: {filter_node:?}"
                    )),
                ));
            }
            let mut new_filter_children = Vec::with_capacity(filter_node.children.len() + 1);
            new_filter_children.push(table_id);
            new_filter_children.extend(filter_node.children.iter().copied());
            self.nodes.set_children(filter_id, new_filter_children)?;
            self.nodes.set_children(id, vec![filter_id])?;
        }
        Ok(())
    }

    /// `Select` node is not IR-friendly as it can have up to six children.
    /// Transform this node in IR-way (to a binary sub-tree).
    ///
    /// The task of `transform_select_i` functions is to build nested structure of rules.
    /// E.g. if we work with query `select ... from t ... where expr`, the initial structure like
    /// `Select [
    ///     children = [Projection, Scan(t), WhereClause(expr)]
    /// ]`
    /// will be transformed into
    /// `Select [
    ///     children = [Projection [
    ///         children = [Scan(t) [
    ///             children = [WhereClause(expr)]
    ///         ]
    ///     ]
    /// ]`
    ///
    /// At the end of `transform_select` work all `Select` nodes are replaced with
    /// their first child (always Projection):
    /// * In some node contained Select as a child, we replace that child with Select child
    /// * In case Select is a top node, it's replaced itself
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
        for node in &self.nodes.arena {
            if let Type::Projection = node.rule {
                let mut pos = 0;
                for child_id in &node.children {
                    let child = self.nodes.get_node(*child_id)?;
                    if let Type::Column = child.rule {
                        // Value or Expr.
                        let col_child_id = *child.children.first().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues("Column has no children".into())
                        })?;
                        let col_child = self.nodes.get_node(col_child_id)?;

                        let col_alias_id = child.children.get(1);
                        if let Some(col_alias_id) = col_alias_id {
                            let alias_name = parse_string_value_node(self, *col_alias_id)?;
                            columns.push((*child_id, Some(String::from(alias_name))));
                            continue;
                        }
                        if let Type::Reference = &col_child.rule {
                            let col_name_id: usize = if let (Some(_), Some(col_name_id)) =
                                (col_child.children.first(), col_child.children.get(1))
                            {
                                // Case of table_name.col_name.
                                *col_name_id
                            } else if let (Some(col_name_id), None) =
                                (col_child.children.first(), col_child.children.get(1))
                            {
                                // Case of col_name.
                                *col_name_id
                            } else {
                                return Err(SbroadError::NotFound(
                                    Entity::Node,
                                    "that is first child of the Column".into(),
                                ));
                            };
                            let col_name = parse_string_value_node(self, col_name_id)?;
                            columns.push((*child_id, Some(String::from(col_name))));
                        } else {
                            pos += 1;
                            columns.push((*child_id, Some(format!("COL_{pos}"))));
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
            if let Type::Asterisk = child.rule {
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
                        let filter = |node_id: usize| -> bool {
                            if let Ok(node) = self.nodes.get_node(node_id) {
                                matches!(node.rule, Type::Reference | Type::Asterisk)
                            } else {
                                false
                            }
                        };
                        let mut subtree = PostOrderWithFilter::with_capacity(
                            |node| self.nodes.ast_iter(node),
                            EXPR_CAPACITY,
                            Box::new(filter),
                        );
                        for (_, id) in subtree.iter(*top) {
                            if let Entry::Vacant(entry) = map.entry(id) {
                                entry.insert(vec![*rel_id]);
                            }
                        }
                    }
                }
                Type::Selection | Type::Having | Type::DeleteFilter => {
                    let rel_id = rel_node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(format!(
                            "AST {:?} has no children.",
                            rel_node.rule
                        ))
                    })?;
                    let filter_id = rel_node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "that is AST selection filter child with index 1".into(),
                        )
                    })?;
                    let filter = |node_id: usize| -> bool {
                        if let Ok(node) = self.nodes.get_node(node_id) {
                            matches!(node.rule, Type::Reference)
                        } else {
                            false
                        }
                    };
                    let mut subtree = PostOrderWithFilter::with_capacity(
                        |node| self.nodes.ast_iter(node),
                        EXPR_CAPACITY,
                        Box::new(filter),
                    );
                    for (_, id) in subtree.iter(*filter_id) {
                        if let Entry::Vacant(entry) = map.entry(id) {
                            entry.insert(vec![*rel_id]);
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
                    let filter = |node_id: usize| -> bool {
                        if let Ok(node) = self.nodes.get_node(node_id) {
                            matches!(node.rule, Type::Reference)
                        } else {
                            false
                        }
                    };
                    let mut subtree = PostOrderWithFilter::with_capacity(
                        |node| self.nodes.ast_iter(node),
                        EXPR_CAPACITY,
                        Box::new(filter),
                    );
                    for (_, id) in subtree.iter(*cond_id) {
                        if let Entry::Vacant(entry) = map.entry(id) {
                            entry.insert(vec![*left_id, *right_id]);
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
                        let filter = |node_id: usize| -> bool {
                            if let Ok(node) = self.nodes.get_node(node_id) {
                                matches!(node.rule, Type::Reference)
                            } else {
                                false
                            }
                        };
                        let mut subtree = PostOrderWithFilter::with_capacity(
                            |node| self.nodes.ast_iter(node),
                            EXPR_CAPACITY,
                            Box::new(filter),
                        );
                        for (_, id) in subtree.iter(*top) {
                            if let Entry::Vacant(entry) = map.entry(id) {
                                entry.insert(vec![*rel_id]);
                            }
                        }
                    }
                }
                Type::Update => {
                    let rel_id = rel_node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "Update AST doesn't have any children.".into(),
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
