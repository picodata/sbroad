//! SQL frontend module.
//!
//! Parses an SQL statement to the abstract syntax tree (AST)
//! and builds the intermediate representation (IR).

use pest::Parser;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::{
    helpers::{normalize_name_for_space_api, normalize_name_from_sql},
    Metadata,
};
use crate::frontend::sql::ast::{
    AbstractSyntaxTree, ParseNode, ParseNodes, ParseTree, Rule, StackParseNode, Type,
};
use crate::frontend::sql::ir::Translation;
use crate::frontend::Ast;
use crate::ir::ddl::{ColumnDef, Ddl};
use crate::ir::expression::cast::Type as CastType;
use crate::ir::expression::{ColumnPositionMap, Expression, ExpressionId};
use crate::ir::operator::{Arithmetic, Bool, ConflictStrategy, JoinKind, Relational, Unary};
use crate::ir::relation::{Column, ColumnRole, Type as RelationType};
use crate::ir::tree::traversal::PostOrder;
use crate::ir::value::Value;
use crate::ir::{Node, OptionKind, OptionSpec, Plan};
use crate::otm::child_span;

use crate::errors::Entity::AST;
use crate::ir::acl::AlterOption;
use crate::ir::acl::{Acl, GrantRevokeType, Privilege};
use crate::ir::aggregates::AggregateKind;
use crate::ir::helpers::RepeatableState;
use crate::ir::transformation::redistribution::ColumnPosition;
use sbroad_proc::otm_child_span;
use tarantool::decimal::Decimal;
use tarantool::space::SpaceEngineType;

// DDL timeout in seconds (1 day).
const DEFAULT_TIMEOUT_F64: f64 = 24.0 * 60.0 * 60.0;
const DEFAULT_AUTH_METHOD: &str = "chap-sha1";

fn get_default_timeout() -> Decimal {
    Decimal::from_str(&format!("{DEFAULT_TIMEOUT_F64}")).unwrap()
}

fn get_default_auth_method() -> String {
    String::from(DEFAULT_AUTH_METHOD)
}

/// Helper structure to fix the double linking
/// problem in the BETWEEN operator.
/// We transform `left BETWEEN center AND right` to
/// `left >= center AND left <= right`.
struct Between {
    /// Left node id.
    left_id: usize,
    /// Less or equal node id (`left <= right`)
    less_eq_id: usize,
}

impl Between {
    fn new(left_id: usize, less_eq_id: usize) -> Self {
        Self {
            left_id,
            less_eq_id,
        }
    }
}

#[allow(clippy::uninlined_format_args)]
fn get_timeout(ast: &AbstractSyntaxTree, node_id: usize) -> Result<Decimal, SbroadError> {
    let param_node = ast.nodes.get_node(node_id)?;
    if let (Some(duration_id), None) = (param_node.children.first(), param_node.children.get(1)) {
        let duration_node = ast.nodes.get_node(*duration_id)?;
        if duration_node.rule != Type::Duration {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!(
                    "AST table option duration node {:?} contains unexpected children",
                    duration_node,
                )),
            ));
        }
        if let Some(duration_value) = duration_node.value.as_ref() {
            let res = Decimal::from_str(duration_value).map_err(|_| {
                SbroadError::Invalid(
                    Entity::Node,
                    Some(format!(
                        "AST table duration node {:?} contains invalid value",
                        duration_node,
                    )),
                )
            })?;
            return Ok(res);
        }
        return Err(SbroadError::Invalid(
            Entity::AST,
            Some("Duration node has no value".into()),
        ));
    }
    Err(SbroadError::Invalid(
        Entity::AST,
        Some("expected Timeout node to have exactly one child".into()),
    ))
}

/// Parse node from which we want to get String value.
fn parse_string_value_node(ast: &AbstractSyntaxTree, node_id: usize) -> Result<&str, SbroadError> {
    let string_value_node = ast.nodes.get_node(node_id)?;
    let string_value = string_value_node
        .value
        .as_ref()
        .expect("Rule node must contain string value.");
    Ok(string_value.as_str())
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::uninlined_format_args)]
fn parse_create_table(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Ddl, SbroadError> {
    if node.rule != Type::CreateTable {
        return Err(SbroadError::Invalid(
            Entity::Type,
            Some("create table".into()),
        ));
    }
    let mut table_name: String = String::new();
    let mut columns: Vec<ColumnDef> = Vec::new();
    let mut pk_keys: Vec<String> = Vec::new();
    let mut shard_key: Vec<String> = Vec::new();
    let mut engine_type: SpaceEngineType = SpaceEngineType::default();
    let mut timeout = get_default_timeout();
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Type::NewTable => {
                table_name = normalize_name_for_space_api(parse_string_value_node(ast, *child_id)?);
            }
            Type::Columns => {
                let columns_node = ast.nodes.get_node(*child_id)?;
                for col_id in &columns_node.children {
                    let mut column_def = ColumnDef::default();
                    let column_def_node = ast.nodes.get_node(*col_id)?;
                    for def_child_id in &column_def_node.children {
                        let def_child_node = ast.nodes.get_node(*def_child_id)?;
                        match def_child_node.rule {
                            Type::ColumnDefName => {
                                column_def.name = normalize_name_for_space_api(
                                    parse_string_value_node(ast, *def_child_id)?,
                                );
                            }
                            Type::ColumnDefType => {
                                if let (Some(type_id), None) = (
                                    def_child_node.children.first(),
                                    def_child_node.children.get(1),
                                ) {
                                    let type_node = ast.nodes.get_node(*type_id)?;
                                    match type_node.rule {
                                        Type::TypeBool => {
                                            column_def.data_type = RelationType::Boolean;
                                        }
                                        Type::TypeDecimal => {
                                            column_def.data_type = RelationType::Decimal;
                                        }
                                        Type::TypeDouble => {
                                            column_def.data_type = RelationType::Double;
                                        }
                                        Type::TypeInt => {
                                            column_def.data_type = RelationType::Integer;
                                        }
                                        Type::TypeNumber => {
                                            column_def.data_type = RelationType::Number;
                                        }
                                        Type::TypeScalar => {
                                            column_def.data_type = RelationType::Scalar;
                                        }
                                        Type::TypeString | Type::TypeText | Type::TypeVarchar => {
                                            column_def.data_type = RelationType::String;
                                        }
                                        Type::TypeUnsigned => {
                                            column_def.data_type = RelationType::Unsigned;
                                        }
                                        _ => {
                                            return Err(SbroadError::Invalid(
                                                Entity::Node,
                                                Some(format!(
                                                    "AST column type node {:?} has unexpected type",
                                                    type_node,
                                                )),
                                            ));
                                        }
                                    }
                                } else {
                                    return Err(SbroadError::Invalid(
                                        Entity::Node,
                                        Some(format!(
                                            "AST column type node {:?} contains unexpected children",
                                            def_child_node,
                                        )),
                                    ));
                                }
                            }
                            Type::ColumnDefIsNull => {
                                match (def_child_node.children.first(), def_child_node.children.get(1)) {
                                    (None, None) => {
                                        column_def.is_nullable = true;
                                    }
                                    (Some(child_id), None) => {
                                        let not_flag_node = ast.nodes.get_node(*child_id)?;
                                        if let Type::NotFlag = not_flag_node.rule {
                                            column_def.is_nullable = false;
                                        } else {
                                            return Err(SbroadError::Invalid(
                                                Entity::Node,
                                                Some(format!(
                                                    "Expected NotFlag node, got: {:?}",
                                                    not_flag_node,
                                                ))))
                                        }
                                    }
                                    _ => return Err(SbroadError::Invalid(
                                        Entity::Node,
                                        Some(format!(
                                            "AST column null node {:?} contains unexpected children",
                                            def_child_node,
                                        )),
                                    )),
                                }
                            }
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format!(
                                        "AST column def node {:?} contains unexpected children",
                                        def_child_node,
                                    )),
                                ));
                            }
                        }
                    }
                    columns.push(column_def);
                }
            }
            Type::PrimaryKey => {
                let pk_node = ast.nodes.get_node(*child_id)?;
                for pk_col_id in &pk_node.children {
                    let pk_col_node = ast.nodes.get_node(*pk_col_id)?;
                    if pk_col_node.rule != Type::PrimaryKeyColumn {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some(format!(
                                "AST primary key node {:?} contains unexpected children",
                                pk_col_node,
                            )),
                        ));
                    }
                    let pk_col_name =
                        normalize_name_for_space_api(parse_string_value_node(ast, *pk_col_id)?);
                    let mut column_found = false;
                    for column in &columns {
                        if column.name == pk_col_name {
                            column_found = true;
                            if column.is_nullable {
                                return Err(SbroadError::Invalid(
                                    Entity::Column,
                                    Some(String::from(
                                        "Primary key mustn't contain nullable columns",
                                    )),
                                ));
                            }
                        }
                    }
                    if !column_found {
                        return Err(SbroadError::Invalid(
                            Entity::Column,
                            Some(format!("Primary key column {pk_col_name} not found.")),
                        ));
                    }
                    pk_keys.push(pk_col_name);
                }
            }
            Type::Engine => {
                if let (Some(engine_type_id), None) =
                    (child_node.children.first(), child_node.children.get(1))
                {
                    let engine_type_node = ast.nodes.get_node(*engine_type_id)?;
                    match engine_type_node.rule {
                        Type::Memtx => {
                            engine_type = SpaceEngineType::Memtx;
                        }
                        Type::Vinyl => {
                            // todo: when global spaces will be supported
                            // check that vinyl space is not global.
                            engine_type = SpaceEngineType::Vinyl;
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some(format!(
                                    "AST table engine node {:?} contains unexpected children",
                                    engine_type_node,
                                )),
                            ));
                        }
                    }
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Node,
                        Some(format!(
                            "AST table engine node {:?} contains unexpected children",
                            child_node,
                        )),
                    ));
                }
            }
            Type::Distribution => {
                let distribution_node = ast.nodes.get_node(*child_id)?;
                if let (Some(distribution_type_id), None) = (
                    distribution_node.children.first(),
                    distribution_node.children.get(1),
                ) {
                    let distribution_type_node = ast.nodes.get_node(*distribution_type_id)?;
                    match distribution_type_node.rule {
                        Type::Global => {}
                        Type::Sharding => {
                            let shard_node = ast.nodes.get_node(*distribution_type_id)?;
                            for shard_col_id in &shard_node.children {
                                let shard_col_node = ast.nodes.get_node(*shard_col_id)?;
                                if shard_col_node.rule != Type::ShardingColumn {
                                    return Err(SbroadError::Invalid(
                                        Entity::Node,
                                        Some(format!(
                                            "AST shard key node {:?} contains unexpected children",
                                            shard_col_node,
                                        )),
                                    ));
                                }
                                let shard_col_name = normalize_name_for_space_api(
                                    parse_string_value_node(ast, *shard_col_id)?,
                                );

                                let column_found = columns.iter().any(|c| c.name == shard_col_name);
                                if !column_found {
                                    return Err(SbroadError::Invalid(
                                        Entity::Column,
                                        Some(format!(
                                            "Sharding key column {shard_col_name} not found."
                                        )),
                                    ));
                                }

                                shard_key.push(shard_col_name);
                            }
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some(format!(
                                    "AST table distribution node {:?} contains unexpected children",
                                    distribution_type_node,
                                )),
                            ));
                        }
                    }
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Node,
                        Some(format!(
                            "AST table distribution node {:?} contains unexpected children",
                            child_node,
                        )),
                    ));
                }
            }
            Type::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!(
                        "AST create table node {:?} contains unexpected children",
                        child_node,
                    )),
                ));
            }
        }
    }
    let create_sharded_table = if shard_key.is_empty() {
        if engine_type != SpaceEngineType::Memtx {
            return Err(SbroadError::Unsupported(
                Entity::Query,
                Some("global spaces can use only memtx engine".into()),
            ));
        }
        Ddl::CreateTable {
            name: table_name,
            format: columns,
            primary_key: pk_keys,
            sharding_key: None,
            engine_type,
            timeout,
        }
    } else {
        Ddl::CreateTable {
            name: table_name,
            format: columns,
            primary_key: pk_keys,
            sharding_key: Some(shard_key),
            engine_type,
            timeout,
        }
    };
    Ok(create_sharded_table)
}

/// Get String value under `RoleName` node.
fn parse_role_name(ast: &AbstractSyntaxTree, node_id: usize) -> Result<String, SbroadError> {
    Ok(normalize_name_for_space_api(parse_string_value_node(
        ast, node_id,
    )?))
}

/// Common logic for parsing GRANT/REVOKE queries.
#[allow(clippy::too_many_lines)]
fn parse_grant_revoke(
    node: &ParseNode,
    ast: &AbstractSyntaxTree,
) -> Result<(GrantRevokeType, String, Decimal), SbroadError> {
    let privilege_block_node_id = node
        .children
        .first()
        .expect("Specific privilege block expected under GRANT/REVOKE");
    let privilege_block_node = ast.nodes.get_node(*privilege_block_node_id)?;
    let grant_revoke_type = match privilege_block_node.rule {
        Type::PrivBlockPrivilege => {
            let privilege_node_id = privilege_block_node
                .children
                .first()
                .expect("Expected to see Privilege under PrivBlockPrivilege");
            let privilege_node = ast.nodes.get_node(*privilege_node_id)?;
            let privilege = match privilege_node.rule {
                Type::PrivilegeAlter => Privilege::Alter,
                Type::PrivilegeCreate => Privilege::Create,
                Type::PrivilegeDrop => Privilege::Drop,
                Type::PrivilegeExecute => Privilege::Execute,
                Type::PrivilegeRead => Privilege::Read,
                Type::PrivilegeSession => Privilege::Session,
                Type::PrivilegeUsage => Privilege::Usage,
                Type::PrivilegeWrite => Privilege::Write,
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Privilege,
                        Some(format!(
                            "Expected to see Privilege node. Got: {privilege_node:?}"
                        )),
                    ))
                }
            };
            let inner_privilege_block_node_id = privilege_block_node
                .children
                .get(1)
                .expect("Expected to see inner priv block under PrivBlockPrivilege");
            let inner_privilege_block_node = ast.nodes.get_node(*inner_privilege_block_node_id)?;
            match inner_privilege_block_node.rule {
                Type::PrivBlockUser => GrantRevokeType::user(privilege)?,
                Type::PrivBlockSpecificUser => {
                    let user_name_node_id = inner_privilege_block_node
                        .children
                        .first()
                        .expect("Expected to see RoleName under PrivBlockSpecificUser");
                    let user_name = parse_role_name(ast, *user_name_node_id)?;
                    GrantRevokeType::specific_user(privilege, user_name)?
                }
                Type::PrivBlockRole => GrantRevokeType::role(privilege)?,
                Type::PrivBlockSpecificRole => {
                    let role_name_node_id = inner_privilege_block_node
                        .children
                        .first()
                        .expect("Expected to see RoleName under PrivBlockSpecificUser");
                    let role_name = parse_role_name(ast, *role_name_node_id)?;
                    GrantRevokeType::specific_role(privilege, role_name)?
                }
                Type::PrivBlockTable => GrantRevokeType::table(privilege)?,
                Type::PrivBlockSpecificTable => {
                    let table_node_id = inner_privilege_block_node.children.first().expect(
                        "Expected to see TableName as a first child of PrivBlockSpecificTable",
                    );
                    let table_name =
                        normalize_name_for_space_api(parse_string_value_node(ast, *table_node_id)?);
                    GrantRevokeType::specific_table(privilege, table_name)?
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::ParseNode,
                        Some(format!(
                            "Expected specific priv block, got: {inner_privilege_block_node:?}"
                        )),
                    ))
                }
            }
        }
        Type::PrivBlockRolePass => {
            let role_name_id = privilege_block_node
                .children
                .first()
                .expect("RoleName must be a first child of PrivBlockRolePass");
            let role_name = parse_role_name(ast, *role_name_id)?;
            GrantRevokeType::role_pass(role_name)
        }
        _ => {
            return Err(SbroadError::Invalid(
                Entity::ParseNode,
                Some(format!(
                    "Expected specific priv block, got: {privilege_block_node:?}"
                )),
            ))
        }
    };

    let grantee_name_node_id = node
        .children
        .get(1)
        .expect("RoleName must be a second child of GRANT/REVOKE");
    let grantee_name = parse_role_name(ast, *grantee_name_node_id)?;

    let mut timeout = get_default_timeout();
    if let Some(timeout_child_id) = node.children.get(2) {
        timeout = get_timeout(ast, *timeout_child_id)?;
    }

    Ok((grant_revoke_type, grantee_name, timeout))
}

impl Ast for AbstractSyntaxTree {
    /// Build an empty AST.
    fn empty() -> Self {
        AbstractSyntaxTree {
            nodes: ParseNodes::new(),
            top: None,
            map: HashMap::new(),
        }
    }

    /// Constructor.
    /// Builds a tree (nodes are in postorder reverse).
    ///
    /// # Errors
    /// - Failed to parse an SQL query.
    #[otm_child_span("ast.parse")]
    fn new(query: &str) -> Result<Self, SbroadError> {
        let mut ast = AbstractSyntaxTree::empty();

        let mut command_pair = match ParseTree::parse(Rule::Command, query) {
            Ok(p) => p,
            Err(e) => return Err(SbroadError::ParsingError(Entity::Rule, format!("{e}"))),
        };
        let top_pair = command_pair.next().ok_or_else(|| {
            SbroadError::UnexpectedNumberOfValues("no query found in the parse tree.".to_string())
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

        ast.transform_update()?;
        ast.transform_delete()?;
        ast.transform_select()?;
        ast.add_aliases_to_projection()?;
        ast.build_ref_to_relation_map()?;

        Ok(ast)
    }

    fn is_empty(&self) -> bool {
        self.nodes.arena.is_empty()
    }

    /// Function that transforms `AbstractSyntaxTree` into `Plan`.
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::uninlined_format_args)]
    #[otm_child_span("ast.resolve")]
    fn resolve_metadata<M>(&self, metadata: &M) -> Result<Plan, SbroadError>
    where
        M: Metadata,
    {
        let mut plan = Plan::default();

        let Some(top) = self.top else {
            return Err(SbroadError::Invalid(Entity::AST, None))
        };
        let capacity = self.nodes.arena.len();
        let mut dft_post = PostOrder::with_capacity(|node| self.nodes.ast_iter(node), capacity);
        // Map of { `ParseNode` id -> `Node` id }.
        let mut map = Translation::with_capacity(self.nodes.next_id());
        // Set of all `Expression::Row` generated from AST.
        let mut rows: HashSet<usize> = HashSet::with_capacity(self.nodes.next_id());
        // Counter for `Expression::ValuesRow` output column name aliases ("COLUMN_<`col_idx`>").
        // Is it global for every `ValuesRow` met in the AST.
        let mut col_idx: usize = 0;

        let mut betweens: Vec<Between> = Vec::new();
        // Ids of arithmetic expressions that have parentheses.
        let mut arith_expr_with_parentheses_ids: Vec<usize> = Vec::new();

        // Closure to retrieve arithmetic expression under parenthesis.
        let get_arithmetic_plan_id = |plan: &mut Plan,
                                      map: &Translation,
                                      arith_expr_with_parentheses_ids: &mut Vec<usize>,
                                      rows: &mut HashSet<usize>,
                                      ast_id: usize|
         -> Result<usize, SbroadError> {
            let plan_id;
            // If child of current multiplication or addition is `(expr)` then
            // we need to get expr that is child of `()` and add it to the plan
            // also we will mark this expr to add in the future `()`
            let arithmetic_parse_node = self.nodes.get_node(ast_id)?;
            if arithmetic_parse_node.rule == Type::ArithParentheses {
                let arithmetic_id = arithmetic_parse_node
                    .children
                    .first()
                    .expect("ArithParentheses has no children.");
                plan_id = plan.as_row(map.get(*arithmetic_id)?, rows)?;
                arith_expr_with_parentheses_ids.push(plan_id);
            } else {
                plan_id = plan.as_row(map.get(ast_id)?, rows)?;
            }

            Ok(plan_id)
        };

        // Closure to add arithmetic expression operator to plan and get id of newly added node.
        let get_arithmetic_op_id = |plan: &mut Plan,
                                    current_node: &ParseNode,
                                    map: &Translation,
                                    arith_expr_with_parentheses_ids: &mut Vec<usize>,
                                    rows: &mut HashSet<usize>| {
            let ast_left_id = current_node
                .children
                .first()
                .expect("Multiplication or Addition has no children.");
            let plan_left_id = get_arithmetic_plan_id(
                plan,
                map,
                arith_expr_with_parentheses_ids,
                rows,
                *ast_left_id,
            )?;

            let ast_right_id = current_node.children.get(2).expect(
                "that is right node with index 2 among Multiplication or Addition children",
            );
            let plan_right_id = get_arithmetic_plan_id(
                plan,
                map,
                arith_expr_with_parentheses_ids,
                rows,
                *ast_right_id,
            )?;

            let ast_op_id = current_node.children.get(1).expect("that is center node (operator) with index 1 among Multiplication or Addition children");

            let op_node = self.nodes.get_node(*ast_op_id)?;
            let op = Arithmetic::from_node_type(&op_node.rule)?;
            // Even though arithmetic expression is added without parenthesis here, they will
            // be added later with `fix_arithmetic_parentheses` function call.
            let op_id = plan.add_arithmetic_to_plan(plan_left_id, op, plan_right_id, false)?;
            Ok::<usize, SbroadError>(op_id)
        };

        for (_, id) in dft_post.iter(top) {
            let node = self.nodes.get_node(id)?;
            match &node.rule {
                Type::Scan => {
                    let ast_scan_id = node
                        .children
                        .first()
                        .expect("could not find first child id in scan node");
                    let plan_child_id = map.get(*ast_scan_id)?;
                    map.add(id, plan_child_id);
                    if let Some(ast_alias_id) = node.children.get(1) {
                        let ast_alias = self.nodes.get_node(*ast_alias_id)?;
                        if let Type::ScanName = ast_alias.rule {
                            let ast_alias_name =
                                ast_alias.value.as_deref().map(normalize_name_from_sql);
                            // Update scan name in the plan.
                            let scan = plan.get_mut_relation_node(plan_child_id)?;
                            scan.set_scan_name(ast_alias_name)?;
                        } else {
                            return Err(SbroadError::Invalid(
                                Entity::Type,
                                Some("expected scan name AST node.".into()),
                            ));
                        }
                    }
                }
                Type::ScanTable => {
                    let ast_table_id = node
                        .children
                        .first()
                        .expect("could not find first child id in scan table node");
                    let ast_table = self.nodes.get_node(*ast_table_id)?;
                    match ast_table.value {
                        Some(ref table) if ast_table.rule == Type::Table => {
                            let scan_id = plan.add_scan(&normalize_name_from_sql(table), None)?;
                            map.add(id, scan_id);
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Type,
                                Some("Table scan child must be a valid table.".into()),
                            ));
                        }
                    }
                }
                Type::Table => {
                    if let Some(node_val) = &node.value {
                        let table = node_val.as_str();
                        let t = metadata.table(table)?;
                        plan.add_rel(t);
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Type,
                            Some("Table name is not found.".into()),
                        ));
                    }
                }
                Type::SubQuery => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("child node id is not found among sub-query children.");
                    let plan_child_id = map.get(*ast_child_id)?;
                    let plan_sq_id = plan.add_sub_query(plan_child_id, None)?;
                    map.add(id, plan_sq_id);
                }
                Type::Reference => {
                    let ast_rel_list = self.get_referred_relational_nodes(id)?;
                    let mut plan_rel_list = Vec::with_capacity(ast_rel_list.len());
                    for ast_id in ast_rel_list {
                        let plan_id = map.get(ast_id)?;
                        plan_rel_list.push(plan_id);
                    }

                    // Closure to get uppercase name from AST `Name` node.
                    let get_name = |ast_id: usize| -> Result<String, SbroadError> {
                        Ok(normalize_name_from_sql(parse_string_value_node(
                            self, ast_id,
                        )?))
                    };

                    let plan_left_id = plan_rel_list.first();
                    let plan_right_id = plan_rel_list.get(1);
                    if let (Some(plan_left_id), Some(plan_right_id)) = (plan_left_id, plan_right_id)
                    {
                        // Handling case of referencing join node.

                        // Get column position maps for the left and right children
                        // of the join node.
                        let left_col_map = ColumnPositionMap::new(&plan, *plan_left_id)?;
                        let right_col_map = ColumnPositionMap::new(&plan, *plan_right_id)?;

                        // We get both column and scan names from the AST.
                        if let (Some(ast_scan_name_id), Some(ast_col_name_id)) =
                            (node.children.first(), node.children.get(1))
                        {
                            let scan_name = get_name(*ast_scan_name_id)?;
                            // Get the column name and its positions in the output tuples.
                            let col_name = get_name(*ast_col_name_id)?;
                            // First, try to find the column name in the left child.
                            let present_in_left = left_col_map
                                .get_with_scan(&col_name, Some(&scan_name))
                                .is_ok();
                            let ref_id = if present_in_left {
                                plan.add_row_from_left_branch(
                                    *plan_left_id,
                                    *plan_right_id,
                                    &[&col_name],
                                )?
                            } else {
                                // If it is not found, try to find it in the right child.
                                let present_in_right = right_col_map
                                    .get_with_scan(&col_name, Some(&scan_name))
                                    .is_ok();
                                if present_in_right {
                                    plan.add_row_from_right_branch(
                                        *plan_left_id,
                                        *plan_right_id,
                                        &[&col_name],
                                    )?
                                } else {
                                    return Err(SbroadError::NotFound(
                                        Entity::Column,
                                        format!("'{col_name}' in the join children",),
                                    ));
                                }
                            };
                            rows.insert(ref_id);
                            map.add(id, ref_id);
                        // We get only column name from the AST.
                        } else if let (Some(ast_col_name_id), None) =
                            (node.children.first(), node.children.get(1))
                        {
                            // Determine the referred side of the join (left or right).
                            let col_name = get_name(*ast_col_name_id)?;

                            // We need to check that the column name is unique in the join children.
                            // Otherwise, we cannot determine the referred side of the join
                            // and the user should specify the scan name in the SQL.
                            let present_in_left = left_col_map.get(&col_name).is_ok();
                            let present_in_right = right_col_map.get(&col_name).is_ok();
                            if present_in_left && present_in_right {
                                return Err(SbroadError::Invalid(
                                    Entity::Column,
                                    Some(format!(
                                        "column name '{col_name}' is present in both join children",
                                    )),
                                ));
                            } else if present_in_left {
                                let ref_id = plan.add_row_from_left_branch(
                                    *plan_left_id,
                                    *plan_right_id,
                                    &[&col_name],
                                )?;
                                rows.insert(ref_id);
                                map.add(id, ref_id);
                            } else if present_in_right {
                                let ref_id = plan.add_row_from_right_branch(
                                    *plan_left_id,
                                    *plan_right_id,
                                    &[&col_name],
                                )?;
                                rows.insert(ref_id);
                                map.add(id, ref_id);
                            } else {
                                return Err(SbroadError::NotFound(
                                    Entity::Column,
                                    format!("'{col_name}' in the join left or right children"),
                                ));
                            }
                        } else {
                            return Err(SbroadError::UnexpectedNumberOfValues(
                                "expected children nodes contain a column name.".into(),
                            ));
                        };
                    } else if let (Some(plan_rel_id), None) = (plan_left_id, plan_right_id) {
                        // Handling case of referencing a single child node.

                        let first_child_id = node.children.first();
                        let second_child_id = node.children.get(1);
                        if let (Some(ast_scan_name_id), Some(ast_col_name_id)) =
                            (first_child_id, second_child_id)
                        {
                            // Get column name.
                            let col_name = get_name(*ast_col_name_id)?;
                            // Check that scan name in the reference matches to the one in scan node.
                            let scan_name = get_name(*ast_scan_name_id)?;
                            let col_position = ColumnPositionMap::new(&plan, *plan_rel_id)?
                                .get_with_scan(&col_name, Some(&scan_name))?;
                            // Manually build the reference node.
                            let child = plan.get_relation_node(*plan_rel_id)?;
                            let child_alias_ids =
                                plan.get_expression_node(child.output())?.get_row_list()?;
                            let child_alias_id = child_alias_ids
                                .get(col_position)
                                .expect("column position is invalid");
                            let col_type = plan
                                .get_expression_node(*child_alias_id)?
                                .calculate_type(&plan)?;
                            let ref_id =
                                plan.nodes
                                    .add_ref(None, Some(vec![0]), col_position, col_type);
                            map.add(id, ref_id);
                        } else if let (Some(ast_col_name_id), None) =
                            (first_child_id, second_child_id)
                        {
                            // Get the column name.
                            let col_name = get_name(*ast_col_name_id)?;
                            let ref_list = plan.new_columns(
                                &[*plan_rel_id],
                                false,
                                &[0],
                                &[&col_name],
                                false,
                                true,
                            )?;
                            let ref_id = *ref_list.first().ok_or_else(|| {
                                SbroadError::UnexpectedNumberOfValues(
                                    "Referred column is not found.".into(),
                                )
                            })?;
                            map.add(id, ref_id);
                        } else {
                            return Err(SbroadError::UnexpectedNumberOfValues(
                                "no child node found in the AST reference.".into(),
                            ));
                        };
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "expected one or two referred relational nodes, got less or more."
                                .into(),
                        ));
                    }
                }
                Type::Integer
                | Type::Decimal
                | Type::Double
                | Type::Unsigned
                | Type::String
                | Type::Null
                | Type::True
                | Type::False => {
                    let val = Value::from_node(node)?;
                    map.add(id, plan.add_const(val));
                }
                Type::VTableMaxRows => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("no children for sql_vdbe_max_steps option");
                    let ast_child_node = self.nodes.get_node(*ast_child_id)?;
                    let val: Option<Value> = match ast_child_node.rule {
                        Type::Parameter => None,
                        Type::Unsigned => {
                            let v = {
                                if let Some(str_value) = ast_child_node.value.as_ref() {
                                    str_value.parse::<u64>().map_err(|_|
                                        SbroadError::Invalid(Entity::Query,
                                                             Some(format!("sql_vdbe_max_steps value is not unsigned integer: {str_value}")))
                                    )?
                                } else {
                                    return Err(SbroadError::Invalid(
                                        AST,
                                        Some("Unsigned node has value".into()),
                                    ));
                                }
                            };
                            Some(Value::Unsigned(v))
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                AST,
                                Some(format!(
                                "unexpected child of sql_vdbe_max_steps option. id: {ast_child_id}"
                            )),
                            ))
                        }
                    };
                    plan.raw_options.push(OptionSpec {
                        kind: OptionKind::VTableMaxRows,
                        val,
                    });
                }
                Type::SqlVdbeMaxSteps => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("no children for sql_vdbe_max_steps option");
                    let ast_child_node = self.nodes.get_node(*ast_child_id)?;
                    let val: Option<Value> = match ast_child_node.rule {
                        Type::Parameter => None,
                        Type::Unsigned => {
                            let v = {
                                if let Some(str_value) = ast_child_node.value.as_ref() {
                                    str_value.parse::<u64>().map_err(|_|
                                    SbroadError::Invalid(Entity::Query,
                                                         Some(format!("sql_vdbe_max_steps value is not unsigned integer: {str_value}")))
                                    )?
                                } else {
                                    return Err(SbroadError::Invalid(
                                        AST,
                                        Some("Unsigned node has value".into()),
                                    ));
                                }
                            };
                            Some(Value::Unsigned(v))
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                AST,
                                Some(format!(
                                "unexpected child of sql_vdbe_max_steps option. id: {ast_child_id}"
                            )),
                            ))
                        }
                    };
                    plan.raw_options.push(OptionSpec {
                        kind: OptionKind::SqlVdbeMaxSteps,
                        val,
                    });
                }
                Type::Parameter => {
                    map.add(id, plan.add_param());
                }
                Type::Asterisk => {
                    // We can get an asterisk only in projection.
                    let ast_rel_list = self.get_referred_relational_nodes(id)?;
                    let mut plan_rel_list = Vec::new();
                    for ast_id in ast_rel_list {
                        let plan_id = map.get(ast_id)?;
                        plan_rel_list.push(plan_id);
                    }
                    if plan_rel_list.len() > 1 {
                        return Err(SbroadError::NotImplemented(
                            Entity::SubQuery,
                            "in projections".into(),
                        ));
                    }
                    let plan_rel_id = *plan_rel_list.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "list of referred relational nodes is empty.".into(),
                        )
                    })?;
                    let plan_asterisk_id = plan.add_row_for_output(plan_rel_id, &[], false)?;
                    map.add(id, plan_asterisk_id);
                }
                Type::Alias => {
                    let ast_ref_id = node
                        .children
                        .first()
                        .expect("list of alias children is empty, Reference node id is not found.");
                    let plan_ref_id = map.get(*ast_ref_id)?;
                    let ast_name_id = node.children.get(1).expect("(Alias name) with index 1");
                    let name = parse_string_value_node(self, *ast_name_id)?;
                    let plan_alias_id = plan
                        .nodes
                        .add_alias(&normalize_name_from_sql(name), plan_ref_id)?;
                    map.add(id, plan_alias_id);
                }
                Type::Column => {
                    let ast_child_id = node.children.first().expect("Column has no children.");
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(id, plan_child_id);
                }
                Type::Row => {
                    let mut plan_col_list = Vec::new();
                    for ast_child_id in &node.children {
                        let plan_child_id = map.get(*ast_child_id)?;
                        // If the child is a row that was generated by our
                        // reference-to-row logic in AST code, we should unwrap it back.
                        let plan_id = if rows.get(&plan_child_id).is_some() {
                            let plan_inner_expr = plan.get_expression_node(plan_child_id)?;
                            *plan_inner_expr.get_row_list()?.first().ok_or_else(|| {
                                SbroadError::UnexpectedNumberOfValues("Row is empty.".into())
                            })?
                        } else {
                            plan_child_id
                        };
                        plan_col_list.push(plan_id);
                    }
                    let plan_row_id = plan.nodes.add_row(plan_col_list, None);
                    map.add(id, plan_row_id);
                }
                Type::And | Type::Or => {
                    let ast_left_id = node.children.first().expect("Comparison has no children.");
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_right_id = node
                        .children
                        .get(1)
                        .expect("that is right node with index 1 among comparison children");
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;
                    let op = Bool::from_node_type(&node.rule)?;
                    let cond_id = plan.add_cond(plan_left_id, op, plan_right_id)?;
                    map.add(id, cond_id);
                }
                Type::Cmp => {
                    let ast_left_id = node.children.first().expect("Comparison has no children.");
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_right_id = node
                        .children
                        .get(2)
                        .expect("that is right node with index 2 among comparison children");
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;
                    let ast_op_id = node
                        .children
                        .get(1)
                        .expect("that is operator node with index 1 among comparison children");
                    let op_node = self.nodes.get_node(*ast_op_id)?;
                    let op = Bool::from_node_type(&op_node.rule)?;
                    let is_not = match op {
                        Bool::In => {
                            matches!(op_node.children.first(), Some(_))
                        }
                        _ => false,
                    };
                    let cond_id = plan.add_cond(plan_left_id, op, plan_right_id)?;
                    let not_id = if is_not {
                        plan.add_unary(Unary::Not, cond_id)?
                    } else {
                        cond_id
                    };
                    map.add(id, not_id);
                }
                Type::Not => {
                    if node.children.first().is_none() {
                        return Err(SbroadError::UnexpectedNumberOfValues(format!(
                            "{:?} must contain not flag as its first child.",
                            &node.rule
                        )));
                    }
                    let ast_child_id = node.children.get(1).expect("Not has no children.");
                    let plan_child_id = plan.as_row(map.get(*ast_child_id)?, &mut rows)?;
                    let op = Unary::from_node_type(&node.rule)?;
                    let unary_id = plan.add_unary(op, plan_child_id)?;
                    map.add(id, unary_id);
                }
                Type::Exists => {
                    let (is_not, child_index) = match node.children.len() {
                        2 => (true, 1),
                        1 => (false, 0),
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some(format!(
                                    "AST Exists node {:?} contains unexpected children",
                                    node,
                                )),
                            ))
                        }
                    };
                    let ast_child_id = node
                        .children
                        .get(child_index)
                        .expect("{:?} has no children.");
                    let plan_child_id = plan.as_row(map.get(*ast_child_id)?, &mut rows)?;
                    let op = Unary::from_node_type(&node.rule)?;
                    let op_id = plan.add_unary(op, plan_child_id)?;
                    let not_id = if is_not {
                        plan.add_unary(Unary::Not, op_id)?
                    } else {
                        op_id
                    };
                    map.add(id, not_id);
                }
                Type::IsNull => {
                    let is_not = match node.children.len() {
                        2 => true,
                        1 => false,
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some(format!(
                                    "AST Exists node {:?} contains unexpected children",
                                    node,
                                )),
                            ))
                        }
                    };
                    let ast_child_id = node.children.first().expect("IsNull has no children.");
                    let plan_child_id = plan.as_row(map.get(*ast_child_id)?, &mut rows)?;
                    let op = Unary::from_node_type(&node.rule)?;
                    let op_id = plan.add_unary(op, plan_child_id)?;
                    let not_id = if is_not {
                        plan.add_unary(Unary::Not, op_id)?
                    } else {
                        op_id
                    };
                    map.add(id, not_id);
                }
                Type::Between => {
                    // left NOT? BETWEEN center AND right
                    let (is_not, left_index, center_index, right_index) = match node.children.len()
                    {
                        4 => (true, 0, 2, 3),
                        3 => (false, 0, 1, 2),
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some(format!(
                                    "AST Between node {:?} contains unexpected children",
                                    node,
                                )),
                            ))
                        }
                    };
                    let ast_left_id = node
                        .children
                        .get(left_index)
                        .expect("Between has no children.");
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_center_id = node
                        .children
                        .get(center_index)
                        .expect("Center not found among between children");
                    let plan_center_id = plan.as_row(map.get(*ast_center_id)?, &mut rows)?;
                    let ast_right_id = node
                        .children
                        .get(right_index)
                        .expect("Right not found among between children");
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;

                    let greater_eq_id = plan.add_cond(plan_left_id, Bool::GtEq, plan_center_id)?;
                    let less_eq_id = plan.add_cond(plan_left_id, Bool::LtEq, plan_right_id)?;
                    let and_id = plan.add_cond(greater_eq_id, Bool::And, less_eq_id)?;
                    let not_id = if is_not {
                        plan.add_unary(Unary::Not, and_id)?
                    } else {
                        and_id
                    };
                    map.add(id, not_id);
                    betweens.push(Between::new(plan_left_id, less_eq_id));
                }
                Type::Cast => {
                    let ast_child_id = node.children.first().expect("Condition has no children.");
                    let plan_child_id = map.get(*ast_child_id)?;
                    let ast_type_id = node
                        .children
                        .get(1)
                        .expect("Cast type not found among cast children");
                    let ast_type = self.nodes.get_node(*ast_type_id)?;
                    let cast_type = if ast_type.rule == Type::TypeVarchar {
                        // Get the length of the varchar.
                        let ast_len_id = ast_type
                            .children
                            .first()
                            .expect("Cast has no children. Cast type length node id is not found.");
                        let ast_len = self.nodes.get_node(*ast_len_id)?;
                        let len = ast_len
                            .value
                            .as_ref()
                            .ok_or_else(|| {
                                SbroadError::UnexpectedNumberOfValues(
                                    "Varchar length is empty".into(),
                                )
                            })?
                            .parse::<usize>()
                            .map_err(|e| {
                                SbroadError::ParsingError(
                                    Entity::Value,
                                    format!("failed to parse varchar length: {e:?}"),
                                )
                            })?;
                        Ok(CastType::Varchar(len))
                    } else {
                        CastType::try_from(&ast_type.rule)
                    }?;
                    let cast_id = plan.add_cast(plan_child_id, cast_type)?;
                    map.add(id, cast_id);
                }
                Type::Concat => {
                    let ast_left_id = node.children.first().expect("Concat has no children.");
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_right_id = node
                        .children
                        .get(1)
                        .expect("Right not found among concat children");
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;
                    let concat_id = plan.add_concat(plan_left_id, plan_right_id)?;
                    map.add(id, concat_id);
                }
                Type::Condition => {
                    let ast_child_id = node.children.first().expect("Condition has no children.");
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(id, plan_child_id);
                }
                Type::Function => {
                    if let Some((first, mut other)) = node.children.split_first() {
                        let mut is_distinct = false;
                        let function_name = parse_string_value_node(self, *first)?;
                        if let Some(first_id) = other.first() {
                            let rule = &self.nodes.get_node(*first_id)?.rule;
                            match rule {
                                Type::Distinct => {
                                    is_distinct = true;
                                    let Some((_, args)) = other.split_first() else {
                                        return Err(SbroadError::Invalid(
                                            Entity::AST,
                                            Some("function ast has no arguments".into()),
                                        ));
                                    };
                                    other = args;
                                }
                                Type::CountAsterisk => {
                                    if other.len() > 1 {
                                        return Err(SbroadError::UnexpectedNumberOfValues(
                                            "function ast with Asterisk has extra children".into(),
                                        ));
                                    }
                                    let normalized_name = function_name.to_lowercase();
                                    if "count" != normalized_name.as_str() {
                                        return Err(SbroadError::Invalid(
                                            Entity::Query,
                                            Some(format!(
                                                "\"*\" is allowed only inside \"count\" aggregate function. Got: {}",
                                                normalized_name,
                                            ))
                                        ));
                                    }
                                }
                                _ => {}
                            }
                        }
                        let mut plan_arg_list = Vec::new();
                        for ast_child_id in other {
                            let plan_child_id = map.get(*ast_child_id)?;
                            plan_arg_list.push(plan_child_id);
                        }

                        if let Some(kind) = AggregateKind::new(function_name) {
                            let plan_id = plan.add_aggregate_function(
                                function_name,
                                kind,
                                plan_arg_list.clone(),
                                is_distinct,
                            )?;
                            map.add(id, plan_id);
                            continue;
                        } else if is_distinct {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(
                                    "DISTINCT modifier is allowed only for aggregate functions"
                                        .into(),
                                ),
                            ));
                        }

                        let func = metadata.function(function_name)?;
                        if func.is_stable() {
                            let plan_func_id = plan.add_stable_function(func, plan_arg_list)?;
                            map.add(id, plan_func_id);
                        } else {
                            // At the moment we don't support any non-stable functions.
                            // Later this code block should handle other function behaviors.
                            return Err(SbroadError::Invalid(
                                Entity::SQLFunction,
                                Some(format!("function {function_name} is not stable.")),
                            ));
                        }
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "function has no children.".into(),
                        ));
                    }
                }
                Type::GroupBy => {
                    if node.children.len() < 2 {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "Group by must have at least 2 children.".into(),
                        ));
                    }
                    let mut children: Vec<usize> = Vec::with_capacity(node.children.len());
                    for ast_column_id in &node.children {
                        let plan_column_id = map.get(*ast_column_id)?;
                        children.push(plan_column_id);
                    }
                    let groupby_id = plan.add_groupby_from_ast(&children)?;
                    map.add(id, groupby_id);
                }
                Type::Join => {
                    // Join ast has the following structure:
                    // Join
                    // - left Scan
                    // - kind
                    // - right scan
                    // - condition
                    let ast_left_id = node.children.first().expect("Join has no children.");
                    let plan_left_id = map.get(*ast_left_id)?;
                    let ast_right_id = node
                        .children
                        .get(2)
                        .expect("Right not found among Join children.");
                    let plan_right_id = map.get(*ast_right_id)?;
                    let ast_cond_id = node
                        .children
                        .get(3)
                        .expect("Condition not found among Join children");
                    let ast_kind_id = node
                        .children
                        .get(1)
                        .expect("Kind not found among Join children.");
                    let ast_kind_node = self.nodes.get_node(*ast_kind_id)?;
                    let kind = match ast_kind_node.rule {
                        Type::LeftJoinKind => JoinKind::LeftOuter,
                        Type::InnerJoinKind => JoinKind::Inner,
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::AST,
                                Some(format!(
                                    "expected join kind node as 1 child of join. Got: {:?}",
                                    ast_kind_node,
                                )),
                            ))
                        }
                    };
                    let plan_cond_id = map.get(*ast_cond_id)?;
                    let plan_join_id =
                        plan.add_join(plan_left_id, plan_right_id, plan_cond_id, kind)?;
                    map.add(id, plan_join_id);
                }
                Type::Selection | Type::Having => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("Selection or Having has no children.");
                    let plan_child_id = map.get(*ast_child_id)?;
                    let ast_filter_id = node
                        .children
                        .get(1)
                        .expect("Filter not found among Selection children");
                    let plan_filter_id = map.get(*ast_filter_id)?;
                    let plan_node_id = match &node.rule {
                        Type::Selection => plan.add_select(&[plan_child_id], plan_filter_id)?,
                        Type::Having => plan.add_having(&[plan_child_id], plan_filter_id)?,
                        _ => return Err(SbroadError::Invalid(Entity::AST, None)), // never happens
                    };
                    map.add(id, plan_node_id);
                }
                Type::SelectWithOptionalContinuation => {
                    let first_select_id = node.children.first().expect(
                        "SelectWithOptionalContinuation must always have Select as a first child.",
                    );
                    let first_select_plan_id = map.get(*first_select_id)?;

                    let continuation_id = node.children.get(1);
                    if let Some(continuation_id) = continuation_id {
                        let continuation_node = self.nodes.get_node(*continuation_id)?;
                        match continuation_node.rule {
                            Type::UnionAllContinuation => {
                                let second_select_id = continuation_node
                                    .children
                                    .first()
                                    .expect("UnionAllContinuation must contain Select as a first child.");
                                let second_select_plan_id = map.get(*second_select_id)?;
                                let plan_union_all_id = plan.add_union_all(first_select_plan_id, second_select_plan_id)?;
                                map.add(id, plan_union_all_id);
                            }
                            Type::ExceptContinuation => {
                                let second_select_id = continuation_node
                                    .children
                                    .first()
                                    .expect("ExceptContinuation must contain Select as a first child.");
                                let second_select_plan_id = map.get(*second_select_id)?;
                                let plan_except_id = plan.add_except(first_select_plan_id, second_select_plan_id)?;
                                map.add(id, plan_except_id);
                            }
                            _ => unreachable!("SelectWithOptionalContinuation must contain UnionAllContinuation or ExceptContinuation as child")
                        }
                    } else {
                        map.add(id, first_select_plan_id);
                    }
                }
                Type::Projection => {
                    let (child_id, ast_columns_ids, is_distinct) = if let Some((first, other)) =
                        node.children.split_first()
                    {
                        let mut is_distinct: bool = false;
                        let mut other = other;
                        let first_col_ast_id = other.first().ok_or_else(|| {
                            SbroadError::Invalid(
                                Entity::AST,
                                Some("projection ast has no columns!".into()),
                            )
                        })?;
                        if let Type::Distinct = self.nodes.get_node(*first_col_ast_id)?.rule {
                            is_distinct = true;
                            (_, other) = other.split_first().ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::AST,
                                    Some("projection ast has no children except distinct".into()),
                                )
                            })?;
                        }
                        (*first, other, is_distinct)
                    } else {
                        return Err(SbroadError::Invalid(Entity::AST, None));
                    };
                    let plan_child_id = map.get(child_id)?;
                    let mut proj_columns: Vec<usize> = Vec::with_capacity(ast_columns_ids.len());
                    for ast_column_id in ast_columns_ids {
                        let ast_column = self.nodes.get_node(*ast_column_id)?;
                        match ast_column.rule {
                            Type::Column => {
                                let ast_alias_id = ast_column
                                    .children
                                    .first()
                                    .expect("Column has no children.");
                                let plan_alias_id = map.get(*ast_alias_id)?;
                                proj_columns.push(plan_alias_id);
                            }
                            Type::Asterisk => {
                                let plan_asterisk_id = map.get(*ast_column_id)?;
                                if let Node::Expression(Expression::Row { list, .. }) =
                                    plan.get_node(plan_asterisk_id)?
                                {
                                    for row_id in list {
                                        proj_columns.push(*row_id);
                                    }
                                } else {
                                    return Err(SbroadError::Invalid(
                                        Entity::Node,
                                        Some(
                                            "a plan node corresponding to asterisk is not a Row."
                                                .into(),
                                        ),
                                    ));
                                }
                            }
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Type,
                                    Some(format!(
                                        "expected a Column, Asterisk, ArithmeticExprAlias in projection, got {:?}.",
                                        ast_column.rule
                                    )),
                                ));
                            }
                        }
                    }
                    let projection_id =
                        plan.add_proj_internal(plan_child_id, &proj_columns, is_distinct)?;
                    map.add(id, projection_id);
                }
                Type::Multiplication | Type::Addition => {
                    let cond_id = get_arithmetic_op_id(
                        &mut plan,
                        node,
                        &map,
                        &mut arith_expr_with_parentheses_ids,
                        &mut rows,
                    )?;
                    map.add(id, cond_id);
                }
                Type::ArithParentheses => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("ArithParentheses have no child!");
                    let plan_child_id = map.get(*ast_child_id)?;
                    arith_expr_with_parentheses_ids.push(plan_child_id);
                    map.add(id, plan_child_id);
                }
                Type::ValuesRow => {
                    // TODO(ars): check that all row elements are constants
                    let ast_child_id = node.children.first().expect("Values Row has no children.");
                    let plan_child_id = map.get(*ast_child_id)?;
                    let values_row_id = plan.add_values_row(plan_child_id, &mut col_idx)?;
                    map.add(id, values_row_id);
                }
                Type::Values => {
                    let mut plan_value_row_ids: Vec<usize> =
                        Vec::with_capacity(node.children.len());
                    for ast_child_id in &node.children {
                        let plan_child_id = map.get(*ast_child_id)?;
                        plan_value_row_ids.push(plan_child_id);
                    }
                    let plan_values_id = plan.add_values(plan_value_row_ids)?;
                    map.add(id, plan_values_id);
                }
                Type::Update => {
                    let rel_child_ast_id = node.children.first().expect("Update has no children.");
                    let rel_child_id = map.get(*rel_child_ast_id)?;
                    let ast_table_id = node.children.get(1).expect("Update has no children.");
                    let ast_table = self.nodes.get_node(*ast_table_id)?;
                    if let Type::ScanTable = ast_table.rule {
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Type,
                            Some(format!(
                                "expected a table scan in update, got {ast_table:?}.",
                            )),
                        ));
                    }
                    let plan_scan_id = map.get(*ast_table_id)?;
                    let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
                    let relation = if let Relational::ScanRelation { relation, .. } = plan_scan_node
                    {
                        relation.clone()
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Type,
                            Some(format!(
                                "expected a table scan in update, got {ast_table:?}.",
                            )),
                        ));
                    };
                    let update_list_id = node
                        .children
                        .get(2)
                        .expect("Update lise expected as a second child of Update");
                    let update_list = self.nodes.get_node(*update_list_id)?;
                    // Maps position of column in table to corresponding update expression
                    let mut update_defs: HashMap<ColumnPosition, ExpressionId, RepeatableState> =
                        HashMap::with_capacity_and_hasher(
                            update_list.children.len(),
                            RepeatableState,
                        );
                    // Map of { column_name -> (column_role, column_position) }.
                    let mut names: HashMap<&str, (&ColumnRole, usize)> = HashMap::new();
                    let rel = plan.relations.get(&relation).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Table,
                            format!("{relation} among plan relations"),
                        )
                    })?;
                    rel.columns.iter().enumerate().for_each(|(i, c)| {
                        names.insert(c.name.as_str(), (c.get_role(), i));
                    });
                    let mut pk_positions: HashSet<usize> =
                        HashSet::with_capacity(rel.primary_key.positions.len());
                    rel.primary_key.positions.iter().for_each(|pos| {
                        pk_positions.insert(*pos);
                    });
                    for update_item_id in &update_list.children {
                        let update_item = self.nodes.get_node(*update_item_id)?;
                        let ast_column_id = update_item
                            .children
                            .first()
                            .expect("Column expected as first child of UpdateItem");
                        let expr_ast_id = update_item
                            .children
                            .get(1)
                            .expect("Expression expected as second child of UpdateItem");
                        let expr_id = map.get(*expr_ast_id)?;
                        if plan.contains_aggregates(expr_id, true)? {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(
                                    "aggregate functions are not supported in update expression."
                                        .into(),
                                ),
                            ));
                        }
                        let col = self.nodes.get_node(*ast_column_id)?;
                        let col_name =
                            normalize_name_from_sql(parse_string_value_node(self, *ast_column_id)?);
                        if let Type::ColumnName = col.rule {
                            match names.get(col_name.as_str()) {
                                Some((&ColumnRole::User, pos)) => {
                                    if pk_positions.contains(pos) {
                                        return Err(SbroadError::Invalid(
                                            Entity::Query,
                                            Some(format!(
                                                "it is illegal to update primary key column: {}",
                                                col_name
                                            )),
                                        ));
                                    }
                                    if update_defs.contains_key(pos) {
                                        return Err(SbroadError::Invalid(
                                            Entity::Query,
                                            Some(format!("The same column is specified twice in update list: {}", col_name))
                                        ));
                                    }
                                    update_defs.insert(*pos, expr_id);
                                }
                                Some((&ColumnRole::Sharding, _)) => {
                                    return Err(SbroadError::FailedTo(
                                        Action::Update,
                                        Some(Entity::Column),
                                        format!("system column {col_name} cannot be updated"),
                                    ))
                                }
                                None => {
                                    return Err(SbroadError::NotFound(
                                        Entity::Column,
                                        (*col_name).to_string(),
                                    ))
                                }
                            }
                        } else {
                            return Err(SbroadError::Invalid(
                                Entity::Type,
                                Some(format!("expected a Column name in insert, got {col:?}.")),
                            ));
                        }
                    }
                    let update_id = plan.add_update(&relation, &update_defs, rel_child_id)?;
                    map.add(id, update_id);
                }
                Type::Delete => {
                    // Get table name and selection plan node id.
                    let (proj_child_id, table_name) = if let Some(child_id) = node.children.first()
                    {
                        let child_node = self.nodes.get_node(*child_id)?;
                        match child_node.rule {
                            Type::ScanTable => {
                                let plan_scan_id = map.get(*child_id)?;
                                let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
                                let table = if let Relational::ScanRelation { relation, .. } =
                                    plan_scan_node
                                {
                                    relation.clone()
                                } else {
                                    return Err(SbroadError::Invalid(
                                        Entity::AST,
                                        Some(format!(
                                            "{}, got {plan_scan_node:?}",
                                            "expected scan as the first child of the delete node",
                                        )),
                                    ));
                                };
                                (plan_scan_id, table)
                            }
                            Type::DeleteFilter => {
                                let ast_table_id = child_node
                                    .children
                                    .first()
                                    .expect("Table not found among DeleteFilter children");
                                let plan_scan_id = map.get(*ast_table_id)?;
                                let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
                                let table = if let Relational::ScanRelation { relation, .. } =
                                    plan_scan_node
                                {
                                    relation.clone()
                                } else {
                                    return Err(SbroadError::Invalid(
                                        Entity::AST,
                                        Some(format!(
                                            "{}, got {plan_scan_node:?}",
                                            "expected scan as the first child in delete filter",
                                        )),
                                    ));
                                };
                                let ast_filter_id = child_node
                                    .children
                                    .get(1)
                                    .expect("Expr not found among DeleteFilter children");
                                let plan_filter_id = map.get(*ast_filter_id)?;
                                let plan_select_id =
                                    plan.add_select(&[plan_scan_id], plan_filter_id)?;
                                (plan_select_id, table)
                            }
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format!(
                                        "AST delete node {:?} contains unexpected children",
                                        child_node,
                                    )),
                                ));
                            }
                        }
                    } else {
                        return Err(SbroadError::UnexpectedNumberOfValues(
                            "AST delete node has no children.".into(),
                        ));
                    };
                    let table = metadata.table(&table_name)?;
                    // The projection in the delete operator contains only the primary key columns.
                    let mut pk_columns = Vec::with_capacity(table.primary_key.positions.len());
                    for pos in &table.primary_key.positions {
                        let column: &Column = table.columns.get(*pos).ok_or_else(|| {
                            SbroadError::Invalid(
                                Entity::Table,
                                Some(format!(
                                    "{} {} {pos}",
                                    "primary key refers to non-existing column", "at position",
                                )),
                            )
                        })?;
                        pk_columns.push(column.name.as_str());
                    }
                    let pk_column_ids = plan.new_columns(
                        &[proj_child_id],
                        false,
                        &[0],
                        pk_columns.as_ref(),
                        false,
                        false,
                    )?;
                    let mut alias_ids = Vec::with_capacity(pk_column_ids.len());
                    for (pk_pos, pk_column_id) in pk_column_ids.iter().enumerate() {
                        let pk_alias_id = plan
                            .nodes
                            .add_alias(&format!("pk_col_{pk_pos}"), *pk_column_id)?;
                        alias_ids.push(pk_alias_id);
                    }
                    let plan_proj_id = plan.add_proj_internal(proj_child_id, &alias_ids, false)?;
                    let plan_delete_id = plan.add_delete(table.name, plan_proj_id)?;

                    map.add(id, plan_delete_id);
                }
                Type::Insert => {
                    let ast_table_id = node.children.first().expect("Insert has no children.");
                    let ast_table = self.nodes.get_node(*ast_table_id)?;
                    if let Type::Table = ast_table.rule {
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Type,
                            Some(format!("expected a Table in insert, got {ast_table:?}.",)),
                        ));
                    }
                    let relation =
                        normalize_name_from_sql(parse_string_value_node(self, *ast_table_id)?);

                    let ast_child_id = node
                        .children
                        .get(1)
                        .expect("Second child not found among Insert children");
                    let get_conflict_strategy =
                        |child_idx: usize| -> Result<ConflictStrategy, SbroadError> {
                            let Some(child_id) = node.children.get(child_idx).copied() else {
                            return Ok(ConflictStrategy::DoFail)
                        };
                            let rule = &self.nodes.get_node(child_id)?.rule;
                            let res = match rule {
                                Type::DoNothing => ConflictStrategy::DoNothing,
                                Type::DoReplace => ConflictStrategy::DoReplace,
                                Type::DoFail => ConflictStrategy::DoFail,
                                _ => {
                                    return Err(SbroadError::Invalid(
                                        Entity::AST,
                                        Some(format!(
                                            "expected conflict strategy on \
                                AST id ({child_id}). Got: {rule:?}"
                                        )),
                                    ))
                                }
                            };
                            Ok(res)
                        };
                    let ast_child = self.nodes.get_node(*ast_child_id)?;
                    let plan_insert_id = if let Type::TargetColumns = ast_child.rule {
                        // insert into t (a, b, c) ...
                        let mut selected_col_names: Vec<&str> =
                            Vec::with_capacity(ast_child.children.len());
                        for col_id in &ast_child.children {
                            let col = self.nodes.get_node(*col_id)?;
                            if let Type::ColumnName = col.rule {
                                selected_col_names.push(parse_string_value_node(self, *col_id)?);
                            } else {
                                return Err(SbroadError::Invalid(
                                    Entity::Type,
                                    Some(format!("expected a Column name in insert, got {col:?}.")),
                                ));
                            }
                        }

                        let rel = plan.relations.get(&relation).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Table,
                                format!("{relation} among plan relations"),
                            )
                        })?;
                        for column in &rel.columns {
                            if let ColumnRole::Sharding = column.get_role() {
                                continue;
                            }
                            if !column.is_nullable
                                && !selected_col_names.contains(&column.name.as_str())
                            {
                                return Err(SbroadError::Invalid(
                                    Entity::Column,
                                    Some(format!(
                                        "NonNull column {} must be specified",
                                        column.name
                                    )),
                                ));
                            }
                        }

                        let ast_rel_child_id = node
                            .children
                            .get(2)
                            .expect("Third child not found among Insert children");
                        let plan_rel_child_id = map.get(*ast_rel_child_id)?;
                        let conflict_strategy = get_conflict_strategy(3)?;
                        plan.add_insert(
                            &relation,
                            plan_rel_child_id,
                            &selected_col_names,
                            conflict_strategy,
                        )?
                    } else {
                        // insert into t ...
                        let plan_child_id = map.get(*ast_child_id)?;
                        let conflict_strategy = get_conflict_strategy(2)?;
                        plan.add_insert(&relation, plan_child_id, &[], conflict_strategy)?
                    };
                    map.add(id, plan_insert_id);
                }
                Type::Explain => {
                    plan.mark_as_explain();

                    let ast_child_id = node.children.first().expect("Explain has no children.");
                    map.add(0, map.get(*ast_child_id)?);
                }
                Type::SingleQuotedString => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("SingleQuotedString has no children.");
                    map.add(id, map.get(*ast_child_id)?);
                }
                Type::CountAsterisk => {
                    let plan_id = plan.nodes.push(Node::Expression(Expression::CountAsterisk));
                    map.add(id, plan_id);
                }
                Type::Query => {
                    // Query may have two children:
                    // 1. select | insert | except | ..
                    // 2. Option child - for which no plan node is created
                    let child_id =
                        map.get(*node.children.first().expect("no children for Query rule"))?;
                    map.add(id, child_id);
                }
                Type::CreateTable => {
                    let create_sharded_table = parse_create_table(self, node)?;
                    let plan_id = plan.nodes.push(Node::Ddl(create_sharded_table));
                    map.add(id, plan_id);
                }
                Type::GrantPrivilege => {
                    let (grant_type, grantee_name, timeout) = parse_grant_revoke(node, self)?;
                    let grant_privilege = Acl::GrantPrivilege {
                        grant_type,
                        grantee_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Acl(grant_privilege));
                    map.add(id, plan_id);
                }
                Type::RevokePrivilege => {
                    let (revoke_type, grantee_name, timeout) = parse_grant_revoke(node, self)?;
                    let revoke_privilege = Acl::RevokePrivilege {
                        revoke_type,
                        grantee_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Acl(revoke_privilege));
                    map.add(id, plan_id);
                }
                Type::DropRole => {
                    let role_name_id = node
                        .children
                        .first()
                        .expect("RoleName expected under DropRole node");
                    let role_name =
                        normalize_name_for_space_api(parse_string_value_node(self, *role_name_id)?);

                    let mut timeout = get_default_timeout();
                    if let Some(timeout_child_id) = node.children.get(1) {
                        timeout = get_timeout(self, *timeout_child_id)?;
                    }
                    let drop_role = Acl::DropRole {
                        name: role_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Acl(drop_role));
                    map.add(id, plan_id);
                }
                Type::DropTable => {
                    let mut table_name: String = String::new();
                    let mut timeout = get_default_timeout();
                    for child_id in &node.children {
                        let child_node = self.nodes.get_node(*child_id)?;
                        match child_node.rule {
                            Type::DeletedTable => {
                                table_name = normalize_name_for_space_api(parse_string_value_node(
                                    self, *child_id,
                                )?);
                            }
                            Type::Timeout => {
                                timeout = get_timeout(self, *child_id)?;
                            }
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format!(
                                        "AST drop table node {:?} contains unexpected children",
                                        child_node,
                                    )),
                                ));
                            }
                        }
                    }
                    let drop_table = Ddl::DropTable {
                        name: table_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Ddl(drop_table));
                    map.add(id, plan_id);
                }
                Type::AlterUser => {
                    let user_name_node_id = node
                        .children
                        .first()
                        .expect("RoleName expected as a first child");
                    let user_name = parse_role_name(self, *user_name_node_id)?;

                    let alter_option_node_id = node
                        .children
                        .get(1)
                        .expect("Some AlterOption expected as a second child");
                    let alter_option_node = self.nodes.get_node(*alter_option_node_id)?;
                    let alter_option = match alter_option_node.rule {
                        Type::AlterLogin => AlterOption::Login,
                        Type::AlterNoLogin => AlterOption::NoLogin,
                        Type::AlterPassword => {
                            let pwd_node_id = alter_option_node
                                .children
                                .first()
                                .expect("Password expected as a first child");
                            let password =
                                String::from(parse_string_value_node(self, *pwd_node_id)?);

                            let mut auth_method = get_default_auth_method();
                            if let Some(auth_method_node_id) = alter_option_node.children.get(1) {
                                let auth_method_node = self.nodes.get_node(*auth_method_node_id)?;
                                let auth_method_string_node_id = auth_method_node
                                    .children
                                    .first()
                                    .expect("Method expected under AuthMethod node");
                                auth_method = String::from(parse_string_value_node(
                                    self,
                                    *auth_method_string_node_id,
                                )?);
                            }

                            AlterOption::Password {
                                password,
                                auth_method,
                            }
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::ParseNode,
                                Some(String::from("Expected to see concrete alter option")),
                            ))
                        }
                    };

                    let mut timeout = get_default_timeout();
                    if let Some(timeout_node_id) = node.children.get(2) {
                        timeout = get_timeout(self, *timeout_node_id)?;
                    }

                    let alter_user = Acl::AlterUser {
                        name: user_name,
                        alter_option,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Acl(alter_user));
                    map.add(id, plan_id);
                }
                Type::CreateUser => {
                    let mut iter = node.children.iter();
                    let user_name_node_id = iter.next().ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::ParseNode,
                            Some(String::from("RoleName expected as a first child")),
                        )
                    })?;
                    let user_name = parse_role_name(self, *user_name_node_id)?;

                    let pwd_node_id = iter.next().ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::ParseNode,
                            Some(String::from("Password expected as a second child")),
                        )
                    })?;
                    let password = String::from(parse_string_value_node(self, *pwd_node_id)?);

                    let mut timeout = get_default_timeout();
                    let mut auth_method = get_default_auth_method();
                    for child_id in iter {
                        let child_node = self.nodes.get_node(*child_id)?;
                        match child_node.rule {
                            Type::Timeout => {
                                timeout = get_timeout(self, *child_id)?;
                            }
                            Type::AuthMethod => {
                                let auth_method_node_id = child_node
                                    .children
                                    .first()
                                    .expect("Method expected under AuthMethod node");
                                auth_method = String::from(parse_string_value_node(
                                    self,
                                    *auth_method_node_id,
                                )?);
                            }
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format!(
                                        "ACL node contains unexpected child: {child_node:?}",
                                    )),
                                ));
                            }
                        }
                    }

                    let create_user = Acl::CreateUser {
                        name: user_name,
                        password,
                        auth_method,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Acl(create_user));
                    map.add(id, plan_id);
                }
                Type::DropUser => {
                    let user_name_id = node
                        .children
                        .first()
                        .expect("RoleName expected under DropUser node");
                    let user_name = parse_role_name(self, *user_name_id)?;

                    let mut timeout = get_default_timeout();
                    if let Some(timeout_child_id) = node.children.get(1) {
                        timeout = get_timeout(self, *timeout_child_id)?;
                    }
                    let drop_user = Acl::DropUser {
                        name: user_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Acl(drop_user));
                    map.add(id, plan_id);
                }
                Type::CreateRole => {
                    let role_name_id = node
                        .children
                        .first()
                        .expect("RoleName expected under CreateRole node");
                    let role_name = parse_role_name(self, *role_name_id)?;

                    let mut timeout = get_default_timeout();
                    if let Some(timeout_child_id) = node.children.get(1) {
                        timeout = get_timeout(self, *timeout_child_id)?;
                    }
                    let create_role = Acl::CreateRole {
                        name: role_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Acl(create_role));
                    map.add(id, plan_id);
                }
                Type::Add
                | Type::AliasName
                | Type::AlterLogin
                | Type::AlterNoLogin
                | Type::AlterPassword
                | Type::AuthMethod
                | Type::ChapSha1
                | Type::Columns
                | Type::ColumnDef
                | Type::ColumnDefName
                | Type::ColumnDefType
                | Type::ColumnDefIsNull
                | Type::ColumnName
                | Type::DeleteFilter
                | Type::DeletedTable
                | Type::Divide
                | Type::Distinct
                | Type::Distribution
                | Type::Duration
                | Type::DoNothing
                | Type::DoReplace
                | Type::DoFail
                | Type::Engine
                | Type::Eq
                | Type::FunctionName
                | Type::Global
                | Type::Gt
                | Type::GtEq
                | Type::In
                | Type::InnerJoinKind
                | Type::Ldap
                | Type::LeftJoinKind
                | Type::Length
                | Type::Lt
                | Type::LtEq
                | Type::Md5
                | Type::Memtx
                | Type::Multiply
                | Type::NewTable
                | Type::NotEq
                | Type::Name
                | Type::NotFlag
                | Type::Password
                | Type::PrimaryKey
                | Type::PrimaryKeyColumn
                | Type::RoleName
                | Type::PrivBlockPrivilege
                | Type::PrivBlockUser
                | Type::PrivBlockSpecificUser
                | Type::PrivBlockRole
                | Type::PrivBlockSpecificRole
                | Type::PrivBlockTable
                | Type::PrivBlockSpecificTable
                | Type::PrivBlockRolePass
                | Type::PrivilegeAlter
                | Type::PrivilegeCreate
                | Type::PrivilegeDrop
                | Type::PrivilegeExecute
                | Type::PrivilegeRead
                | Type::PrivilegeSession
                | Type::PrivilegeUsage
                | Type::PrivilegeWrite
                | Type::ScanName
                | Type::Select
                | Type::Sharding
                | Type::ExceptContinuation
                | Type::UnionAllContinuation
                | Type::ShardingColumn
                | Type::Subtract
                | Type::TargetColumns
                | Type::Timeout
                | Type::TypeAny
                | Type::TypeBool
                | Type::TypeDecimal
                | Type::TypeDouble
                | Type::TypeInt
                | Type::TypeNumber
                | Type::TypeScalar
                | Type::TypeString
                | Type::TypeText
                | Type::TypeUnsigned
                | Type::TypeVarchar
                | Type::UpdateList
                | Type::UpdateItem
                | Type::Vinyl => {}
                rule => {
                    return Err(SbroadError::NotImplemented(
                        Entity::Type,
                        format!("{rule:?}"),
                    ));
                }
            }
        }
        // get root node id
        let plan_top_id = map
            .get(self.top.ok_or_else(|| {
                SbroadError::Invalid(Entity::AST, Some("no top in AST".into()))
            })?)?;
        plan.set_top(plan_top_id)?;
        let replaces = plan.replace_sq_with_references()?;
        plan.fix_betweens(&betweens, &replaces)?;
        plan.fix_arithmetic_parentheses(&arith_expr_with_parentheses_ids)?;
        Ok(plan)
    }
}

impl Plan {
    /// Wrap references, constants, functions, concatenations and casts in the plan into rows.
    /// Leave other nodes (e.g. rows) unchanged.
    fn as_row(&mut self, expr_id: usize, rows: &mut HashSet<usize>) -> Result<usize, SbroadError> {
        if let Node::Expression(
            Expression::Reference { .. }
            | Expression::Constant { .. }
            | Expression::Cast { .. }
            | Expression::Concat { .. }
            | Expression::StableFunction { .. },
        ) = self.get_node(expr_id)?
        {
            let row_id = self.nodes.add_row(vec![expr_id], None);
            rows.insert(row_id);
            Ok(row_id)
        } else {
            Ok(expr_id)
        }
    }
}

pub mod ast;
pub mod ir;
pub mod tree;
