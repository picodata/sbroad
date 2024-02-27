//! SQL frontend module.
//!
//! Parses an SQL statement to the abstract syntax tree (AST)
//! and builds the intermediate representation (IR).

use pest::iterators::{Pair, Pairs};
use pest::pratt_parser::PrattParser;
use pest::Parser;
use std::collections::VecDeque;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::{helpers::normalize_name_for_space_api, Metadata};
use crate::frontend::sql::ast::{
    AbstractSyntaxTree, ParseNode, ParseNodes, ParseTree, Rule, StackParseNode,
};
use crate::frontend::sql::ir::Translation;
use crate::frontend::Ast;
use crate::ir::ddl::{ColumnDef, Ddl};
use crate::ir::ddl::{Language, ParamDef};
use crate::ir::expression::cast::Type as CastType;
use crate::ir::expression::{ColumnPositionMap, Expression, ExpressionId};
use crate::ir::operator::{Arithmetic, Bool, ConflictStrategy, JoinKind, Relational, Unary};
use crate::ir::relation::{Column, ColumnRole, Type as RelationType};
use crate::ir::tree::traversal::PostOrder;
use crate::ir::value::Value;
use crate::ir::{Node, NodeId, OptionKind, OptionParamValue, OptionSpec, Plan};
use crate::otm::child_span;

use crate::errors::Entity::AST;
use crate::executor::engine::helpers::normalize_name_from_sql;
use crate::ir::acl::AlterOption;
use crate::ir::acl::{Acl, GrantRevokeType, Privilege};
use crate::ir::aggregates::AggregateKind;
use crate::ir::block::Block;
use crate::ir::helpers::RepeatableState;
use crate::ir::transformation::redistribution::ColumnPosition;
use sbroad_proc::otm_child_span;
use tarantool::decimal::Decimal;
use tarantool::space::SpaceEngineType;

// DDL timeout in seconds (1 day).
const DEFAULT_TIMEOUT_F64: f64 = 24.0 * 60.0 * 60.0;
const DEFAULT_AUTH_METHOD: &str = "chap-sha1";

fn get_default_timeout() -> Decimal {
    Decimal::from_str(&format!("{DEFAULT_TIMEOUT_F64}")).expect("default timeout casting failed")
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
        if duration_node.rule != Rule::Duration {
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

fn parse_proc_params(
    ast: &AbstractSyntaxTree,
    params_node: &ParseNode,
) -> Result<Vec<ParamDef>, SbroadError> {
    let mut params = Vec::with_capacity(params_node.children.len());
    for param_id in &params_node.children {
        let column_def_type_node = ast.nodes.get_node(*param_id)?;
        let type_node_inner_id = column_def_type_node
            .children
            .first()
            .expect("Expected specific type under ColumnDefType node");
        let type_node = ast.nodes.get_node(*type_node_inner_id)?;
        let data_type = match type_node.rule {
            Rule::TypeBool => RelationType::Boolean,
            Rule::TypeDecimal => RelationType::Decimal,
            Rule::TypeDouble => RelationType::Double,
            Rule::TypeInt => RelationType::Integer,
            Rule::TypeNumber => RelationType::Number,
            Rule::TypeScalar => RelationType::Scalar,
            Rule::TypeString | Rule::TypeText | Rule::TypeVarchar => RelationType::String,
            Rule::TypeUnsigned => RelationType::Unsigned,
            _ => unreachable!("Unexpected node: {type_node:?}"),
        };
        params.push(ParamDef { data_type });
    }
    Ok(params)
}

fn parse_call_proc<M: Metadata>(
    ast: &AbstractSyntaxTree,
    node: &ParseNode,
    pairs_map: &mut ParsingPairsMap,
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<Block, SbroadError> {
    let proc_name_ast_id = node.children.first().expect("Expected to get Proc name");
    let proc_name = parse_identifier(ast, *proc_name_ast_id)?;

    let proc_values_id = node.children.get(1).expect("Expected to get Proc values");
    let proc_values = ast.nodes.get_node(*proc_values_id)?;
    let mut values: Vec<NodeId> = Vec::with_capacity(proc_values.children.len());
    for proc_value_id in &proc_values.children {
        let proc_value = ast.nodes.get_node(*proc_value_id)?;
        let plan_value_id = match proc_value.rule {
            Rule::Parameter => parse_param(
                &ParameterSource::AstNode {
                    ast,
                    ast_node_id: *proc_value_id,
                },
                worker,
                plan,
            )?,
            Rule::Literal => {
                let literal_pair = pairs_map.remove_pair(*proc_value_id);
                parse_expr(Pairs::single(literal_pair), &[], worker, plan)?
            }
            _ => unreachable!("Unexpected rule met under ProcValue"),
        };
        values.push(plan_value_id);
    }

    let call_proc = Block::Procedure {
        name: proc_name,
        values,
    };
    Ok(call_proc)
}

fn parse_rename_proc(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Ddl, SbroadError> {
    if node.rule != Rule::RenameProc {
        return Err(SbroadError::Invalid(
            Entity::Type,
            Some("rename procedure".into()),
        ));
    }

    let mut old_name: String = String::new();
    let mut new_name: String = String::new();
    let mut params: Option<Vec<ParamDef>> = None;
    let mut timeout = get_default_timeout();
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::NewProc => {
                new_name = normalize_name_for_space_api(parse_string_value_node(ast, *child_id)?);
            }
            Rule::OldProc => {
                old_name = normalize_name_for_space_api(parse_string_value_node(ast, *child_id)?);
            }
            Rule::ProcParams => {
                params = Some(parse_proc_params(ast, child_node)?);
            }
            Rule::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            _ => panic!("Unexpected node: {child_node:?}"),
        }
    }
    Ok(Ddl::RenameRoutine {
        old_name,
        new_name,
        params,
        timeout,
    })
}

fn parse_create_proc(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Ddl, SbroadError> {
    let proc_name_id = node.children.first().expect("Expected to get Proc name");
    let proc_name = parse_identifier(ast, *proc_name_id)?;

    let proc_params_id = node.children.get(1).expect("Expedcted to get Proc params");
    let proc_params = ast.nodes.get_node(*proc_params_id)?;
    let params = parse_proc_params(ast, proc_params)?;

    let language = Language::SQL;
    let mut body: String = String::new();
    let mut timeout = get_default_timeout();
    for child_id in node.children.iter().skip(2) {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::ProcLanguage => {
                // We don't need to parse language node, because we support only SQL.
            }
            Rule::ProcBody => {
                body = child_node
                    .value
                    .as_ref()
                    .expect("procedure body must not be empty")
                    .clone();
            }
            Rule::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            _ => unreachable!("Unexpected node: {child_node:?}"),
        }
    }
    let create_proc = Ddl::CreateProc {
        name: proc_name,
        params,
        language,
        body,
        timeout,
    };
    Ok(create_proc)
}

fn parse_proc_with_optional_params(
    ast: &AbstractSyntaxTree,
    node: &ParseNode,
) -> Result<(String, Option<Vec<ParamDef>>), SbroadError> {
    let proc_name_id = node.children.first().expect("Expected to get Proc name");
    let proc_name = parse_identifier(ast, *proc_name_id)?;

    let params = if let Some(params_node_id) = node.children.get(1) {
        let params_node = ast.nodes.get_node(*params_node_id)?;
        Some(parse_proc_params(ast, params_node)?)
    } else {
        None
    };

    Ok((proc_name, params))
}

fn parse_drop_proc(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Ddl, SbroadError> {
    let proc_with_optional_params_id = node
        .children
        .first()
        .expect("Expected to see ProcWithOptionalParams");
    let proc_with_optional_params = ast.nodes.get_node(*proc_with_optional_params_id)?;
    let (name, params) = parse_proc_with_optional_params(ast, proc_with_optional_params)?;

    let timeout = if let Some(timeout_id) = node.children.get(1) {
        get_timeout(ast, *timeout_id)?
    } else {
        get_default_timeout()
    };

    Ok(Ddl::DropProc {
        name,
        params,
        timeout,
    })
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::uninlined_format_args)]
fn parse_create_table(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Ddl, SbroadError> {
    if node.rule != Rule::CreateTable {
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
            Rule::NewTable => {
                table_name = parse_identifier(ast, *child_id)?;
            }
            Rule::Columns => {
                let columns_node = ast.nodes.get_node(*child_id)?;
                for col_id in &columns_node.children {
                    let mut column_def = ColumnDef::default();
                    let column_def_node = ast.nodes.get_node(*col_id)?;
                    for def_child_id in &column_def_node.children {
                        let def_child_node = ast.nodes.get_node(*def_child_id)?;
                        match def_child_node.rule {
                            Rule::Identifier => {
                                column_def.name = parse_identifier(ast, *def_child_id)?;
                            }
                            Rule::ColumnDefType => {
                                let type_id_child = def_child_node
                                    .children
                                    .first()
                                    .expect("ColumnDefType must have a type child");
                                let type_node = ast.nodes.get_node(*type_id_child)?;
                                match type_node.rule {
                                    Rule::TypeBool => {
                                        column_def.data_type = RelationType::Boolean;
                                    }
                                    Rule::TypeDecimal => {
                                        column_def.data_type = RelationType::Decimal;
                                    }
                                    Rule::TypeDouble => {
                                        column_def.data_type = RelationType::Double;
                                    }
                                    Rule::TypeInt => {
                                        column_def.data_type = RelationType::Integer;
                                    }
                                    Rule::TypeNumber => {
                                        column_def.data_type = RelationType::Number;
                                    }
                                    Rule::TypeScalar => {
                                        column_def.data_type = RelationType::Scalar;
                                    }
                                    Rule::TypeString | Rule::TypeText | Rule::TypeVarchar => {
                                        column_def.data_type = RelationType::String;
                                    }
                                    Rule::TypeUnsigned => {
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
                            }
                            Rule::ColumnDefIsNull => {
                                match (def_child_node.children.first(), def_child_node.children.get(1)) {
                                    (None, None) => {
                                        column_def.is_nullable = true;
                                    }
                                    (Some(child_id), None) => {
                                        let not_flag_node = ast.nodes.get_node(*child_id)?;
                                        if let Rule::NotFlag = not_flag_node.rule {
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
            Rule::PrimaryKey => {
                let pk_node = ast.nodes.get_node(*child_id)?;
                for pk_col_id in &pk_node.children {
                    let pk_col_name = parse_identifier(ast, *pk_col_id)?;
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
            Rule::Engine => {
                if let (Some(engine_type_id), None) =
                    (child_node.children.first(), child_node.children.get(1))
                {
                    let engine_type_node = ast.nodes.get_node(*engine_type_id)?;
                    match engine_type_node.rule {
                        Rule::Memtx => {
                            engine_type = SpaceEngineType::Memtx;
                        }
                        Rule::Vinyl => {
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
            Rule::Distribution => {
                let distribution_node = ast.nodes.get_node(*child_id)?;
                if let (Some(distribution_type_id), None) = (
                    distribution_node.children.first(),
                    distribution_node.children.get(1),
                ) {
                    let distribution_type_node = ast.nodes.get_node(*distribution_type_id)?;
                    match distribution_type_node.rule {
                        Rule::Global => {}
                        Rule::Sharding => {
                            let shard_node = ast.nodes.get_node(*distribution_type_id)?;
                            for shard_col_id in &shard_node.children {
                                let shard_col_name = parse_identifier(ast, *shard_col_id)?;

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
            Rule::Timeout => {
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

/// Get String value under node that is considered to be an identifier
/// (on which rules on name normalization should be applied).
fn parse_identifier(ast: &AbstractSyntaxTree, node_id: usize) -> Result<String, SbroadError> {
    Ok(normalize_name_for_space_api(parse_string_value_node(
        ast, node_id,
    )?))
}

fn parse_normalized_identifier(
    ast: &AbstractSyntaxTree,
    node_id: usize,
) -> Result<String, SbroadError> {
    Ok(normalize_name_from_sql(parse_string_value_node(
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
        Rule::PrivBlockPrivilege => {
            let privilege_node_id = privilege_block_node
                .children
                .first()
                .expect("Expected to see Privilege under PrivBlockPrivilege");
            let privilege_node = ast.nodes.get_node(*privilege_node_id)?;
            let privilege = match privilege_node.rule {
                Rule::PrivilegeAlter => Privilege::Alter,
                Rule::PrivilegeCreate => Privilege::Create,
                Rule::PrivilegeDrop => Privilege::Drop,
                Rule::PrivilegeExecute => Privilege::Execute,
                Rule::PrivilegeRead => Privilege::Read,
                Rule::PrivilegeSession => Privilege::Session,
                Rule::PrivilegeUsage => Privilege::Usage,
                Rule::PrivilegeWrite => Privilege::Write,
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
                Rule::PrivBlockUser => GrantRevokeType::user(privilege)?,
                Rule::PrivBlockSpecificUser => {
                    let user_name_node_id = inner_privilege_block_node
                        .children
                        .first()
                        .expect("Expected to see RoleName under PrivBlockSpecificUser");
                    let user_name = parse_identifier(ast, *user_name_node_id)?;
                    GrantRevokeType::specific_user(privilege, user_name)?
                }
                Rule::PrivBlockRole => GrantRevokeType::role(privilege)?,
                Rule::PrivBlockSpecificRole => {
                    let role_name_node_id = inner_privilege_block_node
                        .children
                        .first()
                        .expect("Expected to see RoleName under PrivBlockSpecificUser");
                    let role_name = parse_identifier(ast, *role_name_node_id)?;
                    GrantRevokeType::specific_role(privilege, role_name)?
                }
                Rule::PrivBlockTable => GrantRevokeType::table(privilege)?,
                Rule::PrivBlockSpecificTable => {
                    let table_node_id = inner_privilege_block_node.children.first().expect(
                        "Expected to see TableName as a first child of PrivBlockSpecificTable",
                    );
                    let table_name = parse_identifier(ast, *table_node_id)?;
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
        Rule::PrivBlockRolePass => {
            let role_name_id = privilege_block_node
                .children
                .first()
                .expect("RoleName must be a first child of PrivBlockRolePass");
            let role_name = parse_identifier(ast, *role_name_id)?;
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
    let grantee_name = parse_identifier(ast, *grantee_name_node_id)?;

    let mut timeout = get_default_timeout();
    if let Some(timeout_child_id) = node.children.get(2) {
        timeout = get_timeout(ast, *timeout_child_id)?;
    }

    Ok((grant_revoke_type, grantee_name, timeout))
}

/// Common logic for `SqlVdbeMaxSteps` and `VTableMaxRows` parsing.
fn parse_option<M: Metadata>(
    ast: &AbstractSyntaxTree,
    option_node_id: usize,
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<OptionParamValue, SbroadError> {
    let ast_node = ast.nodes.get_node(option_node_id)?;
    let value = match ast_node.rule {
        Rule::Parameter => {
            let plan_id = parse_param(
                &ParameterSource::AstNode {
                    ast,
                    ast_node_id: option_node_id,
                },
                worker,
                plan,
            )?;
            OptionParamValue::Parameter { plan_id }
        }
        Rule::Unsigned => {
            let v = {
                if let Some(str_value) = ast_node.value.as_ref() {
                    str_value.parse::<u64>().map_err(|_| {
                        SbroadError::Invalid(
                            Entity::Query,
                            Some(format!("option value is not unsigned integer: {str_value}")),
                        )
                    })?
                } else {
                    return Err(SbroadError::Invalid(
                        AST,
                        Some("Unsigned node has value".into()),
                    ));
                }
            };
            OptionParamValue::Value {
                val: Value::Unsigned(v),
            }
        }
        _ => {
            return Err(SbroadError::Invalid(
                AST,
                Some(format!("unexpected child of option. id: {option_node_id}")),
            ))
        }
    };
    Ok(value)
}

enum ParameterSource<'parameter> {
    AstNode {
        ast: &'parameter AbstractSyntaxTree,
        ast_node_id: usize,
    },
    Pair {
        pair: Pair<'parameter, Rule>,
    },
}

impl<'parameter> ParameterSource<'parameter> {
    fn get_param_index(&self) -> Result<Option<usize>, SbroadError> {
        let param_index = match self {
            ParameterSource::AstNode { ast, ast_node_id } => {
                let param_node = ast.nodes.get_node(*ast_node_id)?;
                let child = param_node
                    .children
                    .first()
                    .expect("Expected child node under Parameter");
                let inner_param_node = ast.nodes.get_node(*child)?;
                match inner_param_node.rule {
                    Rule::TntParameter => None,
                    Rule::PgParameter => {
                        let param_index = inner_param_node
                            .children
                            .first()
                            .expect("Expected Unsigned under PgParameter");
                        let param_index_node = ast.nodes.get_node(*param_index)?;
                        let Some(ref param_index_value) = param_index_node.value else {
                            unreachable!("Expected value for Unsigned")
                        };
                        let param_index_usize = param_index_value
                            .parse::<usize>()
                            .expect("usize param expected under PgParameter");
                        Some(param_index_usize)
                    }
                    _ => unreachable!("Unexpected node met under Parameter"),
                }
            }
            ParameterSource::Pair { pair } => {
                let inner_param = pair
                    .clone()
                    .into_inner()
                    .next()
                    .expect("Concrete param expected under Parameter node");
                match inner_param.as_rule() {
                    Rule::TntParameter => None,
                    Rule::PgParameter => {
                        let inner_unsigned = inner_param
                            .into_inner()
                            .next()
                            .expect("Unsigned not found under PgParameter");
                        let value_idx = inner_unsigned
                            .as_str()
                            .parse::<usize>()
                            .expect("usize param expected under PgParameter");

                        if value_idx == 0 {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some("$n parameters are indexed from 1!".into()),
                            ));
                        }
                        Some(value_idx)
                    }
                    _ => unreachable!("Unexpected Rule met under Parameter"),
                }
            }
        };
        Ok(param_index)
    }
}

/// Binding to Tarantool/Postgres parameter resulted from parsing.
enum Parameter {
    TntParameter,
    PgParameter { index: usize },
}

fn parse_param<M: Metadata>(
    param: &ParameterSource,
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<usize, SbroadError> {
    let param_index = param.get_param_index()?;
    let parameter = match param_index {
        None => {
            // Tarantool parameter.
            if worker.met_pg_param {
                return Err(SbroadError::UseOfBothParamsStyles);
            }
            worker.met_tnt_param = true;
            Parameter::TntParameter
        }
        Some(index) => {
            // Postgres parameter.
            if worker.met_tnt_param {
                return Err(SbroadError::UseOfBothParamsStyles);
            }
            worker.met_pg_param = true;
            Parameter::PgParameter { index }
        }
    };
    let param_id = plan.add_param();
    if let Parameter::PgParameter { index } = parameter {
        plan.pg_params_map.insert(param_id, index - 1);
    }
    Ok(param_id)
}

// Helper structure used to resolve expression operators priority.
lazy_static::lazy_static! {
    static ref PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::{Assoc::{Left, Right}, Op};
        use Rule::{Add, And, Between, ConcatInfixOp, Divide, Eq, Gt, GtEq, In, IsNullPostfix, Lt, LtEq, Multiply, NotEq, Or, Subtract, UnaryNot};

        // Precedence is defined lowest to highest.
        PrattParser::new()
            .op(Op::infix(Or, Left))
            .op(Op::infix(Between, Left))
            .op(Op::infix(And, Left))
            .op(Op::prefix(UnaryNot))
            .op(
                Op::infix(Eq, Right) | Op::infix(NotEq, Right) | Op::infix(NotEq, Right)
                | Op::infix(Gt, Right) | Op::infix(GtEq, Right) | Op::infix(Lt, Right)
                | Op::infix(LtEq, Right) | Op::infix(In, Right)
            )
            .op(Op::infix(Add, Left) | Op::infix(Subtract, Left))
            .op(Op::infix(Multiply, Left) | Op::infix(Divide, Left) | Op::infix(ConcatInfixOp, Left))
            .op(Op::postfix(IsNullPostfix))
    };
}

/// Total average number of Reference (`ReferenceContinuation`) rule nodes that we expect to get
/// in the parsing result returned from pest. Used for preallocating `reference_to_name_map`.
const REFERENCES_MAP_CAPACITY: usize = 50;

/// Total average number of Expr and Row rule nodes that we expect to get
/// in the parsing result returned from pest. Used for preallocating `ParsingPairsMap`.
const PARSING_PAIRS_MAP_CAPACITY: usize = 100;

/// Helper struct holding values and references needed for `parse_expr` calls.
struct ExpressionsWorker<'worker, M>
where
    M: Metadata,
{
    subquery_ids_queue: VecDeque<usize>,
    betweens: Vec<Between>,
    metadata: &'worker M,
    /// Map of { reference plan_id -> (it's column name, whether it's covered with row)}
    /// We have to save column name in order to use it later for alias creation.
    /// We use information about row coverage later when handling Row expression and need to know
    /// whether we should uncover our reference.
    pub reference_to_name_map: HashMap<usize, (String, bool)>,
    met_tnt_param: bool,
    met_pg_param: bool,
}

impl<'worker, M> ExpressionsWorker<'worker, M>
where
    M: Metadata,
{
    fn new<'plan: 'worker, 'meta: 'worker>(metadata: &'meta M) -> Self {
        Self {
            subquery_ids_queue: VecDeque::new(),
            metadata,
            betweens: Vec::new(),
            reference_to_name_map: HashMap::with_capacity(REFERENCES_MAP_CAPACITY),
            met_tnt_param: false,
            met_pg_param: false,
        }
    }
}

#[derive(Clone)]
enum ParseExpressionInfixOperator {
    InfixBool(Bool),
    InfixArithmetic(Arithmetic),
    Concat,
}

#[derive(Clone)]
enum ParseExpression {
    PlanId {
        plan_id: usize,
    },
    Parentheses {
        child: Box<ParseExpression>,
    },
    Infix {
        op: ParseExpressionInfixOperator,
        is_not: bool,
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
    Function {
        name: String,
        args: Vec<ParseExpression>,
        is_distinct: bool,
    },
    Row {
        children: Vec<ParseExpression>,
    },
    Prefix {
        op: Unary,
        child: Box<ParseExpression>,
    },
    Exists {
        is_not: bool,
        child: Box<ParseExpression>,
    },
    IsNull {
        is_not: bool,
        child: Box<ParseExpression>,
    },
    Cast {
        cast_type: CastType,
        child: Box<ParseExpression>,
    },
    Between {
        is_not: bool,
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
}

impl Plan {
    /// Find a pair of:
    /// * AND operator that must actually represent BETWEEN right part
    ///   (presented as it's left and right children)
    /// * This ^ operator optional AND parent which child we should replace with BETWEEN operator
    fn find_leftmost_and(
        &self,
        current_node_plan_id: usize,
        parent: Option<usize>,
    ) -> Result<(usize, usize, Option<usize>), SbroadError> {
        let root_expr = self.get_expression_node(current_node_plan_id)?;
        match root_expr {
            Expression::Alias { .. }
            | Expression::ExprInParentheses { .. }
            | Expression::Arithmetic { .. }
            | Expression::Cast { .. }
            | Expression::Concat { .. }
            | Expression::Constant { .. }
            | Expression::Reference { .. }
            | Expression::Row { .. }
            | Expression::StableFunction { .. }
            | Expression::Unary { .. }
            | Expression::CountAsterisk => Err(SbroadError::Invalid(
                Entity::Expression,
                Some(String::from(
                    "Expected to see bool node during leftmost AND search",
                )),
            )),
            Expression::Bool { left, op, right } => match op {
                Bool::And => {
                    let left_child = self.get_expression_node(*left)?;
                    if let Expression::Bool { op: Bool::And, .. } = left_child {
                        self.find_leftmost_and(*left, Some(current_node_plan_id))
                    } else {
                        Ok((*left, *right, parent))
                    }
                }
                Bool::Eq
                | Bool::In
                | Bool::Gt
                | Bool::GtEq
                | Bool::Lt
                | Bool::LtEq
                | Bool::NotEq
                | Bool::Or
                | Bool::Between => Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some(String::from(
                        "Expected to see AND bool node during leftmost AND search",
                    )),
                )),
            },
        }
    }
}

impl ParseExpression {
    #[allow(clippy::too_many_lines)]
    fn populate_plan<M>(
        &self,
        plan: &mut Plan,
        worker: &mut ExpressionsWorker<M>,
    ) -> Result<usize, SbroadError>
    where
        M: Metadata,
    {
        let plan_id = match self {
            ParseExpression::PlanId { plan_id } => *plan_id,
            ParseExpression::Parentheses { child } => {
                let child_plan_id = child.populate_plan(plan, worker)?;

                let child_expr = plan.get_node(child_plan_id)?;
                if let Node::Expression(Expression::Bool { op, .. }) = child_expr {
                    // We don't want simple infix comparisons to be covered with parentheses
                    // as soon as it breaks logic of conflicts resolving which currently
                    // work adequately only with ROWs.
                    //
                    // TODO: 1. Applied logic works good until we met expressions like `(1 < 2) + 3`
                    //          that will be transformed into `1 < 2 + 3` local sql
                    //          having the semantics of `1 < (2 + 3)` that is wrong.
                    //          We have to fix it later.
                    //       2. Move it to `parse_expr` stage and not here.
                    //       3. Find deepest parentheses child and not just first. The
                    //          same thing for Parentheses -> Row transformation under In operator)
                    match op {
                        Bool::And | Bool::Or => plan.add_covered_with_parentheses(child_plan_id),
                        _ => child_plan_id,
                    }
                } else {
                    plan.add_covered_with_parentheses(child_plan_id)
                }
            }
            ParseExpression::Cast { cast_type, child } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                plan.add_cast(child_plan_id, cast_type.to_owned())?
            }
            ParseExpression::Between {
                is_not,
                left,
                right,
            } => {
                let plan_left_id = left.populate_plan(plan, worker)?;
                let left_covered_with_row = plan.row(plan_left_id)?;

                let plan_right_id = right.populate_plan(plan, worker)?;
                let (left_and_child, right_and_child, parent) =
                    plan.find_leftmost_and(plan_right_id, None)?;

                let center_covered_with_row = plan.row(left_and_child)?;
                let right_covered_with_row = plan.row(right_and_child)?;

                let greater_eq_id =
                    plan.add_cond(left_covered_with_row, Bool::GtEq, center_covered_with_row)?;
                let less_eq_id =
                    plan.add_cond(left_covered_with_row, Bool::LtEq, right_covered_with_row)?;
                let and_id = plan.add_cond(greater_eq_id, Bool::And, less_eq_id)?;
                let between_id = if *is_not {
                    plan.add_unary(Unary::Not, and_id)?
                } else {
                    and_id
                };

                worker
                    .betweens
                    .push(Between::new(left_covered_with_row, less_eq_id));

                if let Some(leftmost_and_parent) = parent {
                    let and_to_replace_child = plan.get_mut_expression_node(leftmost_and_parent)?;
                    let Expression::Bool { ref mut left, .. } = and_to_replace_child else {
                        unreachable!("Expected to see AND node as leftmost parent")
                    };
                    *left = between_id;
                    plan_right_id
                } else {
                    between_id
                }
            }
            ParseExpression::Infix {
                op,
                is_not,
                left,
                right,
            } => {
                let left_plan_id = left.populate_plan(plan, worker)?;
                let left_row_id = plan.row(left_plan_id)?;

                // Workaround specific for In operator:
                // During parsing it's hard for us to distinguish Row and ExpressionInParentheses.
                // Under In operator we expect to see Row even if there is a single expression
                // covered in parentheses. Here we reinterpret ExpressionInParentheses as Row.
                let right_plan_id = if let ParseExpressionInfixOperator::InfixBool(Bool::In) = op {
                    if let ParseExpression::Parentheses { child } = *right.clone() {
                        let reinterpreted_right = ParseExpression::Row {
                            children: vec![*child],
                        };
                        reinterpreted_right.populate_plan(plan, worker)?
                    } else {
                        right.populate_plan(plan, worker)?
                    }
                } else {
                    right.populate_plan(plan, worker)?
                };
                let right_row_id = plan.row(right_plan_id)?;

                let op_plan_id = match op {
                    ParseExpressionInfixOperator::Concat => {
                        plan.add_concat(left_row_id, right_row_id)?
                    }
                    ParseExpressionInfixOperator::InfixArithmetic(arith) => {
                        plan.add_arithmetic_to_plan(left_row_id, arith.to_owned(), right_row_id)?
                    }
                    ParseExpressionInfixOperator::InfixBool(bool) => {
                        plan.add_cond(left_row_id, bool.to_owned(), right_row_id)?
                    }
                };
                if *is_not {
                    plan.add_unary(Unary::Not, op_plan_id)?
                } else {
                    op_plan_id
                }
            }
            ParseExpression::Prefix { op, child } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                let child_covered_with_row = plan.row(child_plan_id)?;
                plan.add_unary(op.to_owned(), child_covered_with_row)?
            }
            ParseExpression::Function {
                name,
                args,
                is_distinct,
            } => {
                let mut plan_arg_ids = Vec::new();
                for arg in args {
                    let arg_plan_id = arg.populate_plan(plan, worker)?;
                    plan_arg_ids.push(arg_plan_id);
                }
                if let Some(kind) = AggregateKind::new(name) {
                    plan.add_aggregate_function(name, kind, plan_arg_ids, *is_distinct)?
                } else if *is_distinct {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some("DISTINCT modifier is allowed only for aggregate functions".into()),
                    ));
                } else {
                    let func = worker.metadata.function(name)?;
                    if func.is_stable() {
                        plan.add_stable_function(func, plan_arg_ids)?
                    } else {
                        // At the moment we don't support any non-stable functions.
                        // Later this code block should handle other function behaviors.
                        return Err(SbroadError::Invalid(
                            Entity::SQLFunction,
                            Some(format!("function {name} is not stable.")),
                        ));
                    }
                }
            }
            ParseExpression::Row { children } => {
                let mut plan_children_ids = Vec::new();
                for child in children {
                    let plan_child_id = child.populate_plan(plan, worker)?;

                    // When handling references, we always cover them with rows
                    // (for the unification of the transformation process in case they are met in
                    // the selection of join condition).
                    // But references may also occur under the Row node (like
                    // `select (ref_1, ref_2) ...`). In such case we don't want out References to
                    // be covered with rows. E.g. in `set_distribution` we presume that
                    // references are not covered with additional rows.
                    let reference = worker.reference_to_name_map.get(&plan_child_id);
                    let uncovered_plan_child_id = if let Some((_, is_row)) = reference {
                        if *is_row {
                            let plan_inner_expr = plan.get_expression_node(plan_child_id)?;
                            *plan_inner_expr.get_row_list()?.first().ok_or_else(|| {
                                SbroadError::UnexpectedNumberOfValues(
                                    "There must be a Reference under Row.".into(),
                                )
                            })?
                        } else {
                            plan_child_id
                        }
                    } else {
                        plan_child_id
                    };
                    plan_children_ids.push(uncovered_plan_child_id);
                }
                plan.nodes.add_row(plan_children_ids, None)
            }
            ParseExpression::Exists { is_not, child } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                let op_id = plan.add_unary(Unary::Exists, child_plan_id)?;
                if *is_not {
                    plan.add_unary(Unary::Not, op_id)?
                } else {
                    op_id
                }
            }
            ParseExpression::IsNull { is_not, child } => {
                let child_plan_id = child.populate_plan(plan, worker)?;
                let child_covered_with_row = plan.row(child_plan_id)?;
                let op_id = plan.add_unary(Unary::IsNull, child_covered_with_row)?;
                if *is_not {
                    plan.add_unary(Unary::Not, op_id)?
                } else {
                    op_id
                }
            }
        };
        Ok(plan_id)
    }
}

/// Function responsible for parsing expressions using Pratt parser.
///
/// Parameters:
/// * Raw `expression_pair`, resulted from pest parsing. General idea is that we always have to
///   pass `Expr` pair with `into_inner` call.
/// * `ast` resulted from some parsing node transformations
/// * `plan` currently being built
///
/// Returns:
/// * Id of root `Expression` node added into the plan
#[allow(clippy::too_many_lines)]
fn parse_expr_pratt<M>(
    expression_pairs: Pairs<Rule>,
    referred_relation_ids: &[usize],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<ParseExpression, SbroadError>
where
    M: Metadata,
{
    PRATT_PARSER
        .map_primary(|primary| {
            let parse_expr = match primary.as_rule() {
                Rule::Expr => {
                    parse_expr_pratt(primary.into_inner(), referred_relation_ids, worker, plan)?
                }
                Rule::ExpressionInParentheses => {
                    let mut inner_pairs = primary.into_inner();
                    let child_expr_pair = inner_pairs
                        .next()
                        .expect("Expected to see inner expression under parentheses");
                    let child_parse_expr = parse_expr_pratt(
                        Pairs::single(child_expr_pair),
                        referred_relation_ids,
                        worker,
                        plan
                    )?;
                    ParseExpression::Parentheses { child: Box::new(child_parse_expr) }
                }
                Rule::Parameter => {
                    let plan_id = parse_param(&ParameterSource::Pair { pair: primary}, worker, plan)?;
                    ParseExpression::PlanId { plan_id }
                }
                Rule::IdentifierWithOptionalContinuation => {
                    let mut inner_pairs = primary.into_inner();
                    let first_identifier = inner_pairs.next().expect(
                        "Identifier expected under IdentifierWithOptionalContinuation"
                    ).as_str();

                    let mut scan_name = None;
                    let mut col_name = normalize_name_from_sql(first_identifier);

                    if inner_pairs.len() != 0 {
                        let continuation = inner_pairs.next().expect("Continuation expected after Identifier");
                        match continuation.as_rule() {
                            Rule::ReferenceContinuation => {
                                let col_name_pair = continuation.into_inner()
                                    .next().expect("Reference continuation must contain an Identifier");
                                let second_identifier = normalize_name_from_sql(col_name_pair.as_str());
                                scan_name = Some(col_name);
                                col_name = second_identifier;
                            }
                            Rule::FunctionInvocationContinuation => {
                                // Handle function invocation case.
                                let function_name = String::from(first_identifier);
                                let mut args_pairs = continuation.into_inner();
                                let mut is_distinct = false;
                                let mut parse_exprs_args = Vec::new();
                                let function_args = args_pairs.next();
                                if let Some(function_args) = function_args {
                                    match function_args.as_rule() {
                                        Rule::CountAsterisk => {
                                            let normalized_name = function_name.to_lowercase();
                                            if "count" != normalized_name.as_str() {
                                                return Err(SbroadError::Invalid(
                                                    Entity::Query,
                                                    Some(format!(
                                                        "\"*\" is allowed only inside \"count\" aggregate function. Got: {normalized_name}",
                                                    ))
                                                ));
                                            }
                                            let count_asterisk_plan_id = plan.nodes.push(Node::Expression(Expression::CountAsterisk));
                                            parse_exprs_args.push(ParseExpression::PlanId { plan_id: count_asterisk_plan_id });
                                        }
                                        Rule::FunctionArgs => {
                                            let mut args_inner = function_args.into_inner();
                                            let mut arg_pairs_to_parse = Vec::new();
                                            let first_arg_pair = args_inner.next().expect("First arg expected under function");
                                            if let Rule::Distinct = first_arg_pair.as_rule() {
                                                is_distinct = true;
                                            } else {
                                                arg_pairs_to_parse.push(first_arg_pair);
                                            }

                                            for arg_pair in args_inner {
                                                arg_pairs_to_parse.push(arg_pair);
                                            }

                                            for arg in arg_pairs_to_parse {
                                                let arg_expr = parse_expr_pratt(
                                                    arg.into_inner(),
                                                    referred_relation_ids,
                                                    worker,
                                                    plan
                                                )?;
                                                parse_exprs_args.push(arg_expr);
                                            }
                                        }
                                        rule => unreachable!("{}", format!("Unexpected rule under FunctionInvocation: {rule:?}"))
                                    }
                                }
                                return Ok(ParseExpression::Function {
                                    name: function_name,
                                    args: parse_exprs_args,
                                    is_distinct,
                                })
                            }
                            rule => unreachable!("Expr::parse expected identifier continuation, found {:?}", rule)
                        }
                    };

                    let plan_left_id = referred_relation_ids.first().expect("Reference must refer to at least one relational node");
                    let left_col_map = ColumnPositionMap::new(plan, *plan_left_id)?;

                    let left_child_col_position = if let Some(ref scan_name) = scan_name {
                        left_col_map.get_with_scan(&col_name, Some(scan_name))
                    } else {
                        left_col_map.get(&col_name)
                    };

                    let plan_right_id = referred_relation_ids.get(1);
                    let (ref_id, is_row) = if let Some(plan_right_id) = plan_right_id {
                        // Referencing Join node.
                        let right_col_map = ColumnPositionMap::new(plan, *plan_right_id)?;

                        let right_child_col_position = if let Some(scan_name) = scan_name {
                            right_col_map.get_with_scan(&col_name, Some(&scan_name))
                        } else {
                            right_col_map.get(&col_name)
                        };

                        let present_in_left = left_child_col_position.is_ok();
                        let present_in_right = right_child_col_position.is_ok();

                        let ref_id = if present_in_left && present_in_right {
                            return Err(SbroadError::Invalid(
                                Entity::Column,
                                Some(format!(
                                    "column name '{col_name}' is present in both join children",
                                )),
                            ));
                        } else if present_in_left {
                            plan.add_row_from_left_branch(
                                *plan_left_id,
                                *plan_right_id,
                                &[&col_name],
                            )?
                        } else if present_in_right {
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
                        };
                        (ref_id, true)
                    } else {
                        // Referencing single node.
                        let Ok(col_position) = left_child_col_position else {
                            return Err(SbroadError::NotFound(
                                Entity::Column,
                                format!("with name {col_name}"),
                            ));
                        };
                        let child = plan.get_relation_node(*plan_left_id)?;
                        let child_alias_ids = plan.get_expression_node(
                            child.output()
                        )?.get_row_list()?;
                        let child_alias_id = child_alias_ids
                            .get(col_position)
                            .expect("column position is invalid");
                        let col_type = plan
                            .get_expression_node(*child_alias_id)?
                            .calculate_type(plan)?;
                        let ref_id = plan.nodes.add_ref(None, Some(vec![0]), col_position, col_type);
                        (ref_id, false)
                    };
                    worker.reference_to_name_map.insert(ref_id, (col_name, is_row));
                    ParseExpression::PlanId { plan_id: ref_id }
                }
                Rule::SubQuery => {
                    let subquery_plan_id = *worker.subquery_ids_queue
                        .back()
                        .expect("Corresponding expression subquery is not found");
                    worker.subquery_ids_queue.pop_back();
                    ParseExpression::PlanId{ plan_id: subquery_plan_id }
                }
                Rule::Row => {
                    let mut children = Vec::new();

                    for expr_pair in primary.into_inner() {
                        let child_parse_expr = parse_expr_pratt(
                            expr_pair.into_inner(),
                            referred_relation_ids,
                            worker,
                            plan
                        )?;
                        children.push(child_parse_expr);
                    }
                    ParseExpression::Row { children }
                }
                Rule::Literal => {
                    parse_expr_pratt(primary.into_inner(), referred_relation_ids, worker, plan)?
                }
                Rule::Decimal
                | Rule::Double
                | Rule::Unsigned
                | Rule::Null
                | Rule::True
                | Rule::SingleQuotedString
                | Rule::Integer
                | Rule::False => {
                    let val = Value::from_node(&primary)?;
                    let plan_id = plan.add_const(val);
                    ParseExpression::PlanId { plan_id }
                }
                Rule::Exists => {
                    let mut inner_pairs = primary.into_inner();
                    let first_pair = inner_pairs.next()
                        .expect("No child found under Exists node");
                    let first_is_not = matches!(first_pair.as_rule(), Rule::NotFlag);
                    let expr_pair = if first_is_not {
                        inner_pairs.next()
                            .expect("Expr expected next to NotFlag under Exists")
                    } else {
                        first_pair
                    };

                    let child_parse_expr = parse_expr_pratt(
                        Pairs::single(expr_pair),
                        referred_relation_ids,
                        worker,
                        plan
                    )?;
                    ParseExpression::Exists { is_not: first_is_not, child: Box::new(child_parse_expr)}
                }
                Rule::Cast => {
                    let mut inner_pairs = primary.into_inner();
                    let expr_pair = inner_pairs.next().expect("Cast has no expr child.");
                    let child_parse_expr = parse_expr_pratt(
                        expr_pair.into_inner(),
                        referred_relation_ids,
                        worker,
                        plan
                    )?;
                    let type_pairs = inner_pairs.next().expect("Cast has no type child");
                    let cast_type = if type_pairs.as_rule() == Rule::ColumnDefType {
                        let mut column_def_type_pairs = type_pairs.into_inner();
                        let column_def_type = column_def_type_pairs.next()
                            .expect("concrete type expected under ColumnDefType");
                        if column_def_type.as_rule() == Rule::TypeVarchar {
                            let mut type_pairs_inner = column_def_type.into_inner();
                            let varchar_length = type_pairs_inner.next().expect("Length is missing under Varchar");
                            let len = varchar_length
                                .as_str()
                                .parse::<usize>()
                                .map_err(|e| {
                                    SbroadError::ParsingError(
                                        Entity::Value,
                                        format!("failed to parse varchar length: {e:?}"),
                                    )
                                })?;
                            Ok(CastType::Varchar(len))
                        } else {
                            CastType::try_from(&column_def_type.as_rule())
                        }
                    } else {
                        // TypeAny.
                        CastType::try_from(&type_pairs.as_rule())
                    }?;
                    ParseExpression::Cast { cast_type, child: Box::new(child_parse_expr) }
                }
                Rule::CountAsterisk => {
                    let plan_id = plan.nodes.push(Node::Expression(Expression::CountAsterisk));
                    ParseExpression::PlanId { plan_id }
                }
                rule      => unreachable!("Expr::parse expected atomic rule, found {:?}", rule),
            };
            Ok(parse_expr)
        })
        .map_infix(|lhs, op, rhs| {
            let mut is_not = false;
            let op = match op.as_rule() {
                Rule::And => ParseExpressionInfixOperator::InfixBool(Bool::And),
                Rule::Or => ParseExpressionInfixOperator::InfixBool(Bool::Or),
                Rule::Between => {
                    let mut op_inner = op.into_inner();
                    is_not = op_inner.next().is_some();
                    return Ok(ParseExpression::Between {
                        is_not,
                        left: Box::new(lhs?),
                        right: Box::new(rhs?),
                    })
                },
                Rule::Eq => ParseExpressionInfixOperator::InfixBool(Bool::Eq),
                Rule::NotEq => ParseExpressionInfixOperator::InfixBool(Bool::NotEq),
                Rule::Lt => ParseExpressionInfixOperator::InfixBool(Bool::Lt),
                Rule::LtEq => ParseExpressionInfixOperator::InfixBool(Bool::LtEq),
                Rule::Gt => ParseExpressionInfixOperator::InfixBool(Bool::Gt),
                Rule::GtEq => ParseExpressionInfixOperator::InfixBool(Bool::GtEq),
                Rule::In => {
                    let mut op_inner = op.into_inner();
                    is_not = op_inner.next().is_some();
                    ParseExpressionInfixOperator::InfixBool(Bool::In)
                }
                Rule::Subtract      => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Subtract),
                Rule::Divide        => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Divide),
                Rule::Multiply      => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Multiply),
                Rule::Add        => ParseExpressionInfixOperator::InfixArithmetic(Arithmetic::Add),
                Rule::ConcatInfixOp => ParseExpressionInfixOperator::Concat,
                rule           => unreachable!("Expr::parse expected infix operation, found {:?}", rule),
            };
            Ok(ParseExpression::Infix {
                op,
                is_not,
                left: Box::new(lhs?),
                right: Box::new(rhs?),
            })
        })
        .map_prefix(|op, child| {
            let op = match op.as_rule() {
                Rule::UnaryNot => Unary::Not,
                rule => unreachable!("Expr::parse expected prefix operator, found {:?}", rule),
            };
            Ok(ParseExpression::Prefix { op, child: Box::new(child?)})
        })
        .map_postfix(|child, op| {
            match op.as_rule() {
                Rule::IsNullPostfix => {
                    let is_not = match op.into_inner().len() {
                        1 => true,
                        0 => false,
                        _ => unreachable!("IsNull must have 0 or 1 children")
                    };
                    Ok(ParseExpression::IsNull { is_not, child: Box::new(child?)})
                },
                rule => unreachable!("Expr::parse expected postfix operator, found {:?}", rule),
            }
        })
        .parse(expression_pairs)
}

/// Parse expression pair and get plan id.
/// * Retrieve expressions tree-like structure using `parse_expr_pratt`
/// * Traverse tree to populate plan with new nodes
/// * Return `plan_id` of root Expression node
fn parse_expr<M>(
    expression_pairs: Pairs<Rule>,
    referred_relation_ids: &[usize],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<usize, SbroadError>
where
    M: Metadata,
{
    let parse_expr = parse_expr_pratt(expression_pairs, referred_relation_ids, worker, plan)?;
    parse_expr.populate_plan(plan, worker)
}

/// Generate an alias for the unnamed projection expressions.
#[must_use]
pub fn get_unnamed_column_alias(pos: usize) -> String {
    format!("COL_{pos}")
}

/// Map of { `AbstractSyntaxTree` node id -> parsing pairs copy, corresponding to ast node }.
struct ParsingPairsMap<'pairs_map> {
    inner: HashMap<usize, Pair<'pairs_map, Rule>>,
}

impl<'pairs_map> ParsingPairsMap<'pairs_map> {
    fn new() -> Self {
        Self {
            inner: HashMap::with_capacity(PARSING_PAIRS_MAP_CAPACITY),
        }
    }

    fn insert(
        &mut self,
        key: usize,
        value: Pair<'pairs_map, Rule>,
    ) -> Option<Pair<'pairs_map, Rule>> {
        self.inner.insert(key, value)
    }

    fn remove_pair(&mut self, key: usize) -> Pair<Rule> {
        self.inner
            .remove(&key)
            .expect("pairs_map doesn't contain value for key")
    }
}

impl AbstractSyntaxTree {
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
    /// Builds abstract syntax tree (AST) from SQL query.
    ///
    /// # Errors
    /// - Failed to parse an SQL query.
    #[otm_child_span("ast.parse")]
    fn fill<'query>(
        &mut self,
        query: &'query str,
        pairs_map: &mut ParsingPairsMap<'query>,
    ) -> Result<(), SbroadError> {
        let mut command_pair = match ParseTree::parse(Rule::Command, query) {
            Ok(p) => p,
            Err(e) => return Err(SbroadError::ParsingError(Entity::Rule, format!("{e}"))),
        };
        let top_pair = command_pair
            .next()
            .expect("Query expected as a first parsing tree child.");
        let top = StackParseNode::new(top_pair, None);

        let mut stack: Vec<StackParseNode> = vec![top];
        while !stack.is_empty() {
            let stack_node: StackParseNode = match stack.pop() {
                Some(node) => node,
                None => break,
            };

            // Save node to AST.
            let arena_node_id = self.nodes.push_node(ParseNode::new(
                stack_node.pair.as_rule(),
                Some(String::from(stack_node.pair.as_str())),
            ));

            // Save procedure body (a special case).
            if stack_node.pair.as_rule() == Rule::ProcBody {
                let span = stack_node.pair.as_span();
                let body = &query[span.start()..span.end()];
                self.nodes
                    .update_value(arena_node_id, Some(String::from(body)))?;
            }

            // Update parent's node children list.
            self.nodes
                .add_child(stack_node.arena_parent_id, arena_node_id)?;

            // Clean parent values (only leafs and special nodes like
            // procedure body should contain data)
            if let Some(parent) = stack_node.arena_parent_id {
                let parent_node = self.nodes.get_node(parent)?;
                if parent_node.rule != Rule::ProcBody {
                    self.nodes.update_value(parent, None)?;
                }
            }

            match stack_node.pair.as_rule() {
                Rule::Expr | Rule::Row | Rule::Literal => {
                    // * `Expr`s are parsed using Pratt parser with a separate `parse_expr`
                    //   function call on the stage of `resolve_metadata`.
                    // * `Row`s are added to support parsing Row expressions under `Values` nodes.
                    // * `Literal`s are added to support procedure calls which should not contain
                    //   all possible `Expr`s.
                    pairs_map.insert(arena_node_id, stack_node.pair.clone());
                }
                _ => {}
            }

            for parse_child in stack_node.pair.into_inner() {
                stack.push(StackParseNode::new(parse_child, Some(arena_node_id)));
            }
        }

        self.set_top(0)?;

        self.transform_update()?;
        self.transform_delete()?;
        self.transform_select()?;
        Ok(())
    }

    /// Function that transforms `AbstractSyntaxTree` into `Plan`.
    /// Build a plan from the AST with parameters as placeholders for the values.
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    #[allow(clippy::uninlined_format_args)]
    #[otm_child_span("ast.resolve")]
    fn resolve_metadata<M>(
        &self,
        metadata: &M,
        pairs_map: &mut ParsingPairsMap,
    ) -> Result<Plan, SbroadError>
    where
        M: Metadata,
    {
        let mut plan = Plan::default();

        let Some(top) = self.top else {
            return Err(SbroadError::Invalid(Entity::AST, None));
        };
        let capacity = self.nodes.arena.len();
        let mut dft_post = PostOrder::with_capacity(|node| self.nodes.ast_iter(node), capacity);
        // Map of { ast `ParseNode` id -> plan `Node` id }.
        let mut map = Translation::with_capacity(self.nodes.next_id());
        // Counter for `Expression::ValuesRow` output column name aliases ("COLUMN_<`col_idx`>").
        // Is it global for every `ValuesRow` met in the AST.
        let mut col_idx: usize = 0;
        let mut worker = ExpressionsWorker::new(metadata);

        for (_, id) in dft_post.iter(top) {
            let node = self.nodes.get_node(id)?;
            match &node.rule {
                Rule::Scan => {
                    let rel_child_id_ast = node
                        .children
                        .first()
                        .expect("could not find first child id in scan node");
                    let rel_child_id_plan = map.get(*rel_child_id_ast)?;
                    let rel_child_node = plan.get_relation_node(rel_child_id_plan)?;
                    if let Relational::ScanSubQuery { .. } = rel_child_node {
                        // We want `SubQuery` ids to be used only during expressions parsing.
                        worker.subquery_ids_queue.pop_back();
                    }

                    map.add(id, rel_child_id_plan);
                    if let Some(ast_alias_id) = node.children.get(1) {
                        let alias_name = parse_normalized_identifier(self, *ast_alias_id)?;
                        let scan = plan.get_mut_relation_node(rel_child_id_plan)?;
                        scan.set_scan_name(Some(alias_name))?;
                    }
                }
                Rule::ScanTable => {
                    let ast_table_id = node
                        .children
                        .first()
                        .expect("could not find first child id in scan table node");
                    let scan_id = plan.add_scan(
                        parse_normalized_identifier(self, *ast_table_id)?.as_str(),
                        None,
                    )?;
                    map.add(id, scan_id);
                }
                Rule::Table => {
                    // The thing is we don't want to normalize name.
                    // Should we fix `parse_identifier` or `table` logic?
                    let table_name = parse_string_value_node(self, id)?;
                    let t = metadata.table(table_name)?;
                    plan.add_rel(t);
                }
                Rule::SubQuery => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("child node id is not found among sub-query children.");
                    let plan_child_id = map.get(*ast_child_id)?;
                    let plan_sq_id = plan.add_sub_query(plan_child_id, None)?;
                    worker.subquery_ids_queue.push_front(plan_sq_id);
                    map.add(id, plan_sq_id);
                }
                Rule::VTableMaxRows => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("no children for sql_vdbe_max_steps option");
                    let val = parse_option(self, *ast_child_id, &mut worker, &mut plan)?;
                    plan.raw_options.push(OptionSpec {
                        kind: OptionKind::VTableMaxRows,
                        val,
                    });
                }
                Rule::SqlVdbeMaxSteps => {
                    let ast_child_id = node
                        .children
                        .first()
                        .expect("no children for sql_vdbe_max_steps option");
                    let val = parse_option(self, *ast_child_id, &mut worker, &mut plan)?;

                    plan.raw_options.push(OptionSpec {
                        kind: OptionKind::SqlVdbeMaxSteps,
                        val,
                    });
                }
                Rule::GroupBy => {
                    // Reminder: first GroupBy child in `node.children` is always a relational node.
                    let mut children: Vec<usize> = Vec::with_capacity(node.children.len());
                    let first_relational_child_ast_id =
                        node.children.first().expect("GroupBy has no children");
                    let first_relational_child_plan_id = map.get(*first_relational_child_ast_id)?;
                    children.push(first_relational_child_plan_id);
                    for ast_column_id in node.children.iter().skip(1) {
                        let expr_pair = pairs_map.remove_pair(*ast_column_id);
                        let expr_id = parse_expr(
                            Pairs::single(expr_pair),
                            &[first_relational_child_plan_id],
                            &mut worker,
                            &mut plan,
                        )?;
                        children.push(expr_id);
                    }
                    let groupby_id = plan.add_groupby_from_ast(&children)?;
                    map.add(id, groupby_id);
                }
                Rule::Join => {
                    // Reminder: Join structure = [left child, kind, right child, condition expr]
                    let ast_left_id = node.children.first().expect("Join has no children.");
                    let plan_left_id = map.get(*ast_left_id)?;

                    let ast_kind_id = node
                        .children
                        .get(1)
                        .expect("Kind not found among Join children.");
                    let ast_kind_node = self.nodes.get_node(*ast_kind_id)?;
                    let kind = match ast_kind_node.rule {
                        Rule::LeftJoinKind => JoinKind::LeftOuter,
                        Rule::InnerJoinKind => JoinKind::Inner,
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

                    let ast_right_id = node
                        .children
                        .get(2)
                        .expect("Right not found among Join children.");
                    let plan_right_id = map.get(*ast_right_id)?;

                    let ast_expr_id = node
                        .children
                        .get(3)
                        .expect("Condition not found among Join children");
                    let ast_expr = self.nodes.get_node(*ast_expr_id)?;
                    let cond_expr_child_id = ast_expr
                        .children
                        .first()
                        .expect("Expected to see child under Expr node");
                    let cond_expr_child = self.nodes.get_node(*cond_expr_child_id)?;
                    let condition_expr_id = if let Rule::True = cond_expr_child.rule {
                        plan.add_const(Value::Boolean(true))
                    } else {
                        let expr_pair = pairs_map.remove_pair(*ast_expr_id);
                        parse_expr(
                            Pairs::single(expr_pair),
                            &[plan_left_id, plan_right_id],
                            &mut worker,
                            &mut plan,
                        )?
                    };

                    let plan_join_id =
                        plan.add_join(plan_left_id, plan_right_id, condition_expr_id, kind)?;
                    map.add(id, plan_join_id);
                }
                Rule::Selection | Rule::Having => {
                    let ast_rel_child_id = node
                        .children
                        .first()
                        .expect("Selection or Having has no children.");
                    let plan_rel_child_id = map.get(*ast_rel_child_id)?;

                    let ast_expr_id = node
                        .children
                        .get(1)
                        .expect("Filter not found among Selection children");
                    let expr_pair = pairs_map.remove_pair(*ast_expr_id);
                    let expr_plan_node_id = parse_expr(
                        Pairs::single(expr_pair),
                        &[plan_rel_child_id],
                        &mut worker,
                        &mut plan,
                    )?;

                    let plan_node_id = match &node.rule {
                        Rule::Selection => {
                            plan.add_select(&[plan_rel_child_id], expr_plan_node_id)?
                        }
                        Rule::Having => plan.add_having(&[plan_rel_child_id], expr_plan_node_id)?,
                        _ => return Err(SbroadError::Invalid(Entity::AST, None)), // never happens
                    };
                    map.add(id, plan_node_id);
                }
                Rule::SelectWithOptionalContinuation => {
                    let first_select_id = node.children.first().expect(
                        "SelectWithOptionalContinuation must always have Select as a first child.",
                    );
                    let first_select_plan_id = map.get(*first_select_id)?;

                    let continuation_id = node.children.get(1);
                    if let Some(continuation_id) = continuation_id {
                        let continuation_node = self.nodes.get_node(*continuation_id)?;
                        match continuation_node.rule {
                            Rule::UnionAllContinuation => {
                                let second_select_id = continuation_node
                                    .children
                                    .first()
                                    .expect("UnionAllContinuation must contain Select as a first child.");
                                let second_select_plan_id = map.get(*second_select_id)?;
                                let plan_union_all_id = plan.add_union_all(first_select_plan_id, second_select_plan_id)?;
                                map.add(id, plan_union_all_id);
                            }
                            Rule::ExceptContinuation => {
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
                Rule::Projection => {
                    let (rel_child_id, other_children) = node
                        .children
                        .split_first()
                        .expect("More than one child expected under Projection");
                    let mut is_distinct: bool = false;
                    let mut ast_columns_ids = other_children;
                    let first_col_ast_id = other_children
                        .first()
                        .expect("At least one child expected under Projection");
                    if let Rule::Distinct = self.nodes.get_node(*first_col_ast_id)?.rule {
                        is_distinct = true;
                        (_, ast_columns_ids) = other_children
                            .split_first()
                            .expect("Projection must have some columns children");
                    }

                    let plan_rel_child_id = map.get(*rel_child_id)?;
                    let mut proj_columns: Vec<usize> = Vec::with_capacity(ast_columns_ids.len());

                    let mut unnamed_col_pos = 0;
                    for ast_column_id in ast_columns_ids {
                        let ast_column = self.nodes.get_node(*ast_column_id)?;
                        match ast_column.rule {
                            Rule::Column => {
                                let expr_ast_id = ast_column
                                    .children
                                    .first()
                                    .expect("Column has no children.");
                                let expr_pair = pairs_map.remove_pair(*expr_ast_id);
                                let expr_plan_node_id = parse_expr(
                                    Pairs::single(expr_pair),
                                    &[plan_rel_child_id],
                                    &mut worker,
                                    &mut plan,
                                )?;

                                let alias_name =
                                    if let Some(alias_ast_node_id) = ast_column.children.get(1) {
                                        parse_normalized_identifier(self, *alias_ast_node_id)?
                                    } else {
                                        // We don't use `get_expression_node` here, because we may encounter a `Parameter`.
                                        if let Node::Expression(Expression::Reference { .. }) =
                                            plan.get_node(expr_plan_node_id)?
                                        {
                                            let (col_name, _) = worker
                                                .reference_to_name_map
                                                .get(&expr_plan_node_id)
                                                .expect("reference must be in a map");
                                            normalize_name_from_sql(col_name.as_str())
                                        } else {
                                            unnamed_col_pos += 1;
                                            get_unnamed_column_alias(unnamed_col_pos)
                                        }
                                    };

                                let plan_alias_id = plan.nodes.add_alias(
                                    &normalize_name_from_sql(&alias_name),
                                    expr_plan_node_id,
                                )?;
                                proj_columns.push(plan_alias_id);
                            }
                            Rule::Asterisk => {
                                let plan_asterisk_id =
                                    plan.add_row_for_output(plan_rel_child_id, &[], false)?;
                                let Node::Expression(Expression::Row { list, .. }) =
                                    plan.get_node(plan_asterisk_id).expect(
                                        "`add_row_for_output` must've added an Expression::Row",
                                    )
                                else {
                                    unreachable!("Row expected under Asterisk")
                                };
                                for row_id in list {
                                    proj_columns.push(*row_id);
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
                        plan.add_proj_internal(plan_rel_child_id, &proj_columns, is_distinct)?;
                    map.add(id, projection_id);
                }
                Rule::Values => {
                    let mut plan_value_row_ids: Vec<usize> =
                        Vec::with_capacity(node.children.len());
                    for ast_child_id in &node.children {
                        let row_pair = pairs_map.remove_pair(*ast_child_id);
                        let expr_id =
                            parse_expr(Pairs::single(row_pair), &[], &mut worker, &mut plan)?;
                        let values_row_id = plan.add_values_row(expr_id, &mut col_idx)?;
                        plan_value_row_ids.push(values_row_id);
                    }
                    let plan_values_id = plan.add_values(plan_value_row_ids)?;
                    map.add(id, plan_values_id);
                }
                Rule::Update => {
                    let rel_child_ast_id = node
                        .children
                        .first()
                        .expect("Update must have at least two children.");
                    let rel_child_id = map.get(*rel_child_ast_id)?;

                    let ast_scan_table_id = node
                        .children
                        .get(1)
                        .expect("Update must have at least two children.");
                    let plan_scan_id = map.get(*ast_scan_table_id)?;
                    let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
                    let scan_relation =
                        if let Relational::ScanRelation { relation, .. } = plan_scan_node {
                            relation.clone()
                        } else {
                            unreachable!("Scan expected under Update")
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
                    let mut col_name_to_position_map: HashMap<&str, (&ColumnRole, usize)> =
                        HashMap::new();

                    let relation = plan
                        .relations
                        .get(&scan_relation)
                        .ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Table,
                                format!("{scan_relation} among plan relations"),
                            )
                        })?
                        .clone();
                    relation.columns.iter().enumerate().for_each(|(i, c)| {
                        col_name_to_position_map.insert(c.name.as_str(), (c.get_role(), i));
                    });

                    let mut pk_positions: HashSet<usize> =
                        HashSet::with_capacity(relation.primary_key.positions.len());
                    relation.primary_key.positions.iter().for_each(|pos| {
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

                        let expr_pair = pairs_map.remove_pair(*expr_ast_id);
                        let expr_plan_node_id = parse_expr(
                            Pairs::single(expr_pair),
                            &[rel_child_id],
                            &mut worker,
                            &mut plan,
                        )?;

                        if plan.contains_aggregates(expr_plan_node_id, true)? {
                            return Err(SbroadError::Invalid(
                                Entity::Query,
                                Some(
                                    "aggregate functions are not supported in update expression."
                                        .into(),
                                ),
                            ));
                        }
                        let col_name = parse_normalized_identifier(self, *ast_column_id)?;
                        match col_name_to_position_map.get(col_name.as_str()) {
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
                                        Some(format!(
                                            "The same column is specified twice in update list: {}",
                                            col_name
                                        )),
                                    ));
                                }
                                update_defs.insert(*pos, expr_plan_node_id);
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
                    }
                    let update_id = plan.add_update(&scan_relation, &update_defs, rel_child_id)?;
                    map.add(id, update_id);
                }
                Rule::Delete => {
                    // Get table name and selection plan node id.
                    // Reminder: first child of Delete is a `ScanTable` or `DeleteFilter`
                    //           (under which there must be a `ScanTable`).
                    let first_child_id = node
                        .children
                        .first()
                        .expect("Delte must have at least one child");
                    let first_child_node = self.nodes.get_node(*first_child_id)?;
                    let (proj_child_id, table_name) = match first_child_node.rule {
                        Rule::ScanTable => {
                            let plan_scan_id = map.get(*first_child_id)?;
                            let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
                            let Relational::ScanRelation { relation, .. } = plan_scan_node else {
                                unreachable!("Scan expected under ScanTable")
                            };
                            (plan_scan_id, relation.clone())
                        }
                        Rule::DeleteFilter => {
                            let ast_table_id = first_child_node
                                .children
                                .first()
                                .expect("Table not found among DeleteFilter children");
                            let plan_scan_id = map.get(*ast_table_id)?;
                            let plan_scan_node = plan.get_relation_node(plan_scan_id)?;
                            let relation_name =
                                if let Relational::ScanRelation { relation, .. } = plan_scan_node {
                                    relation.clone()
                                } else {
                                    unreachable!("Scan expected under DeleteFilter")
                                };

                            let ast_expr_id = first_child_node
                                .children
                                .get(1)
                                .expect("Expr not found among DeleteFilter children");
                            let expr_pair = pairs_map.remove_pair(*ast_expr_id);
                            let expr_plan_node_id = parse_expr(
                                Pairs::single(expr_pair),
                                &[plan_scan_id],
                                &mut worker,
                                &mut plan,
                            )?;

                            let plan_select_id =
                                plan.add_select(&[plan_scan_id], expr_plan_node_id)?;
                            (plan_select_id, relation_name)
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Node,
                                Some(format!(
                                    "AST delete node {:?} contains unexpected children",
                                    first_child_node,
                                )),
                            ));
                        }
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
                Rule::Insert => {
                    let ast_table_id = node.children.first().expect("Insert has no children.");
                    let relation = parse_normalized_identifier(self, *ast_table_id)?;

                    let ast_child_id = node
                        .children
                        .get(1)
                        .expect("Second child not found among Insert children");
                    let get_conflict_strategy =
                        |child_idx: usize| -> Result<ConflictStrategy, SbroadError> {
                            let Some(child_id) = node.children.get(child_idx).copied() else {
                                return Ok(ConflictStrategy::DoFail);
                            };
                            let rule = &self.nodes.get_node(child_id)?.rule;
                            let res = match rule {
                                Rule::DoNothing => ConflictStrategy::DoNothing,
                                Rule::DoReplace => ConflictStrategy::DoReplace,
                                Rule::DoFail => ConflictStrategy::DoFail,
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
                    let plan_insert_id = if let Rule::TargetColumns = ast_child.rule {
                        // insert into t (a, b, c) ...
                        let mut selected_col_names: Vec<String> =
                            Vec::with_capacity(ast_child.children.len());
                        for col_id in &ast_child.children {
                            selected_col_names.push(parse_normalized_identifier(self, *col_id)?);
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
                            if !column.is_nullable && !selected_col_names.contains(&column.name) {
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
                Rule::Explain => {
                    plan.mark_as_explain();

                    let ast_child_id = node.children.first().expect("Explain has no children.");
                    map.add(0, map.get(*ast_child_id)?);
                }
                Rule::Query => {
                    // Query may have two children:
                    // 1. select | insert | except | ..
                    // 2. Option child - for which no plan node is created
                    let child_id =
                        map.get(*node.children.first().expect("no children for Query rule"))?;
                    map.add(id, child_id);
                }
                Rule::Block => {
                    // Query may have two children:
                    // 1. call
                    // 2. Option child - for which no plan node is created
                    let child_id =
                        map.get(*node.children.first().expect("no children for Block rule"))?;
                    map.add(id, child_id);
                }
                Rule::CallProc => {
                    let call_proc = parse_call_proc(self, node, pairs_map, &mut worker, &mut plan)?;
                    let plan_id = plan.nodes.push(Node::Block(call_proc));
                    map.add(id, plan_id);
                }
                Rule::CreateProc => {
                    let create_proc = parse_create_proc(self, node)?;
                    let plan_id = plan.nodes.push(Node::Ddl(create_proc));
                    map.add(id, plan_id);
                }
                Rule::CreateTable => {
                    let create_sharded_table = parse_create_table(self, node)?;
                    let plan_id = plan.nodes.push(Node::Ddl(create_sharded_table));
                    map.add(id, plan_id);
                }
                Rule::GrantPrivilege => {
                    let (grant_type, grantee_name, timeout) = parse_grant_revoke(node, self)?;
                    let grant_privilege = Acl::GrantPrivilege {
                        grant_type,
                        grantee_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Acl(grant_privilege));
                    map.add(id, plan_id);
                }
                Rule::RevokePrivilege => {
                    let (revoke_type, grantee_name, timeout) = parse_grant_revoke(node, self)?;
                    let revoke_privilege = Acl::RevokePrivilege {
                        revoke_type,
                        grantee_name,
                        timeout,
                    };
                    let plan_id = plan.nodes.push(Node::Acl(revoke_privilege));
                    map.add(id, plan_id);
                }
                Rule::DropRole => {
                    let role_name_id = node
                        .children
                        .first()
                        .expect("RoleName expected under DropRole node");
                    let role_name = parse_identifier(self, *role_name_id)?;

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
                Rule::DropTable => {
                    let mut table_name: String = String::new();
                    let mut timeout = get_default_timeout();
                    for child_id in &node.children {
                        let child_node = self.nodes.get_node(*child_id)?;
                        match child_node.rule {
                            Rule::Table => {
                                table_name = parse_identifier(self, *child_id)?;
                            }
                            Rule::Timeout => {
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
                Rule::DropProc => {
                    let drop_proc = parse_drop_proc(self, node)?;
                    let plan_id = plan.nodes.push(Node::Ddl(drop_proc));
                    map.add(id, plan_id);
                }
                Rule::RenameProc => {
                    let rename_proc = parse_rename_proc(self, node)?;
                    let plan_id = plan.nodes.push(Node::Ddl(rename_proc));
                    map.add(id, plan_id);
                }
                Rule::AlterUser => {
                    let user_name_node_id = node
                        .children
                        .first()
                        .expect("RoleName expected as a first child");
                    let user_name = parse_identifier(self, *user_name_node_id)?;

                    let alter_option_node_id = node
                        .children
                        .get(1)
                        .expect("Some AlterOption expected as a second child");
                    let alter_option_node = self.nodes.get_node(*alter_option_node_id)?;
                    let alter_option = match alter_option_node.rule {
                        Rule::AlterLogin => AlterOption::Login,
                        Rule::AlterNoLogin => AlterOption::NoLogin,
                        Rule::AlterPassword => {
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
                Rule::CreateUser => {
                    let mut iter = node.children.iter();
                    let user_name_node_id = iter.next().ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::ParseNode,
                            Some(String::from("RoleName expected as a first child")),
                        )
                    })?;
                    let user_name = parse_identifier(self, *user_name_node_id)?;

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
                            Rule::Timeout => {
                                timeout = get_timeout(self, *child_id)?;
                            }
                            Rule::AuthMethod => {
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
                Rule::DropUser => {
                    let user_name_id = node
                        .children
                        .first()
                        .expect("RoleName expected under DropUser node");
                    let user_name = parse_identifier(self, *user_name_id)?;

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
                Rule::CreateRole => {
                    let role_name_id = node
                        .children
                        .first()
                        .expect("RoleName expected under CreateRole node");
                    let role_name = parse_identifier(self, *role_name_id)?;

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
                _ => {}
            }
        }
        // get root node id
        let plan_top_id = map
            .get(self.top.ok_or_else(|| {
                SbroadError::Invalid(Entity::AST, Some("no top in AST".into()))
            })?)?;
        plan.set_top(plan_top_id)?;
        let replaces = plan.replace_sq_with_references()?;
        plan.fix_betweens(&worker.betweens, &replaces)?;
        Ok(plan)
    }
}

impl Ast for AbstractSyntaxTree {
    fn transform_into_plan<M>(query: &str, metadata: &M) -> Result<Plan, SbroadError>
    where
        M: Metadata + Sized,
    {
        // While traversing pest `Pair`s iterator, we build a tree-like structure in a view of
        // { `rule`, `children`, ... }, where `children` is a vector of `ParseNode` ids that were
        // previously added in ast arena.
        // Children appears after unwrapping `Pair` structure, but in a case of `Expr` nodes, that
        // we'd like to handle separately, we don't want to unwrap it for future use.
        // That's why we:
        // * Add expressions `ParseNode`s into `arena`
        // * Save copy of them into map of { expr_arena_id -> corresponding pair copy }.
        let mut ast_id_to_pairs_map = ParsingPairsMap::new();
        let mut ast = AbstractSyntaxTree::empty();
        ast.fill(query, &mut ast_id_to_pairs_map)?;
        ast.resolve_metadata(metadata, &mut ast_id_to_pairs_map)
    }
}

impl Plan {
    /// Wrap references, constants, functions, concatenations and casts in the plan into rows.
    /// Leave other nodes (e.g. rows) unchanged.
    ///
    /// Used for unification of expression nodes transformations (e.g. dnf).
    fn row(&mut self, expr_id: usize) -> Result<usize, SbroadError> {
        let row_id = if let Node::Expression(
            Expression::Reference { .. }
            | Expression::Constant { .. }
            | Expression::Cast { .. }
            | Expression::Concat { .. }
            | Expression::StableFunction { .. },
        ) = self.get_node(expr_id)?
        {
            self.nodes.add_row(vec![expr_id], None)
        } else {
            expr_id
        };
        Ok(row_id)
    }
}

pub mod ast;
pub mod ir;
pub mod tree;
