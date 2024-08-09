//! SQL frontend module.
//!
//! Parses an SQL statement to the abstract syntax tree (AST)
//! and builds the intermediate representation (IR).

use ahash::{AHashMap, AHashSet};
use core::panic;
use itertools::Itertools;
use pest::iterators::{Pair, Pairs};
use pest::pratt_parser::PrattParser;
use pest::Parser;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::collections::VecDeque;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
use tarantool::datetime::Datetime;
use tarantool::index::{IndexType, RtreeIndexDistanceType};
use time::{OffsetDateTime, Time};

use crate::errors::{Action, Entity, SbroadError};
use crate::executor::engine::{helpers::normalize_name_for_space_api, Metadata};
use crate::frontend::sql::ast::{
    AbstractSyntaxTree, ParseNode, ParseNodes, ParseTree, Rule, StackParseNode,
};
use crate::frontend::sql::ir::Translation;
use crate::frontend::Ast;
use crate::ir::ddl::{ColumnDef, Ddl, SetParamScopeType, SetParamValue};
use crate::ir::ddl::{Language, ParamDef};
use crate::ir::expression::cast::Type as CastType;
use crate::ir::expression::{
    ColumnPositionMap, ColumnWithScan, ColumnsRetrievalSpec, Expression, ExpressionId,
    FunctionFeature, Position, TrimKind,
};
use crate::ir::operator::{
    Arithmetic, Bool, ConflictStrategy, JoinKind, OrderByElement, OrderByEntity, OrderByType,
    Relational, Unary,
};
use crate::ir::relation::{Column, ColumnRole, TableKind, Type as RelationType};
use crate::ir::tree::traversal::{PostOrder, EXPR_CAPACITY};
use crate::ir::value::Value;
use crate::ir::{Node, NodeId, OptionKind, OptionParamValue, OptionSpec, Plan};
use crate::otm::child_span;

use crate::errors::Entity::AST;
use crate::executor::engine::helpers::normalize_name_from_sql;
use crate::ir::acl::AlterOption;
use crate::ir::acl::{Acl, GrantRevokeType, Privilege};
use crate::ir::aggregates::AggregateKind;
use crate::ir::block::Block;
use crate::ir::expression::NewColumnsSource;
use crate::ir::helpers::RepeatableState;
use crate::ir::transformation::redistribution::ColumnPosition;
use crate::warn;
use sbroad_proc::otm_child_span;
use tarantool::decimal::Decimal;
use tarantool::space::SpaceEngineType;

// DDL timeout in seconds (1 day).
const DEFAULT_TIMEOUT_F64: f64 = 24.0 * 60.0 * 60.0;
const DEFAULT_AUTH_METHOD: &str = "chap-sha1";

fn get_default_timeout() -> Decimal {
    Decimal::from_str(&format!("{DEFAULT_TIMEOUT_F64}")).expect("default timeout casting failed")
}

fn get_default_auth_method() -> SmolStr {
    SmolStr::from(DEFAULT_AUTH_METHOD)
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

// Helper map to store CTE node ids by their names.
type CTEs = AHashMap<SmolStr, NodeId>;

#[allow(clippy::uninlined_format_args)]
fn get_timeout(ast: &AbstractSyntaxTree, node_id: usize) -> Result<Decimal, SbroadError> {
    let param_node = ast.nodes.get_node(node_id)?;
    if let (Some(duration_id), None) = (param_node.children.first(), param_node.children.get(1)) {
        let duration_node = ast.nodes.get_node(*duration_id)?;
        if duration_node.rule != Rule::Duration {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "AST table option duration node {:?} contains unexpected children",
                    duration_node,
                )),
            ));
        }
        if let Some(duration_value) = duration_node.value.as_ref() {
            let res = Decimal::from_str(duration_value).map_err(|_| {
                SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
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

fn parse_string_literal(ast: &AbstractSyntaxTree, node_id: usize) -> Result<SmolStr, SbroadError> {
    let node = ast.nodes.get_node(node_id)?;
    let str_ref = node
        .value
        .as_ref()
        .expect("Rule node must contain string value.");
    assert!(
        node.rule == Rule::SingleQuotedString,
        "Expected SingleQuotedString, got: {:?}",
        node.rule
    );

    Ok(str_ref[1..str_ref.len() - 1].to_smolstr())
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
            Rule::TypeUuid => RelationType::Uuid,
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

    let mut old_name = SmolStr::default();
    let mut new_name = SmolStr::default();
    let mut params: Option<Vec<ParamDef>> = None;
    let mut timeout = get_default_timeout();
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::NewProc => {
                new_name = parse_identifier(ast, *child_id)?;
            }
            Rule::OldProc => {
                old_name = parse_identifier(ast, *child_id)?;
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
    let mut body = SmolStr::default();
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
) -> Result<(SmolStr, Option<Vec<ParamDef>>), SbroadError> {
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
fn parse_create_index(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Ddl, SbroadError> {
    assert_eq!(node.rule, Rule::CreateIndex);
    let mut name = SmolStr::default();
    let mut table_name = SmolStr::default();
    let mut columns = Vec::new();
    let mut unique = false;
    let mut index_type = IndexType::Tree;
    let mut bloom_fpr = None;
    let mut page_size = None;
    let mut range_size = None;
    let mut run_count_per_level = None;
    let mut run_size_ratio = None;
    let mut dimension = None;
    let mut distance = None;
    let mut hint = None;
    let mut timeout = get_default_timeout();

    let first_child = |node: &ParseNode| -> &ParseNode {
        let child_id = node.children.first().expect("Expected to see first child");
        ast.nodes
            .get_node(*child_id)
            .expect("Expected to see first child node")
    };
    let decimal_value = |node: &ParseNode| -> Decimal {
        Decimal::from_str(
            first_child(node)
                .value
                .as_ref()
                .expect("Expected to see Decimal value")
                .as_str(),
        )
        .expect("Expected to parse decimal value")
    };
    let u32_value = |node: &ParseNode| -> u32 {
        first_child(node)
            .value
            .as_ref()
            .expect("Expected to see u32 value")
            .parse()
            .expect("Expected to parse u32 value")
    };
    let bool_value = |node: &ParseNode| -> bool {
        let node = first_child(node);
        match node.rule {
            Rule::True => true,
            Rule::False => false,
            _ => panic!("expected True or False rule!"),
        }
    };

    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Unique => unique = true,
            Rule::Identifier => name = parse_identifier(ast, *child_id)?,
            Rule::Table => table_name = parse_identifier(ast, *child_id)?,
            Rule::IndexType => {
                let type_node = first_child(child_node);
                match type_node.rule {
                    Rule::Tree => index_type = IndexType::Tree,
                    Rule::Hash => index_type = IndexType::Hash,
                    Rule::RTree => index_type = IndexType::Rtree,
                    Rule::BitSet => index_type = IndexType::Bitset,
                    _ => panic!("Unexpected type node: {type_node:?}"),
                }
            }
            Rule::Parts => {
                let parts_node = ast.nodes.get_node(*child_id)?;
                columns.reserve(parts_node.children.len());
                for part_id in &parts_node.children {
                    let single_part_node = ast.nodes.get_node(*part_id)?;
                    assert!(
                        single_part_node.rule == Rule::Identifier,
                        "Unexpected part node: {single_part_node:?}"
                    );
                    columns.push(parse_identifier(ast, *part_id)?);
                }
            }
            Rule::IndexOptions => {
                let options_node = ast.nodes.get_node(*child_id)?;
                for option_id in &options_node.children {
                    let option_param_node = ast.nodes.get_node(*option_id)?;
                    assert!(
                        option_param_node.rule == Rule::IndexOptionParam,
                        "Unexpected option node: {option_param_node:?}"
                    );
                    let param_node = first_child(option_param_node);
                    match param_node.rule {
                        Rule::BloomFpr => bloom_fpr = Some(decimal_value(param_node)),
                        Rule::PageSize => page_size = Some(u32_value(param_node)),
                        Rule::RangeSize => range_size = Some(u32_value(param_node)),
                        Rule::RunCountPerLevel => run_count_per_level = Some(u32_value(param_node)),
                        Rule::RunSizeRatio => run_size_ratio = Some(decimal_value(param_node)),
                        Rule::Dimension => dimension = Some(u32_value(param_node)),
                        Rule::Distance => {
                            let distance_node = first_child(param_node);
                            match distance_node.rule {
                                Rule::Euclid => distance = Some(RtreeIndexDistanceType::Euclid),
                                Rule::Manhattan => {
                                    distance = Some(RtreeIndexDistanceType::Manhattan);
                                }
                                _ => panic!("Unexpected distance node: {distance_node:?}"),
                            }
                        }
                        Rule::Hint => {
                            hint = Some(bool_value(param_node));
                            warn!(None, "Hint option is not supported yet");
                        }
                        _ => panic!("Unexpected option param node: {param_node:?}"),
                    }
                }
            }
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            _ => panic!("Unexpected index rule: {child_node:?}"),
        }
    }
    let index = Ddl::CreateIndex {
        name,
        table_name,
        columns,
        unique,
        index_type,
        bloom_fpr,
        page_size,
        range_size,
        run_count_per_level,
        run_size_ratio,
        dimension,
        distance,
        hint,
        timeout,
    };
    Ok(index)
}

fn parse_drop_index(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Ddl, SbroadError> {
    assert_eq!(node.rule, Rule::DropIndex);
    let mut name = SmolStr::default();
    let mut timeout = get_default_timeout();
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Identifier => name = parse_identifier(ast, *child_id)?,
            Rule::Timeout => timeout = get_timeout(ast, *child_id)?,
            _ => panic!("Unexpected drop index node: {child_node:?}"),
        }
    }
    Ok(Ddl::DropIndex { name, timeout })
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::uninlined_format_args)]
fn parse_create_table(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Ddl, SbroadError> {
    assert_eq!(
        node.rule,
        Rule::CreateTable,
        "Expected rule CreateTable, got {:?}.",
        node.rule
    );
    let mut table_name = SmolStr::default();
    let mut columns: Vec<ColumnDef> = Vec::new();
    let mut pk_keys: Vec<SmolStr> = Vec::new();
    let mut shard_key: Vec<SmolStr> = Vec::new();
    let mut engine_type: SpaceEngineType = SpaceEngineType::default();
    let mut explicit_null_columns: AHashSet<SmolStr> = AHashSet::new();
    let mut timeout = get_default_timeout();
    let mut tier = None;
    let mut is_global = false;

    let nullable_primary_key_column_error = Err(SbroadError::Invalid(
        Entity::Column,
        Some(SmolStr::from(
            "Primary key mustn't contain nullable columns.",
        )),
    ));
    let primary_key_already_declared_error = Err(SbroadError::Invalid(
        Entity::Node,
        Some(format_smolstr!("Primary key has been already declared.",)),
    ));

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
                                    Rule::TypeDatetime => {
                                        column_def.data_type = RelationType::Datetime;
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
                                    Rule::TypeUuid => {
                                        column_def.data_type = RelationType::Uuid;
                                    }
                                    _ => {
                                        panic!(
                                            "Met unexpected rule under ColumnDef: {:?}.",
                                            type_node.rule
                                        );
                                    }
                                }
                            }
                            Rule::ColumnDefIsNull => {
                                match (
                                    def_child_node.children.first(),
                                    def_child_node.children.get(1),
                                ) {
                                    (None, None) => {
                                        explicit_null_columns.insert(column_def.name.clone());
                                        column_def.is_nullable = true;
                                    }
                                    (Some(child_id), None) => {
                                        let not_flag_node = ast.nodes.get_node(*child_id)?;
                                        if let Rule::NotFlag = not_flag_node.rule {
                                            column_def.is_nullable = false;
                                        } else {
                                            panic!(
                                                "Expected NotFlag rule, got: {:?}.",
                                                not_flag_node.rule
                                            );
                                        }
                                    }
                                    _ => panic!("Unexpected rule met under ColumnDefIsNull."),
                                }
                            }
                            Rule::PrimaryKeyMark => {
                                if !pk_keys.is_empty() {
                                    return primary_key_already_declared_error;
                                }
                                if column_def.is_nullable
                                    && explicit_null_columns.contains(&column_def.name)
                                {
                                    return nullable_primary_key_column_error;
                                }
                                // Infer not null on primary key column
                                column_def.is_nullable = false;
                                pk_keys.push(column_def.name.clone());
                            }
                            _ => panic!("Unexpected rules met under ColumnDef."),
                        }
                    }
                    columns.push(column_def);
                }
            }
            Rule::PrimaryKey => {
                if !pk_keys.is_empty() {
                    return primary_key_already_declared_error;
                }
                let pk_node = ast.nodes.get_node(*child_id)?;

                // First child is a `PrimaryKeyMark` that we should skip.
                for pk_col_id in pk_node.children.iter().skip(1) {
                    let pk_col_name = parse_identifier(ast, *pk_col_id)?;
                    let mut column_found = false;
                    for column in &mut columns {
                        if column.name == pk_col_name {
                            column_found = true;
                            if column.is_nullable && explicit_null_columns.contains(&column.name) {
                                return nullable_primary_key_column_error;
                            }
                            // Infer not null on primary key column
                            column.is_nullable = false;
                        }
                    }
                    if !column_found {
                        return Err(SbroadError::Invalid(
                            Entity::Column,
                            Some(format_smolstr!(
                                "Primary key column {pk_col_name} not found."
                            )),
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
                            engine_type = SpaceEngineType::Vinyl;
                        }
                        _ => panic!("Unexpected rule met under Engine."),
                    }
                } else {
                    panic!("Engine rule contains more than one child rule.")
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
                        Rule::Global => {
                            is_global = true;
                        }
                        Rule::Sharding => {
                            let sharding_node = ast.nodes.get_node(*distribution_type_id)?;
                            for sharding_node_child in &sharding_node.children {
                                let shard_child = ast.nodes.get_node(*sharding_node_child)?;
                                match shard_child.rule {
                                    Rule::Tier => {
                                        let Some(tier_node_id) = shard_child.children.first()
                                        else {
                                            return Err(SbroadError::Invalid(
                                                Entity::Node,
                                                Some(format_smolstr!(
                                                    "AST table tier node {:?} contains unexpected children",
                                                    distribution_type_node,
                                                )),
                                            ));
                                        };

                                        let tier_name = parse_identifier(ast, *tier_node_id)?;
                                        tier = Some(tier_name);
                                    }
                                    Rule::Identifier => {
                                        let shard_col_name =
                                            parse_identifier(ast, *sharding_node_child)?;

                                        let column_found =
                                            columns.iter().find(|c| c.name == shard_col_name);
                                        if column_found.is_none() {
                                            return Err(SbroadError::Invalid(
                                                Entity::Column,
                                                Some(format_smolstr!(
                                            "Sharding key column {shard_col_name} not found."
                                        )),
                                            ));
                                        }

                                        if let Some(column) = column_found {
                                            if !column.data_type.is_scalar() {
                                                return Err(SbroadError::Invalid(
                                            Entity::Column,
                                            Some(format_smolstr!(
                                                "Sharding key column {shard_col_name} is not of scalar type."
                                            )),
                                        ));
                                            }
                                        }

                                        shard_key.push(shard_col_name);
                                    }
                                    _ => {
                                        return Err(SbroadError::Invalid(
                                            Entity::Node,
                                            Some(format_smolstr!(
                                                "AST table sharding node {:?} contains unexpected children",
                                                distribution_type_node,
                                            )),
                                        ));
                                    }
                                }
                            }
                        }
                        _ => panic!("Unexpected rule met under Distribution."),
                    }
                } else {
                    panic!("Distribution rule contains more than one child rule.")
                }
            }
            Rule::Timeout => {
                timeout = get_timeout(ast, *child_id)?;
            }
            _ => panic!("Unexpected rule met under CreateTable."),
        }
    }
    if pk_keys.is_empty() {
        return Err(SbroadError::Invalid(
            Entity::PrimaryKey,
            Some(format_smolstr!("Primary key must be declared.")),
        ));
    }
    // infer sharding key from primary key
    if shard_key.is_empty() && !is_global {
        shard_key = pk_keys.clone();
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
            tier,
        }
    } else {
        Ddl::CreateTable {
            name: table_name,
            format: columns,
            primary_key: pk_keys,
            sharding_key: Some(shard_key),
            engine_type,
            timeout,
            tier,
        }
    };
    Ok(create_sharded_table)
}

fn parse_set_param(ast: &AbstractSyntaxTree, node: &ParseNode) -> Result<Ddl, SbroadError> {
    let mut scope_type = SetParamScopeType::Session;
    let mut param_value = None;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::SetScope => {
                let set_scope_child_id = child_node
                    .children
                    .first()
                    .expect("SetScope must have child.");
                let set_scope_child = ast.nodes.get_node(*set_scope_child_id)?;
                match set_scope_child.rule {
                    Rule::ScopeSession => {}
                    Rule::ScopeLocal => scope_type = SetParamScopeType::Local,
                    _ => panic!("Unexpected rule met under SetScope."),
                }
            }
            Rule::ConfParam => {
                let conf_param_child_id = child_node
                    .children
                    .first()
                    .expect("ConfParam must have child.");
                let conf_param_child = ast.nodes.get_node(*conf_param_child_id)?;
                match conf_param_child.rule {
                    Rule::NamedParam => {
                        let param_name_id = conf_param_child
                            .children
                            .first()
                            .expect("Param name expected under NamedParam.");
                        let param_name = parse_identifier(ast, *param_name_id)?;
                        param_value = Some(SetParamValue::NamedParam { name: param_name });
                    }
                    Rule::TimeZoneParam => param_value = Some(SetParamValue::TimeZone),
                    _ => panic!("Unexpected rule met under ConfParam."),
                }
            }
            _ => panic!("Unexpected rule met under SetParam."),
        }
    }
    Ok(Ddl::SetParam {
        scope_type,
        param_value: param_value.unwrap(),
        timeout: get_default_timeout(),
    })
}

fn parse_select_full(
    ast: &AbstractSyntaxTree,
    node_id: usize,
    map: &mut Translation,
) -> Result<(), SbroadError> {
    let node = ast.nodes.get_node(node_id)?;
    assert_eq!(node.rule, Rule::SelectFull);
    let mut top_id = None;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Cte => continue,
            Rule::SelectWithOptionalContinuation => {
                let select_id = map.get(*child_id)?;
                top_id = Some(select_id);
            }
            _ => unreachable!("Unexpected node: {child_node:?}"),
        }
    }
    let top_id = top_id.expect("Select must contain at least one child");
    map.add(node_id, top_id);
    Ok(())
}

fn parse_scan_cte_or_table<M>(
    ast: &AbstractSyntaxTree,
    metadata: &M,
    node_id: usize,
    map: &mut Translation,
    ctes: &mut CTEs,
    plan: &mut Plan,
) -> Result<(), SbroadError>
where
    M: Metadata,
{
    let node = ast.nodes.get_node(node_id)?;
    assert_eq!(node.rule, Rule::ScanCteOrTable);
    let scan_name = parse_normalized_identifier(ast, node_id)?;
    // First we try to find a table with the given name.
    let table = metadata.table(scan_name.as_str());
    match table {
        Ok(table) => {
            // We should also check that CTE with the same name doesn't exist.
            if ctes.contains_key(&scan_name) {
                return Err(SbroadError::Invalid(
                    Entity::Table,
                    Some(format_smolstr!(
                        "table with name {scan_name} is already defined as a CTE",
                    )),
                ));
            }
            plan.add_rel(table);
            let scan_id = plan.add_scan(scan_name.as_str(), None)?;
            map.add(node_id, scan_id);
        }
        Err(SbroadError::NotFound(..)) => {
            // If the table is not found, we try to find a CTE with the given name.
            let cte_id = *ctes.get(&scan_name).ok_or_else(|| {
                SbroadError::NotFound(Entity::Table, format_smolstr!("with name {scan_name}"))
            })?;
            map.add(node_id, cte_id);
        }
        Err(e) => return Err(e),
    }
    Ok(())
}

fn parse_cte(
    ast: &AbstractSyntaxTree,
    node_id: usize,
    map: &mut Translation,
    ctes: &mut CTEs,
    plan: &mut Plan,
) -> Result<(), SbroadError> {
    let node = ast.nodes.get_node(node_id)?;
    assert_eq!(node.rule, Rule::Cte);
    let mut name = None;
    let mut columns = Vec::with_capacity(node.children.len());
    let mut top_id = None;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Rule::Identifier => {
                name = Some(parse_normalized_identifier(ast, *child_id)?);
            }
            Rule::CteColumn => {
                let column_name = parse_normalized_identifier(ast, *child_id)?;
                columns.push(column_name);
            }
            Rule::SelectWithOptionalContinuation | Rule::Values => {
                let select_id = map.get(*child_id)?;
                top_id = Some(select_id);
            }
            _ => unreachable!("Unexpected node: {child_node:?}"),
        }
    }
    let name = name.expect("CTE must have a name");
    let child_id = top_id.expect("CTE must contain a single child");
    if ctes.get(&name).is_some() {
        return Err(SbroadError::Invalid(
            Entity::Cte,
            Some(format_smolstr!("CTE with name {name} is already defined")),
        ));
    }
    let cte_id = plan.add_cte(child_id, name.clone(), columns)?;
    ctes.insert(name, cte_id);
    map.add(node_id, cte_id);
    Ok(())
}

/// Get String value under node that is considered to be an identifier
/// (on which rules on name normalization should be applied).
fn parse_identifier(ast: &AbstractSyntaxTree, node_id: usize) -> Result<SmolStr, SbroadError> {
    Ok(normalize_name_for_space_api(parse_string_value_node(ast, node_id)?).to_smolstr())
}

fn parse_normalized_identifier(
    ast: &AbstractSyntaxTree,
    node_id: usize,
) -> Result<SmolStr, SbroadError> {
    Ok(normalize_name_from_sql(parse_string_value_node(
        ast, node_id,
    )?))
}

/// Common logic for parsing GRANT/REVOKE queries.
#[allow(clippy::too_many_lines)]
fn parse_grant_revoke(
    node: &ParseNode,
    ast: &AbstractSyntaxTree,
) -> Result<(GrantRevokeType, SmolStr, Decimal), SbroadError> {
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
                        Some(format_smolstr!(
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
                Rule::PrivBlockProcedure => GrantRevokeType::procedure(privilege)?,
                Rule::PrivBlockSpecificProcedure => {
                    let proc_node_id = inner_privilege_block_node.children.first().expect(
                        "Expected to see Name as a first child of PrivBlockSpecificProcedure",
                    );
                    let proc_node = ast.nodes.get_node(*proc_node_id)?;
                    let (proc_name, proc_params) = parse_proc_with_optional_params(ast, proc_node)?;
                    GrantRevokeType::specific_procedure(privilege, proc_name, proc_params)?
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::ParseNode,
                        Some(format_smolstr!(
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
                Some(format_smolstr!(
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

fn parse_trim<M: Metadata>(
    pair: Pair<Rule>,
    referred_relation_ids: &[usize],
    worker: &mut ExpressionsWorker<M>,
    plan: &mut Plan,
) -> Result<ParseExpression, SbroadError> {
    assert_eq!(pair.as_rule(), Rule::Trim);
    let mut kind = None;
    let mut pattern = None;
    let mut target = None;

    let inner_pairs = pair.into_inner();
    for child_pair in &mut inner_pairs.into_iter() {
        match child_pair.as_rule() {
            Rule::TrimKind => {
                let kind_pair = child_pair
                    .into_inner()
                    .next()
                    .expect("Expected child of TrimKind");
                match kind_pair.as_rule() {
                    Rule::TrimKindBoth => kind = Some(TrimKind::Both),
                    Rule::TrimKindLeading => kind = Some(TrimKind::Leading),
                    Rule::TrimKindTrailing => kind = Some(TrimKind::Trailing),
                    _ => {
                        panic!("Unexpected node: {kind_pair:?}");
                    }
                }
            }
            Rule::TrimPattern => {
                let inner_pattern = child_pair.into_inner();
                pattern = Some(Box::new(parse_expr_pratt(
                    inner_pattern,
                    referred_relation_ids,
                    worker,
                    plan,
                )?));
            }
            Rule::TrimTarget => {
                let inner_target = child_pair.into_inner();
                target = Some(Box::new(parse_expr_pratt(
                    inner_target,
                    referred_relation_ids,
                    worker,
                    plan,
                )?));
            }
            _ => {
                panic!("Unexpected node: {child_pair:?}");
            }
        }
    }
    let trim = ParseExpression::Trim {
        kind,
        pattern,
        target: target.expect("Trim target must be specified"),
    };
    Ok(trim)
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
                            Some(format_smolstr!(
                                "option value is not unsigned integer: {str_value}"
                            )),
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
                Some(format_smolstr!(
                    "unexpected child of option. id: {option_node_id}"
                )),
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
        use Rule::{Add, And, Between, ConcatInfixOp, Divide, Eq, Gt, GtEq, In, IsNullPostfix, CastPostfix, Lt, LtEq, Multiply, NotEq, Or, Subtract, UnaryNot};

        // Precedence is defined lowest to highest.
        PrattParser::new()
            .op(Op::infix(Or, Left))
            .op(Op::infix(And, Left))
            .op(Op::prefix(UnaryNot))
            .op(Op::infix(Between, Left))
            .op(
                Op::infix(Eq, Right) | Op::infix(NotEq, Right) | Op::infix(NotEq, Right)
                | Op::infix(Gt, Right) | Op::infix(GtEq, Right) | Op::infix(Lt, Right)
                | Op::infix(LtEq, Right) | Op::infix(In, Right)
            )
            .op(Op::infix(Add, Left) | Op::infix(Subtract, Left))
            .op(Op::infix(Multiply, Left) | Op::infix(Divide, Left) | Op::infix(ConcatInfixOp, Left))
            .op(Op::postfix(IsNullPostfix))
            .op(Op::postfix(CastPostfix))
    };
}

lazy_static::lazy_static! {
    static ref SELECT_PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::{Assoc::Left, Op};
        use Rule::{UnionOp, UnionAllOp, ExceptOp};

        PrattParser::new()
            .op(
                Op::infix(UnionOp, Left)
            | Op::infix(UnionAllOp, Left)
            | Op::infix(ExceptOp, Left)
        )
    };
}

/// Number of relational nodes we expect to retrieve column positions for.
const COLUMN_POSITIONS_CACHE_CAPACITY: usize = 10;
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
    pub reference_to_name_map: HashMap<usize, (SmolStr, bool)>,
    /// Flag indicating whether parameter in Tarantool (? mark) style was met.
    met_tnt_param: bool,
    /// Flag indicating whether parameter in Postgres ($<index>) style was met.
    met_pg_param: bool,
    /// Map of (relational_node_id, columns_position_map).
    /// As `ColumnPositionMap` is used for parsing references and as it may be shared for the same
    /// relational node we cache it so that we don't have to recreate it every time.
    column_positions_cache: HashMap<usize, ColumnPositionMap>,
    /// Time at the start of the plan building stage without timezone.
    /// It is used to replace CURRENT_DATE to actual value.
    current_time: OffsetDateTime,
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
            column_positions_cache: HashMap::with_capacity(COLUMN_POSITIONS_CACHE_CAPACITY),
            current_time: OffsetDateTime::now_utc(),
        }
    }

    fn build_columns_map(&mut self, plan: &Plan, rel_id: usize) -> Result<(), SbroadError> {
        if self.column_positions_cache.get(&rel_id).is_none() {
            let new_map = ColumnPositionMap::new(plan, rel_id)?;
            self.column_positions_cache.insert(rel_id, new_map);
        }
        Ok(())
    }

    fn columns_map_get_positions(
        &self,
        rel_id: usize,
        col_name: &str,
        scan_name: Option<&str>,
    ) -> Result<Position, SbroadError> {
        let col_map = self
            .column_positions_cache
            .get(&rel_id)
            .expect("Columns map should be in the cache already");

        if let Some(scan_name) = scan_name {
            col_map.get_with_scan(col_name, Some(scan_name))
        } else {
            col_map.get(col_name)
        }
    }
}

#[derive(Clone, Debug)]
enum ParseExpressionInfixOperator {
    InfixBool(Bool),
    InfixArithmetic(Arithmetic),
    Concat,
}

#[derive(Clone, Debug)]
enum ParseExpression {
    PlanId {
        plan_id: usize,
    },
    Parentheses {
        child: Box<ParseExpression>,
    },
    Infix {
        is_not: bool,
        op: ParseExpressionInfixOperator,
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
    Function {
        name: String,
        args: Vec<ParseExpression>,
        feature: Option<FunctionFeature>,
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
    Case {
        search_expr: Option<Box<ParseExpression>>,
        when_blocks: Vec<(Box<ParseExpression>, Box<ParseExpression>)>,
        else_expr: Option<Box<ParseExpression>>,
    },
    Trim {
        kind: Option<TrimKind>,
        pattern: Option<Box<ParseExpression>>,
        target: Box<ParseExpression>,
    },
    /// Workaround for the mixfix BETWEEN operator breaking the logic of
    /// pratt parsing for infix operators.
    /// For expression `expr_1 BETWEEN expr_2 AND expr_3` we would create ParseExpression tree of
    /// And
    ///   - left  = InterimBetween
    ///               - left  = expr_1
    ///               - right = expr_2
    ///   - right = expr_3
    /// because priority of BETWEEN is higher than of AND.
    ///
    /// When we face such a tree, we transform it into Expression::Between. So during parsing AND
    /// operator we have to check whether we have to transform it into BETWEEN.
    InterimBetween {
        is_not: bool,
        left: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
    /// Fixed version of `InterimBetween`.
    FinalBetween {
        is_not: bool,
        left: Box<ParseExpression>,
        center: Box<ParseExpression>,
        right: Box<ParseExpression>,
    },
}

#[derive(Clone)]
pub enum SelectOp {
    Union,
    UnionAll,
    Except,
}

#[derive(Clone)]
pub enum SelectExpr {
    PlanId {
        plan_id: usize,
    },
    Infix {
        op: SelectOp,
        left: Box<SelectExpr>,
        right: Box<SelectExpr>,
    },
}

impl SelectExpr {
    fn populate_plan(&self, plan: &mut Plan) -> Result<usize, SbroadError> {
        match self {
            SelectExpr::PlanId { plan_id } => Ok(*plan_id),
            SelectExpr::Infix { op, left, right } => {
                let left_id = left.populate_plan(plan)?;
                let right_id = right.populate_plan(plan)?;
                match op {
                    u @ (SelectOp::Union | SelectOp::UnionAll) => {
                        let remove_duplicates = matches!(u, SelectOp::Union);
                        plan.add_union(left_id, right_id, remove_duplicates)
                    }
                    SelectOp::Except => {
                        let r = plan.add_except(left_id, right_id).unwrap();
                        Ok(r)
                    }
                }
            }
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
                plan.add_cast(child_plan_id, *cast_type)?
            }
            ParseExpression::Case {
                search_expr,
                when_blocks,
                else_expr,
            } => {
                let search_expr_id = if let Some(search_expr) = search_expr {
                    Some(search_expr.populate_plan(plan, worker)?)
                } else {
                    None
                };
                let when_block_ids = when_blocks
                    .iter()
                    .map(|(cond, res)| {
                        Ok((
                            cond.populate_plan(plan, worker)?,
                            (res.populate_plan(plan, worker)?),
                        ))
                    })
                    .collect::<Result<Vec<(usize, usize)>, SbroadError>>()?;
                let else_expr_id = if let Some(else_expr) = else_expr {
                    Some(else_expr.populate_plan(plan, worker)?)
                } else {
                    None
                };
                plan.add_case(search_expr_id, when_block_ids, else_expr_id)
            }
            ParseExpression::Trim {
                kind,
                pattern,
                target,
            } => {
                let pattern = match pattern {
                    Some(p) => Some(p.populate_plan(plan, worker)?),
                    None => None,
                };
                let trim_expr = Expression::Trim {
                    kind: kind.clone(),
                    pattern,
                    target: target.populate_plan(plan, worker)?,
                };
                plan.nodes.push(Node::Expression(trim_expr))
            }
            ParseExpression::FinalBetween {
                is_not,
                left,
                center,
                right,
            } => {
                let plan_left_id = left.populate_plan(plan, worker)?;
                let left_covered_with_row = plan.row(plan_left_id)?;

                let plan_center_id = center.populate_plan(plan, worker)?;
                let center_covered_with_row = plan.row(plan_center_id)?;

                let plan_right_id = right.populate_plan(plan, worker)?;
                let right_covered_with_row = plan.row(plan_right_id)?;

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

                between_id
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

                // In case:
                // * `op` = AND (left = `left_plan_id`, right = `right_plan_id`)
                // * `right_plan_id` = AND (left = expr_1, right = expr_2) (resulted from BETWEEN
                //   transformation)
                //                And
                //  left_plan_id        And
                //                expr_1    expr_2
                //
                // we'll end up in a situation when AND is a right child of another AND (We don't
                // expect it later as soon as AND is a left-associative operator).
                // We have to fix it the following way:
                //                      And
                //               And        expr_2
                //  left_plan_id   expr_1
                let right_plan_is_and = {
                    let right_expr = plan.get_node(right_plan_id)?;
                    matches!(
                        right_expr,
                        Node::Expression(Expression::Bool { op: Bool::And, .. })
                    )
                };
                if matches!(op, ParseExpressionInfixOperator::InfixBool(Bool::And))
                    && right_plan_is_and
                {
                    let right_expr = plan.get_expression_node(right_plan_id)?;
                    let fixed_left_and_id = if let Expression::Bool {
                        op: Bool::And,
                        left,
                        ..
                    } = right_expr
                    {
                        plan.add_cond(left_row_id, Bool::And, *left)?
                    } else {
                        panic!("Expected to see AND operator as right child.");
                    };

                    let right_expr_mut = plan.get_mut_expression_node(right_plan_id)?;
                    if let Expression::Bool {
                        op: Bool::And,
                        left,
                        ..
                    } = right_expr_mut
                    {
                        *left = fixed_left_and_id;
                        return Ok(right_plan_id);
                    }
                }

                let right_row_id = plan.row(right_plan_id)?;

                let op_plan_id = match op {
                    ParseExpressionInfixOperator::Concat => {
                        plan.add_concat(left_row_id, right_row_id)?
                    }
                    ParseExpressionInfixOperator::InfixArithmetic(arith) => {
                        plan.add_arithmetic_to_plan(left_row_id, arith.clone(), right_row_id)?
                    }
                    ParseExpressionInfixOperator::InfixBool(bool) => {
                        plan.add_cond(left_row_id, bool.clone(), right_row_id)?
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
                plan.add_unary(op.clone(), child_covered_with_row)?
            }
            ParseExpression::Function {
                name,
                args,
                feature,
            } => {
                let is_distinct = matches!(feature, Some(FunctionFeature::Distinct));
                let mut plan_arg_ids = Vec::new();
                for arg in args {
                    let arg_plan_id = arg.populate_plan(plan, worker)?;
                    plan_arg_ids.push(arg_plan_id);
                }
                if let Some(kind) = AggregateKind::new(name) {
                    plan.add_aggregate_function(name, kind, plan_arg_ids, is_distinct)?
                } else if is_distinct {
                    return Err(SbroadError::Invalid(
                        Entity::Query,
                        Some("DISTINCT modifier is allowed only for aggregate functions".into()),
                    ));
                } else {
                    let func = worker.metadata.function(name)?;
                    if func.is_stable() {
                        plan.add_stable_function(func, plan_arg_ids, feature.clone())?
                    } else {
                        // At the moment we don't support any non-stable functions.
                        // Later this code block should handle other function behaviors.
                        return Err(SbroadError::Invalid(
                            Entity::SQLFunction,
                            Some(format_smolstr!("function {name} is not stable.")),
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
            ParseExpression::InterimBetween { .. } => return Err(SbroadError::Invalid(
                Entity::Expression,
                Some(SmolStr::from(
                    "BETWEEN operator should have a view of `expr_1 BETWEEN expr_2 AND expr_3`.",
                )),
            )),
        };
        Ok(plan_id)
    }
}

/// Workaround for the following expressions:
///     * `expr_1 AND expr_2 BETWEEN expr_3 AND expr_4`
///     * `NOT expr_1 BETWEEN expr_2 AND expr_3`
/// which are parsed as:
///     * `(expr_1 AND (expr_2 BETWEEN expr_3)) AND expr_4`
///     * `(NOT (expr_1 BETWEEN expr_2)) AND expr_3`
/// but we should transform them into:
///     * `expr_1 AND (expr_2 BETWEEN expr_3 AND expr_4)`
///     * `NOT (expr_1 BETWEEN expr_2 AND expr_3)`
///
/// Returns:
/// * None in case `expr` doesn't have `InterimBetween` as a child
/// * Expression whose right child is an `InterimBetween` (or `InterimBetween` itself)
fn find_interim_between(mut expr: &mut ParseExpression) -> Option<(&mut ParseExpression, bool)> {
    let mut is_exact_match = true;
    loop {
        match expr {
            ParseExpression::Infix {
                op: ParseExpressionInfixOperator::InfixBool(Bool::And),
                right,
                ..
            } => {
                expr = right;
                is_exact_match = false;
            }
            ParseExpression::Prefix {
                op: Unary::Not,
                child,
            } => {
                expr = child;
                is_exact_match = false;
            }
            ParseExpression::InterimBetween { .. } => break Some((expr, is_exact_match)),
            _ => break None,
        }
    }
}

fn cast_type_from_pair(type_pair: Pair<Rule>) -> Result<CastType, SbroadError> {
    if type_pair.as_rule() != Rule::ColumnDefType {
        // TypeAny.
        return CastType::try_from(&type_pair.as_rule());
    }

    let mut column_def_type_pairs = type_pair.into_inner();
    let column_def_type = column_def_type_pairs
        .next()
        .expect("concrete type expected under ColumnDefType");
    if column_def_type.as_rule() != Rule::TypeVarchar {
        return CastType::try_from(&column_def_type.as_rule());
    }

    let mut type_pairs_inner = column_def_type.into_inner();
    let varchar_length = type_pairs_inner
        .next()
        .expect("Length is missing under Varchar");
    let len = varchar_length.as_str().parse::<usize>().map_err(|e| {
        SbroadError::ParsingError(
            Entity::Value,
            format_smolstr!("Failed to parse varchar length: {e:?}."),
        )
    })?;
    Ok(CastType::Varchar(len))
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
    type WhenBlocks = Vec<(Box<ParseExpression>, Box<ParseExpression>)>;

    PRATT_PARSER
        .map_primary(|primary| {
            let parse_expr = match primary.as_rule() {
                Rule::Expr | Rule::Literal => {
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
                                let mut feature = None;
                                let mut parse_exprs_args = Vec::new();
                                let function_args = args_pairs.next();
                                if let Some(function_args) = function_args {
                                    match function_args.as_rule() {
                                        Rule::CountAsterisk => {
                                            let normalized_name = function_name.to_lowercase();
                                            if "count" != normalized_name.as_str() {
                                                return Err(SbroadError::Invalid(
                                                    Entity::Query,
                                                    Some(format_smolstr!(
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

                                            if let Some(first_arg_pair) = args_inner.next() {
                                                if let Rule::Distinct = first_arg_pair.as_rule() {
                                                    feature = Some(FunctionFeature::Distinct);
                                                } else {
                                                    arg_pairs_to_parse.push(first_arg_pair);
                                                }
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
                                    feature,
                                })
                            }
                            rule => unreachable!("Expr::parse expected identifier continuation, found {:?}", rule)
                        }
                    };

                    let plan_left_id = referred_relation_ids
                        .first()
                        .ok_or(SbroadError::Invalid(
                            Entity::Query,
                            Some("Reference must point to some relational node".into())
                        ))?;

                    worker.build_columns_map(plan, *plan_left_id)?;

                    let left_child_col_position = worker.columns_map_get_positions(*plan_left_id, &col_name, scan_name.as_deref());

                    let plan_right_id = referred_relation_ids.get(1);
                    let (ref_id, is_row) = if let Some(plan_right_id) = plan_right_id {
                        // Referencing Join node.
                        worker.build_columns_map(plan, *plan_right_id)?;
                        let right_child_col_position = worker.columns_map_get_positions(*plan_right_id, &col_name, scan_name.as_deref());

                        let present_in_left = left_child_col_position.is_ok();
                        let present_in_right = right_child_col_position.is_ok();

                        let ref_id = if present_in_left && present_in_right {
                            return Err(SbroadError::Invalid(
                                Entity::Column,
                                Some(format_smolstr!(
                                    "column name '{col_name}' is present in both join children",
                                )),
                            ));
                        } else if present_in_left {
                            let col_with_scan = ColumnWithScan::new(&col_name, scan_name.as_deref());
                            plan.add_row_from_left_branch(
                                *plan_left_id,
                                *plan_right_id,
                                &[col_with_scan],
                            )?
                        } else if present_in_right {
                            let col_with_scan = ColumnWithScan::new(&col_name, scan_name.as_deref());
                            plan.add_row_from_right_branch(
                                *plan_left_id,
                                *plan_right_id,
                                &[col_with_scan],
                            )?
                        } else {
                            return Err(SbroadError::NotFound(
                                Entity::Column,
                                format_smolstr!("'{col_name}' in the join children",),
                            ));
                        };
                        (ref_id, true)
                    } else {
                        // Referencing single node.
                        let col_position = match left_child_col_position {
                            Ok(col_position) => col_position,
                            Err(e) => return Err(e)
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
                Rule::Trim => parse_trim(primary, referred_relation_ids, worker, plan)?,
                Rule::CastOp => {
                    let mut inner_pairs = primary.into_inner();
                    let expr_pair = inner_pairs.next().expect("Cast has no expr child.");
                    let child_parse_expr = parse_expr_pratt(
                        expr_pair.into_inner(),
                        referred_relation_ids,
                        worker,
                        plan
                    )?;
                    let type_pair = inner_pairs.next().expect("CastOp has no type child");
                    let cast_type = cast_type_from_pair(type_pair)?;

                    ParseExpression::Cast { cast_type, child: Box::new(child_parse_expr) }
                }
                Rule::Case => {
                    let mut inner_pairs = primary.into_inner();

                    let first_pair = inner_pairs.next().expect("Case must have at least one child");
                    let mut when_block_pairs = Vec::new();
                    let search_expr = if let Rule::Expr = first_pair.as_rule() {
                        let expr = parse_expr_pratt(
                            first_pair.into_inner(),
                            referred_relation_ids,
                            worker,
                            plan,
                        )?;
                        Some(Box::new(expr))
                    } else {
                        when_block_pairs.push(first_pair);
                        None
                    };

                    let mut else_expr = None;
                    for pair in inner_pairs {
                        if Rule::CaseElseBlock == pair.as_rule() {
                            let expr = parse_expr_pratt(
                                pair.into_inner(),
                                referred_relation_ids,
                                worker,
                                plan,
                            )?;
                            else_expr = Some(Box::new(expr));
                        } else {
                            when_block_pairs.push(pair);
                        }
                    }

                    let when_blocks: Result<WhenBlocks, SbroadError> = when_block_pairs
                        .into_iter()
                        .map(|when_block_pair| {
                            let mut inner_pairs = when_block_pair.into_inner();
                            let condition_expr_pair = inner_pairs.next().expect("When block must contain condition expression.");
                            let condition_expr = parse_expr_pratt(
                                condition_expr_pair.into_inner(),
                                referred_relation_ids,
                                worker,
                                plan,
                            )?;

                            let result_expr_pair = inner_pairs.next().expect("When block must contain result expression.");
                            let result_expr = parse_expr_pratt(
                                result_expr_pair.into_inner(),
                                referred_relation_ids,
                                worker,
                                plan,
                            )?;

                            Ok::<(Box<ParseExpression>, Box<ParseExpression>), SbroadError>((
                                Box::new(condition_expr),
                                Box::new(result_expr)
                            ))

                        })
                        .collect();

                    ParseExpression::Case {
                        search_expr,
                        when_blocks: when_blocks?,
                        else_expr,
                    }
                }
                Rule::CurrentDate => {
                    let date = worker.current_time.replace_time(Time::MIDNIGHT);
                    let val = Value::Datetime(Datetime::from_inner(date));
                    let plan_id = plan.add_const(val);
                    ParseExpression::PlanId { plan_id }
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
            let mut lhs = lhs?;
            let rhs = rhs?;
            let mut is_not = false;
            let op = match op.as_rule() {
                Rule::And => ParseExpressionInfixOperator::InfixBool(Bool::And),
                Rule::Or => ParseExpressionInfixOperator::InfixBool(Bool::Or),
                Rule::Between => {
                    let mut op_inner = op.into_inner();
                    is_not = op_inner.next().is_some();
                    return Ok(ParseExpression::InterimBetween {
                        is_not,
                        left: Box::new(lhs),
                        right: Box::new(rhs),
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

            // HACK: InterimBetween(e1, e2) AND e3 => FinalBetween(e1, e2, e3).
            if matches!(op, ParseExpressionInfixOperator::InfixBool(Bool::And)) {
                if let Some((expr, is_exact_match)) = find_interim_between(&mut lhs) {
                    let ParseExpression::InterimBetween { is_not, left, right } = expr else {
                        panic!("expected ParseExpression::InterimBetween");
                    };

                    let fb = ParseExpression::FinalBetween {
                        is_not: *is_not,
                        left: left.clone(),
                        center: right.clone(),
                        right: Box::new(rhs),
                    };

                    if is_exact_match {
                        return Ok(fb);
                    }
                    *expr = fb;
                    return Ok(lhs);
                }
            }

            Ok(ParseExpression::Infix {
                op,
                is_not,
                left: Box::new(lhs),
                right: Box::new(rhs),
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
            let child = child?;
            match op.as_rule() {
                Rule::CastPostfix => {
                    let ty_pair = op.into_inner().next()
                        .expect("Expected ColumnDefType under CastPostfix.");
                    let cast_type = cast_type_from_pair(ty_pair)?;
                    Ok(ParseExpression::Cast { child: Box::new(child), cast_type })
                }
                Rule::IsNullPostfix => {
                    let is_not = match op.into_inner().len() {
                        1 => true,
                        0 => false,
                        _ => unreachable!("IsNull must have 0 or 1 children")
                    };
                    Ok(ParseExpression::IsNull { is_not, child: Box::new(child)})
                },
                rule => unreachable!("Expr::parse expected postfix operator, found {:?}", rule),
            }
        })
        .parse(expression_pairs)
}

// Mapping between pest's Pair and corresponding id
// of the ast node. This map stores only ids for
// possible select child (Projection, OrderBy)
pub(crate) type SelectChildPairTranslation = HashMap<(usize, usize), usize>;

fn parse_select_pratt(
    select_pairs: Pairs<Rule>,
    pos_to_plan_id: &SelectChildPairTranslation,
    ast_to_plan: &Translation,
) -> Result<SelectExpr, SbroadError> {
    SELECT_PRATT_PARSER
        .map_primary(|primary| {
            let select_expr = match primary.as_rule() {
                Rule::Select => {
                    let mut pairs = primary.into_inner();
                    let mut select_child_pair =
                        pairs.next().expect("select must have at least one child");
                    assert!(matches!(select_child_pair.as_rule(), Rule::Projection));
                    for pair in pairs {
                        if let Rule::OrderBy = pair.as_rule() {
                            select_child_pair = pair;
                            break;
                        }
                    }
                    let ast_id = pos_to_plan_id.get(&select_child_pair.line_col()).unwrap();
                    let id = ast_to_plan.get(*ast_id)?;
                    SelectExpr::PlanId { plan_id: id }
                }
                Rule::SelectWithOptionalContinuation => {
                    parse_select_pratt(primary.into_inner(), pos_to_plan_id, ast_to_plan)?
                }
                rule => unreachable!("Select::parse expected atomic rule, found {:?}", rule),
            };
            Ok(select_expr)
        })
        .map_infix(|lhs, op, rhs| {
            let op = match op.as_rule() {
                Rule::UnionOp => SelectOp::Union,
                Rule::UnionAllOp => SelectOp::UnionAll,
                Rule::ExceptOp => SelectOp::Except,
                rule => unreachable!("Expr::parse expected infix operation, found {:?}", rule),
            };
            Ok(SelectExpr::Infix {
                op,
                left: Box::new(lhs?),
                right: Box::new(rhs?),
            })
        })
        .parse(select_pairs)
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

fn parse_select(
    select_pairs: Pairs<Rule>,
    pos_to_ast_id: &SelectChildPairTranslation,
    ast_to_plan: &Translation,
    plan: &mut Plan,
) -> Result<usize, SbroadError> {
    let select_expr = parse_select_pratt(select_pairs, pos_to_ast_id, ast_to_plan)?;
    select_expr.populate_plan(plan)
}

/// Generate an alias for the unnamed projection expressions.
#[must_use]
pub fn get_unnamed_column_alias(pos: usize) -> SmolStr {
    format_smolstr!("COL_{pos}")
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
        pos_to_ast_id: &mut SelectChildPairTranslation,
    ) -> Result<(), SbroadError> {
        let mut command_pair = match ParseTree::parse(Rule::Command, query) {
            Ok(p) => p,
            Err(e) => {
                return Err(SbroadError::ParsingError(
                    Entity::Rule,
                    format_smolstr!("{e}"),
                ))
            }
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
                Some(SmolStr::from(stack_node.pair.as_str())),
            ));

            // Save procedure body (a special case).
            if stack_node.pair.as_rule() == Rule::ProcBody {
                let span = stack_node.pair.as_span();
                let body = &query[span.start()..span.end()];
                self.nodes
                    .update_value(arena_node_id, Some(SmolStr::from(body)))?;
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
                Rule::Expr | Rule::Row | Rule::Literal | Rule::SelectWithOptionalContinuation => {
                    // * `Expr`s are parsed using Pratt parser with a separate `parse_expr`
                    //   function call on the stage of `resolve_metadata`.
                    // * `Row`s are added to support parsing Row expressions under `Values` nodes.
                    // * `Literal`s are added to support procedure calls which should not contain
                    //   all possible `Expr`s.
                    pairs_map.insert(arena_node_id, stack_node.pair.clone());
                }
                Rule::Projection | Rule::OrderBy => {
                    pos_to_ast_id.insert(stack_node.pair.line_col(), arena_node_id);
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

    fn parse_order_by<M: Metadata>(
        &self,
        plan: &mut Plan,
        node_id: usize,
        map: &mut Translation,
        pairs_map: &mut ParsingPairsMap,
        worker: &mut ExpressionsWorker<M>,
    ) -> Result<(), SbroadError> {
        let node = self.nodes.get_node(node_id)?;
        let projection_ast_id = node.children.first().expect("OrderBy has no children.");
        let projection_plan_id = map.get(*projection_ast_id)?;
        let sq_plan_id = plan.add_sub_query(projection_plan_id, None)?;

        let sq_output_id = plan.get_relational_output(sq_plan_id)?;
        let sq_output_len = plan.get_row_list(sq_output_id)?.len();

        let mut order_by_elements: Vec<OrderByElement> = Vec::with_capacity(node.children.len());
        for node_child_index in node.children.iter().skip(1) {
            let order_by_element_node = self.nodes.get_node(*node_child_index)?;
            let order_by_element_expr_id = order_by_element_node
                .children
                .first()
                .expect("OrderByElement must have at least one child");
            let expr_pair = pairs_map.remove_pair(*order_by_element_expr_id);
            let expr_plan_node_id = parse_expr(
                Pairs::single(expr_pair),
                &[projection_plan_id],
                worker,
                plan,
            )?;

            // In case index is specified as ordering element, we have to check that
            // the index (starting from 1) is not bigger than the number of columns in
            // the projection output.
            let expr = plan.get_node(expr_plan_node_id)?;

            let entity = match expr {
                Node::Expression(expr) => {
                    if let Expression::Constant {value: Value::Unsigned(index)} = expr {
                        let index_usize = usize::try_from(*index).map_err(|_| {
                            SbroadError::Invalid(
                                Entity::Expression,
                                Some(format_smolstr!(
                            "Unable to convert OrderBy index ({index}) to usize"
                        )),
                            )
                        })?;
                        if index_usize > sq_output_len {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(format_smolstr!("Ordering index ({index}) is bigger than child projection output length ({sq_output_len})."))
                            ));
                        }
                        OrderByEntity::Index { value: index_usize }
                    } else {
                        // Check that at least one reference is met in expression tree.
                        // Otherwise, ordering expression has no sense.
                        let mut expr_tree =
                            PostOrder::with_capacity(|node| plan.nodes.expr_iter(node, false), EXPR_CAPACITY);
                        let mut reference_met = false;
                        for (_, node_id) in expr_tree.iter(expr_plan_node_id) {
                            if let Expression::Reference { .. } = plan.get_expression_node(node_id)? {
                                reference_met = true;
                                break;
                            }
                        }

                        if !reference_met {
                            return Err(SbroadError::Invalid(
                                Entity::Expression,
                                Some(SmolStr::from("ORDER BY element that is not position and doesn't contain reference doesn't influence ordering."))
                            ))
                        }

                        OrderByEntity::Expression {
                            expr_id: expr_plan_node_id,
                        }
                    }
                }
                Node::Parameter(..) => return Err(SbroadError::Invalid(
                    Entity::Expression,
                    Some(SmolStr::from("Using parameter as a standalone ORDER BY expression doesn't influence sorting."))
                )),
                _ => unreachable!("Unacceptable node as an ORDER BY element.")
            };

            let order_type =
                if let Some(order_type_child_node_id) = order_by_element_node.children.get(1) {
                    let order_type_node = self.nodes.get_node(*order_type_child_node_id)?;
                    let order_type = match order_type_node.rule {
                        Rule::Asc => OrderByType::Asc,
                        Rule::Desc => OrderByType::Desc,
                        rule => unreachable!(
                            "{}",
                            format!("Unexpected rule met under OrderByElement: {rule:?}")
                        ),
                    };
                    Some(order_type)
                } else {
                    None
                };

            order_by_elements.push(OrderByElement { entity, order_type });
        }
        let plan_node_id = plan.add_order_by(sq_plan_id, order_by_elements)?;
        map.add(node_id, plan_node_id);
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
        pos_to_ast_id: &mut SelectChildPairTranslation,
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
        let mut ctes = CTEs::new();

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
                        // CTE scans can have different aliases, so clone the CTE scan node,
                        // preserving its subtree.
                        if let Relational::ScanCte { child, .. } = rel_child_node {
                            let scan_id = plan.add_cte(*child, alias_name, vec![])?;
                            map.add(id, scan_id);
                        } else {
                            let scan = plan.get_mut_relation_node(rel_child_id_plan)?;
                            scan.set_scan_name(Some(alias_name.to_smolstr()))?;
                        }
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
                Rule::ScanCteOrTable => {
                    parse_scan_cte_or_table(self, metadata, id, &mut map, &mut ctes, &mut plan)?;
                }
                Rule::Cte => {
                    parse_cte(self, id, &mut map, &mut ctes, &mut plan)?;
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
                                Some(format_smolstr!(
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
                Rule::OrderBy => {
                    self.parse_order_by(&mut plan, id, &mut map, pairs_map, &mut worker)?;
                }
                Rule::SelectWithOptionalContinuation => {
                    let select_pair = pairs_map.remove_pair(id);
                    let select_plan_node_id =
                        parse_select(Pairs::single(select_pair), pos_to_ast_id, &map, &mut plan)?;
                    map.add(id, select_plan_node_id);
                }
                Rule::SelectFull => {
                    parse_select_full(self, id, &mut map)?;
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
                                let table_name_id = ast_column.children.first();
                                let plan_asterisk_id = if let Some(table_name_id) = table_name_id {
                                    let table_name =
                                        parse_normalized_identifier(self, *table_name_id)?;

                                    let col_name_pos_map =
                                        ColumnPositionMap::new(&plan, plan_rel_child_id)?;
                                    let filtered_col_ids =
                                        col_name_pos_map.get_by_scan_name(&table_name)?;
                                    plan.add_row_by_indices(
                                        plan_rel_child_id,
                                        filtered_col_ids,
                                        false,
                                    )?
                                } else {
                                    plan.add_row_for_output(plan_rel_child_id, &[], false)?
                                };

                                let row_list = plan.get_row_list(plan_asterisk_id)?;
                                for row_id in row_list {
                                    proj_columns.push(*row_id);
                                }
                            }
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Type,
                                    Some(format_smolstr!(
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
                                format_smolstr!("{scan_relation} among plan relations"),
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
                                        Some(format_smolstr!(
                                            "it is illegal to update primary key column: {}",
                                            col_name
                                        )),
                                    ));
                                }
                                if update_defs.contains_key(pos) {
                                    return Err(SbroadError::Invalid(
                                        Entity::Query,
                                        Some(format_smolstr!(
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
                                    format_smolstr!("system column {col_name} cannot be updated"),
                                ))
                            }
                            None => {
                                return Err(SbroadError::NotFound(
                                    Entity::Column,
                                    (*col_name).to_smolstr(),
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
                        .expect("Delete must have at least one child");
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
                                Some(format_smolstr!(
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
                                Some(format_smolstr!(
                                    "{} {} {pos}",
                                    "primary key refers to non-existing column",
                                    "at position",
                                )),
                            )
                        })?;
                        let col_with_scan = ColumnWithScan::new(column.name.as_str(), None);
                        pk_columns.push(col_with_scan);
                    }
                    let pk_column_ids = plan.new_columns(
                        &NewColumnsSource::Other {
                            child: proj_child_id,
                            columns_spec: Some(ColumnsRetrievalSpec::Names(pk_columns)),
                        },
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
                                        Some(format_smolstr!(
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
                        let mut selected_col_names: Vec<SmolStr> =
                            Vec::with_capacity(ast_child.children.len());
                        for col_id in &ast_child.children {
                            selected_col_names
                                .push(parse_normalized_identifier(self, *col_id)?.to_smolstr());
                        }

                        let rel = plan.relations.get(&relation).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Table,
                                format_smolstr!("{relation} among plan relations"),
                            )
                        })?;
                        for column in &rel.columns {
                            if let ColumnRole::Sharding = column.get_role() {
                                continue;
                            }
                            if !column.is_nullable && !selected_col_names.contains(&column.name) {
                                return Err(SbroadError::Invalid(
                                    Entity::Column,
                                    Some(format_smolstr!(
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
                Rule::CreateIndex => {
                    let create_index = parse_create_index(self, node)?;
                    let plan_id = plan.nodes.push(Node::Ddl(create_index));
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
                Rule::DropIndex => {
                    let drop_index = parse_drop_index(self, node)?;
                    let plan_id = plan.nodes.push(Node::Ddl(drop_index));
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
                    let mut table_name = SmolStr::default();
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
                                    Some(format_smolstr!(
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
                            let password = parse_string_literal(self, *pwd_node_id)?;

                            let mut auth_method = get_default_auth_method();
                            if let Some(auth_method_node_id) = alter_option_node.children.get(1) {
                                let auth_method_node = self.nodes.get_node(*auth_method_node_id)?;
                                let auth_method_string_node_id = auth_method_node
                                    .children
                                    .first()
                                    .expect("Method expected under AuthMethod node");
                                auth_method = SmolStr::from(parse_string_value_node(
                                    self,
                                    *auth_method_string_node_id,
                                )?);
                            }

                            AlterOption::Password {
                                password,
                                auth_method,
                            }
                        }
                        Rule::AlterRename => {
                            let identifier_node_id = alter_option_node
                                .children
                                .first()
                                .expect("Expected to see an identifier node under AlterRename");
                            let identifier = parse_identifier(self, *identifier_node_id)?;
                            AlterOption::Rename {
                                new_name: identifier,
                            }
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::ParseNode,
                                Some(SmolStr::from("Expected to see concrete alter option")),
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
                            Some(SmolStr::from("RoleName expected as a first child")),
                        )
                    })?;
                    let user_name = parse_identifier(self, *user_name_node_id)?;

                    let pwd_node_id = iter.next().ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::ParseNode,
                            Some(SmolStr::from("Password expected as a second child")),
                        )
                    })?;
                    let password = parse_string_literal(self, *pwd_node_id)?;

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
                                auth_method = SmolStr::from(parse_string_value_node(
                                    self,
                                    *auth_method_node_id,
                                )?);
                            }
                            _ => {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some(format_smolstr!(
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
                Rule::SetParam => {
                    let set_param_node = parse_set_param(self, node)?;
                    let plan_id = plan.nodes.push(Node::Ddl(set_param_node));
                    map.add(id, plan_id);
                }
                Rule::SetTransaction => {
                    let set_transaction_node = Ddl::SetTransaction {
                        timeout: get_default_timeout(),
                    };
                    let plan_id = plan.nodes.push(Node::Ddl(set_transaction_node));
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

        // check that all tables from query from one tier. Ignore global tables.
        if !plan
            .relations
            .tables
            .iter()
            .filter(|(_, table)| matches!(table.kind, TableKind::ShardedSpace { .. }))
            .map(|(_, table)| table.tier.as_ref())
            .all_equal()
        {
            return Err(SbroadError::Invalid(
                Entity::Query,
                Some("Query cannot use tables from different tiers".into()),
            ));
        }

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
        let mut pos_to_ast_id: SelectChildPairTranslation = HashMap::new();
        let mut ast = AbstractSyntaxTree::empty();
        ast.fill(query, &mut ast_id_to_pairs_map, &mut pos_to_ast_id)?;
        ast.resolve_metadata(metadata, &mut ast_id_to_pairs_map, &mut pos_to_ast_id)
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
