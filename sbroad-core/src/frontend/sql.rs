//! SQL frontend module.
//!
//! Parses an SQL statement to the abstract syntax tree (AST)
//! and builds the intermediate representation (IR).

use pest::Parser;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use crate::errors::{Entity, SbroadError};
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
use crate::ir::expression::Expression;
use crate::ir::operator::{Arithmetic, Bool, ConflictStrategy, JoinKind, Unary};
use crate::ir::relation::Type as RelationType;
use crate::ir::tree::traversal::PostOrder;
use crate::ir::value::Value;
use crate::ir::{Node, Plan};
use crate::otm::child_span;

use crate::ir::aggregates::AggregateKind;
use sbroad_proc::otm_child_span;
use tarantool::decimal::Decimal;

// DDL timeout in seconds (1 day).
const DEFAULT_TIMEOUT: f64 = 24.0 * 60.0 * 60.0;

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

fn get_timeout(ast: &AbstractSyntaxTree, node_id: usize) -> Result<Decimal, SbroadError> {
    let option_node = ast.nodes.get_node(node_id)?;
    if option_node.rule != Type::OptionParam {
        return Err(SbroadError::Invalid(
            Entity::Node,
            Some(format!(
                "AST table option parameter node {:?} contains unexpected children",
                option_node,
            )),
        ));
    }
    let mut timeout: Decimal = Decimal::from_str(&format!("{DEFAULT_TIMEOUT}"))
        .map_err(|_| SbroadError::Invalid(Entity::Type, Some("timeout option".into())))?;
    if let Some(param_id) = option_node.children.first() {
        let param_node = ast.nodes.get_node(*param_id)?;
        if param_node.rule != Type::Timeout {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!(
                    "AST table option node {:?} contains unexpected children",
                    param_node,
                )),
            ));
        }
        if let (Some(duration_id), None) = (param_node.children.first(), param_node.children.get(1))
        {
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
                timeout = Decimal::from_str(duration_value).map_err(|_| {
                    SbroadError::Invalid(
                        Entity::Node,
                        Some(format!(
                            "AST table duration node {:?} contains invalid value",
                            duration_node,
                        )),
                    )
                })?;
            }
        }
    }
    Ok(timeout)
}

#[allow(clippy::too_many_lines)]
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
    let mut shard_keys: Vec<String> = Vec::new();
    let mut timeout: Decimal = Decimal::from_str(&format!("{DEFAULT_TIMEOUT}")).map_err(|_| {
        SbroadError::Invalid(Entity::Type, Some("timeout value in create table".into()))
    })?;
    for child_id in &node.children {
        let child_node = ast.nodes.get_node(*child_id)?;
        match child_node.rule {
            Type::NewTable => {
                table_name =
                    normalize_name_for_space_api(child_node.value.as_ref().ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "table name in the create table AST".into(),
                        )
                    })?);
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
                                    def_child_node.value.as_ref().ok_or_else(|| {
                                        SbroadError::Invalid(
                                            Entity::Column,
                                            Some(
                                                "column name is empty in the create table AST"
                                                    .into(),
                                            ),
                                        )
                                    })?,
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
                                let is_null_node = ast.nodes.get_node(*def_child_id)?;
                                if let (Some(null_id), None) =
                                    (is_null_node.children.first(), is_null_node.children.get(1))
                                {
                                    let null_node = ast.nodes.get_node(*null_id)?;
                                    match null_node.rule {
                                        Type::ColumnIsNull => {
                                            column_def.is_nullable = true;
                                        }
                                        Type::ColumnIsNotNull => {
                                            column_def.is_nullable = false;
                                        }
                                        _ => {
                                            return Err(SbroadError::Invalid(
                                                Entity::Node,
                                                Some(format!(
                                                    "AST column null node {:?} has unexpected type",
                                                    null_node,
                                                )),
                                            ));
                                        }
                                    }
                                } else {
                                    return Err(SbroadError::Invalid(
                                        Entity::Node,
                                        Some(format!(
                                            "AST column null node {:?} contains unexpected children",
                                            def_child_node,
                                        )),
                                    ));
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
                    let pk_col_name = normalize_name_for_space_api(
                        pk_col_node.value.as_ref().ok_or_else(|| {
                            SbroadError::Invalid(
                                Entity::Column,
                                Some(
                                    "primary key column name is empty in the create table AST"
                                        .into(),
                                ),
                            )
                        })?,
                    );
                    pk_keys.push(pk_col_name);
                }
            }
            Type::Engine => {
                if let (Some(engine_type_id), None) =
                    (child_node.children.first(), child_node.children.get(1))
                {
                    let engine_type_node = ast.nodes.get_node(*engine_type_id)?;
                    match engine_type_node.rule {
                        Type::Memtx => {}
                        Type::Vinyl => {
                            // TODO: improve picodata's DDL API to support vinyl engine.
                            return Err(SbroadError::NotImplemented(
                                Entity::Engine,
                                "vinyl".into(),
                            ));
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
                        Type::Global => {
                            // TODO: support global spaces via SQL API.
                            return Err(SbroadError::NotImplemented(
                                Entity::Node,
                                "global spaces are not supported yet".into(),
                            ));
                        }
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
                                    shard_col_node.value.as_ref().ok_or_else(|| {
                                        SbroadError::Invalid(
                                            Entity::Column,
                                            Some("AST shard key column name is empty".into()),
                                        )
                                    })?,
                                );
                                shard_keys.push(shard_col_name);
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
            Type::OptionParam => {
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
    let create_sharded_table = Ddl::CreateShardedTable {
        name: table_name,
        format: columns,
        primary_key: pk_keys,
        sharding_key: shard_keys,
        timeout,
    };
    Ok(create_sharded_table)
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
                let arithmetic_id = arithmetic_parse_node.children.first().ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(
                        "ArithParentheses has no children.".into(),
                    )
                })?;
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
            let ast_left_id = current_node.children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(
                    "Multiplication or Addition has no children.".into(),
                )
            })?;
            let plan_left_id = get_arithmetic_plan_id(
                plan,
                map,
                arith_expr_with_parentheses_ids,
                rows,
                *ast_left_id,
            )?;

            let ast_right_id = current_node.children.get(2).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::Node,
                    "that is right node with index 2 among Multiplication or Addition children"
                        .into(),
                )
            })?;
            let plan_right_id = get_arithmetic_plan_id(
                plan,
                map,
                arith_expr_with_parentheses_ids,
                rows,
                *ast_right_id,
            )?;

            let ast_op_id = current_node.children.get(1).ok_or_else(|| {
                    SbroadError::NotFound(
                        Entity::Node,
                        "that is center node (operator) with index 1 among Multiplication or Addition children"
                            .into(),
                    )
                })?;

            let op_node = self.nodes.get_node(*ast_op_id)?;
            let op = Arithmetic::from_node_type(&op_node.rule)?;
            let op_id = plan.add_arithmetic_to_plan(plan_left_id, op, plan_right_id, false)?;
            Ok::<usize, SbroadError>(op_id)
        };

        let default_timeout: Decimal = Decimal::from_str(&format!("{DEFAULT_TIMEOUT}"))
            .map_err(|_| SbroadError::Invalid(Entity::Type, Some("timeout option".into())))?;

        for (_, id) in dft_post.iter(top) {
            let node = self.nodes.get_node(id)?;
            match &node.rule {
                Type::Scan => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "could not find child id in scan node".to_string(),
                        )
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(id, plan_child_id);
                    if let Some(ast_scan_id) = node.children.get(1) {
                        let ast_scan = self.nodes.get_node(*ast_scan_id)?;
                        if let Type::ScanName = ast_scan.rule {
                            let ast_scan_name =
                                ast_scan.value.as_deref().map(normalize_name_from_sql);
                            // Update scan name in the plan.
                            let scan = plan.get_mut_relation_node(plan_child_id)?;
                            scan.set_scan_name(ast_scan_name)?;
                        } else {
                            return Err(SbroadError::Invalid(
                                Entity::Type,
                                Some("expected scan name AST node.".into()),
                            ));
                        }
                    }
                }
                Type::Table => {
                    if let Some(node_val) = &node.value {
                        let table = node_val.as_str();
                        let t = metadata.table(table)?;
                        plan.add_rel(t);
                        let scan_id = plan.add_scan(&normalize_name_from_sql(table), None)?;
                        map.add(id, scan_id);
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Type,
                            Some("Table name is not found.".into()),
                        ));
                    }
                }
                Type::SubQuery => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "child node id is not found among sub-query children.".into(),
                        )
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    let plan_sq_id = plan.add_sub_query(plan_child_id, None)?;
                    map.add(id, plan_sq_id);
                }
                Type::Reference => {
                    let ast_rel_list = self.get_referred_relational_nodes(id)?;
                    let mut plan_rel_list = Vec::new();
                    for ast_id in ast_rel_list {
                        let plan_id = map.get(ast_id)?;
                        plan_rel_list.push(plan_id);
                    }

                    // Closure to get uppercase name from AST `ColumnName` node.
                    let get_column_name = |ast_id: usize| -> Result<String, SbroadError> {
                        let ast_col_name = self.nodes.get_node(ast_id)?;
                        if let Type::ColumnName = ast_col_name.rule {
                            let name: Option<String> =
                                ast_col_name.value.as_deref().map(normalize_name_from_sql);
                            Ok(name.ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::Name,
                                    Some("empty AST column name".into()),
                                )
                            })?)
                        } else {
                            Err(SbroadError::Invalid(
                                Entity::Type,
                                Some("expected column name AST node.".into()),
                            ))
                        }
                    };

                    // Closure to get the nearest name of relation the output column came from
                    // E.g. for `Scan` it would be it's `realation`.
                    let get_scan_name =
                        |col_name: &str, plan_id: usize| -> Result<Option<String>, SbroadError> {
                            let child = plan.get_relation_node(plan_id)?;
                            let col_position = child
                                .output_alias_position_map(&plan.nodes)?
                                .get(col_name)
                                .copied();
                            match col_position {
                                Some(pos) => Ok(plan
                                    .get_relation_node(plan_id)?
                                    .scan_name(&plan, pos)?
                                    .map(String::from)),
                                None => Ok(None),
                            }
                        };

                    let plan_left_id = plan_rel_list.first();
                    let plan_right_id = plan_rel_list.get(1);
                    if let (Some(plan_left_id), Some(plan_right_id)) = (plan_left_id, plan_right_id)
                    {
                        // Handling case of referencing join node.

                        if let (Some(ast_scan_id), Some(ast_col_name_id)) =
                            (node.children.first(), node.children.get(1))
                        {
                            let ast_scan = self.nodes.get_node(*ast_scan_id)?;
                            if let Type::ScanName = ast_scan.rule {
                                // Get the column name and its positions in the output tuples.
                                let col_name = get_column_name(*ast_col_name_id)?;
                                let left_name = get_scan_name(&col_name, *plan_left_id)?;
                                let right_name = get_scan_name(&col_name, *plan_right_id)?;
                                // Check that the AST scan name matches to the children scan names in the plan join node.
                                let ast_scan_name: Option<String> =
                                    ast_scan.value.as_deref().map(normalize_name_from_sql);
                                // Determine the referred side of the join (left or right).
                                if left_name == ast_scan_name {
                                    let left_col_map = plan
                                        .get_relation_node(*plan_left_id)?
                                        .output_alias_position_map(&plan.nodes)?;
                                    if left_col_map.get(&col_name.as_str()).is_some() {
                                        let ref_id = plan.add_row_from_left_branch(
                                            *plan_left_id,
                                            *plan_right_id,
                                            &[&col_name],
                                        )?;
                                        rows.insert(ref_id);
                                        map.add(id, ref_id);
                                    } else {
                                        return Err(SbroadError::NotFound(
                                            Entity::Column,
                                            format!(
                                                "'{col_name}' in the join left child '{left_name:?}'"
                                            ),
                                        ));
                                    }
                                } else if right_name == ast_scan_name {
                                    let right_col_map = plan
                                        .get_relation_node(*plan_right_id)?
                                        .output_alias_position_map(&plan.nodes)?;
                                    if right_col_map.get(&col_name.as_str()).is_some() {
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
                                            format!(
                                                "'{col_name}' in the join right child '{right_name:?}'"
                                            ),
                                        ));
                                    }
                                } else {
                                    return Err(SbroadError::Invalid(
                                        Entity::Plan,
                                        Some(format!(
                                            "left {:?} and right {:?} plan nodes do not match the AST scan name: {:?}",
                                            left_name,
                                            right_name,
                                            ast_scan_name,
                                            )),
                                    ));
                                }
                            } else {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some("expected AST node to be a scan name.".into()),
                                ));
                            }
                        } else if let (Some(ast_col_name_id), None) =
                            (node.children.first(), node.children.get(1))
                        {
                            // Determine the referred side of the join (left or right).
                            let col_name = get_column_name(*ast_col_name_id)?;
                            let left_col_map = plan
                                .get_relation_node(*plan_left_id)?
                                .output_alias_position_map(&plan.nodes)?;
                            if left_col_map.get(&col_name.as_str()).is_some() {
                                let ref_id = plan.add_row_from_left_branch(
                                    *plan_left_id,
                                    *plan_right_id,
                                    &[&col_name],
                                )?;
                                rows.insert(ref_id);
                                map.add(id, ref_id);
                            }
                            let right_col_map = plan
                                .get_relation_node(*plan_right_id)?
                                .output_alias_position_map(&plan.nodes)?;
                            if right_col_map.get(&col_name.as_str()).is_some() {
                                let ref_id = plan.add_row_from_right_branch(
                                    *plan_left_id,
                                    *plan_right_id,
                                    &[&col_name],
                                )?;
                                rows.insert(ref_id);
                                map.add(id, ref_id);
                            }
                            return Err(SbroadError::NotFound(
                                Entity::Column,
                                format!("'{col_name}' for the join left or right children"),
                            ));
                        } else {
                            return Err(SbroadError::UnexpectedNumberOfValues(
                                "expected children nodes contain a column name.".into(),
                            ));
                        };
                    } else if let (Some(plan_rel_id), None) = (plan_left_id, plan_right_id) {
                        // Handling case of referencing a single child node.

                        let first_child_id = node.children.first();
                        let second_child_id = node.children.get(1);
                        let col_name: String = if let (Some(ast_scan_id), Some(ast_col_id)) =
                            (first_child_id, second_child_id)
                        {
                            // Get column name.
                            let col_name = get_column_name(*ast_col_id)?;
                            // Check that scan name in the reference matches to the one in scan node.
                            let ast_scan = self.nodes.get_node(*ast_scan_id)?;
                            if let Type::ScanName = ast_scan.rule {
                                let ast_scan_name = Some(normalize_name_from_sql(
                                    ast_scan.value.as_ref().ok_or_else(|| {
                                        SbroadError::UnexpectedNumberOfValues(
                                            "empty scan name for AST node.".into(),
                                        )
                                    })?,
                                ));
                                let plan_scan_name = get_scan_name(&col_name, *plan_rel_id)?;
                                if plan_scan_name != ast_scan_name {
                                    return Err(SbroadError::UnexpectedNumberOfValues(
                                        format!(
                                            "Scan name for the column {:?} doesn't match: expected {:?}, found {:?}",
                                            get_column_name(*ast_col_id),
                                            plan_scan_name,
                                            ast_scan_name,
                                    )));
                                }
                            } else {
                                return Err(SbroadError::Invalid(
                                    Entity::Node,
                                    Some("expected AST node to be a scan name.".into()),
                                ));
                            };
                            col_name
                        } else if let (Some(ast_col_id), None) = (first_child_id, second_child_id) {
                            // Get the column name.
                            get_column_name(*ast_col_id)?
                        } else {
                            return Err(SbroadError::UnexpectedNumberOfValues(
                                "no child node found in the AST reference.".into(),
                            ));
                        };

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
                    let ast_ref_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "list of alias children is empty, Reference node id is not found."
                                .into(),
                        )
                    })?;
                    let plan_ref_id = map.get(*ast_ref_id)?;
                    let ast_name_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(Entity::Node, "(Alias name) with index 1".into())
                    })?;
                    let name = self
                        .nodes
                        .get_node(*ast_name_id)?
                        .value
                        .as_ref()
                        .ok_or_else(|| SbroadError::NotFound(Entity::Name, "of Alias".into()))?;
                    let plan_alias_id = plan
                        .nodes
                        .add_alias(&normalize_name_from_sql(name), plan_ref_id)?;
                    map.add(id, plan_alias_id);
                }
                Type::Column => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Column has no children.".into())
                    })?;
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
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Comparison has no children.".into())
                    })?;
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_right_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "that is right node with index 1 among comparison children".into(),
                        )
                    })?;
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;
                    let op = Bool::from_node_type(&node.rule)?;
                    let cond_id = plan.add_cond(plan_left_id, op, plan_right_id)?;
                    map.add(id, cond_id);
                }
                Type::Cmp => {
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Comparison has no children.".into())
                    })?;
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_right_id = node.children.get(2).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "that is right node with index 2 among comparison children".into(),
                        )
                    })?;
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;
                    let ast_op_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "that is operator node with index 1 among comparison children".into(),
                        )
                    })?;
                    let op_node = self.nodes.get_node(*ast_op_id)?;
                    let op = Bool::from_node_type(&op_node.rule)?;
                    let cond_id = plan.add_cond(plan_left_id, op, plan_right_id)?;
                    map.add(id, cond_id);
                }
                Type::IsNull | Type::IsNotNull | Type::Exists | Type::NotExists => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(format!(
                            "{:?} has no children.",
                            &node.rule
                        ))
                    })?;
                    let plan_child_id = plan.as_row(map.get(*ast_child_id)?, &mut rows)?;
                    let op = Unary::from_node_type(&node.rule)?;
                    let unary_id = plan.add_unary(op, plan_child_id)?;
                    map.add(id, unary_id);
                }
                Type::Between => {
                    // left BETWEEN center AND right
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Between has no children.".into())
                    })?;
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_center_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "(center) among between children".into(),
                        )
                    })?;
                    let plan_center_id = plan.as_row(map.get(*ast_center_id)?, &mut rows)?;
                    let ast_right_id = node.children.get(2).ok_or_else(|| {
                        SbroadError::NotFound(Entity::Node, "(right) among between children".into())
                    })?;
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;

                    let greater_eq_id = plan.add_cond(plan_left_id, Bool::GtEq, plan_center_id)?;
                    let less_eq_id = plan.add_cond(plan_left_id, Bool::LtEq, plan_right_id)?;
                    let and_id = plan.add_cond(greater_eq_id, Bool::And, less_eq_id)?;
                    map.add(id, and_id);
                    betweens.push(Between::new(plan_left_id, less_eq_id));
                }
                Type::Cast => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Condition has no children.".into())
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    let ast_type_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "(Cast type) among cast children".into(),
                        )
                    })?;
                    let ast_type = self.nodes.get_node(*ast_type_id)?;
                    let cast_type = if ast_type.rule == Type::TypeVarchar {
                        // Get the length of the varchar.
                        let ast_len_id = ast_type.children.first().ok_or_else(|| {
                            SbroadError::UnexpectedNumberOfValues(
                                "Cast has no children. Cast type length node id is not found."
                                    .into(),
                            )
                        })?;
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
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Concat has no children.".into())
                    })?;
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_right_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(Entity::Node, "(right) among concat children".into())
                    })?;
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;
                    let concat_id = plan.add_concat(plan_left_id, plan_right_id)?;
                    map.add(id, concat_id);
                }
                Type::Condition => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Condition has no children.".into())
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(id, plan_child_id);
                }
                Type::Function => {
                    if let Some((first, mut other)) = node.children.split_first() {
                        let mut is_distinct = false;
                        let function_name =
                            self.nodes.get_node(*first)?.value.as_ref().ok_or_else(|| {
                                SbroadError::NotFound(Entity::Name, "of sql function".into())
                            })?;
                        if let Some(first_id) = other.first() {
                            let rule = &self.nodes.get_node(*first_id)?.rule;
                            match rule {
                                Type::Distinct => {
                                    is_distinct = true;
                                    let Some((_, args)) = other.split_first() else {
                                        return Err(SbroadError::Invalid(
                                            Entity::AST,
                                            Some("function ast has no arguments".into())))
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
                                &function_name.to_string(),
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
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Join has no children.".into())
                    })?;
                    let plan_left_id = map.get(*ast_left_id)?;
                    let ast_right_id = node.children.get(2).ok_or_else(|| {
                        SbroadError::NotFound(Entity::Node, "(right) among Join children.".into())
                    })?;
                    let plan_right_id = map.get(*ast_right_id)?;
                    let ast_cond_id = node.children.get(3).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "(Condition) among Join children".into(),
                        )
                    })?;
                    let ast_kind_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(Entity::Node, "(kind) among Join children.".into())
                    })?;
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
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(format!("{node:?} has no children."))
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    let ast_filter_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "(Filter) among Selection children".into(),
                        )
                    })?;
                    let plan_filter_id = map.get(*ast_filter_id)?;
                    let plan_node_id = match &node.rule {
                        Type::Selection => plan.add_select(&[plan_child_id], plan_filter_id)?,
                        Type::Having => plan.add_having(&[plan_child_id], plan_filter_id)?,
                        _ => return Err(SbroadError::Invalid(Entity::AST, None)), // never happens
                    };
                    map.add(id, plan_node_id);
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
                                let ast_alias_id =
                                    *ast_column.children.first().ok_or_else(|| {
                                        SbroadError::UnexpectedNumberOfValues(
                                            "Column has no children.".into(),
                                        )
                                    })?;
                                let plan_alias_id = map.get(ast_alias_id)?;
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
                    let ast_child_id = *node.children.first().ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::AST,
                            Some(format!("ArithParentheses ({id}) have no child!")),
                        )
                    })?;
                    let plan_child_id = map.get(ast_child_id)?;
                    arith_expr_with_parentheses_ids.push(plan_child_id);
                    map.add(id, plan_child_id);
                }
                Type::Except => {
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Except has no children.".into())
                    })?;
                    let plan_left_id = map.get(*ast_left_id)?;
                    let ast_right_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(Entity::Node, "(right) among Except children".into())
                    })?;
                    let plan_right_id = map.get(*ast_right_id)?;
                    let plan_except_id = plan.add_except(plan_left_id, plan_right_id)?;
                    map.add(id, plan_except_id);
                }
                Type::UnionAll => {
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Union All has no children.".into())
                    })?;
                    let plan_left_id = map.get(*ast_left_id)?;
                    let ast_right_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "(right) among Union All children".into(),
                        )
                    })?;
                    let plan_right_id = map.get(*ast_right_id)?;
                    let plan_union_all_id = plan.add_union_all(plan_left_id, plan_right_id)?;
                    map.add(id, plan_union_all_id);
                }
                Type::ValuesRow => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Values Row has no children.".into())
                    })?;
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
                Type::Insert => {
                    let ast_table_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Insert has no children.".into())
                    })?;
                    let ast_table = self.nodes.get_node(*ast_table_id)?;
                    if let Type::Table = ast_table.rule {
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Type,
                            Some(format!("expected a Table in insert, got {ast_table:?}.",)),
                        ));
                    }
                    let relation =
                        normalize_name_from_sql(ast_table.value.as_ref().ok_or_else(|| {
                            SbroadError::NotFound(Entity::Name, "of table in the AST".into())
                        })?);

                    let ast_child_id = node.children.get(1).ok_or_else(|| {
                        SbroadError::NotFound(
                            Entity::Node,
                            "(second child) among insert children".into(),
                        )
                    })?;
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
                        let mut col_names: Vec<&str> = Vec::with_capacity(ast_child.children.len());
                        for col_id in &ast_child.children {
                            let col = self.nodes.get_node(*col_id)?;
                            if let Type::ColumnName = col.rule {
                                col_names.push(col.value.as_ref().ok_or_else(|| {
                                    SbroadError::NotFound(
                                        Entity::Name,
                                        "of Column among the AST target columns (insert)".into(),
                                    )
                                })?);
                            } else {
                                return Err(SbroadError::Invalid(
                                    Entity::Type,
                                    Some(format!("expected a Column name in insert, got {col:?}.")),
                                ));
                            }
                        }
                        let ast_rel_child_id = node.children.get(2).ok_or_else(|| {
                            SbroadError::NotFound(
                                Entity::Node,
                                "(third child) among Insert children".into(),
                            )
                        })?;
                        let plan_rel_child_id = map.get(*ast_rel_child_id)?;
                        let conflict_strategy = get_conflict_strategy(3)?;
                        plan.add_insert(
                            &relation,
                            plan_rel_child_id,
                            &col_names,
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

                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues("Explain has no children.".into())
                    })?;
                    map.add(0, map.get(*ast_child_id)?);
                }
                Type::SingleQuotedString => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        SbroadError::UnexpectedNumberOfValues(
                            "SingleQuotedString has no children.".into(),
                        )
                    })?;
                    map.add(id, map.get(*ast_child_id)?);
                }
                Type::CountAsterisk => {
                    let plan_id = plan.nodes.push(Node::Expression(Expression::CountAsterisk));
                    map.add(id, plan_id);
                }
                Type::CreateTable => {
                    let create_sharded_table = parse_create_table(self, node)?;
                    let plan_id = plan.nodes.push(Node::Ddl(create_sharded_table));
                    map.add(id, plan_id);
                }
                Type::DropTable => {
                    let mut table_name: String = String::new();
                    let mut timeout = default_timeout;
                    for child_id in &node.children {
                        let child_node = self.nodes.get_node(*child_id)?;
                        match child_node.rule {
                            Type::DeletedTable => {
                                table_name = normalize_name_for_space_api(
                                    child_node.value.as_ref().ok_or_else(|| {
                                        SbroadError::NotFound(
                                            Entity::Node,
                                            "table name in the drop table AST".into(),
                                        )
                                    })?,
                                );
                            }
                            Type::OptionParam => {
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
                Type::Add
                | Type::AliasName
                | Type::Columns
                | Type::ColumnDef
                | Type::ColumnDefName
                | Type::ColumnDefType
                | Type::ColumnDefIsNull
                | Type::ColumnIsNull
                | Type::ColumnIsNotNull
                | Type::ColumnName
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
                | Type::LeftJoinKind
                | Type::Length
                | Type::Lt
                | Type::LtEq
                | Type::Memtx
                | Type::Multiply
                | Type::NewTable
                | Type::NotEq
                | Type::NotIn
                | Type::OptionParam
                | Type::PrimaryKey
                | Type::PrimaryKeyColumn
                | Type::ScanName
                | Type::Select
                | Type::Sharding
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
