//! SQL frontend module.
//!
//! Parses an SQL statement to the abstract syntax tree (AST)
//! and builds the intermediate representation (IR).

use ahash::RandomState;
use pest::Parser;
use std::collections::{HashMap, HashSet};
use traversal::DftPost;

use crate::errors::QueryPlannerError;
use crate::executor::engine::{normalize_name_from_sql, CoordinatorMetadata};
use crate::frontend::sql::ast::{
    AbstractSyntaxTree, ParseNode, ParseNodes, ParseTree, Rule, StackParseNode, Type,
};
use crate::frontend::sql::ir::Translation;
use crate::frontend::Ast;
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational, Unary};
use crate::ir::value::Value;
use crate::ir::{Node, Plan};
use crate::otm::child_span;

use sbroad_proc::otm_child_span;

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
    fn new(query: &str) -> Result<Self, QueryPlannerError> {
        let mut ast = AbstractSyntaxTree::empty();

        let mut command_pair = match ParseTree::parse(Rule::Command, query) {
            Ok(p) => p,
            Err(e) => {
                return Err(QueryPlannerError::CustomError(format!(
                    "Parsing error: {}",
                    e
                )))
            }
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

    fn is_empty(&self) -> bool {
        self.nodes.arena.is_empty()
    }

    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    #[otm_child_span("ast.resolve")]
    fn resolve_metadata<M>(&self, metadata: &M) -> Result<Plan, QueryPlannerError>
    where
        M: CoordinatorMetadata,
    {
        let mut plan = Plan::default();

        let top = match self.top {
            Some(t) => t,
            None => return Err(QueryPlannerError::InvalidAst),
        };
        let dft_post = DftPost::new(&top, |node| self.nodes.ast_iter(node));
        let mut map = Translation::with_capacity(self.nodes.next_id());
        let mut rows: HashSet<usize> = HashSet::with_capacity(self.nodes.next_id());
        let mut col_idx: usize = 0;

        for (_, id) in dft_post {
            let node = self.nodes.get_node(*id)?;
            match &node.rule {
                Type::Scan => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Could not find child id in scan node".to_string(),
                        )
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(*id, plan_child_id);
                    if let Some(ast_scan_id) = node.children.get(1) {
                        let ast_scan = self.nodes.get_node(*ast_scan_id)?;
                        if let Type::ScanName = ast_scan.rule {
                            let ast_scan_name =
                                ast_scan.value.as_deref().map(normalize_name_from_sql);
                            // Update scan name in the plan.
                            let scan = plan.get_mut_relation_node(plan_child_id)?;
                            scan.set_scan_name(ast_scan_name)?;
                        } else {
                            return Err(QueryPlannerError::CustomError(
                                "Expected scan name AST node.".into(),
                            ));
                        }
                    }
                }
                Type::Table => {
                    if let Some(node_val) = &node.value {
                        let table = node_val.as_str();
                        let t = metadata.get_table_segment(table)?;
                        plan.add_rel(t);
                        let scan_id = plan.add_scan(&normalize_name_from_sql(table), None)?;
                        map.add(*id, scan_id);
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Table name is not found.".into(),
                        ));
                    }
                }
                Type::SubQuery => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Child node id is not found among sub-query children.".into(),
                        )
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    let alias_name: Option<String> = if let Some(ast_name_id) = node.children.get(1)
                    {
                        let ast_alias = self.nodes.get_node(*ast_name_id)?;
                        if let Type::SubQueryName = ast_alias.rule {
                        } else {
                            return Err(QueryPlannerError::CustomError(format!(
                                "Expected a sub-query name, got {:?}.",
                                ast_alias.rule
                            )));
                        }
                        ast_alias.value.as_deref().map(normalize_name_from_sql)
                    } else {
                        None
                    };
                    let plan_sq_id = plan.add_sub_query(plan_child_id, alias_name.as_deref())?;
                    map.add(*id, plan_sq_id);
                }
                Type::Reference => {
                    let ast_rel_list = self.get_referred_relational_nodes(*id)?;
                    let mut plan_rel_list = Vec::new();
                    for ast_id in ast_rel_list {
                        let plan_id = map.get(ast_id)?;
                        plan_rel_list.push(plan_id);
                    }

                    let get_column_name = |ast_id: usize| -> Result<String, QueryPlannerError> {
                        let ast_col_name = self.nodes.get_node(ast_id)?;
                        if let Type::ColumnName = ast_col_name.rule {
                            let name: Option<String> =
                                ast_col_name.value.as_deref().map(normalize_name_from_sql);
                            Ok(name.ok_or_else(|| {
                                QueryPlannerError::CustomError("Empty AST column name".into())
                            })?)
                        } else {
                            Err(QueryPlannerError::CustomError(
                                "Expected column name AST node.".into(),
                            ))
                        }
                    };

                    let get_scan_name =
                        |col_name: &str,
                         plan_id: usize|
                         -> Result<Option<String>, QueryPlannerError> {
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

                    // Reference to the join node.
                    if let (Some(plan_left_id), Some(plan_right_id)) =
                        (plan_rel_list.first(), plan_rel_list.get(1))
                    {
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
                                        map.add(*id, ref_id);
                                    } else {
                                        return Err(QueryPlannerError::CustomError(format!(
                                            "Column '{}' not found in for the join left child '{:?}'.",
                                            col_name, left_name
                                        )));
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
                                        map.add(*id, ref_id);
                                    } else {
                                        return Err(QueryPlannerError::CustomError(format!(
                                            "Column '{}' not found in for the join right child '{:?}'.",
                                            col_name, right_name
                                        )));
                                    }
                                } else {
                                    return Err(QueryPlannerError::CustomError(
                                        "Left and right plan nodes do not match the AST scan name."
                                            .into(),
                                    ));
                                }
                            } else {
                                return Err(QueryPlannerError::CustomError(
                                    "Expected AST node to be a scan name.".into(),
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
                                map.add(*id, ref_id);
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
                                map.add(*id, ref_id);
                            }
                            return Err(QueryPlannerError::CustomError(format!(
                                "Column '{}' not found in for the join left or right children.",
                                col_name
                            )));
                        } else {
                            return Err(QueryPlannerError::CustomError(
                                "Expected children nodes contain a column name.".into(),
                            ));
                        };

                    // Reference to a single child node.
                    } else if let (Some(plan_rel_id), None) =
                        (plan_rel_list.first(), plan_rel_list.get(1))
                    {
                        let col_name: String = if let (Some(ast_scan_id), Some(ast_col_id)) =
                            (node.children.first(), node.children.get(1))
                        {
                            // Get column name.
                            let col_name = get_column_name(*ast_col_id)?;
                            // Check that scan name in the reference matches to the one in scan node.
                            let ast_scan = self.nodes.get_node(*ast_scan_id)?;
                            if let Type::ScanName = ast_scan.rule {
                                let ast_scan_name = Some(normalize_name_from_sql(
                                    ast_scan.value.as_ref().ok_or_else(|| {
                                        QueryPlannerError::CustomError(
                                            "Expected AST node to have a non-empty scan name."
                                                .into(),
                                        )
                                    })?,
                                ));
                                let plan_scan_name = get_scan_name(&col_name, *plan_rel_id)?;
                                if plan_scan_name != ast_scan_name {
                                    return Err(QueryPlannerError::CustomError(
                                            format!("Scan name for the column {:?} doesn't match: expected {:?}, found {:?}",
                                            get_column_name(*ast_col_id), plan_scan_name, ast_scan_name
                                        )));
                                }
                            } else {
                                return Err(QueryPlannerError::CustomError(
                                    "Expected AST node to be a scan name.".into(),
                                ));
                            };
                            col_name
                        } else if let (Some(ast_col_id), None) =
                            (node.children.first(), node.children.get(1))
                        {
                            // Get the column name.
                            get_column_name(*ast_col_id)?
                        } else {
                            return Err(QueryPlannerError::CustomError(
                                "No child node found in the AST reference.".into(),
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
                            QueryPlannerError::CustomError("Referred column is not found.".into())
                        })?;
                        map.add(*id, ref_id);
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Expected one or two referred relational nodes, got less or more."
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
                    map.add(*id, plan.add_const(val));
                }
                Type::Parameter => {
                    map.add(*id, plan.add_param());
                }
                Type::Asterisk => {
                    // We can get an asterisk only in projection.
                    let ast_rel_list = self.get_referred_relational_nodes(*id)?;
                    let mut plan_rel_list = Vec::new();
                    for ast_id in ast_rel_list {
                        let plan_id = map.get(ast_id)?;
                        plan_rel_list.push(plan_id);
                    }
                    if plan_rel_list.len() > 1 {
                        return Err(QueryPlannerError::CustomError(
                            "Sub-queries in projections are not implemented yet.".into(),
                        ));
                    }
                    let plan_rel_id = *plan_rel_list.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Referred relational node is not found.".into(),
                        )
                    })?;
                    let plan_asterisk_id = plan.add_row_for_output(plan_rel_id, &[], false)?;
                    map.add(*id, plan_asterisk_id);
                }
                Type::Alias => {
                    let ast_ref_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Reference node id is not found among alias children.".into(),
                        )
                    })?;
                    let plan_ref_id = map.get(*ast_ref_id)?;
                    let ast_name_id = node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Alias name node id is not found among alias children.".into(),
                        )
                    })?;
                    let name = self
                        .nodes
                        .get_node(*ast_name_id)?
                        .value
                        .as_ref()
                        .ok_or_else(|| {
                            QueryPlannerError::CustomError("Alias name is not found.".into())
                        })?;
                    let plan_alias_id = plan
                        .nodes
                        .add_alias(&normalize_name_from_sql(name), plan_ref_id)?;
                    map.add(*id, plan_alias_id);
                }
                Type::Column => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError("Column has no children.".into())
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(*id, plan_child_id);
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
                                QueryPlannerError::CustomError("Row is empty.".into())
                            })?
                        } else {
                            plan_child_id
                        };
                        plan_col_list.push(plan_id);
                    }
                    let plan_row_id = plan.nodes.add_row(plan_col_list, None);
                    map.add(*id, plan_row_id);
                }
                Type::And
                | Type::Or
                | Type::Eq
                | Type::In
                | Type::Gt
                | Type::GtEq
                | Type::Lt
                | Type::LtEq
                | Type::NotEq
                | Type::NotIn => {
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Left node id is not found among comparison children.".into(),
                        )
                    })?;
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_right_id = node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Right node id is not found among comparison children.".into(),
                        )
                    })?;
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;
                    let op = Bool::from_node_type(&node.rule)?;
                    let cond_id = plan.add_cond(plan_left_id, op, plan_right_id)?;
                    map.add(*id, cond_id);
                }
                Type::IsNull | Type::IsNotNull => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(format!("{:?} has no children.", &node.rule))
                    })?;
                    let plan_child_id = plan.as_row(map.get(*ast_child_id)?, &mut rows)?;
                    let op = Unary::from_node_type(&node.rule)?;
                    let unary_id = plan.add_unary(op, plan_child_id)?;
                    map.add(*id, unary_id);
                }
                Type::Between => {
                    // left BETWEEN center AND right
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Left node id is not found among between children.".into(),
                        )
                    })?;
                    let plan_left_id = plan.as_row(map.get(*ast_left_id)?, &mut rows)?;
                    let ast_center_id = node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Center node id is not found among between children.".into(),
                        )
                    })?;
                    let plan_center_id = plan.as_row(map.get(*ast_center_id)?, &mut rows)?;
                    let ast_right_id = node.children.get(2).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Right node id is not found among between children.".into(),
                        )
                    })?;
                    let plan_right_id = plan.as_row(map.get(*ast_right_id)?, &mut rows)?;

                    let greater_eq_id = plan.add_cond(plan_left_id, Bool::GtEq, plan_center_id)?;
                    let less_eq_id = plan.add_cond(plan_left_id, Bool::LtEq, plan_right_id)?;
                    let and_id = plan.add_cond(greater_eq_id, Bool::And, less_eq_id)?;
                    map.add(*id, and_id);
                }
                Type::Condition => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError("Condition has no children.".into())
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(*id, plan_child_id);
                }
                Type::Function => {
                    if let Some((first, other)) = node.children.split_first() {
                        let mut plan_arg_list = Vec::new();
                        for ast_child_id in other {
                            let plan_child_id = map.get(*ast_child_id)?;
                            plan_arg_list.push(plan_child_id);
                        }
                        let function_name =
                            self.nodes.get_node(*first)?.value.as_ref().ok_or_else(|| {
                                QueryPlannerError::CustomError("Function name is not found.".into())
                            })?;
                        let func = metadata.get_function(function_name)?;
                        if func.is_stable() {
                            let plan_func_id = plan.add_stable_function(func, plan_arg_list)?;
                            map.add(*id, plan_func_id);
                        } else {
                            // At the moment we don't support any non-stable functions.
                            // Later this code block should handle other function behaviors.
                            return Err(QueryPlannerError::CustomError(format!(
                                "Function {} is not stable.",
                                function_name
                            )));
                        }
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Function has no children.".into(),
                        ));
                    }
                }
                Type::InnerJoin => {
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Left node id is not found among join children.".into(),
                        )
                    })?;
                    let plan_left_id = map.get(*ast_left_id)?;
                    let ast_right_id = node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Right node id is not found among join children.".into(),
                        )
                    })?;
                    let plan_right_id = map.get(*ast_right_id)?;
                    let ast_cond_id = node.children.get(2).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Condition node id is not found among join children.".into(),
                        )
                    })?;
                    let plan_cond_id = map.get(*ast_cond_id)?;
                    let plan_join_id = plan.add_join(plan_left_id, plan_right_id, plan_cond_id)?;
                    map.add(*id, plan_join_id);
                }
                Type::Selection => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Child node id is not found among selection children.".into(),
                        )
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    let ast_filter_id = node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Filter node id is not found among selection children.".into(),
                        )
                    })?;
                    let plan_filter_id = map.get(*ast_filter_id)?;
                    let plan_selection_id = plan.add_select(&[plan_child_id], plan_filter_id)?;
                    map.add(*id, plan_selection_id);
                }
                Type::Projection => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Child node id is not found among projection children.".into(),
                        )
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    let mut columns: Vec<usize> = Vec::new();
                    for ast_column_id in node.children.iter().skip(1) {
                        let ast_column = self.nodes.get_node(*ast_column_id)?;
                        match ast_column.rule {
                            Type::Column => {
                                let ast_alias_id =
                                    *ast_column.children.first().ok_or_else(|| {
                                        QueryPlannerError::CustomError(
                                            "Alias node id is not found among column children."
                                                .into(),
                                        )
                                    })?;
                                let plan_alias_id = map.get(ast_alias_id)?;
                                columns.push(plan_alias_id);
                            }
                            Type::Asterisk => {
                                let plan_asterisk_id = map.get(*ast_column_id)?;
                                if let Node::Expression(Expression::Row { list, .. }) =
                                    plan.get_node(plan_asterisk_id)?
                                {
                                    for row_id in list {
                                        columns.push(*row_id);
                                    }
                                } else {
                                    return Err(QueryPlannerError::CustomError(
                                        "A plan node corresponding to asterisk is not a row."
                                            .into(),
                                    ));
                                }
                            }
                            _ => {
                                return Err(QueryPlannerError::CustomError(format!(
                                    "Expected a column in projection, got {:?}.",
                                    ast_column.rule
                                )));
                            }
                        }
                    }
                    let projection_id = plan.add_proj_internal(plan_child_id, &columns)?;
                    map.add(*id, projection_id);
                }
                Type::Except => {
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Left node id is not found among except children.".into(),
                        )
                    })?;
                    let plan_left_id = map.get(*ast_left_id)?;
                    let ast_right_id = node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Right node id is not found among except children.".into(),
                        )
                    })?;
                    let plan_right_id = map.get(*ast_right_id)?;
                    let plan_except_id = plan.add_except(plan_left_id, plan_right_id)?;
                    map.add(*id, plan_except_id);
                }
                Type::UnionAll => {
                    let ast_left_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Left node id is not found among union all children.".into(),
                        )
                    })?;
                    let plan_left_id = map.get(*ast_left_id)?;
                    let ast_right_id = node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Right node id is not found among union all children.".into(),
                        )
                    })?;
                    let plan_right_id = map.get(*ast_right_id)?;
                    let plan_union_all_id = plan.add_union_all(plan_left_id, plan_right_id)?;
                    map.add(*id, plan_union_all_id);
                }
                Type::ValuesRow => {
                    let ast_child_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError("Values row has no children.".into())
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    let values_row_id = plan.add_values_row(plan_child_id, &mut col_idx)?;
                    map.add(*id, values_row_id);
                }
                Type::Values => {
                    let mut plan_children_ids: Vec<usize> = Vec::with_capacity(node.children.len());
                    for ast_child_id in &node.children {
                        let plan_child_id = map.get(*ast_child_id)?;
                        plan_children_ids.push(plan_child_id);
                    }
                    let plan_values_id = plan.add_values(plan_children_ids)?;
                    map.add(*id, plan_values_id);
                }
                Type::Insert => {
                    let ast_table_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Table node id is not found among insert children.".into(),
                        )
                    })?;
                    let ast_table = self.nodes.get_node(*ast_table_id)?;
                    if let Type::Table = ast_table.rule {
                    } else {
                        return Err(QueryPlannerError::CustomError(format!(
                            "Expected a table in insert, got {:?}.",
                            ast_table
                        )));
                    }
                    let relation: &str = ast_table.value.as_ref().ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Table name was not found in the AST.".into(),
                        )
                    })?;

                    let ast_child_id = node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Second child is not found among insert children.".into(),
                        )
                    })?;
                    let ast_child = self.nodes.get_node(*ast_child_id)?;
                    let plan_insert_id = if let Type::TargetColumns = ast_child.rule {
                        // insert into t (a, b, c) ...
                        let mut col_names: Vec<&str> = Vec::with_capacity(ast_child.children.len());
                        for col_id in &ast_child.children {
                            let col = self.nodes.get_node(*col_id)?;
                            if let Type::ColumnName = col.rule {
                                col_names.push(col.value.as_ref().ok_or_else(||
                                        QueryPlannerError::CustomError(
                                            "Column name was not found among the AST target columns (insert).".into(),
                                    ))?);
                            } else {
                                return Err(QueryPlannerError::CustomError(format!(
                                    "Expected a column name in insert, got {:?}.",
                                    col
                                )));
                            }
                        }
                        let ast_rel_child_id = node.children.get(2).ok_or_else(|| {
                            QueryPlannerError::CustomError(
                                "Third child is not found among insert children.".into(),
                            )
                        })?;
                        let plan_rel_child_id = map.get(*ast_rel_child_id)?;
                        plan.add_insert(relation, plan_rel_child_id, &col_names)?
                    } else {
                        // insert into t ...
                        let plan_child_id = map.get(*ast_child_id)?;
                        plan.add_insert(relation, plan_child_id, &[])?
                    };
                    map.add(*id, plan_insert_id);
                }
                Type::Explain => {
                    plan.mark_as_explain();

                    let ast_child_id = node.children.first().ok_or_else(|| {
                        QueryPlannerError::CustomError("Explain has no children.".into())
                    })?;
                    map.add(0, map.get(*ast_child_id)?);
                }
                Type::AliasName
                | Type::ColumnName
                | Type::FunctionName
                | Type::ScanName
                | Type::Select
                | Type::SubQueryName
                | Type::TargetColumns => {}
                rule => {
                    return Err(QueryPlannerError::CustomError(format!(
                        "Not implements type: {:?}",
                        rule
                    )));
                }
            }
        }

        // get root node id
        let plan_top_id = map.get(
            self.top
                .ok_or_else(|| QueryPlannerError::CustomError("No top in AST.".into()))?,
        )?;
        plan.set_top(plan_top_id)?;
        plan.replace_sq_with_references()?;

        Ok(plan)
    }
}

impl Plan {
    /// Substitute parameters to the plan.
    ///
    /// # Errors
    /// - Invalid amount of parameters.
    /// - Internal errors.
    #[allow(clippy::too_many_lines)]
    #[otm_child_span("plan.bind")]
    pub fn bind_params(&mut self, mut params: Vec<Value>) -> Result<(), QueryPlannerError> {
        // Nothing to do here.
        if params.is_empty() {
            return Ok(());
        }

        let top_id = self.get_top()?;
        let tree = DftPost::new(&top_id, |node| self.subtree_iter(node));
        let nodes: Vec<usize> = tree.map(|(_, id)| *id).collect();

        // Transform parameters to values. The result values are stored in the
        // opposite to parameters order.
        let mut value_ids: Vec<usize> = Vec::with_capacity(params.len());
        while let Some(param) = params.pop() {
            value_ids.push(self.add_const(param));
        }

        // We need to use rows instead of values in some cases (AST can solve
        // this problem for non-parameterized queries, but for parameterized
        // queries it is IR responsibility).
        let mut row_ids: HashMap<usize, usize, RandomState> =
            HashMap::with_hasher(RandomState::new());

        // Gather all parameter nodes from the tree to a hash set.
        let param_set = self.get_params();

        // Closure to retrieve a corresponding value for a parameter node.
        let get_value = |pos: usize| -> Result<usize, QueryPlannerError> {
            let val_id = value_ids.get(pos).ok_or_else(|| {
                QueryPlannerError::CustomError(format!(
                    "Parameter in position {} is not found.",
                    pos
                ))
            })?;
            Ok(*val_id)
        };

        // Populate rows.
        let mut idx = value_ids.len();
        for id in &nodes {
            let node = self.get_node(*id)?;
            match node {
                Node::Relational(rel) => match rel {
                    Relational::Selection {
                        filter: ref param_id,
                        ..
                    }
                    | Relational::InnerJoin {
                        condition: ref param_id,
                        ..
                    }
                    | Relational::Projection {
                        output: ref param_id,
                        ..
                    } => {
                        if param_set.contains(param_id) {
                            idx -= 1;
                            let val_id = get_value(idx)?;
                            row_ids.insert(idx, self.nodes.add_row(vec![val_id], None));
                        }
                    }
                    _ => {}
                },
                Node::Expression(expr) => match expr {
                    Expression::Alias {
                        child: ref param_id,
                        ..
                    }
                    | Expression::Unary {
                        child: ref param_id,
                        ..
                    } => {
                        if param_set.contains(param_id) {
                            idx -= 1;
                        }
                    }
                    Expression::Bool {
                        ref left,
                        ref right,
                        ..
                    } => {
                        for param_id in &[*left, *right] {
                            if param_set.contains(param_id) {
                                idx -= 1;
                                let val_id = get_value(idx)?;
                                row_ids.insert(idx, self.nodes.add_row(vec![val_id], None));
                            }
                        }
                    }
                    Expression::Row { ref list, .. }
                    | Expression::StableFunction {
                        children: ref list, ..
                    } => {
                        for param_id in list {
                            if param_set.contains(param_id) {
                                idx -= 1;
                            }
                        }
                    }
                    Expression::Constant { .. } | Expression::Reference { .. } => {}
                },
                Node::Parameter => {}
            }
        }

        let get_row = |idx: usize| -> Result<usize, QueryPlannerError> {
            let row_id = row_ids.get(&idx).ok_or_else(|| {
                QueryPlannerError::CustomError(format!("Row in position {} is not found.", idx))
            })?;
            Ok(*row_id)
        };

        // Replace parameters in the plan.
        idx = value_ids.len();
        for id in &nodes {
            let node = self.get_mut_node(*id)?;
            match node {
                Node::Relational(rel) => match rel {
                    Relational::Selection {
                        filter: ref mut param_id,
                        ..
                    }
                    | Relational::InnerJoin {
                        condition: ref mut param_id,
                        ..
                    }
                    | Relational::Projection {
                        output: ref mut param_id,
                        ..
                    } => {
                        if param_set.contains(param_id) {
                            idx -= 1;
                            let row_id = get_row(idx)?;
                            *param_id = row_id;
                        }
                    }
                    _ => {}
                },
                Node::Expression(expr) => match expr {
                    Expression::Alias {
                        child: ref mut param_id,
                        ..
                    }
                    | Expression::Unary {
                        child: ref mut param_id,
                        ..
                    } => {
                        if param_set.contains(param_id) {
                            idx -= 1;
                            let val_id = get_value(idx)?;
                            *param_id = val_id;
                        }
                    }
                    Expression::Bool {
                        ref mut left,
                        ref mut right,
                        ..
                    } => {
                        for param_id in &mut [left, right].iter_mut() {
                            if param_set.contains(param_id) {
                                idx -= 1;
                                let row_id = get_row(idx)?;
                                **param_id = row_id;
                            }
                        }
                    }
                    Expression::Row { ref mut list, .. }
                    | Expression::StableFunction {
                        children: ref mut list,
                        ..
                    } => {
                        for param_id in list {
                            if param_set.contains(param_id) {
                                idx -= 1;
                                let val_id = get_value(idx)?;
                                *param_id = val_id;
                            }
                        }
                    }
                    Expression::Constant { .. } | Expression::Reference { .. } => {}
                },
                Node::Parameter => {}
            }
        }

        // Update values row output.
        for id in nodes {
            if let Ok(Relational::ValuesRow { .. }) = self.get_relation_node(id) {
                self.update_values_row(id)?;
            }
        }

        Ok(())
    }

    /// Wrap references and constants in the plan into rows.
    fn as_row(
        &mut self,
        expr_id: usize,
        rows: &mut HashSet<usize>,
    ) -> Result<usize, QueryPlannerError> {
        if let Node::Expression(Expression::Reference { .. } | Expression::Constant { .. }) =
            self.get_node(expr_id)?
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
