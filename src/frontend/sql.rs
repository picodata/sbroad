//! SQL frontend module.
//!
//! Parses an SQL statement to the abstract syntax tree (AST)
//! and builds the intermediate representation (IR).

use pest::Parser;
use std::collections::{HashMap, HashSet};
use traversal::DftPost;

use crate::errors::QueryPlannerError;
use crate::executor::engine::Metadata;
use crate::frontend::sql::ast::{
    AbstractSyntaxTree, ParseNode, ParseNodes, ParseTree, Rule, StackParseNode, Type,
};
use crate::frontend::sql::ir::{to_name, Translation};
use crate::frontend::Ast;
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational};
use crate::ir::value::Value;
use crate::ir::{Node, Plan};

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
    fn new(query: &str) -> Result<Self, QueryPlannerError> {
        let mut ast = AbstractSyntaxTree::empty();

        let mut command_pair = match ParseTree::parse(Rule::Command, query) {
            Ok(p) => p,
            Err(e) => {
                return Err(QueryPlannerError::CustomError(format!(
                    "Parsing error: {:?}",
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
    fn resolve_metadata<M>(&self, metadata: &M) -> Result<Plan, QueryPlannerError>
    where
        M: Metadata,
    {
        let mut plan = Plan::new();

        let top = match self.top {
            Some(t) => t,
            None => return Err(QueryPlannerError::InvalidAst),
        };
        let dft_post = DftPost::new(&top, |node| self.nodes.ast_iter(node));
        let mut map = Translation::with_capacity(self.nodes.next_id());
        let mut rows: HashSet<usize> = HashSet::with_capacity(self.nodes.next_id());

        for (_, id) in dft_post {
            let node = self.nodes.get_node(*id)?.clone();
            match &node.rule {
                Type::Scan => {
                    let ast_child_id = node.children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Could not find child id in scan node".to_string(),
                        )
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(*id, plan_child_id);
                    if let Some(ast_scan_name_id) = node.children.get(1) {
                        let ast_scan_name = self.nodes.get_node(*ast_scan_name_id)?;
                        if let Type::ScanName = ast_scan_name.rule {
                            // Update scan name in the plan.
                            let scan = plan.get_mut_relation_node(plan_child_id)?;
                            scan.set_scan_name(ast_scan_name.value.as_ref().map(|s| to_name(s)))?;
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
                        let scan_id = plan.add_scan(table, None)?;
                        map.add(*id, scan_id);
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Table name is not found.".into(),
                        ));
                    }
                }
                Type::SubQuery => {
                    let ast_child_id = node.children.get(0).ok_or_else(|| {
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
                        ast_alias.value.as_deref().map(to_name)
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
                            let name: Option<String> = ast_col_name.value.as_deref().map(to_name);
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
                        (plan_rel_list.get(0), plan_rel_list.get(1))
                    {
                        if let (Some(ast_scan_name_id), Some(ast_col_name_id)) =
                            (node.children.get(0), node.children.get(1))
                        {
                            let ast_scan_name = self.nodes.get_node(*ast_scan_name_id)?;
                            if let Type::ScanName = ast_scan_name.rule {
                                // Get the column name and its positions in the output tuples.
                                let col_name = get_column_name(*ast_col_name_id)?;
                                let left_name = get_scan_name(&col_name, *plan_left_id)?;
                                let right_name = get_scan_name(&col_name, *plan_right_id)?;
                                // Check that the AST scan name matches to the children scan names in the plan join node.
                                let scan_name: Option<String> =
                                    ast_scan_name.value.as_deref().map(to_name);
                                // Determine the referred side of the join (left or right).
                                if left_name == scan_name {
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
                                } else if right_name == scan_name {
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
                            (node.children.get(0), node.children.get(1))
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
                        (plan_rel_list.get(0), plan_rel_list.get(1))
                    {
                        let col_name: String = if let (
                            Some(ast_scan_name_id),
                            Some(ast_col_name_id),
                        ) = (node.children.get(0), node.children.get(1))
                        {
                            // Get column name.
                            let col_name = get_column_name(*ast_col_name_id)?;
                            // Check that scan name in the reference matches to the one in scan node.
                            let ast_scan_name = self.nodes.get_node(*ast_scan_name_id)?;
                            if let Type::ScanName = ast_scan_name.rule {
                                let plan_scan_name = get_scan_name(&col_name, *plan_rel_id)?;
                                if plan_scan_name != ast_scan_name.value {
                                    return Err(QueryPlannerError::CustomError(
                                            format!("Scan name for the column {:?} doesn't match: expected {:?}, found {:?}",
                                            get_column_name(*ast_col_name_id), plan_scan_name, ast_scan_name.value
                                        )));
                                }
                            } else {
                                return Err(QueryPlannerError::CustomError(
                                    "Expected AST node to be a scan name.".into(),
                                ));
                            };
                            col_name
                        } else if let (Some(ast_col_name_id), None) =
                            (node.children.get(0), node.children.get(1))
                        {
                            // Get the column name.
                            get_column_name(*ast_col_name_id)?
                        } else {
                            return Err(QueryPlannerError::CustomError(
                                "No child node found in the AST reference.".into(),
                            ));
                        };

                        let ref_list =
                            plan.new_columns(&[*plan_rel_id], false, &[0], &[&col_name], false)?;
                        let ref_id = *ref_list.get(0).ok_or_else(|| {
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
                Type::Number | Type::String | Type::Null | Type::True | Type::False => {
                    let val = Value::from_node(&node)?;
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
                            "Joins are not implemented yet.".into(),
                        ));
                    }
                    let plan_rel_id = *plan_rel_list.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Referred relational node is not found.".into(),
                        )
                    })?;
                    let plan_asterisk_id = plan.add_row_for_output(plan_rel_id, &[])?;
                    map.add(*id, plan_asterisk_id);
                }
                Type::Alias => {
                    let ast_ref_id = node.children.get(0).ok_or_else(|| {
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
                    let plan_alias_id = plan.nodes.add_alias(&to_name(name), plan_ref_id)?;
                    map.add(*id, plan_alias_id);
                }
                Type::Column => {
                    let ast_child_id = node.children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError("Column has no children.".into())
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(*id, plan_child_id);
                }
                Type::Row => {
                    let mut plan_col_list = Vec::new();
                    for ast_child_id in node.children {
                        let plan_child_id = map.get(ast_child_id)?;
                        // If the child is a row that was generated by our
                        // reference-to-row logic in AST code, we should unwrap it back.
                        let plan_id = if rows.get(&plan_child_id).is_some() {
                            let plan_inner_expr = plan.get_expression_node(plan_child_id)?;
                            *plan_inner_expr.extract_row_list()?.get(0).ok_or_else(|| {
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
                | Type::NotEq => {
                    let mut to_row = |plan_id| -> Result<usize, QueryPlannerError> {
                        if let Node::Expression(
                            Expression::Reference { .. } | Expression::Constant { .. },
                        ) = plan.get_node(plan_id)?
                        {
                            let row_id = plan.nodes.add_row(vec![plan_id], None);
                            rows.insert(row_id);
                            Ok(row_id)
                        } else {
                            Ok(plan_id)
                        }
                    };
                    let ast_left_id = node.children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Left node id is not found among comparison children.".into(),
                        )
                    })?;
                    let plan_left_id = to_row(map.get(*ast_left_id)?)?;
                    let ast_right_id = node.children.get(1).ok_or_else(|| {
                        QueryPlannerError::CustomError(
                            "Right node id is not found among comparison children.".into(),
                        )
                    })?;
                    let plan_right_id = to_row(map.get(*ast_right_id)?)?;
                    let op = Bool::from_node_type(&node.rule)?;
                    let cond_id = plan.add_cond(plan_left_id, op, plan_right_id)?;
                    map.add(*id, cond_id);
                }
                Type::Condition => {
                    let ast_child_id = node.children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError("Condition has no children.".into())
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(*id, plan_child_id);
                }
                Type::InnerJoin => {
                    let ast_left_id = node.children.get(0).ok_or_else(|| {
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
                    let ast_child_id = node.children.get(0).ok_or_else(|| {
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
                    let ast_child_id = node.children.get(0).ok_or_else(|| {
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
                                    *ast_column.children.get(0).ok_or_else(|| {
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
                Type::UnionAll => {
                    let ast_left_id = node.children.get(0).ok_or_else(|| {
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
                Type::AliasName
                | Type::ColumnName
                | Type::ScanName
                | Type::Select
                | Type::SubQueryName => {}
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
    pub fn bind_params(&mut self, params: &[Value]) -> Result<(), QueryPlannerError> {
        let top_id = self.get_top()?;
        let tree = DftPost::new(&top_id, |node| self.nodes.subtree_iter(node));
        let nodes: Vec<usize> = tree.map(|(_, id)| *id).collect();
        let mut idx = 0;

        // Add parameter values to the plan arena (but they are still unlinked to the tree).
        let mut value_ids: Vec<usize> = Vec::with_capacity(params.len());
        // We need to use rows instead of values in some cases (AST can solve
        // this problem for non-parameterized queries, but for parameterized
        // queries it is IR responsibility).
        let mut row_ids: Vec<usize> = Vec::with_capacity(params.len());
        for param in params {
            let val_id = self.add_const(param.clone());
            value_ids.push(val_id);
            let row_id = self.nodes.add_row(vec![val_id], None);
            row_ids.push(row_id);
        }

        // Gather all parameter nodes from the tree to a hash set.
        let param_set = self.get_params();

        // Closure to retrieve a corresponding value for a parameter node.
        let get_value =
            |param_id: &usize, pos: usize| -> Result<Option<usize>, QueryPlannerError> {
                if !param_set.contains(param_id) {
                    return Ok(None);
                }
                let val_id = value_ids.get(pos).ok_or_else(|| {
                    QueryPlannerError::CustomError(format!(
                        "Parameter in position {} is not found.",
                        pos
                    ))
                })?;
                Ok(Some(*val_id))
            };

        // Closure to retrieve a corresponding row for a parameter node.
        let get_row = |param_id: &usize, pos: usize| -> Result<Option<usize>, QueryPlannerError> {
            if !param_set.contains(param_id) {
                return Ok(None);
            }
            let row_id = row_ids.get(pos).ok_or_else(|| {
                QueryPlannerError::CustomError(format!(
                    "Parameter in position {} is not found.",
                    pos
                ))
            })?;
            Ok(Some(*row_id))
        };

        // Replace parameters in the plan.
        for id in nodes {
            let node = self.get_mut_node(id)?;
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
                        if let Some(row_id) = get_row(param_id, idx)? {
                            *param_id = row_id;
                            idx += 1;
                        }
                    }
                    _ => {}
                },
                Node::Expression(expr) => match expr {
                    Expression::Alias {
                        child: ref mut param_id,
                        ..
                    } => {
                        if let Some(val_id) = get_value(param_id, idx)? {
                            *param_id = val_id;
                            idx += 1;
                        }
                    }
                    Expression::Bool {
                        ref mut left,
                        ref mut right,
                        ..
                    } => {
                        for param_id in &mut [left, right].iter_mut() {
                            if let Some(row_id) = get_row(param_id, idx)? {
                                **param_id = row_id;
                                idx += 1;
                            }
                        }
                    }
                    Expression::Row { ref mut list, .. } => {
                        for param_id in list {
                            if let Some(val_id) = get_value(param_id, idx)? {
                                *param_id = val_id;
                                idx += 1;
                            }
                        }
                    }
                    Expression::Constant { .. } | Expression::Reference { .. } => {}
                },
                Node::Parameter => {}
            }
        }
        Ok(())
    }
}

pub mod ast;
mod ir;
pub mod tree;
