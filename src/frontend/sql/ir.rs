use std::collections::{HashMap, HashSet};
use traversal::DftPost;

use crate::errors::QueryPlannerError;
use crate::executor::engine::Metadata;
use crate::frontend::sql::ast::{AbstractSyntaxTree, ParseNode, Type};
use crate::ir::expression::Expression;
use crate::ir::operator::{Bool, Relational};
use crate::ir::value::Value;
use crate::ir::{Node, Plan};

impl Bool {
    /// Create `Bool` from ast node type.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the operator is invalid.
    #[allow(dead_code)]
    fn from_node_type(s: &Type) -> Result<Self, QueryPlannerError> {
        match s {
            Type::And => Ok(Bool::And),
            Type::Or => Ok(Bool::Or),
            Type::Eq => Ok(Bool::Eq),
            Type::In => Ok(Bool::In),
            Type::Gt => Ok(Bool::Gt),
            Type::GtEq => Ok(Bool::GtEq),
            Type::Lt => Ok(Bool::Lt),
            Type::LtEq => Ok(Bool::LtEq),
            Type::NotEq => Ok(Bool::NotEq),
            _ => Err(QueryPlannerError::InvalidBool),
        }
    }
}

impl Value {
    /// Create `Value` from ast node type and text.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the operator is invalid.
    #[allow(dead_code)]
    fn from_node(s: &ParseNode) -> Result<Self, QueryPlannerError> {
        let val = match s.clone().value {
            Some(v) => v,
            None => "".into(),
        };

        match s.rule {
            Type::Number => Ok(Value::number_from_str(val.as_str())?),
            Type::String => Ok(Value::string_from_str(val.as_str())),
            Type::Null => Ok(Value::Null),
            _ => Err(QueryPlannerError::UnsupportedIrValueType),
        }
    }
}

#[derive(Debug)]
struct Translation {
    map: HashMap<usize, usize>,
}

impl Translation {
    fn new() -> Self {
        Translation {
            map: HashMap::new(),
        }
    }

    fn add(&mut self, parse_id: usize, plan_id: usize) {
        self.map.insert(parse_id, plan_id);
    }

    fn get(&self, old: usize) -> Result<usize, QueryPlannerError> {
        self.map.get(&old).copied().ok_or_else(|| {
            QueryPlannerError::CustomError(
                "Could not find parse node in translation map".to_string(),
            )
        })
    }
}

fn to_name(s: &str) -> String {
    if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
        return s.to_string();
    }
    s.to_lowercase()
}

impl AbstractSyntaxTree {
    /// Transform AST to IR plan tree.
    ///
    /// # Errors
    /// - IR plan can't be built.
    #[allow(dead_code)]
    #[allow(clippy::too_many_lines)]
    pub fn to_ir<T>(&self, metadata: &T) -> Result<Plan, QueryPlannerError>
    where
        T: Metadata,
    {
        let mut plan = Plan::new();

        let top = match self.top {
            Some(t) => t,
            None => return Err(QueryPlannerError::InvalidAst),
        };
        let dft_post = DftPost::new(&top, |node| self.nodes.tree_iter(node));
        let mut map = Translation::new();
        let mut rows: HashSet<usize> = HashSet::new();

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

                    // Reference to the join node.
                    if let (Some(plan_left_id), Some(plan_right_id)) =
                        (plan_rel_list.get(0), plan_rel_list.get(1))
                    {
                        if let (Some(ast_scan_name_id), Some(ast_col_name_id)) =
                            (node.children.get(0), node.children.get(1))
                        {
                            let ast_scan_name = self.nodes.get_node(*ast_scan_name_id)?;
                            if let Type::ScanName = ast_scan_name.rule {
                                // Check that the scan name matches to the children in the join node.
                                let left_name =
                                    plan.get_relation_node(*plan_left_id)?.scan_name(&plan, 0)?;
                                let right_name = plan
                                    .get_relation_node(*plan_right_id)?
                                    .scan_name(&plan, 0)?;
                                let scan_name = ast_scan_name.value.as_deref();
                                let col_name = get_column_name(*ast_col_name_id)?;
                                // Determine the referred side of the join (left or right).
                                if left_name == scan_name {
                                    let left_col_map = plan
                                        .get_relation_node(*plan_left_id)?
                                        .output_alias_position_map(&plan.nodes)?;
                                    if left_col_map.get(&col_name).is_some() {
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
                                    if right_col_map.get(&col_name).is_some() {
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
                            if left_col_map.get(&col_name).is_some() {
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
                            if right_col_map.get(&col_name).is_some() {
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
                        let col_name: String =
                            if let (Some(ast_scan_name_id), Some(ast_col_name_id)) =
                                (node.children.get(0), node.children.get(1))
                            {
                                // Check that scan name in the reference matches to the one in scan node.
                                let ast_scan_name = self.nodes.get_node(*ast_scan_name_id)?;
                                if let Type::ScanName = ast_scan_name.rule {
                                    let plan_scan_name = plan
                                        .get_relation_node(*plan_rel_id)?
                                        .scan_name(&plan, 0)?;
                                    let scan_name = ast_scan_name.value.as_deref();
                                    if plan_scan_name != scan_name {
                                        return Err(QueryPlannerError::CustomError(
                                            "Scan name is not matched.".into(),
                                        ));
                                    }
                                } else {
                                    return Err(QueryPlannerError::CustomError(
                                        "Expected AST node to be a scan name.".into(),
                                    ));
                                };
                                // Get column name.
                                get_column_name(*ast_col_name_id)?
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
                Type::Number | Type::String => {
                    let val = Value::from_node(&node)?;
                    map.add(*id, plan.add_const(val));
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
                            if let Expression::Row { list, .. } = plan_inner_expr {
                                *list.get(0).ok_or_else(|| {
                                    QueryPlannerError::CustomError("Row is empty.".into())
                                })?
                            } else {
                                return Err(QueryPlannerError::CustomError(
                                    "Expected a row expression.".into(),
                                ));
                            }
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

#[derive(Hash, PartialEq, Debug)]
struct SubQuery {
    relational: usize,
    operator: usize,
    sq: usize,
}
impl Eq for SubQuery {}

impl SubQuery {
    fn new(relational: usize, operator: usize, sq: usize) -> SubQuery {
        SubQuery {
            relational,
            operator,
            sq,
        }
    }
}

impl Plan {
    fn gather_sq_for_replacement(&self) -> Result<HashSet<SubQuery>, QueryPlannerError> {
        let mut set: HashSet<SubQuery> = HashSet::new();
        let top = self.get_top()?;
        let rel_post = DftPost::new(&top, |node| self.nodes.rel_iter(node));
        // Traverse expression trees of the selection and join nodes.
        // Gather all sub-queries in the boolean expressions there.
        for (_, rel_id) in rel_post {
            match self.get_node(*rel_id)? {
                Node::Relational(
                    Relational::Selection { filter: tree, .. }
                    | Relational::InnerJoin {
                        condition: tree, ..
                    },
                ) => {
                    let expr_post = DftPost::new(tree, |node| self.nodes.expr_iter(node, false));
                    for (_, id) in expr_post {
                        if let Node::Expression(Expression::Bool { left, right, .. }) =
                            self.get_node(*id)?
                        {
                            let children = &[*left, *right];
                            for child in children {
                                if let Node::Relational(Relational::ScanSubQuery { .. }) =
                                    self.get_node(*child)?
                                {
                                    set.insert(SubQuery::new(*rel_id, *id, *child));
                                }
                            }
                        }
                    }
                }
                _ => continue,
            }
        }
        Ok(set)
    }

    /// Replace sub-queries with references to the sub-query.
    fn replace_sq_with_references(&mut self) -> Result<(), QueryPlannerError> {
        let set = self.gather_sq_for_replacement()?;
        for sq in set {
            // Append sub-query to relational node.
            match self.get_mut_node(sq.relational)? {
                Node::Relational(
                    Relational::Selection { children, .. } | Relational::InnerJoin { children, .. },
                ) => {
                    children.push(sq.sq);
                }
                _ => {
                    return Err(QueryPlannerError::CustomError(
                        "Sub-query is not in selection or join node".into(),
                    ))
                }
            }

            // Generate a reference to the sub-query.
            let row_id: usize = match self.get_node(sq.relational)? {
                Node::Relational(
                    Relational::Selection { children, .. } | Relational::InnerJoin { children, .. },
                ) => {
                    let nodes = children.clone();
                    let sq_output = self.get_relation_node(sq.sq)?.output();
                    let row = self.get_expression_node(sq_output)?;
                    if let Expression::Row { list, .. } = row {
                        let mut names: Vec<String> = Vec::new();
                        for col_id in list {
                            names.push(self.get_expression_node(*col_id)?.get_alias_name()?);
                        }
                        let names_str: Vec<_> = names.iter().map(String::as_str).collect();
                        // TODO: should we add current row_id to the set of the generated rows?
                        let row_id =
                            self.add_row_from_sub_query(&nodes, nodes.len() - 1, &names_str)?;
                        self.replace_parent_in_subtree(row_id, None, Some(sq.relational))?;
                        row_id
                    } else {
                        return Err(QueryPlannerError::CustomError(
                            "Sub-query output is not a row".into(),
                        ));
                    }
                }
                _ => {
                    return Err(QueryPlannerError::CustomError(
                        "Sub-query is not in selection or join node".into(),
                    ))
                }
            };

            // Replace sub-query with reference.
            let op = self.get_mut_expression_node(sq.operator)?;
            if let Expression::Bool {
                ref mut left,
                ref mut right,
                ..
            } = op
            {
                if *left == sq.sq {
                    *left = row_id;
                } else if *right == sq.sq {
                    *right = row_id;
                } else {
                    return Err(QueryPlannerError::CustomError(
                        "Sub-query is not a left or right operand".into(),
                    ));
                }
            } else {
                return Err(QueryPlannerError::CustomError(
                    "Sub-query is not in a boolean expression".into(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
