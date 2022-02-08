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
        let dft_pre = DftPost::new(&top, |node| self.nodes.tree_iter(node));
        let mut map = Translation::new();

        for (_, id) in dft_pre {
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
                }
                Type::Table => {
                    if let Some(node_val) = &node.value {
                        let table = node_val.as_str().trim_matches('\"');
                        let t = metadata.get_table_segment(table)?;
                        plan.add_rel(t);
                        let scan_id = plan.add_scan(table)?;
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
                        ast_alias.value.clone()
                    } else {
                        None
                    };
                    let name = alias_name.as_deref().map(|s| s.trim_matches('\"'));
                    let plan_sq_id = plan.add_sub_query(plan_child_id, name)?;
                    map.add(*id, plan_sq_id);
                }
                Type::Reference => {
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
                    let col_name = node
                        .value
                        .as_ref()
                        .ok_or_else(|| {
                            QueryPlannerError::CustomError("Column name is not found.".into())
                        })?
                        .trim_matches('\"')
                        .to_string();
                    let ref_list =
                        plan.new_columns(&[plan_rel_id], false, &[0], &[col_name.as_str()], false)?;
                    let ref_id = *ref_list.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError("Referred column is not found.".into())
                    })?;
                    map.add(*id, ref_id);
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
                        })?
                        .trim_matches('\"');
                    let plan_alias_id = plan.nodes.add_alias(name, plan_ref_id)?;
                    map.add(*id, plan_alias_id);
                }
                Type::Column => {
                    let ast_child_id = node.children.get(0).ok_or_else(|| {
                        QueryPlannerError::CustomError("Column has no children.".into())
                    })?;
                    let plan_child_id = map.get(*ast_child_id)?;
                    map.add(*id, plan_child_id);
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
                Type::AliasName | Type::SubQueryName | Type::Select => {}
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
