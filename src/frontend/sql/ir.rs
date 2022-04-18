use std::collections::{HashMap, HashSet};

use traversal::DftPost;

use crate::errors::QueryPlannerError;
use crate::frontend::sql::ast::{ParseNode, Type};
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
    pub(super) fn from_node_type(s: &Type) -> Result<Self, QueryPlannerError> {
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
    pub(super) fn from_node(s: &ParseNode) -> Result<Self, QueryPlannerError> {
        let val = match s.clone().value {
            Some(v) => v,
            None => "".into(),
        };

        match s.rule {
            Type::False => Ok(Value::Boolean(false)),
            Type::Null => Ok(Value::Null),
            Type::Number => Ok(Value::number_from_str(val.as_str())?),
            Type::String => Ok(Value::string_from_str(val.as_str())),
            Type::True => Ok(Value::Boolean(true)),
            _ => Err(QueryPlannerError::UnsupportedIrValueType),
        }
    }
}

#[derive(Debug)]
pub(super) struct Translation {
    map: HashMap<usize, usize>,
}

impl Translation {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Translation {
            map: HashMap::with_capacity(capacity),
        }
    }

    pub(super) fn add(&mut self, parse_id: usize, plan_id: usize) {
        self.map.insert(parse_id, plan_id);
    }

    pub(super) fn get(&self, old: usize) -> Result<usize, QueryPlannerError> {
        self.map.get(&old).copied().ok_or_else(|| {
            QueryPlannerError::CustomError(
                "Could not find parse node in translation map".to_string(),
            )
        })
    }
}

pub(super) fn to_name(s: &str) -> String {
    if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
        return s.to_string();
    }
    s.to_lowercase()
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
    pub(super) fn replace_sq_with_references(&mut self) -> Result<(), QueryPlannerError> {
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
