use crate::{
    errors::{Entity, SbroadError},
    ir::{expression::Expression, relation::Type, Node, Plan},
};

impl Plan {
    fn get_node_type(&self, node_id: usize) -> Result<Type, SbroadError> {
        match self.get_node(node_id)? {
            Node::Expression(expr) => expr.calculate_type(self),
            Node::Relational(relational) => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("relational node {relational:?} has no type")),
            )),
            // Parameter nodes must recalculate their type during
            // binding (see `bind_params` function).
            Node::Parameter => Ok(Type::Scalar),
            Node::Ddl(_) => Err(SbroadError::Invalid(
                Entity::Node,
                Some("DDL node has no type".to_string()),
            )),
        }
    }
}

impl Expression {
    /// Calculate the type of the expression.
    ///
    /// # Errors
    /// - the row list contains non-expression nodes;
    pub fn calculate_type(&self, plan: &Plan) -> Result<Type, SbroadError> {
        match self {
            Expression::Alias { child, .. } => plan.get_node_type(*child),
            Expression::Bool { .. } | Expression::Unary { .. } => Ok(Type::Boolean),
            Expression::Arithmetic {
                left, right, op, ..
            } => {
                let left_type = plan.get_node_type(*left)?;
                let right_type = plan.get_node_type(*right)?;
                match (&left_type, &right_type) {
                    (Type::Double, Type::Unsigned | Type::Integer | Type::Decimal)
                    | (Type::Unsigned | Type::Integer | Type::Decimal, Type::Double) => {
                        Ok(Type::Double)
                    }
                    (Type::Decimal, Type::Unsigned | Type::Integer)
                    | (Type::Unsigned | Type::Integer, Type::Decimal) => Ok(Type::Decimal),
                    (Type::Integer, Type::Unsigned | Type::Integer)
                    | (Type::Unsigned, Type::Integer) => Ok(Type::Integer),
                    (Type::Unsigned, Type::Unsigned) => Ok(Type::Unsigned),
                    _ => Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(format!("types {left_type} and {right_type} are not supported for arithmetic expression ({:?} {op:?} {:?})",
                        plan.get_node(*left)?, plan.get_node(*right)?)),
                    )),
                }
            }
            Expression::Cast { to, .. } => Ok(to.as_relation_type()),
            Expression::Concat { .. } => Ok(Type::String),
            Expression::Constant { value, .. } => Ok(value.get_type()),
            Expression::Reference { col_type, .. } => Ok(col_type.clone()),
            Expression::Row { list, .. } => {
                if let (Some(expr_id), None) = (list.first(), list.get(1)) {
                    let expr = plan.get_expression_node(*expr_id)?;
                    expr.calculate_type(plan)
                } else {
                    Ok(Type::Array)
                }
            }
            Expression::StableFunction { func_type, .. } => Ok(func_type.clone()),
            Expression::CountAsterisk => Ok(Type::Integer),
        }
    }

    /// Returns the recalculated type of the expression.
    /// At the moment we recalculate only references, because they can change their
    /// type during binding.
    /// E.g. in case of query like
    /// `SELECT "COL_1" FROM (
    ///     SELECT * FROM (
    ///         VALUES ((?))
    ///     ))`,
    /// where we can't calculate type of
    /// upper reference, because we don't know what value will be
    /// passed as an argument.
    /// When `resolve_metadata` is called references are typed with `Scalar`.
    /// When `bind_params` is called references types are refined.
    ///
    /// # Errors
    /// - if the reference is invalid;
    pub fn recalculate_type(&self, plan: &Plan) -> Result<Type, SbroadError> {
        if let Expression::Reference {
            parent,
            targets,
            position,
            ..
        } = self
        {
            let parent_id = parent.ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Expression,
                    Some("reference expression has no parent".to_string()),
                )
            })?;
            let parent_rel = plan.get_relation_node(parent_id)?;
            // We are interested only in the first target, because:
            // - union all relies on the first child type;
            // - scan has no children (and the space column type can't change anyway);
            if let Some(Some(target)) = targets.as_ref().map(|targets| targets.first()) {
                let target_children = parent_rel.children().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Expression,
                        Some("reference expression has no target relation".to_string()),
                    )
                })?;
                let target_rel_id = *target_children.get(*target).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Expression,
                        Some(format!(
                            "reference expression has no target relation at position {}",
                            target
                        )),
                    )
                })?;
                let target_rel = plan.get_relation_node(target_rel_id)?;
                let columns = plan.get_row_list(target_rel.output())?;
                let column_id = *columns.get(*position).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Expression,
                        Some(format!(
                            "reference expression has no target column at position {}",
                            position
                        )),
                    )
                })?;
                let col_expr = plan.get_expression_node(column_id)?;
                return col_expr.calculate_type(plan);
            }
        }
        self.calculate_type(plan)
    }

    pub fn set_ref_type(&mut self, new_type: Type) {
        if let Expression::Reference { col_type, .. } = self {
            *col_type = new_type;
        }
    }
}
