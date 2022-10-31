use std::fmt::{Display, Formatter};

use crate::errors::QueryPlannerError;
use crate::frontend::sql::ast::Type as AstType;
use crate::ir::expression::Expression;
use crate::ir::{Node, Plan};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum Type {
    Any,
    Boolean,
    Decimal,
    Double,
    Integer,
    Number,
    Scalar,
    String,
    Text,
    Unsigned,
    Varchar(usize),
}

impl TryFrom<&AstType> for Type {
    type Error = QueryPlannerError;

    /// Pay attention that we can't build `Type::Varchar(length)` from string
    /// because it has an additional length parameter. It should be constructed
    /// separately.
    fn try_from(ast_type: &AstType) -> Result<Self, Self::Error> {
        match ast_type {
            AstType::TypeAny => Ok(Type::Any),
            AstType::TypeBool => Ok(Type::Boolean),
            AstType::TypeDecimal => Ok(Type::Decimal),
            AstType::TypeDouble => Ok(Type::Double),
            AstType::TypeInt => Ok(Type::Integer),
            AstType::TypeNumber => Ok(Type::Number),
            AstType::TypeScalar => Ok(Type::Scalar),
            AstType::TypeString => Ok(Type::String),
            AstType::TypeText => Ok(Type::Text),
            AstType::TypeUnsigned => Ok(Type::Unsigned),
            _ => Err(QueryPlannerError::CustomError(format!(
                "Unsupported type: {:?}",
                ast_type
            ))),
        }
    }
}

impl From<&Type> for String {
    fn from(t: &Type) -> Self {
        match t {
            Type::Any => "any".to_string(),
            Type::Boolean => "bool".to_string(),
            Type::Decimal => "decimal".to_string(),
            Type::Double => "double".to_string(),
            Type::Integer => "int".to_string(),
            Type::Number => "number".to_string(),
            Type::Scalar => "scalar".to_string(),
            Type::String => "string".to_string(),
            Type::Text => "text".to_string(),
            Type::Unsigned => "unsigned".to_string(),
            Type::Varchar(length) => format!("varchar({})", length),
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl Plan {
    /// Adds a cast expression to the plan.
    ///
    /// # Errors
    /// - Child node is not of the expression type.
    pub fn add_cast(&mut self, expr_id: usize, to_type: Type) -> Result<usize, QueryPlannerError> {
        self.get_expression_node(expr_id)?;
        let cast_expr = Expression::Cast {
            child: expr_id,
            to: to_type,
        };
        let cast_id = self.nodes.push(Node::Expression(cast_expr));
        Ok(cast_id)
    }
}