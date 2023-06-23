use std::fmt::{Display, Formatter};

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::Type as AstType;
use crate::ir::expression::Expression;
use crate::ir::relation::Type as RelationType;
use crate::ir::{Node, Plan};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
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
    type Error = SbroadError;

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
            _ => Err(SbroadError::Unsupported(
                Entity::Type,
                Some(format!("{ast_type:?}")),
            )),
        }
    }
}

impl TryFrom<&RelationType> for Type {
    type Error = SbroadError;

    fn try_from(relational_type: &RelationType) -> Result<Self, Self::Error> {
        match relational_type {
            RelationType::Boolean => Ok(Type::Boolean),
            RelationType::Decimal => Ok(Type::Decimal),
            RelationType::Double => Ok(Type::Double),
            RelationType::Integer => Ok(Type::Integer),
            RelationType::Scalar => Ok(Type::Scalar),
            RelationType::String => Ok(Type::String),
            RelationType::Number => Ok(Type::Number),
            RelationType::Unsigned => Ok(Type::Unsigned),
            RelationType::Array => Err(SbroadError::Unsupported(
                Entity::Type,
                Some("array int the cast operation".to_string()),
            )),
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
            Type::Varchar(length) => format!("varchar({length})"),
        }
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl Type {
    #[must_use]
    pub fn as_type(&self) -> RelationType {
        match self {
            Type::Any | Type::Scalar => RelationType::Scalar,
            Type::Boolean => RelationType::Boolean,
            Type::Decimal => RelationType::Decimal,
            Type::Double => RelationType::Double,
            Type::Integer => RelationType::Integer,
            Type::Number => RelationType::Number,
            Type::String | Type::Text | Type::Varchar(_) => RelationType::String,
            Type::Unsigned => RelationType::Unsigned,
        }
    }
}

impl Plan {
    /// Adds a cast expression to the plan.
    ///
    /// # Errors
    /// - Child node is not of the expression type.
    pub fn add_cast(&mut self, expr_id: usize, to_type: Type) -> Result<usize, SbroadError> {
        self.get_expression_node(expr_id)?;
        let cast_expr = Expression::Cast {
            child: expr_id,
            to: to_type,
        };
        let cast_id = self.nodes.push(Node::Expression(cast_expr));
        Ok(cast_id)
    }
}
