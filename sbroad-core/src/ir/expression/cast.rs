use std::fmt::{Display, Formatter};

use crate::errors::{Entity, SbroadError};
use crate::frontend::sql::ast::Rule;
use crate::ir::expression::Expression;
use crate::ir::relation::Type as RelationType;
use crate::ir::{Node, Plan};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum Type {
    Any,
    Map,
    Boolean,
    Decimal,
    Double,
    Integer,
    Number,
    Scalar,
    String,
    Text,
    Uuid,
    Unsigned,
    Varchar(usize),
}

impl TryFrom<&Rule> for Type {
    type Error = SbroadError;

    /// Pay attention that we can't build `Type::Varchar(length)` from string
    /// because it has an additional length parameter. It should be constructed
    /// separately.
    fn try_from(ast_type: &Rule) -> Result<Self, Self::Error> {
        match ast_type {
            Rule::TypeAny => Ok(Type::Any),
            Rule::TypeBool => Ok(Type::Boolean),
            Rule::TypeDecimal => Ok(Type::Decimal),
            Rule::TypeDouble => Ok(Type::Double),
            Rule::TypeInt => Ok(Type::Integer),
            Rule::TypeNumber => Ok(Type::Number),
            Rule::TypeScalar => Ok(Type::Scalar),
            Rule::TypeString => Ok(Type::String),
            Rule::TypeText => Ok(Type::Text),
            Rule::TypeUuid => Ok(Type::Uuid),
            Rule::TypeUnsigned => Ok(Type::Unsigned),
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
            RelationType::Map => Ok(Type::Map),
            RelationType::Any => Ok(Type::Any),
            RelationType::Uuid => Ok(Type::Uuid),
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
            Type::Map => "map".to_string(),
            Type::Boolean => "bool".to_string(),
            Type::Decimal => "decimal".to_string(),
            Type::Double => "double".to_string(),
            Type::Integer => "int".to_string(),
            Type::Number => "number".to_string(),
            Type::Scalar => "scalar".to_string(),
            Type::String => "string".to_string(),
            Type::Text => "text".to_string(),
            Type::Uuid => "uuid".to_string(),
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
    pub fn as_relation_type(&self) -> RelationType {
        match self {
            Type::Any | Type::Scalar => RelationType::Scalar,
            Type::Map => RelationType::Map,
            Type::Boolean => RelationType::Boolean,
            Type::Decimal => RelationType::Decimal,
            Type::Double => RelationType::Double,
            Type::Integer => RelationType::Integer,
            Type::Number => RelationType::Number,
            Type::Uuid => RelationType::Uuid,
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
