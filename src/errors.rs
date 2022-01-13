use std::fmt;

use serde::Serialize;

const BUCKET_ID_ERROR: &str = "field doesn't contains sharding key value";
const DUPLICATE_COLUMN_ERROR: &str = "duplicate column";
const EMPTY_PLAN_RELATION: &str = "empty plan relations";
const INCORRECT_BUCKET_ID_ERROR: &str = "incorrect bucket id";
const INVALID_AST: &str = "invalid AST";
const INVALID_BOOL_ERROR: &str = "invalid boolean";
const INVALID_COLUMN_NAME: &str = "invalid column name";
const INVALID_CLUSTER_SCHEMA: &str = "cluster schema is invalid";
const INVALID_INPUT: &str = "invalid input";
const INVALID_NAME_ERROR: &str = "invalid name";
const INVALID_NODE: &str = "invalid node";
const INVALID_NUMBER_ERROR: &str = "invalid number";
const INVALID_PLAN_ERROR: &str = "invalid plan";
const INVALID_REFERENCE: &str = "invalid reference";
const INVALID_RELATION_ERROR: &str = "invalid relation";
const INVALID_ROW_ERROR: &str = "invalid row";
const INVALID_SCHEMA_SPACES: &str = "not found spaces in schema";
const INVALID_SHARDING_KEY_ERROR: &str = "invalid sharding key";
const INVALID_SPACE_NAME: &str = "invalid space name";
const NOT_EQUAL_ROWS: &str = "not equal rows";
const QUERY_NOT_IMPLEMENTED: &str = "query wasn't s implemented";
const REQUIRE_MOTION: &str = "require motion";
const SERIALIZATION_ERROR: &str = "serialization";
const SIMPLE_QUERY_ERROR: &str = "query doesn't simple";
const SIMPLE_UNION_QUERY_ERROR: &str = "query doesn't simple union";
const SPACE_NOT_FOUND: &str = "space not found";
const SPACE_FORMAT_NOT_FOUND: &str = "space format not found";
const UNINITIALIZED_DISTRIBUTION: &str = "uninitialized distribution";
const VALUE_OUT_OF_RANGE_ERROR: &str = "value out of range";
const TYPE_NOT_IMPLEMENTED: &str = "type is not implemented";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum QueryPlannerError {
    BucketIdError,
    DuplicateColumn,
    EmptyPlanRelations,
    IncorrectBucketIdError,
    InvalidAst,
    InvalidBool,
    InvalidColumnName,
    InvalidClusterSchema,
    InvalidInput,
    InvalidName,
    InvalidNode,
    InvalidNumber,
    InvalidPlan,
    InvalidReference,
    InvalidRelation,
    InvalidRow,
    InvalidSchemaSpaces,
    InvalidShardingKey,
    InvalidSpaceName,
    NotEqualRows,
    QueryNotImplemented,
    RequireMotion,
    Serialization,
    SimpleQueryError,
    SimpleUnionQueryError,
    SpaceFormatNotFound,
    SpaceNotFound,
    UninitializedDistribution,
    ValueOutOfRange,
    TypeNotImplemented,
}

impl fmt::Display for QueryPlannerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            QueryPlannerError::BucketIdError => BUCKET_ID_ERROR,
            QueryPlannerError::DuplicateColumn => DUPLICATE_COLUMN_ERROR,
            QueryPlannerError::EmptyPlanRelations => EMPTY_PLAN_RELATION,
            QueryPlannerError::IncorrectBucketIdError => INCORRECT_BUCKET_ID_ERROR,
            QueryPlannerError::InvalidAst => INVALID_AST,
            QueryPlannerError::InvalidBool => INVALID_BOOL_ERROR,
            QueryPlannerError::InvalidColumnName => INVALID_COLUMN_NAME,
            QueryPlannerError::InvalidClusterSchema => INVALID_CLUSTER_SCHEMA,
            QueryPlannerError::InvalidInput => INVALID_INPUT,
            QueryPlannerError::InvalidName => INVALID_NAME_ERROR,
            QueryPlannerError::InvalidNode => INVALID_NODE,
            QueryPlannerError::InvalidNumber => INVALID_NUMBER_ERROR,
            QueryPlannerError::InvalidPlan => INVALID_PLAN_ERROR,
            QueryPlannerError::InvalidReference => INVALID_REFERENCE,
            QueryPlannerError::InvalidRelation => INVALID_RELATION_ERROR,
            QueryPlannerError::InvalidRow => INVALID_ROW_ERROR,
            QueryPlannerError::InvalidSchemaSpaces => INVALID_SCHEMA_SPACES,
            QueryPlannerError::InvalidShardingKey => INVALID_SHARDING_KEY_ERROR,
            QueryPlannerError::InvalidSpaceName => INVALID_SPACE_NAME,
            QueryPlannerError::NotEqualRows => NOT_EQUAL_ROWS,
            QueryPlannerError::QueryNotImplemented => QUERY_NOT_IMPLEMENTED,
            QueryPlannerError::RequireMotion => REQUIRE_MOTION,
            QueryPlannerError::Serialization => SERIALIZATION_ERROR,
            QueryPlannerError::SimpleQueryError => SIMPLE_QUERY_ERROR,
            QueryPlannerError::SimpleUnionQueryError => SIMPLE_UNION_QUERY_ERROR,
            QueryPlannerError::SpaceFormatNotFound => SPACE_FORMAT_NOT_FOUND,
            QueryPlannerError::SpaceNotFound => SPACE_NOT_FOUND,
            QueryPlannerError::UninitializedDistribution => UNINITIALIZED_DISTRIBUTION,
            QueryPlannerError::ValueOutOfRange => VALUE_OUT_OF_RANGE_ERROR,
            QueryPlannerError::TypeNotImplemented => TYPE_NOT_IMPLEMENTED,
        };
        write!(f, "{}", p)
    }
}
