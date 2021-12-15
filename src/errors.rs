use std::fmt;

use serde::Serialize;

const BUCKET_ID_ERROR: &str = "field doesn't contains sharding key value";
const DUPLICATE_COLUMN_ERROR: &str = "duplicate column";
const EMPTY_PLAN_RELATION: &str = "empty plan relations";
const INCORRECT_BUCKET_ID_ERROR: &str = "incorrect bucket id";
const INVALID_BOOL_ERROR: &str = "invalid boolean";
const INVALID_INPUT: &str = "invalid input";
const INVALID_NAME_ERROR: &str = "invalid name";
const INVALID_NODE: &str = "invalid node";
const INVALID_NUMBER_ERROR: &str = "invalid number";
const INVALID_PLAN_ERROR: &str = "invalid plan";
const INVALID_REFERENCE: &str = "invalid reference";
const INVALID_RELATION_ERROR: &str = "invalid relation";
const INVALID_ROW_ERROR: &str = "invalid row";
const INVALID_SHARDING_KEY_ERROR: &str = "invalid sharding key";
const NOT_EQUAL_ROWS: &str = "not equal rows";
const QUERY_NOT_IMPLEMENTED: &str = "query wasn't s implemented";
const REQUIRE_MOTION: &str = "require motion";
const SERIALIZATION_ERROR: &str = "serialization";
const SIMPLE_QUERY_ERROR: &str = "query doesn't simple";
const SIMPLE_UNION_QUERY_ERROR: &str = "query doesn't simple union";
const UNINITIALIZED_DISTRIBUTION: &str = "uninitialized distribution";
const VALUE_OUT_OF_RANGE_ERROR: &str = "value out of range";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum QueryPlannerError {
    BucketIdError,
    DuplicateColumn,
    EmptyPlanRelations,
    IncorrectBucketIdError,
    InvalidBool,
    InvalidInput,
    InvalidName,
    InvalidNode,
    InvalidNumber,
    InvalidPlan,
    InvalidReference,
    InvalidRelation,
    InvalidRow,
    InvalidShardingKey,
    NotEqualRows,
    QueryNotImplemented,
    RequireMotion,
    Serialization,
    SimpleQueryError,
    SimpleUnionQueryError,
    UninitializedDistribution,
    ValueOutOfRange,
}

impl fmt::Display for QueryPlannerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            QueryPlannerError::BucketIdError => BUCKET_ID_ERROR,
            QueryPlannerError::DuplicateColumn => DUPLICATE_COLUMN_ERROR,
            QueryPlannerError::EmptyPlanRelations => EMPTY_PLAN_RELATION,
            QueryPlannerError::IncorrectBucketIdError => INCORRECT_BUCKET_ID_ERROR,
            QueryPlannerError::InvalidBool => INVALID_BOOL_ERROR,
            QueryPlannerError::InvalidInput => INVALID_INPUT,
            QueryPlannerError::InvalidName => INVALID_NAME_ERROR,
            QueryPlannerError::InvalidNode => INVALID_NODE,
            QueryPlannerError::InvalidNumber => INVALID_NUMBER_ERROR,
            QueryPlannerError::InvalidPlan => INVALID_PLAN_ERROR,
            QueryPlannerError::InvalidReference => INVALID_REFERENCE,
            QueryPlannerError::InvalidRelation => INVALID_RELATION_ERROR,
            QueryPlannerError::InvalidRow => INVALID_ROW_ERROR,
            QueryPlannerError::InvalidShardingKey => INVALID_SHARDING_KEY_ERROR,
            QueryPlannerError::NotEqualRows => NOT_EQUAL_ROWS,
            QueryPlannerError::QueryNotImplemented => QUERY_NOT_IMPLEMENTED,
            QueryPlannerError::RequireMotion => REQUIRE_MOTION,
            QueryPlannerError::Serialization => SERIALIZATION_ERROR,
            QueryPlannerError::SimpleQueryError => SIMPLE_QUERY_ERROR,
            QueryPlannerError::SimpleUnionQueryError => SIMPLE_UNION_QUERY_ERROR,
            QueryPlannerError::UninitializedDistribution => UNINITIALIZED_DISTRIBUTION,
            QueryPlannerError::ValueOutOfRange => VALUE_OUT_OF_RANGE_ERROR,
        };
        write!(f, "{}", p)
    }
}
