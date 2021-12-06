use std::fmt;

use serde::Serialize;

const BUCKET_ID_ERROR: &str = "field doesn't contains sharding key value";
const DUPLICATE_COLUMN_ERROR: &str = "duplicate column";
const INVALID_NUMBER_ERROR: &str = "invalid number";
const INVALID_PLAN_ERROR: &str = "invalid plan";
const INVALID_RELATION_ERROR: &str = "invalid relation";
const INVALID_ROW_ERROR: &str = "invalid row";
const INVALID_SHARDING_KEY_ERROR: &str = "invalid sharding key";
const SERIALIZATION_ERROR: &str = "serialization";
const SIMPLE_QUERY_ERROR: &str = "query doesn't simple";
const SIMPLE_UNION_QUERY_ERROR: &str = "query doesn't simple union";
const QUERY_NOT_IMPLEMENTED: &str = "query wasn't s implemented";
const VALUE_OUT_OF_RANGE_ERROR: &str = "value out of range";
const INCORRECT_BUCKET_ID_ERROR: &str = "incorrect bucket id";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum QueryPlannerError {
    BucketIdError,
    DuplicateColumn,
    InvalidNumber,
    InvalidPlan,
    InvalidRelation,
    InvalidRow,
    InvalidShardingKey,
    Serialization,
    SimpleQueryError,
    SimpleUnionQueryError,
    QueryNotImplemented,
    ValueOutOfRange,
    IncorrectBucketIdError,
}

impl fmt::Display for QueryPlannerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            QueryPlannerError::BucketIdError => BUCKET_ID_ERROR,
            QueryPlannerError::DuplicateColumn => DUPLICATE_COLUMN_ERROR,
            QueryPlannerError::InvalidNumber => INVALID_NUMBER_ERROR,
            QueryPlannerError::InvalidPlan => INVALID_PLAN_ERROR,
            QueryPlannerError::InvalidRelation => INVALID_RELATION_ERROR,
            QueryPlannerError::InvalidRow => INVALID_ROW_ERROR,
            QueryPlannerError::InvalidShardingKey => INVALID_SHARDING_KEY_ERROR,
            QueryPlannerError::Serialization => SERIALIZATION_ERROR,
            QueryPlannerError::SimpleQueryError => SIMPLE_QUERY_ERROR,
            QueryPlannerError::SimpleUnionQueryError => SIMPLE_UNION_QUERY_ERROR,
            QueryPlannerError::QueryNotImplemented => QUERY_NOT_IMPLEMENTED,
            QueryPlannerError::ValueOutOfRange => VALUE_OUT_OF_RANGE_ERROR,
            QueryPlannerError::IncorrectBucketIdError => INCORRECT_BUCKET_ID_ERROR,
        };
        write!(f, "{}", p)
    }
}
