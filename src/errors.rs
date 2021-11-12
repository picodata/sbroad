use serde::Serialize;
use std::fmt;

const SIMPLE_QUERY_ERROR: &str = "query doesn't simple";
const SIMPLE_UNION_QUERY_ERROR: &str = "query doesn't simple union";
const QUERY_NOT_IMPLEMENTED: &str = "query wasn't s implemented";
const BUCKET_ID_ERROR: &str = "field doesn't contains sharding key value";

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum QueryPlannerError {
    SimpleQueryError,
    SimpleUnionQueryError,
    QueryNotImplemented,
    BucketIdError,
}

impl fmt::Display for QueryPlannerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            QueryPlannerError::SimpleQueryError => SIMPLE_QUERY_ERROR,
            QueryPlannerError::SimpleUnionQueryError => SIMPLE_UNION_QUERY_ERROR,
            QueryPlannerError::QueryNotImplemented => QUERY_NOT_IMPLEMENTED,
            QueryPlannerError::BucketIdError => BUCKET_ID_ERROR,
        };
        write!(f, "{}", p)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SimpleQueryError;

impl fmt::Display for SimpleQueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", SIMPLE_QUERY_ERROR)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SimpleUnionQueryError;

impl fmt::Display for SimpleUnionQueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", SIMPLE_UNION_QUERY_ERROR)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QueryNotImplemented;

impl fmt::Display for QueryNotImplemented {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", QUERY_NOT_IMPLEMENTED)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BucketIdError;

impl fmt::Display for BucketIdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", BUCKET_ID_ERROR)
    }
}
