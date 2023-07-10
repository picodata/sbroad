use serde::Serialize;
use std::fmt;
use tarantool::transaction::TransactionError;

const DO_SKIP: &str = "do skip";

/// Reason or object of errors.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Entity {
    /// corresponding to enum Args
    Args,
    /// corresponding to struct AbstractSyntaxTree
    AST,
    /// corresponding to struct ParseNode
    ParseNode,
    /// corresponding to trait Aggregate
    Aggregate,
    /// corresponding to struct AggregateSignature
    AggregateSignature,
    /// corresponding to struct AggregateCollector
    AggregateCollector,
    /// corresponding to struct Buckets
    Buckets,
    /// raw bytes
    Bytes,
    /// general variant for cache
    Cache,
    /// corresponding to struct Chain
    Chain,
    /// cartridge cluster schema
    ClusterSchema,
    /// general variant
    Column,
    /// corresponds to enum Distribution
    Distribution,
    /// tarantool distribution key
    DistributionKey,
    /// tarantool engine (memtx, vinyl)
    Engine,
    /// corresponds to enum Expression
    Expression,
    /// corresponds to struct ExpressionMapper
    ExpressionMapper,
    /// corresponds to struct Histogram
    Histogram,
    /// tarantool index
    Index,
    /// tuple key definition
    KeyDef,
    /// corresponds to metadata field of struct ProducerResult
    Metadata,
    /// corresponds to enum MotionPolicy
    Motion,
    /// tarantool msgpack
    MsgPack,
    /// general variant for Name of some object
    Name,
    /// variant for node of tree
    Node,
    /// SQL operator
    Operator,
    /// corresponds to struct PatternWithParams
    PatternWithParams,
    /// corresponds to struct Plan
    Plan,
    /// corresponds to struct ProducerResult
    ProducerResult,
    /// SQL query
    Query,
    /// corresponds to enum Relational
    Relational,
    /// corresponds to struct RequiredData
    RequiredData,
    /// parser rule
    Rule,
    /// corresponds to struct RouterRuntime
    Runtime,
    /// Metadata schema
    Schema,
    /// sharding key of tarantool space
    ShardingKey,
    /// tarantool space
    Space,
    // tarantool space engine type
    SpaceEngine,
    /// tarantool space metadata
    SpaceMetadata,
    /// corresponds to Function structs
    SQLFunction,
    /// corresponds to struct Statement
    Statement,
    /// corresponds to CBO statistics
    Statistics,
    /// SQL sub-query
    SubQuery,
    /// sub-tree of the Plan
    SubTree,
    /// corresponds to sctruct SyntaxNode
    SyntaxNode,
    /// corresponds to struct SyntaxNodes
    SyntaxNodes,
    /// corresponds to struct SyntaxPlan
    SyntaxPlan,
    /// corresponds to struct Table
    Table,
    /// corresponds to struct Target
    Target,
    /// tarantool transaction
    Transaction,
    /// general variant for tuple
    Tuple,
    /// general variant for type of some object
    Type,
    /// general variant for value of some object
    Value,
    /// corresponds to struct VirtualTable
    VirtualTable,
}

impl fmt::Display for Entity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            Entity::Args => "args".to_string(),
            Entity::AST => "AST".to_string(),
            Entity::Aggregate => "aggregate".to_string(),
            Entity::AggregateCollector => "aggregate collector".to_string(),
            Entity::AggregateSignature => "aggregate signature".to_string(),
            Entity::Buckets => "buckets".to_string(),
            Entity::Bytes => "bytes".to_string(),
            Entity::Cache => "cache".to_string(),
            Entity::Chain => "chain".to_string(),
            Entity::ClusterSchema => "cluster schema".to_string(),
            Entity::Column => "column".to_string(),
            Entity::Distribution => "distribution".to_string(),
            Entity::DistributionKey => "distribution key".to_string(),
            Entity::Engine => "engine".to_string(),
            Entity::Expression => "expression".to_string(),
            Entity::ExpressionMapper => "expression mapper".to_string(),
            Entity::Histogram => "histogram".to_string(),
            Entity::Index => "index".to_string(),
            Entity::KeyDef => "key definition".to_string(),
            Entity::Metadata => "metadata".to_string(),
            Entity::Motion => "motion".to_string(),
            Entity::MsgPack => "msgpack".to_string(),
            Entity::Name => "name".to_string(),
            Entity::Node => "node".to_string(),
            Entity::Operator => "operator".to_string(),
            Entity::PatternWithParams => "pattern with parameters".to_string(),
            Entity::Plan => "plan".to_string(),
            Entity::ProducerResult => "producer result".to_string(),
            Entity::ParseNode => "parse node".to_string(),
            Entity::Query => "query".to_string(),
            Entity::Relational => "relational".to_string(),
            Entity::RequiredData => "required data".to_string(),
            Entity::Rule => "rule".to_string(),
            Entity::Runtime => "runtime".to_string(),
            Entity::Schema => "schema".to_string(),
            Entity::ShardingKey => "sharding key".to_string(),
            Entity::Space => "space".to_string(),
            Entity::SpaceEngine => "space engine".to_string(),
            Entity::SpaceMetadata => "space metadata".to_string(),
            Entity::SQLFunction => "SQL function".to_string(),
            Entity::Statement => "statement".to_string(),
            Entity::Statistics => "statistics".to_string(),
            Entity::SubQuery => "sub-query plan subtree".to_string(),
            Entity::SubTree => "execution plan subtree".to_string(),
            Entity::SyntaxNode => "syntax node".to_string(),
            Entity::SyntaxNodes => "syntax nodes".to_string(),
            Entity::SyntaxPlan => "syntax plan".to_string(),
            Entity::Table => "table".to_string(),
            Entity::Target => "target".to_string(),
            Entity::Transaction => "transaction".to_string(),
            Entity::Tuple => "tuple".to_string(),
            Entity::Type => "type".to_string(),
            Entity::Value => "value".to_string(),
            Entity::VirtualTable => "virtual table".to_string(),
        };
        write!(f, "{p}")
    }
}

/// Action that failed
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Action {
    Add,
    Borrow,
    Build,
    Clear,
    Create,
    Decode,
    Deserialize,
    Drop,
    Find,
    Get,
    Insert,
    Prepare,
    Put,
    Replace,
    Retrieve,
    Serialize,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            Action::Add => "add".to_string(),
            Action::Borrow => "borrow".to_string(),
            Action::Build => "build".to_string(),
            Action::Clear => "clear".to_string(),
            Action::Create => "create".to_string(),
            Action::Decode => "decode".to_string(),
            Action::Drop => "drop".to_string(),
            Action::Find => "find".to_string(),
            Action::Deserialize => "deserialize".to_string(),
            Action::Get => "get".to_string(),
            Action::Insert => "insert".to_string(),
            Action::Prepare => "prepare".to_string(),
            Action::Put => "put".to_string(),
            Action::Replace => "replace".to_string(),
            Action::Retrieve => "retrieve".to_string(),
            Action::Serialize => "serialize".to_string(),
        };
        write!(f, "{p}")
    }
}

/// Types of error
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum SbroadError {
    /// DoSkip is a special case of an error - nothing bad had happened, the target node doesn't contain
    /// anything interesting for us, skip it without any serious error.
    DoSkip,
    /// Some value that is considered to be unique is duplicated.
    /// Second param represents description.
    DuplicatedValue(String),
    /// Process of Action variant failed.
    /// Second param represents object of action.
    /// Third param represents reason of fail.
    FailedTo(Action, Option<Entity>, String),
    /// Object is invalid.
    /// Second param represents description and can be empty (None).
    Invalid(Entity, Option<String>),
    LuaError(String),
    /// Object not found.
    /// Second param represents description or name that let to identify object.
    NotFound(Entity, String),
    /// Object is not implemented yet.
    /// Second param represents description or name.
    NotImplemented(Entity, String),
    /// Error raised by object parsing.
    /// Second param represents error description.
    ParsingError(Entity, String),
    /// Unexpected number of values (list length etc.).
    /// Second param is information what was expected and what got.
    UnexpectedNumberOfValues(String),
    /// Object is not supported.
    /// Second param represents description or name that let to identify object.
    /// and can be empty (None).
    Unsupported(Entity, Option<String>),
}

impl fmt::Display for SbroadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let p = match self {
            SbroadError::DoSkip => DO_SKIP.to_string(),
            SbroadError::DuplicatedValue(s) => format!("duplicated value: {s}"),
            SbroadError::FailedTo(a, e, s) => match e {
                Some(entity) => format!("failed to {a} {entity}: {s}"),
                None => format!("failed to {a} {s}"),
            },
            SbroadError::Invalid(e, s) => match s {
                Some(msg) => format!("invalid {e}: {msg}"),
                None => format!("invalid {e}"),
            },
            SbroadError::NotFound(e, s) => format!("{e} {s} not found"),
            SbroadError::NotImplemented(e, s) => format!("{e} {s} not implemented"),
            SbroadError::ParsingError(e, s) => format!("{e} parsing error: {s}"),
            SbroadError::Unsupported(e, s) => match s {
                Some(msg) => format!("unsupported {e}: {msg}"),
                None => format!("unsupported {e}"),
            },
            SbroadError::UnexpectedNumberOfValues(s) => format!("unexpected number of values: {s}"),
            SbroadError::LuaError(e) => e.clone(),
        };

        write!(f, "{p}")
    }
}

impl std::error::Error for SbroadError {}

impl<E: fmt::Debug> From<TransactionError<E>> for SbroadError {
    fn from(error: TransactionError<E>) -> Self {
        SbroadError::FailedTo(
            Action::Create,
            Some(Entity::Transaction),
            format!("{error:?}"),
        )
    }
}
