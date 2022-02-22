use crate::errors::QueryPlannerError;
use crate::executor::result::BoxExecuteFormat;

pub mod cartridge;

/// `Metadata` trait is interface for working with metadata storage, that was needed the query execute.
pub trait Metadata {
    fn get_table_segment(
        &self,
        table_name: &str,
    ) -> Result<crate::ir::relation::Table, QueryPlannerError>;

    fn to_name(s: &str) -> String {
        if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
            s.to_string()
        } else if s.to_uppercase() == s {
            s.to_lowercase()
        } else {
            format!("\"{}\"", s)
        }
    }
}

/// `Engine` trait is interface for working with execution engine.
pub trait Engine {
    type Metadata;

    /// Return object of metadata storage
    fn metadata(&self) -> Self::Metadata
    where
        Self: Sized;

    /// Checking that metadata isn't empty
    fn has_metadata(&self) -> bool;

    /// Clear metadata information
    fn clear_metadata(&mut self);

    /// Load metadate information to storage
    fn load_metadata(&mut self) -> Result<(), QueryPlannerError>;

    /// Execute sql query on the particular shard
    ///
    /// # Errors
    /// - internal executor errors
    fn exec_query(
        &self,
        shard_key: &str,
        query: &str,
    ) -> Result<BoxExecuteFormat, QueryPlannerError>;

    /// Execute sql query on the all shards in cluster
    ///
    /// # Errors
    /// - internal executor errors
    fn mp_exec_query(&self, query: &str) -> Result<BoxExecuteFormat, QueryPlannerError>;

    /// Determine shard for query execution by sharding key value
    fn determine_bucket_id(&self, s: &str) -> usize;
}

#[cfg(test)]
pub mod mock;
