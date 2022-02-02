use std::convert::TryInto;

use tarantool::log::{say, SayLevel};

use crate::cache::Metadata;
use crate::errors::QueryPlannerError;
use crate::executor::result::BoxExecuteFormat;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::Plan;
use crate::lua_bridge::{bucket_count, exec_query, mp_exec_query};

pub mod result;
pub mod shard;

/// Query object for executing
#[allow(dead_code)]
pub struct Query {
    /// Query IR
    plan: Plan,
    /// Number of buckets in cluster
    bucket_count: u64,
}

#[allow(dead_code)]
impl Query {
    /// Create query object
    ///
    /// # Errors
    /// - query isn't valid or not support yet.
    pub fn new(metadata: &Metadata, sql: &str) -> Result<Self, QueryPlannerError> {
        let bucket_count = match bucket_count() {
            Ok(bc) => bc,
            Err(e) => {
                say(
                    SayLevel::Error,
                    "executor.rs",
                    line!().try_into().unwrap_or(0),
                    None,
                    &format!("{:?}", e),
                );
                return Err(QueryPlannerError::CustomError(String::from("Lua error")));
            }
        };

        let ast = AbstractSyntaxTree::new(sql)?;
        Ok(Query {
            plan: ast.to_ir(metadata)?,
            bucket_count,
        })
    }

    /// Execute query in cluster
    ///
    /// # Errors
    /// - query can't be executed in cluster
    /// - invalid bucket id
    pub fn exec(&mut self) -> Result<BoxExecuteFormat, QueryPlannerError> {
        say(
            SayLevel::Debug,
            file!(),
            line!().try_into().unwrap_or(0),
            None,
            "start query execute",
        );

        let top = self.plan.get_top()?;

        let cast_bucket_count: usize = match self.bucket_count.try_into() {
            Ok(v) => v,
            Err(_) => {
                return Err(QueryPlannerError::CustomError(
                    "Invalid bucket_count".into(),
                ));
            }
        };
        let mut result = BoxExecuteFormat::new();
        let sql = &self.plan.subtree_as_sql(top)?;
        say(
            SayLevel::Debug,
            "executor.rs",
            line!().try_into().unwrap_or(0),
            None,
            &format!("query: {}", sql),
        );

        if let Some(bucket_ids) = self.plan.get_bucket_ids(top, cast_bucket_count)? {
            say(
                SayLevel::Debug,
                file!(),
                line!().try_into().unwrap_or(0),
                None,
                "distribution keys were found",
            );

            // sending query to nodes
            for bucket_id in bucket_ids {
                // exec query on node
                let cast_bucket_id: u64 = match bucket_id.try_into() {
                    Ok(v) => v,
                    Err(_) => {
                        return Err(QueryPlannerError::CustomError("Invalid bucket id".into()));
                    }
                };

                let temp_result = match exec_query(cast_bucket_id, sql) {
                    Ok(r) => r,
                    Err(e) => {
                        say(
                            SayLevel::Error,
                            file!(),
                            line!().try_into().unwrap_or(0),
                            None,
                            &format!("{:?}", e),
                        );
                        return Err(QueryPlannerError::LuaError(e.to_string()));
                    }
                };

                result.extend(temp_result)?;
            }
        } else {
            say(
                SayLevel::Debug,
                file!(),
                line!().try_into().unwrap_or(0),
                None,
                "distribution keys were not found",
            );

            let temp_result = match mp_exec_query(sql) {
                Ok(r) => r,
                Err(e) => {
                    say(
                        SayLevel::Error,
                        file!(),
                        line!().try_into().unwrap_or(0),
                        None,
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(e.to_string()));
                }
            };

            result.extend(temp_result)?;
        }
        Ok(result)
    }

    /// Apply optimize rules
    ///
    /// # Errors
    /// - transformation can't be applied
    pub fn optimize(&mut self) -> Result<(), QueryPlannerError> {
        self.plan.add_motions()?;
        Ok(())
    }
}
