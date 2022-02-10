use std::convert::TryInto;

use tarantool::log::{say, SayLevel};

use crate::errors::QueryPlannerError;
use crate::executor::engine::Engine;
pub use crate::executor::engine::Metadata;
use crate::executor::result::BoxExecuteFormat;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::Plan;

pub mod engine;
pub mod result;
pub mod shard;

/// Query object for executing
#[allow(dead_code)]
pub struct Query<T>
where
    T: Engine,
{
    /// Query IR
    plan: Plan,
    /// Execute engine object
    engine: T,
}

#[allow(dead_code)]
impl<T> Query<T>
where
    T: Engine,
{
    /// Create query object
    ///
    /// # Errors
    /// - query isn't valid or not support yet.
    pub fn new(engine: T, sql: &str) -> Result<Self, QueryPlannerError>
    where
        T::Metadata: Metadata,
    {
        let ast = AbstractSyntaxTree::new(sql)?;
        Ok(Query {
            plan: ast.to_ir(&engine.metadata())?,
            engine,
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

        let mut result = BoxExecuteFormat::new();
        let sql = &self.plan.subtree_as_sql(top)?;

        say(
            SayLevel::Debug,
            file!(),
            line!().try_into().unwrap_or(0),
            None,
            &format!("query: {}", sql),
        );

        if let Some(shard_keys) = self.plan.get_sharding_keys(top)? {
            say(
                SayLevel::Debug,
                file!(),
                line!().try_into().unwrap_or(0),
                None,
                "distribution keys were found",
            );

            // sending query to nodes
            for shard in shard_keys {
                // exec query on node
                let temp_result = match self.engine.exec_query(&shard, sql) {
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

            let temp_result = match self.engine.mp_exec_query(sql) {
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
