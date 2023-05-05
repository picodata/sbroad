use std::{any::Any, cell::RefCell, rc::Rc};

use sbroad::{
    debug,
    errors::{Action, Entity, SbroadError},
    executor::{
        engine::{
            helpers::{
                compile_encoded_optional,
                storage::{
                    meta::StorageMetadata,
                    runtime::{
                        prepare, read_prepared, read_unprepared, unprepare, write_prepared,
                        write_unprepared,
                    },
                    PreparedStmt,
                },
            },
            QueryCache,
        },
        ir::QueryType,
        lru::{Cache, LRUCache, DEFAULT_CAPACITY},
        protocol::RequiredData,
    },
    warn,
};

thread_local!(static STATEMENT_CACHE: Rc<RefCell<LRUCache<String, PreparedStmt>>> = Rc::new(RefCell::new(LRUCache::new(DEFAULT_CAPACITY, Some(Box::new(unprepare))).unwrap())));

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    pub metadata: RefCell<StorageMetadata>,
    cache: Rc<RefCell<LRUCache<String, PreparedStmt>>>,
}

impl QueryCache for StorageRuntime {
    type Cache = LRUCache<String, PreparedStmt>;

    fn cache(&self) -> &RefCell<Self::Cache> {
        &self.cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self
            .cache()
            .try_borrow()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Borrow, Some(Entity::Cache), format!("{e:?}"))
            })?
            .capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format!("{e:?}"))
        })? = Self::Cache::new(DEFAULT_CAPACITY, None)?;
        Ok(())
    }
}

impl StorageRuntime {
    /// Build a new storage runtime.
    ///
    /// # Errors
    /// - Failed to initialize the LRU cache.
    pub fn new() -> Result<Self, SbroadError> {
        let runtime = STATEMENT_CACHE.with(|cache| StorageRuntime {
            metadata: RefCell::new(StorageMetadata::new()),
            cache: cache.clone(),
        });
        Ok(runtime)
    }

    /// Execute dispatched plan (divided into required and optional parts).
    ///
    /// # Errors
    /// - Something went wrong while executing the plan.
    #[allow(unused_variables)]
    pub fn execute_plan(
        &self,
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, SbroadError> {
        if required.can_be_cached {
            return self.execute_cacheable_plan(required, raw_optional);
        }
        Self::execute_non_cacheable_plan(required, raw_optional)
    }

    #[allow(unused_variables)]
    fn execute_non_cacheable_plan(
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let plan_id = required.plan_id.clone();

        if required.can_be_cached {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("Expected a plan that can not be cached.".to_string()),
            ));
        }

        let (pattern_with_params, _tmp_spaces) = compile_encoded_optional(raw_optional)?;
        debug!(
            Option::from("execute"),
            &format!(
                "Failed to execute the statement: {}",
                pattern_with_params.pattern
            ),
        );
        let result = if required.query_type == QueryType::DML {
            write_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
        } else {
            read_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
        };

        result
    }

    #[allow(unused_variables)]
    fn execute_cacheable_plan(
        &self,
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let plan_id = required.plan_id.clone();

        if !required.can_be_cached {
            return Err(SbroadError::Invalid(
                Entity::Plan,
                Some("Expected a plan that can be cached.".to_string()),
            ));
        }

        // Look for the prepared statement in the cache.
        if let Some(stmt) = self
            .cache
            .try_borrow_mut()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Borrow, Some(Entity::Cache), format!("{e}"))
            })?
            .get(&plan_id)?
        {
            let stmt_id = stmt.id()?;
            // The statement was found in the cache, so we can execute it.
            debug!(
                Option::from("execute plan"),
                &format!("Execute prepared statement: {stmt:?}"),
            );
            let result = match required.query_type {
                QueryType::DML => write_prepared(stmt_id, "", &required.parameters),
                QueryType::DQL => read_prepared(stmt_id, "", &required.parameters),
            };

            // If prepared statement is invalid for some reason, fallback to the long pass
            // and recompile the query.
            if result.is_ok() {
                return result;
            }
        }
        debug!(
            Option::from("execute plan"),
            &format!("Failed to find a plan (id {plan_id}) in the cache."),
        );

        let (pattern_with_params, _tmp_spaces) = compile_encoded_optional(raw_optional)?;
        let result = match prepare(&pattern_with_params.pattern) {
            Ok(stmt) => {
                let stmt_id = stmt.id()?;
                debug!(
                    Option::from("execute plan"),
                    &format!(
                        "Created prepared statement {} for the pattern {}",
                        stmt_id,
                        stmt.pattern()?
                    ),
                );
                self.cache
                    .try_borrow_mut()
                    .map_err(|e| {
                        SbroadError::FailedTo(
                            Action::Put,
                            None,
                            format!("prepared statement {stmt:?} into the cache: {e:?}"),
                        )
                    })?
                    .put(plan_id, stmt)?;
                // The statement was found in the cache, so we can execute it.
                debug!(
                    Option::from("execute plan"),
                    &format!("Execute prepared statement: {stmt_id}"),
                );
                if required.query_type == QueryType::DML {
                    write_prepared(
                        stmt_id,
                        &pattern_with_params.pattern,
                        &pattern_with_params.params,
                    )
                } else {
                    read_prepared(
                        stmt_id,
                        &pattern_with_params.pattern,
                        &pattern_with_params.params,
                    )
                }
            }
            Err(e) => {
                // Possibly the statement is correct, but doesn't fit into
                // Tarantool's prepared statements cache (`sql_cache_size`).
                // So we try to execute it bypassing the cache.
                warn!(
                    Option::from("execute"),
                    &format!(
                        "Failed to prepare the statement: {}, error: {e}",
                        pattern_with_params.pattern
                    ),
                );
                if required.query_type == QueryType::DML {
                    write_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
                } else {
                    read_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
                }
            }
        };

        result
    }
}
