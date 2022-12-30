use crate::api::exec_query::protocol::{EncodedOptionalData, OptionalData, RequiredData};
use crate::cartridge::config::StorageConfiguration;
use crate::cartridge::update_tracing;
use sbroad::errors::QueryPlannerError;
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::Configuration;
use sbroad::executor::ir::QueryType;
use sbroad::executor::lru::{Cache, LRUCache, DEFAULT_CAPACITY};
use sbroad::ir::value::Value;
use sbroad::otm::child_span;
use sbroad::{debug, error, warn};
use sbroad_proc::otm_child_span;
use std::any::Any;
use std::cell::RefCell;
use std::fmt::Display;
use tarantool::tlua::LuaFunction;
use tarantool::tuple::Tuple;

struct Statement {
    id: u32,
    pattern: String,
}

#[derive(Default)]
struct PreparedStmt(Option<Statement>);

impl PreparedStmt {
    /// Extract prepared statement from the cache.
    ///
    /// # Errors
    /// - Returns None instead of a regular statement (sentinel node in the cache).
    fn statement(&self) -> Result<&Statement, QueryPlannerError> {
        self.0
            .as_ref()
            .ok_or_else(|| QueryPlannerError::CustomError("Statement is not prepared".to_string()))
    }

    fn id(&self) -> Result<u32, QueryPlannerError> {
        Ok(self.statement()?.id)
    }

    fn pattern(&self) -> Result<&str, QueryPlannerError> {
        Ok(&self.statement()?.pattern)
    }
}

impl std::fmt::Debug for PreparedStmt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref stmt) = self.0 {
            write!(f, "PreparedStmt {:?}", stmt.pattern)
        } else {
            write!(f, "PreparedStmt None")
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct StorageRuntime {
    metadata: StorageConfiguration,
    cache: RefCell<LRUCache<String, PreparedStmt>>,
}

impl Configuration for StorageRuntime {
    type Configuration = StorageConfiguration;

    fn cached_config(&self) -> &Self::Configuration {
        &self.metadata
    }

    fn clear_config(&mut self) {
        self.metadata = StorageConfiguration::default();
    }

    fn is_config_empty(&self) -> bool {
        self.metadata.is_empty()
    }

    fn get_config(&self) -> Result<Option<Self::Configuration>, QueryPlannerError> {
        if self.is_config_empty() {
            let lua = tarantool::lua_state();

            let storage_cache_capacity: LuaFunction<_> =
                lua.eval("return get_storage_cache_capacity;").unwrap();
            let capacity: u64 = match storage_cache_capacity.call() {
                Ok(capacity) => capacity,
                Err(e) => {
                    error!(
                        Option::from("getting storage cache capacity"),
                        &format!("{e:?}"),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")));
                }
            };
            let storage_capacity = usize::try_from(capacity)
                .map_err(|e| QueryPlannerError::CustomError(format!("{e:?}")))?;

            let storage_cache_size_bytes: LuaFunction<_> =
                lua.eval("return get_storage_cache_size_bytes;").unwrap();
            let cache_size_bytes = match storage_cache_size_bytes.call::<u64>() {
                Ok(size_bytes) => size_bytes,
                Err(e) => {
                    error!(
                        Option::from("getting storage cache size bytes"),
                        &format!("{e:?}"),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")));
                }
            };
            let storage_size_bytes = usize::try_from(cache_size_bytes)
                .map_err(|e| QueryPlannerError::CustomError(format!("{e}")))?;

            let jaeger_agent_host: LuaFunction<_> =
                lua.eval("return get_jaeger_agent_host;").unwrap();
            let jaeger_host: String = match jaeger_agent_host.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting jaeger agent host"), &format!("{e:?}"),);
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")));
                }
            };

            let jaeger_agent_port: LuaFunction<_> =
                lua.eval("return get_jaeger_agent_port;").unwrap();
            let jaeger_port: u16 = match jaeger_agent_port.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting jaeger agent port"), &format!("{e:?}"),);
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")));
                }
            };

            let mut metadata = StorageConfiguration::new();
            metadata.storage_capacity = storage_capacity;
            metadata.storage_size_bytes = storage_size_bytes;
            metadata.jaeger_agent_host = jaeger_host;
            metadata.jaeger_agent_port = jaeger_port;
            update_tracing(&metadata.jaeger_agent_host, metadata.jaeger_agent_port)?;

            return Ok(Some(metadata));
        }
        Ok(None)
    }

    fn update_config(&mut self, metadata: Self::Configuration) {
        self.metadata = metadata;
        update_box_param("sql_cache_size", self.metadata.storage_size_bytes);
    }
}

impl StorageRuntime {
    /// Build a new storage runtime.
    ///
    /// # Errors
    /// - Failed to initialize the LRU cache.
    pub fn new() -> Result<Self, QueryPlannerError> {
        let cache: LRUCache<String, PreparedStmt> =
            LRUCache::new(DEFAULT_CAPACITY, Some(Box::new(unprepare)))?;
        let result = StorageRuntime {
            metadata: StorageConfiguration::new(),
            cache: RefCell::new(cache),
        };

        Ok(result)
    }

    #[allow(unused_variables)]
    pub fn execute_plan(
        &self,
        required: &mut RequiredData,
        raw_optional: &mut Vec<u8>,
    ) -> Result<Box<dyn Any>, QueryPlannerError> {
        let plan_id = required.plan_id.clone();

        // Use all buckets as we don't want to filter any data from the execution plan
        // (this work has already been done on the coordinator).
        let buckets = Buckets::All;

        // Look for the prepared statement in the cache.
        if required.can_be_cached {
            if let Some(stmt) = self
                .cache
                .try_borrow_mut()
                .map_err(|e| {
                    QueryPlannerError::CustomError(format!("Failed to borrow cache: {e}"))
                })?
                .get(&plan_id)?
            {
                let stmt_id = stmt.id()?;
                // The statement was found in the cache, so we can execute it.
                debug!(
                    Option::from("execute plan"),
                    &format!("Execute prepared statement {stmt:?}"),
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
        }

        // Find a statement in the Tarantool's cache or prepare it
        // (i.e. compile and put into the cache).
        let data = std::mem::take(raw_optional);
        let mut optional = OptionalData::try_from(EncodedOptionalData::from(data))?;
        optional.exec_plan.get_mut_ir_plan().restore_constants()?;
        let nodes = optional.ordered.to_syntax_data()?;
        let pattern_with_params = optional.exec_plan.to_sql(&nodes, &buckets)?;
        match prepare(&pattern_with_params.pattern) {
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
                if required.can_be_cached {
                    self.cache
                        .try_borrow_mut()
                        .map_err(|e| {
                            QueryPlannerError::CustomError(format!(
                                "Failed to put prepared statement {:?} into the cache: {:?}",
                                stmt, e
                            ))
                        })?
                        .put(plan_id, stmt)?;
                }
                // The statement was found in the cache, so we can execute it.
                debug!(
                    Option::from("execute plan"),
                    &format!("Execute prepared statement {stmt_id}"),
                );
                if required.query_type == QueryType::DML {
                    return write_prepared(
                        stmt_id,
                        &pattern_with_params.pattern,
                        &pattern_with_params.params,
                    );
                }
                read_prepared(
                    stmt_id,
                    &pattern_with_params.pattern,
                    &pattern_with_params.params,
                )
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
                    return write_unprepared(
                        &pattern_with_params.pattern,
                        &pattern_with_params.params,
                    );
                }
                read_unprepared(&pattern_with_params.pattern, &pattern_with_params.params)
            }
        }
    }
}

#[otm_child_span("tarantool.statement.prepare")]
fn prepare(pattern: &str) -> Result<PreparedStmt, QueryPlannerError> {
    let lua = tarantool::lua_state();

    let prepare_stmt: LuaFunction<_> = lua
        .get("prepare")
        .ok_or_else(|| QueryPlannerError::LuaError("Lua function `prepare` not found".into()))?;

    match prepare_stmt.call_with_args::<u32, _>(pattern) {
        Ok(stmt_id) => {
            let stmt = Statement {
                id: stmt_id,
                pattern: pattern.to_string(),
            };
            Ok(PreparedStmt(Some(stmt)))
        }
        Err(e) => {
            error!(Option::from("prepare"), &format!("{e:?}"));
            Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.unprepare")]
fn unprepare(stmt: &mut PreparedStmt) -> Result<(), QueryPlannerError> {
    let lua = tarantool::lua_state();

    let unprepare_stmt: LuaFunction<_> = lua
        .get("unprepare")
        .ok_or_else(|| QueryPlannerError::LuaError("Lua function `unprepare` not found".into()))?;

    match unprepare_stmt.call_with_args::<(), _>(stmt.id()?) {
        Ok(_) => Ok(()),
        Err(e) => {
            error!(Option::from("unprepare"), &format!("{e:?}"));
            Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.prepared.read")]
fn read_prepared(
    stmt_id: u32,
    stmt: &str,
    params: &[Value],
) -> Result<Box<dyn Any>, QueryPlannerError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("read")
        .ok_or_else(|| QueryPlannerError::LuaError("Lua function `read` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((stmt_id, stmt, params)) {
        Ok(v) => Ok(Box::new(v) as Box<dyn Any>),
        Err(e) => {
            error!(Option::from("read_prepared"), &format!("{e:?}"));
            Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.unprepared.read")]
fn read_unprepared(stmt: &str, params: &[Value]) -> Result<Box<dyn Any>, QueryPlannerError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("read")
        .ok_or_else(|| QueryPlannerError::LuaError("Lua function `read` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((0, stmt, params)) {
        Ok(v) => Ok(Box::new(v) as Box<dyn Any>),
        Err(e) => {
            error!(Option::from("read_unprepared"), &format!("{e:?}"));
            Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.prepared.write")]
fn write_prepared(
    stmt_id: u32,
    stmt: &str,
    params: &[Value],
) -> Result<Box<dyn Any>, QueryPlannerError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("write")
        .ok_or_else(|| QueryPlannerError::LuaError("Lua function `write` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((stmt_id, stmt, params)) {
        Ok(v) => Ok(Box::new(v) as Box<dyn Any>),
        Err(e) => {
            error!(Option::from("write_prepared"), &format!("{e:?}"));
            Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.unprepared.write")]
fn write_unprepared(stmt: &str, params: &[Value]) -> Result<Box<dyn Any>, QueryPlannerError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("write")
        .ok_or_else(|| QueryPlannerError::LuaError("Lua function `write` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((0, stmt, params)) {
        Ok(v) => Ok(Box::new(v) as Box<dyn Any>),
        Err(e) => {
            error!(Option::from("write_unprepared"), &format!("{e:?}"));
            Err(QueryPlannerError::LuaError(format!("Lua error: {e:?}")))
        }
    }
}

fn update_box_param<T>(param: &str, val: T)
where
    T: Display,
{
    let lua = tarantool::lua_state();
    match lua.exec(&format!("box.cfg{{{param} = {val}}}")) {
        Ok(_) => debug!(
            Option::from("update_box_param"),
            &format!("box.cfg param {param} was updated to {val}")
        ),
        Err(e) => warn!(
            Option::from("update_box_param"),
            &format!("box.cfg update error: {e}")
        ),
    }
}
