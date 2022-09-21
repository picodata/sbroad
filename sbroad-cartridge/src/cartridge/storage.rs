use crate::cartridge::config::StorageConfiguration;
use crate::cartridge::update_tracing;
use sbroad::errors::QueryPlannerError;
use sbroad::executor::engine::Configuration;
use sbroad::executor::lru::{Cache, LRUCache, DEFAULT_CAPACITY};
use sbroad::ir::value::Value;
use sbroad::otm::child_span;
use sbroad::{debug, error, warn};
use sbroad_proc::otm_child_span;
use std::any::Any;
use std::cell::RefCell;
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
    cache: RefCell<LRUCache<u32, PreparedStmt>>,
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
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                }
            };
            let storage_capacity = usize::try_from(capacity)
                .map_err(|e| QueryPlannerError::CustomError(format!("{:?}", e)))?;

            let storage_cache_size_bytes: LuaFunction<_> =
                lua.eval("return get_storage_cache_size_bytes;").unwrap();
            let cache_size_bytes = match storage_cache_size_bytes.call::<u64>() {
                Ok(size_bytes) => size_bytes,
                Err(e) => {
                    error!(
                        Option::from("getting storage cache size bytes"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                }
            };
            let storage_size_bytes = usize::try_from(cache_size_bytes)
                .map_err(|e| QueryPlannerError::CustomError(format!("{}", e)))?;

            let jaeger_agent_host: LuaFunction<_> =
                lua.eval("return get_jaeger_agent_host;").unwrap();
            let jaeger_host: String = match jaeger_agent_host.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(
                        Option::from("getting jaeger agent host"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                }
            };

            let jaeger_agent_port: LuaFunction<_> =
                lua.eval("return get_jaeger_agent_port;").unwrap();
            let jaeger_port: u16 = match jaeger_agent_port.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(
                        Option::from("getting jaeger agent port"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
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
    }
}

impl StorageRuntime {
    /// Build a new storage runtime.
    ///
    /// # Errors
    /// - Failed to initialize the LRU cache.
    pub fn new() -> Result<Self, QueryPlannerError> {
        let cache: LRUCache<u32, PreparedStmt> =
            LRUCache::new(DEFAULT_CAPACITY, Some(Box::new(unprepare)))?;
        let result = StorageRuntime {
            metadata: StorageConfiguration::new(),
            cache: RefCell::new(cache),
        };

        Ok(result)
    }

    /// Put a prepared statement into the cache and execute it.
    ///
    /// # Errors
    /// - Failed to prepare the statement (invalid SQL or lack of memory in `sql_cache_size`).
    /// - Failed to put or get a prepared statement from the cache.
    /// - Failed to execute the prepared statement.
    #[allow(unused_variables)]
    pub fn execute(
        &self,
        pattern: &str,
        params: &[Value],
        is_data_modifier: bool,
    ) -> Result<Box<dyn Any>, QueryPlannerError> {
        // Find a statement in the Tarantool's cache or prepare it
        // (i.e. compile and put into the cache).
        let stmt_id = match prepare(pattern) {
            Ok(stmt) => {
                let stmt_id = stmt.id()?;
                debug!(
                    Option::from("execute"),
                    &format!("Created prepared statement {}", stmt_id),
                );
                self.cache
                    .try_borrow_mut()
                    .map_err(|e| {
                        QueryPlannerError::CustomError(format!(
                            "Failed to put prepared statement {:?} into the cache: {:?}",
                            stmt, e
                        ))
                    })?
                    .put(stmt_id, stmt)?;
                stmt_id
            }
            Err(e) => {
                // Possibly the statement is correct, but doesn't fit into
                // Tarantool's prepared statements cache (`sql_cache_size`).
                // So we try to execute it bypassing the cache.
                warn!(
                    Option::from("execute"),
                    &format!("Failed to prepare the statement: {}, error: {}", pattern, e),
                );
                if is_data_modifier {
                    return write_unprepared(pattern, params);
                }
                return read_unprepared(pattern, params);
            }
        };

        // The statement was found in the cache, so we can execute it.
        debug!(
            Option::from("execute"),
            &format!("Execute prepared statement {}", stmt_id),
        );
        if is_data_modifier {
            return write_prepared(stmt_id, pattern, params);
        }
        read_prepared(stmt_id, pattern, params)
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
            error!(Option::from("prepare"), &format!("{:?}", e));
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
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
            error!(Option::from("unprepare"), &format!("{:?}", e));
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
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
            error!(Option::from("read_prepared"), &format!("{:?}", e));
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
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
            error!(Option::from("read_unprepared"), &format!("{:?}", e));
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
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
            error!(Option::from("write_prepared"), &format!("{:?}", e));
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
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
            error!(Option::from("write_unprepared"), &format!("{:?}", e));
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
        }
    }
}
