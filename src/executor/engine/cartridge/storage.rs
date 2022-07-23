use crate::errors::QueryPlannerError;
use crate::executor::engine::cartridge::config::StorageConfiguration;
use crate::executor::engine::Configuration;
use crate::executor::lru::{Cache, LRUCache, DEFAULT_CAPACITY};
use crate::ir::value::Value;
use std::any::Any;
use std::cell::RefCell;
use tarantool::log::{say, SayLevel};
use tarantool::tlua::LuaFunction;
use tarantool::tuple::Tuple;

pub const DEFAULT_SIZE_BYTES: usize = 10 * 1024 * 1024;

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
                    say(
                        SayLevel::Error,
                        file!(),
                        line!().try_into().unwrap_or(0),
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
                    say(
                        SayLevel::Error,
                        file!(),
                        line!().try_into().unwrap_or(0),
                        Option::from("getting storage cache size bytes"),
                        &format!("{:?}", e),
                    );
                    return Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)));
                }
            };
            let storage_size_bytes = usize::try_from(cache_size_bytes)
                .map_err(|e| QueryPlannerError::CustomError(format!("{}", e)))?;

            let mut metadata = StorageConfiguration::new();
            metadata.storage_capacity = storage_capacity;
            metadata.storage_size_bytes = storage_size_bytes;

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
                say(
                    SayLevel::Debug,
                    file!(),
                    line!().try_into().unwrap_or(0),
                    Option::from("execute"),
                    &format!("Created prepared statement {}", stmt_id),
                );
                self.cache.borrow_mut().put(stmt_id, stmt)?;
                stmt_id
            }
            Err(e) => {
                // Possibly the statement is correct, but doesn't fit into
                // Tarantool's prepared statements cache (`sql_cache_size`).
                // So we try to execute it bypassing the cache.
                say(
                    SayLevel::Warn,
                    file!(),
                    line!().try_into().unwrap_or(0),
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
        say(
            SayLevel::Debug,
            file!(),
            line!().try_into().unwrap_or(0),
            Option::from("execute"),
            &format!("Execute prepared statement {}", stmt_id),
        );
        if is_data_modifier {
            return write_prepared(stmt_id, pattern, params);
        }
        read_prepared(stmt_id, pattern, params)
    }
}

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
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("prepare"),
                &format!("{:?}", e),
            );
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
        }
    }
}

fn unprepare(stmt: &mut PreparedStmt) -> Result<(), QueryPlannerError> {
    let lua = tarantool::lua_state();

    let unprepare_stmt: LuaFunction<_> = lua
        .get("unprepare")
        .ok_or_else(|| QueryPlannerError::LuaError("Lua function `unprepare` not found".into()))?;

    match unprepare_stmt.call_with_args::<(), _>(stmt.id()?) {
        Ok(_) => Ok(()),
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("unprepare"),
                &format!("{:?}", e),
            );
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
        }
    }
}

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
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("read_prepared"),
                &format!("{:?}", e),
            );
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
        }
    }
}

fn read_unprepared(stmt: &str, params: &[Value]) -> Result<Box<dyn Any>, QueryPlannerError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("read")
        .ok_or_else(|| QueryPlannerError::LuaError("Lua function `read` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((0, stmt, params)) {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("read_unprepared"),
                &format!("{:?}", e),
            );
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
        }
    }
}

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
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("write_prepared"),
                &format!("{:?}", e),
            );
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
        }
    }
}

fn write_unprepared(stmt: &str, params: &[Value]) -> Result<Box<dyn Any>, QueryPlannerError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("write")
        .ok_or_else(|| QueryPlannerError::LuaError("Lua function `write` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((0, stmt, params)) {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("write_unprepared"),
                &format!("{:?}", e),
            );
            Err(QueryPlannerError::LuaError(format!("Lua error: {:?}", e)))
        }
    }
}

/// Load Lua code required for the storage runtime.
///
/// # Errors
/// - Failed to load Lua code.
#[allow(clippy::too_many_lines)]
pub fn load_storage_functions() -> Result<(), QueryPlannerError> {
    let lua = tarantool::lua_state();

    match lua.exec(
        r#"local vshard = require('vshard')
    local yaml = require('yaml')
    local log = require('log')
    local cartridge = require('cartridge')

    function get_storage_cache_capacity()
        local cfg = cartridge.config_get_readonly()

        if cfg["storage_cache_capacity"] == nil then
            return 200
        end

        return cfg["storage_cache_capacity"]
    end


    function get_storage_cache_size_bytes()
        local cfg = cartridge.config_get_readonly()

        if cfg["storage_cache_size_bytes"] == nil then
            return 204800
        end

        return cfg["storage_cache_size_bytes"]
    end

    function prepare(pattern)
        local prep, err = box.prepare(pattern)
        if err ~= nil then
            error("Failed to prepare statement: %s. Error: %s", pattern, err)
        end
        return prep.stmt_id
    end

    function unprepare(stmt_id)
        box.unprepare(stmt_id)
    end

    function read(stmt_id, stmt, params)
        local res, err = box.execute(stmt_id, params)
        if err ~= nil then
        -- The statement can be evicted from the cache,
        -- while we were yielding in Lua. So we execute
        -- it without the cache.
            res, err = box.execute(stmt, params)
            if err ~= nil then
                error(err)
            end
        end

        local result = {}
        result.metadata = res.metadata
        result.rows = {}
        for _, row in ipairs(res.rows) do
            local tuple = {}
            for _, field in ipairs(row) do
                table.insert(tuple, field)
            end
            table.insert(result.rows, tuple)
        end

        return box.tuple.new{result}
    end

    function write(stmt_id, stmt, params)
        local res, err = box.execute(stmt_id, params)
        if err ~= nil then
            -- The statement can be evicted from the cache,
            -- while we were yielding in Lua. So we execute
            -- it without the cache.
            res, err = box.execute(stmt, params)
            if err ~= nil then
                error(err)
            end
        end

        return box.tuple.new{res}
    end
"#,
    ) {
        Ok(_) => Ok(()),
        Err(e) => {
            say(
                SayLevel::Error,
                file!(),
                line!().try_into().unwrap_or(0),
                Option::from("exec_query"),
                &format!("{:?}", e),
            );
            Err(QueryPlannerError::LuaError(format!(
                "Failed lua code loading: {:?}",
                e
            )))
        }
    }
}
