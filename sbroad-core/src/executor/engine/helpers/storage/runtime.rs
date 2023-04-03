use std::any::Any;

use sbroad_proc::otm_child_span;
use tarantool::{tlua::LuaFunction, tuple::Tuple};

use crate::{error, errors::SbroadError, ir::value::Value, otm::child_span};

use super::{PreparedStmt, Statement};

#[otm_child_span("tarantool.statement.prepare")]
pub fn prepare(pattern: &str) -> Result<PreparedStmt, SbroadError> {
    let lua = tarantool::lua_state();

    let prepare_stmt: LuaFunction<_> = lua
        .get("prepare")
        .ok_or_else(|| SbroadError::LuaError("Lua function `prepare` not found".into()))?;

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
            Err(SbroadError::LuaError(format!("{e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.unprepare")]
pub fn unprepare(stmt: &mut PreparedStmt) -> Result<(), SbroadError> {
    let lua = tarantool::lua_state();

    let unprepare_stmt: LuaFunction<_> = lua
        .get("unprepare")
        .ok_or_else(|| SbroadError::LuaError("Lua function `unprepare` not found".into()))?;

    match unprepare_stmt.call_with_args::<(), _>(stmt.id()?) {
        Ok(_) => Ok(()),
        Err(e) => {
            error!(Option::from("unprepare"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format!("{e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.prepared.read")]
pub fn read_prepared(
    stmt_id: u32,
    stmt: &str,
    params: &[Value],
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("read")
        .ok_or_else(|| SbroadError::LuaError("Lua function `read` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((stmt_id, stmt, params)) {
        Ok(v) => Ok(Box::new(v) as Box<dyn Any>),
        Err(e) => {
            error!(Option::from("read_prepared"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format!("{e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.unprepared.read")]
pub fn read_unprepared(stmt: &str, params: &[Value]) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("read")
        .ok_or_else(|| SbroadError::LuaError("Lua function `read` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((0, stmt, params)) {
        Ok(v) => Ok(Box::new(v) as Box<dyn Any>),
        Err(e) => {
            error!(Option::from("read_unprepared"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format!("{e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.prepared.write")]
pub fn write_prepared(
    stmt_id: u32,
    stmt: &str,
    params: &[Value],
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("write")
        .ok_or_else(|| SbroadError::LuaError("Lua function `write` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((stmt_id, stmt, params)) {
        Ok(v) => Ok(Box::new(v) as Box<dyn Any>),
        Err(e) => {
            error!(Option::from("write_prepared"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format!("{e:?}")))
        }
    }
}

#[otm_child_span("tarantool.statement.unprepared.write")]
pub fn write_unprepared(stmt: &str, params: &[Value]) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("write")
        .ok_or_else(|| SbroadError::LuaError("Lua function `write` not found".into()))?;

    match exec_sql.call_with_args::<Tuple, _>((0, stmt, params)) {
        Ok(v) => Ok(Box::new(v) as Box<dyn Any>),
        Err(e) => {
            error!(Option::from("write_unprepared"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format!("{e:?}")))
        }
    }
}
