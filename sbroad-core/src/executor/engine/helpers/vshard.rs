use std::{
    any::Any,
    collections::{HashMap, HashSet},
};

use crate::{executor::engine::Router, ir::helpers::RepeatableState, otm::child_span};
use rand::{thread_rng, Rng};
use sbroad_proc::otm_child_span;
use tarantool::{tlua::LuaFunction, tuple::Tuple};

use crate::{
    debug, error,
    errors::{Entity, SbroadError},
    executor::{
        bucket::Buckets,
        engine::{helpers::empty_query_result, Metadata, Vshard},
        ir::{ConnectionType, ExecutionPlan, QueryType},
        protocol::{Binary, Message},
    },
};

use super::{encode_plan, filter_vtable};

fn dql_on_some(
    metadata: &impl Metadata,
    rs_ir: HashMap<String, Message>,
    is_readonly: bool,
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("dql_on_some")
        .ok_or_else(|| SbroadError::LuaError("Lua function `dql_on_some` not found".into()))?;

    let waiting_timeout = metadata.waiting_timeout();
    match exec_sql.call_with_args::<Tuple, _>((rs_ir, is_readonly, waiting_timeout)) {
        Ok(v) => {
            debug!(Option::from("dql_on_some"), &format!("Result: {:?}", &v));
            Ok(Box::new(v))
        }
        Err(e) => {
            error!(Option::from("dql_on_some"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format!(
                "Lua error (IR dispatch): {e:?}"
            )))
        }
    }
}

fn dml_on_some(
    metadata: &impl Metadata,
    rs_ir: HashMap<String, Message>,
    is_readonly: bool,
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("dml_on_some")
        .ok_or_else(|| SbroadError::LuaError("Lua function `dml_on_some` not found".into()))?;

    let waiting_timeout = metadata.waiting_timeout();
    match exec_sql.call_with_args::<Tuple, _>((rs_ir, is_readonly, waiting_timeout)) {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            error!(Option::from("dml_on_some"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format!(
                "Lua error (IR dispatch): {e:?}"
            )))
        }
    }
}

fn dql_on_all(
    metadata: &impl Metadata,
    required: Binary,
    optional: Binary,
    is_readonly: bool,
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();
    let exec_sql: LuaFunction<_> = lua
        .get("dql_on_all")
        .ok_or_else(|| SbroadError::LuaError("Lua function `dql_on_all` not found".into()))?;

    let waiting_timeout = metadata.waiting_timeout();
    match exec_sql.call_with_args::<Tuple, _>((required, optional, is_readonly, waiting_timeout)) {
        Ok(v) => {
            debug!(Option::from("dql_on_all"), &format!("Result: {:?}", &v));
            Ok(Box::new(v))
        }
        Err(e) => {
            error!(Option::from("dql_on_all"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format!(
                "Lua error (dispatch IR): {e:?}"
            )))
        }
    }
}

fn dml_on_all(
    metadata: &impl Metadata,
    required: Binary,
    optional: Binary,
    is_readonly: bool,
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("dml_on_all")
        .ok_or_else(|| SbroadError::LuaError("Lua function `dml_on_all` not found".into()))?;

    let waiting_timeout = metadata.waiting_timeout();
    match exec_sql.call_with_args::<Tuple, _>((required, optional, is_readonly, waiting_timeout)) {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            error!(Option::from("dml_on_all"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format!(
                "Lua error (dispatch IR): {e:?}"
            )))
        }
    }
}

#[otm_child_span("query.dispatch.all")]
pub fn exec_ir_on_all(
    metadata: &impl Metadata,
    required: Binary,
    optional: Binary,
    query_type: QueryType,
    conn_type: ConnectionType,
) -> Result<Box<dyn Any>, SbroadError> {
    match &query_type {
        QueryType::DQL => dql_on_all(metadata, required, optional, conn_type.is_readonly()),
        QueryType::DML => dml_on_all(metadata, required, optional, conn_type.is_readonly()),
    }
}

#[otm_child_span("query.dispatch.cartridge.some")]
pub fn exec_with_filtered_buckets(
    runtime: &(impl Router + Vshard),
    mut sub_plan: ExecutionPlan,
    buckets: &Buckets,
) -> Result<Box<dyn Any>, SbroadError> {
    let query_type = sub_plan.query_type()?;
    let conn_type = sub_plan.connection_type()?;
    let Buckets::Filtered(bucket_set) = buckets else {
        return Err(SbroadError::Invalid(
            Entity::Buckets,
            Some(format!("Expected Buckets::Filtered, got {buckets:?}")),
        ))
    };
    let mut buckets = buckets;
    let random_bucket = runtime.get_random_bucket();
    if bucket_set.is_empty() {
        match empty_query_result(&sub_plan)? {
            Some(res) => return Ok(res),
            None => {
                buckets = &random_bucket;
            }
        }
    }

    let mut rs_ir: HashMap<String, Message> = HashMap::new();
    let rs_bucket_vec: Vec<(String, Vec<u64>)> = group(buckets)?.drain().collect();
    if rs_bucket_vec.is_empty() {
        return Err(SbroadError::UnexpectedNumberOfValues(format!(
            "no replica sets were found for the buckets {buckets:?} to execute the query on"
        )));
    }
    rs_ir.reserve(rs_bucket_vec.len());

    if let Some((last, other)) = rs_bucket_vec.split_last() {
        for (rs, bucket_ids) in other {
            let mut rs_plan = sub_plan.clone();
            filter_vtable(&mut rs_plan, bucket_ids);
            rs_ir.insert(rs.clone(), Message::from(encode_plan(rs_plan)?));
        }

        let (rs, bucket_ids) = last;
        filter_vtable(&mut sub_plan, bucket_ids);
        rs_ir.insert(rs.clone(), Message::from(encode_plan(sub_plan)?));
    }
    match &query_type {
        QueryType::DQL => dql_on_some(&*runtime.metadata()?, rs_ir, conn_type.is_readonly()),
        QueryType::DML => dml_on_some(&*runtime.metadata()?, rs_ir, conn_type.is_readonly()),
    }
}

type GroupedBuckets = HashMap<String, Vec<u64>>;

#[otm_child_span("buckets.group")]
fn group(buckets: &Buckets) -> Result<HashMap<String, Vec<u64>>, SbroadError> {
    let lua_buckets: Vec<u64> = match buckets {
        Buckets::All | Buckets::Single => {
            return Err(SbroadError::Unsupported(
                Entity::Buckets,
                Some(
                    "grouping buckets is not supported for Buckets::All or Buckets::Single".into(),
                ),
            ))
        }
        Buckets::Filtered(list) => list.iter().copied().collect(),
    };

    let lua = tarantool::lua_state();

    let fn_group: LuaFunction<_> = lua.get("group_buckets_by_replicasets").ok_or_else(|| {
        SbroadError::LuaError("Lua function `group_buckets_by_replicasets` not found".into())
    })?;

    let res: GroupedBuckets = match fn_group.call_with_args(lua_buckets) {
        Ok(v) => v,
        Err(e) => {
            error!(Option::from("buckets group"), &format!("{e:?}"));
            return Err(SbroadError::LuaError(format!("{e:?}")));
        }
    };

    Ok(res)
}

#[otm_child_span("buckets.random")]
pub fn get_random_bucket(runtime: &impl Vshard) -> Buckets {
    let mut rng = thread_rng();
    let bucket_id: u64 = rng.gen_range(1..=runtime.bucket_count());
    let bucket_set: HashSet<u64, RepeatableState> = HashSet::from_iter(vec![bucket_id]);
    Buckets::Filtered(bucket_set)
}
