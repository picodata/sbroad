use std::{
    any::Any,
    collections::{HashMap, HashSet},
};

use crate::utils::MutexLike;
use crate::{
    executor::engine::Router,
    ir::{
        helpers::RepeatableState,
        operator::Relational,
        transformation::redistribution::{MotionOpcode, MotionPolicy},
        tree::{
            relation::RelationalIterator,
            traversal::{PostOrderWithFilter, REL_CAPACITY},
        },
        Node, Plan,
    },
    otm::child_span,
};
use rand::{thread_rng, Rng};
use sbroad_proc::otm_child_span;
use smol_str::format_smolstr;
use tarantool::session::with_su;
use tarantool::{tlua::LuaFunction, tuple::Tuple};

use crate::backend::sql::space::ADMIN_ID;
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

/// Map between replicaset uuid and plan subtree (with additional info), sending to it.
/// (see `Message`documentation for more info).
type ReplicasetMessage = HashMap<String, Message>;

/// Function to execute DQL (SELECT) query only on some replicasets (which uuid's are
/// present in the `rs_ir` argument).
///
/// Rust binding to Lua `dql_on_some` function.
fn dql_on_some(
    metadata: &impl Metadata,
    rs_ir: ReplicasetMessage,
    is_readonly: bool,
    vtable_max_rows: u64,
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("dql_on_some")
        .ok_or_else(|| SbroadError::LuaError("Lua function `dql_on_some` not found".into()))?;

    let waiting_timeout = metadata.waiting_timeout();
    // `with_su` is used to read from virtual tables previously created by admin.
    let call_res = with_su(ADMIN_ID, || {
        exec_sql.call_with_args::<Tuple, _>((rs_ir, is_readonly, waiting_timeout, vtable_max_rows))
    })?;
    match call_res {
        Ok(v) => {
            debug!(Option::from("dql_on_some"), &format!("Result: {:?}", &v));
            Ok(Box::new(v))
        }
        Err(e) => {
            error!(Option::from("dql_on_some"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format_smolstr!(
                "Lua error (IR dispatch): {e:?}"
            )))
        }
    }
}

/// Function to execute DML (INSERT) query only on some replicasets (which uuid's are
/// present in the `rs_ir` argument).
///
/// Rust binding to Lua `dml_on_some` function.
fn dml_on_some(
    metadata: &impl Metadata,
    rs_ir: ReplicasetMessage,
    is_readonly: bool,
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();

    let exec_sql: LuaFunction<_> = lua
        .get("dml_on_some")
        .ok_or_else(|| SbroadError::LuaError("Lua function `dml_on_some` not found".into()))?;

    let waiting_timeout = metadata.waiting_timeout();
    // `with_su` is used to read from virtual tables previously created by admin.
    let call_res = with_su(ADMIN_ID, || {
        exec_sql.call_with_args::<Tuple, _>((rs_ir, is_readonly, waiting_timeout))
    })?;
    match call_res {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            error!(Option::from("dml_on_some"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format_smolstr!(
                "Lua error (IR dispatch): {e:?}"
            )))
        }
    }
}

/// Function to execute DQL (SELECT) query on all replicasets.
///
/// Rust binding to Lua `dql_on_all` function.
fn dql_on_all(
    metadata: &impl Metadata,
    required: Binary,
    optional: Binary,
    vtable_max_rows: u64,
) -> Result<Box<dyn Any>, SbroadError> {
    let lua = tarantool::lua_state();
    let exec_sql: LuaFunction<_> = lua
        .get("dql_on_all")
        .ok_or_else(|| SbroadError::LuaError("Lua function `dql_on_all` not found".into()))?;

    let waiting_timeout = metadata.waiting_timeout();
    // `with_su` coverage is used to read from virtual tables previously created by admin.
    let call_res = with_su(ADMIN_ID, || {
        exec_sql.call_with_args::<Tuple, _>((required, optional, waiting_timeout, vtable_max_rows))
    })?;
    match call_res {
        Ok(v) => {
            debug!(Option::from("dql_on_all"), &format!("Result: {:?}", &v));
            Ok(Box::new(v))
        }
        Err(e) => {
            error!(Option::from("dql_on_all"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format_smolstr!(
                "Lua error (dispatch IR): {e:?}"
            )))
        }
    }
}

/// Function to execute DML (INSERT) query on all replicasets.
///
/// Rust binding to Lua `dml_on_all` function.
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
    // `with_su` coverage is used to read from virtual tables previously created by admin.
    let call_res = with_su(ADMIN_ID, || {
        exec_sql.call_with_args::<Tuple, _>((required, optional, is_readonly, waiting_timeout))
    })?;
    match call_res {
        Ok(v) => Ok(Box::new(v)),
        Err(e) => {
            error!(Option::from("dml_on_all"), &format!("{e:?}"));
            Err(SbroadError::LuaError(format_smolstr!(
                "Lua error (dispatch IR): {e:?}"
            )))
        }
    }
}

/// Generic function over `dql_on_all` and `dml_on_all` functions.
/// Used to execute IR on all replicasets.
#[otm_child_span("query.dispatch.all")]
pub fn exec_ir_on_all_buckets(
    metadata: &impl Metadata,
    required: Binary,
    optional: Binary,
    query_type: QueryType,
    conn_type: ConnectionType,
    vtable_max_rows: u64,
) -> Result<Box<dyn Any>, SbroadError> {
    match &query_type {
        QueryType::DQL => dql_on_all(metadata, required, optional, vtable_max_rows),
        QueryType::DML => dml_on_all(metadata, required, optional, conn_type.is_readonly()),
    }
}

/// Generic function over `dql_on_some` and `dml_on_some` functions.
/// Used to execute IR on some replicasets (that are discovered from given `buckets`).
#[otm_child_span("query.dispatch.cartridge.some")]
pub fn exec_ir_on_some_buckets(
    runtime: &(impl Router + Vshard),
    sub_plan: ExecutionPlan,
    buckets: &Buckets,
) -> Result<Box<dyn Any>, SbroadError> {
    let query_type = sub_plan.query_type()?;
    let conn_type = sub_plan.connection_type()?;
    let vtable_max_rows = sub_plan.get_vtable_max_rows();
    let Buckets::Filtered(bucket_set) = buckets else {
        return Err(SbroadError::Invalid(
            Entity::Buckets,
            Some(format_smolstr!(
                "Expected Buckets::Filtered, got {buckets:?}"
            )),
        ));
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

    // todo(ars): group should be a runtime function not global,
    // this way we could implement it for mock runtime for better testing
    // Vec of { replicaset_uuid, corresponding bucket ids }.
    let rs_bucket_vec: Vec<(String, Vec<u64>)> = group(buckets)?.drain().collect();
    if rs_bucket_vec.is_empty() {
        return Err(SbroadError::UnexpectedNumberOfValues(format_smolstr!(
            "no replica sets were found for the buckets {buckets:?} to execute the query on"
        )));
    }
    let rs_ir = prepare_rs_to_ir_map(&rs_bucket_vec, sub_plan)?;
    let mut rs_message = HashMap::with_capacity(rs_ir.len());
    for (rs, ir) in rs_ir {
        rs_message.insert(rs, Message::from(encode_plan(ir)?));
    }
    match &query_type {
        QueryType::DQL => dql_on_some(
            &*runtime.metadata().lock(),
            rs_message,
            conn_type.is_readonly(),
            vtable_max_rows,
        ),
        QueryType::DML => dml_on_some(
            &*runtime.metadata().lock(),
            rs_message,
            conn_type.is_readonly(),
        ),
    }
}

// Helper struct to hold information
// needed to apply SerializeAsEmpty opcode
// to subtree.
struct SerializeAsEmptyInfo {
    // ids of topmost motion nodes which have this opcode
    // with `true` value
    top_motion_ids: Vec<usize>,
    // ids of motions which have this opcode
    target_motion_ids: Vec<usize>,
    // ids of motions that are located below
    // top_motion_id, vtables corresponding
    // to those motions must be deleted from
    // replicaset message.
    unused_motions: Vec<usize>,
}

impl Plan {
    // return true if given node is Motion containing seriliaze as empty
    // opcode. If `check_enabled` is true checks that the opcode is enabled.
    fn is_serialize_as_empty_motion(&self, node_id: usize, check_enabled: bool) -> bool {
        if let Ok(Node::Relational(Relational::Motion { program, .. })) = self.get_node(node_id) {
            if let Some(op) = program
                .0
                .iter()
                .find(|op| matches!(op, MotionOpcode::SerializeAsEmptyTable(_)))
            {
                return !check_enabled || matches!(op, MotionOpcode::SerializeAsEmptyTable(true));
            };
        }
        false
    }

    fn collect_top_ids(&self) -> Result<Vec<usize>, SbroadError> {
        let mut stop_nodes: HashSet<usize> = HashSet::new();
        let iter_children = |node_id| -> RelationalIterator<'_> {
            if self.is_serialize_as_empty_motion(node_id, true) {
                stop_nodes.insert(node_id);
            }
            // do not traverse subtree with this child
            if stop_nodes.contains(&node_id) {
                return self.nodes.empty_rel_iter();
            }
            self.nodes.rel_iter(node_id)
        };
        let filter = |node_id: usize| -> bool { self.is_serialize_as_empty_motion(node_id, true) };
        let mut dfs = PostOrderWithFilter::with_capacity(iter_children, 4, Box::new(filter));

        Ok(dfs.iter(self.get_top()?).map(|(_, id)| id).collect())
    }

    fn serialize_as_empty_info(&self) -> Result<Option<SerializeAsEmptyInfo>, SbroadError> {
        let top_ids = self.collect_top_ids()?;
        if top_ids.is_empty() {
            return Ok(None);
        }

        // all motion nodes that are inside the subtrees
        // defined by `top_ids`
        let all_motion_nodes = {
            let is_motion = |node_id: usize| -> bool {
                matches!(
                    self.get_node(node_id),
                    Ok(Node::Relational(Relational::Motion { .. }))
                )
            };
            let mut all_motions = Vec::new();
            for top_id in &top_ids {
                let mut dfs = PostOrderWithFilter::with_capacity(
                    |x| self.nodes.rel_iter(x),
                    REL_CAPACITY,
                    Box::new(is_motion),
                );
                all_motions.extend(dfs.iter(*top_id).map(|(_, id)| id));
            }
            all_motions
        };
        let mut target_motions = Vec::new();
        let mut unused_motions = Vec::new();
        for id in all_motion_nodes {
            if self.is_serialize_as_empty_motion(id, false) {
                target_motions.push(id);
            } else {
                unused_motions.push(id);
            }
        }

        Ok(Some(SerializeAsEmptyInfo {
            top_motion_ids: top_ids,
            target_motion_ids: target_motions,
            unused_motions,
        }))
    }
}

/// Prepares execution plan for each replicaset.
///
/// # Errors
/// - Failed to apply customization opcodes
/// - Failed to filter vtable
pub fn prepare_rs_to_ir_map(
    rs_bucket_vec: &[(String, Vec<u64>)],
    mut sub_plan: ExecutionPlan,
) -> Result<HashMap<String, ExecutionPlan>, SbroadError> {
    let mut rs_ir: HashMap<String, ExecutionPlan> = HashMap::new();
    rs_ir.reserve(rs_bucket_vec.len());
    if let Some((last, other)) = rs_bucket_vec.split_last() {
        let sae_info = sub_plan.get_ir_plan().serialize_as_empty_info()?;
        for (rs, bucket_ids) in other {
            let mut rs_plan = sub_plan.clone();
            if let Some(ref info) = sae_info {
                apply_serialize_as_empty_opcode(&mut rs_plan, info)?;
            }
            filter_vtable(&mut rs_plan, bucket_ids)?;
            rs_ir.insert(rs.clone(), rs_plan);
        }

        if let Some(ref info) = sae_info {
            disable_serialize_as_empty_opcode(&mut sub_plan, info)?;
        }
        let (rs, bucket_ids) = last;
        filter_vtable(&mut sub_plan, bucket_ids)?;
        rs_ir.insert(rs.clone(), sub_plan);
    }

    Ok(rs_ir)
}

fn apply_serialize_as_empty_opcode(
    sub_plan: &mut ExecutionPlan,
    info: &SerializeAsEmptyInfo,
) -> Result<(), SbroadError> {
    if let Some(vtables_map) = sub_plan.get_mut_vtables() {
        for motion_id in &info.unused_motions {
            vtables_map.remove(motion_id);
        }
    }

    for top_id in &info.top_motion_ids {
        sub_plan.unlink_motion_subtree(*top_id)?;
    }
    Ok(())
}

fn disable_serialize_as_empty_opcode(
    sub_plan: &mut ExecutionPlan,
    info: &SerializeAsEmptyInfo,
) -> Result<(), SbroadError> {
    for motion_id in &info.target_motion_ids {
        let program = if let Relational::Motion {
            policy, program, ..
        } = sub_plan
            .get_mut_ir_plan()
            .get_mut_relation_node(*motion_id)?
        {
            if !matches!(policy, MotionPolicy::Local) {
                continue;
            }
            program
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("expected motion node on id {motion_id}")),
            ));
        };
        for op in &mut program.0 {
            if let MotionOpcode::SerializeAsEmptyTable(enabled) = op {
                *enabled = false;
            }
        }
    }

    Ok(())
}

/// Map between replicaset uuid and the set of buckets (their ids) which correspond to that replicaset.
/// This set is defined by vshard `router.route` function call. See `group_buckets_by_replicasets`
/// function for more details.
pub(crate) type GroupedBuckets = HashMap<String, Vec<u64>>;

/// Function that transforms `Buckets` (set of bucket_ids)
/// into `GroupedBuckets` (map from replicaset uuid to set of bucket_ids).
///
/// Rust binding to Lua `group_buckets_by_replicasets` function.
#[otm_child_span("buckets.group")]
fn group(buckets: &Buckets) -> Result<GroupedBuckets, SbroadError> {
    let lua_buckets: Vec<u64> = match buckets {
        Buckets::All | Buckets::Any => {
            return Err(SbroadError::Unsupported(
                Entity::Buckets,
                Some("grouping buckets is not supported for Buckets::All or Buckets::Any".into()),
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
            return Err(SbroadError::LuaError(format_smolstr!("{e:?}")));
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
