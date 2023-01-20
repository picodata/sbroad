//! Tarantool cartridge engine module.

use rand::prelude::*;

use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::rc::Rc;

use tarantool::tlua::LuaFunction;
use tarantool::tuple::Tuple;

use crate::api::exec_query::protocol::{
    Binary, EncodedOptionalData, EncodedRequiredData, Message, OptionalData, RequiredData,
};
use crate::cartridge::config::RouterConfiguration;
use crate::cartridge::update_tracing;

use sbroad::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use sbroad::errors::{Action, Entity, SbroadError};
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::{
    normalize_name_from_schema, sharding_keys_from_map, sharding_keys_from_tuple, Configuration,
    Coordinator, CoordinatorMetadata,
};
use sbroad::executor::hash::bucket_id_by_tuple;
use sbroad::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use sbroad::executor::lru::Cache;
use sbroad::executor::lru::{LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::result::ProducerResult;
use sbroad::executor::vtable::VirtualTable;
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::ir::helpers::RepeatableState;
use sbroad::ir::tree::Snapshot;
use sbroad::ir::value::Value;
use sbroad::ir::Plan;
use sbroad::otm::child_span;
use sbroad::{debug, error};
use sbroad_proc::otm_child_span;

type GroupedBuckets = HashMap<String, Vec<u64>>;

/// The runtime (cluster configuration, buckets, IR cache) of the dispatcher node.
#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntime {
    metadata: RouterConfiguration,
    bucket_count: usize,
    ir_cache: RefCell<LRUCache<String, Plan>>,
}

impl Configuration for RouterRuntime {
    type Configuration = RouterConfiguration;

    fn cached_config(&self) -> &Self::Configuration {
        &self.metadata
    }

    fn clear_config(&mut self) {
        self.metadata = Self::Configuration::new();
    }

    fn is_config_empty(&self) -> bool {
        self.metadata.is_empty()
    }

    #[allow(clippy::too_many_lines)]
    fn get_config(&self) -> Result<Option<Self::Configuration>, SbroadError> {
        if self.is_config_empty() {
            let lua = tarantool::lua_state();

            let get_schema: LuaFunction<_> = lua.eval("return get_schema;").unwrap();
            let schema: String = match get_schema.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting schema"), &format!("{e:?}"));
                    return Err(SbroadError::LuaError(format!("Lua error: {e:?}")));
                }
            };

            let jaeger_agent_host: LuaFunction<_> =
                lua.eval("return get_jaeger_agent_host;").unwrap();
            let jaeger_host: String = match jaeger_agent_host.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting jaeger agent host"), &format!("{e:?}"),);
                    return Err(SbroadError::LuaError(format!("Lua error: {e:?}")));
                }
            };

            let jaeger_agent_port: LuaFunction<_> =
                lua.eval("return get_jaeger_agent_port;").unwrap();
            let jaeger_port: u16 = match jaeger_agent_port.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting jaeger agent port"), &format!("{e:?}"),);
                    return Err(SbroadError::LuaError(format!("Lua error: {e:?}")));
                }
            };

            let waiting_timeout: LuaFunction<_> = lua.eval("return get_waiting_timeout;").unwrap();
            let timeout: u64 = match waiting_timeout.call() {
                Ok(res) => res,
                Err(e) => {
                    error!(Option::from("getting waiting timeout"), &format!("{e:?}"));
                    return Err(SbroadError::LuaError(format!("Lua error: {e:?}")));
                }
            };

            let router_cache_capacity: LuaFunction<_> =
                lua.eval("return get_router_cache_capacity;").unwrap();
            let router_capacity: usize = match router_cache_capacity.call() {
                Ok(capacity) => {
                    let val: u64 = capacity;
                    usize::try_from(val).map_err(|_| {
                        SbroadError::Invalid(
                            Entity::Cache,
                            Some(format!("router cache capacity is too big: {capacity}")),
                        )
                    })?
                }
                Err(e) => {
                    error!(
                        Option::from("getting router cache capacity"),
                        &format!("{e:?}"),
                    );
                    return Err(SbroadError::LuaError(format!("Lua error: {e:?}")));
                }
            };

            let sharding_column: LuaFunction<_> = lua.eval("return get_sharding_column;").unwrap();
            let column: String = match sharding_column.call() {
                Ok(column) => column,
                Err(e) => {
                    error!(Option::from("getting sharding column"), &format!("{e:?}"));
                    return Err(SbroadError::LuaError(format!("Lua error: {e:?}")));
                }
            };

            let mut metadata = RouterConfiguration::new();
            metadata.set_jaeger_agent_host(jaeger_host);
            metadata.set_jaeger_agent_port(jaeger_port);
            metadata.set_waiting_timeout(timeout);
            metadata.set_cache_capacity(router_capacity);
            metadata.set_sharding_column(normalize_name_from_schema(column.as_str()));
            // We should always load the schema **after** setting the sharding column.
            metadata.load_schema(&schema)?;
            update_tracing(
                metadata.get_jaeger_agent_host(),
                metadata.get_jaeger_agent_port(),
            )?;

            return Ok(Some(metadata));
        }
        Ok(None)
    }

    fn update_config(&mut self, metadata: Self::Configuration) {
        self.metadata = metadata;
    }
}

impl Coordinator for RouterRuntime {
    type ParseTree = AbstractSyntaxTree;
    type Cache = LRUCache<String, Plan>;

    fn clear_ir_cache(&self) -> Result<(), SbroadError> {
        *self.ir_cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format!("{e:?}"))
        })? = Self::Cache::new(DEFAULT_CAPACITY, None)?;
        Ok(())
    }

    fn ir_cache(&self) -> &RefCell<Self::Cache> {
        &self.ir_cache
    }

    /// Execute a sub tree on the nodes
    #[otm_child_span("query.dispatch.cartridge")]
    fn dispatch(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        debug!(
            Option::from("dispatch"),
            &format!("dispatching plan: {plan:?}")
        );
        let mut sub_plan = plan.take_subtree(top_id)?;
        let sub_plan_id = sub_plan.get_ir_plan().pattern_id()?;
        let query_type = sub_plan.query_type()?;
        let conn_type = sub_plan.connection_type()?;
        debug!(Option::from("dispatch"), &format!("sub plan: {sub_plan:?}"));

        let filter_vtable = |plan: &mut ExecutionPlan, bucket_ids: &[u64]| {
            if let Some(vtables) = plan.get_mut_vtables() {
                for rc_vtable in vtables.values_mut() {
                    // If the virtual table id hashed by the bucket_id, we can filter its tuples.
                    // Otherwise (full motion policy) we need to preserve all tuples.
                    if !rc_vtable.get_index().is_empty() {
                        *rc_vtable = Rc::new(rc_vtable.new_with_buckets(bucket_ids));
                    }
                }
            }
        };

        // We should not use the cache on the storage if the plan contains virtual tables,
        // as they can contain different amount of tuples that are not taken into account
        // when calculating the cache key.
        let can_be_cached = plan.vtables_empty();

        let encode_plan = |exec_plan: ExecutionPlan| -> Result<(Binary, Binary), SbroadError> {
            let sp_top_id = exec_plan.get_ir_plan().get_top()?;
            let sp = SyntaxPlan::new(&exec_plan, sp_top_id, Snapshot::Oldest)?;
            let ordered = OrderedSyntaxNodes::try_from(sp)?;
            let nodes = ordered.to_syntax_data()?;
            // Virtual tables in the plan must be already filtered, so we can use all buckets here.
            let params = exec_plan.to_params(&nodes, &Buckets::All)?;
            let query_type = exec_plan.query_type()?;
            let required_data =
                RequiredData::new(sub_plan_id.clone(), params, query_type, can_be_cached);
            let encoded_required_data = EncodedRequiredData::try_from(required_data)?;
            let raw_required_data: Vec<u8> = encoded_required_data.into();
            let optional_data = OptionalData::new(exec_plan, ordered);
            let encoded_optional_data = EncodedOptionalData::try_from(optional_data)?;
            let raw_optional_data: Vec<u8> = encoded_optional_data.into();
            Ok((raw_required_data.into(), raw_optional_data.into()))
        };

        if let Buckets::Filtered(bucket_set) = buckets {
            let random_bucket = self.get_random_bucket();
            let buckets = if bucket_set.is_empty() {
                // There are no buckets to execute the query on.
                // At the moment we don't keep types inside our IR tree and
                // there is no easy way to get column types in the result.
                // So we just choose a random bucket and to execute the query on,
                // as we are sure that any bucket returns an empty result.

                // TODO: return an empty result without actual execution.
                &random_bucket
            } else {
                buckets
            };

            let mut rs_ir: HashMap<String, Message> = HashMap::new();
            let rs_bucket_vec: Vec<(String, Vec<u64>)> = group(buckets)?.drain().collect();
            if rs_bucket_vec.is_empty() {
                return Err(SbroadError::UnexpectedNumberOfValues(format!(
                    "no replica sets were found for the buckets {:?} to execute the query on",
                    buckets
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
            return self.exec_ir_on_some(rs_ir, query_type, conn_type);
        }

        let (required, optional) = encode_plan(sub_plan)?;
        self.exec_ir_on_all(required, optional, query_type, conn_type)
    }

    fn explain_format(&self, explain: String) -> Result<Box<dyn Any>, SbroadError> {
        let e = explain.lines().collect::<Vec<&str>>();

        match Tuple::new(&vec![e]) {
            Ok(t) => Ok(Box::new(t)),
            Err(e) => Err(SbroadError::FailedTo(
                Action::Create,
                Some(Entity::Tuple),
                format!("{e}"),
            )),
        }
    }

    /// Transform sub query results into a virtual table.
    #[otm_child_span("query.motion.materialize")]
    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
        buckets: &Buckets,
    ) -> Result<VirtualTable, SbroadError> {
        let top_id = plan.get_motion_subtree_root(motion_node_id)?;
        // We should get a motion alias name before we take the subtree in dispatch.
        let alias = plan.get_motion_alias(motion_node_id)?.map(String::from);
        let result = self.dispatch(plan, top_id, buckets)?;
        // Unlink motion node's child sub tree (it is already replaced with invalid values).
        plan.unlink_motion_subtree(motion_node_id)?;
        let mut vtable = if let Ok(tuple) = result.downcast::<Tuple>() {
            let data = tuple.decode::<Vec<ProducerResult>>().map_err(|e| {
                SbroadError::FailedTo(
                    Action::Decode,
                    Some(Entity::Tuple),
                    format!("motion node {motion_node_id}. {e}"),
                )
            })?;
            data.get(0)
                .ok_or_else(|| {
                    SbroadError::NotFound(Entity::ProducerResult, "from the tuple".into())
                })?
                .as_virtual_table()?
        } else {
            return Err(SbroadError::Invalid(
                Entity::Motion,
                Some("the result of the motion is not a tuple".to_string()),
            ));
        };
        if let Some(name) = alias {
            vtable.set_alias(&name)?;
        }

        Ok(vtable)
    }

    fn extract_sharding_keys_from_map<'engine, 'rec>(
        &'engine self,
        space: String,
        map: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_keys_from_map(&self.metadata, &space, map)
    }

    fn extract_sharding_keys_from_tuple<'engine, 'rec>(
        &'engine self,
        space: String,
        rec: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_keys_from_tuple(self.cached_config(), &space, rec)
    }

    /// Calculate bucket for a key.
    fn determine_bucket_id(&self, s: &[&Value]) -> u64 {
        bucket_id_by_tuple(s, self.bucket_count)
    }
}

impl RouterRuntime {
    /// Create new Tarantool cartridge runtime.
    ///
    /// # Errors
    /// - Failed to detect the correct amount of buckets.
    pub fn new() -> Result<Self, SbroadError> {
        let cache: LRUCache<String, Plan> = LRUCache::new(DEFAULT_CAPACITY, None)?;
        let mut result = RouterRuntime {
            metadata: RouterConfiguration::new(),
            bucket_count: 0,
            ir_cache: RefCell::new(cache),
        };

        result.set_bucket_count()?;

        Ok(result)
    }

    #[otm_child_span("buckets.random")]
    fn get_random_bucket(&self) -> Buckets {
        let mut rng = thread_rng();
        let bucket_id: u64 = rng.gen_range(1..=self.bucket_count as u64);
        let bucket_set: HashSet<u64, RepeatableState> = HashSet::from_iter(vec![bucket_id]);
        Buckets::Filtered(bucket_set)
    }

    /// Function get summary count of bucket from `vshard`
    fn set_bucket_count(&mut self) -> Result<(), SbroadError> {
        let lua = tarantool::lua_state();

        let bucket_count_fn: LuaFunction<_> =
            match lua.eval("return require('vshard').router.bucket_count") {
                Ok(v) => v,
                Err(e) => {
                    error!(Option::from("set_bucket_count"), &format!("{e:?}"));
                    return Err(SbroadError::LuaError(format!(
                        "Failed lua function load: {}",
                        e
                    )));
                }
            };

        let bucket_count: u64 = match bucket_count_fn.call() {
            Ok(r) => r,
            Err(e) => {
                error!(Option::from("set_bucket_count"), &format!("{e:?}"));
                return Err(SbroadError::LuaError(e.to_string()));
            }
        };

        self.bucket_count = match bucket_count.try_into() {
            Ok(v) => v,
            Err(_) => {
                return Err(SbroadError::Invalid(
                    Entity::Runtime,
                    Some("invalid bucket count".into()),
                ));
            }
        };

        Ok(())
    }

    fn dql_on_some(
        &self,
        rs_ir: HashMap<String, Message>,
        is_readonly: bool,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let lua = tarantool::lua_state();

        let exec_sql: LuaFunction<_> = lua
            .get("dql_on_some")
            .ok_or_else(|| SbroadError::LuaError("Lua function `dql_on_some` not found".into()))?;

        let waiting_timeout = &self.cached_config().get_exec_waiting_timeout();
        match exec_sql.call_with_args::<Tuple, _>((rs_ir, is_readonly, waiting_timeout)) {
            Ok(v) => {
                debug!(Option::from("dql_on_some"), &format!("Result: {:?}", &v));
                Ok(Box::new(v))
            }
            Err(e) => {
                error!(Option::from("dql_on_some"), &format!("{e:?}"));
                Err(SbroadError::LuaError(format!(
                    "Lua error (IR dispatch): {:?}",
                    e
                )))
            }
        }
    }

    fn dml_on_some(
        &self,
        rs_ir: HashMap<String, Message>,
        is_readonly: bool,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let lua = tarantool::lua_state();

        let exec_sql: LuaFunction<_> = lua
            .get("dml_on_some")
            .ok_or_else(|| SbroadError::LuaError("Lua function `dml_on_some` not found".into()))?;

        let waiting_timeout = &self.cached_config().get_exec_waiting_timeout();
        match exec_sql.call_with_args::<Tuple, _>((rs_ir, is_readonly, waiting_timeout)) {
            Ok(v) => Ok(Box::new(v)),
            Err(e) => {
                error!(Option::from("dml_on_some"), &format!("{e:?}"));
                Err(SbroadError::LuaError(format!(
                    "Lua error (IR dispatch): {:?}",
                    e
                )))
            }
        }
    }

    #[otm_child_span("query.dispatch.cartridge.some")]
    fn exec_ir_on_some(
        &self,
        rs_ir: HashMap<String, Message>,
        query_type: QueryType,
        conn_type: ConnectionType,
    ) -> Result<Box<dyn Any>, SbroadError> {
        match &query_type {
            QueryType::DQL => self.dql_on_some(rs_ir, conn_type.is_readonly()),
            QueryType::DML => self.dml_on_some(rs_ir, conn_type.is_readonly()),
        }
    }

    fn dql_on_all(
        &self,
        required: Binary,
        optional: Binary,
        is_readonly: bool,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let lua = tarantool::lua_state();
        let exec_sql: LuaFunction<_> = lua
            .get("dql_on_all")
            .ok_or_else(|| SbroadError::LuaError("Lua function `dql_on_all` not found".into()))?;

        let waiting_timeout = &self.cached_config().get_exec_waiting_timeout();
        match exec_sql.call_with_args::<Tuple, _>((
            required,
            optional,
            is_readonly,
            waiting_timeout,
        )) {
            Ok(v) => {
                debug!(Option::from("dql_on_all"), &format!("Result: {:?}", &v));
                Ok(Box::new(v))
            }
            Err(e) => {
                error!(Option::from("dql_on_all"), &format!("{e:?}"));
                Err(SbroadError::LuaError(format!(
                    "Lua error (dispatch IR): {:?}",
                    e
                )))
            }
        }
    }

    fn dml_on_all(
        &self,
        required: Binary,
        optional: Binary,
        is_readonly: bool,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let lua = tarantool::lua_state();

        let exec_sql: LuaFunction<_> = lua
            .get("dml_on_all")
            .ok_or_else(|| SbroadError::LuaError("Lua function `dml_on_all` not found".into()))?;

        let waiting_timeout = &self.cached_config().get_exec_waiting_timeout();
        match exec_sql.call_with_args::<Tuple, _>((
            required,
            optional,
            is_readonly,
            waiting_timeout,
        )) {
            Ok(v) => Ok(Box::new(v)),
            Err(e) => {
                error!(Option::from("dml_on_all"), &format!("{e:?}"));
                Err(SbroadError::LuaError(format!(
                    "Lua error (dispatch IR): {:?}",
                    e
                )))
            }
        }
    }

    #[otm_child_span("query.dispatch.all")]
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
    ) -> Result<Box<dyn Any>, SbroadError> {
        match &query_type {
            QueryType::DQL => self.dql_on_all(required, optional, conn_type.is_readonly()),
            QueryType::DML => self.dml_on_all(required, optional, conn_type.is_readonly()),
        }
    }
}

#[otm_child_span("buckets.group")]
fn group(buckets: &Buckets) -> Result<HashMap<String, Vec<u64>>, SbroadError> {
    let lua_buckets: Vec<u64> = match buckets {
        Buckets::All => {
            return Err(SbroadError::Unsupported(
                Entity::Buckets,
                Some("grouping buckets is not supported for all buckets".into()),
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
            return Err(SbroadError::LuaError(format!("Lua error: {e:?}")));
        }
    };

    Ok(res)
}
