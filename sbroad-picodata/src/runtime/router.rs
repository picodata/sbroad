use std::{
    any::Any,
    cell::{Ref, RefCell},
    collections::HashMap,
    rc::Rc,
};

use sbroad::{
    errors::{Action, Entity, SbroadError},
    executor::{
        bucket::Buckets,
        engine::{
            helpers::{
                dispatch, explain_format, materialize_motion, sharding_keys_from_map,
                sharding_keys_from_tuple,
                vshard::{exec_ir_on_all, exec_with_filtered_buckets, get_random_bucket},
            },
            QueryCache, Router, Vshard,
        },
        hash::bucket_id_by_tuple,
        ir::{ConnectionType, ExecutionPlan, QueryType},
        lru::{Cache, LRUCache, DEFAULT_CAPACITY},
        protocol::Binary,
    },
    frontend::sql::ast::AbstractSyntaxTree,
    ir::Plan,
};

use super::{meta::router::RouterMetadata, DEFAULT_BUCKET_COUNT};

thread_local! (static PLAN_CACHE: Rc<RefCell<LRUCache<String, Plan>>> = Rc::new(RefCell::new(LRUCache::new(DEFAULT_CAPACITY, None).unwrap())));

#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntime {
    metadata: RefCell<RouterMetadata>,
    bucket_count: u64,
    ir_cache: Rc<RefCell<LRUCache<String, Plan>>>,
}

impl RouterRuntime {
    /// Build a new router runtime.
    ///
    /// # Errors
    /// - If the cache cannot be initialized.
    pub fn new() -> Result<Self, SbroadError> {
        let metadata = RouterMetadata::default();
        let bucket_count = DEFAULT_BUCKET_COUNT;
        let runtime = PLAN_CACHE.with(|cache| RouterRuntime {
            metadata: RefCell::new(metadata),
            bucket_count,
            ir_cache: cache.clone(),
        });
        Ok(runtime)
    }
}

impl QueryCache for RouterRuntime {
    type Cache = LRUCache<String, Plan>;

    fn cache(&self) -> &RefCell<Self::Cache> {
        &self.ir_cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self
            .ir_cache
            .try_borrow()
            .map_err(|e| SbroadError::FailedTo(Action::Get, Some(Entity::Cache), format!("{e:?}")))?
            .capacity())
    }

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.ir_cache.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Clear, Some(Entity::Cache), format!("{e:?}"))
        })? = Self::Cache::new(self.cache_capacity()?, None)?;
        Ok(())
    }
}

impl Router for RouterRuntime {
    type ParseTree = AbstractSyntaxTree;
    type MetadataProvider = RouterMetadata;

    fn metadata(&self) -> Result<Ref<Self::MetadataProvider>, SbroadError> {
        self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Get, Some(Entity::Metadata), format!("{e:?}"))
        })
    }

    fn materialize_motion(
        &self,
        plan: &mut sbroad::executor::ir::ExecutionPlan,
        motion_node_id: usize,
        buckets: &sbroad::executor::bucket::Buckets,
    ) -> Result<sbroad::executor::vtable::VirtualTable, SbroadError> {
        materialize_motion(self, plan, motion_node_id, buckets)
    }

    fn dispatch(
        &self,
        plan: &mut sbroad::executor::ir::ExecutionPlan,
        top_id: usize,
        buckets: &sbroad::executor::bucket::Buckets,
    ) -> Result<Box<dyn std::any::Any>, SbroadError> {
        dispatch(self, plan, top_id, buckets)
    }

    fn explain_format(&self, explain: String) -> Result<Box<dyn std::any::Any>, SbroadError> {
        explain_format(&explain)
    }

    fn extract_sharding_keys_from_map<'rec>(
        &self,
        space: String,
        args: &'rec HashMap<String, sbroad::ir::value::Value>,
    ) -> Result<Vec<&'rec sbroad::ir::value::Value>, SbroadError> {
        let metadata = self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e:?}"))
        })?;
        sharding_keys_from_map(&*metadata, &space, args)
    }

    fn extract_sharding_keys_from_tuple<'rec>(
        &self,
        space: String,
        args: &'rec [sbroad::ir::value::Value],
    ) -> Result<Vec<&'rec sbroad::ir::value::Value>, SbroadError> {
        sharding_keys_from_tuple(&*self.metadata()?, &space, args)
    }
}

impl Vshard for RouterRuntime {
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_all(
            &*self.metadata()?,
            required,
            optional,
            query_type,
            conn_type,
        )
    }

    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&sbroad::ir::value::Value]) -> u64 {
        bucket_id_by_tuple(s, self.bucket_count())
    }

    fn exec_ir_on_some(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_with_filtered_buckets(self, sub_plan, buckets)
    }
}

impl Vshard for &RouterRuntime {
    fn exec_ir_on_all(
        &self,
        required: Binary,
        optional: Binary,
        query_type: QueryType,
        conn_type: ConnectionType,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_ir_on_all(
            &*self.metadata()?,
            required,
            optional,
            query_type,
            conn_type,
        )
    }

    fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&sbroad::ir::value::Value]) -> u64 {
        bucket_id_by_tuple(s, self.bucket_count())
    }

    fn exec_ir_on_some(
        &self,
        sub_plan: ExecutionPlan,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        exec_with_filtered_buckets(*self, sub_plan, buckets)
    }
}
