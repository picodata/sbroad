use std::any::Any;
use std::cell::{Ref, RefCell};
use std::collections::{HashMap, HashSet};

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::collection;
use crate::errors::{Action, Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::{
    normalize_name_from_sql, sharding_keys_from_map, sharding_keys_from_tuple, Configuration,
    Coordinator,
};
use crate::executor::hash::bucket_id_by_tuple;
use crate::executor::ir::ExecutionPlan;
use crate::executor::lru::{LRUCache, DEFAULT_CAPACITY};
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::executor::{Cache, CoordinatorMetadata};
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::function::Function;
use crate::ir::helpers::RepeatableState;
use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table, Type};
use crate::ir::tree::Snapshot;
use crate::ir::value::{EncodedValue, Value};
use crate::ir::Plan;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct RouterConfigurationMock {
    functions: HashMap<String, Function>,
    tables: HashMap<String, Table>,
    bucket_count: usize,
    sharding_column: String,
}

impl CoordinatorMetadata for RouterConfigurationMock {
    fn get_table_segment(&self, table_name: &str) -> Result<Table, SbroadError> {
        let name = normalize_name_from_sql(table_name);
        match self.tables.get(&name) {
            Some(v) => Ok(v.clone()),
            None => Err(SbroadError::NotFound(Entity::Space, table_name.to_string())),
        }
    }

    fn get_function(&self, fn_name: &str) -> Result<&Function, SbroadError> {
        let name = normalize_name_from_sql(fn_name);
        match self.functions.get(&name) {
            Some(v) => Ok(v),
            None => Err(SbroadError::NotFound(Entity::SQLFunction, name)),
        }
    }

    fn get_exec_waiting_timeout(&self) -> u64 {
        0
    }

    fn get_sharding_column(&self) -> &str {
        self.sharding_column.as_str()
    }

    fn get_sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, SbroadError> {
        let table = self.get_table_segment(space)?;
        table.get_sharding_column_names()
    }

    fn get_sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError> {
        let table = self.get_table_segment(space)?;
        Ok(table.get_sharding_positions().to_vec())
    }

    fn get_fields_amount_by_space(&self, space: &str) -> Result<usize, SbroadError> {
        let table = self.get_table_segment(space)?;
        Ok(table.columns.len())
    }
}

impl Default for RouterConfigurationMock {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterConfigurationMock {
    /// Mock engine constructor.
    ///
    /// # Panics
    /// - If schema is invalid.
    #[allow(clippy::too_many_lines)]
    #[must_use]
    pub fn new() -> Self {
        let name_bucket_id = normalize_name_from_sql("bucket_id");
        let fn_bucket_id = Function::new_stable(name_bucket_id.clone());
        let mut functions = HashMap::new();
        functions.insert(name_bucket_id, fn_bucket_id);

        let mut tables = HashMap::new();

        let columns = vec![
            Column::new("\"identification_number\"", Type::Integer, ColumnRole::User),
            Column::new("\"product_code\"", Type::String, ColumnRole::User),
            Column::new("\"product_units\"", Type::Boolean, ColumnRole::User),
            Column::new("\"sys_op\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
        ];
        let sharding_key = vec!["\"identification_number\"", "\"product_code\""];
        tables.insert(
            "\"hash_testing\"".to_string(),
            Table::new_seg(
                "\"hash_testing\"",
                columns.clone(),
                &sharding_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        tables.insert(
            "\"hash_testing_hist\"".to_string(),
            Table::new_seg(
                "\"hash_testing_hist\"",
                columns.clone(),
                &sharding_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let sharding_key = vec!["\"identification_number\""];
        tables.insert(
            "\"hash_single_testing\"".to_string(),
            Table::new_seg(
                "\"hash_single_testing\"",
                columns.clone(),
                &sharding_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        tables.insert(
            "\"hash_single_testing_hist\"".to_string(),
            Table::new_seg(
                "\"hash_single_testing_hist\"",
                columns,
                &sharding_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"id\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"sysFrom\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"FIRST_NAME\"", Type::String, ColumnRole::User),
            Column::new("\"sys_op\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
        ];
        let sharding_key = vec!["\"id\""];

        tables.insert(
            "\"test_space\"".to_string(),
            Table::new_seg(
                "\"test_space\"",
                columns.clone(),
                &sharding_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        tables.insert(
            "\"test_space_hist\"".to_string(),
            Table::new_seg(
                "\"test_space_hist\"",
                columns,
                &sharding_key,
                SpaceEngine::Memtx,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"id\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
        ];
        let sharding_key: &[&str] = &["\"id\""];
        tables.insert(
            "\"history\"".to_string(),
            Table::new_seg("\"history\"", columns, sharding_key, SpaceEngine::Memtx).unwrap(),
        );

        let columns = vec![
            Column::new("\"a\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"b\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"c\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"d\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
        ];
        let sharding_key: &[&str] = &["\"a\"", "\"b\""];
        tables.insert(
            "\"t\"".to_string(),
            Table::new_seg("\"t\"", columns, sharding_key, SpaceEngine::Memtx).unwrap(),
        );

        let columns = vec![
            Column::new("\"a\"", Type::String, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
            Column::new("\"b\"", Type::Integer, ColumnRole::User),
        ];
        let sharding_key: &[&str] = &["\"a\"", "\"b\""];
        tables.insert(
            "\"t1\"".to_string(),
            Table::new_seg("\"t1\"", columns, sharding_key, SpaceEngine::Memtx).unwrap(),
        );

        let columns = vec![
            Column::new("\"e\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"f\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"g\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"h\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
        ];
        let sharding_key: &[&str] = &["\"e\"", "\"f\""];
        tables.insert(
            "\"t2\"".to_string(),
            Table::new_seg("\"t2\"", columns, sharding_key, SpaceEngine::Memtx).unwrap(),
        );

        let columns = vec![
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
            Column::new("\"a\"", Type::String, ColumnRole::User),
            Column::new("\"b\"", Type::Integer, ColumnRole::User),
        ];
        let sharding_key: &[&str] = &["\"a\""];
        tables.insert(
            "\"t3\"".to_string(),
            Table::new_seg("\"t3\"", columns, sharding_key, SpaceEngine::Memtx).unwrap(),
        );

        RouterConfigurationMock {
            functions,
            tables,
            bucket_count: 10000,
            sharding_column: "\"bucket_id\"".into(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntimeMock {
    metadata: RefCell<RouterConfigurationMock>,
    virtual_tables: RefCell<HashMap<usize, VirtualTable>>,
    ir_cache: RefCell<LRUCache<String, Plan>>,
}

impl std::fmt::Debug for RouterRuntimeMock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
            .field(&self.metadata)
            .field(&self.virtual_tables)
            .finish()
    }
}

impl ProducerResult {
    /// Merge two record sets. If current result is empty function sets metadata and appends result rows.
    /// If the current result is not empty compare its metadata with the one from the new result.
    /// If they differ return error.
    ///
    ///  # Errors
    ///  - metadata isn't equal.
    fn extend(&mut self, result: ProducerResult) -> Result<(), SbroadError> {
        if self.metadata.is_empty() {
            self.metadata = result.clone().metadata;
        }

        if self.metadata != result.metadata {
            return Err(SbroadError::Invalid(
                Entity::Metadata,
                Some("Metadata mismatch. Producer results can't be extended".into()),
            ));
        }
        self.rows.extend(result.rows);
        Ok(())
    }
}

impl Configuration for RouterRuntimeMock {
    type Configuration = RouterConfigurationMock;

    fn cached_config(&self) -> Result<Ref<Self::Configuration>, SbroadError> {
        self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e:?}"))
        })
    }

    fn clear_config(&self) -> Result<(), SbroadError> {
        let mut metadata = self.metadata.try_borrow_mut().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e:?}"))
        })?;
        metadata.tables.clear();
        Ok(())
    }

    fn is_config_empty(&self) -> Result<bool, SbroadError> {
        let metadata = self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e:?}"))
        })?;
        Ok(metadata.tables.is_empty())
    }

    fn get_config(&self) -> Result<Option<Self::Configuration>, SbroadError> {
        let config = RouterConfigurationMock::new();
        Ok(Some(config))
    }

    fn update_config(&self, _config: Self::Configuration) -> Result<(), SbroadError> {
        *self.metadata.borrow_mut() = RouterConfigurationMock::new();
        Ok(())
    }
}

impl Coordinator for RouterRuntimeMock {
    type ParseTree = AbstractSyntaxTree;
    type Cache = LRUCache<String, Plan>;

    fn clear_ir_cache(&self) -> Result<(), SbroadError> {
        *self.ir_cache.borrow_mut() = Self::Cache::new(DEFAULT_CAPACITY, None)?;
        Ok(())
    }

    fn ir_cache(&self) -> &RefCell<Self::Cache> {
        &self.ir_cache
    }

    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
        _buckets: &Buckets,
    ) -> Result<VirtualTable, SbroadError> {
        plan.get_motion_subtree_root(motion_node_id)?;

        if let Some(virtual_table) = self.virtual_tables.borrow().get(&motion_node_id) {
            Ok(virtual_table.clone())
        } else {
            Err(SbroadError::NotFound(
                Entity::VirtualTable,
                format!("for motion node {motion_node_id}"),
            ))
        }
    }

    fn dispatch(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        let mut result = ProducerResult::new();
        let sp = SyntaxPlan::new(plan, top_id, Snapshot::Oldest)?;
        let ordered = OrderedSyntaxNodes::try_from(sp)?;
        let nodes = ordered.to_syntax_data()?;

        match buckets {
            Buckets::All => {
                let (sql, _) = plan.to_sql(&nodes, buckets, "test")?;
                result.extend(exec_on_all(String::from(sql).as_str()))?;
            }
            Buckets::Filtered(list) => {
                for bucket in list {
                    let bucket_set: HashSet<u64, RepeatableState> = collection! { *bucket };
                    let (sql, _) = plan.to_sql(&nodes, &Buckets::Filtered(bucket_set), "test")?;
                    let temp_result = exec_on_some(*bucket, String::from(sql).as_str());
                    result.extend(temp_result)?;
                }
            }
        }

        // Sort results to make tests reproducible.
        result.rows.sort_by_key(|k| k[0].to_string());
        Ok(Box::new(result))
    }

    fn explain_format(&self, explain: String) -> Result<Box<dyn Any>, SbroadError> {
        Ok(Box::new(explain))
    }

    fn extract_sharding_keys_from_map<'engine, 'rec>(
        &'engine self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_keys_from_map(&*self.metadata.borrow(), &space, args)
    }

    fn extract_sharding_keys_from_tuple<'engine, 'rec>(
        &'engine self,
        space: String,
        rec: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_keys_from_tuple(&*self.metadata.borrow(), &space, rec)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> u64 {
        bucket_id_by_tuple(s, self.metadata.borrow().bucket_count)
    }
}

impl Default for RouterRuntimeMock {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterRuntimeMock {
    #[allow(dead_code)]
    #[allow(clippy::missing_panics_doc)]
    #[must_use]
    pub fn new() -> Self {
        let cache: LRUCache<String, Plan> = LRUCache::new(DEFAULT_CAPACITY, None).unwrap();
        RouterRuntimeMock {
            metadata: RefCell::new(RouterConfigurationMock::new()),
            virtual_tables: RefCell::new(HashMap::new()),
            ir_cache: RefCell::new(cache),
        }
    }

    #[allow(dead_code)]
    pub fn add_virtual_table(&self, id: usize, table: VirtualTable) {
        self.virtual_tables.borrow_mut().insert(id, table);
    }
}

fn exec_on_some(bucket: u64, query: &str) -> ProducerResult {
    let mut result = ProducerResult::new();

    result.rows.push(vec![
        EncodedValue::String(format!("Execute query on a bucket [{bucket}]")),
        EncodedValue::String(String::from(query)),
    ]);

    result
}

fn exec_on_all(query: &str) -> ProducerResult {
    let mut result = ProducerResult::new();

    result.rows.push(vec![
        EncodedValue::String(String::from("Execute query on all buckets")),
        EncodedValue::String(String::from(query)),
    ]);

    result
}
