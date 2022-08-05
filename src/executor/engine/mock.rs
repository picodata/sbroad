use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use crate::collection;
use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::engine::{Configuration, Coordinator};
use crate::executor::ir::ExecutionPlan;
use crate::executor::lru::{LRUCache, DEFAULT_CAPACITY};
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::executor::{Cache, CoordinatorMetadata};
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::helpers::RepeatableState;
use crate::ir::relation::{Column, ColumnRole, Table, Type};
use crate::ir::value::Value;
use crate::ir::Plan;

use super::cartridge::hash::bucket_id_by_tuple;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct RouterConfigurationMock {
    tables: HashMap<String, Table>,
    bucket_count: usize,
    sharding_column: String,
}

impl CoordinatorMetadata for RouterConfigurationMock {
    fn get_table_segment(&self, table_name: &str) -> Result<Table, QueryPlannerError> {
        let name = Self::to_name(table_name);
        match self.tables.get(&name) {
            Some(v) => Ok(v.clone()),
            None => Err(QueryPlannerError::CustomError(format!(
                "Space {} not found",
                table_name
            ))),
        }
    }

    fn get_exec_waiting_timeout(&self) -> u64 {
        0
    }

    fn get_sharding_column(&self) -> &str {
        self.sharding_column.as_str()
    }

    fn get_sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, QueryPlannerError> {
        let table = self.get_table_segment(&Self::to_name(space))?;
        table.get_sharding_column_names()
    }

    fn get_sharding_positions_by_space(
        &self,
        space: &str,
    ) -> Result<Vec<usize>, QueryPlannerError> {
        let table = self.get_table_segment(&Self::to_name(space))?;
        Ok(table.get_sharding_positions().to_vec())
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
            Table::new_seg("\"hash_testing\"", columns.clone(), &sharding_key).unwrap(),
        );

        tables.insert(
            "\"hash_testing_hist\"".to_string(),
            Table::new_seg("\"hash_testing_hist\"", columns.clone(), &sharding_key).unwrap(),
        );

        let sharding_key = vec!["\"identification_number\""];
        tables.insert(
            "\"hash_single_testing\"".to_string(),
            Table::new_seg("\"hash_single_testing\"", columns.clone(), &sharding_key).unwrap(),
        );

        tables.insert(
            "\"hash_single_testing_hist\"".to_string(),
            Table::new_seg("\"hash_single_testing_hist\"", columns, &sharding_key).unwrap(),
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
            Table::new_seg("\"test_space\"", columns.clone(), &sharding_key).unwrap(),
        );

        tables.insert(
            "\"test_space_hist\"".to_string(),
            Table::new_seg("\"test_space_hist\"", columns, &sharding_key).unwrap(),
        );

        let columns = vec![
            Column::new("\"id\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
        ];
        let sharding_key: &[&str] = &["\"id\""];
        tables.insert(
            "\"history\"".to_string(),
            Table::new_seg("\"history\"", columns, sharding_key).unwrap(),
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
            Table::new_seg("\"t\"", columns, sharding_key).unwrap(),
        );

        let columns = vec![
            Column::new("\"a\"", Type::String, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
            Column::new("\"b\"", Type::Integer, ColumnRole::User),
        ];
        let sharding_key: &[&str] = &["\"a\"", "\"b\""];
        tables.insert(
            "\"t1\"".to_string(),
            Table::new_seg("\"t1\"", columns, sharding_key).unwrap(),
        );

        RouterConfigurationMock {
            tables,
            bucket_count: 10000,
            sharding_column: "\"bucket_id\"".into(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntimeMock {
    metadata: RouterConfigurationMock,
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
    fn extend(&mut self, result: ProducerResult) -> Result<(), QueryPlannerError> {
        if self.metadata.is_empty() {
            self.metadata = result.clone().metadata;
        }

        if self.metadata != result.metadata {
            return Err(QueryPlannerError::CustomError(String::from(
                "Metadata mismatch. Producer results can't be extended",
            )));
        }
        self.rows.extend(result.rows.clone());
        Ok(())
    }
}

impl Configuration for RouterRuntimeMock {
    type Configuration = RouterConfigurationMock;

    fn cached_config(&self) -> &Self::Configuration {
        &self.metadata
    }

    fn clear_config(&mut self) {
        self.metadata.tables.clear();
    }

    fn is_config_empty(&self) -> bool {
        self.metadata.tables.is_empty()
    }

    fn get_config(&self) -> Result<Option<Self::Configuration>, QueryPlannerError> {
        let config = RouterConfigurationMock::new();
        Ok(Some(config))
    }

    fn update_config(&mut self, _config: Self::Configuration) {
        self.metadata = RouterConfigurationMock::new();
    }
}

impl Coordinator for RouterRuntimeMock {
    type ParseTree = AbstractSyntaxTree;
    type Cache = LRUCache<String, Plan>;

    fn clear_ir_cache(&self, capacity: usize) -> Result<(), QueryPlannerError> {
        *self.ir_cache.borrow_mut() = Self::Cache::new(capacity, None)?;
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
    ) -> Result<VirtualTable, QueryPlannerError> {
        plan.get_motion_subtree_root(motion_node_id)?;

        if let Some(virtual_table) = self.virtual_tables.borrow().get(&motion_node_id) {
            Ok(virtual_table.clone())
        } else {
            Err(QueryPlannerError::CustomError(
                "No virtual table found for motion node".to_string(),
            ))
        }
    }

    fn dispatch(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<Box<dyn Any>, QueryPlannerError> {
        let mut result = ProducerResult::new();
        let nodes = plan.get_sql_order(top_id)?;

        match buckets {
            Buckets::All => {
                let sql = plan.syntax_nodes_as_sql(&nodes, buckets)?;
                result.extend(exec_on_all(&String::from(sql).as_str()))?;
            }
            Buckets::Filtered(list) => {
                for bucket in list {
                    let bucket_set: HashSet<u64, RepeatableState> = collection! { *bucket };
                    let sql = plan.syntax_nodes_as_sql(&nodes, &Buckets::Filtered(bucket_set))?;
                    let temp_result = exec_on_some(*bucket, &String::from(sql).as_str());
                    result.extend(temp_result)?;
                }
            }
        }

        // Sort results to make tests reproducible.
        result.rows.sort_by_key(|k| k[0].to_string());
        Ok(Box::new(result))
    }

    fn extract_sharding_keys_from_map<'engine, 'rec>(
        &'engine self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, QueryPlannerError> {
        Ok(self
            .cached_config()
            .get_sharding_key_by_space(&space)
            .unwrap()
            .iter()
            .fold(Vec::new(), |mut acc: Vec<&Value>, v| {
                acc.push(args.get(v).unwrap());
                acc
            }))
    }

    fn extract_sharding_keys_from_tuple<'engine, 'rec>(
        &'engine self,
        space: String,
        rec: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, QueryPlannerError> {
        match self
            .cached_config()
            .get_sharding_positions_by_space(space.as_str())
        {
            Ok(vec) => {
                let mut vec_values = Vec::new();
                vec.into_iter()
                    .for_each(|index| vec_values.push(&rec[index]));
                Ok(vec_values)
            }
            Err(e) => Err(e),
        }
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> u64 {
        bucket_id_by_tuple(s, self.metadata.bucket_count)
    }
}

impl Default for RouterRuntimeMock {
    fn default() -> Self {
        Self::new()
    }
}

impl RouterRuntimeMock {
    #[allow(dead_code)]
    #[must_use]
    pub fn new() -> Self {
        let cache: LRUCache<String, Plan> = LRUCache::new(DEFAULT_CAPACITY, None).unwrap();
        RouterRuntimeMock {
            metadata: RouterConfigurationMock::new(),
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
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(String::from(query)),
    ]);

    result
}

fn exec_on_all(query: &str) -> ProducerResult {
    let mut result = ProducerResult::new();

    result.rows.push(vec![
        Value::String(String::from("Execute query on all buckets")),
        Value::String(String::from(query)),
    ]);

    result
}
