use std::any::Any;
use std::cell::{Ref, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::cbo::histogram::MostCommonValueWithFrequency;
use crate::cbo::{TableColumnPair, TableStats};
use crate::collection;
use crate::errors::{Action, Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::{
    helpers::{sharding_keys_from_map, sharding_keys_from_tuple, vshard::get_random_bucket},
    InitialBucket, InitialColumnStats, InitialHistogram, Router, Statistics, Vshard,
};
use crate::executor::hash::bucket_id_by_tuple;
use crate::executor::ir::{ConnectionType, ExecutionPlan, QueryType};
use crate::executor::lru::{LRUCache, DEFAULT_CAPACITY};
use crate::executor::protocol::Binary;
use crate::executor::result::ProducerResult;
use crate::executor::vtable::VirtualTable;
use crate::executor::Cache;
use crate::frontend::sql::ast::AbstractSyntaxTree;
use crate::ir::function::Function;
use crate::ir::helpers::RepeatableState;
use crate::ir::relation::{Column, ColumnRole, SpaceEngine, Table, Type};
use crate::ir::tree::Snapshot;
use crate::ir::value::{LuaValue, Value};
use crate::ir::Plan;

use super::helpers::normalize_name_from_sql;
use super::{Metadata, QueryCache};

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct RouterConfigurationMock {
    functions: HashMap<String, Function>,
    tables: HashMap<String, Table>,
    bucket_count: u64,
    sharding_column: String,
}

impl Metadata for RouterConfigurationMock {
    fn table(&self, table_name: &str) -> Result<Table, SbroadError> {
        let name = normalize_name_from_sql(table_name);
        match self.tables.get(&name) {
            Some(v) => Ok(v.clone()),
            None => Err(SbroadError::NotFound(Entity::Space, table_name.to_string())),
        }
    }

    fn function(&self, fn_name: &str) -> Result<&Function, SbroadError> {
        let name = normalize_name_from_sql(fn_name);
        match self.functions.get(&name) {
            Some(v) => Ok(v),
            None => Err(SbroadError::NotFound(Entity::SQLFunction, name)),
        }
    }

    fn waiting_timeout(&self) -> u64 {
        0
    }

    fn sharding_column(&self) -> &str {
        self.sharding_column.as_str()
    }

    fn sharding_key_by_space(&self, space: &str) -> Result<Vec<String>, SbroadError> {
        let table = self.table(space)?;
        table.get_sharding_column_names()
    }

    fn sharding_positions_by_space(&self, space: &str) -> Result<Vec<usize>, SbroadError> {
        let table = self.table(space)?;
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
        let name_func = normalize_name_from_sql("func");
        let fn_func = Function::new_stable(name_func.clone());
        let mut functions = HashMap::new();
        functions.insert(name_func, fn_func);

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
    table_statistics_cache: RefCell<HashMap<String, Rc<TableStats>>>,
    initial_column_statistics_cache: RefCell<HashMap<TableColumnPair, Rc<InitialColumnStats>>>,
}

impl std::fmt::Debug for RouterRuntimeMock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("")
            .field(&self.metadata)
            .field(&self.virtual_tables)
            .field(&self.table_statistics_cache)
            .field(&self.initial_column_statistics_cache)
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

impl QueryCache for RouterRuntimeMock {
    type Cache = LRUCache<String, Plan>;

    fn clear_cache(&self) -> Result<(), SbroadError> {
        *self.ir_cache.borrow_mut() = LRUCache::new(self.cache_capacity()?, None)?;
        Ok(())
    }

    fn cache(&self) -> &RefCell<Self::Cache> {
        &self.ir_cache
    }

    fn cache_capacity(&self) -> Result<usize, SbroadError> {
        Ok(self
            .cache()
            .try_borrow()
            .map_err(|e| {
                SbroadError::FailedTo(Action::Borrow, Some(Entity::Cache), format!("{e:?}"))
            })?
            .capacity())
    }
}

impl Vshard for RouterRuntimeMock {
    fn exec_ir_on_all(
        &self,
        _required: Binary,
        _optional: Binary,
        _query_type: QueryType,
        _conn_type: ConnectionType,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_all is not supported for the mock runtime".to_string()),
        ))
    }

    fn bucket_count(&self) -> u64 {
        self.metadata.borrow().bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> u64 {
        bucket_id_by_tuple(s, self.bucket_count())
    }

    fn exec_ir_on_some(
        &self,
        _sub_plan: ExecutionPlan,
        _buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_some is not supported for the mock runtime".to_string()),
        ))
    }
}

impl Vshard for &RouterRuntimeMock {
    fn exec_ir_on_all(
        &self,
        _required: Binary,
        _optional: Binary,
        _query_type: QueryType,
        _conn_type: ConnectionType,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_all is not supported for the mock runtime".to_string()),
        ))
    }

    fn bucket_count(&self) -> u64 {
        self.metadata.borrow().bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> u64 {
        bucket_id_by_tuple(s, self.bucket_count())
    }

    fn exec_ir_on_some(
        &self,
        _sub_plan: ExecutionPlan,
        _buckets: &Buckets,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_some is not supported for the mock runtime".to_string()),
        ))
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

        let mut table_statistics_cache = HashMap::new();
        table_statistics_cache.insert(
            "\"hash_testing_hist\"".to_string(),
            Rc::new(TableStats::new(
                "hash_testing_hist".to_string(),
                100,
                2100,
                1000,
                2000,
            )),
        );
        table_statistics_cache.insert(
            "\"hash_testing\"".to_string(),
            Rc::new(TableStats::new(
                "hash_testing".to_string(),
                1000,
                1200,
                300,
                200,
            )),
        );
        table_statistics_cache.insert(
            "\"test_space\"".to_string(),
            Rc::new(TableStats::new(
                "test_space".to_string(),
                2500,
                3000,
                1000,
                500,
            )),
        );

        // Note that `rows_number` field in inserted column statistics must be equal to the
        // `rows_number` field in the corresponding table.
        let mut column_statistics_cache = HashMap::new();
        // Column statistics with empty histogram.
        //
        // * rows_number: 100
        // * min_value: 1
        // * max_value: 50
        // * avg_size: 4
        // * histogram:
        //   - null_fraction: 0.0
        //   - most_common: []
        //   - ndv (absolute value): 0
        //   - buckets_count: 0
        //   - buckets_frequency: 0
        //   - buckets_boundaries: []
        column_statistics_cache.insert(
            TableColumnPair::new("\"hash_testing_hist\"".to_string(), 0),
            Rc::new(InitialColumnStats {
                min_value: Value::Integer(1),
                max_value: Value::Integer(50),
                avg_size: 4,
                histogram: InitialHistogram {
                    most_common: vec![],
                    buckets: vec![],
                    null_fraction: 0.0,
                    distinct_values_fraction: 0.0,
                },
            }),
        );

        // Casual column statistics.
        //
        // Values `min_value` and `max_value` of `ColumnStats` structure are in fact
        // displaying MIN and MAX values of `Histogram` structure (that is seen from its
        // `most_common` and `buckets_boundaries` fields).
        // An example of statistics, where general column statistics and histogram statistics conform.
        //
        // * rows_number: 1000
        // * min_value: 0
        // * max_value: 15
        // * avg_size: 8
        // * histogram:
        //   - null_fraction: 0.1 (100)
        //   - most_common:
        //     [0 -> 100,
        //      1 -> 100,
        //      2 -> 50,
        //      3 -> 50,
        //      4 -> 100]
        //   - ndv (absolute value): 15 (only 10 in buckets)
        //   - buckets_count: 5
        //   - buckets_frequency: 100 (as only 500 elements are stored in buckets)
        //   - buckets_boundaries: [5, 7, 9, 11, 13, 15]
        column_statistics_cache.insert(
            TableColumnPair::new("\"hash_testing\"".to_string(), 0),
            Rc::new(InitialColumnStats {
                min_value: Value::Integer(0),
                max_value: Value::Integer(15),
                avg_size: 8,
                histogram: InitialHistogram {
                    most_common: vec![
                        MostCommonValueWithFrequency::new(Value::Integer(0), 100.0),
                        MostCommonValueWithFrequency::new(Value::Integer(1), 100.0),
                        MostCommonValueWithFrequency::new(Value::Integer(2), 50.0),
                        MostCommonValueWithFrequency::new(Value::Integer(3), 50.0),
                        MostCommonValueWithFrequency::new(Value::Integer(4), 100.0),
                    ],
                    buckets: vec![
                        InitialBucket::First {
                            from: Value::Integer(5),
                            to: Value::Integer(7),
                            frequency: 100,
                        },
                        InitialBucket::NonFirst {
                            from: Value::Integer(7),
                            to: Value::Integer(9),
                            frequency: 100,
                        },
                        InitialBucket::NonFirst {
                            from: Value::Integer(9),
                            to: Value::Integer(11),
                            frequency: 100,
                        },
                        InitialBucket::NonFirst {
                            from: Value::Integer(11),
                            to: Value::Integer(13),
                            frequency: 100,
                        },
                        InitialBucket::NonFirst {
                            from: Value::Integer(13),
                            to: Value::Integer(15),
                            frequency: 100,
                        },
                    ],
                    null_fraction: 0.1,
                    // 15 / (1000 * (1 -  0.1)) ~ 15
                    distinct_values_fraction: 0.01666,
                },
            }),
        );
        // Column statistics with unique values.
        // Note that it's also a column statistics with no `most_common` values.
        //
        // * rows_number: 1000
        // * min_value: 1
        // * max_value: 90
        // * avg_size: 4
        // * histogram:
        //   - null_fraction: 0.1 (100)
        //   - most_common: []
        //   - ndv (absolute value): 900
        //   - buckets_count: 3
        //   - buckets_frequency: 300 (as all 900 left elements are stored in buckets)
        //   - buckets_boundaries: [1, 40, 65, 90]
        column_statistics_cache.insert(
            TableColumnPair::new("\"hash_testing\"".to_string(), 1),
            Rc::new(InitialColumnStats {
                min_value: Value::Integer(1),
                max_value: Value::Integer(900),
                avg_size: 4,
                histogram: InitialHistogram {
                    most_common: vec![],
                    buckets: vec![
                        InitialBucket::First {
                            from: Value::Integer(1),
                            to: Value::Integer(40),
                            frequency: 300,
                        },
                        InitialBucket::NonFirst {
                            from: Value::Integer(40),
                            to: Value::Integer(65),
                            frequency: 300,
                        },
                        InitialBucket::NonFirst {
                            from: Value::Integer(65),
                            to: Value::Integer(90),
                            frequency: 300,
                        },
                    ],
                    null_fraction: 0.1,
                    // 900 / (1000 * (1 -  0.1)) = 1
                    distinct_values_fraction: 1.0,
                },
            }),
        );
        // Casual column statistics, but for different column in different table.
        //
        // Values `min_value` and `max_value` of `ColumnStats` structure differ
        // from MIN and MAX values that we can get from `Histogram` structure (that is seen from its
        // `most_common` and `buckets_boundaries` fields).
        // An example of statistics, where general column statistics and histogram statistics
        // do NOT conform. That means histogram was gathered before updates to the corresponding table were made.
        //
        // * rows_number: 2500
        // * min_value: 1
        // * max_value: 2000
        // * avg_size: 8
        // * histogram:
        //   - null_fraction: 0.2 (500)
        //   - most_common:
        //     [3 -> 250,
        //      4 -> 50,
        //      5 -> 50,
        //      6 -> 150]
        //   - ndv: 104 (only 100 in buckets)
        //   - buckets_count: 4
        //   - buckets_frequency: 375 (as only 1500 elements are stored in buckets)
        //   - buckets_boundaries: [0, 78, 200, 780, 1800]
        column_statistics_cache.insert(
            TableColumnPair::new("\"test_space\"".to_string(), 0),
            Rc::new(InitialColumnStats {
                min_value: Value::Integer(1),
                max_value: Value::Integer(2000),
                avg_size: 8,
                histogram: InitialHistogram {
                    most_common: vec![
                        MostCommonValueWithFrequency::new(Value::Integer(3), 150.0),
                        MostCommonValueWithFrequency::new(Value::Integer(4), 50.0),
                        MostCommonValueWithFrequency::new(Value::Integer(5), 50.0),
                        MostCommonValueWithFrequency::new(Value::Integer(6), 150.0),
                    ],
                    buckets: vec![
                        InitialBucket::First {
                            from: Value::Integer(0),
                            to: Value::Integer(78),
                            frequency: 375,
                        },
                        InitialBucket::NonFirst {
                            from: Value::Integer(78),
                            to: Value::Integer(200),
                            frequency: 375,
                        },
                        InitialBucket::NonFirst {
                            from: Value::Integer(200),
                            to: Value::Integer(780),
                            frequency: 375,
                        },
                        InitialBucket::NonFirst {
                            from: Value::Integer(780),
                            to: Value::Integer(1800),
                            frequency: 375,
                        },
                    ],
                    null_fraction: 0.2,
                    // 104 / (2500 * (1 -  0.2)) = 0.052
                    distinct_values_fraction: 0.052,
                },
            }),
        );

        RouterRuntimeMock {
            metadata: RefCell::new(RouterConfigurationMock::new()),
            virtual_tables: RefCell::new(HashMap::new()),
            ir_cache: RefCell::new(cache),
            table_statistics_cache: RefCell::new(table_statistics_cache),
            initial_column_statistics_cache: RefCell::new(column_statistics_cache),
        }
    }

    #[allow(dead_code)]
    pub fn add_virtual_table(&self, id: usize, table: VirtualTable) {
        self.virtual_tables.borrow_mut().insert(id, table);
    }
}

impl Router for RouterRuntimeMock {
    type ParseTree = AbstractSyntaxTree;
    type MetadataProvider = RouterConfigurationMock;

    fn metadata(&self) -> Result<Ref<Self::MetadataProvider>, SbroadError> {
        self.metadata.try_borrow().map_err(|e| {
            SbroadError::FailedTo(Action::Borrow, Some(Entity::Metadata), format!("{e:?}"))
        })
    }

    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
        _buckets: &Buckets,
    ) -> Result<VirtualTable, SbroadError> {
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
            Buckets::Single => {
                let (sql, _) = plan.to_sql(&nodes, buckets, "test")?;
                result.extend(exec_on_some(1_u64, String::from(sql).as_str()))?;
            }
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

    fn extract_sharding_keys_from_map<'rec>(
        &self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_keys_from_map(&*self.metadata.borrow(), &space, args)
    }

    fn extract_sharding_keys_from_tuple<'rec>(
        &self,
        space: String,
        rec: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_keys_from_tuple(&*self.metadata.borrow(), &space, rec)
    }
}

impl Statistics for RouterRuntimeMock {
    fn get_table_stats(&self, table_name: String) -> Result<Rc<TableStats>, SbroadError> {
        if let Ok(borrow_res) = self.table_statistics_cache.try_borrow() {
            if let Some(value) = borrow_res.get(table_name.as_str()) {
                Ok(value.clone())
            } else {
                Err(SbroadError::NotFound(
                    Entity::Statistics,
                    String::from("Mocked statistics for table {table_name} wasn't found"),
                ))
            }
        } else {
            Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(String::from("Couldn't borrow table statistics")),
            ))
        }
    }

    fn get_initial_column_stats(
        &self,
        table_column_pair: TableColumnPair,
    ) -> Result<Rc<InitialColumnStats>, SbroadError> {
        if let Ok(borrow_res) = self.initial_column_statistics_cache.try_borrow() {
            if let Some(value) = borrow_res.get(&table_column_pair) {
                Ok(value.clone())
            } else {
                Err(SbroadError::NotFound(
                    Entity::Statistics,
                    String::from(
                        "Mocked statistics for table/column pair {table_column_paid} wasn't found",
                    ),
                ))
            }
        } else {
            Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(String::from("Couldn't borrow initial column statistics")),
            ))
        }
    }

    fn update_table_stats_cache(
        &mut self,
        table_name: String,
        table_stats: TableStats,
    ) -> Result<(), SbroadError> {
        if let Ok(mut borrow_res) = self.table_statistics_cache.try_borrow_mut() {
            let value = borrow_res.get_mut(table_name.as_str());
            if let Some(value) = value {
                *value = Rc::new(table_stats)
            } else {
                borrow_res.insert(table_name, Rc::new(table_stats));
            }
            Ok(())
        } else {
            Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(String::from("Couldn't borrow table statistics")),
            ))
        }
    }

    fn update_column_initial_stats_cache(
        &self,
        table_column_pair: TableColumnPair,
        initial_column_stats: InitialColumnStats,
    ) -> Result<(), SbroadError> {
        if let Ok(mut borrow_res) = self.initial_column_statistics_cache.try_borrow_mut() {
            let value = borrow_res.get_mut(&table_column_pair);
            if let Some(value) = value {
                *value = Rc::new(initial_column_stats)
            } else {
                borrow_res.insert(table_column_pair, Rc::new(initial_column_stats));
            }
            Ok(())
        } else {
            Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(String::from("Couldn't borrow initial column statistics")),
            ))
        }
    }
}

fn exec_on_some(bucket: u64, query: &str) -> ProducerResult {
    let mut result = ProducerResult::new();

    result.rows.push(vec![
        LuaValue::String(format!("Execute query on a bucket [{bucket}]")),
        LuaValue::String(String::from(query)),
    ]);

    result
}

fn exec_on_all(query: &str) -> ProducerResult {
    let mut result = ProducerResult::new();

    result.rows.push(vec![
        LuaValue::String(String::from("Execute query on all buckets")),
        LuaValue::String(String::from(query)),
    ]);

    result
}
