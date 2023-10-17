use std::any::Any;
use std::cell::{Ref, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::rc::Rc;
use tarantool::decimal;
use tarantool::decimal::Decimal;

use crate::backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan};
use crate::cbo::histogram::HistogramBuckets;
use crate::cbo::histogram::{Histogram, Mcv, McvSet, Scalar};
use crate::cbo::tests::construct_i64_buckets;
use crate::cbo::{ColumnStats, TableColumnPair, TableStats};
use crate::collection;
use crate::errors::{Action, Entity, SbroadError};
use crate::executor::bucket::Buckets;
use crate::executor::engine::{
    helpers::{sharding_key_from_map, sharding_key_from_tuple, vshard::get_random_bucket},
    Router, Statistics, Vshard,
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
        Ok(table.get_sk()?.to_vec())
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
        let fn_func = Function::new_stable(name_func.clone(), Type::Integer);
        let mut functions = HashMap::new();
        functions.insert(name_func, fn_func);

        let mut tables = HashMap::new();

        let columns = vec![
            Column::new(
                "\"identification_number\"",
                Type::Integer,
                ColumnRole::User,
                false,
            ),
            Column::new("\"product_code\"", Type::String, ColumnRole::User, false),
            Column::new("\"product_units\"", Type::Boolean, ColumnRole::User, true),
            Column::new("\"sys_op\"", Type::Unsigned, ColumnRole::User, true),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key = &["\"identification_number\"", "\"product_code\""];
        let primary_key = &["\"product_code\"", "\"identification_number\""];
        tables.insert(
            "\"hash_testing\"".to_string(),
            Table::new(
                "\"hash_testing\"",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        tables.insert(
            "\"hash_testing_hist\"".to_string(),
            Table::new(
                "\"hash_testing_hist\"",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        let sharding_key = &["\"identification_number\""];
        tables.insert(
            "\"hash_single_testing\"".to_string(),
            Table::new(
                "\"hash_single_testing\"",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        tables.insert(
            "\"hash_single_testing_hist\"".to_string(),
            Table::new(
                "\"hash_single_testing_hist\"",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"id\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"sysFrom\"", Type::Unsigned, ColumnRole::User, true),
            Column::new("\"FIRST_NAME\"", Type::String, ColumnRole::User, true),
            Column::new("\"sys_op\"", Type::Unsigned, ColumnRole::User, true),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key = &["\"id\""];
        let primary_key = &["\"id\""];

        tables.insert(
            "\"test_space\"".to_string(),
            Table::new(
                "\"test_space\"",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        tables.insert(
            "\"test_space_hist\"".to_string(),
            Table::new(
                "\"test_space_hist\"",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"id\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key: &[&str] = &["\"id\""];
        let primary_key: &[&str] = &["\"id\""];
        tables.insert(
            "\"history\"".to_string(),
            Table::new(
                "\"history\"",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"a\"", Type::Unsigned, ColumnRole::User, true),
            Column::new("\"b\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"c\"", Type::Unsigned, ColumnRole::User, true),
            Column::new("\"d\"", Type::Unsigned, ColumnRole::User, true),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key: &[&str] = &["\"a\"", "\"b\""];
        let primary_key: &[&str] = &["\"b\""];
        tables.insert(
            "\"t\"".to_string(),
            Table::new(
                "\"t\"",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"a\"", Type::String, ColumnRole::User, false),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding, true),
            Column::new("\"b\"", Type::Integer, ColumnRole::User, false),
        ];
        let sharding_key: &[&str] = &["\"a\"", "\"b\""];
        let primary_key: &[&str] = &["\"a\"", "\"b\""];
        tables.insert(
            "\"t1\"".to_string(),
            Table::new(
                "\"t1\"",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"e\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"f\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"g\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"h\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key: &[&str] = &["\"e\"", "\"f\""];
        let primary_key: &[&str] = &["\"g\"", "\"h\""];
        tables.insert(
            "\"t2\"".to_string(),
            Table::new(
                "\"t2\"",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding, true),
            Column::new("\"a\"", Type::String, ColumnRole::User, false),
            Column::new("\"b\"", Type::Integer, ColumnRole::User, false),
        ];
        let sharding_key: &[&str] = &["\"a\""];
        let primary_key: &[&str] = &["\"a\""];
        tables.insert(
            "\"t3\"".to_string(),
            Table::new(
                "\"t3\"",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"a\"", Type::Integer, ColumnRole::User, false),
            Column::new("\"b\"", Type::Integer, ColumnRole::User, false),
        ];
        let sharding_key: &[&str] = &[];
        let primary_key: &[&str] = &["\"a\""];
        tables.insert(
            "\"global_t\"".to_string(),
            Table::new(
                "\"global_t\"",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );

        // Table for sbroad-benches
        let columns = vec![
            Column::new("\"vehicleguid\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"reestrid\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"reestrstatus\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"vehicleregno\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"vehiclevin\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"vehiclevin2\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"vehiclechassisnum\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehiclereleaseyear\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"operationregdoctypename\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"operationregdoc\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"operationregdocissuedate\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"operationregdoccomments\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleptstypename\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"vehicleptsnum\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"vehicleptsissuedate\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleptsissuer\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleptscomments\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehiclebodycolor\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"vehiclebrand\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"vehiclemodel\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"vehiclebrandmodel\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehiclebodynum\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"vehiclecost\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"vehiclegasequip\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleproducername\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehiclegrossmass\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"vehiclemass\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"vehiclesteeringwheeltypeid\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehiclekpptype\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicletransmissiontype\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicletypename\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehiclecategory\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicletypeunit\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleecoclass\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehiclespecfuncname\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleenclosedvolume\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleenginemodel\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleenginenum\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleenginepower\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleenginepowerkw\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"vehicleenginetype\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holdrestrictiondate\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"approvalnum\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"approvaldate\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"approvaltype\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"utilizationfeename\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"customsdoc\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"customsdocdate\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"customsdocissue\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"customsdocrestriction\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"customscountryremovalid\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"customscountryremovalname\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"ownerorgname\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"ownerinn\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"ownerogrn\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"ownerkpp\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"ownerpersonlastname\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"ownerpersonfirstname\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"ownerpersonmiddlename\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"ownerpersonbirthdate\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"ownerbirthplace\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"ownerpersonogrnip\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"owneraddressindex\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"owneraddressmundistrict\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"owneraddresssettlement\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"owneraddressstreet\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"ownerpersoninn\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"ownerpersondoccode\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"ownerpersondocnum\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"ownerpersondocdate\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"operationname\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"operationdate\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"operationdepartmentname\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"operationattorney\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"operationlising\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"holdertypeid\"", Type::Unsigned, ColumnRole::User, false),
            Column::new(
                "\"holderpersondoccode\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersondocnum\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersondocdate\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersondocissuer\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersonlastname\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersonfirstname\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersonmiddlename\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersonbirthdate\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersonbirthregionid\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersonsex\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersonbirthplace\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersoninn\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersonsnils\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderpersonogrnip\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressguid\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressregionid\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressregionname\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressdistrict\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressmundistrict\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddresssettlement\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressstreet\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressbuilding\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressstructureid\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressstructurename\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new(
                "\"holderaddressstructure\"",
                Type::Unsigned,
                ColumnRole::User,
                false,
            ),
            Column::new("\"sys_from\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"sys_to\"", Type::Unsigned, ColumnRole::User, false),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding, true),
        ];
        let sharding_key: &[&str] = &["\"reestrid\""];
        let primary_key: &[&str] = &["\"reestrid\""];
        tables.insert(
            "\"test__gibdd_db__vehicle_reg_and_res100_actual\"".to_string(),
            Table::new(
                "\"test__gibdd_db__vehicle_reg_and_res100_actual\"",
                columns.clone(),
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
        );
        tables.insert(
            "\"test__gibdd_db__vehicle_reg_and_res100_history\"".to_string(),
            Table::new(
                "\"test__gibdd_db__vehicle_reg_and_res100_history\"",
                columns,
                sharding_key,
                primary_key,
                SpaceEngine::Memtx,
                false,
            )
            .unwrap(),
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
    initial_column_statistics_cache: RefCell<HashMap<TableColumnPair, Rc<Box<dyn Any>>>>,
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

    fn provides_versions(&self) -> bool {
        false
    }

    fn get_table_version(&self, _: &str) -> Result<u64, SbroadError> {
        Err(SbroadError::DoSkip)
    }
}

impl Vshard for RouterRuntimeMock {
    fn exec_ir_on_all(
        &self,
        _required: Binary,
        _optional: Binary,
        _query_type: QueryType,
        _conn_type: ConnectionType,
        _vtable_max_rows: u64,
    ) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_on_all is not supported for the mock runtime".to_string()),
        ))
    }

    fn exec_ir_locally(&self, _sub_plan: ExecutionPlan) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_locally is not supported for the mock runtime".to_string()),
        ))
    }

    fn bucket_count(&self) -> u64 {
        self.metadata.borrow().bucket_count
    }

    fn get_random_bucket(&self) -> Buckets {
        get_random_bucket(self)
    }

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        Ok(bucket_id_by_tuple(s, self.bucket_count()))
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
        _vtable_max_rows: u64,
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

    fn determine_bucket_id(&self, s: &[&Value]) -> Result<u64, SbroadError> {
        Ok(bucket_id_by_tuple(s, self.bucket_count()))
    }

    fn exec_ir_locally(&self, _sub_plan: ExecutionPlan) -> Result<Box<dyn Any>, SbroadError> {
        Err(SbroadError::Unsupported(
            Entity::Runtime,
            Some("exec_ir_locally is not supported for the mock runtime".to_string()),
        ))
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
    #[allow(dead_code, clippy::missing_panics_doc, clippy::too_many_lines)]
    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    #[must_use]
    pub fn new() -> Self {
        let cache: LRUCache<String, Plan> = LRUCache::new(DEFAULT_CAPACITY, None).unwrap();

        let mut table_statistics_cache = HashMap::new();
        let hash_testing_hist_rows_number = 1000.0;
        table_statistics_cache.insert(
            "\"hash_testing_hist\"".to_string(),
            Rc::new(TableStats::new(hash_testing_hist_rows_number as u64)),
        );
        let hash_testing_rows_number = 10000.0;
        table_statistics_cache.insert(
            "\"hash_testing\"".to_string(),
            Rc::new(TableStats::new(hash_testing_rows_number as u64)),
        );
        let test_space_rows_number = 25000.0;
        table_statistics_cache.insert(
            "\"test_space\"".to_string(),
            Rc::new(TableStats::new(test_space_rows_number as u64)),
        );

        let mut column_statistics_cache: HashMap<TableColumnPair, Rc<Box<dyn Any>>> =
            HashMap::new();
        // Column statistics with empty histogram.
        //
        // * rows_number: 1000
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
        let boxed_column_stats = Box::new(ColumnStats::new(
            1i64,
            50i64,
            4,
            Some(Histogram::new(
                McvSet::default(),
                HistogramBuckets::default(),
                decimal!(0.0),
                decimal!(0.0),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("\"hash_testing_hist\"".to_string(), 0),
            Rc::new(boxed_column_stats),
        );

        // Casual column statistics.
        //
        // Values `min_value` and `max_value` of `ColumnStats` structure are in fact
        // displaying MIN and MAX values of `Histogram` structure (that is seen from its
        // `most_common` and `buckets_boundaries` fields).
        // An example of statistics, where general column statistics and histogram statistics conform.
        //
        // * rows_number: 10000
        // * min_value: 0
        // * max_value: 1005
        // * avg_size: 8
        // * histogram:
        //   - null_fraction: 0.1 (1000)
        //   - most_common:
        //     [0 -> 1000,
        //      1 -> 1000,
        //      2 -> 500,
        //      3 -> 500,
        //      4 -> 1000]
        //   - ndv (absolute value): 15 (only 10 in buckets)
        //   - buckets_count: 100
        //   - buckets_frequency: 50 (as only 5000 elements are stored in buckets)
        //   - buckets_boundaries: equally splitted [5; 1005] range
        let mcv_vec = vec![
            Mcv::new(
                0i64,
                Decimal::try_from(1000.0 / hash_testing_rows_number).unwrap(),
            ),
            Mcv::new(
                1i64,
                Decimal::try_from(1000.0 / hash_testing_rows_number).unwrap(),
            ),
            Mcv::new(
                2i64,
                Decimal::try_from(500.0 / hash_testing_rows_number).unwrap(),
            ),
            Mcv::new(
                3i64,
                Decimal::try_from(500.0 / hash_testing_rows_number).unwrap(),
            ),
            Mcv::new(
                4i64,
                Decimal::try_from(1000.0 / hash_testing_rows_number).unwrap(),
            ),
        ];
        let default_histogram_buckets_number = 100;
        let buckets = construct_i64_buckets(default_histogram_buckets_number, 5, 1005).unwrap();
        let boxed_column_stats = Box::new(ColumnStats::new(
            0i64,
            1005i64,
            8,
            Some(Histogram::new(
                McvSet::from_vec(&mcv_vec),
                buckets,
                decimal!(0.1),
                // 15 / (10000 * (1 -  0.1))
                decimal!(0.001666),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("\"hash_testing\"".to_string(), 0),
            Rc::new(boxed_column_stats),
        );

        // Column statistics with unique values.
        // Note that it's also a column statistics with no `most_common` values.
        //
        // * rows_number: 10000
        // * min_value: 1
        // * max_value: 90
        // * avg_size: 4
        // * histogram:
        //   - null_fraction: 0.1 (1000)
        //   - most_common: []
        //   - ndv (absolute value): 900
        //   - buckets_count: 3
        //   - buckets_frequency: 3000 (as all 9000 left elements are stored in buckets)
        //   - buckets_boundaries: [1, 40, 65, 90]
        let mut boundaries = vec![1i64, 40i64, 65i64, 90i64];
        let buckets = HistogramBuckets::try_from(&mut boundaries).unwrap();
        let boxed_column_stats = Box::new(ColumnStats::new(
            1i64,
            900i64,
            4,
            Some(Histogram::new(
                McvSet::new(),
                buckets,
                decimal!(0.1),
                // 900 / (10000 * (1 -  0.1))
                decimal!(0.1),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("\"hash_testing\"".to_string(), 3),
            Rc::new(boxed_column_stats),
        );

        // Casual column statistics, but for different column in different table.
        //
        // Values `min_value` and `max_value` of `ColumnStats` structure differ
        // from MIN and MAX values that we can get from `Histogram` structure (that is seen from its
        // `most_common` and `buckets_boundaries` fields).
        // An example of statistics, where general column statistics and histogram statistics
        // do NOT conform. That means histogram was gathered before updates to the corresponding table were made.
        //
        // * rows_number: 25000
        // * min_value: 1
        // * max_value: 2000
        // * avg_size: 8
        // * histogram:
        //   - null_fraction: 0.2 (5000)
        //   - most_common:
        //     [3 -> 2500,
        //      4 -> 500,
        //      5 -> 500,
        //      6 -> 1500]
        //   - ndv: 104 (only 100 in buckets)
        //   - buckets_count: 4
        //   - buckets_frequency: 3750 (as only 15000 elements are stored in buckets)
        //   - buckets_boundaries: [0, 78, 200, 780, 1800]
        let mcv_vec = vec![
            Mcv::new(
                3i64,
                Decimal::try_from(2500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                4i64,
                Decimal::try_from(500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                5i64,
                Decimal::try_from(500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                6i64,
                Decimal::try_from(1500.0 / test_space_rows_number).unwrap(),
            ),
        ];
        let mut boundaries = vec![0, 78, 200, 780, 1800];
        let boxed_column_stats = Box::new(ColumnStats::new(
            1i64,
            2000i64,
            8,
            Some(Histogram::new(
                McvSet::from_vec(&mcv_vec),
                HistogramBuckets::try_from(&mut boundaries).unwrap(),
                decimal!(0.2),
                // 104 / (25000 * (1 -  0.2))
                decimal!(0.0052),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("\"test_space\"".to_string(), 0),
            Rc::new(boxed_column_stats),
        );

        // Casual column statistics (one more column with adequate histogram buckets):
        //
        // * rows_number: 25000
        // * min_value: 100
        // * max_value: 400
        // * avg_size: 8
        // * histogram:
        //   - null_fraction: 0.2 (5000)
        //   - most_common: []
        //   - ndv (absolute value): 10 (10 in buckets)
        //   - buckets_count: 100
        //   - buckets_frequency: 200 (as only 20000 elements are stored in buckets)
        //   - buckets_boundaries: equally splitted [100; 400] range
        let buckets = construct_i64_buckets(default_histogram_buckets_number, 100, 400).unwrap();
        let boxed_column_stats = Box::new(ColumnStats::new(
            100i64,
            400i64,
            8,
            Some(Histogram::new(
                McvSet::new(),
                buckets,
                decimal!(0.2),
                // 10 / (25000 * (1 -  0.2))
                decimal!(0.0005),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("\"test_space\"".to_string(), 1),
            Rc::new(boxed_column_stats),
        );

        // String column statistics.
        //
        // * rows_number: 25000
        // * min_value: "a"
        // * max_value: "z"
        // * avg_size: 1
        // * histogram:
        //   - null_fraction: 0.2 (5000)
        //   - most_common:
        //     ["a" -> 2500,
        //      "b" -> 500,
        //      "c" -> 500,
        //      "d" -> 1500]
        //   - ndv: 104 (only 100 in buckets)
        //   - buckets_count: 4
        //   - buckets_frequency: 3750 (as only 15000 elements are stored in buckets)
        //   - buckets_boundaries: ["e", "j", "q", "s", "z"]
        let mcv_vec = vec![
            Mcv::new(
                String::from("a"),
                Decimal::try_from(2500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                String::from("b"),
                Decimal::try_from(500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                String::from("c"),
                Decimal::try_from(500.0 / test_space_rows_number).unwrap(),
            ),
            Mcv::new(
                String::from("d"),
                Decimal::try_from(1500.0 / test_space_rows_number).unwrap(),
            ),
        ];
        let mut boundaries = vec![
            String::from("e"),
            String::from("j"),
            String::from("q"),
            String::from("s"),
            String::from("z"),
        ];
        let boxed_column_stats = Box::new(ColumnStats::new(
            String::from("a"),
            String::from("z"),
            1,
            Some(Histogram::new(
                McvSet::from_vec(&mcv_vec),
                HistogramBuckets::try_from(&mut boundaries).unwrap(),
                decimal!(0.2),
                // 104 / (25000 * (1 -  0.2))
                decimal!(0.0052),
            )),
        ));
        column_statistics_cache.insert(
            TableColumnPair::new("\"test_space\"".to_string(), 2),
            Rc::new(boxed_column_stats),
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
        _plan: &mut ExecutionPlan,
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
            Buckets::Local => {
                let (sql, _) = plan.to_sql(&nodes, buckets, "test")?;
                result.extend(exec_locally(String::from(sql).as_str()))?;
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

    fn extract_sharding_key_from_map<'rec>(
        &self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_key_from_map(&*self.metadata.borrow(), &space, args)
    }

    fn extract_sharding_key_from_tuple<'rec>(
        &self,
        space: String,
        rec: &'rec [Value],
    ) -> Result<Vec<&'rec Value>, SbroadError> {
        sharding_key_from_tuple(&*self.metadata.borrow(), &space, rec)
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
                    format!("Mocked statistics for table {table_name} wasn't found"),
                ))
            }
        } else {
            Err(SbroadError::Invalid(
                Entity::Statistics,
                Some(String::from("Couldn't borrow table statistics")),
            ))
        }
    }

    fn get_column_stats(
        &self,
        table_column_pair: TableColumnPair,
    ) -> Result<Rc<Box<dyn Any>>, SbroadError> {
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

    fn update_table_stats(
        &mut self,
        table_name: String,
        table_stats: TableStats,
    ) -> Result<(), SbroadError> {
        if let Ok(mut borrow_res) = self.table_statistics_cache.try_borrow_mut() {
            let value = borrow_res.get_mut(table_name.as_str());
            if let Some(value) = value {
                *value = Rc::new(table_stats);
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

    fn update_column_stats<T: Scalar>(
        &self,
        table_column_pair: TableColumnPair,
        column_stats: ColumnStats<T>,
    ) -> Result<(), SbroadError> {
        if let Ok(mut borrow_res) = self.initial_column_statistics_cache.try_borrow_mut() {
            let boxed = Box::new(column_stats);
            let value = borrow_res.get_mut(&table_column_pair);
            if let Some(value) = value {
                *value = Rc::new(boxed);
            } else {
                borrow_res.insert(table_column_pair, Rc::new(boxed));
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

fn exec_locally(query: &str) -> ProducerResult {
    let mut result = ProducerResult::new();

    result.rows.push(vec![
        LuaValue::String("Execute query locally".into()),
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
