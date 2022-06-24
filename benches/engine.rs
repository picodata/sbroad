extern crate sbroad;

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;

use sbroad::errors::QueryPlannerError;
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::cartridge::config::RouterConfiguration;
use sbroad::executor::engine::cartridge::hash::bucket_id_by_tuple;
use sbroad::executor::engine::{Coordinator, CoordinatorMetadata, QueryCache};
use sbroad::executor::ir::ExecutionPlan;
use sbroad::executor::lru::{LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::result::ProducerResult;
use sbroad::executor::vtable::VirtualTable;
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::ir::relation::{Column, ColumnRole, Table, Type};
use sbroad::ir::value::Value;
use sbroad::ir::Plan;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct MetadataMock {
    schema: HashMap<String, Vec<String>>,
    tables: HashMap<String, Table>,
    bucket_count: usize,
    sharding_column: String,
}

impl CoordinatorMetadata for MetadataMock {
    fn get_table_segment(&self, table_name: &str) -> Result<Table, QueryPlannerError> {
        let name = Self::to_name(table_name);
        match self.tables.get(&name) {
            Some(v) => Ok(v.clone()),
            None => Err(QueryPlannerError::SpaceNotFound),
        }
    }

    fn get_exec_waiting_timeout(&self) -> u64 {
        0
    }

    fn get_sharding_column(&self) -> &str {
        self.sharding_column.as_str()
    }

    fn get_sharding_key_by_space(&self, space: &str) -> Result<Vec<&str>, QueryPlannerError> {
        Ok(self
            .schema
            .get(space)
            .map(|v| v.iter().map(String::as_str).collect::<Vec<&str>>())
            .unwrap())
    }
}

impl Default for MetadataMock {
    fn default() -> Self {
        Self::new()
    }
}

impl MetadataMock {
    /// Mock engine constructor.
    ///
    /// # Panics
    /// - If schema is invalid.
    #[allow(clippy::too_many_lines)]
    #[must_use]
    pub fn new() -> Self {
        let mut tables = HashMap::new();

        let columns = vec![
            Column::new("\"vehicleguid\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"reestrid\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"reestrstatus\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleregno\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclevin\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclevin2\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclechassisnum\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclereleaseyear\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"operationregdoctypename\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"operationregdoc\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"operationregdocissuedate\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"operationregdoccomments\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"vehicleptstypename\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleptsnum\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleptsissuedate\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleptsissuer\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleptscomments\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclebodycolor\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclebrand\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclemodel\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclebrandmodel\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclebodynum\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclecost\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclegasequip\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleproducername\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclegrossmass\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclemass\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"vehiclesteeringwheeltypeid\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"vehiclekpptype\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"vehicletransmissiontype\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"vehicletypename\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclecategory\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicletypeunit\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleecoclass\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehiclespecfuncname\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"vehicleenclosedvolume\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"vehicleenginemodel\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleenginenum\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleenginepower\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleenginepowerkw\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"vehicleenginetype\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"holdrestrictiondate\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"approvalnum\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"approvaldate\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"approvaltype\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"utilizationfeename\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"customsdoc\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"customsdocdate\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"customsdocissue\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"customsdocrestriction\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"customscountryremovalid\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"customscountryremovalname\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"ownerorgname\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerinn\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerogrn\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerkpp\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerpersonlastname\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerpersonfirstname\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"ownerpersonmiddlename\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"ownerpersonbirthdate\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerbirthplace\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerpersonogrnip\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"owneraddressindex\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"owneraddressmundistrict\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"owneraddresssettlement\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"owneraddressstreet\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerpersoninn\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerpersondoccode\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerpersondocnum\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"ownerpersondocdate\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"operationname\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"operationdate\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"operationdepartmentname\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"operationattorney\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"operationlising\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"holdertypeid\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"holderpersondoccode\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"holderpersondocnum\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"holderpersondocdate\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"holderpersondocissuer\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"holderpersonlastname\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"holderpersonfirstname\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderpersonmiddlename\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderpersonbirthdate\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderpersonbirthregionid\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"holderpersonsex\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"holderpersonbirthplace\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"holderpersoninn\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"holderpersonsnils\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"holderpersonogrnip\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"holderaddressguid\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"holderaddressregionid\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderaddressregionname\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderaddressdistrict\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderaddressmundistrict\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderaddresssettlement\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"holderaddressstreet\"", Type::Unsigned, ColumnRole::User),
            Column::new(
                "\"holderaddressbuilding\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderaddressstructureid\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderaddressstructurename\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderaddressstructure\"",
                Type::Unsigned,
                ColumnRole::User,
            ),
            Column::new("\"sys_from\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"sys_to\"", Type::Unsigned, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Unsigned, ColumnRole::Sharding),
        ];
        let sharding_key: &[&str] = &["\"reestrid\""];
        tables.insert(
            "\"test__gibdd_db__vehicle_reg_and_res100_actual\"".to_string(),
            Table::new_seg(
                "\"test__gibdd_db__vehicle_reg_and_res100_actual\"",
                columns.clone(),
                sharding_key,
            )
            .unwrap(),
        );
        let sharding_key: &[&str] = &["\"reestrid\""];
        tables.insert(
            "\"test__gibdd_db__vehicle_reg_and_res100_history\"".to_string(),
            Table::new_seg(
                "\"test__gibdd_db__vehicle_reg_and_res100_history\"",
                columns,
                sharding_key,
            )
            .unwrap(),
        );

        MetadataMock {
            schema: [
                ("EMPLOYEES".into(), vec!["ID".into()]),
                (
                    "hash_testing".into(),
                    vec!["identification_number".into(), "product_code".into()],
                ),
            ]
            .into_iter()
            .collect(),
            tables,
            bucket_count: 10000,
            sharding_column: "\"bucket_id\"".into(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct RouterRuntimeMock {
    metadata: MetadataMock,
    virtual_tables: HashMap<usize, VirtualTable>,
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

impl Coordinator for RouterRuntimeMock {
    type Metadata = MetadataMock;
    type ParseTree = AbstractSyntaxTree;
    type QueryCache = LRUCache<String, Plan>;
    type Configuration = RouterConfiguration;

    fn clear_ir_cache(&self, capacity: usize) -> Result<(), QueryPlannerError> {
        *self.ir_cache.borrow_mut() = Self::QueryCache::new(capacity, None)?;
        Ok(())
    }

    fn ir_cache(&self) -> &RefCell<Self::QueryCache> {
        &self.ir_cache
    }

    fn metadata(&self) -> &Self::Metadata
    where
        Self: Sized,
    {
        &self.metadata
    }

    fn clear_metadata(&mut self) {
        self.metadata.tables.clear();
    }

    fn is_metadata_empty(&self) -> bool {
        self.metadata.tables.is_empty()
    }

    fn get_configuration(&self) -> Result<Option<RouterConfiguration>, QueryPlannerError> {
        let metadata = RouterConfiguration::new();
        Ok(Some(metadata))
    }

    fn update_configuration(&mut self, _metadata: RouterConfiguration) {
        self.metadata = MetadataMock::new();
    }

    fn materialize_motion(
        &self,
        _plan: &mut ExecutionPlan,
        motion_node_id: usize,
        _buckets: &Buckets,
    ) -> Result<VirtualTable, QueryPlannerError> {
        if let Some(virtual_table) = self.virtual_tables.get(&motion_node_id) {
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
        let result = ProducerResult::new();
        let nodes = plan.get_sql_order(top_id)?;
        plan.syntax_nodes_as_sql(&nodes, buckets)?;

        Ok(Box::new(result))
    }

    fn extract_sharding_keys<'engine, 'rec>(
        &'engine self,
        space: String,
        args: &'rec HashMap<String, Value>,
    ) -> Result<Vec<&'rec Value>, QueryPlannerError> {
        Ok(self
            .metadata()
            .get_sharding_key_by_space(&space)
            .unwrap()
            .iter()
            .fold(Vec::new(), |mut acc: Vec<&Value>, &v| {
                acc.push(args.get(v).unwrap());
                acc
            }))
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
            metadata: MetadataMock::new(),
            virtual_tables: HashMap::new(),
            ir_cache: RefCell::new(cache),
        }
    }

    #[allow(dead_code)]
    pub fn add_virtual_table(&mut self, id: usize, table: VirtualTable) {
        self.virtual_tables.insert(id, table);
    }
}
