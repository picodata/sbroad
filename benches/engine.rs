extern crate sbroad;

use ahash::RandomState;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use sbroad::collection;
use sbroad::errors::QueryPlannerError;
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::cartridge::cache::lru::{LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::engine::cartridge::hash::bucket_id_by_tuple;
use sbroad::executor::engine::{Engine, LocalMetadata, Metadata, QueryCache};
use sbroad::executor::ir::ExecutionPlan;
use sbroad::executor::result::{ExecutorResults, ProducerResults};
use sbroad::executor::vtable::VirtualTable;
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::ir::relation::{Column, Table, Type};
use sbroad::ir::value::Value as IrValue;
use sbroad::ir::Plan;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct MetadataMock {
    schema: HashMap<String, Vec<String>>,
    tables: HashMap<String, Table>,
    bucket_count: usize,
    system_columns: HashSet<String, RandomState>,
}

impl Metadata for MetadataMock {
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

    fn get_system_columns(&self) -> &HashSet<String, RandomState> {
        &self.system_columns
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
            Column::new("\"vehicleguid\"", Type::Number, false),
            Column::new("\"reestrid\"", Type::Number, false),
            Column::new("\"reestrstatus\"", Type::Number, false),
            Column::new("\"vehicleregno\"", Type::Number, false),
            Column::new("\"vehiclevin\"", Type::Number, false),
            Column::new("\"vehiclevin2\"", Type::Number, false),
            Column::new("\"vehiclechassisnum\"", Type::Number, false),
            Column::new("\"vehiclereleaseyear\"", Type::Number, false),
            Column::new("\"operationregdoctypename\"", Type::Number, false),
            Column::new("\"operationregdoc\"", Type::Number, false),
            Column::new("\"operationregdocissuedate\"", Type::Number, false),
            Column::new("\"operationregdoccomments\"", Type::Number, false),
            Column::new("\"vehicleptstypename\"", Type::Number, false),
            Column::new("\"vehicleptsnum\"", Type::Number, false),
            Column::new("\"vehicleptsissuedate\"", Type::Number, false),
            Column::new("\"vehicleptsissuer\"", Type::Number, false),
            Column::new("\"vehicleptscomments\"", Type::Number, false),
            Column::new("\"vehiclebodycolor\"", Type::Number, false),
            Column::new("\"vehiclebrand\"", Type::Number, false),
            Column::new("\"vehiclemodel\"", Type::Number, false),
            Column::new("\"vehiclebrandmodel\"", Type::Number, false),
            Column::new("\"vehiclebodynum\"", Type::Number, false),
            Column::new("\"vehiclecost\"", Type::Number, false),
            Column::new("\"vehiclegasequip\"", Type::Number, false),
            Column::new("\"vehicleproducername\"", Type::Number, false),
            Column::new("\"vehiclegrossmass\"", Type::Number, false),
            Column::new("\"vehiclemass\"", Type::Number, false),
            Column::new("\"vehiclesteeringwheeltypeid\"", Type::Number, false),
            Column::new("\"vehiclekpptype\"", Type::Number, false),
            Column::new("\"vehicletransmissiontype\"", Type::Number, false),
            Column::new("\"vehicletypename\"", Type::Number, false),
            Column::new("\"vehiclecategory\"", Type::Number, false),
            Column::new("\"vehicletypeunit\"", Type::Number, false),
            Column::new("\"vehicleecoclass\"", Type::Number, false),
            Column::new("\"vehiclespecfuncname\"", Type::Number, false),
            Column::new("\"vehicleenclosedvolume\"", Type::Number, false),
            Column::new("\"vehicleenginemodel\"", Type::Number, false),
            Column::new("\"vehicleenginenum\"", Type::Number, false),
            Column::new("\"vehicleenginepower\"", Type::Number, false),
            Column::new("\"vehicleenginepowerkw\"", Type::Number, false),
            Column::new("\"vehicleenginetype\"", Type::Number, false),
            Column::new("\"holdrestrictiondate\"", Type::Number, false),
            Column::new("\"approvalnum\"", Type::Number, false),
            Column::new("\"approvaldate\"", Type::Number, false),
            Column::new("\"approvaltype\"", Type::Number, false),
            Column::new("\"utilizationfeename\"", Type::Number, false),
            Column::new("\"customsdoc\"", Type::Number, false),
            Column::new("\"customsdocdate\"", Type::Number, false),
            Column::new("\"customsdocissue\"", Type::Number, false),
            Column::new("\"customsdocrestriction\"", Type::Number, false),
            Column::new("\"customscountryremovalid\"", Type::Number, false),
            Column::new("\"customscountryremovalname\"", Type::Number, false),
            Column::new("\"ownerorgname\"", Type::Number, false),
            Column::new("\"ownerinn\"", Type::Number, false),
            Column::new("\"ownerogrn\"", Type::Number, false),
            Column::new("\"ownerkpp\"", Type::Number, false),
            Column::new("\"ownerpersonlastname\"", Type::Number, false),
            Column::new("\"ownerpersonfirstname\"", Type::Number, false),
            Column::new("\"ownerpersonmiddlename\"", Type::Number, false),
            Column::new("\"ownerpersonbirthdate\"", Type::Number, false),
            Column::new("\"ownerbirthplace\"", Type::Number, false),
            Column::new("\"ownerpersonogrnip\"", Type::Number, false),
            Column::new("\"owneraddressindex\"", Type::Number, false),
            Column::new("\"owneraddressmundistrict\"", Type::Number, false),
            Column::new("\"owneraddresssettlement\"", Type::Number, false),
            Column::new("\"owneraddressstreet\"", Type::Number, false),
            Column::new("\"ownerpersoninn\"", Type::Number, false),
            Column::new("\"ownerpersondoccode\"", Type::Number, false),
            Column::new("\"ownerpersondocnum\"", Type::Number, false),
            Column::new("\"ownerpersondocdate\"", Type::Number, false),
            Column::new("\"operationname\"", Type::Number, false),
            Column::new("\"operationdate\"", Type::Number, false),
            Column::new("\"operationdepartmentname\"", Type::Number, false),
            Column::new("\"operationattorney\"", Type::Number, false),
            Column::new("\"operationlising\"", Type::Number, false),
            Column::new("\"holdertypeid\"", Type::Number, false),
            Column::new("\"holderpersondoccode\"", Type::Number, false),
            Column::new("\"holderpersondocnum\"", Type::Number, false),
            Column::new("\"holderpersondocdate\"", Type::Number, false),
            Column::new("\"holderpersondocissuer\"", Type::Number, false),
            Column::new("\"holderpersonlastname\"", Type::Number, false),
            Column::new("\"holderpersonfirstname\"", Type::Number, false),
            Column::new("\"holderpersonmiddlename\"", Type::Number, false),
            Column::new("\"holderpersonbirthdate\"", Type::Number, false),
            Column::new("\"holderpersonbirthregionid\"", Type::Number, false),
            Column::new("\"holderpersonsex\"", Type::Number, false),
            Column::new("\"holderpersonbirthplace\"", Type::Number, false),
            Column::new("\"holderpersoninn\"", Type::Number, false),
            Column::new("\"holderpersonsnils\"", Type::Number, false),
            Column::new("\"holderpersonogrnip\"", Type::Number, false),
            Column::new("\"holderaddressguid\"", Type::Number, false),
            Column::new("\"holderaddressregionid\"", Type::Number, false),
            Column::new("\"holderaddressregionname\"", Type::Number, false),
            Column::new("\"holderaddressdistrict\"", Type::Number, false),
            Column::new("\"holderaddressmundistrict\"", Type::Number, false),
            Column::new("\"holderaddresssettlement\"", Type::Number, false),
            Column::new("\"holderaddressstreet\"", Type::Number, false),
            Column::new("\"holderaddressbuilding\"", Type::Number, false),
            Column::new("\"holderaddressstructureid\"", Type::Number, false),
            Column::new("\"holderaddressstructurename\"", Type::Number, false),
            Column::new("\"holderaddressstructure\"", Type::Number, false),
            Column::new("\"sys_from\"", Type::Number, false),
            Column::new("\"sys_to\"", Type::Number, false),
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
            system_columns: collection! { "\"bucket_id\"".into() },
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct EngineMock {
    metadata: MetadataMock,
    virtual_tables: HashMap<usize, VirtualTable>,
    query_cache: RefCell<LRUCache<String, Plan>>,
}

impl Engine for EngineMock {
    type Metadata = MetadataMock;
    type ParseTree = AbstractSyntaxTree;
    type QueryCache = LRUCache<String, Plan>;

    fn clear_query_cache(&self, capacity: usize) -> Result<(), QueryPlannerError> {
        *self.query_cache.borrow_mut() = Self::QueryCache::new(capacity)?;
        Ok(())
    }

    fn query_cache(&self) -> &RefCell<Self::QueryCache> {
        &self.query_cache
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

    fn get_metadata(&self) -> Result<Option<LocalMetadata>, QueryPlannerError> {
        let metadata = LocalMetadata {
            schema: "".into(),
            timeout: 0,
            capacity: DEFAULT_CAPACITY,
            system_columns: HashSet::with_hasher(RandomState::new()),
        };
        Ok(Some(metadata))
    }

    fn update_metadata(&mut self, _metadata: LocalMetadata) -> Result<(), QueryPlannerError> {
        self.metadata = MetadataMock::new();
        Ok(())
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

    fn exec(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
        buckets: &Buckets,
    ) -> Result<ExecutorResults, QueryPlannerError> {
        let result = ProducerResults::new();
        let nodes = plan.get_sql_order(top_id)?;
        plan.syntax_nodes_as_sql(&nodes, buckets)?;

        Ok(ExecutorResults::from(result))
    }

    fn extract_sharding_keys<'engine, 'rec>(
        &'engine self,
        space: String,
        args: &'rec HashMap<String, IrValue>,
    ) -> Result<Vec<&'rec IrValue>, QueryPlannerError> {
        Ok(self
            .metadata()
            .get_sharding_key_by_space(&space)
            .unwrap()
            .iter()
            .fold(Vec::new(), |mut acc: Vec<&IrValue>, &v| {
                acc.push(args.get(v).unwrap());
                acc
            }))
    }

    fn determine_bucket_id(&self, s: &[&IrValue]) -> u64 {
        bucket_id_by_tuple(s, self.metadata.bucket_count)
    }
}

impl Default for EngineMock {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineMock {
    #[allow(dead_code)]
    #[must_use]
    pub fn new() -> Self {
        let cache: LRUCache<String, Plan> = LRUCache::new(DEFAULT_CAPACITY).unwrap();
        EngineMock {
            metadata: MetadataMock::new(),
            virtual_tables: HashMap::new(),
            query_cache: RefCell::new(cache),
        }
    }

    #[allow(dead_code)]
    pub fn add_virtual_table(&mut self, id: usize, table: VirtualTable) {
        self.virtual_tables.insert(id, table);
    }
}
