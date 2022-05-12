extern crate sbroad;

use std::cell::RefCell;
use std::collections::HashMap;

use sbroad::errors::QueryPlannerError;
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::cartridge::cache::lru::{LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::engine::cartridge::hash::bucket_id_by_tuple;
use sbroad::executor::engine::{Engine, LocalMetadata, Metadata, QueryCache};
use sbroad::executor::ir::ExecutionPlan;
use sbroad::executor::result::BoxExecuteFormat;
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
            Column::new("\"vehicleguid\"", Type::Number),
            Column::new("\"reestrid\"", Type::Number),
            Column::new("\"reestrstatus\"", Type::Number),
            Column::new("\"vehicleregno\"", Type::Number),
            Column::new("\"vehiclevin\"", Type::Number),
            Column::new("\"vehiclevin2\"", Type::Number),
            Column::new("\"vehiclechassisnum\"", Type::Number),
            Column::new("\"vehiclereleaseyear\"", Type::Number),
            Column::new("\"operationregdoctypename\"", Type::Number),
            Column::new("\"operationregdoc\"", Type::Number),
            Column::new("\"operationregdocissuedate\"", Type::Number),
            Column::new("\"operationregdoccomments\"", Type::Number),
            Column::new("\"vehicleptstypename\"", Type::Number),
            Column::new("\"vehicleptsnum\"", Type::Number),
            Column::new("\"vehicleptsissuedate\"", Type::Number),
            Column::new("\"vehicleptsissuer\"", Type::Number),
            Column::new("\"vehicleptscomments\"", Type::Number),
            Column::new("\"vehiclebodycolor\"", Type::Number),
            Column::new("\"vehiclebrand\"", Type::Number),
            Column::new("\"vehiclemodel\"", Type::Number),
            Column::new("\"vehiclebrandmodel\"", Type::Number),
            Column::new("\"vehiclebodynum\"", Type::Number),
            Column::new("\"vehiclecost\"", Type::Number),
            Column::new("\"vehiclegasequip\"", Type::Number),
            Column::new("\"vehicleproducername\"", Type::Number),
            Column::new("\"vehiclegrossmass\"", Type::Number),
            Column::new("\"vehiclemass\"", Type::Number),
            Column::new("\"vehiclesteeringwheeltypeid\"", Type::Number),
            Column::new("\"vehiclekpptype\"", Type::Number),
            Column::new("\"vehicletransmissiontype\"", Type::Number),
            Column::new("\"vehicletypename\"", Type::Number),
            Column::new("\"vehiclecategory\"", Type::Number),
            Column::new("\"vehicletypeunit\"", Type::Number),
            Column::new("\"vehicleecoclass\"", Type::Number),
            Column::new("\"vehiclespecfuncname\"", Type::Number),
            Column::new("\"vehicleenclosedvolume\"", Type::Number),
            Column::new("\"vehicleenginemodel\"", Type::Number),
            Column::new("\"vehicleenginenum\"", Type::Number),
            Column::new("\"vehicleenginepower\"", Type::Number),
            Column::new("\"vehicleenginepowerkw\"", Type::Number),
            Column::new("\"vehicleenginetype\"", Type::Number),
            Column::new("\"holdrestrictiondate\"", Type::Number),
            Column::new("\"approvalnum\"", Type::Number),
            Column::new("\"approvaldate\"", Type::Number),
            Column::new("\"approvaltype\"", Type::Number),
            Column::new("\"utilizationfeename\"", Type::Number),
            Column::new("\"customsdoc\"", Type::Number),
            Column::new("\"customsdocdate\"", Type::Number),
            Column::new("\"customsdocissue\"", Type::Number),
            Column::new("\"customsdocrestriction\"", Type::Number),
            Column::new("\"customscountryremovalid\"", Type::Number),
            Column::new("\"customscountryremovalname\"", Type::Number),
            Column::new("\"ownerorgname\"", Type::Number),
            Column::new("\"ownerinn\"", Type::Number),
            Column::new("\"ownerogrn\"", Type::Number),
            Column::new("\"ownerkpp\"", Type::Number),
            Column::new("\"ownerpersonlastname\"", Type::Number),
            Column::new("\"ownerpersonfirstname\"", Type::Number),
            Column::new("\"ownerpersonmiddlename\"", Type::Number),
            Column::new("\"ownerpersonbirthdate\"", Type::Number),
            Column::new("\"ownerbirthplace\"", Type::Number),
            Column::new("\"ownerpersonogrnip\"", Type::Number),
            Column::new("\"owneraddressindex\"", Type::Number),
            Column::new("\"owneraddressmundistrict\"", Type::Number),
            Column::new("\"owneraddresssettlement\"", Type::Number),
            Column::new("\"owneraddressstreet\"", Type::Number),
            Column::new("\"ownerpersoninn\"", Type::Number),
            Column::new("\"ownerpersondoccode\"", Type::Number),
            Column::new("\"ownerpersondocnum\"", Type::Number),
            Column::new("\"ownerpersondocdate\"", Type::Number),
            Column::new("\"operationname\"", Type::Number),
            Column::new("\"operationdate\"", Type::Number),
            Column::new("\"operationdepartmentname\"", Type::Number),
            Column::new("\"operationattorney\"", Type::Number),
            Column::new("\"operationlising\"", Type::Number),
            Column::new("\"holdertypeid\"", Type::Number),
            Column::new("\"holderpersondoccode\"", Type::Number),
            Column::new("\"holderpersondocnum\"", Type::Number),
            Column::new("\"holderpersondocdate\"", Type::Number),
            Column::new("\"holderpersondocissuer\"", Type::Number),
            Column::new("\"holderpersonlastname\"", Type::Number),
            Column::new("\"holderpersonfirstname\"", Type::Number),
            Column::new("\"holderpersonmiddlename\"", Type::Number),
            Column::new("\"holderpersonbirthdate\"", Type::Number),
            Column::new("\"holderpersonbirthregionid\"", Type::Number),
            Column::new("\"holderpersonsex\"", Type::Number),
            Column::new("\"holderpersonbirthplace\"", Type::Number),
            Column::new("\"holderpersoninn\"", Type::Number),
            Column::new("\"holderpersonsnils\"", Type::Number),
            Column::new("\"holderpersonogrnip\"", Type::Number),
            Column::new("\"holderaddressguid\"", Type::Number),
            Column::new("\"holderaddressregionid\"", Type::Number),
            Column::new("\"holderaddressregionname\"", Type::Number),
            Column::new("\"holderaddressdistrict\"", Type::Number),
            Column::new("\"holderaddressmundistrict\"", Type::Number),
            Column::new("\"holderaddresssettlement\"", Type::Number),
            Column::new("\"holderaddressstreet\"", Type::Number),
            Column::new("\"holderaddressbuilding\"", Type::Number),
            Column::new("\"holderaddressstructureid\"", Type::Number),
            Column::new("\"holderaddressstructurename\"", Type::Number),
            Column::new("\"holderaddressstructure\"", Type::Number),
            Column::new("\"sys_from\"", Type::Number),
            Column::new("\"sys_to\"", Type::Number),
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
    ) -> Result<BoxExecuteFormat, QueryPlannerError> {
        let result = BoxExecuteFormat::new();
        let nodes = plan.get_sql_order(top_id)?;
        plan.subtree_as_sql(&nodes, buckets)?;

        Ok(result)
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
