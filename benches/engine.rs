extern crate sbroad;

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;

use sbroad::errors::QueryPlannerError;
use sbroad::executor::bucket::Buckets;
use sbroad::executor::engine::cartridge::cache::lru::{LRUCache, DEFAULT_CAPACITY};
use sbroad::executor::engine::cartridge::hash::bucket_id_by_tuple;
use sbroad::executor::engine::{Engine, LocalMetadata, Metadata, QueryCache};
use sbroad::executor::ir::ExecutionPlan;
use sbroad::executor::result::ProducerResult;
use sbroad::executor::vtable::VirtualTable;
use sbroad::frontend::sql::ast::AbstractSyntaxTree;
use sbroad::ir::relation::{Column, ColumnRole, Table, Type};
use sbroad::ir::value::Value as IrValue;
use sbroad::ir::Plan;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone)]
pub struct MetadataMock {
    schema: HashMap<String, Vec<String>>,
    tables: HashMap<String, Table>,
    bucket_count: usize,
    sharding_column: String,
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
            Column::new("\"vehicleguid\"", Type::Number, ColumnRole::User),
            Column::new("\"reestrid\"", Type::Number, ColumnRole::User),
            Column::new("\"reestrstatus\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleregno\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclevin\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclevin2\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclechassisnum\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclereleaseyear\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"operationregdoctypename\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"operationregdoc\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"operationregdocissuedate\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new(
                "\"operationregdoccomments\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"vehicleptstypename\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleptsnum\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleptsissuedate\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleptsissuer\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleptscomments\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclebodycolor\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclebrand\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclemodel\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclebrandmodel\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclebodynum\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclecost\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclegasequip\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleproducername\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclegrossmass\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclemass\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"vehiclesteeringwheeltypeid\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"vehiclekpptype\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"vehicletransmissiontype\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"vehicletypename\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclecategory\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicletypeunit\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleecoclass\"", Type::Number, ColumnRole::User),
            Column::new("\"vehiclespecfuncname\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleenclosedvolume\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleenginemodel\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleenginenum\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleenginepower\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleenginepowerkw\"", Type::Number, ColumnRole::User),
            Column::new("\"vehicleenginetype\"", Type::Number, ColumnRole::User),
            Column::new("\"holdrestrictiondate\"", Type::Number, ColumnRole::User),
            Column::new("\"approvalnum\"", Type::Number, ColumnRole::User),
            Column::new("\"approvaldate\"", Type::Number, ColumnRole::User),
            Column::new("\"approvaltype\"", Type::Number, ColumnRole::User),
            Column::new("\"utilizationfeename\"", Type::Number, ColumnRole::User),
            Column::new("\"customsdoc\"", Type::Number, ColumnRole::User),
            Column::new("\"customsdocdate\"", Type::Number, ColumnRole::User),
            Column::new("\"customsdocissue\"", Type::Number, ColumnRole::User),
            Column::new("\"customsdocrestriction\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"customscountryremovalid\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new(
                "\"customscountryremovalname\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"ownerorgname\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerinn\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerogrn\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerkpp\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerpersonlastname\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerpersonfirstname\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerpersonmiddlename\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerpersonbirthdate\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerbirthplace\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerpersonogrnip\"", Type::Number, ColumnRole::User),
            Column::new("\"owneraddressindex\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"owneraddressmundistrict\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"owneraddresssettlement\"", Type::Number, ColumnRole::User),
            Column::new("\"owneraddressstreet\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerpersoninn\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerpersondoccode\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerpersondocnum\"", Type::Number, ColumnRole::User),
            Column::new("\"ownerpersondocdate\"", Type::Number, ColumnRole::User),
            Column::new("\"operationname\"", Type::Number, ColumnRole::User),
            Column::new("\"operationdate\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"operationdepartmentname\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"operationattorney\"", Type::Number, ColumnRole::User),
            Column::new("\"operationlising\"", Type::Number, ColumnRole::User),
            Column::new("\"holdertypeid\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersondoccode\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersondocnum\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersondocdate\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersondocissuer\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersonlastname\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersonfirstname\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersonmiddlename\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersonbirthdate\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"holderpersonbirthregionid\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"holderpersonsex\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersonbirthplace\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersoninn\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersonsnils\"", Type::Number, ColumnRole::User),
            Column::new("\"holderpersonogrnip\"", Type::Number, ColumnRole::User),
            Column::new("\"holderaddressguid\"", Type::Number, ColumnRole::User),
            Column::new("\"holderaddressregionid\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"holderaddressregionname\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"holderaddressdistrict\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"holderaddressmundistrict\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderaddresssettlement\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"holderaddressstreet\"", Type::Number, ColumnRole::User),
            Column::new("\"holderaddressbuilding\"", Type::Number, ColumnRole::User),
            Column::new(
                "\"holderaddressstructureid\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new(
                "\"holderaddressstructurename\"",
                Type::Number,
                ColumnRole::User,
            ),
            Column::new("\"holderaddressstructure\"", Type::Number, ColumnRole::User),
            Column::new("\"sys_from\"", Type::Number, ColumnRole::User),
            Column::new("\"sys_to\"", Type::Number, ColumnRole::User),
            Column::new("\"bucket_id\"", Type::Number, ColumnRole::Sharding),
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
            sharding_column: "".into(),
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
    ) -> Result<Box<dyn Any>, QueryPlannerError> {
        let result = ProducerResult::new();
        let nodes = plan.get_sql_order(top_id)?;
        plan.syntax_nodes_as_sql(&nodes, buckets)?;

        Ok(Box::new(result))
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
