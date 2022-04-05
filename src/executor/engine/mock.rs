use std::collections::HashMap;

use crate::errors::QueryPlannerError;
use crate::executor::bucket::Buckets;
use crate::executor::engine::cartridge::hash::str_to_bucket_id;
use crate::executor::engine::Engine;
use crate::executor::ir::ExecutionPlan;
use crate::executor::result::{BoxExecuteFormat, Value};
use crate::executor::vtable::VirtualTable;
use crate::executor::Metadata;
use crate::ir::relation::{Column, Table, Type};

#[derive(Debug, Clone)]
pub struct MetadataMock {
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
}

impl MetadataMock {
    pub fn new() -> Self {
        let mut tables = HashMap::new();

        let columns = vec![
            Column::new("\"identification_number\"", Type::Integer),
            Column::new("\"product_code\"", Type::String),
            Column::new("\"product_units\"", Type::Boolean),
            Column::new("\"sys_op\"", Type::Number),
            Column::new("\"bucket_id\"", Type::Unsigned),
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
            Table::new_seg(
                "\"hash_single_testing_hist\"",
                columns.clone(),
                &sharding_key,
            )
            .unwrap(),
        );

        let columns = vec![
            Column::new("\"id\"", Type::Number),
            Column::new("\"sysFrom\"", Type::Number),
            Column::new("\"FIRST_NAME\"", Type::String),
            Column::new("\"sys_op\"", Type::Number),
            Column::new("\"bucket_id\"", Type::Unsigned),
        ];
        let sharding_key = vec!["\"id\""];

        tables.insert(
            "\"test_space\"".to_string(),
            Table::new_seg("\"test_space\"", columns.clone(), &sharding_key).unwrap(),
        );

        tables.insert(
            "\"test_space_hist\"".to_string(),
            Table::new_seg("\"test_space_hist\"", columns.clone(), &sharding_key).unwrap(),
        );

        let columns = vec![Column::new("\"id\"", Type::Number)];
        let sharding_key: &[&str] = &["\"id\""];
        tables.insert(
            "\"history\"".to_string(),
            Table::new_seg("\"history\"", columns.clone(), sharding_key).unwrap(),
        );

        let columns = vec![
            Column::new("\"a\"", Type::Number),
            Column::new("\"b\"", Type::Number),
            Column::new("\"c\"", Type::Number),
        ];
        let sharding_key: &[&str] = &["\"a\"", "\"b\""];
        tables.insert(
            "\"t\"".to_string(),
            Table::new_seg("\"t\"", columns.clone(), sharding_key).unwrap(),
        );

        MetadataMock {
            tables,
            bucket_count: 10000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EngineMock {
    metadata: MetadataMock,
    virtual_tables: HashMap<usize, VirtualTable>,
}

impl Engine for EngineMock {
    type Metadata = MetadataMock;

    fn metadata(&self) -> &Self::Metadata
    where
        Self: Sized,
    {
        &self.metadata
    }

    fn has_metadata(&self) -> bool {
        self.metadata.tables.is_empty()
    }

    fn clear_metadata(&mut self) {
        self.metadata.tables.clear()
    }

    fn load_metadata(&mut self) -> Result<(), QueryPlannerError> {
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
        let mut result = BoxExecuteFormat::new();
        let sql = plan.subtree_as_sql(top_id)?;

        match buckets {
            Buckets::All => {
                result.extend(cluster_exec_query(&sql)?)?;
            }
            Buckets::Filtered(list) => {
                for bucket in list {
                    let temp_result = bucket_exec_query(*bucket, &sql)?;
                    result.extend(temp_result)?;
                }
            }
        }

        // Sort results to make tests reproducible.
        result.rows.sort_by_key(|k| k[0].to_string());
        Ok(result)
    }

    fn determine_bucket_id(&self, s: &str) -> u64 {
        str_to_bucket_id(s, self.metadata.bucket_count)
    }
}

impl EngineMock {
    pub fn new() -> Self {
        EngineMock {
            metadata: MetadataMock::new(),
            virtual_tables: HashMap::new(),
        }
    }

    pub fn add_virtual_table(
        &mut self,
        id: usize,
        table: VirtualTable,
    ) -> Result<(), QueryPlannerError> {
        self.virtual_tables.insert(id, table);
        Ok(())
    }
}

fn bucket_exec_query(bucket: u64, query: &str) -> Result<BoxExecuteFormat, QueryPlannerError> {
    let mut result = BoxExecuteFormat::new();

    result.rows.push(vec![
        Value::String(format!("Execute query on a bucket [{}]", bucket)),
        Value::String(String::from(query)),
    ]);

    Ok(result.clone())
}

fn cluster_exec_query(query: &str) -> Result<BoxExecuteFormat, QueryPlannerError> {
    let mut result = BoxExecuteFormat::new();

    result.rows.push(vec![
        Value::String(String::from("Execute query on all buckets")),
        Value::String(String::from(query)),
    ]);
    Ok(result.clone())
}
