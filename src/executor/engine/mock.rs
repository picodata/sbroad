use std::collections::HashMap;

use crate::errors::QueryPlannerError;
use crate::executor::engine::Engine;
use crate::executor::result::{BoxExecuteFormat, Value};
use crate::executor::Metadata;
use crate::ir::relation::{Column, Table, Type};

#[derive(Debug, Clone)]
pub struct MetadataMock {
    tables: HashMap<String, Table>,
}

impl Metadata for MetadataMock {
    fn get_table_segment(&self, table_name: &str) -> Result<Table, QueryPlannerError> {
        match self.tables.get(table_name) {
            Some(v) => Ok(v.clone()),
            None => Err(QueryPlannerError::SpaceNotFound),
        }
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

        MetadataMock { tables }
    }
}

#[derive(Debug, Clone)]
pub struct EngineMock {
    metadata: MetadataMock,
    pub shard_query: BoxExecuteFormat,
    pub mp_query: BoxExecuteFormat,
}

impl Engine for EngineMock {
    type Metadata = MetadataMock;

    fn metadata(&self) -> Self::Metadata
    where
        Self: Sized,
    {
        self.metadata.clone()
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

    fn exec_query(
        &self,
        _shard_key: &str,
        _query: &str,
    ) -> Result<BoxExecuteFormat, QueryPlannerError> {
        Ok(self.shard_query.clone())
    }

    fn mp_exec_query(&self, _query: &str) -> Result<BoxExecuteFormat, QueryPlannerError> {
        Ok(self.mp_query.clone())
    }

    fn determine_bucket_id(&self, _s: &str) -> usize {
        return 1;
    }
}

impl EngineMock {
    pub fn new() -> Self {
        EngineMock {
            metadata: MetadataMock::new(),
            shard_query: BoxExecuteFormat {
                metadata: vec![Column {
                    name: "FIRST_NAME".into(),
                    r#type: Type::String,
                }],
                rows: vec![vec![Value::String(String::from("is shard query"))]],
            },
            mp_query: BoxExecuteFormat {
                metadata: vec![Column {
                    name: "product_code".into(),
                    r#type: Type::String,
                }],
                rows: vec![vec![Value::String(String::from("is map reduce query"))]],
            },
        }
    }
}
