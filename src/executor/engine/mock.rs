use std::collections::HashMap;

use crate::errors::QueryPlannerError;
use crate::executor::Metadata;
use crate::ir::relation::{Column, Table, Type};

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
            Column::new("identification_number", Type::Integer),
            Column::new("product_code", Type::String),
            Column::new("product_units", Type::Boolean),
            Column::new("sys_op", Type::Number),
            Column::new("bucket_id", Type::Unsigned),
        ];
        let sharding_key = vec!["identification_number", "product_code"];
        tables.insert(
            "hash_testing".to_string(),
            Table::new_seg("hash_testing", columns.clone(), &sharding_key).unwrap(),
        );

        tables.insert(
            "hash_testing_hist".to_string(),
            Table::new_seg("hash_testing_hist", columns.clone(), &sharding_key).unwrap(),
        );

        let columns = vec![
            Column::new("id", Type::Number),
            Column::new("sysFrom", Type::Number),
            Column::new("FIRST_NAME", Type::String),
            Column::new("sys_op", Type::Number),
            Column::new("bucket_id", Type::Unsigned),
        ];
        let sharding_key = vec!["id"];

        tables.insert(
            "test_space".to_string(),
            Table::new_seg("test_space", columns.clone(), &sharding_key).unwrap(),
        );

        tables.insert(
            "test_space_hist".to_string(),
            Table::new_seg("test_space_hist", columns.clone(), &sharding_key).unwrap(),
        );

        MetadataMock { tables }
    }
}
