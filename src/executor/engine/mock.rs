use std::cell::RefCell;
use std::collections::HashMap;

use crate::errors::QueryPlannerError;
use crate::executor::engine::Engine;
use crate::executor::ir::ExecutionPlan;
use crate::executor::result::{BoxExecuteFormat, Value};
use crate::executor::vtable::VirtualTable;
use crate::executor::Metadata;
use crate::ir::operator::Relational;
use crate::ir::relation::{Column, Table, Type};
use crate::ir::value::Value as IrValue;

#[derive(Debug, Clone)]
pub struct MetadataMock {
    tables: HashMap<String, Table>,
}

impl Metadata for MetadataMock {
    fn get_table_segment(&self, table_name: &str) -> Result<Table, QueryPlannerError> {
        let name = Self::to_name(table_name);
        match self.tables.get(&name) {
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

        MetadataMock { tables }
    }
}

#[derive(Debug, Clone)]
pub struct EngineMock {
    metadata: MetadataMock,
    pub query_result: RefCell<BoxExecuteFormat>,
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

    fn materialize_motion(
        &self,
        plan: &mut ExecutionPlan,
        motion_node_id: usize,
    ) -> Result<VirtualTable, QueryPlannerError> {
        let sq_id = &plan.get_motion_child(motion_node_id)?;

        let mut vtable = VirtualTable::new();

        if let Relational::ScanSubQuery { alias, .. } =
            &plan.get_ir_plan().get_relation_node(*sq_id)?
        {
            if let Some(name) = alias {
                vtable.set_alias(name)?;
            }
        }

        vtable.add_column(Column {
            name: "identification_number".into(),
            r#type: Type::Integer,
        });

        vtable.add_values_tuple(vec![IrValue::number_from_str("2")?]);
        vtable.add_values_tuple(vec![IrValue::number_from_str("3")?]);

        Ok(vtable)
    }

    fn exec(
        &self,
        plan: &mut ExecutionPlan,
        top_id: usize,
    ) -> Result<BoxExecuteFormat, QueryPlannerError> {
        let sql = plan.subtree_as_sql(top_id)?;

        if let Some(shard_keys) = plan.discovery(top_id)? {
            for shard in shard_keys {
                self.exec_query(&shard, &sql)?;
            }
        } else {
            self.mp_exec_query(&sql)?;
        }

        let mut result = self.query_result.borrow_mut();

        // sorting for test reproduce
        result.rows.sort_by_key(|k| k[0].to_string());

        Ok(result.clone())
    }

    fn determine_bucket_id(&self, _s: &str) -> usize {
        1
    }
}

impl EngineMock {
    pub fn new() -> Self {
        let result_cell = RefCell::new(BoxExecuteFormat {
            metadata: vec![],
            rows: vec![],
        });

        EngineMock {
            metadata: MetadataMock::new(),
            query_result: result_cell,
        }
    }

    fn exec_query(
        &self,
        shard_key: &str,
        query: &str,
    ) -> Result<BoxExecuteFormat, QueryPlannerError> {
        let mut result = self.query_result.borrow_mut();

        result.rows.push(vec![
            Value::String(format!("query send to [{}] shard", shard_key)),
            Value::String(String::from(query)),
        ]);

        Ok(result.clone())
    }

    fn mp_exec_query(&self, query: &str) -> Result<BoxExecuteFormat, QueryPlannerError> {
        let mut result = self.query_result.borrow_mut();

        result.rows.push(vec![
            Value::String(String::from("query send to all shards")),
            Value::String(String::from(query)),
        ]);
        Ok(result.clone())
    }
}
