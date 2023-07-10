use crate::{
    errors::{Entity, SbroadError},
    ir::{relation::Type, Node, Plan},
};
use serde::{Deserialize, Serialize};
use tarantool::decimal::Decimal;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: Type,
    pub is_nullable: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Ddl {
    CreateShardedTable {
        name: String,
        format: Vec<ColumnDef>,
        primary_key: Vec<String>,
        sharding_key: Vec<String>,
        timeout: Decimal,
    },
}

impl Plan {
    /// Get DDL node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of DDL type
    pub fn get_ddl_node(&self, node_id: usize) -> Result<&Ddl, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Ddl(ddl) => Ok(ddl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not DDL type: {node:?}")),
            )),
        }
    }

    /// Get a mutable DDL node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of DDL type
    pub fn get_mut_ddl_node(&mut self, node_id: usize) -> Result<&mut Ddl, SbroadError> {
        let node = self.get_mut_node(node_id)?;
        match node {
            Node::Ddl(ddl) => Ok(ddl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not DDL type: {node:?}")),
            )),
        }
    }
}
