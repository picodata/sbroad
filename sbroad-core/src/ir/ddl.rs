use crate::{
    errors::{Entity, SbroadError},
    ir::{relation::Type, Node, Plan},
};
use serde::{Deserialize, Serialize};
use tarantool::decimal::Decimal;
use tarantool::space::SpaceEngineType;

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
        engine_type: SpaceEngineType,
        timeout: Decimal,
    },
    DropTable {
        name: String,
        timeout: Decimal,
    },
}

impl Ddl {
    /// Return DDL node timeout.
    ///
    /// # Errors
    /// - timeout parsing error
    pub fn timeout(&self) -> Result<f64, SbroadError> {
        match self {
            Ddl::CreateShardedTable { ref timeout, .. } | Ddl::DropTable { ref timeout, .. } => {
                timeout
            }
        }
        .to_string()
        .parse()
        .map_err(|e| {
            SbroadError::Invalid(
                Entity::SpaceMetadata,
                Some(format!("timeout parsing error {e:?}")),
            )
        })
    }
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

    /// Take DDL node from the plan arena and replace it with parameter node.
    ///
    /// # Errors
    /// - current node is not of DDL type
    pub fn take_ddl_node(&mut self, node_id: usize) -> Result<Ddl, SbroadError> {
        // Check that node is DDL type (before making any distructive operations).
        let _ = self.get_ddl_node(node_id)?;
        // Replace DDL with parameter node.
        let node = std::mem::replace(self.get_mut_node(node_id)?, Node::Parameter);
        match node {
            Node::Ddl(ddl) => Ok(ddl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not DDL type: {node:?}")),
            )),
        }
    }
}
