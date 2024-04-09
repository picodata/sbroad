use crate::{
    errors::{Entity, SbroadError},
    ir::{relation::Type, Node, Plan},
};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use tarantool::space::SpaceEngineType;
use tarantool::{
    decimal::Decimal,
    index::{IndexType, RtreeIndexDistanceType},
};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ColumnDef {
    pub name: SmolStr,
    pub data_type: Type,
    pub is_nullable: bool,
}

impl Default for ColumnDef {
    fn default() -> Self {
        Self {
            name: SmolStr::default(),
            data_type: Type::default(),
            is_nullable: true,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct ParamDef {
    pub data_type: Type,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub enum Language {
    #[default]
    SQL,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Ddl {
    CreateTable {
        name: SmolStr,
        format: Vec<ColumnDef>,
        primary_key: Vec<SmolStr>,
        /// If `None`, create global table.
        sharding_key: Option<Vec<SmolStr>>,
        /// Vinyl is supported only for sharded tables.
        engine_type: SpaceEngineType,
        timeout: Decimal,
    },
    DropTable {
        name: SmolStr,
        timeout: Decimal,
    },
    CreateProc {
        name: SmolStr,
        params: Vec<ParamDef>,
        body: SmolStr,
        language: Language,
        timeout: Decimal,
    },
    DropProc {
        name: SmolStr,
        params: Option<Vec<ParamDef>>,
        timeout: Decimal,
    },
    RenameRoutine {
        old_name: SmolStr,
        new_name: SmolStr,
        params: Option<Vec<ParamDef>>,
        timeout: Decimal,
    },
    CreateIndex {
        name: SmolStr,
        table_name: SmolStr,
        columns: Vec<SmolStr>,
        unique: bool,
        index_type: IndexType,
        bloom_fpr: Option<Decimal>,
        page_size: Option<u32>,
        range_size: Option<u32>,
        run_count_per_level: Option<u32>,
        run_size_ratio: Option<Decimal>,
        dimension: Option<u32>,
        distance: Option<RtreeIndexDistanceType>,
        hint: Option<bool>,
        timeout: Decimal,
    },
    DropIndex {
        name: SmolStr,
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
            Ddl::CreateTable { ref timeout, .. }
            | Ddl::DropTable { ref timeout, .. }
            | Ddl::CreateIndex { ref timeout, .. }
            | Ddl::DropIndex { ref timeout, .. }
            | Ddl::CreateProc { ref timeout, .. }
            | Ddl::DropProc { ref timeout, .. }
            | Ddl::RenameRoutine { ref timeout, .. } => timeout,
        }
        .to_smolstr()
        .parse()
        .map_err(|e| {
            SbroadError::Invalid(
                Entity::SpaceMetadata,
                Some(format_smolstr!("timeout parsing error {e:?}")),
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
                Some(format_smolstr!("node is not DDL type: {node:?}")),
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
                Some(format_smolstr!("node is not DDL type: {node:?}")),
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
                Some(format_smolstr!("node is not DDL type: {node:?}")),
            )),
        }
    }
}
