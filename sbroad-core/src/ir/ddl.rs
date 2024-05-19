use crate::ir::value::Value;
use crate::{
    errors::{Entity, SbroadError},
    ir::{relation::Type as RelationType, Node, Plan},
};
use serde::{Deserialize, Serialize};
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use tarantool::space::SpaceEngineType;
use tarantool::{
    decimal::Decimal,
    index::{IndexType, RtreeIndexDistanceType},
};

use super::expression::NodeId;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct ColumnDef {
    pub name: SmolStr,
    pub data_type: RelationType,
    pub is_nullable: bool,
}

impl Default for ColumnDef {
    fn default() -> Self {
        Self {
            name: SmolStr::default(),
            data_type: RelationType::default(),
            is_nullable: true,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct ParamDef {
    pub data_type: RelationType,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub enum Language {
    #[default]
    SQL,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum SetParamScopeType {
    Local,
    Session,
}

// TODO: Fill with actual values.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum SetParamValue {
    NamedParam { name: SmolStr },
    TimeZone,
}

impl SetParamValue {
    #[must_use]
    pub fn param_name(&self) -> SmolStr {
        match self {
            SetParamValue::NamedParam { name } => name.clone(),
            SetParamValue::TimeZone => SmolStr::from("TimeZone"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum AlterSystemType {
    AlterSystemSet {
        param_name: SmolStr,
        param_value: Value,
    },
    AlterSystemReset {
        /// In case of None, all params are supposed to be reset.
        param_name: Option<SmolStr>,
    },
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
        /// Shows which tier the sharded table belongs to.
        /// Field has value, only if it was specified in [ON TIER] part of CREATE TABLE statement.
        /// Field is None, if:
        /// 1) Global table.
        /// 2) Sharded table without [ON TIER] part. In this case picodata will use default tier.
        tier: Option<SmolStr>,
    },
    DropTable {
        name: SmolStr,
        timeout: Decimal,
    },
    AlterSystem {
        ty: AlterSystemType,
        /// In case of None, ALTER is supposed
        /// to be executed on all tiers.
        tier_name: Option<SmolStr>,
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
    SetParam {
        scope_type: SetParamScopeType,
        param_value: SetParamValue,
        timeout: Decimal,
    },
    // TODO: Fill with actual values.
    SetTransaction {
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
            | Ddl::AlterSystem { ref timeout, .. }
            | Ddl::SetParam { ref timeout, .. }
            | Ddl::SetTransaction { ref timeout, .. }
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
    pub fn get_ddl_node(&self, node_id: NodeId) -> Result<&Ddl, SbroadError> {
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
    pub fn get_mut_ddl_node(&mut self, node_id: NodeId) -> Result<&mut Ddl, SbroadError> {
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
    pub fn take_ddl_node(&mut self, node_id: NodeId) -> Result<Ddl, SbroadError> {
        // Check that node is DDL type (before making any distructive operations).
        let _ = self.get_ddl_node(node_id)?;
        // Replace DDL with parameter node.
        let node = std::mem::replace(self.get_mut_node(node_id)?, Node::Parameter(None));
        match node {
            Node::Ddl(ddl) => Ok(ddl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!("node is not DDL type: {node:?}")),
            )),
        }
    }
}
