use crate::ir::{Entity, Node, Plan, SbroadError};
use serde::{Deserialize, Serialize};
use tarantool::decimal::Decimal;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum Acl {
    DropRole {
        name: String,
        timeout: Decimal,
    },
    DropUser {
        name: String,
        timeout: Decimal,
    },
    CreateRole {
        name: String,
        timeout: Decimal,
    },
    CreateUser {
        name: String,
        password: String,
        auth_method: String,
        timeout: Decimal,
    },
}

impl Acl {
    /// Return ACL node timeout.
    ///
    /// # Errors
    /// - timeout parsing error
    pub fn timeout(&self) -> Result<f64, SbroadError> {
        match self {
            Acl::DropRole { ref timeout, .. }
            | Acl::DropUser { ref timeout, .. }
            | Acl::CreateRole { ref timeout, .. }
            | Acl::CreateUser { ref timeout, .. } => timeout,
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
    /// Get ACL node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of ACL type
    pub fn get_acl_node(&self, node_id: usize) -> Result<&Acl, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Acl(acl) => Ok(acl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not ACL type: {node:?}")),
            )),
        }
    }

    /// Get a mutable ACL node from the plan arena.
    ///
    /// # Errors
    /// - the node index is absent in arena
    /// - current node is not of ACL type
    pub fn get_mut_acl_node(&mut self, node_id: usize) -> Result<&mut Acl, SbroadError> {
        let node = self.get_mut_node(node_id)?;
        match node {
            Node::Acl(acl) => Ok(acl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not ACL type: {node:?}")),
            )),
        }
    }

    /// Take ACL node from the plan arena and replace it with parameter node.
    ///
    /// # Errors
    /// - current node is not of ACL type
    pub fn take_acl_node(&mut self, node_id: usize) -> Result<Acl, SbroadError> {
        // Check that node is ACL type (before making any distructive operations).
        let _ = self.get_acl_node(node_id)?;
        // Replace ACL with parameter node.
        let node = std::mem::replace(self.get_mut_node(node_id)?, Node::Parameter);
        match node {
            Node::Acl(acl) => Ok(acl),
            _ => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node is not ACL type: {node:?}")),
            )),
        }
    }
}
