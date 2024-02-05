use crate::errors::{Entity, SbroadError};
use crate::ir::{Node, NodeId, Plan};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Block {
    /// Procedure body.
    Procedure {
        /// The name of the procedure.
        name: String,
        /// Passed values to the procedure.
        values: Vec<NodeId>,
    },
}

impl Default for Block {
    fn default() -> Self {
        Block::Procedure {
            name: String::new(),
            values: vec![],
        }
    }
}

impl Plan {
    /// Get a reference to a block node.
    ///
    /// # Errors
    /// - the node is not a block node.
    pub fn get_block_node(&self, node_id: NodeId) -> Result<&Block, SbroadError> {
        let node = self.get_node(node_id)?;
        match node {
            Node::Block(block) => Ok(block),
            Node::Expression(_)
            | Node::Relational(_)
            | Node::Ddl(..)
            | Node::Acl(..)
            | Node::Parameter => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node {node:?} (id {node_id}) is not Block type")),
            )),
        }
    }

    /// Get a mutable reference to a block node.
    ///
    /// # Errors
    /// - the node is not a block node.
    pub fn get_mut_block_node(&mut self, node_id: NodeId) -> Result<&mut Block, SbroadError> {
        let node = self.get_mut_node(node_id)?;
        match node {
            Node::Block(block) => Ok(block),
            Node::Expression(_)
            | Node::Relational(_)
            | Node::Ddl(..)
            | Node::Acl(..)
            | Node::Parameter => Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("node {node:?} (id {node_id}) is not Block type")),
            )),
        }
    }
}
