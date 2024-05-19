//! IR parameters module.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ir::Node;

use super::expression::NodeId;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Parameters(HashMap<NodeId, Node>);

impl Default for Parameters {
    fn default() -> Self {
        Self::new()
    }
}

impl Parameters {
    #[must_use]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert(&mut self, index: NodeId, node: Node) {
        self.0.insert(index, node);
    }

    #[must_use]
    pub fn get(&self, index: NodeId) -> Option<&Node> {
        self.0.get(&index)
    }

    pub fn drain(&mut self) -> HashMap<NodeId, Node> {
        std::mem::take(&mut self.0)
    }
}
