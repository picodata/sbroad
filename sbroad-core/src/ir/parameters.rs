//! IR parameters module.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::ir::Node;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Parameters(HashMap<usize, Node>);

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

    pub fn insert(&mut self, index: usize, node: Node) {
        self.0.insert(index, node);
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Node> {
        self.0.get(&index)
    }

    pub fn drain(&mut self) -> HashMap<usize, Node> {
        std::mem::take(&mut self.0)
    }
}
