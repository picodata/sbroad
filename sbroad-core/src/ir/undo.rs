//! Undo transformation module.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Transformation log keep the history of the plan subtree modifications.
/// When we modify the plan subtree, we add a new entry to the log, where
/// the key is a new subtree top node and the value is the previous version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransformationLog {
    log: HashMap<usize, usize>,
}

impl Default for TransformationLog {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformationLog {
    #[must_use]
    pub fn new() -> Self {
        Self {
            log: HashMap::new(),
        }
    }

    pub fn add(&mut self, old_id: usize, new_id: usize) {
        self.log.insert(new_id, old_id);
    }

    #[must_use]
    pub fn get(&self, new_id: &usize) -> Option<&usize> {
        self.log.get(new_id)
    }

    #[must_use]
    pub fn get_oldest(&self, new_id: &usize) -> Option<&usize> {
        match self.log.get_key_value(new_id) {
            None => None,
            Some((id, _)) => {
                let mut current = id;
                while let Some(parent) = self.get(current) {
                    current = parent;
                }
                Some(current)
            }
        }
    }
}
