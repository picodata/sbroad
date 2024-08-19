use serde::Serialize;

use super::{Procedure, SizeNode};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum BlockOwned {
    /// Procedure body.
    Procedure(Procedure),
}

impl From<BlockOwned> for SizeNode {
    fn from(value: BlockOwned) -> Self {
        match value {
            BlockOwned::Procedure(proc) => proc.into(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Eq, PartialEq, Serialize)]
pub enum MutBlock<'a> {
    /// Procedure body.
    Procedure(&'a mut Procedure),
}

impl MutBlock<'_> {
    #[must_use]
    pub fn get_block_owned(&self) -> BlockOwned {
        match self {
            MutBlock::Procedure(proc) => BlockOwned::Procedure((*proc).clone()),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum Block<'a> {
    /// Procedure body.
    Procedure(&'a Procedure),
}

impl Block<'_> {
    #[must_use]
    pub fn get_block_owned(&self) -> BlockOwned {
        match self {
            Block::Procedure(proc) => BlockOwned::Procedure((*proc).clone()),
        }
    }
}
