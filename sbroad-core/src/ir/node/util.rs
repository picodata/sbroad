use serde::Serialize;
use crate::ir::node::{Invalid as Inv, Parameter as Param};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum Invalid<'a> {
    /// Procedure body.
    Invalid(&'a Inv),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum Parameter<'a> {
    /// Procedure body.
    Parameter(&'a Param),
}