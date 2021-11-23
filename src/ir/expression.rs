use super::operator;
use super::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Branch {
    Both,
    Left,
    Right,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Expression {
    // a > 42
    // b in (select c from ...)
    Bool {
        left: usize,
        op: operator::Bool,
        right: usize,
    },
    // 42
    Constant {
        value: Value,
    },
    // &0 (left) as b
    Reference {
        alias: String,
        branch: Branch,
        /// expression position in the input row
        position: usize,
    },
    // (a, b, 1)
    Row {
        list: Vec<usize>,
    },
}

#[allow(dead_code)]
impl Expression {
    pub fn new_const(value: Value) -> Self {
        Expression::Constant { value }
    }

    pub fn new_ref(alias: String, branch: Branch, position: usize) -> Self {
        Expression::Reference {
            alias,
            branch,
            position,
        }
    }

    pub fn new_row(list: Vec<usize>) -> Self {
        Expression::Row { list }
    }

    pub fn new_bool(left: usize, op: operator::Bool, right: usize) -> Self {
        Expression::Bool { left, op, right }
    }
}
