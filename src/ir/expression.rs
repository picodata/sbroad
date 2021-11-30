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
    // 42 as a
    Alias {
        name: String,
        child: usize,
    },
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
    // &0 (left)
    Reference {
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
    pub fn new_alias(name: &str, child: usize) -> Self {
        Expression::Alias {
            name: String::from(name),
            child,
        }
    }

    pub fn new_const(value: Value) -> Self {
        Expression::Constant { value }
    }

    pub fn new_ref(branch: Branch, position: usize) -> Self {
        Expression::Reference { branch, position }
    }

    // TODO: check that doesn't contain top-level aliases with the same names
    pub fn new_row(list: Vec<usize>) -> Self {
        Expression::Row { list }
    }

    pub fn new_bool(left: usize, op: operator::Bool, right: usize) -> Self {
        Expression::Bool { left, op, right }
    }
}
