pub mod expression;
pub mod operator;
pub mod relation;
pub mod value;

use crate::errors::QueryPlannerError;
use expression::Expression;
use operator::Relational;
use relation::Table;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Node {
    Expression(Expression),
    Relational(Relational),
}

pub fn push_and_get_idx<T>(v: &mut Vec<T>, item: T) -> usize {
    let idx = v.len();
    v.push(item);
    idx
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Plan {
    nodes: Vec<Node>,
    relations: Option<HashMap<String, Table>>,
    slices: Option<Vec<Vec<usize>>>,
    top: Option<usize>,
}

#[allow(dead_code)]
impl Plan {
    pub fn add_rel(&mut self, table: Table) {
        match &mut self.relations {
            None => {
                let mut map = HashMap::new();
                map.insert(String::from(table.name()), table);
                self.relations = Some(map);
            }
            Some(relations) => {
                relations.entry(String::from(table.name())).or_insert(table);
            }
        }
    }

    pub fn check(&self) -> Result<(), QueryPlannerError> {
        if self.top.is_none() {
            return Err(QueryPlannerError::InvalidPlan);
        } else if self.nodes.get(self.top.unwrap()).is_none() {
            return Err(QueryPlannerError::ValueOutOfRange);
        }

        //TODO: additional consistency checks

        Ok(())
    }

    pub fn empty() -> Self {
        Plan {
            nodes: Vec::new(),
            relations: None,
            slices: None,
            top: None,
        }
    }

    pub fn get_node(&self, pos: usize) -> Result<&Node, QueryPlannerError> {
        match self.nodes.get(pos) {
            None => Err(QueryPlannerError::ValueOutOfRange),
            Some(node) => Ok(node),
        }
    }

    pub fn from_yaml(s: &str) -> Result<Self, QueryPlannerError> {
        let plan: Plan = match serde_yaml::from_str(s) {
            Ok(p) => p,
            Err(_) => return Err(QueryPlannerError::Serialization),
        };
        plan.check()?;
        Ok(plan)
    }
}

#[derive(Debug)]
pub struct BranchIterator<'n> {
    node: &'n Node,
    step: RefCell<usize>,
    plan: &'n Plan,
}

#[allow(dead_code)]
impl<'n> BranchIterator<'n> {
    pub fn new(node: &'n Node, plan: &'n Plan) -> Self {
        BranchIterator {
            node,
            step: RefCell::new(0),
            plan,
        }
    }
}

impl<'n> Iterator for BranchIterator<'n> {
    type Item = &'n Node;

    fn next(&mut self) -> Option<Self::Item> {
        match self.node {
            Node::Expression(expr) => match expr {
                Expression::Alias { child, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*child);
                    }
                    None
                }
                Expression::Bool { left, right, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*left);
                    } else if current_step == 1 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*right);
                    }
                    None
                }
                Expression::Constant { .. } | Expression::Reference { .. } => None,
                Expression::Row { list } => {
                    let current_step = *self.step.borrow();
                    if let Some(node) = list.get(current_step) {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*node);
                    }
                    None
                }
            },
            Node::Relational(rel) => match rel {
                Relational::InnerJoin {
                    left,
                    right,
                    condition,
                    ..
                } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*left);
                    } else if current_step == 1 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*right);
                    } else if current_step == 2 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*condition);
                    }
                    None
                }
                Relational::ScanRelation { .. } => None,
                Relational::ScanSubQuery { child, .. }
                | Relational::Motion { child, .. }
                | Relational::Selection { child, .. }
                | Relational::Projection { child, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*child);
                    }
                    None
                }
                Relational::UnionAll { left, right, .. } => {
                    let current_step = *self.step.borrow();
                    if current_step == 0 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*left);
                    } else if current_step == 1 {
                        *self.step.borrow_mut() += 1;
                        return self.plan.nodes.get(*right);
                    }
                    None
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::relation::*;
    use pretty_assertions::assert_eq;
    use std::fs;
    use std::path::Path;

    #[test]
    fn plan_no_top() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("plan_no_top.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            QueryPlannerError::InvalidPlan,
            Plan::from_yaml(&s).unwrap_err()
        );
    }

    #[test]
    fn plan_oor_top() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("plan_oor_top.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(
            QueryPlannerError::ValueOutOfRange,
            Plan::from_yaml(&s).unwrap_err()
        );
    }

    #[test]
    fn get_node() {
        let mut plan = Plan::empty();

        let t = Table::new_seg("t", vec![Column::new("a", Type::Boolean)], &["a"]).unwrap();
        plan.add_rel(t);

        let scan = Relational::new_scan("t", &mut plan).unwrap();
        let scan_id = push_and_get_idx(&mut plan.nodes, Node::Relational(scan));

        if let Node::Relational(Relational::ScanRelation { relation, .. }) =
            plan.get_node(scan_id).unwrap()
        {
            assert_eq!(relation, "t");
        } else {
            panic!("Unexpected node returned!")
        }
    }

    #[test]
    fn get_node_oor() {
        let plan = Plan::empty();
        assert_eq!(
            QueryPlannerError::ValueOutOfRange,
            plan.get_node(42).unwrap_err()
        );
    }

    //TODO: add relation test
}
