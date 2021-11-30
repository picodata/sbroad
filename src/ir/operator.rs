use super::expression::{Branch, Expression};
use super::relation::Table;
use super::{push_and_get_idx, Node, Plan};
use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Distribution {
    Random,
    Replicated,
    Segment { key: Vec<usize> },
    Single,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Bool {
    And,
    Eq,
    EqAll,
    Gt,
    GtEq,
    Lt,
    LtEq,
    NotEq,
    Or,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Relational {
    InnerJoin {
        distribution: Distribution,
        condition: usize,
        left: usize,
        output: usize,
        right: usize,
    },
    Motion {
        child: usize,
        distribution: Distribution,
        output: usize,
    },
    Projection {
        child: usize,
        distribution: Distribution,
        output: usize,
    },
    ScanRelation {
        distribution: Distribution,
        output: usize,
        relation: String,
    },
    ScanSubQuery {
        child: usize,
        distribution: Distribution,
        output: usize,
    },
    Selection {
        child: usize,
        distribution: Distribution,
        filter: usize,
        output: usize,
    },
    UnionAll {
        distribution: Distribution,
        left: usize,
        right: usize,
        output: usize,
    },
}

#[allow(dead_code)]
impl Relational {
    pub fn output(&self) -> usize {
        match self {
            Relational::InnerJoin { output, .. }
            | Relational::Motion { output, .. }
            | Relational::Projection { output, .. }
            | Relational::ScanRelation { output, .. }
            | Relational::ScanSubQuery { output, .. }
            | Relational::Selection { output, .. }
            | Relational::UnionAll { output, .. } => *output,
        }
    }

    pub fn distribution(&self) -> Distribution {
        match self {
            Relational::InnerJoin {
                ref distribution, ..
            }
            | Relational::Motion {
                ref distribution, ..
            }
            | Relational::Projection {
                ref distribution, ..
            }
            | Relational::ScanRelation {
                ref distribution, ..
            }
            | Relational::ScanSubQuery {
                ref distribution, ..
            }
            | Relational::Selection {
                ref distribution, ..
            }
            | Relational::UnionAll {
                ref distribution, ..
            } => match distribution {
                Distribution::Random => Distribution::Random,
                Distribution::Replicated => Distribution::Replicated,
                Distribution::Segment { ref key } => Distribution::Segment { key: key.clone() },
                Distribution::Single => Distribution::Single,
            },
        }
    }

    pub fn new_scan(table_name: &str, plan: &mut Plan) -> Result<Self, QueryPlannerError> {
        let nodes = &mut plan.nodes;
        if let Some(relations) = &plan.relations {
            if let Some(rel) = relations.get(table_name) {
                match rel {
                    Table::Segment {
                        ref columns,
                        key,
                        name: _,
                    } => {
                        let refs = columns
                            .iter()
                            .enumerate()
                            .map(|(pos, col)| {
                                let r = Expression::new_ref(Branch::Left, pos);
                                let r_id = push_and_get_idx(nodes, Node::Expression(r));
                                push_and_get_idx(
                                    nodes,
                                    Node::Expression(Expression::new_alias(&col.name, r_id)),
                                )
                            })
                            .collect();

                        return Ok(Relational::ScanRelation {
                            distribution: Distribution::Segment { key: key.clone() },
                            output: push_and_get_idx(
                                nodes,
                                Node::Expression(Expression::new_row(refs)),
                            ),
                            relation: String::from(table_name),
                        });
                    }
                    //TODO: implement virtual tables as well
                    _ => return Err(QueryPlannerError::InvalidRelation),
                }
            }
        }
        Err(QueryPlannerError::InvalidRelation)
    }

    // TODO: replace output with column name list
    pub fn new_proj(
        plan: &mut Plan,
        child: usize,
        output: Vec<usize>,
    ) -> Result<Self, QueryPlannerError> {
        let nodes = &mut plan.nodes;

        let get_node = |pos: usize| -> Result<&Node, QueryPlannerError> {
            match nodes.get(pos) {
                None => Err(QueryPlannerError::ValueOutOfRange),
                Some(node) => Ok(node),
            }
        };

        match get_node(child)? {
            Node::Expression(_) => Err(QueryPlannerError::InvalidPlan),
            Node::Relational(rel) => {
                if let Node::Expression(Expression::Row { ref list }) = get_node(rel.output())? {
                    if list.len() < output.len() {
                        Err(QueryPlannerError::InvalidPlan)
                    } else {
                        Ok(Relational::Projection {
                            child,
                            distribution: rel.distribution(),
                            output: push_and_get_idx(
                                nodes,
                                Node::Expression(Expression::new_row(output)),
                            ),
                        })
                    }
                } else {
                    Err(QueryPlannerError::InvalidPlan)
                }
            }
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
    fn scan_rel() {
        let mut plan = Plan::empty();

        let t = Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean),
                Column::new("b", Type::Number),
                Column::new("c", Type::String),
                Column::new("d", Type::String),
            ],
            &["b", "a"],
        )
        .unwrap();
        plan.add_rel(t);

        let scan = Relational::new_scan("t", &mut plan).unwrap();
        assert_eq!(
            Relational::ScanRelation {
                distribution: Distribution::Segment { key: vec![1, 0] },
                output: 8,
                relation: String::from("t"),
            },
            scan
        );

        assert_eq!(9, push_and_get_idx(&mut plan.nodes, Node::Relational(scan)));
    }

    #[test]
    fn scan_rel_serialized() {
        let mut plan = Plan::empty();

        let t = Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean),
                Column::new("b", Type::Number),
                Column::new("c", Type::String),
                Column::new("d", Type::String),
            ],
            &["b", "a"],
        )
        .unwrap();
        plan.add_rel(t);

        let scan = Relational::new_scan("t", &mut plan).unwrap();
        plan.nodes.push(Node::Relational(scan));
        plan.top = Some(5);

        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("operator")
            .join("scan_rel.yaml");
        let s = fs::read_to_string(path).unwrap();
        assert_eq!(plan, Plan::from_yaml(&s).unwrap());
    }

    #[test]
    fn projection() {
        let mut plan = Plan::empty();

        let t = Table::new_seg(
            "t",
            vec![
                Column::new("a", Type::Boolean),
                Column::new("b", Type::Number),
                Column::new("c", Type::String),
                Column::new("d", Type::String),
            ],
            &["b", "a"],
        )
        .unwrap();
        plan.add_rel(t);

        let scan = Relational::new_scan("t", &mut plan).unwrap();
        let scan_idx = push_and_get_idx(&mut plan.nodes, Node::Relational(scan));

        let proj = Relational::new_proj(&mut plan, scan_idx, vec![3, 4]).unwrap();
    }
}
