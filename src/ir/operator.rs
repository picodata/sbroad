use super::expression::{Branch, Expression};
use super::relation::Table;
use super::{push_and_get_idx, Node, Plan};
use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    /// We expect that the top level of the node's expression tree
    /// is a row of aliases with unique names.
    pub fn output_aliases(&self, plan: &Plan) -> Result<HashMap<String, usize>, QueryPlannerError> {
        let mut map: HashMap<String, usize> = HashMap::new();

        if let Some(Node::Expression(Expression::Row { list })) = plan.nodes.get(self.output()) {
            let valid = list.iter().enumerate().all(|(pos, item)| {
                // Check that expressions in the row list are all aliases
                if let Some(Node::Expression(Expression::Alias { ref name, .. })) =
                    plan.nodes.get(*item)
                {
                    // Populate the map and check duplicate absence
                    if map.insert(String::from(name), pos).is_none() {
                        return true;
                    }
                }
                false
            });
            if valid {
                return Ok(map);
            }
            return Err(QueryPlannerError::InvalidPlan);
        }
        Err(QueryPlannerError::ValueOutOfRange)
    }

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

    pub fn pushup_distribution(
        &self,
        row: Option<usize>,
        plan: &Plan,
    ) -> Result<Distribution, QueryPlannerError> {
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
                Distribution::Random => Ok(Distribution::Random),
                Distribution::Replicated => Ok(Distribution::Replicated),
                Distribution::Segment { ref key } => {
                    // Expression tree structure:
                    // level 0: row
                    // level 1: aliases
                    // level 2: references and other expressions
                    // ...
                    // To get the new distribution key we need only references on the second level.
                    if let Some(row_node) = row {
                        if let Node::Expression(Expression::Row { ref list }) =
                            plan.get_node(row_node)?
                        {
                            let mut map: HashMap<usize, usize> = HashMap::new();
                            for (row_pos, r) in list.iter().enumerate() {
                                if let Node::Expression(Expression::Alias { child, .. }) =
                                    plan.get_node(*r)?
                                {
                                    if let Node::Expression(Expression::Reference {
                                        position,
                                        ..
                                    }) = plan.get_node(*child)?
                                    {
                                        if map.insert(*position, row_pos).is_some() {
                                            return Err(QueryPlannerError::InvalidPlan);
                                        }
                                    }
                                }
                            }

                            let mut new_key: Vec<usize> = Vec::new();
                            let all_found = key.iter().all(|pos| {
                                if let Some(new_pos) = map.get(pos) {
                                    new_key.push(*new_pos);
                                    return true;
                                }
                                false
                            });
                            if all_found {
                                return Ok(Distribution::Segment { key: new_key });
                            }
                            return Ok(Distribution::Random);
                        }
                    }
                    Err(QueryPlannerError::InvalidPlan)
                }
                Distribution::Single => Ok(Distribution::Single),
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

    pub fn new_proj(
        plan: &mut Plan,
        child: usize,
        output: &[&str],
    ) -> Result<Self, QueryPlannerError> {
        if output.is_empty() {
            return Err(QueryPlannerError::InvalidRow);
        }

        if let Node::Relational(child_node) = plan.get_node(child)? {
            let map = child_node.output_aliases(plan)?;
            let mut alias_exprs: Vec<usize> = Vec::new();
            let mut used_child_pos: Vec<usize> = Vec::new();

            let all_found = output.iter().all(|col| {
                map.get(*col).map_or(false, |pos| {
                    // Create new references with aliases and push alias positions
                    let r_id = push_and_get_idx(
                        &mut plan.nodes,
                        Node::Expression(Expression::new_ref(Branch::Left, *pos)),
                    );
                    let a_id = push_and_get_idx(
                        &mut plan.nodes,
                        Node::Expression(Expression::new_alias(col, r_id)),
                    );
                    alias_exprs.push(a_id);
                    used_child_pos.push(*pos);
                    true
                })
            });

            if all_found {
                let new_output = push_and_get_idx(
                    &mut plan.nodes,
                    Node::Expression(Expression::new_row(alias_exprs)),
                );
                // Re-read child node after plan modification (old pointers can become invalid)
                if let Node::Relational(node) = plan.get_node(child)? {
                    return Ok(Relational::Projection {
                        child,
                        distribution: node.pushup_distribution(Some(new_output), plan)?,
                        output: new_output,
                    });
                }
            }

            return Err(QueryPlannerError::InvalidRow);
        }

        Err(QueryPlannerError::InvalidPlan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::QueryPlannerError;
    use crate::ir::relation::*;
    use itertools::Itertools;
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
        plan.top = Some(9);

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
        let scan_id = push_and_get_idx(&mut plan.nodes, Node::Relational(scan));
        let proj_seg = Relational::new_proj(&mut plan, scan_id, &["b", "a"]).unwrap();
        assert_eq!(
            Relational::Projection {
                child: scan_id,
                distribution: Distribution::Segment { key: vec![0, 1] },
                output: 14
            },
            proj_seg
        );

        let proj_rand = Relational::new_proj(&mut plan, scan_id, &["a", "d"]).unwrap();
        assert_eq!(
            Relational::Projection {
                child: scan_id,
                distribution: Distribution::Random,
                output: 19
            },
            proj_rand
        );

        // Empty output
        assert_eq!(
            QueryPlannerError::InvalidRow,
            Relational::new_proj(&mut plan, scan_id, &[]).unwrap_err()
        );

        // Invalid alias names in the output
        assert_eq!(
            QueryPlannerError::InvalidRow,
            Relational::new_proj(&mut plan, scan_id, &["a", "e"]).unwrap_err()
        );

        // Expression node instead of relational one
        assert_eq!(
            QueryPlannerError::InvalidPlan,
            Relational::new_proj(&mut plan, 1, &["a"]).unwrap_err()
        );

        // Try to build projection from the invalid node
        assert_eq!(
            QueryPlannerError::ValueOutOfRange,
            Relational::new_proj(&mut plan, 42, &["a"]).unwrap_err()
        );
    }

    #[test]
    fn projection_serialize() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("operator")
            .join("projection.yaml");
        let s = fs::read_to_string(path).unwrap();
        Plan::from_yaml(&s).unwrap();
    }

    #[test]
    fn output_aliases() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("operator")
            .join("output_aliases.yaml");
        let s = fs::read_to_string(path).unwrap();
        let plan = Plan::from_yaml(&s).unwrap();

        let top = plan.nodes.get(plan.top.unwrap()).unwrap();
        if let Node::Relational(rel) = top {
            let col_map = rel.output_aliases(&plan).unwrap();

            let expected_keys = vec!["a", "b"];
            assert_eq!(expected_keys.len(), col_map.len());
            expected_keys
                .iter()
                .zip(col_map.keys().sorted())
                .for_each(|(e, k)| assert_eq!(e, k));

            let expected_val = vec![0, 1];
            expected_val
                .iter()
                .zip(col_map.values().sorted())
                .for_each(|(e, v)| assert_eq!(e, v));
        } else {
            panic!("Plan top should be a relational operator!");
        }
    }

    #[test]
    fn output_aliases_duplicates() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("operator")
            .join("output_aliases_duplicates.yaml");
        let s = fs::read_to_string(path).unwrap();
        let plan = Plan::from_yaml(&s).unwrap();

        let top = plan.nodes.get(plan.top.unwrap()).unwrap();
        if let Node::Relational(rel) = top {
            assert_eq!(
                QueryPlannerError::InvalidPlan,
                rel.output_aliases(&plan).unwrap_err()
            );
        } else {
            panic!("Plan top should be a relational operator!");
        }
    }

    #[test]
    fn output_aliases_unsupported_type() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("operator")
            .join("output_aliases_unsupported_type.yaml");
        let s = fs::read_to_string(path).unwrap();
        let plan = Plan::from_yaml(&s).unwrap();

        let top = plan.nodes.get(plan.top.unwrap()).unwrap();
        if let Node::Relational(rel) = top {
            assert_eq!(
                QueryPlannerError::InvalidPlan,
                rel.output_aliases(&plan).unwrap_err()
            );
        } else {
            panic!("Plan top should be a relational operator!");
        }
    }

    #[test]
    fn output_alias_oor() {
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("operator")
            .join("output_aliases_oor.yaml");
        let s = fs::read_to_string(path).unwrap();
        let plan = Plan::from_yaml(&s).unwrap();

        let top = plan.nodes.get(plan.top.unwrap()).unwrap();
        if let Node::Relational(rel) = top {
            assert_eq!(
                QueryPlannerError::ValueOutOfRange,
                rel.output_aliases(&plan).unwrap_err()
            );
        } else {
            panic!("Plan top should be a relational operator!");
        }
    }

    #[test]
    fn pushup_distribution() {
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
        let ref5_a = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::new_ref(Branch::Left, 0)),
        );
        let ref6_b = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::new_ref(Branch::Left, 1)),
        );
        let ref7_c = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::new_ref(Branch::Left, 2)),
        );
        let a8_a = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::new_alias("a", ref5_a)),
        );
        let a9_b = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::new_alias("b", ref6_b)),
        );
        let a10_c = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::new_alias("c", ref7_c)),
        );

        // Same order in distribution key
        let fake_row = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::Row {
                list: vec![a8_a, a9_b, a10_c],
            }),
        );
        assert_eq!(
            Distribution::Segment { key: vec![1, 0] },
            scan.pushup_distribution(Some(fake_row), &plan).unwrap()
        );

        // Shuffle distribution key
        let fake_row = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::Row {
                list: vec![a9_b, a8_a],
            }),
        );
        assert_eq!(
            Distribution::Segment { key: vec![0, 1] },
            scan.pushup_distribution(Some(fake_row), &plan).unwrap()
        );

        // Shrink distribution key #1
        let fake_row = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::Row {
                list: vec![a10_c, a8_a],
            }),
        );
        assert_eq!(
            Distribution::Random,
            scan.pushup_distribution(Some(fake_row), &plan).unwrap()
        );

        // Shrink distribution key #2
        let fake_row = push_and_get_idx(
            &mut plan.nodes,
            Node::Expression(Expression::Row { list: vec![a9_b] }),
        );
        assert_eq!(
            Distribution::Random,
            scan.pushup_distribution(Some(fake_row), &plan).unwrap()
        );

        //TODO: implement checks for Replicated and Single
    }
}
