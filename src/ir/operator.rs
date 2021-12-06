//! Operators for expression transformations.

use super::expression::{Branch, Distribution, Expression};
use super::relation::Table;
use super::{vec_alloc, Node, Plan};
use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Binary operator returning Bool expression.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Bool {
    /// `&&`
    And,
    /// `=`
    Eq,
    /// `=all` (also named `in`)
    EqAll,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `!=`
    NotEq,
    /// `||`
    Or,
}

/// Relational algebra operator returning a new tuple.
///
/// Transforms input tuple(s) into the output one using
/// relation algebra logic.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Relational {
    /// Inner Join
    InnerJoin {
        /// Left and right tuple comparison condition.
        /// In fact - an expression tree top index in plan node arena.
        condition: usize,
        /// Left branch tuple node index in the plan node arena.
        left: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
        /// Right branch tuple node index in the plan node arena.
        right: usize,
    },
    Motion {
        /// Child tuple node index in the plan node arena (left branch).
        child: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    Projection {
        /// Child tuple node index in the plan node arena (left branch).
        child: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    ScanRelation {
        /// Output tuple node index in the plan node arena.
        output: usize,
        /// Relation name.
        relation: String,
    },
    ScanSubQuery {
        /// Child tuple node index in the plan node arena (left branch).
        child: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    Selection {
        /// Child tuple node index in the plan node arena (left branch).
        child: usize,
        /// Filter expression node index in the plan node arena.
        filter: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
    UnionAll {
        /// Left branch tuple node index in the plan node arena.
        left: usize,
        /// Right branch tuple node index in the plan node arena.
        right: usize,
        /// Output tuple node index in the plan node arena.
        output: usize,
    },
}

#[allow(dead_code)]
impl Relational {
    /// Get output tuple alias map.
    ///
    /// We expect that the top level of the node's expression tree
    /// is a row of aliases with unique names. Return them.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the output tuple is invalid.
    pub fn output_aliases(&self, plan: &Plan) -> Result<HashMap<String, usize>, QueryPlannerError> {
        let mut map: HashMap<String, usize> = HashMap::new();

        if let Some(Node::Expression(Expression::Row { list, .. })) = plan.nodes.get(self.output())
        {
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

    /// Get output tuple node index in plan node arena.
    #[must_use]
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

    /// New `ScanRelation` constructor.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when relation is invalid.
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
                                let r_id = vec_alloc(nodes, Node::Expression(r));
                                vec_alloc(
                                    nodes,
                                    Node::Expression(Expression::new_alias(&col.name, r_id)),
                                )
                            })
                            .collect();

                        return Ok(Relational::ScanRelation {
                            output: vec_alloc(
                                nodes,
                                Node::Expression(Expression::new_row(
                                    refs,
                                    Distribution::Segment { key: key.clone() },
                                )),
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

    /// New `Projection` constructor.
    ///
    /// # Errors
    /// Returns `QueryPlannerError` when the child node is invalid.
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
                    let r_id = vec_alloc(
                        &mut plan.nodes,
                        Node::Expression(Expression::new_ref(Branch::Left, *pos)),
                    );
                    let a_id = vec_alloc(
                        &mut plan.nodes,
                        Node::Expression(Expression::new_alias(col, r_id)),
                    );
                    alias_exprs.push(a_id);
                    used_child_pos.push(*pos);
                    true
                })
            });

            if all_found {
                // Re-read child node after plan modification (old pointers can become invalid)
                if let Node::Relational(child_node) = plan.get_node(child)? {
                    if let Node::Expression(child_row) = plan.get_node(child_node.output())? {
                        let dist =
                            child_row.suggested_distribution(&Branch::Left, &alias_exprs, plan)?;
                        let new_output = vec_alloc(
                            &mut plan.nodes,
                            Node::Expression(Expression::new_row(alias_exprs, dist)),
                        );
                        return Ok(Relational::Projection {
                            child,
                            output: new_output,
                        });
                    }
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
                output: 8,
                relation: String::from("t"),
            },
            scan
        );

        if let Node::Expression(row) = plan.get_node(8).unwrap() {
            assert_eq!(
                *row.distribution().unwrap(),
                Distribution::Segment { key: vec![1, 0] }
            );
        } else {
            panic!("Wrong output node type!");
        }

        assert_eq!(9, vec_alloc(&mut plan.nodes, Node::Relational(scan)));
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
        let scan_id = vec_alloc(&mut plan.nodes, Node::Relational(scan));
        let proj_seg = Relational::new_proj(&mut plan, scan_id, &["b", "a"]).unwrap();
        assert_eq!(
            Relational::Projection {
                child: scan_id,
                output: 14
            },
            proj_seg
        );

        if let Node::Expression(row) = plan.get_node(14).unwrap() {
            assert_eq!(
                *row.distribution().unwrap(),
                Distribution::Segment { key: vec![0, 1] }
            );
        }

        let proj_rand = Relational::new_proj(&mut plan, scan_id, &["a", "d"]).unwrap();
        assert_eq!(
            Relational::Projection {
                child: scan_id,
                output: 19
            },
            proj_rand
        );

        if let Node::Expression(row) = plan.get_node(19).unwrap() {
            assert_eq!(*row.distribution().unwrap(), Distribution::Random);
        }

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
}
