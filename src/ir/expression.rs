use super::operator;
use super::value::Value;
use super::{Node, Plan};
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
        distribution: Distribution,
    },
}

#[allow(dead_code)]
impl Expression {
    /// When a parent row deduces its distribution, it builds a reference map for each
    /// branch and asks the corresponding child for a suggestion about distribution.
    /// This function is executed on the child's side.
    pub fn suggested_distribution(
        &self,
        my_branch: &Branch,
        aliases: &[usize],
        plan: &Plan,
    ) -> Result<Distribution, QueryPlannerError> {
        if let Expression::Row {
            ref distribution, ..
        } = self
        {
            match *distribution {
                Distribution::Random => return Ok(Distribution::Random),
                Distribution::Replicated => return Ok(Distribution::Replicated),
                Distribution::Segment { ref key } => {
                    // When expression is a Row, it has a three level structure:
                    //
                    // level 0: row itself
                    // level 1: list of aliases
                    // level 2: arbitrary expressions (references as well)
                    // ...
                    // Now we traverse the level 2 for the reference expressions, as only
                    // they can contain positions of the distribution key from the input row, then
                    // build a map <input row list position, self row list position of the reference>.
                    let mut map: HashMap<usize, usize> = HashMap::new();

                    for (self_pos, alias_id) in aliases.iter().enumerate() {
                        if let Node::Expression(Expression::Alias { child, .. }) =
                            plan.get_node(*alias_id)?
                        {
                            if let Node::Expression(Expression::Reference {
                                branch: ref_branch,
                                position: ref_pos,
                            }) = plan.get_node(*child)?
                            {
                                if *ref_branch == *my_branch
                                    && map.insert(*ref_pos, self_pos).is_some()
                                {
                                    return Err(QueryPlannerError::InvalidPlan);
                                }
                            }
                        } else {
                            return Err(QueryPlannerError::InvalidRow);
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
                Distribution::Single => return Ok(Distribution::Single),
            }
        }
        Err(QueryPlannerError::InvalidRow)
    }

    pub fn distribution(&self) -> Result<&Distribution, QueryPlannerError> {
        if let Expression::Row { distribution, .. } = self {
            return Ok(distribution);
        }
        Err(QueryPlannerError::InvalidRow)
    }

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
    pub fn new_row(list: Vec<usize>, distribution: Distribution) -> Self {
        Expression::Row { list, distribution }
    }

    pub fn new_bool(left: usize, op: operator::Bool, right: usize) -> Self {
        Expression::Bool { left, op, right }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::fs;
    use std::path::Path;

    #[test]
    fn suggested_distribution() {
        // Load a table "t (a, b, c, d)" distributed by ["b", "a"]
        // with a sec scan and three additional alias-reference pairs
        // for "a", "b" and "c". We want to see, what suggestions would
        // sec scan output row make for a new parent row constructed
        // from different combinations of new "a", "b" and "c".
        let path = Path::new("")
            .join("tests")
            .join("artifactory")
            .join("ir")
            .join("expression")
            .join("suggested_distribution.yaml");
        let s = fs::read_to_string(path).unwrap();
        let plan = Plan::from_yaml(&s).unwrap();

        let a = 11;
        let b = 13;
        let c = 15;
        let scan_output = 8;

        if let Node::Expression(output) = plan.get_node(scan_output).unwrap() {
            // Same order in distribution key
            assert_eq!(
                Distribution::Segment { key: vec![1, 0] },
                output
                    .suggested_distribution(&Branch::Left, &[a, b, c], &plan)
                    .unwrap()
            );

            // Shuffle distribution key
            assert_eq!(
                Distribution::Segment { key: vec![0, 1] },
                output
                    .suggested_distribution(&Branch::Left, &[b, a], &plan)
                    .unwrap()
            );

            // Shrink distribution key #1
            assert_eq!(
                Distribution::Random,
                output
                    .suggested_distribution(&Branch::Left, &[c, a], &plan)
                    .unwrap()
            );

            // Shrink distribution key #2
            assert_eq!(
                Distribution::Random,
                output
                    .suggested_distribution(&Branch::Left, &[a], &plan)
                    .unwrap()
            );

            //TODO: implement checks for Replicated and Single
        } else {
            panic!("Wrong output node type!");
        }
    }
}
