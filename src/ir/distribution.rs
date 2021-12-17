use crate::errors::QueryPlannerError;
use serde::{Deserialize, Serialize};

/// Tuple data chunk distribution policy in the cluster.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Distribution {
    /// Only coordinator contains all the data.
    ///
    /// Example: insertion to the virtual table on coordinator.
    Coordinator,
    /// Each segment contains a random portion of data.
    ///
    /// Example: "erased" distribution key by projection operator.
    Random,
    /// Each segment contains a full copy of data.
    ///
    /// Example: constant expressions.
    Replicated,
    /// Each segment contains a portion of data,
    /// determined by the distribution key rule.
    ///
    /// Example: segmented table.
    Segment {
        /// A list of column positions in the output tuple,
        /// that for a distribution key.
        key: Vec<usize>,
    },
}

impl Distribution {
    /// Calculate a new distribution for the `UnionAll` output tuple.
    ///
    /// # Errors
    /// Returns `QueryPlannerError`:
    /// - distribution conflict that should be resolved by adding a `Motion` node
    pub fn new_union(
        left: Distribution,
        right: Distribution,
    ) -> Result<Distribution, QueryPlannerError> {
        match left {
            Distribution::Coordinator => match right {
                Distribution::Coordinator => Ok(left),
                _ => Err(QueryPlannerError::RequireMotion),
            },
            Distribution::Random => match right {
                Distribution::Coordinator => Err(QueryPlannerError::RequireMotion),
                _ => Ok(left),
            },
            Distribution::Replicated => match right {
                Distribution::Coordinator => Err(QueryPlannerError::RequireMotion),
                _ => Ok(right),
            },
            Distribution::Segment {
                key: ref left_key, ..
            } => match right {
                Distribution::Random => Ok(Distribution::Random),
                Distribution::Replicated => Ok(left),
                Distribution::Segment {
                    key: ref right_key, ..
                } => {
                    if left_key.iter().zip(right_key.iter()).all(|(l, r)| l == r) {
                        Ok(left)
                    } else {
                        Ok(Distribution::Random)
                    }
                }
                Distribution::Coordinator => Err(QueryPlannerError::RequireMotion),
            },
        }
    }
}

#[cfg(test)]
mod tests;
