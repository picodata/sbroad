use crate::errors::{Entity, SbroadError};
use crate::ir::expression::Expression;
use crate::ir::expression::Expression::StableFunction;
use crate::ir::{Node, Plan};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::rc::Rc;

/// The kind of aggregate function
///
/// Examples: avg, sum, count,  ..
#[derive(Clone, Debug, Hash, Eq, PartialEq, Copy)]
pub enum AggregateKind {
    COUNT,
    SUM,
}

impl Display for AggregateKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            AggregateKind::COUNT => "count",
            AggregateKind::SUM => "sum",
        };
        write!(f, "{name}")
    }
}

impl AggregateKind {
    #[must_use]
    pub fn new(name: &str) -> Option<AggregateKind> {
        let normalized = name.to_lowercase();
        match normalized.as_str() {
            "count" => Some(AggregateKind::COUNT),
            "sum" => Some(AggregateKind::SUM),
            _ => None,
        }
    }

    #[must_use]
    pub fn get_local_aggregates_kinds(&self) -> Vec<AggregateKind> {
        match self {
            AggregateKind::COUNT => vec![AggregateKind::COUNT],
            AggregateKind::SUM => vec![AggregateKind::SUM],
        }
    }

    /// Get final aggregate corresponding to given local aggregate
    ///
    /// # Errors
    /// - Invalid combination of this aggregate and local aggregate
    pub fn get_final_aggregate_kind(
        &self,
        local_aggregate: &AggregateKind,
    ) -> Result<AggregateKind, SbroadError> {
        let res = match (self, local_aggregate) {
            (AggregateKind::COUNT, AggregateKind::COUNT)
            | (AggregateKind::SUM, AggregateKind::SUM) => AggregateKind::SUM,
            (_, _) => {
                return Err(SbroadError::Invalid(
                    Entity::Aggregate,
                    Some(format!(
                        "invalid local aggregate {local_aggregate} for original aggregate: {self}"
                    )),
                ))
            }
        };
        Ok(res)
    }
}

/// Helper struct for adding aggregates to ir
///
/// This struct can be used for adding any Tarantool aggregate:
/// avg, sum, count, min, max, total
#[derive(Debug, Clone)]
pub struct SimpleAggregate {
    /// The aggregate function being added, like COUNT
    pub kind: AggregateKind,
    /// For non-distinct aggregate maps local aggregate kind to
    /// corresponding local alias. For distinct aggregate maps
    /// its aggregate kind to local alias used for corresponding
    /// grouping expr.
    ///
    /// For example, if `AggregateKind` is `AVG` then we have two local aggregates:
    /// `sum` and `count`. Each of those aggregates will have its local alias in map
    /// query:
    /// original query: `select avg(b) from t`
    /// map query: `select sum(b) as l1, count(b) as l2 from t`
    ///
    /// So, this map will contain `sum` -> `l1`, `count` -> `l2`.
    ///
    /// Example for distinct aggregate:
    /// original query: `select avg(distinct b) from t`
    /// map query: `select b as l1 from t group by b)`
    /// map will contain: `avg` -> `l1`
    pub lagg_alias: HashMap<AggregateKind, Rc<String>>,
    /// id of aggregate function in IR
    pub fun_id: usize,
}

#[cfg(not(feature = "mock"))]
#[must_use]
pub fn generate_local_alias_for_aggr(kind: &AggregateKind, suffix: &str) -> String {
    format!(
        "\"{}_{kind}_{suffix}\"",
        uuid::Uuid::new_v4().as_simple().to_string()
    )
}

#[cfg(feature = "mock")]
#[must_use]
pub fn generate_local_alias_for_aggr(kind: &AggregateKind, suffix: &str) -> String {
    format!("\"{kind}_{suffix}\"")
}

impl SimpleAggregate {
    #[must_use]
    pub fn new(name: &str, fun_id: usize) -> Option<SimpleAggregate> {
        let Some(kind) = AggregateKind::new(name) else {
            return None
        };
        let laggr_alias: HashMap<AggregateKind, Rc<String>> = HashMap::new();
        let aggr = SimpleAggregate {
            kind,
            fun_id,
            lagg_alias: laggr_alias,
        };
        Some(aggr)
    }
}

impl SimpleAggregate {
    /// Create columns with final aggregates in final `Projection`
    ///
    /// # Errors
    /// - Invalid aggregate
    /// - Could not find local alias position in child output
    ///
    pub fn create_column_for_final_projection(
        &self,
        parent: usize,
        plan: &mut Plan,
        alias_to_pos: &HashMap<String, usize>,
        is_distinct: bool,
    ) -> Result<usize, SbroadError> {
        let mut final_aggregates: Vec<usize> = vec![];
        let mut create_final_aggr = |local_alias: &str,
                                     final_func: &str|
         -> Result<(), SbroadError> {
            let Some(position) = alias_to_pos.get(local_alias) else {
                let parent_node = plan.get_relation_node(parent)?;
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!("could find aggregate column in final {parent_node:?} child by local alias: {local_alias}. Aliases: {alias_to_pos:?}"))))
            };
            let ref_node = Expression::Reference {
                parent: Some(parent),
                // projection has only one child
                targets: Some(vec![0]),
                position: *position,
            };
            let ref_id = plan.nodes.push(Node::Expression(ref_node));
            let final_aggr = StableFunction {
                name: final_func.to_string(),
                children: vec![ref_id],
                is_distinct,
            };
            let aggr_id = plan.nodes.push(Node::Expression(final_aggr));
            final_aggregates.push(aggr_id);
            Ok(())
        };
        if is_distinct {
            let local_alias = self.lagg_alias.get(&self.kind).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Aggregate,
                    Some(format!(
                        "missing local alias for distinct aggregate: {self:?}"
                    )),
                )
            })?;
            let final_aggregate_name = self.kind.to_string();
            create_final_aggr(local_alias, final_aggregate_name.as_str())?;
        } else {
            for aggr_kind in self.kind.get_local_aggregates_kinds() {
                let local_alias = self.lagg_alias.get(&aggr_kind).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Aggregate,
                        Some(format!(
                            "missing local alias for local aggregate ({aggr_kind}): {self:?}"
                        )),
                    )
                })?;
                let final_aggregate_name =
                    self.kind.get_final_aggregate_kind(&aggr_kind)?.to_string();
                create_final_aggr(local_alias, final_aggregate_name.as_str())?;
            }
        }
        let final_expr_id = if final_aggregates.len() == 1 {
            *final_aggregates.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues("final_aggregates is empty".into())
            })?
        } else {
            return Err(SbroadError::Unsupported(
                Entity::Aggregate,
                Some(format!(
                    "aggregate with multiple final aggregates: {self:?}"
                )),
            ));
        };
        Ok(final_expr_id)
    }
}
