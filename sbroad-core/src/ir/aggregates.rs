use crate::errors::{Entity, SbroadError};
use crate::ir::expression::Expression;
use crate::ir::expression::Expression::StableFunction;
use crate::ir::function::{Behavior, Function};
use crate::ir::{Node, Plan};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

/// The kind of aggregate function
///
/// Examples: avg, sum, count,  ..
#[derive(Clone, Debug)]
pub enum AggregateKind {
    COUNT,
    SUM,
}

pub enum LocalAggrs {
    Aggregates(Vec<String>),
    GroupingExpressions(Vec<usize>),
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
    pub fn get_local_aggregates(&self) -> Vec<String> {
        match self {
            AggregateKind::COUNT => vec!["count".to_string()],
            AggregateKind::SUM => vec!["sum".to_string()],
        }
    }
}

/// Helper struct for adding aggregates to ir
///
/// This struct can be used for adding any Tarantool aggregate:
/// avg, sum, count, min, max, total
#[derive(Debug)]
pub struct SimpleAggregate {
    /// The aggregate function being added, like COUNT
    pub kind: AggregateKind,
    /// Aliases used in local stage of aggregation,
    /// all aggregates except AVG will have only one alias,
    /// AVG will have two
    pub local_aliases: Vec<String>,
    /// id of aggregate function in IR
    pub fun_id: usize,
}

#[cfg(not(feature = "mock"))]
fn generate_local_alias(kind: &AggregateKind, suffix: &str) -> String {
    format!(
        "\"{}_{kind}_{suffix}\"",
        uuid::Uuid::new_v4().as_simple().to_string()
    )
}

#[cfg(feature = "mock")]
fn generate_local_alias(kind: &AggregateKind, suffix: &str) -> String {
    format!("\"{kind}_{suffix}\"")
}

impl SimpleAggregate {
    fn get_local_aggregates(
        &self,
        plan: &Plan,
        is_distinct: bool,
    ) -> Result<LocalAggrs, SbroadError> {
        if is_distinct {
            let fun_expr = plan.get_expression_node(self.fun_id)?;
            if let Expression::StableFunction { children, .. } = fun_expr {
                return Ok(LocalAggrs::GroupingExpressions(children.clone()));
            }
            return Err(SbroadError::Invalid(
                Entity::Aggregate,
                Some(format!(
                    "invalid fun_id ({}), expected function got: {fun_expr:?}",
                    &self.fun_id
                )),
            ));
        }
        Ok(LocalAggrs::Aggregates(self.kind.get_local_aggregates()))
    }

    #[must_use]
    pub fn new(name: &str, fun_id: usize) -> Option<SimpleAggregate> {
        let Some(kind) = AggregateKind::new(name) else {
            return None
        };
        let mut local_aliases: Vec<String> = vec![];
        match kind {
            AggregateKind::COUNT | AggregateKind::SUM => {
                local_aliases.push(generate_local_alias(&kind, fun_id.to_string().as_str()));
            }
        }
        let aggr = SimpleAggregate {
            kind,
            local_aliases,
            fun_id,
        };
        Some(aggr)
    }
}

impl SimpleAggregate {
    /// Creates local aggregates for local `Projection`
    ///
    /// # Errors
    /// - Invalid aggregate
    pub fn create_columns_for_local_projection(
        &self,
        plan: &mut Plan,
        is_distinct: bool,
    ) -> Result<Vec<usize>, SbroadError> {
        let local_aggregates = self.kind.get_local_aggregates();
        let mut local_proj_cols: Vec<usize> = Vec::with_capacity(local_aggregates.len());
        let aggr_fun = plan.get_expression_node(self.fun_id)?;
        let aggregate_expression = if let Expression::StableFunction { children, .. } = aggr_fun {
            // this struct supports only aggregates that have a single argument
            if children.len() != 1 {
                return Err(SbroadError::UnexpectedNumberOfValues(format!(
                    "Expected aggregate function that takes only one argument. Got: {aggr_fun:?}"
                )));
            }
            *children.first().ok_or_else(|| {
                SbroadError::UnexpectedNumberOfValues(format!(
                    "Aggregate function has no children: {aggr_fun:?}"
                ))
            })?
        } else {
            return Err(SbroadError::Invalid(Entity::Aggregate, Some(format!("expected StableFunction as top of aggregate expression! Got: {aggr_fun:?}. Self: {self:?}"))));
        };
        match self.get_local_aggregates(plan, is_distinct)? {
            LocalAggrs::Aggregates(agg_names) => {
                for (pos, aggr) in agg_names.into_iter().enumerate() {
                    let fun = Function {
                        name: aggr,
                        behavior: Behavior::Stable,
                    };
                    // We can reuse `aggregate_expression` between local aggregates, because
                    // all local aggregates are located inside the same motion subtree and we
                    // assume that each local aggregate does not need to modify its expression
                    let local_fun_id =
                        plan.add_stable_function(&fun, vec![aggregate_expression])?;
                    let alias_id = plan
                        .nodes
                        .add_alias(self.local_aliases[pos].as_str(), local_fun_id)?;
                    local_proj_cols.push(alias_id);
                }
            }
            LocalAggrs::GroupingExpressions(expr_ids) => {
                // in case we have distinct modifier for aggregate,
                // we do group by aggregate function arguments.
                // We have already added those arguments to GroupBy,
                // so here we need only to create projection columns
                for (pos, expr_id) in expr_ids.iter().enumerate() {
                    let alias_id = plan
                        .nodes
                        .add_alias(self.local_aliases[pos].as_str(), *expr_id)?;
                    local_proj_cols.push(alias_id);
                }
            }
        }
        Ok(local_proj_cols)
    }

    /// Create columns with final aggregates in final `Projection`
    ///
    /// # Errors
    /// - Invalid aggregate
    /// - Could not find local alias position in child output
    ///
    pub fn create_column_for_final_projection(
        &self,
        final_proj_id: usize,
        plan: &mut Plan,
        alias_to_pos: &HashMap<String, usize>,
        is_distinct: bool,
    ) -> Result<usize, SbroadError> {
        let final_aggregate_name = if is_distinct {
            self.kind.to_string()
        } else {
            match self.kind {
                AggregateKind::COUNT | AggregateKind::SUM => "sum".to_string(),
            }
        };
        let local_alias = self.local_aliases.first().ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Aggregate,
                Some(format!("aggregate has 0 local aliases: {self:?}")),
            )
        })?;
        let Some(position) = alias_to_pos.get(local_alias.as_str()) else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format!("could find aggregate column in final Projection child by local alias: {local_alias}. Aliases: {alias_to_pos:?}"))))
        };
        let ref_node = Expression::Reference {
            parent: Some(final_proj_id),
            // projection has only one child
            targets: Some(vec![0]),
            position: *position,
        };
        let ref_id = plan.nodes.push(Node::Expression(ref_node));
        let final_aggr = StableFunction {
            name: final_aggregate_name,
            children: vec![ref_id],
            is_distinct,
        };
        let final_aggr_id = plan.nodes.push(Node::Expression(final_aggr));
        Ok(final_aggr_id)
    }
}
