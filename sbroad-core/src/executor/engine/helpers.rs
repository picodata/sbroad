use std::{
    any::Any,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    rc::Rc,
};

use tarantool::tuple::Tuple;

use crate::{
    backend::sql::tree::{OrderedSyntaxNodes, SyntaxPlan},
    debug,
    errors::{Action, Entity, SbroadError},
    executor::{
        bucket::Buckets,
        ir::ExecutionPlan,
        protocol::{Binary, EncodedOptionalData, EncodedRequiredData, OptionalData, RequiredData},
        result::ProducerResult,
        vtable::VirtualTable,
    },
    ir::{helpers::RepeatableState, tree::Snapshot, value::Value},
};

use super::{Metadata, Router, Vshard};

pub mod vshard;

#[must_use]
pub fn normalize_name_from_schema(s: &str) -> String {
    format!("\"{s}\"")
}

#[must_use]
pub fn normalize_name_from_sql(s: &str) -> String {
    if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
        return s.to_string();
    }
    format!("\"{}\"", s.to_uppercase())
}

/// A helper function to encode the execution plan into a pair of binary data:
/// * required data (plan id, parameters, etc.)
/// * optional data (execution plan, etc.)
///
/// # Errors
/// - Failed to encode the execution plan.
pub fn encode_plan(exec_plan: ExecutionPlan) -> Result<(Binary, Binary), SbroadError> {
    // We should not use the cache on the storage if the plan contains virtual tables,
    // as they can contain different amount of tuples that are not taken into account
    // when calculating the cache key.
    let can_be_cached = exec_plan.vtables_empty();
    let sub_plan_id = exec_plan.get_ir_plan().pattern_id()?;
    let sp_top_id = exec_plan.get_ir_plan().get_top()?;
    let sp = SyntaxPlan::new(&exec_plan, sp_top_id, Snapshot::Oldest)?;
    let ordered = OrderedSyntaxNodes::try_from(sp)?;
    let nodes = ordered.to_syntax_data()?;
    // Virtual tables in the plan must be already filtered, so we can use all buckets here.
    let params = exec_plan.to_params(&nodes, &Buckets::All)?;
    let query_type = exec_plan.query_type()?;
    let required_data = RequiredData::new(sub_plan_id, params, query_type, can_be_cached);
    let encoded_required_data = EncodedRequiredData::try_from(required_data)?;
    let raw_required_data: Vec<u8> = encoded_required_data.into();
    let optional_data = OptionalData::new(exec_plan, ordered);
    let encoded_optional_data = EncodedOptionalData::try_from(optional_data)?;
    let raw_optional_data: Vec<u8> = encoded_optional_data.into();
    Ok((raw_required_data.into(), raw_optional_data.into()))
}

/// Format explain output into a tuple.
///
/// # Errors
/// - Failed to create a tuple.
pub fn explain_format(explain: &str) -> Result<Box<dyn Any>, SbroadError> {
    let e = explain.lines().collect::<Vec<&str>>();

    match Tuple::new(&vec![e]) {
        Ok(t) => Ok(Box::new(t)),
        Err(e) => Err(SbroadError::FailedTo(
            Action::Create,
            Some(Entity::Tuple),
            format!("{e}"),
        )),
    }
}

/// A helper function to dispatch the execution plan from the router to the storages.
///
/// # Errors
/// - Internal errors during the execution.
pub fn dispatch(
    runtime: &impl Vshard,
    plan: &mut ExecutionPlan,
    top_id: usize,
    buckets: &Buckets,
) -> Result<Box<dyn Any>, SbroadError> {
    debug!(
        Option::from("dispatch"),
        &format!("dispatching plan: {plan:?}")
    );
    let sub_plan = plan.take_subtree(top_id)?;
    let query_type = sub_plan.query_type()?;
    let conn_type = sub_plan.connection_type()?;
    debug!(Option::from("dispatch"), &format!("sub plan: {sub_plan:?}"));

    match buckets {
        Buckets::Filtered(_) => runtime.exec_ir_on_some(sub_plan, buckets),
        Buckets::All => {
            if sub_plan.has_segmented_tables() {
                let bucket_set: HashSet<u64, RepeatableState> =
                    (1..=runtime.bucket_count()).into_iter().collect();
                let all_buckets = Buckets::new_filtered(bucket_set);
                return runtime.exec_ir_on_some(sub_plan, &all_buckets);
            }
            let (required, optional) = encode_plan(sub_plan)?;
            runtime.exec_ir_on_all(required, optional, query_type, conn_type)
        }
    }
}

/// Materialize a motion subtree into a virtual table.
///
/// # Errors
/// - Internal errors during the execution.
pub fn materialize_motion(
    runtime: &impl Router,
    plan: &mut ExecutionPlan,
    motion_node_id: usize,
    buckets: &Buckets,
) -> Result<VirtualTable, SbroadError> {
    let top_id = plan.get_motion_subtree_root(motion_node_id)?;
    let column_names = plan.get_ir_plan().get_relational_aliases(top_id)?;
    // We should get a motion alias name before we take the subtree in dispatch.
    let alias = plan.get_motion_alias(motion_node_id)?.map(String::from);
    // We also need to find out, if the motion subtree contains values node (as a result we can retrieve
    // incorrect types from the result metadata).
    let possibly_incorrect_types = plan.get_ir_plan().subtree_contains_values(motion_node_id)?;
    // Dispatch the motion subtree (it will be replaced with invalid values).
    let result = runtime.dispatch(plan, top_id, buckets)?;
    // Unlink motion node's child sub tree (it is already replaced with invalid values).
    plan.unlink_motion_subtree(motion_node_id)?;
    let mut vtable = if let Ok(tuple) = result.downcast::<Tuple>() {
        let data = tuple.decode::<Vec<ProducerResult>>().map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::Tuple),
                format!("motion node {motion_node_id}. {e}"),
            )
        })?;
        data.get(0)
            .ok_or_else(|| SbroadError::NotFound(Entity::ProducerResult, "from the tuple".into()))?
            .as_virtual_table(column_names, possibly_incorrect_types)?
    } else {
        return Err(SbroadError::Invalid(
            Entity::Motion,
            Some("the result of the motion is not a tuple".to_string()),
        ));
    };
    if let Some(name) = alias {
        vtable.set_alias(&name)?;
    }

    Ok(vtable)
}

pub fn filter_vtable(plan: &mut ExecutionPlan, bucket_ids: &[u64]) {
    if let Some(vtables) = plan.get_mut_vtables() {
        for rc_vtable in vtables.values_mut() {
            // If the virtual table id hashed by the bucket_id, we can filter its tuples.
            // Otherwise (full motion policy) we need to preserve all tuples.
            if !rc_vtable.get_index().is_empty() {
                *rc_vtable = Rc::new(rc_vtable.new_with_buckets(bucket_ids));
            }
        }
    }
}

/// A common function for all engines to calculate the sharding key value from a tuple.
///
/// # Errors
/// - The space was not found in the metadata.
/// - The sharding keys are not present in the space.
pub fn sharding_keys_from_tuple<'tuple>(
    conf: &impl Metadata,
    space: &str,
    tuple: &'tuple [Value],
) -> Result<Vec<&'tuple Value>, SbroadError> {
    let quoted_space = normalize_name_from_schema(space);
    let sharding_positions = conf.sharding_positions_by_space(&quoted_space)?;
    let mut sharding_tuple = Vec::with_capacity(sharding_positions.len());
    let table_col_amount = conf.table(&quoted_space)?.columns.len();
    if table_col_amount == tuple.len() {
        // The tuple contains a "bucket_id" column.
        for position in &sharding_positions {
            let value = tuple.get(*position).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format!("position {position:?} in the tuple {tuple:?}"),
                )
            })?;
            sharding_tuple.push(value);
        }
        Ok(sharding_tuple)
    } else if table_col_amount == tuple.len() + 1 {
        // The tuple doesn't contain the "bucket_id" column.
        let table = conf.table(&quoted_space)?;
        let bucket_position = table.get_bucket_id_position()?;

        // If the "bucket_id" splits the sharding key, we need to shift the sharding
        // key positions of the right part by one.
        // For example, we have a table with columns a, bucket_id, b, and the sharding
        // key is (a, b). Then the sharding key positions are (0, 2).
        // If someone gives us a tuple (42, 666) we should tread is as (42, null, 666).
        for position in &sharding_positions {
            let corrected_pos = match position.cmp(&bucket_position) {
                Ordering::Less => *position,
                Ordering::Equal => {
                    return Err(SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format!(
                            r#"the tuple {tuple:?} contains a "bucket_id" position {position} in a sharding key {sharding_positions:?}"#
                        )),
                    ))
                }
                Ordering::Greater => *position - 1,
            };
            let value = tuple.get(corrected_pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format!("position {corrected_pos:?} in the tuple {tuple:?}"),
                )
            })?;
            sharding_tuple.push(value);
        }
        Ok(sharding_tuple)
    } else {
        Err(SbroadError::Invalid(
            Entity::Tuple,
            Some(format!(
                "the tuple {:?} was expected to have {} filed(s), got {}.",
                tuple,
                table_col_amount - 1,
                tuple.len()
            )),
        ))
    }
}

/// A common function for all engines to calculate the sharding key value from a map.
///
/// # Errors
/// - The space was not found in the metadata.
/// - The sharding keys are not present in the space.
pub fn sharding_keys_from_map<'rec, S: ::std::hash::BuildHasher>(
    conf: &impl Metadata,
    space: &str,
    map: &'rec HashMap<String, Value, S>,
) -> Result<Vec<&'rec Value>, SbroadError> {
    let quoted_space = normalize_name_from_schema(space);
    let sharding_key = conf.sharding_key_by_space(&quoted_space)?;
    let quoted_map = map
        .iter()
        .map(|(k, _)| (normalize_name_from_schema(k), k.as_str()))
        .collect::<HashMap<String, &str>>();
    let mut tuple = Vec::with_capacity(sharding_key.len());
    for quoted_column in &sharding_key {
        if let Some(column) = quoted_map.get(quoted_column) {
            let value = map.get(*column).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format!("column {column:?} in the map {map:?}"),
                )
            })?;
            tuple.push(value);
        } else {
            return Err(SbroadError::NotFound(
                Entity::ShardingKey,
                format!(
                "(quoted) column {quoted_column:?} in the quoted map {quoted_map:?} (original map: {map:?})"
            )));
        }
    }
    Ok(tuple)
}
