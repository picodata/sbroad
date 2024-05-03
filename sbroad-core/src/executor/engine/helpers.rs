use ahash::AHashMap;

use crate::utils::MutexLike;
use itertools::enumerate;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::{
    any::Any,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    rc::Rc,
};
use tarantool::error::{Error, TarantoolErrorCode};
use tarantool::space::Space;
use tarantool::transaction::transaction;

use crate::executor::engine::helpers::storage::runtime::{prepare, read_prepared, read_unprepared};
use crate::executor::engine::{QueryCache, StorageCache};
use crate::executor::protocol::SchemaInfo;
use crate::ir::operator::ConflictStrategy;
use crate::ir::value::{EncodedValue, LuaValue, MsgPackValue};
use crate::ir::ExecuteOptions;
use crate::otm::child_span;
use crate::{
    backend::sql::{
        ir::{PatternWithParams, TmpSpaceMap},
        tree::{OrderedSyntaxNodes, SyntaxPlan},
    },
    debug,
    errors::{Action, Entity, SbroadError},
    executor::{
        bucket::Buckets,
        ir::{ExecutionPlan, QueryType},
        protocol::{Binary, EncodedOptionalData, EncodedRequiredData, OptionalData, RequiredData},
        result::{ConsumerResult, MetadataColumn, ProducerResult},
        vtable::{VTableTuple, VirtualTable},
    },
    ir::{
        distribution::Distribution,
        expression::Expression,
        helpers::RepeatableState,
        operator::Relational,
        relation::{Column, ColumnRole, Type},
        transformation::redistribution::{MotionKey, MotionPolicy},
        tree::Snapshot,
        value::Value,
        Node, Plan,
    },
    warn,
};
use sbroad_proc::otm_child_span;
use tarantool::msgpack::rmp::{self, decode::RmpRead};
use tarantool::tuple::Tuple;

use super::{Metadata, Router, Vshard};

pub mod storage;
pub mod vshard;

#[must_use]
pub fn normalize_name_from_schema(s: &str) -> SmolStr {
    format_smolstr!("\"{s}\"")
}

/// Transform:
/// * "s" -> "s" (same cased, quoted)
/// * s   -> "S" (uppercased, quoted)
#[must_use]
pub fn normalize_name_from_sql(s: &str) -> SmolStr {
    if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
        return s.to_smolstr();
    }
    format_smolstr!("\"{}\"", s.to_uppercase())
}

/// Transform:
/// * "s" -> s (same cased, unquoted)
/// * s   -> S (uppercased, unquoted)
#[must_use]
pub fn normalize_name_for_space_api(s: &str) -> SmolStr {
    if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
        return SmolStr::from(&s[1..s.len() - 1]);
    }
    s.to_uppercase().to_smolstr()
}

/// A helper function to encode the execution plan into a pair of binary data (see `Message`):
/// * required data (plan id, parameters, etc.)
/// * optional data (execution plan, etc.)
///
/// # Errors
/// - Failed to encode the execution plan.
pub fn encode_plan(mut exec_plan: ExecutionPlan) -> Result<(Binary, Binary), SbroadError> {
    let can_be_cached = exec_plan.vtables_empty();
    let query_type = exec_plan.query_type()?;
    let (ordered, sub_plan_id) = match query_type {
        QueryType::DQL => {
            let top_id = exec_plan.get_ir_plan().get_top()?;
            let sub_plan_id = if can_be_cached {
                exec_plan.get_ir_plan().pattern_id(top_id)?
            } else {
                // plan id is used as cache key on storages, no need to calculate it.
                SmolStr::default()
            };
            let sp_top_id = exec_plan.get_ir_plan().get_top()?;
            let sp = SyntaxPlan::new(&exec_plan, sp_top_id, Snapshot::Oldest)?;
            let ordered = OrderedSyntaxNodes::try_from(sp)?;
            (ordered, sub_plan_id)
        }
        QueryType::DML => {
            let plan = exec_plan.get_ir_plan();
            let sp_top_id = plan.get_top()?;
            let sp_top = plan.get_relation_node(sp_top_id)?;
            let motion_id = match sp_top {
                Relational::Insert { children, .. }
                | Relational::Delete { children, .. }
                | Relational::Update { children, .. } => *children.first().ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Plan,
                        Some(format_smolstr!(
                            "expected at least one child under DML node {sp_top:?}",
                        )),
                    )
                })?,
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::Plan,
                        Some(format_smolstr!("unsupported DML statement: {sp_top:?}",)),
                    ));
                }
            };
            let motion = plan.get_relation_node(motion_id)?;
            let Relational::Motion { policy, .. } = motion else {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "expected motion node under dml node, got: {motion:?}",
                    )),
                ));
            };
            let mut sub_plan_id = SmolStr::default();
            // SQL is needed only for the motion node subtree.
            // HACK: we don't actually need SQL when the subtree is already
            //       materialized into a virtual table on the router.
            let already_materialized = exec_plan.get_motion_vtable(motion_id).is_ok();
            let ordered = if already_materialized {
                OrderedSyntaxNodes::empty()
            } else if let MotionPolicy::LocalSegment { .. } | MotionPolicy::Local = policy {
                let proj_id = exec_plan.get_motion_subtree_root(motion_id)?;
                sub_plan_id = exec_plan.get_ir_plan().pattern_id(proj_id)?;
                let motion_child_id = exec_plan.get_motion_child(motion_id)?;
                let sp = SyntaxPlan::new(&exec_plan, motion_child_id, Snapshot::Oldest)?;
                OrderedSyntaxNodes::try_from(sp)?
            } else {
                // In case we are not dealing with `LocalSegment` and `Local` policies, `exec_plan`
                // must contain vtable for `motion_id` (See `dispatch` method in `src/executor.rs`)
                // so we mustn't got here.
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format_smolstr!(
                        "unsupported motion policy under DML node: {policy:?}",
                    )),
                ));
            };
            (ordered, sub_plan_id)
        }
    };
    // We should not use the cache on the storage if the plan contains virtual tables,
    // as they can contain different amount of tuples that are not taken into account
    // when calculating the cache key.
    let nodes = ordered.to_syntax_data()?;
    // Virtual tables in the plan must be already filtered, so we can use all buckets here.
    let params = exec_plan.to_params(&nodes, &Buckets::All)?;
    let router_version_map = std::mem::take(&mut exec_plan.get_mut_ir_plan().version_map);
    let schema_info = SchemaInfo::new(router_version_map);
    let required_data = RequiredData::new(
        sub_plan_id,
        params,
        query_type,
        can_be_cached,
        exec_plan.get_ir_plan().options.clone(),
        schema_info,
    );
    let encoded_required_data = EncodedRequiredData::try_from(required_data)?;
    let raw_required_data: Vec<u8> = encoded_required_data.into();
    let optional_data = OptionalData::new(exec_plan, ordered);
    let encoded_optional_data = EncodedOptionalData::try_from(optional_data)?;
    let raw_optional_data: Vec<u8> = encoded_optional_data.into();
    Ok((raw_required_data.into(), raw_optional_data.into()))
}

/// Decode the execution plan from msgpack into a pair of binary data:
/// * required data (plan id, parameters, etc.)
/// * optional data (execution plan, etc.)
///
/// # Errors
/// - Failed to decode the execution plan.
pub fn decode_msgpack(tuple_buf: &[u8]) -> Result<(Vec<u8>, Vec<u8>), SbroadError> {
    let mut stream = rmp::decode::Bytes::from(tuple_buf);
    let array_len = rmp::decode::read_array_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("array length: {e:?}"),
        )
    })? as usize;
    if array_len != 2 {
        return Err(SbroadError::Invalid(
            Entity::Tuple,
            Some(format_smolstr!(
                "expected tuple of 2 elements, got {array_len}"
            )),
        ));
    }
    let req_len = rmp::decode::read_str_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read required data length: {e:?}"),
        )
    })? as usize;
    let mut required: Vec<u8> = vec![0_u8; req_len];
    stream.read_exact_buf(&mut required).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read required data: {e:?}"),
        )
    })?;

    let opt_len = rmp::decode::read_str_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read optional data string length: {e:?}"),
        )
    })? as usize;
    let mut optional: Vec<u8> = vec![0_u8; opt_len];
    stream.read_exact_buf(&mut optional).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format_smolstr!("read optional data: {e:?}"),
        )
    })?;

    Ok((required, optional))
}

/// Decode dispatched optional data (execution plan, etc.) from msgpack
/// and compile it into a pattern with parameters and temporary space map.
///
/// # Errors
/// - Failed to decode or compile optional data.
pub fn compile_encoded_optional(
    raw_optional: &mut Vec<u8>,
) -> Result<(PatternWithParams, TmpSpaceMap), SbroadError> {
    let data = std::mem::take(raw_optional);
    let mut optional = OptionalData::try_from(EncodedOptionalData::from(data))?;
    compile_optional(&mut optional)
}

/// Compile already decoded optional data into a pattern with parameters
/// and temporary space map
///
/// # Errors
/// - Failed to compile optional data
pub fn compile_optional(
    optional: &mut OptionalData,
) -> Result<(PatternWithParams, TmpSpaceMap), SbroadError> {
    // Use all buckets as we don't want to filter any data from the execution plan
    // (this work has already been done on the coordinator).
    let buckets = Buckets::All;

    optional.exec_plan.get_mut_ir_plan().restore_constants()?;
    let nodes = optional.ordered.to_syntax_data()?;
    let (u, v) = optional.exec_plan.to_sql(
        &nodes,
        &buckets,
        &uuid::Uuid::new_v4().as_simple().to_string(),
    )?;
    Ok((u, v))
}

/// Command to build a tuple suitable to be passed into Tarantool API functions.
/// For more information see `TupleBuilderPattern` docs.
#[derive(Debug)]
pub enum TupleBuilderCommand {
    /// Take a value from the original tuple
    /// at the specified position.
    TakePosition(usize),
    /// Take a value from the original tuple and cast
    /// it into specified type.
    TakeAndCastPosition(usize, Type),
    /// Set a specified value.
    /// Related only to the tuple we are currently constructing and not to the original tuple.
    SetValue(Value),
    /// Calculate a bucket_id for the new tuple
    /// using the specified motion key.
    CalculateBucketId(MotionKey),
    /// Update table column to the value in original tupleon specified position.
    /// Needed only for `Update`.
    UpdateColToPos(usize, usize),
    /// Update table column to the value in original tuple on specified position and cast it
    /// into specifeid type.
    /// Needed only for `Update`.
    UpdateColToCastedPos(usize, usize, Type),
}

/// Vec of commands that helps us transforming `VTableTuple` into a tuple suitable to be passed
/// into Tarantool API functions (like `delete`, `update`, `replace` and others).
/// Each command in this vec operates on the same `VTableTuple`. E.g. taking some value from
/// it (on specified position) and putting it into the resulting tuple.
pub type TupleBuilderPattern = Vec<TupleBuilderCommand>;

fn init_local_update_tuple_builder(
    plan: &Plan,
    vtable: &VirtualTable,
    update_id: usize,
) -> Result<TupleBuilderPattern, SbroadError> {
    if let Relational::Update {
        relation,
        update_columns_map,
        pk_positions,
        ..
    } = plan.get_relation_node(update_id)?
    {
        let mut commands: TupleBuilderPattern =
            Vec::with_capacity(update_columns_map.len() + pk_positions.len());
        let rel = plan.get_relation_or_error(relation)?;
        for (table_pos, tuple_pos) in update_columns_map {
            let rel_type = &rel
                .columns
                .get(*table_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid position in update table: {table_pos}"
                    ))
                })?
                .r#type;
            let vtable_type = &vtable
                .get_columns()
                .get(*tuple_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid position in update vtable: {tuple_pos}"
                    ))
                })?
                .r#type;
            if rel_type == vtable_type {
                commands.push(TupleBuilderCommand::UpdateColToPos(*table_pos, *tuple_pos));
            } else {
                commands.push(TupleBuilderCommand::UpdateColToCastedPos(
                    *table_pos,
                    *tuple_pos,
                    rel_type.clone(),
                ));
            }
        }
        for (idx, pk_pos) in pk_positions.iter().enumerate() {
            let table_pos = *rel.primary_key.positions.get(idx).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::Update,
                    Some(format_smolstr!(
                        "invalid primary key positions: len: {}, expected len: {}",
                        pk_positions.len(),
                        rel.primary_key.positions.len()
                    )),
                )
            })?;
            let rel_type = &rel
                .columns
                .get(table_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid primary key position in table: {table_pos}"
                    ))
                })?
                .r#type;
            let vtable_type = &vtable
                .get_columns()
                .get(*pk_pos)
                .ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::Update,
                        Some(format_smolstr!("invalid pk position: {pk_pos}")),
                    )
                })?
                .r#type;
            if rel_type == vtable_type {
                commands.push(TupleBuilderCommand::TakePosition(*pk_pos));
            } else {
                commands.push(TupleBuilderCommand::TakeAndCastPosition(
                    *pk_pos,
                    rel_type.clone(),
                ));
            }
        }
        return Ok(commands);
    }
    Err(SbroadError::Invalid(
        Entity::Node,
        Some(format_smolstr!("expected Update on id ({update_id})")),
    ))
}

/// Create commands to build the tuple for insertion
fn init_delete_tuple_builder(
    plan: &Plan,
    delete_id: usize,
) -> Result<TupleBuilderPattern, SbroadError> {
    let table = plan.dml_node_table(delete_id)?;
    let mut commands = Vec::with_capacity(table.primary_key.positions.len());
    for pos in &table.primary_key.positions {
        commands.push(TupleBuilderCommand::TakePosition(*pos));
    }
    Ok(commands)
}

/// Create commands to build the tuple for insertion,
///
/// # Errors
/// - Invalid insert node or plan
fn init_insert_tuple_builder(
    plan: &Plan,
    vtable: &VirtualTable,
    insert_id: usize,
) -> Result<TupleBuilderPattern, SbroadError> {
    let columns = plan.insert_columns(insert_id)?;
    // Revert map of { pos_in_child_node -> pos_in_relation }
    // into map of { pos_in_relation -> pos_in_child_node }.
    let columns_map: AHashMap<usize, usize> = columns
        .iter()
        .enumerate()
        .map(|(pos, id)| (*id, pos))
        .collect::<AHashMap<_, _>>();
    let relation = plan.dml_node_table(insert_id)?;
    let mut commands = Vec::with_capacity(relation.columns.len());
    for (pos, table_col) in relation.columns.iter().enumerate() {
        if table_col.role == ColumnRole::Sharding {
            let motion_key = plan.insert_motion_key(insert_id)?;
            commands.push(TupleBuilderCommand::CalculateBucketId(motion_key));
        } else if columns_map.contains_key(&pos) {
            // It is safe to unwrap here because we have checked that
            // the column is present in the tuple.
            let tuple_pos = columns_map[&pos];
            let vtable_type = &vtable
                .get_columns()
                .get(tuple_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid index in virtual table: {tuple_pos}"
                    ))
                })?
                .r#type;
            let rel_type = &table_col.r#type;
            if vtable_type == rel_type {
                commands.push(TupleBuilderCommand::TakePosition(tuple_pos));
            } else {
                commands.push(TupleBuilderCommand::TakeAndCastPosition(
                    tuple_pos,
                    rel_type.clone(),
                ));
            }
        } else {
            // FIXME: support default values other then NULL (issue #442).
            commands.push(TupleBuilderCommand::SetValue(Column::default_value()));
        }
    }
    Ok(commands)
}

/// Create commands to build the tuple for sharded `Update`,
///
/// # Errors
/// - Invalid insert node or plan
fn init_sharded_update_tuple_builder(
    plan: &Plan,
    vtable: &VirtualTable,
    update_id: usize,
) -> Result<TupleBuilderPattern, SbroadError> {
    let Relational::Update {
        update_columns_map, ..
    } = plan.get_relation_node(update_id)?
    else {
        return Err(SbroadError::Invalid(
            Entity::Node,
            Some(format_smolstr!(
                "update tuple builder: expected update node on id: {update_id}"
            )),
        ));
    };
    let relation = plan.dml_node_table(update_id)?;
    let mut commands = Vec::with_capacity(relation.columns.len());
    for (pos, table_col) in relation.columns.iter().enumerate() {
        if table_col.role == ColumnRole::Sharding {
            // the bucket is taken from the index (see `execute_sharded_update` logic),
            // no need to specify motion key
            commands.push(TupleBuilderCommand::CalculateBucketId(MotionKey {
                targets: vec![],
            }));
        } else if update_columns_map.contains_key(&pos) {
            let tuple_pos = update_columns_map[&pos];
            let vtable_type = &vtable
                .get_columns()
                .get(tuple_pos)
                .ok_or_else(|| {
                    SbroadError::UnexpectedNumberOfValues(format_smolstr!(
                        "invalid index in virtual table: {tuple_pos}"
                    ))
                })?
                .r#type;
            let rel_type = &table_col.r#type;
            if vtable_type == rel_type {
                commands.push(TupleBuilderCommand::TakePosition(tuple_pos));
            } else {
                commands.push(TupleBuilderCommand::TakeAndCastPosition(
                    tuple_pos,
                    rel_type.clone(),
                ));
            }
        } else {
            // Note, that as soon as we're dealing with sharded update, `Projection` output below
            // the `Update` node must contain the same number of values as the updating table.
            // That's why `update_columns_map` must contain value for all the columns present in the
            // `relation`.

            return Err(SbroadError::Invalid(
                Entity::Update,
                Some(format_smolstr!(
                    "user column {pos} not found in update column map"
                )),
            ));
        }
    }
    Ok(commands)
}

/// Generate an empty result for the specified plan:
/// * In case of DML (INSERT) return result with `row_count` equal to 0.
/// * In case of DQL (SELECT) return columns metadata with no result tuples.
///   * **Note**: Returns `None` if the plan is with replicated output (another words query
///     that may be executed on any node) of the top node (i.e. `select from values`).
///
/// # Errors
/// - failed to get query type;
/// - failed to get top node;
/// - the top node is not a valid relation node;
pub fn empty_query_result(plan: &ExecutionPlan) -> Result<Option<Box<dyn Any>>, SbroadError> {
    let query_type = plan.query_type()?;
    match query_type {
        QueryType::DML => {
            let result = ConsumerResult::default();
            let tuple = Tuple::new(&(result,))
                .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format_smolstr!("{e:?}"))))?;
            Ok(Some(Box::new(tuple) as Box<dyn Any>))
        }
        QueryType::DQL => {
            // Get metadata (column types) from the top node's output tuple.
            let ir_plan = plan.get_ir_plan();
            let top_id = ir_plan.get_top()?;
            let top_output_id = ir_plan.get_relation_node(top_id)?.output();
            if let Distribution::Segment { .. } = ir_plan.get_distribution(top_output_id)? {
            } else {
                // We can have some `select values` query that should be executed on a random
                // node rather then returning an empty result.
                return Ok(None);
            }
            let columns = ir_plan.get_row_list(top_output_id)?;
            let mut metadata = Vec::with_capacity(columns.len());
            for col_id in columns {
                let column = ir_plan.get_expression_node(*col_id)?;
                let column_type = column.calculate_type(ir_plan)?;
                let column_name = if let Expression::Alias { name, .. } = column {
                    name.clone()
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Expression,
                        Some(format_smolstr!("expected alias, got {column:?}")),
                    ));
                };
                metadata.push(MetadataColumn::new(
                    column_name.to_string(),
                    column_type.to_string(),
                ));
            }
            let result = ProducerResult {
                metadata,
                ..Default::default()
            };

            let tuple = Tuple::new(&(result,))
                .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format_smolstr!("{e:?}"))))?;
            Ok(Some(Box::new(tuple) as Box<dyn Any>))
        }
    }
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
            format_smolstr!("{e}"),
        )),
    }
}

/// A helper function to dispatch the execution plan from the router to the storages.
///
/// # Errors
/// - Internal errors during the execution.
pub fn dispatch_impl(
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
    debug!(Option::from("dispatch"), &format!("sub plan: {sub_plan:?}"));
    dispatch_by_buckets(sub_plan, buckets, runtime)
}

/// Helper function that chooses one of the methods for execution
/// based on buckets.
///
/// # Errors
/// - Failed to dispatch
pub fn dispatch_by_buckets(
    sub_plan: ExecutionPlan,
    buckets: &Buckets,
    runtime: &impl Vshard,
) -> Result<Box<dyn Any>, SbroadError> {
    let query_type = sub_plan.query_type()?;
    let conn_type = sub_plan.connection_type()?;

    match buckets {
        Buckets::Filtered(_) => runtime.exec_ir_on_some(sub_plan, buckets),
        Buckets::Any => {
            if sub_plan.has_customization_opcodes() {
                return Err(SbroadError::Invalid(
                    Entity::SubTree,
                    Some(
                        "plan customization is needed only when executing on multiple replicasets"
                            .into(),
                    ),
                ));
            }
            // Check that all vtables don't have index. Because if they do,
            // they will be filtered later by filter_vtable
            if let Some(vtables) = &sub_plan.vtables {
                for (motion_id, vtable) in vtables.map() {
                    if !vtable.get_bucket_index().is_empty() {
                        return Err(SbroadError::Invalid(
                            Entity::Motion,
                            Some(format_smolstr!("motion ({motion_id}) in subtree with distribution Single, but policy is not Full!")),
                        ));
                    }
                }
            }
            runtime.exec_ir_on_any_node(sub_plan)
        }
        Buckets::All => {
            if sub_plan.has_segmented_tables() || sub_plan.has_customization_opcodes() {
                let bucket_set: HashSet<u64, RepeatableState> =
                    (1..=runtime.bucket_count()).collect();
                let all_buckets = Buckets::new_filtered(bucket_set);
                return runtime.exec_ir_on_some(sub_plan, &all_buckets);
            }
            let vtable_max_rows = sub_plan.get_vtable_max_rows();
            let (required, optional) = encode_plan(sub_plan)?;
            runtime.exec_ir_on_all(required, optional, query_type, conn_type, vtable_max_rows)
        }
    }
}

#[allow(clippy::too_many_lines)]
pub(crate) fn materialize_values(
    plan: &mut ExecutionPlan,
    motion_node_id: usize,
) -> Result<Option<VirtualTable>, SbroadError> {
    // Check that the motion node has a local segment policy.
    let motion_node = plan.get_ir_plan().get_relation_node(motion_node_id)?;
    if let Relational::Motion {
        policy: MotionPolicy::LocalSegment(_),
        ..
    } = motion_node
    {
    } else {
        return Ok(None);
    };

    // Check that the motion child is a values node with constants in the rows.
    //
    // When the VALUES node supports subqueries, arithmetics, etc. in addition
    // to constants, we have to rewrite this code (need to check that there are
    // no subqueries before node replacement).
    let child_id = plan.get_motion_child(motion_node_id)?;
    if !matches!(
        plan.get_ir_plan().get_relation_node(child_id)?,
        Relational::Values { .. }
    ) {
        return Ok(None);
    }
    let child_node_ref = plan.get_mut_ir_plan().get_mut_node(child_id)?;
    let child_node = std::mem::replace(child_node_ref, Node::Parameter);
    if let Node::Relational(Relational::Values {
        ref children,
        output,
        ..
    }) = child_node
    {
        // Build a new virtual table (check that all the rows are made of constants only).
        let mut vtable = VirtualTable::new();
        vtable.get_mut_tuples().reserve(children.len());

        let columns_len = if let Some(first_row_id) = children.first() {
            let row_node = plan.get_ir_plan().get_relation_node(*first_row_id)?;
            let Relational::ValuesRow { data, .. } = row_node else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!("Expected ValuesRow, got {row_node:?}")),
                ));
            };
            plan.get_ir_plan()
                .get_expression_node(*data)?
                .get_row_list()?
                .len()
        } else {
            return Err(SbroadError::Invalid(
                Entity::Node,
                Some(format_smolstr!(
                    "Values node {child_node:?} must contain children"
                )),
            ));
        };
        let mut nullable_column_indices = HashSet::with_capacity(columns_len);
        for row_id in children {
            let row_node = plan.get_ir_plan().get_relation_node(*row_id)?;
            if let Relational::ValuesRow { data, children, .. } = row_node {
                // Check that there are no subqueries in the values node.
                // (If any we'll need to materialize them first with dispatch
                // to the storages.)
                if !children.is_empty() {
                    return Err(SbroadError::Invalid(
                        Entity::Node,
                        Some("VALUES rows supports only constants in its columns.".to_smolstr()),
                    ));
                }
                let data = *data;
                // Check that all the values are constants.
                let columns_len = plan.get_ir_plan().get_row_list(data)?.len();
                let mut row: VTableTuple = Vec::with_capacity(columns_len);
                for idx in 0..columns_len {
                    let column_id =
                        *plan
                            .get_ir_plan()
                            .get_row_list(data)?
                            .get(idx)
                            .ok_or_else(|| {
                                SbroadError::NotFound(
                                    Entity::Column,
                                    format_smolstr!("at position {idx} in the row"),
                                )
                            })?;
                    let column_node_ref = plan.get_mut_ir_plan().get_mut_node(column_id)?;
                    let column_node = std::mem::replace(column_node_ref, Node::Parameter);
                    if let Node::Expression(Expression::Constant { value, .. }) = column_node {
                        if let Value::Null = value {
                            nullable_column_indices.insert(idx);
                        }
                        row.push(value);
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some(format_smolstr!(
                                "VALUES rows supports only constants in its columns (got: {column_node:?})."
                            )),
                        ));
                    }
                }
                vtable.add_tuple(row);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "value node child ({child_id}) is not a values row node!"
                    )),
                ));
            }
        }
        // Build virtual table's columns.
        let output_cols = plan
            .get_ir_plan()
            .get_expression_node(output)?
            .get_row_list()?;
        let columns = vtable.get_mut_columns();
        columns.reserve(output_cols.len());
        for (idx, column_id) in enumerate(output_cols) {
            let is_nullable = nullable_column_indices.contains(&idx);
            let alias = plan.get_ir_plan().get_expression_node(*column_id)?;
            if let Expression::Alias { name, .. } = alias {
                let column = Column {
                    name: name.clone(),
                    r#type: Type::Scalar,
                    role: ColumnRole::User,
                    is_nullable,
                };
                columns.push(column);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format_smolstr!(
                        "output column ({column_id}) is not an alias node!"
                    )),
                ));
            }
        }
        return Ok(Some(vtable));
    }
    Ok(None)
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
    // We should get a motion alias name before we take the subtree in `dispatch` method.
    let alias = plan.get_motion_alias(motion_node_id)?;
    // We also need to find out, if the motion subtree contains values node
    // (as a result we can retrieve incorrect types from the result metadata).
    let possibly_incorrect_types = plan.get_ir_plan().subtree_contains_values(motion_node_id)?;
    // Dispatch the motion subtree (it will be replaced with invalid values).
    let result = runtime.dispatch(plan, top_id, buckets)?;
    // Unlink motion node's child sub tree (it is already replaced with invalid values).
    plan.unlink_motion_subtree(motion_node_id)?;
    let mut vtable = if let Ok(tuple) = result.downcast::<Tuple>() {
        let mut data = tuple.decode::<Vec<ProducerResult>>().map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::Tuple),
                format_smolstr!("motion node {motion_node_id}. {e}"),
            )
        })?;
        data.get_mut(0)
            .ok_or_else(|| SbroadError::NotFound(Entity::ProducerResult, "from the tuple".into()))?
            .as_virtual_table(column_names, possibly_incorrect_types)?
    } else {
        return Err(SbroadError::Invalid(
            Entity::Motion,
            Some("the result of the motion is not a tuple".to_smolstr()),
        ));
    };

    if let Some(name) = alias {
        vtable.set_alias(name.as_str())?;
    }

    Ok(vtable)
}

/// Function that is called from `exec_ir_on_some_buckets`.
/// Its purpose is to iterate through every vtable presented in `plan` subtree and
/// to replace them by new vtables. New vtables indices (map bucket id -> tuples) will contain
/// only pairs corresponding to buckets, that are presented in given `bucket_ids` (as we are going
/// to execute `plan` subtree only on them).
///
/// # Errors
/// - failed to build a new virtual table with the passed set of buckets
pub fn filter_vtable(plan: &mut ExecutionPlan, bucket_ids: &[u64]) -> Result<(), SbroadError> {
    if let Some(vtables) = plan.get_mut_vtables() {
        for rc_vtable in vtables.values_mut() {
            // If the virtual table id hashed by the bucket_id, we can filter its tuples.
            // Otherwise (full motion policy) we need to preserve all tuples.
            if !rc_vtable.get_bucket_index().is_empty() {
                *rc_vtable = Rc::new(rc_vtable.new_with_buckets(bucket_ids)?);
            }
        }
    }
    Ok(())
}

/// A common function for all engines to calculate the sharding key value from a tuple.
///
/// # Errors
/// - The space was not found in the metadata.
/// - The sharding key are not present in the space.
pub fn sharding_key_from_tuple<'tuple>(
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
                    format_smolstr!("position {position:?} in the tuple {tuple:?}"),
                )
            })?;
            sharding_tuple.push(value);
        }
        Ok(sharding_tuple)
    } else if table_col_amount == tuple.len() + 1 {
        // The tuple doesn't contain the "bucket_id" column.
        let table = conf.table(&quoted_space)?;
        let bucket_position = table.get_bucket_id_position()?.ok_or_else(|| {
            SbroadError::Invalid(
                Entity::Space,
                Some("global space does not have a sharding key!".into()),
            )
        })?;

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
                        Some(format_smolstr!(
                            r#"the tuple {tuple:?} contains a "bucket_id" position {position} in a sharding key {sharding_positions:?}"#
                        )),
                    ));
                }
                Ordering::Greater => *position - 1,
            };
            let value = tuple.get(corrected_pos).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format_smolstr!("position {corrected_pos:?} in the tuple {tuple:?}"),
                )
            })?;
            sharding_tuple.push(value);
        }
        Ok(sharding_tuple)
    } else {
        Err(SbroadError::Invalid(
            Entity::Tuple,
            Some(format_smolstr!(
                "the tuple {:?} was expected to have {} filed(s), got {}.",
                tuple,
                table_col_amount - 1,
                tuple.len()
            )),
        ))
    }
}

/// If the statement with given `plan_id` is found in the cache,
/// execute it and return result. Otherwise, returns `None`.
///
/// This function works only for read statements, and it is a
/// responsibility of a caller to ensure it.
///
/// # Errors
/// - Failed to borrow the cache
/// - Failed to execute given statement.
pub fn exec_if_in_cache<R: QueryCache>(
    runtime: &R,
    params: &[Value],
    plan_id: &SmolStr,
    vtable_max_rows: u64,
    opts: ExecuteOptions,
) -> Result<Option<Box<dyn Any>>, SbroadError>
where
    R::Cache: StorageCache,
{
    // Look for the prepared statement in the cache.
    if let Some(stmt) = runtime.cache().lock().get(plan_id)? {
        let stmt_id = stmt.id()?;
        // The statement was found in the cache, so we can execute it.
        debug!(
            Option::from("execute plan"),
            &format!("Execute prepared statement: {stmt:?}"),
        );
        let result = cache_hit_read_prepared(stmt_id, params, vtable_max_rows, opts);

        // If prepared statement is invalid for some reason, fallback to the long pass
        // and recompile the query.
        if result.is_ok() {
            return Ok(Some(result?));
        }
    }
    debug!(
        Option::from("execute plan"),
        &format!("Failed to find a plan (id {plan_id}) in the cache."),
    );
    Ok(None)
}

/// Helper func to create span for cache hit read.
#[otm_child_span("tarantool.cache.hit.read.prepared")]
fn cache_hit_read_prepared(
    stmt_id: u32,
    params: &[Value],
    vtable_max_rows: u64,
    opts: ExecuteOptions,
) -> Result<Box<dyn Any>, SbroadError> {
    read_prepared(stmt_id, "", params, vtable_max_rows, opts)
}

/// Helper func to create span for cache miss read.
#[otm_child_span("tarantool.cache.miss.read.prepared")]
fn cache_miss_read_prepared(
    stmt_id: u32,
    pattern: &str,
    params: &[Value],
    vtable_max_rows: u64,
    opts: ExecuteOptions,
) -> Result<Box<dyn Any>, SbroadError> {
    read_prepared(stmt_id, pattern, params, vtable_max_rows, opts)
}

/// Tries to prepare read statement and then execute it.
/// In case `prepare` fails, the statement will be executed anyway.
///
/// It is responsibility of the caller, to ensure that it is a
/// read statement - not write.
///
/// # Errors
/// - Failed to prepare the statement
/// - Failed to borrow runtime's cache
/// - Tarantool execution error
#[allow(unused_variables)]
pub fn prepare_and_read<R: Vshard + QueryCache>(
    runtime: &R,
    pattern_with_params: &PatternWithParams,
    plan_id: &SmolStr,
    vtable_max_rows: u64,
    opts: ExecuteOptions,
    schema_info: &SchemaInfo,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    match prepare(&pattern_with_params.pattern) {
        Ok(stmt) => {
            let stmt_id = stmt.id()?;
            debug!(
                Option::from("execute plan"),
                &format!(
                    "Created prepared statement {} for the pattern {}",
                    stmt_id,
                    stmt.pattern()?
                ),
            );
            runtime
                .cache()
                .lock()
                .put(plan_id.clone(), stmt, schema_info)?;
            // The statement was found in the cache, so we can execute it.
            debug!(
                Option::from("execute plan"),
                &format!("Execute prepared statement: {stmt_id}"),
            );
            cache_miss_read_prepared(
                stmt_id,
                &pattern_with_params.pattern,
                &pattern_with_params.params,
                vtable_max_rows,
                opts,
            )
        }
        Err(e) => {
            // Possibly the statement is correct, but doesn't fit into
            // Tarantool's prepared statements cache (`sql_cache_size`).
            // So we try to execute it bypassing the cache.
            warn!(
                Option::from("execute"),
                &format!(
                    "Failed to prepare the statement: {}, error: {}",
                    pattern_with_params.pattern, e
                ),
            );
            read_unprepared(
                &pattern_with_params.pattern,
                &pattern_with_params.params,
                vtable_max_rows,
                opts,
            )
        }
    }
}

/// Execute read statement that can be cached.
///
/// If statement is in the runtime's cache it is executed,
/// otherwise the optional data (plan) is compiled into
/// the string. Then we try to prepare the statement and
/// put it into the cache. if prepare fails, the statement
/// is executed anyway.
///
/// # Note
/// It is responsibility of the caller to check that
/// statement can be cached or that it is a read statement.
///
/// # Errors
/// - Failed to borrow runtime's cache
/// - Tarantool execution error for given statement
/// - Failed to compile the optional data
/// - Failed to prepare the statement
#[allow(unused_variables)]
pub fn execute_cacheable_dql<R: Vshard + QueryCache>(
    runtime: &R,
    required: &mut RequiredData,
    optional: &mut OptionalData,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    let opts = std::mem::take(&mut required.options.execute_options);
    if let Some(res) = exec_if_in_cache(
        runtime,
        &required.parameters,
        &required.plan_id,
        required.options.vtable_max_rows,
        opts.clone(),
    )? {
        return Ok(res);
    }
    let (pattern_with_params, _tmp_spaces) = compile_optional(optional)?;
    prepare_and_read(
        runtime,
        &pattern_with_params,
        &required.plan_id,
        required.options.vtable_max_rows,
        opts,
        &required.schema_info,
    )
}

/// Execute DML on the storage.
///
/// # Errors
/// - Failed to execute DML locally.
#[allow(clippy::too_many_lines)]
pub fn execute_dml_on_storage<R: Vshard + QueryCache>(
    runtime: &R,
    raw_optional: &mut Vec<u8>,
    required: &mut RequiredData,
) -> Result<ConsumerResult, SbroadError>
where
    R::Cache: StorageCache,
{
    let data = std::mem::take(raw_optional);
    let mut optional = OptionalData::try_from(EncodedOptionalData::from(data))?;
    optional.exec_plan.get_mut_ir_plan().restore_constants()?;
    let plan = optional.exec_plan.get_ir_plan();
    let top_id = plan.get_top()?;
    let top = plan.get_relation_node(top_id)?;
    match top {
        Relational::Insert { .. } => execute_insert_on_storage(runtime, &mut optional, required),
        Relational::Delete { .. } => execute_delete_on_storage(runtime, &mut optional, required),
        Relational::Update { .. } => execute_update_on_storage(runtime, &mut optional, required),
        _ => Err(SbroadError::Invalid(
            Entity::Plan,
            Some(format_smolstr!(
                "expected DML node on the plan top, got {top:?}"
            )),
        )),
    }
}

/// Helper function to materialize vtable on storage (not on router).
fn materialize_vtable_locally<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
    child_id: usize,
) -> Result<(), SbroadError>
where
    R::Cache: StorageCache,
{
    let subplan_top_id = optional.exec_plan.get_motion_subtree_root(child_id)?;
    let plan = optional.exec_plan.get_ir_plan();
    let column_names = plan.get_relational_aliases(subplan_top_id)?;
    optional.exec_plan.get_mut_ir_plan().restore_constants()?;
    let result = if required.can_be_cached {
        execute_cacheable_dql(runtime, required, optional)?
    } else {
        let opts = std::mem::take(&mut required.options.execute_options);
        execute_non_cacheable_dql(optional, required.options.vtable_max_rows, opts)?
    };
    let tuple = result.downcast::<Tuple>().map_err(|e| {
        SbroadError::FailedTo(
            Action::Deserialize,
            Some(Entity::Tuple),
            format_smolstr!("motion node {child_id}. {e:?}"),
        )
    })?;
    let mut data = tuple.decode::<Vec<ProducerResult>>().map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::Tuple),
            format_smolstr!("motion node {child_id}. {e}"),
        )
    })?;
    let vtable = data
        .get_mut(0)
        .ok_or_else(|| SbroadError::NotFound(Entity::ProducerResult, "from the tuple".into()))?
        // It is a DML query, so we don't need to care about the column types
        // in response. So, simply use scalar type for all the columns.
        .as_virtual_table(column_names, true)?;
    optional
        .exec_plan
        .set_motion_vtable(child_id, vtable, runtime)?;
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn execute_update_on_storage<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
) -> Result<ConsumerResult, SbroadError>
where
    R::Cache: StorageCache,
{
    let plan = optional.exec_plan.get_ir_plan();
    let update_id = plan.get_top()?;
    let update_child_id = plan.dml_child_id(update_id)?;
    let space_name = normalize_name_for_space_api(plan.dml_node_table(update_id)?.name());
    let mut result = ConsumerResult::default();
    let is_sharded = plan.is_sharded_update(update_id)?;
    let build_vtable_locally = optional
        .exec_plan
        .get_motion_vtable(update_child_id)
        .is_err();
    if build_vtable_locally {
        // it is relevant only for local Update.
        if is_sharded {
            return Err(SbroadError::Invalid(
                Entity::Update,
                Some("sharded Update's vtable must be already materialized".into()),
            ));
        }
        materialize_vtable_locally(runtime, optional, required, update_child_id)?;
    }
    let vtable = optional.exec_plan.get_motion_vtable(update_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Space,
            Some(format_smolstr!("space {space_name} not found")),
        )
    })?;
    transaction(|| -> Result<(), SbroadError> {
        let plan = optional.exec_plan.get_ir_plan();
        if is_sharded {
            let delete_tuple_len = plan.get_update_delete_tuple_len(update_id)?;
            let builder = init_sharded_update_tuple_builder(plan, &vtable, update_id)?;
            execute_sharded_update(&mut result, &vtable, &space, &builder, delete_tuple_len)?;
        } else {
            let builder = init_local_update_tuple_builder(plan, &vtable, update_id)?;
            execute_local_update(&mut result, &builder, &vtable, &space)?;
        }
        Ok(())
    })?;

    Ok(result)
}

/// A working horse for `execute_update_on_storage` in case we're dealing with
/// sharded update.
fn execute_sharded_update(
    result: &mut ConsumerResult,
    vtable: &VirtualTable,
    space: &Space,
    builder: &TupleBuilderPattern,
    delete_tuple_len: usize,
) -> Result<(), SbroadError> {
    for tuple in vtable.get_tuples() {
        if tuple.len() == delete_tuple_len {
            let pk: Vec<EncodedValue> = tuple
                .iter()
                .map(|val| EncodedValue::Ref(MsgPackValue::from(val)))
                .collect();
            if let Err(Error::Tarantool(tnt_err)) = space.delete(&pk) {
                return Err(SbroadError::FailedTo(
                    Action::Delete,
                    Some(Entity::Tuple),
                    format_smolstr!("{tnt_err:?}"),
                ));
            }
        }
    }
    for (bucket_id, positions) in vtable.get_bucket_index() {
        for pos in positions {
            let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                SbroadError::Invalid(
                    Entity::VirtualTable,
                    Some(format_smolstr!("invalid tuple position in index: {pos}")),
                )
            })?;

            if vt_tuple.len() != delete_tuple_len {
                let mut insert_tuple: Vec<EncodedValue> = Vec::with_capacity(builder.len());
                for command in builder {
                    match command {
                        TupleBuilderCommand::TakePosition(tuple_pos) => {
                            let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::Tuple,
                                    Some(format_smolstr!(
                                        "column at position {pos} not found in virtual table"
                                    )),
                                )
                            })?;
                            insert_tuple.push(EncodedValue::Ref(value.into()));
                        }
                        TupleBuilderCommand::TakeAndCastPosition(tuple_pos, table_type) => {
                            let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::Tuple,
                                    Some(format_smolstr!(
                                        "column at position {pos} not found in virtual table"
                                    )),
                                )
                            })?;
                            insert_tuple.push(value.cast(table_type)?);
                        }
                        TupleBuilderCommand::CalculateBucketId(_) => {
                            insert_tuple.push(EncodedValue::Ref(MsgPackValue::Unsigned(bucket_id)));
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::TupleBuilderCommand,
                                Some(format_smolstr!("got command {command:?} for update insert")),
                            ));
                        }
                    }
                }
                // We can have multiple rows with the same primary key,
                // so replace is used.
                if let Err(e) = space.replace(&insert_tuple) {
                    return Err(SbroadError::FailedTo(
                        Action::Insert,
                        Some(Entity::Tuple),
                        format_smolstr!("{e:?}"),
                    ));
                }
                result.row_count += 1;
            }
        }
    }

    Ok(())
}

/// A working horse for `execute_update_on_storage` in case we're dealing with
/// nonsharded update.
fn execute_local_update(
    result: &mut ConsumerResult,
    builder: &TupleBuilderPattern,
    vtable: &VirtualTable,
    space: &Space,
) -> Result<(), SbroadError> {
    let eq_value = Value::String("=".into());
    for vt_tuple in vtable.get_tuples() {
        let mut update_tuple = Vec::with_capacity(builder.len());
        let mut key_tuple = Vec::with_capacity(builder.len());
        for command in builder {
            match command {
                TupleBuilderCommand::UpdateColToPos(table_col, pos) => {
                    let value = vt_tuple.get(*pos).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Tuple,
                            Some(format_smolstr!(
                                "column at position {pos} not found in virtual table"
                            )),
                        )
                    })?;
                    let op = [
                        EncodedValue::Ref(MsgPackValue::from(&eq_value)),
                        EncodedValue::Owned(LuaValue::Unsigned(*table_col as u64)),
                        EncodedValue::Ref(MsgPackValue::from(value)),
                    ];
                    update_tuple.push(op);
                }
                TupleBuilderCommand::TakePosition(pos) => {
                    let value = vt_tuple.get(*pos).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Tuple,
                            Some(format_smolstr!(
                                "column at position {pos} not found in virtual table"
                            )),
                        )
                    })?;
                    key_tuple.push(EncodedValue::Ref(MsgPackValue::from(value)));
                }
                TupleBuilderCommand::TakeAndCastPosition(pos, table_type) => {
                    let value = vt_tuple.get(*pos).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Tuple,
                            Some(format_smolstr!(
                                "column at position {pos} not found in virtual table"
                            )),
                        )
                    })?;
                    key_tuple.push(value.cast(table_type)?);
                }
                TupleBuilderCommand::UpdateColToCastedPos(table_col, pos, table_type) => {
                    let value = vt_tuple.get(*pos).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Tuple,
                            Some(format_smolstr!(
                                "column at position {pos} not found in virtual table"
                            )),
                        )
                    })?;
                    let op = [
                        EncodedValue::Ref(MsgPackValue::from(&eq_value)),
                        EncodedValue::Owned(LuaValue::Unsigned(*table_col as u64)),
                        value.cast(table_type)?,
                    ];
                    update_tuple.push(op);
                }
                _ => {
                    return Err(SbroadError::Invalid(
                        Entity::TupleBuilderCommand,
                        Some(format_smolstr!("got command {command:?} for update")),
                    ));
                }
            }
        }
        let update_res = space.update(&key_tuple, &update_tuple);
        update_res.map_err(|e| {
            SbroadError::FailedTo(Action::Update, Some(Entity::Space), format_smolstr!("{e}"))
        })?;
        result.row_count += 1;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn execute_delete_on_storage<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
) -> Result<ConsumerResult, SbroadError>
where
    R::Cache: StorageCache,
{
    let plan = optional.exec_plan.get_ir_plan();
    let delete_id = plan.get_top()?;
    let delete_child_id = plan.dml_child_id(delete_id)?;
    let builder = init_delete_tuple_builder(plan, delete_id)?;
    let space_name = normalize_name_for_space_api(plan.dml_node_table(delete_id)?.name());
    let mut result = ConsumerResult::default();
    let build_vtable_locally = optional
        .exec_plan
        .get_motion_vtable(delete_child_id)
        .is_err();
    if build_vtable_locally {
        materialize_vtable_locally(runtime, optional, required, delete_child_id)?;
    }
    let vtable = optional.exec_plan.get_motion_vtable(delete_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Space,
            Some(format_smolstr!("space {space_name} not found")),
        )
    })?;
    transaction(|| -> Result<(), SbroadError> {
        for vt_tuple in vtable.get_tuples() {
            let mut delete_tuple = Vec::with_capacity(builder.len());
            for cmd in &builder {
                if let TupleBuilderCommand::TakePosition(pos) = cmd {
                    let value = vt_tuple.get(*pos).ok_or_else(|| {
                        SbroadError::Invalid(
                            Entity::Tuple,
                            Some(format_smolstr!(
                                "column at position {pos} not found in the delete virtual table"
                            )),
                        )
                    })?;
                    delete_tuple.push(EncodedValue::Ref(value.into()));
                } else {
                    return Err(SbroadError::Invalid(
                        Entity::Tuple,
                        Some(format_smolstr!(
                            "unexpected tuple builder cmd for delete primary key: {cmd:?}"
                        )),
                    ));
                }
            }
            if let Err(Error::Tarantool(tnt_err)) = space.delete(&delete_tuple) {
                return Err(SbroadError::FailedTo(
                    Action::Delete,
                    Some(Entity::Tuple),
                    format_smolstr!("{tnt_err:?}"),
                ));
            }
            result.row_count += 1;
        }
        Ok(())
    })?;

    Ok(result)
}

#[allow(clippy::too_many_lines)]
fn execute_insert_on_storage<R: Vshard + QueryCache>(
    runtime: &R,
    optional: &mut OptionalData,
    required: &mut RequiredData,
) -> Result<ConsumerResult, SbroadError>
where
    R::Cache: StorageCache,
{
    // We always generate a virtual table under the `INSERT` node
    // of the execution plan and prefer to execute it via space API
    // instead of SQL (for performance reasons).
    let plan = optional.exec_plan.get_ir_plan();
    let insert_id = plan.get_top()?;
    let insert_child_id = plan.dml_child_id(insert_id)?;
    let space_name = normalize_name_for_space_api(plan.dml_node_table(insert_id)?.name());
    let mut result = ConsumerResult::default();

    // There are two ways to execute an `INSERT` query:
    // 1. Execute SQL subtree under the `INSERT` node (`INSERT .. SELECT ..`)
    //    and then repack and insert results into the space.
    // 2. A virtual table was dispatched under the `INSERT` node.
    //    Simply insert its tuples into the space.
    // The same for `UPDATE`.

    // Check is we need to execute an SQL subtree (case 1).
    let build_vtable_locally = optional
        .exec_plan
        .get_motion_vtable(insert_child_id)
        .is_err();
    if build_vtable_locally {
        materialize_vtable_locally(runtime, optional, required, insert_child_id)?;
    }

    // Check if the virtual table have been dispatched (case 2) or built locally (case 1).
    let vtable = optional.exec_plan.get_motion_vtable(insert_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(
            Entity::Space,
            Some(format_smolstr!("space {space_name} not found")),
        )
    })?;
    let plan = optional.exec_plan.get_ir_plan();
    // let builder = add_casts_to_builder(builder, &tuple_pos_to_type, vtable.as_ref())?;
    let builder = init_insert_tuple_builder(plan, vtable.as_ref(), insert_id)?;
    let conflict_strategy = optional
        .exec_plan
        .get_ir_plan()
        .insert_conflict_strategy(insert_id)?;
    transaction(|| -> Result<(), SbroadError> {
        for (bucket_id, positions) in vtable.get_bucket_index() {
            for pos in positions {
                let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::VirtualTable,
                        Some(format_smolstr!(
                            "tuple at position {pos} not found in virtual table"
                        )),
                    )
                })?;
                let mut insert_tuple = Vec::with_capacity(builder.len());
                for command in &builder {
                    // We don't produce any additional allocations as `MsgPackValue` keeps
                    // a reference to the original value. The only allocation is for message
                    // pack serialization, but it is unavoidable.
                    match command {
                        TupleBuilderCommand::TakePosition(tuple_pos) => {
                            let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::Tuple,
                                    Some(format_smolstr!(
                                        "column at position {pos} not found in virtual table"
                                    )),
                                )
                            })?;
                            insert_tuple.push(EncodedValue::Ref(value.into()));
                        }
                        TupleBuilderCommand::TakeAndCastPosition(tuple_pos, table_type) => {
                            let value = vt_tuple.get(*tuple_pos).ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::Tuple,
                                    Some(format_smolstr!(
                                        "column at position {pos} not found in virtual table"
                                    )),
                                )
                            })?;
                            insert_tuple.push(value.cast(table_type)?);
                        }
                        TupleBuilderCommand::SetValue(value) => {
                            insert_tuple.push(EncodedValue::Ref(MsgPackValue::from(value)));
                        }
                        TupleBuilderCommand::CalculateBucketId(_) => {
                            insert_tuple.push(EncodedValue::Ref(MsgPackValue::Unsigned(bucket_id)));
                        }
                        _ => {
                            return Err(SbroadError::Invalid(
                                Entity::Tuple,
                                Some(format_smolstr!(
                                    "unexpected tuple builder command for insert: {command:?}"
                                )),
                            ));
                        }
                    }
                }
                let insert_result = space.insert(&insert_tuple);
                if let Err(Error::Tarantool(tnt_err)) = &insert_result {
                    if tnt_err.error_code() == TarantoolErrorCode::TupleFound as u32 {
                        match conflict_strategy {
                            ConflictStrategy::DoNothing => {
                                debug!(
                                    Option::from("execute_dml"),
                                    &format!(
                                        "{} {:?}. {}",
                                        "failed to insert tuple:",
                                        insert_tuple,
                                        "Skipping according to conflict strategy"
                                    )
                                );
                            }
                            ConflictStrategy::DoReplace => {
                                debug!(
                                    Option::from("execute_dml"),
                                    &format!(
                                        "{} {:?}. {}",
                                        "failed to insert tuple:",
                                        insert_tuple,
                                        "Trying to replace according to conflict strategy"
                                    )
                                );
                                space.replace(&insert_tuple).map_err(|e| {
                                    SbroadError::FailedTo(
                                        Action::ReplaceOnConflict,
                                        Some(Entity::Space),
                                        format_smolstr!("{e}"),
                                    )
                                })?;
                                result.row_count += 1;
                            }
                            ConflictStrategy::DoFail => {
                                return Err(SbroadError::FailedTo(
                                    Action::Insert,
                                    Some(Entity::Space),
                                    format_smolstr!("{tnt_err}"),
                                ));
                            }
                        }
                        // if either DoReplace or DoNothing was done,
                        // jump to next tuple iteration. Otherwise
                        // the error is not DuplicateKey, and we
                        // should throw it back to user.
                        continue;
                    };
                }
                insert_result.map_err(|e| {
                    SbroadError::FailedTo(
                        Action::Insert,
                        Some(Entity::Space),
                        format_smolstr!("{e}"),
                    )
                })?;
                result.row_count += 1;
            }
        }
        Ok(())
    })?;

    Ok(result)
}

/// Execute DML query locally
///
/// # Errors
/// - Failed to execute DML locally.
#[allow(unused_variables)]
pub fn execute_dml<R: Vshard + QueryCache>(
    runtime: &R,
    required: &mut RequiredData,
    raw_optional: &mut Vec<u8>,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    if required.query_type != QueryType::DML {
        return Err(SbroadError::Invalid(
            Entity::Plan,
            Some("Expected a DML plan.".to_smolstr()),
        ));
    }

    let result = execute_dml_on_storage(runtime, raw_optional, required)?;
    let tuple = Tuple::new(&(result,))
        .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format_smolstr!("{e:?}"))))?;
    Ok(Box::new(tuple) as Box<dyn Any>)
}

/// Execute non-cacheable read statement
///
/// # Errors
/// - failed to compile optional data
/// - Tarantool execution error
pub fn execute_non_cacheable_dql(
    optional: &mut OptionalData,
    vtable_max_rows: u64,
    opts: ExecuteOptions,
) -> Result<Box<dyn Any>, SbroadError> {
    let (pattern_with_params, _tmp_spaces) = compile_optional(optional)?;
    read_unprepared(
        &pattern_with_params.pattern,
        &pattern_with_params.params,
        vtable_max_rows,
        opts,
    )
}

/// Execute non-cacheable read statement
///
/// # Errors
/// - failed to compile optional data
/// - Tarantool execution error
pub fn execute_non_cacheable_dql_with_raw_optional(
    raw_optional: &mut Vec<u8>,
    vtable_max_rows: u64,
    opts: ExecuteOptions,
) -> Result<Box<dyn Any>, SbroadError> {
    let (pattern_with_params, _tmp_spaces) = compile_encoded_optional(raw_optional)?;
    read_unprepared(
        &pattern_with_params.pattern,
        &pattern_with_params.params,
        vtable_max_rows,
        opts,
    )
}

/// The same as `execute_cacheable_dql` but accepts raw optional data.
/// See docs for `execute_cacheable_dql`
///
/// # Errors
/// - Failed to borrow runtime's cache
/// - Tarantool execution error for given statement
/// - Failed to compile the optional data
/// - Failed to prepare the statement
#[allow(unused_variables)]
pub fn execute_cacheable_dql_with_raw_optional<R: Vshard + QueryCache>(
    runtime: &R,
    required: &mut RequiredData,
    raw_optional: &mut Vec<u8>,
) -> Result<Box<dyn Any>, SbroadError>
where
    R::Cache: StorageCache,
{
    let opts = std::mem::take(&mut required.options.execute_options);
    if let Some(res) = exec_if_in_cache(
        runtime,
        &required.parameters,
        &required.plan_id,
        required.options.vtable_max_rows,
        opts.clone(),
    )? {
        return Ok(res);
    }
    let (pattern_with_params, _tmp_spaces) = compile_encoded_optional(raw_optional)?;
    prepare_and_read(
        runtime,
        &pattern_with_params,
        &required.plan_id,
        required.options.vtable_max_rows,
        opts,
        &required.schema_info,
    )
}

/// A common function for all engines to calculate the sharding key value from a `map`
/// of { `column_name` -> value }. Used as a helper function of `extract_sharding_keys_from_map`
/// that is called from `calculate_bucket_id`. `map` must contain a value for each sharding
/// column that is present in `space`.
///
/// # Errors
/// - The space was not found in the metadata.
/// - The sharding key is not present in the space.
pub fn sharding_key_from_map<'rec, S: ::std::hash::BuildHasher>(
    conf: &impl Metadata,
    space: &str,
    map: &'rec HashMap<SmolStr, Value, S>,
) -> Result<Vec<&'rec Value>, SbroadError> {
    let quoted_space = normalize_name_from_schema(space);
    let sharding_key = conf.sharding_key_by_space(&quoted_space)?;
    let quoted_map = map
        .iter()
        .map(|(k, _)| (normalize_name_from_schema(k).to_smolstr(), k.as_str()))
        .collect::<HashMap<SmolStr, &str>>();
    let mut tuple = Vec::with_capacity(sharding_key.len());
    for quoted_column in &sharding_key {
        if let Some(column) = quoted_map.get(quoted_column) {
            let value = map.get(*column).ok_or_else(|| {
                SbroadError::NotFound(
                    Entity::ShardingKey,
                    format_smolstr!("column {column:?} in the map {map:?}"),
                )
            })?;
            tuple.push(value);
        } else {
            return Err(SbroadError::NotFound(
                Entity::ShardingKey,
                format_smolstr!(
                    "(quoted) column {quoted_column:?} in the quoted map {quoted_map:?} (original map: {map:?})"
                )));
        }
    }
    Ok(tuple)
}
