use ahash::AHashMap;

use std::{
    any::Any,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    rc::Rc,
};

use tarantool::{
    space::Space,
    transaction::transaction,
    tuple::{
        rmp::{self, decode::RmpRead},
        Tuple,
    },
};

use crate::{
    backend::sql::{
        ir::{PatternWithParams, TmpSpaceMap},
        tree::{OrderedSyntaxNodes, SyntaxPlan},
    },
    debug,
    errors::{Action, Entity, SbroadError},
    executor::{
        bucket::Buckets,
        engine::helpers::storage::runtime::read_unprepared,
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
        value::{MsgPackValue, Value},
        Node, Plan,
    },
};

use super::{Metadata, Router, Vshard};

pub mod storage;
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

#[must_use]
pub fn normalize_name_for_space_api(s: &str) -> String {
    if let (Some('"'), Some('"')) = (s.chars().next(), s.chars().last()) {
        return s.chars().skip(1).take(s.len() - 2).collect();
    }
    s.to_uppercase()
}

/// A helper function to encode the execution plan into a pair of binary data (see `Message`):
/// * required data (plan id, parameters, etc.)
/// * optional data (execution plan, etc.)
///
/// # Errors
/// - Failed to encode the execution plan.
pub fn encode_plan(exec_plan: ExecutionPlan) -> Result<(Binary, Binary), SbroadError> {
    let query_type = exec_plan.query_type()?;
    let (ordered, sub_plan_id) = match query_type {
        QueryType::DQL => {
            let sub_plan_id = exec_plan.get_ir_plan().pattern_id()?;
            let sp_top_id = exec_plan.get_ir_plan().get_top()?;
            let sp = SyntaxPlan::new(&exec_plan, sp_top_id, Snapshot::Oldest)?;
            let ordered = OrderedSyntaxNodes::try_from(sp)?;
            (ordered, sub_plan_id)
        }
        QueryType::DML => {
            let plan = exec_plan.get_ir_plan();
            let sp_top_id = plan.get_top()?;
            // At the moment we support only `INSERT` statement for DML.
            // TODO: refactor this code when we'll support other DML statements.
            let motion_id = plan.insert_child_id(sp_top_id)?;
            let motion = plan.get_relation_node(motion_id)?;
            let policy = if let Relational::Motion { policy, .. } = motion {
                policy
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format!(
                        "expected motion node under insert node, got: {:?}",
                        motion
                    )),
                ));
            };
            // No need to calculate unique key for the plan, as it's not cached.
            let sub_plan_id = String::new();
            // SQL is needed only for the motion node subtree.
            // HACK: we don't actually need SQL when the subtree is already
            //       materialized into a virtual table on the router.
            let already_materialized = exec_plan.get_motion_vtable(motion_id).is_ok();
            let ordered = if already_materialized {
                OrderedSyntaxNodes::empty()
            } else if let MotionPolicy::LocalSegment { .. } = policy {
                let motion_child_id = exec_plan.get_motion_child(motion_id)?;
                let sp = SyntaxPlan::new(&exec_plan, motion_child_id, Snapshot::Oldest)?;
                OrderedSyntaxNodes::try_from(sp)?
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Plan,
                    Some(format!(
                        "unsupported motion policy under insert node: {:?}",
                        policy
                    )),
                ));
            };
            (ordered, sub_plan_id)
        }
    };
    // We should not use the cache on the storage if the plan contains virtual tables,
    // as they can contain different amount of tuples that are not taken into account
    // when calculating the cache key.
    let can_be_cached = exec_plan.vtables_empty();
    let nodes = ordered.to_syntax_data()?;
    // Virtual tables in the plan must be already filtered, so we can use all buckets here.
    let params = exec_plan.to_params(&nodes, &Buckets::All)?;
    let required_data = RequiredData::new(sub_plan_id, params, query_type, can_be_cached);
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
            format!("array length: {e:?}"),
        )
    })? as usize;
    if array_len != 2 {
        return Err(SbroadError::Invalid(
            Entity::Tuple,
            Some(format!("expected tuple of 2 elements, got {array_len}")),
        ));
    }
    let req_len = rmp::decode::read_str_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format!("read required data length: {e:?}"),
        )
    })? as usize;
    let mut required: Vec<u8> = vec![0_u8; req_len];
    stream.read_exact_buf(&mut required).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format!("read required data: {e:?}"),
        )
    })?;

    let opt_len = rmp::decode::read_str_len(&mut stream).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format!("read optional data string length: {e:?}"),
        )
    })? as usize;
    let mut optional: Vec<u8> = vec![0_u8; opt_len];
    stream.read_exact_buf(&mut optional).map_err(|e| {
        SbroadError::FailedTo(
            Action::Decode,
            Some(Entity::MsgPack),
            format!("read optional data: {e:?}"),
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
    // Use all buckets as we don't want to filter any data from the execution plan
    // (this work has already been done on the coordinator).
    let buckets = Buckets::All;

    // Find a statement in the Tarantool's cache or prepare it
    // (i.e. compile and put into the cache).
    let data = std::mem::take(raw_optional);
    let mut optional = OptionalData::try_from(EncodedOptionalData::from(data))?;
    optional.exec_plan.get_mut_ir_plan().restore_constants()?;
    let nodes = optional.ordered.to_syntax_data()?;
    optional.exec_plan.to_sql(
        &nodes,
        &buckets,
        &uuid::Uuid::new_v4().as_simple().to_string(),
    )
}

#[derive(Debug)]
enum TupleBuilderCommands {
    /// Take a value from the original tuple
    /// at the specified position.
    TakePosition(usize),
    /// Set a specified value.
    SetValue(Value),
    /// Calculate a bucket id for the new tuple
    /// using the specified motion key.
    CalculateBucketId(MotionKey),
}

type TupleBuilderPattern = Vec<TupleBuilderCommands>;

fn insert_tuple_builder(plan: &Plan, insert_id: usize) -> Result<TupleBuilderPattern, SbroadError> {
    let columns = plan.insert_columns(insert_id)?;
    let columns_map: AHashMap<usize, usize> = columns
        .iter()
        .enumerate()
        .map(|(pos, id)| (*id, pos))
        .collect::<AHashMap<_, _>>();
    let table = plan.insert_table(insert_id)?;
    let mut commands = Vec::with_capacity(table.columns.len());
    for (pos, table_col) in table.columns.iter().enumerate() {
        if table_col.role == ColumnRole::Sharding {
            let motion_key = plan.insert_child_motion_key(insert_id)?;
            commands.push(TupleBuilderCommands::CalculateBucketId(motion_key));
        } else if columns_map.contains_key(&pos) {
            // It is safe to unwrap here because we have checked that
            // the column is present in the tuple.
            let tuple_pos = columns_map[&pos];
            commands.push(TupleBuilderCommands::TakePosition(tuple_pos));
        } else {
            // FIXME: support default values other then NULL (issue #442).
            commands.push(TupleBuilderCommands::SetValue(Column::default_value()));
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
                .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format!("{e:?}"))))?;
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
                        Some(format!("expected alias, got {column:?}")),
                    ));
                };
                metadata.push(MetadataColumn::new(column_name, column_type.to_string()));
            }
            let result = ProducerResult {
                metadata,
                ..Default::default()
            };

            let tuple = Tuple::new(&(result,))
                .map_err(|e| SbroadError::Invalid(Entity::Tuple, Some(format!("{e:?}"))))?;
            Ok(Some(Box::new(tuple) as Box<dyn Any>))
        }
    }
}

/// Execute DML on the storage.
///
/// # Errors
/// - Failed to execute DML locally.
///
/// # Panics
/// - Current function never panics (though it contains `unwrap()` calls).
#[allow(clippy::too_many_lines)]
pub fn execute_dml(
    runtime: &impl Vshard,
    raw_optional: &mut Vec<u8>,
) -> Result<ConsumerResult, SbroadError> {
    let data = std::mem::take(raw_optional);
    let mut optional = OptionalData::try_from(EncodedOptionalData::from(data))?;
    optional.exec_plan.get_mut_ir_plan().restore_constants()?;
    let plan = optional.exec_plan.get_ir_plan();

    // At the moment the only supported DML query is `INSERT`.
    // We always generate a virtual table under the `INSERT` node
    // of the execution plan and prefer to execute it via space API
    // instead of SQL (for performance reasons).
    let insert_id = plan.get_top()?;
    let insert_child_id = plan.insert_child_id(insert_id)?;
    let builder = insert_tuple_builder(plan, insert_id)?;
    let space_name = normalize_name_for_space_api(plan.insert_table(insert_id)?.name());
    let mut result = ConsumerResult::default();

    // There are two ways to execute an `INSERT` query:
    // 1. Execute SQL subtree under the `INSERT` node (`INSERT .. SELECT ..`)
    //    and then repack and insert results into the space.
    // 2. A virtual table was dispatched under the `INSERT` node.
    //    Simply insert its tuples into the space.

    // Check is we need to execute an SQL subtree (case 1).
    let build_vtable_locally = optional
        .exec_plan
        .get_motion_vtable(insert_child_id)
        .is_err();
    if build_vtable_locally {
        let subplan_top_id = optional
            .exec_plan
            .get_motion_subtree_root(insert_child_id)?;
        let column_names = plan.get_relational_aliases(subplan_top_id)?;
        optional.exec_plan.get_mut_ir_plan().restore_constants()?;
        let nodes = optional.ordered.to_syntax_data()?;
        let buckets = Buckets::All;
        let (pattern, _) = optional.exec_plan.to_sql(
            &nodes,
            &buckets,
            &uuid::Uuid::new_v4().as_simple().to_string(),
        )?;
        // TODO: first try to use storage cache with read_prepared().
        let result = read_unprepared(&pattern.pattern, &pattern.params)?;
        let tuple = result.downcast::<Tuple>().map_err(|e| {
            SbroadError::FailedTo(
                Action::Deserialize,
                Some(Entity::Tuple),
                format!("motion node {insert_child_id}. {e:?}"),
            )
        })?;
        let mut data = tuple.decode::<Vec<ProducerResult>>().map_err(|e| {
            SbroadError::FailedTo(
                Action::Decode,
                Some(Entity::Tuple),
                format!("motion node {insert_child_id}. {e}"),
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
            .set_motion_vtable(insert_child_id, vtable, runtime)?;
    }

    // Check if the virtual table have been dispatched (case 2) or built locally (case 1).
    let vtable = optional.exec_plan.get_motion_vtable(insert_child_id)?;
    let space = Space::find(&space_name).ok_or_else(|| {
        SbroadError::Invalid(Entity::Space, Some(format!("space {space_name} not found")))
    })?;
    transaction(|| -> Result<(), SbroadError> {
        for (bucket_id, positions) in vtable.get_index().iter() {
            for pos in positions {
                let vt_tuple = vtable.get_tuples().get(*pos).ok_or_else(|| {
                    SbroadError::Invalid(
                        Entity::VirtualTable,
                        Some(format!(
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
                        TupleBuilderCommands::TakePosition(pos) => {
                            let value = vt_tuple.get(*pos).ok_or_else(|| {
                                SbroadError::Invalid(
                                    Entity::Tuple,
                                    Some(format!(
                                        "column at position {pos} not found in virtual table"
                                    )),
                                )
                            })?;
                            insert_tuple.push(MsgPackValue::from(value));
                        }
                        TupleBuilderCommands::SetValue(value) => {
                            insert_tuple.push(MsgPackValue::from(value));
                        }
                        TupleBuilderCommands::CalculateBucketId(_) => {
                            insert_tuple.push(MsgPackValue::Unsigned(bucket_id));
                        }
                    }
                }
                space.insert(&insert_tuple).map_err(|e| {
                    SbroadError::FailedTo(Action::Insert, Some(Entity::Space), format!("{e}"))
                })?;
                result.row_count += 1;
            }
        }
        Ok(())
    })?;

    Ok(result)
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
        Buckets::Single => {
            // Check that all vtables don't have index. Because if they do,
            // they will be filtered later by filter_vtable
            if let Some(vtables) = &sub_plan.vtables {
                for (motion_id, vtable) in vtables.map() {
                    if !vtable.get_index().is_empty() {
                        return Err(SbroadError::Invalid(
                                Entity::Motion,
                                Some(format!("motion ({motion_id}) in subtree with distribution Single, but policy is not Full!"))
                            ));
                    }
                }
            }
            runtime.exec_ir_on_some(sub_plan, &runtime.get_random_bucket())
        }
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
    if let Relational::Values { .. } = plan.get_ir_plan().get_relation_node(child_id)? {
    } else {
        return Ok(None);
    };
    let child_node_ref = plan.get_mut_ir_plan().get_mut_node(child_id)?;
    let child_node = std::mem::replace(child_node_ref, Node::Parameter);
    if let Node::Relational(Relational::Values {
        children, output, ..
    }) = child_node
    {
        // Build a new virtual table (check that all the rows are made of constants only).
        let mut vtable = VirtualTable::new();
        vtable.get_mut_tuples().reserve(children.len());
        for row_id in children {
            let row_node = plan.get_ir_plan().get_relation_node(row_id)?;
            if let Relational::ValuesRow { data, children, .. } = row_node {
                // Check that there are no subqueries in the values node.
                // (If any we'll need to materialize them first with dispatch
                // to the storages.)
                if !children.is_empty() {
                    return Err(SbroadError::Invalid(
                        Entity::Node,
                        Some("VALUES rows supports only constants in its columns.".to_string()),
                    ));
                }
                let data = *data;
                // Check that all the values are constants.
                let columns_len = plan
                    .get_ir_plan()
                    .get_expression_node(data)?
                    .get_row_list()?
                    .len();
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
                                    format!("at position {idx} in the row"),
                                )
                            })?;
                    let column_node_ref = plan.get_mut_ir_plan().get_mut_node(column_id)?;
                    let column_node = std::mem::replace(column_node_ref, Node::Parameter);
                    if let Node::Expression(Expression::Constant { value, .. }) = column_node {
                        row.push(value);
                    } else {
                        return Err(SbroadError::Invalid(
                            Entity::Node,
                            Some(format!(
                                "VALUES rows supports only constants in its columns (got: {column_node:?})."
                            )),
                        ));
                    }
                }
                vtable.add_tuple(row);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!(
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
        for column_id in output_cols {
            let alias = plan.get_ir_plan().get_expression_node(*column_id)?;
            if let Expression::Alias { name, .. } = alias {
                let column = Column {
                    name: name.clone(),
                    r#type: Type::Scalar,
                    role: ColumnRole::User,
                };
                columns.push(column);
            } else {
                return Err(SbroadError::Invalid(
                    Entity::Node,
                    Some(format!("output column ({column_id}) is not an alias node!")),
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
    let alias = plan.get_motion_alias(motion_node_id)?.map(String::from);
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
                format!("motion node {motion_node_id}. {e}"),
            )
        })?;
        data.get_mut(0)
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

/// Function that is called from `exec_ir_on_some_buckets`.
/// Its purpose is to iterate through every vtable presented in `plan` subtree and
/// to replace them by new vtables. New vtables indices (map bucket id -> tuples) will contain
/// only pairs corresponding to buckets, that are presented in given `bucket_ids` (as we are going
/// to execute `plan` subtree only on them).
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
