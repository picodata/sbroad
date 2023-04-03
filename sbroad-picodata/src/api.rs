use crate::runtime::router::RouterRuntime;
use crate::runtime::storage::StorageRuntime;
use sbroad::backend::sql::ir::PatternWithParams;
use sbroad::debug;
use sbroad::errors::{Action, SbroadError};
use sbroad::executor::engine::{helpers::decode_msgpack, Router};
use sbroad::executor::protocol::{EncodedRequiredData, RequiredData};
use sbroad::executor::Query;
use sbroad::otm::query_span;
use tarantool::tuple::{Tuple, TupleBuffer};

/// Dispatch parameterized SQL query from coordinator to the segments.
///
/// # Errors
/// - Failed to build a query.
pub fn dispatch_sql(params: PatternWithParams) -> Result<Vec<u8>, SbroadError> {
    let mut params = params;
    let id = params.clone_id();
    let ctx = params.extract_context();
    let tracer = params.get_tracer();

    query_span(
        "\"api.router\"",
        &id,
        &tracer,
        &ctx,
        &params.pattern,
        || {
            let runtime = RouterRuntime::new()?;
            let mut query = Query::new(&runtime, &params.pattern, params.params)?;
            let result = query.dispatch();
            match result {
                Ok(any_tuple) => {
                    if let Some(tuple) = any_tuple.downcast_ref::<Tuple>() {
                        debug!(
                            Option::from("dispatch"),
                            &format!("Dispatch result: {tuple:?}"),
                        );
                        let bytes = Vec::<u8>::from(TupleBuffer::from(tuple));
                        Ok(bytes)
                    } else {
                        Err(SbroadError::FailedTo(
                            Action::Decode,
                            None,
                            format!("tuple {any_tuple:?}"),
                        ))
                    }
                }
                Err(e) => Err(e),
            }
        },
    )
}

/// Execute a query plan.
///
/// # Errors
/// - Failed to decode dispatched data.
pub fn execute_sql(tuple_buf: &[u8]) -> Result<Vec<u8>, SbroadError> {
    let (raw_required, mut raw_optional) = decode_msgpack(tuple_buf)?;

    let mut required = RequiredData::try_from(EncodedRequiredData::from(raw_required))?;

    let id: String = required.id().into();
    let ctx = required.extract_context();
    let tracer = required.tracer();

    query_span("\"api.storage\"", &id, &tracer, &ctx, "", || {
        let runtime = StorageRuntime::new()?;
        let result = runtime.execute_plan(&mut required, &mut raw_optional);
        match result {
            Ok(any_tuple) => {
                if let Some(tuple) = any_tuple.downcast_ref::<Tuple>() {
                    debug!(
                        Option::from("execute"),
                        &format!("Execution result: {tuple:?}"),
                    );
                    let bytes = Vec::<u8>::from(TupleBuffer::from(tuple));
                    Ok(bytes)
                } else {
                    Err(SbroadError::FailedTo(
                        Action::Decode,
                        None,
                        format!("tuple {any_tuple:?}"),
                    ))
                }
            }
            Err(e) => Err(e),
        }
    })
}

/// Determine bucket id for the given values.
///
/// # Errors
/// - Failed to initialize router runtime.
pub fn determine_bucket_id(values: &[&sbroad::ir::value::Value]) -> Result<u64, SbroadError> {
    let runtime = RouterRuntime::new()?;
    let bucket_id = runtime.determine_bucket_id(values);
    Ok(bucket_id)
}
