//! Opentelemetry module
//!
//! This module contains the opentelemetry instrumentation for the sbroad library.
//! There are two main use case for it:
//! - tracing of some exact query (global tracer exports spans to the jaeger)
//! - query execution statistics sampled from 1% of all queries (statistics tracer
//!   writing to the temporary spaces)

use crate::debug;

use ahash::AHashMap;
use base64ct::{Base64, Encoding};
use opentelemetry::global::{get_text_map_propagator, tracer, BoxedTracer};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::sdk::trace::{
    Config as SdkConfig, Sampler as SdkSampler, Tracer as SdkTracer,
    TracerProvider as SdkTracerProvider,
};
use opentelemetry::trace::TracerProvider;

#[allow(unused_imports)]
use opentelemetry::trace::{SpanBuilder, SpanKind, TraceContextExt, Tracer};
#[allow(unused_imports)]
use opentelemetry::{Context, KeyValue};

use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use tarantool::tlua::{self, Push};

pub mod fiber;
pub mod statistics;

#[cfg(not(feature = "mock"))]
mod prod_imports {
    pub use crate::otm::fiber::fiber_id;
    pub use crate::warn;
    pub use tarantool::error::Error as TntError;
}

use crate::errors::{Entity, SbroadError};
#[cfg(not(feature = "mock"))]
use prod_imports::*;

pub const OTM_CHAR_LIMIT: usize = 512;

static TRACER_NAME: &str = "libsbroad";
static RATIO: f64 = 0.01;
thread_local!(
    /// Thread-local storage for the trace information of the current fiber.
    ///
    /// Pay attention, that all mutable accesses to this variable should be
    /// wrapped with Tarantool transaction. The reason is that statistics
    /// tracer can create temporary tables on the instance. As a result,
    /// Tarantool yields the fiber while the RefCell is still mutably borrowed.
    /// An access to TRACE_MANAGER on a new fiber leads to panic. So we have to
    /// be sure that transaction commit is called after the RefCell is released.
    static TRACE_MANAGER: RefCell<TraceManager> = RefCell::new(TraceManager::new())
);
thread_local!(static GLOBAL_TRACER: RefCell<BoxedTracer> = RefCell::new(tracer(TRACER_NAME)));
lazy_static! {
    static ref STATISTICS_PROVIDER: SdkTracerProvider = SdkTracerProvider::builder()
        .with_span_processor(statistics::StatCollector::new())
        .with_config(SdkConfig::default().with_sampler(SdkSampler::TraceIdRatioBased(RATIO)))
        .build();
    #[derive(Debug)]
    static ref STATISTICS_TRACER: SdkTracer = STATISTICS_PROVIDER.versioned_tracer("stat", None, None);
    /// Like statistic tracer but always create traces. Used only for testing purposes.
    static ref TEST_STATISTICS_PROVIDER: SdkTracerProvider = SdkTracerProvider::builder()
        .with_span_processor(statistics::StatCollector::new())
        .build();
    static ref TEST_STATISTICS_TRACER: SdkTracer = TEST_STATISTICS_PROVIDER.versioned_tracer("test_stat", None, None);
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, Push)]
#[allow(dead_code)]
#[derive(Default)]
pub enum QueryTracer {
    /// Sends all metrics to Jaeger agent
    Global,
    /// Gathers stats about spans and saves them to temporary spaces
    /// on each node, but does it only for 1% of the queries.
    #[default]
    Statistics,
    /// Like STAT_TRACER but saves stats for each query.
    /// It is used only for tests.
    TestStatistics,
}

impl Display for QueryTracer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            QueryTracer::Global => "global",
            QueryTracer::Statistics => "stat",
            QueryTracer::TestStatistics => "test_stat",
        };
        write!(f, "{s}")
    }
}

impl TryFrom<String> for QueryTracer {
    type Error = SbroadError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let normalized = value.to_lowercase();
        let res = match normalized.as_str() {
            "global" => QueryTracer::Global,
            "stat" => QueryTracer::Statistics,
            "test_stat" => QueryTracer::TestStatistics,
            _ => {
                return Err(SbroadError::Invalid(
                    Entity::PatternWithParams,
                    Some(format!("unknown tracer: {value}")),
                ))
            }
        };
        Ok(res)
    }
}

#[allow(dead_code)]
struct TraceInfo {
    tracer: QueryTracer,
    context: Context,
    id: String,
}

impl Debug for TraceInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceInfo")
            .field("tracer", &self.tracer)
            // Context `Debug` trait doesn't print anything useful.
            .field("context", &self.context.span())
            .field("id", &self.id)
            .finish()
    }
}

#[allow(dead_code)]
impl TraceInfo {
    fn new(tracer: QueryTracer, context: Context, id: String) -> Self {
        Self {
            tracer,
            context,
            id,
        }
    }

    fn empty() -> Self {
        // TODO: handle disable statistics.
        Self::new(QueryTracer::Statistics, Context::new(), String::new())
    }

    fn tracer(&self) -> &QueryTracer {
        &self.tracer
    }

    fn context(&self) -> &Context {
        &self.context
    }

    fn id(&self) -> &str {
        &self.id
    }
}

/// Context manager keeps OTM tracing information per fiber.
/// It is required to deal with the fiber switches in Tarantool.
#[allow(dead_code)]
struct TraceManager(AHashMap<u64, TraceInfo>);

#[allow(dead_code)]
impl TraceManager {
    fn new() -> Self {
        Self(AHashMap::new())
    }

    fn get(&self, key: u64) -> Option<&TraceInfo> {
        self.0.get(&key)
    }

    fn insert(&mut self, key: u64, value: TraceInfo) {
        self.0.insert(key, value);
    }

    fn remove(&mut self, key: u64) -> Option<TraceInfo> {
        self.0.remove(&key)
    }
}

#[inline]
#[allow(dead_code)]
// We need this function as statistics tracer doesn't implement `ObjectSafeTracer` trait.
fn build_ctx(tracer: &QueryTracer, sb: SpanBuilder, ctx: &Context) -> Context {
    match tracer {
        QueryTracer::Statistics => {
            let span = STATISTICS_TRACER.build_with_context(sb, ctx);
            ctx.with_span(span)
        }
        QueryTracer::TestStatistics => {
            let span = TEST_STATISTICS_TRACER.build_with_context(sb, ctx);
            ctx.with_span(span)
        }
        QueryTracer::Global => {
            let span = GLOBAL_TRACER.with(|tracer| tracer.borrow().build_with_context(sb, ctx));
            ctx.with_span(span)
        }
    }
}

#[inline]
#[allow(unreachable_code)]
#[allow(unused_variables)]
pub fn child_span<T, F>(name: &'static str, f: F) -> T
where
    F: FnOnce() -> T,
{
    #[cfg(not(feature = "mock"))]
    {
        let fid = fiber_id();
        let id = current_id();
        let old_ti = TRACE_MANAGER.with(|tm| {
            tm.borrow_mut()
                .remove(fid)
                .map_or(TraceInfo::empty(), |ti| ti)
        });
        let ctx = build_ctx(
            old_ti.tracer(),
            SpanBuilder::from_name(name)
                .with_kind(SpanKind::Server)
                .with_attributes(vec![KeyValue::new("id", id.clone())]),
            old_ti.context(),
        );
        let ti = TraceInfo::new(old_ti.tracer().clone(), ctx, id);
        TRACE_MANAGER.with(|tm| match tm.try_borrow_mut() {
            Ok(mut mut_tm) => {
                debug!(
                    Option::from("child span"),
                    &format!(
                        "fiber {}, child span {}: insert trace info {:?}",
                        fid, name, ti
                    ),
                );
                mut_tm.insert(fid, ti);
            }
            Err(_e) => {
                warn!(
                    Option::from("query span"),
                    &format!(
                        "fiber {}, child span {}: failed to insert trace info {:?}, error: {:?}",
                        fid, name, ti, _e
                    ),
                );
            }
        });
        let result = f();
        TRACE_MANAGER.with(|tm| match tm.try_borrow_mut() {
            Ok(mut mut_tm) => {
                debug!(
                    Option::from("child span"),
                    &format!("fiber {}, child span {}: restore old trace info {:?}", fid, name, old_ti),
                );
                mut_tm.insert(fid, old_ti);
            }
            Err(_e) => {
                warn!(
                    Option::from("query span"),
                    &format!(
                        "fiber {}, child span {}: failed to restore old trace info {:?}, error: {:?}",
                        fid, name, old_ti, _e
                    ),
                );
            }
        });
        return result;
    }
    f()
}

#[inline]
#[allow(unreachable_code)]
#[allow(unused_variables)]
pub fn stat_query_span<T, F>(name: &'static str, sql: &str, id: &str, traceable: bool, f: F) -> T
where
    F: FnOnce() -> T,
{
    #[cfg(not(feature = "mock"))]
    {
        let tracer = if traceable {
            QueryTracer::TestStatistics
        } else {
            QueryTracer::Statistics
        };
        let ctx = Context::new();
        return query_span(name, &id, &tracer, &ctx, sql, f);
    }
    f()
}

#[inline]
#[allow(unreachable_code)]
#[allow(unused_variables)]
pub fn query_span<T, F>(
    name: &'static str,
    id: &str,
    tracer: &QueryTracer,
    ctx: &Context,
    sql: &str,
    f: F,
) -> T
where
    F: FnOnce() -> T,
{
    #[cfg(not(feature = "mock"))]
    {
        let mut attributes: Vec<KeyValue> = Vec::new();
        attributes.push(KeyValue::new("id", id.to_string()));
        attributes.push(KeyValue::new(
            "query_sql",
            sql.char_indices()
                // Maximum number of bytes per a single name-value pair: 4096.
                .filter_map(|(i, c)| if i <= OTM_CHAR_LIMIT { Some(c) } else { None })
                .collect::<String>(),
        ));

        let fid = fiber_id();
        let ctx = build_ctx(
            &tracer,
            SpanBuilder::from_name(name)
                .with_kind(SpanKind::Server)
                .with_attributes(attributes),
            ctx,
        );
        let ti = TraceInfo::new(tracer.clone(), ctx, id.to_string());

        TRACE_MANAGER.with(|tm| match tm.try_borrow_mut() {
            Ok(mut mut_tm) => {
                debug!(
                    Option::from("query span"),
                    &format!(
                        "fiber {}, query span {}: insert trace info {:?}",
                        fid, name, ti
                    ),
                );
                mut_tm.insert(fid, ti);
            }
            Err(_e) => {
                warn!(
                    Option::from("query span"),
                    &format!(
                        "fiber {}, query span {}: failed to insert trace info {:?}, error: {:?}",
                        fid, name, ti, _e
                    ),
                );
            }
        });
        let result = f();
        TRACE_MANAGER.with(|tm| match tm.try_borrow_mut() {
            Ok(mut mut_tm) => {
                debug!(
                    Option::from("query span"),
                    &format!("fiber {}, query span {}: remove trace info", fid, name),
                );
                mut_tm.remove(fid);
            }
            Err(_e) => {
                warn!(
                    Option::from("query span"),
                    &format!(
                        "fiber {}, query span {}: failed to remove trace info, error: {:?}",
                        fid, name, _e
                    ),
                );
            }
        });
        return result;
    }
    f()
}

#[must_use]
#[allow(unreachable_code)]
pub fn current_id() -> String {
    #[cfg(not(feature = "mock"))]
    {
        let fid = fiber_id();
        return TRACE_MANAGER.with(|tm| {
            tm.borrow()
                .get(fid)
                .map_or_else(new_id, |ti| ti.id().to_string())
        });
    }
    new_id()
}

#[inline]
#[must_use]
fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[inline]
#[must_use]
pub fn query_id(pattern: &str) -> String {
    Base64::encode_string(blake3::hash(pattern.as_bytes()).to_hex().as_bytes())
}

pub fn update_global_tracer() {
    GLOBAL_TRACER.with(|gt| *gt.borrow_mut() = tracer(TRACER_NAME));
}

#[must_use]
#[allow(unreachable_code)]
pub fn current_tracer() -> QueryTracer {
    #[cfg(not(feature = "mock"))]
    {
        let fid = fiber_id();
        return TRACE_MANAGER.with(|tm| {
            tm.borrow()
                .get(fid)
                .map_or(QueryTracer::default(), |ti| ti.tracer().clone())
        });
    }
    QueryTracer::default()
}

#[allow(unused_variables)]
pub fn inject_context(carrier: &mut dyn Injector) {
    #[cfg(not(feature = "mock"))]
    {
        let fid = fiber_id();
        TRACE_MANAGER.with(|tm| {
            tm.borrow().get(fid).map_or_else(
                || {
                    debug!(
                        Option::from("context injection"),
                        &format!("fiber {}, no trace information found", fid),
                    );
                },
                |ti| {
                    get_text_map_propagator(|propagator| {
                        propagator.inject_context(ti.context(), carrier);
                    });
                },
            );
        });
    }
}

#[allow(unreachable_code)]
pub fn extract_context(carrier: &mut dyn Extractor) -> Context {
    let f = |ctx: &Context| -> Context {
        get_text_map_propagator(|propagator| propagator.extract_with_context(ctx, carrier))
    };
    #[cfg(not(feature = "mock"))]
    {
        let fid = fiber_id();
        return TRACE_MANAGER.with(|tm| {
            tm.borrow()
                .get(fid)
                .map_or_else(|| f(&Context::new()), |ti| f(ti.context()))
        });
    }
    f(&Context::new())
}

pub fn deserialize_context<S: ::std::hash::BuildHasher>(
    context: Option<HashMap<String, String, S>>,
) -> Context {
    if let Some(mut carrier) = context {
        debug!(
            Option::from("dispatched IR"),
            &format!("Serialized OTM span context: {:?}", carrier),
        );
        let ctx = extract_context(&mut carrier);
        debug!(
            Option::from("dispatched IR"),
            &format!("Deserialized OTM span context: {:?}", ctx.span()),
        );
        ctx
    } else {
        Context::new()
    }
}

#[must_use]
pub fn extract_params<S: ::std::hash::BuildHasher>(
    context: Option<HashMap<String, String, S>>,
    id: Option<String>,
    pattern: &str,
    force_trace: bool,
) -> (String, Context, QueryTracer) {
    let tracer = match (force_trace, &id, &context) {
        (_, None, None) | (false, _, _) => QueryTracer::Statistics,
        _ => QueryTracer::Global,
    };
    let id = id.unwrap_or_else(|| query_id(pattern));
    let ctx = if let Some(mut carrier) = context {
        debug!(
            Option::from("parameters extraction"),
            &format!("Serialized OTM span context: {:?}", carrier),
        );
        let ctx = extract_context(&mut carrier);
        debug!(
            Option::from("parameters extraction"),
            &format!("Deserialized OTM span context: {:?}", ctx.span()),
        );
        ctx
    } else {
        Context::new()
    };
    (id, ctx, tracer)
}
