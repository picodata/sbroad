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
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::sdk::propagation::BaggagePropagator;
use opentelemetry::sdk::trace::Span;

#[allow(unused_imports)]
use opentelemetry::trace::{SpanBuilder, SpanKind, TraceContextExt, Tracer};
#[allow(unused_imports)]
use opentelemetry::{Context, KeyValue};

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

pub mod fiber;

#[cfg(all(feature = "tracing", not(feature = "mock")))]
mod tracing_imports {
    pub use crate::error;
    pub use crate::otm::fiber::fiber_id;
    pub use opentelemetry::baggage::BaggageExt;
}

#[cfg(all(feature = "tracing", not(feature = "mock")))]
use tracing_imports::*;

pub const OTM_CHAR_LIMIT: usize = 512;

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

pub trait QueryTracer {
    type Span: opentelemetry::trace::Span;

    fn build_with_context(&self, builder: SpanBuilder, parent_cx: &Context) -> Self::Span;
}

impl<T> QueryTracer for T
where
    T: Tracer,
{
    type Span = T::Span;

    fn build_with_context(&self, builder: SpanBuilder, parent_cx: &Context) -> Self::Span {
        self.build_with_context(builder, parent_cx)
    }
}

pub type TracerRef = &'static dyn QueryTracer<Span = Span>;

#[allow(dead_code)]
struct TraceInfo {
    tracer: TracerRef,
    context: Context,
    id: String,
}

impl Debug for TraceInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TraceInfo")
            // Context `Debug` trait doesn't print anything useful.
            .field("context", &self.context.span())
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

#[allow(dead_code)]
impl TraceInfo {
    fn new(tracer: TracerRef, context: Context, id: String) -> Self {
        Self {
            tracer,
            context,
            id,
        }
    }

    fn tracer(&self) -> TracerRef {
        self.tracer
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

// We need this function as statistics tracer doesn't implement `ObjectSafeTracer` trait.
#[inline]
#[allow(dead_code)]
fn build_ctx(tracer: TracerRef, sb: SpanBuilder, ctx: &Context) -> Context
where
{
    let span = tracer.build_with_context(sb, ctx);
    ctx.with_span(span)
}

#[inline]
#[allow(unreachable_code)]
#[allow(unused_variables)]
pub fn child_span<T, F>(name: &'static str, f: F) -> T
where
    F: FnOnce() -> T,
{
    #[cfg(all(feature = "tracing", not(feature = "mock")))]
    {
        let fid = fiber_id();

        let Some(id) = current_id() else {
            error!(
                Option::from("child span"),
                &format!("fiber {}, child span {}: missing trace info", fid, name),
            );
            return f();
        };

        let Some(old_ti) = TRACE_MANAGER.with(|tm| tm.borrow_mut().remove(fid)) else {
            error!(
                Option::from("child span"),
                &format!("fiber {}, child span {}: missing trace info", fid, name),
            );
            return f();
        };
        let ctx = build_ctx(
            old_ti.tracer(),
            SpanBuilder::from_name(name)
                .with_kind(SpanKind::Server)
                .with_attributes(vec![KeyValue::new("id", id.clone())]),
            old_ti.context(),
        );
        let ti = TraceInfo::new(old_ti.tracer(), ctx, id);
        TRACE_MANAGER.with(|tm| {
            let mut mut_tm = tm.borrow_mut();
            debug!(
                Option::from("child span"),
                &format!(
                    "fiber {}, child span {}: insert trace info {:?}",
                    fid, name, ti
                ),
            );
            mut_tm.insert(fid, ti);
        });
        let result = f();
        TRACE_MANAGER.with(|tm| {
            let mut mut_tm = tm.borrow_mut();
            debug!(
                Option::from("child span"),
                &format!(
                    "fiber {}, child span {}: restore old trace info {:?}",
                    fid, name, old_ti
                ),
            );
            mut_tm.insert(fid, old_ti);
        });
        return result;
    }
    f()
}

#[inline]
#[allow(unreachable_code)]
#[allow(unused_variables)]
#[allow(clippy::needless_pass_by_value)]
pub fn query_span<T, F>(
    name: &'static str,
    id: &str,
    tracer: TracerRef,
    ctx: &Context,
    sql: &str,
    f: F,
) -> T
where
    F: FnOnce() -> T,
{
    #[cfg(all(feature = "tracing", not(feature = "mock")))]
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
            tracer,
            SpanBuilder::from_name(name)
                .with_kind(SpanKind::Server)
                .with_attributes(attributes),
            ctx,
        );
        let ti = TraceInfo::new(tracer, ctx, id.to_string());

        TRACE_MANAGER.with(|tm| {
            let mut mut_tm = tm.borrow_mut();
            debug!(
                Option::from("query span"),
                &format!(
                    "fiber {}, query span {}: insert trace info {:?}",
                    fid, name, ti
                ),
            );
            mut_tm.insert(fid, ti);
        });
        let result = f();
        TRACE_MANAGER.with(|tm| {
            let mut mut_tm = tm.borrow_mut();
            debug!(
                Option::from("query span"),
                &format!("fiber {}, query span {}: remove trace info", fid, name),
            );
            mut_tm.remove(fid);
        });
        return result;
    }
    f()
}

#[must_use]
#[allow(unreachable_code)]
pub fn current_id() -> Option<String> {
    #[cfg(all(feature = "tracing", not(feature = "mock")))]
    {
        let fid = fiber_id();
        return TRACE_MANAGER.with(|tm| tm.borrow().get(fid).map(|ti| ti.id().to_string()));
    }
    None
}

#[inline]
#[must_use]
pub fn query_id(pattern: &str) -> String {
    Base64::encode_string(blake3::hash(pattern.as_bytes()).to_hex().as_bytes())
}

#[allow(unused_variables)]
pub fn inject_context(carrier: &mut dyn Injector) {
    #[cfg(all(feature = "tracing", not(feature = "mock")))]
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
                    debug!(
                        None,
                        &format!("cx: {:?}, {:?}", ti.context(), ti.context().baggage())
                    );
                    let propagator = BaggagePropagator::new();
                    propagator.inject_context(ti.context(), carrier);
                },
            );
        });
    }
}

#[allow(unreachable_code)]
pub fn extract_context(carrier: &mut dyn Extractor) -> Context {
    let f = |ctx: &Context| -> Context {
        let propagator = BaggagePropagator::new();
        propagator.extract_with_context(ctx, carrier)
    };
    #[cfg(all(feature = "tracing", not(feature = "mock")))]
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
) -> (String, Context) {
    let id = id.unwrap_or_else(|| query_id(pattern).to_string());
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
    (id, ctx)
}
