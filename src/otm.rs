//! Opentelemetry module

use ahash::AHashMap;
use opentelemetry::global::{get_text_map_propagator, tracer, BoxedTracer};
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::trace::noop::NoopTracer;

#[allow(unused_imports)]
use opentelemetry::trace::{SpanBuilder, SpanKind, TraceContextExt, Tracer};
#[allow(unused_imports)]
use opentelemetry::{Context, KeyValue};

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

mod fiber;

#[cfg(not(feature = "mock"))]
use fiber::fiber_id;

static TRACER_NAME: &str = "libsbroad";
thread_local!(static TRACE_MANAGER: RefCell<TraceManager> = RefCell::new(TraceManager::new()));
thread_local!(static GLOBAL_TRACER: RefCell<BoxedTracer> = RefCell::new(tracer(TRACER_NAME)));
lazy_static! {
    static ref NOOP_TRACER: NoopTracer = NoopTracer::new();
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum QueryTracer {
    Global,
    Noop,
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
        Self::new(QueryTracer::Noop, Context::new(), "".to_string())
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
// We need this function as NoopTracer doesn't implement `ObjectSafeTracer` trait.
fn build_ctx(tracer: &QueryTracer, sb: SpanBuilder, ctx: &Context) -> Context {
    match tracer {
        QueryTracer::Noop => {
            let span = NOOP_TRACER.build_with_context(sb, ctx);
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
        let old_ti = TRACE_MANAGER.with(|tm| {
            tm.borrow_mut()
                .remove(fid)
                .map_or(TraceInfo::empty(), |ti| ti)
        });
        let id = current_id();
        let ctx = build_ctx(
            old_ti.tracer(),
            SpanBuilder::from_name(name)
                .with_kind(SpanKind::Server)
                .with_attributes(vec![KeyValue::new("id", id.clone())]),
            old_ti.context(),
        );
        let ti = TraceInfo::new(old_ti.tracer().clone(), ctx, id);
        debug(&format!(
            "fiber {}, child span {}: insert new trace info {:?}",
            fid, name, ti
        ));
        TRACE_MANAGER.with(|tm| tm.borrow_mut().insert(fid, ti));
        let result = f();
        debug(&format!(
            "fiber {}, child span {}: restore old trace info {:?}",
            fid, name, old_ti
        ));
        TRACE_MANAGER.with(|tm| tm.borrow_mut().insert(fid, old_ti));
        return result;
    }
    f()
}

#[inline]
#[allow(unreachable_code)]
#[allow(unused_variables)]
pub fn query_span<T, F>(
    name: &'static str,
    id: &str,
    tracer: QueryTracer,
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
                // UTF-8 character can be up to 4 bytes long, `query_id` is 8 bytes long.
                .filter_map(|(i, c)| if i <= 4084 { Some(c) } else { None })
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
        let ti = TraceInfo::new(tracer, ctx, id.to_string());

        debug(&format!(
            "fiber {}, query span {}: insert trace info {:?}",
            fid, name, ti
        ));
        TRACE_MANAGER.with(|tm| tm.borrow_mut().insert(fid, ti));
        let result = f();
        debug(&format!(
            "fiber {}, query span {}: remove trace info",
            fid, name
        ));
        TRACE_MANAGER.with(|tm| tm.borrow_mut().remove(fid));
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
pub fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

pub fn update_global_tracer() {
    GLOBAL_TRACER.with(|gt| *gt.borrow_mut() = tracer(TRACER_NAME));
}

#[allow(unused_variables)]
pub fn inject_context(carrier: &mut dyn Injector) {
    #[cfg(not(feature = "mock"))]
    {
        let fid = fiber_id();
        TRACE_MANAGER.with(|tm| {
            tm.borrow().get(fid).map_or_else(
                || {
                    debug(&format!("fiber {}, no trace information found", fid));
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

#[must_use]
pub fn extract_params<S: ::std::hash::BuildHasher>(
    context: Option<HashMap<String, String, S>>,
    id: Option<String>,
) -> (String, Context, QueryTracer) {
    let tracer = if let (None, None) = (&id, &context) {
        QueryTracer::Noop
    } else {
        QueryTracer::Global
    };
    let id = if let Some(id) = id { id } else { new_id() };
    let ctx = if let Some(mut carrier) = context {
        debug(&format!("Serialized OTM span context: {:?}", carrier));
        let ctx = extract_context(&mut carrier);
        debug(&format!("Deserialized OTM span context: {:?}", ctx.span()));
        ctx
    } else {
        Context::new()
    };
    (id, ctx, tracer)
}

#[inline]
#[allow(unused_variables)]
fn debug(message: &str) {
    #[cfg(not(feature = "mock"))]
    {
        use tarantool::log::{say, SayLevel};

        say(
            SayLevel::Debug,
            file!(),
            line!().try_into().unwrap_or(0),
            None,
            message,
        );
    }
}
