//! Tarantool cartridge engine module.

pub mod backend;
pub mod config;
pub mod hash;
pub mod router;
pub mod storage;

use crate::errors::QueryPlannerError;
use crate::otm::update_global_tracer;
use opentelemetry::global::{set_text_map_propagator, set_tracer_provider};
use opentelemetry::sdk::propagation::{TextMapCompositePropagator, TraceContextPropagator};

static SERVICE_NAME: &str = "sbroad";

/// Update the opentelemetry global trace provider and tracer.
/// Use `OTEL_EXPORTER_JAEGER_AGENT_HOST` and `OTEL_EXPORTER_JAEGER_AGENT_PORT`
/// environment variables to configure the Jaeger agent's endpoint.
///
/// # Errors
/// - failed to build OTM global trace provider
pub fn update_tracing() -> Result<(), QueryPlannerError> {
    let propagator = TextMapCompositePropagator::new(vec![Box::new(TraceContextPropagator::new())]);
    set_text_map_propagator(propagator);
    let provider = opentelemetry_jaeger::new_pipeline()
        .with_service_name(SERVICE_NAME)
        .build_simple()
        .map_err(|e| {
            QueryPlannerError::CustomError(format!(
                "Failed to build OTM global trace provider: {}",
                e
            ))
        })?;
    set_tracer_provider(provider);
    update_global_tracer();
    Ok(())
}
