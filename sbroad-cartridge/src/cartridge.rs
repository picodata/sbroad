//! Tarantool cartridge engine module.

pub mod config;
pub mod router;
pub mod storage;

use opentelemetry::global::{set_text_map_propagator, set_tracer_provider};
use opentelemetry::sdk::propagation::{TextMapCompositePropagator, TraceContextPropagator};
use sbroad::errors::QueryPlannerError;
use sbroad::otm::update_global_tracer;

static SERVICE_NAME: &str = "sbroad";

/// Update the opentelemetry global trace provider and tracer.
///
/// # Errors
/// - failed to build OTM global trace provider
pub fn update_tracing(host: &str, port: u16) -> Result<(), QueryPlannerError> {
    let propagator = TextMapCompositePropagator::new(vec![Box::new(TraceContextPropagator::new())]);
    set_text_map_propagator(propagator);
    let provider = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(&format!("{}:{}", host, port))
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
