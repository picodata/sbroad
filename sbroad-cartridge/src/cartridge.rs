//! Tarantool cartridge engine module.

pub mod config;
pub mod router;
pub mod storage;

use std::cell::Ref;

use opentelemetry::global::{set_text_map_propagator, set_tracer_provider};
use opentelemetry::sdk::propagation::{TextMapCompositePropagator, TraceContextPropagator};
use sbroad::errors::{Action, SbroadError};
use sbroad::otm::update_global_tracer;

static SERVICE_NAME: &str = "sbroad";

/// Update the opentelemetry global trace provider and tracer.
///
/// # Errors
/// - failed to build OTM global trace provider
pub fn update_tracing(host: &str, port: u16) -> Result<(), SbroadError> {
    let propagator = TextMapCompositePropagator::new(vec![Box::new(TraceContextPropagator::new())]);
    set_text_map_propagator(propagator);
    let provider = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(format!("{host}:{port}"))
        .with_service_name(SERVICE_NAME)
        .build_simple()
        .map_err(|e| {
            SbroadError::FailedTo(
                Action::Build,
                None,
                format!("OTM global trace provider: {e}"),
            )
        })?;
    set_tracer_provider(provider);
    update_global_tracer();
    Ok(())
}

/// Cartridge cluster configuration provider.
pub trait ConfigurationProvider: Sized {
    type Configuration;

    /// Return a cached cluster configuration from the Rust memory.
    ///
    /// # Errors
    /// - Failed to get a configuration from the router runtime.
    fn cached_config(&self) -> Result<Ref<Self::Configuration>, SbroadError>;

    /// Clear the cached cluster configuration in the Rust memory.
    ///
    /// # Errors
    /// - Failed to clear the cached configuration.
    fn clear_config(&self) -> Result<(), SbroadError>;

    /// Check if the cached cluster configuration is empty.
    ///
    /// # Errors
    /// - Failed to get the cached configuration.
    fn is_config_empty(&self) -> Result<bool, SbroadError>;

    /// Retrieve cluster configuration from the Lua memory.
    ///
    /// If the configuration is already cached, return None,
    /// otherwise return Some(config).
    ///
    /// # Errors
    /// - Internal error.
    fn retrieve_config(&self) -> Result<Option<Self::Configuration>, SbroadError>;

    /// Update cached cluster configuration.
    ///
    /// # Errors
    /// - Failed to update the configuration.
    fn update_config(&self, metadata: Self::Configuration) -> Result<(), SbroadError>;
}
