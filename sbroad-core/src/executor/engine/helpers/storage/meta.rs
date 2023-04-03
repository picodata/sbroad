use crate::executor::lru::DEFAULT_CAPACITY;

pub const DEFAULT_JAEGER_AGENT_HOST: &str = "localhost";
pub const DEFAULT_JAEGER_AGENT_PORT: u16 = 6831;

/// Storage runtime configuration.
#[allow(clippy::module_name_repetitions)]
pub struct StorageMetadata {
    /// Prepared statements cache capacity (on the storage).
    pub storage_capacity: usize,
    /// Prepared statements cache size in bytes (on the storage).
    /// If a new statement is bigger doesn't fit into the cache,
    /// it would not be cached but executed directly.
    pub storage_size_bytes: usize,
    /// Jaeger agent host
    pub jaeger_agent_host: &'static str,
    /// Jaeger agent port
    pub jaeger_agent_port: u16,
}

impl Default for StorageMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageMetadata {
    #[must_use]
    pub fn new() -> Self {
        StorageMetadata {
            storage_capacity: DEFAULT_CAPACITY,
            storage_size_bytes: 1024 * DEFAULT_CAPACITY,
            jaeger_agent_host: DEFAULT_JAEGER_AGENT_HOST,
            jaeger_agent_port: DEFAULT_JAEGER_AGENT_PORT,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.storage_capacity == 0 && self.storage_size_bytes == 0
    }
}
