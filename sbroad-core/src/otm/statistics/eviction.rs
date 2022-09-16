//! Statistics eviction module.
//!
//! This module is used to evict statistics from the `__sbroad_stat`
//! and `__sbroad_query` spaces. The eviction is performed by LRU
//! strategy at the threshold of 1000 entries in the `__sbroad_query`
//! space.

use crate::debug;
use crate::errors::QueryPlannerError;
use crate::executor::lru::{Cache, LRUCache};
use crate::otm::statistics::table::{TarantoolSpace, QUERY};
use std::cell::RefCell;

pub const STATISTICS_CAPACITY: usize = 1000;
thread_local!(pub(super) static TRACKED_QUERIES: RefCell<TrackedQueries> = RefCell::new(TrackedQueries::new()));

pub struct TrackedQueries {
    queries: LRUCache<String, String>,
}

#[allow(clippy::unnecessary_wraps)]
fn remove_query(query_id: &mut String) -> Result<(), QueryPlannerError> {
    QUERY.with(|query_space| {
        let mut query_space = query_space.borrow_mut();
        debug!(
            Option::from("tracked queries"),
            &format!("remove query: {}", query_id)
        );
        let key = std::mem::take(query_id);
        query_space.delete(&(key,));
    });
    Ok(())
}

impl Default for TrackedQueries {
    fn default() -> Self {
        Self::new()
    }
}

impl TrackedQueries {
    /// Create a new instance of `TrackedQueries`.
    ///
    /// # Panics
    /// - If the `STATISTICS_CAPACITY` is less than 1 (impossible at the moment).
    #[must_use]
    pub fn new() -> Self {
        Self {
            queries: LRUCache::new(STATISTICS_CAPACITY, Some(Box::new(remove_query))).unwrap(),
        }
    }

    /// Add a new query to the tracked queries.
    ///
    /// # Errors
    /// - Internal error in the eviction function.
    pub fn push(&mut self, key: String) -> Result<(), QueryPlannerError> {
        self.queries.put(key.clone(), key)
    }
}
