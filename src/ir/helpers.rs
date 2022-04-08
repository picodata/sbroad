//! Helper module with functions and structures for the IR.

use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasher;

/// Helper macros to build a hash map or set
/// from the list of arguments.
#[macro_export]
macro_rules! collection {
    // map-like
    ($($k:expr => $v:expr),* $(,)?) => {{
        use std::iter::{Iterator, IntoIterator};
        Iterator::collect(IntoIterator::into_iter([$(($k, $v),)*]))
    }};
    // set-like
    ($($v:expr),* $(,)?) => {{
        use std::iter::{Iterator, IntoIterator};
        Iterator::collect(IntoIterator::into_iter([$($v,)*]))
    }};
}

/// By default, `HashSet` uses `RandomState` to initialize the hasher.
/// Because of that, we can't get repeatable order in the set between
/// runs that can cause flaky test results in some cases.
#[derive(Clone)]
pub struct RepeatableState;

impl BuildHasher for RepeatableState {
    type Hasher = DefaultHasher;
    fn build_hasher(&self) -> DefaultHasher {
        DefaultHasher::new()
    }
}
