#[cfg(all(feature = "tracing", not(feature = "mock")))]
#[inline]
pub fn fiber_id() -> u64 {
    tarantool::fiber::id()
}
