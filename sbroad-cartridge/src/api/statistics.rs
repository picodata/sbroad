use sbroad::debug;
use sbroad::otm::statistics::table::{QUERY, SPAN, STAT};
use std::os::raw::c_int;

/// Initialize query statistics tables.
///
/// Though the function always returns a success, it can fail to create
/// the table (for example on read-only replica). In this case, the
/// warning will be logged, but the function will return success.
#[allow(clippy::module_name_repetitions)]
#[no_mangle]
pub extern "C" fn init_statistics() -> c_int {
    debug!(
        Option::from("init_statistics"),
        "Initializing statistics tables"
    );
    QUERY.with(|_| {});
    SPAN.with(|_| {});
    STAT.with(|_| {});
    0
}
