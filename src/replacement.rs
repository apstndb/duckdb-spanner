//! Replacement Scan API integration for ergonomic Spanner table references.
//!
//! Quoted table names with the `spanner:` prefix are rewritten to
//! `spanner_scan(<table>)`. Connection details intentionally come from the
//! existing `spanner_*` config options so plain table names never trigger
//! remote reads by accident.

use std::ffi::{c_void, CStr, CString};
use std::ptr;

use duckdb::ffi;

const SPANNER_TABLE_PREFIX: &str = "spanner:";

/// Register the Spanner replacement scan callback on a DuckDB database.
///
/// # Safety
/// `db` must be a valid `duckdb_database`.
pub unsafe fn register_replacement_scan(db: ffi::duckdb_database) {
    unsafe {
        ffi::duckdb_add_replacement_scan(db, Some(replacement_scan), ptr::null_mut(), None);
    }
}

unsafe extern "C" fn replacement_scan(
    info: ffi::duckdb_replacement_scan_info,
    table_name: *const std::os::raw::c_char,
    _data: *mut c_void,
) {
    let result = std::panic::catch_unwind(|| unsafe { replacement_scan_inner(info, table_name) });

    if result.is_err() {
        unsafe { set_replacement_error(info, "panic in spanner replacement scan") };
    }
}

unsafe fn replacement_scan_inner(
    info: ffi::duckdb_replacement_scan_info,
    table_name: *const std::os::raw::c_char,
) {
    if table_name.is_null() {
        return;
    }

    let name = CStr::from_ptr(table_name).to_string_lossy();
    let Some(spanner_table) = name.strip_prefix(SPANNER_TABLE_PREFIX) else {
        return;
    };
    if spanner_table.is_empty() {
        set_replacement_error(info, "spanner: replacement scan requires a table name");
        return;
    }

    let function_name = c"spanner_scan";
    ffi::duckdb_replacement_scan_set_function_name(info, function_name.as_ptr());

    let value = ffi::duckdb_create_varchar_length(
        spanner_table.as_ptr().cast(),
        spanner_table.len() as u64,
    );
    if value.is_null() {
        set_replacement_error(info, "failed to create replacement scan table parameter");
        return;
    }
    ffi::duckdb_replacement_scan_add_parameter(info, value);
}

unsafe fn set_replacement_error(info: ffi::duckdb_replacement_scan_info, msg: &str) {
    if let Ok(c_msg) = CString::new(msg) {
        ffi::duckdb_replacement_scan_set_error(info, c_msg.as_ptr());
    }
}
