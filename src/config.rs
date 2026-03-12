//! Config Options API integration for session-level Spanner defaults.
//!
//! Registers DuckDB config options (e.g. `SET spanner_project = 'myproj'`) and
//! reads them back during bind via the client context.

use std::ffi::CString;
use std::ptr;
use std::sync::atomic::{AtomicBool, Ordering};

use duckdb::ffi;
use duckdb::vtab::BindInfo;

/// Tracks whether config options have been registered via `register_config_options`.
///
/// When the extension is loaded as a library (e.g. integration tests), config options
/// are not registered. Calling `duckdb_client_context_get_config_option` for an
/// unregistered option triggers a DuckDB assertion failure (SIGABRT). This flag
/// prevents that by skipping the lookup entirely when options aren't registered.
static CONFIG_REGISTERED: AtomicBool = AtomicBool::new(false);

/// Register all spanner_* config options on the given connection.
///
/// # Safety
/// `con` must be a valid `duckdb_connection`.
#[cfg(feature = "loadable-extension")]
pub unsafe fn register_config_options(con: ffi::duckdb_connection) {
    register_varchar_option(con, "spanner_project", "Default Google Cloud project ID for Spanner");
    register_varchar_option(con, "spanner_instance", "Default Spanner instance ID");
    register_varchar_option(con, "spanner_database", "Default Spanner database ID");
    register_varchar_option(
        con,
        "spanner_database_path",
        "Default Spanner database resource path (projects/P/instances/I/databases/D)",
    );
    register_varchar_option(con, "spanner_endpoint", "Default Spanner gRPC endpoint");
}

#[cfg(feature = "loadable-extension")]
unsafe fn register_varchar_option(con: ffi::duckdb_connection, name: &str, description: &str) {
    let option = ffi::duckdb_create_config_option();

    let c_name = CString::new(name).unwrap();
    ffi::duckdb_config_option_set_name(option, c_name.as_ptr());

    // Type: VARCHAR
    let varchar_type = ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
    ffi::duckdb_config_option_set_type(option, varchar_type);
    ffi::duckdb_destroy_logical_type(&mut { varchar_type });

    // Default: empty string (treated as unset)
    let empty = CString::new("").unwrap();
    let default_val = ffi::duckdb_create_varchar_length(empty.as_ptr(), 0);
    ffi::duckdb_config_option_set_default_value(option, default_val);
    ffi::duckdb_destroy_value(&mut { default_val });

    // Scope: SESSION
    ffi::duckdb_config_option_set_default_scope(
        option,
        ffi::duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_SESSION,
    );

    let c_desc = CString::new(description).unwrap();
    ffi::duckdb_config_option_set_description(option, c_desc.as_ptr());

    let rc = ffi::duckdb_register_config_option(con, option);
    if rc != ffi::DuckDBSuccess {
        eprintln!("[duckdb-spanner] Failed to register config option: {name}");
    }
    // duckdb_register_config_option takes ownership; do NOT destroy option here.

    CONFIG_REGISTERED.store(true, Ordering::Release);
}

/// Read a spanner config option from a raw client context.
///
/// Returns `None` if the option is unset, empty, or config options were never registered.
/// Does **not** destroy `ctx` — the caller is responsible for cleanup.
///
/// # Safety
/// `ctx` must be a valid, non-null `duckdb_client_context`.
pub unsafe fn get_config_string_from_context(
    ctx: ffi::duckdb_client_context,
    option_name: &str,
) -> Option<String> {
    if !CONFIG_REGISTERED.load(Ordering::Acquire) {
        return None;
    }
    unsafe {
        let c_name = CString::new(option_name).ok()?;
        let mut scope = ffi::duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_INVALID;
        let val = ffi::duckdb_client_context_get_config_option(ctx, c_name.as_ptr(), &mut scope);

        if val.is_null() {
            return None;
        }

        let c_str = ffi::duckdb_get_varchar(val);
        let result = if c_str.is_null() {
            None
        } else {
            let s = std::ffi::CStr::from_ptr(c_str).to_string_lossy().into_owned();
            ffi::duckdb_free(c_str as *mut _);
            if s.is_empty() { None } else { Some(s) }
        };

        ffi::duckdb_destroy_value(&mut { val });
        result
    }
}

/// Read a spanner config option from the client context during bind.
///
/// Returns `None` if the option is unset, empty, or config options were never registered.
pub fn get_config_string(bind: &BindInfo, option_name: &str) -> Option<String> {
    if !CONFIG_REGISTERED.load(Ordering::Acquire) {
        return None;
    }
    unsafe {
        // Extract raw duckdb_bind_info from BindInfo (single-pointer struct).
        const _: () = assert!(
            std::mem::size_of::<BindInfo>() == std::mem::size_of::<ffi::duckdb_bind_info>(),
            "BindInfo size mismatch — duckdb crate layout may have changed"
        );
        let bind_ptr = *(bind as *const BindInfo as *const ffi::duckdb_bind_info);

        // Get client context from bind info
        let mut ctx: ffi::duckdb_client_context = ptr::null_mut();
        ffi::duckdb_table_function_get_client_context(bind_ptr, &mut ctx);
        if ctx.is_null() {
            return None;
        }

        let result = get_config_string_from_context(ctx, option_name);
        ffi::duckdb_destroy_client_context(&mut ctx);
        result
    }
}
