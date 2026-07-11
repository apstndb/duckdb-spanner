//! Config Options API integration for session-level Spanner defaults.
//!
//! Registers DuckDB config options (e.g. `SET spanner_project = 'myproj'`) and
//! reads them back during bind via the client context.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use duckdb::ffi;
use duckdb::vtab::BindInfo;

macro_rules! owned_duckdb_handle {
    ($name:ident, $raw:ty, $destroy:path) => {
        struct $name($raw);

        impl $name {
            unsafe fn from_raw(raw: $raw) -> Self {
                Self(raw)
            }

            fn as_raw(&self) -> $raw {
                self.0
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    unsafe { $destroy(&mut self.0) };
                }
            }
        }
    };
}

owned_duckdb_handle!(
    OwnedDuckDbConfigOption,
    ffi::duckdb_config_option,
    ffi::duckdb_destroy_config_option
);
owned_duckdb_handle!(
    OwnedDuckDbLogicalType,
    ffi::duckdb_logical_type,
    ffi::duckdb_destroy_logical_type
);
owned_duckdb_handle!(
    OwnedDuckDbValue,
    ffi::duckdb_value,
    ffi::duckdb_destroy_value
);
owned_duckdb_handle!(
    OwnedDuckDbClientContext,
    ffi::duckdb_client_context,
    ffi::duckdb_destroy_client_context
);

struct OwnedDuckDbString(*mut c_char);

impl OwnedDuckDbString {
    unsafe fn from_raw(raw: *mut c_char) -> Self {
        Self(raw)
    }

    fn as_ptr(&self) -> *mut c_char {
        self.0
    }
}

impl Drop for OwnedDuckDbString {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { ffi::duckdb_free(self.0.cast()) };
        }
    }
}

/// Register all spanner_* config options on the given connection.
///
/// # Safety
/// `con` must be a valid `duckdb_connection`.
pub unsafe fn register_config_options(con: ffi::duckdb_connection) {
    register_varchar_option(
        con,
        "spanner_project",
        "Default Google Cloud project ID for Spanner",
    );
    register_varchar_option(con, "spanner_instance", "Default Spanner instance ID");
    register_varchar_option(con, "spanner_database", "Default Spanner database ID");
    register_varchar_option(
        con,
        "spanner_database_path",
        "Default Spanner database resource path (projects/P/instances/I/databases/D)",
    );
    register_varchar_option(con, "spanner_endpoint", "Default Spanner gRPC endpoint");
    register_varchar_option(
        con,
        "spanner_admin_endpoint",
        "Default Spanner admin REST endpoint",
    );
}

unsafe fn register_varchar_option(con: ffi::duckdb_connection, name: &str, description: &str) {
    let option = OwnedDuckDbConfigOption::from_raw(ffi::duckdb_create_config_option());
    if option.as_raw().is_null() {
        eprintln!("[duckdb-spanner] Failed to allocate config option: {name}");
        return;
    }

    let c_name = CString::new(name).unwrap();
    ffi::duckdb_config_option_set_name(option.as_raw(), c_name.as_ptr());

    // Type: VARCHAR
    {
        let varchar_type = OwnedDuckDbLogicalType::from_raw(ffi::duckdb_create_logical_type(
            ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
        ));
        if varchar_type.as_raw().is_null() {
            eprintln!("[duckdb-spanner] Failed to allocate config option type: {name}");
            return;
        }
        ffi::duckdb_config_option_set_type(option.as_raw(), varchar_type.as_raw());
    }

    // Default: empty string (treated as unset)
    {
        let default_val =
            OwnedDuckDbValue::from_raw(ffi::duckdb_create_varchar_length(c"".as_ptr(), 0));
        if default_val.as_raw().is_null() {
            eprintln!("[duckdb-spanner] Failed to allocate config option default: {name}");
            return;
        }
        ffi::duckdb_config_option_set_default_value(option.as_raw(), default_val.as_raw());
    }

    // Scope: SESSION
    ffi::duckdb_config_option_set_default_scope(
        option.as_raw(),
        ffi::duckdb_config_option_scope_DUCKDB_CONFIG_OPTION_SCOPE_SESSION,
    );

    let c_desc = CString::new(description).unwrap();
    ffi::duckdb_config_option_set_description(option.as_raw(), c_desc.as_ptr());

    // Registration copies the builder; the guard destroys it on either result.
    let rc = ffi::duckdb_register_config_option(con, option.as_raw());
    if rc != ffi::DuckDBSuccess {
        eprintln!("[duckdb-spanner] Failed to register config option: {name}");
    }
}

/// Read a spanner config option from a raw client context.
///
/// Returns `None` if the option is unset, empty, or not registered on this database.
/// Does **not** destroy `ctx` — the caller is responsible for cleanup.
///
/// # Safety
/// `ctx` must be a valid, non-null `duckdb_client_context`.
pub unsafe fn get_config_string_from_context(
    ctx: ffi::duckdb_client_context,
    option_name: &str,
) -> Option<String> {
    unsafe {
        let c_name = CString::new(option_name).ok()?;
        let val = OwnedDuckDbValue::from_raw(ffi::duckdb_client_context_get_config_option(
            ctx,
            c_name.as_ptr(),
            ptr::null_mut(),
        ));

        if val.as_raw().is_null() {
            return None;
        }

        let c_str = OwnedDuckDbString::from_raw(ffi::duckdb_get_varchar(val.as_raw()));
        if c_str.as_ptr().is_null() {
            None
        } else {
            let s = CStr::from_ptr(c_str.as_ptr())
                .to_string_lossy()
                .into_owned();
            if s.is_empty() {
                None
            } else {
                Some(s)
            }
        }
    }
}

/// Read a spanner config option from the client context during bind.
///
/// Returns `None` if the option is unset, empty, or not registered on this database.
pub fn get_config_string(bind: &BindInfo, option_name: &str) -> Option<String> {
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
        let ctx = OwnedDuckDbClientContext::from_raw(ctx);
        if ctx.as_raw().is_null() {
            return None;
        }

        get_config_string_from_context(ctx.as_raw(), option_name)
    }
}
