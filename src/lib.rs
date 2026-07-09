mod bind_utils;
mod client;
mod config;
mod convert;
mod copy;
mod ddl;
mod error;
mod params;
mod query;
mod replacement;
mod runtime;
mod scalars;
mod scan;
mod schema;
mod tables;
mod types;

pub use config::register_config_options;
pub use copy::register_copy_function;
pub use ddl::{SpannerDdlAsyncVTab, SpannerDdlVTab, SpannerOperationsVTab};
pub use query::SpannerQueryVTab;
pub use replacement::register_replacement_scan;
pub use scan::SpannerScanVTab;
pub use tables::SpannerTablesVTab;

use duckdb::Connection;

/// Register Rust VTabs, scalar functions, and SQL table macros on a connection.
///
/// Used by the loadable extension entrypoint and integration tests.
pub fn register_extension_functions(con: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    ensure_rustls_crypto_provider();
    con.register_table_function::<SpannerQueryVTab>("spanner_query_raw")?;
    con.register_table_function::<SpannerScanVTab>("spanner_scan")?;
    con.register_table_function::<SpannerDdlVTab>("spanner_ddl_raw")?;
    con.register_table_function::<SpannerDdlAsyncVTab>("spanner_ddl_async_raw")?;
    con.register_table_function::<SpannerOperationsVTab>("spanner_operations_raw")?;
    con.register_table_function::<SpannerTablesVTab>("spanner_tables")?;
    con.execute_batch(include_str!("macros.sql"))?;
    Ok(())
}

/// Register C API extensions that require a raw `duckdb_connection` / `duckdb_database`.
///
/// # Safety
/// `db` and `raw_con` must be valid DuckDB handles.
pub unsafe fn register_c_api_extensions(
    db: duckdb::ffi::duckdb_database,
    raw_con: duckdb::ffi::duckdb_connection,
) {
    ensure_rustls_crypto_provider();
    unsafe {
        config::register_config_options(raw_con);
        copy::register_copy_function(raw_con, true);
        scalars::register_scalars_c_api(raw_con);
        replacement::register_replacement_scan(db);
    }
}

fn ensure_rustls_crypto_provider() {
    // The official Google Cloud client crates are built without their default
    // aws-lc provider so the MinGW artifact build does not link aws-lc-sys.
    // The loadable extension must install a process-wide provider itself
    // because it can run outside this crate's test harness.
    let _ = rustls::crypto::ring::default_provider().install_default();
}

#[cfg(feature = "loadable-extension")]
const MIN_DUCKDB_C_API_VERSION: &str = "v1.5.0";

#[cfg(feature = "loadable-extension")]
fn extension_entrypoint(con: duckdb::Connection) -> Result<(), Box<dyn std::error::Error>> {
    register_extension_functions(&con)
}

// ─── Manual C API init (replaces #[duckdb_entrypoint_c_api] macro) ───────────
//
// We write the init function manually because we need the raw duckdb_connection
// to register config options, and Connection.db is private.

#[cfg(feature = "loadable-extension")]
unsafe fn spanner_init_c_api_internal(
    info: duckdb::ffi::duckdb_extension_info,
    access: *const duckdb::ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn std::error::Error>> {
    unsafe {
        let have_api_struct =
            duckdb::ffi::duckdb_rs_extension_api_init(info, access, MIN_DUCKDB_C_API_VERSION)?;
        if !have_api_struct {
            return Ok(false);
        }

        let get_database = (*access)
            .get_database
            .ok_or("get_database function pointer is null in duckdb_extension_access")?;
        let db_ptr = get_database(info);
        if db_ptr.is_null() {
            return Ok(false);
        }
        let db: duckdb::ffi::duckdb_database = *db_ptr;

        // Create a raw connection for config option registration.
        let mut raw_con: duckdb::ffi::duckdb_connection = std::ptr::null_mut();
        let rc = duckdb::ffi::duckdb_connect(db, &mut raw_con);
        if rc != duckdb::ffi::DuckDBSuccess {
            return Err("Failed to create connection for config option registration".into());
        }
        register_c_api_extensions(db, raw_con);
        duckdb::ffi::duckdb_disconnect(&mut raw_con);

        // Create the Rust Connection wrapper for table function and macro registration.
        let connection = duckdb::Connection::open_from_raw(db.cast())?;
        extension_entrypoint(connection)?;

        Ok(true)
    }
}

#[cfg(feature = "loadable-extension")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn spanner_init_c_api(
    info: duckdb::ffi::duckdb_extension_info,
    access: *const duckdb::ffi::duckdb_extension_access,
) -> bool {
    unsafe {
        match spanner_init_c_api_internal(info, access) {
            Ok(v) => v,
            Err(x) => {
                if let Some(set_error_fn) = (*access).set_error {
                    match std::ffi::CString::new(x.to_string()) {
                        Ok(e) => set_error_fn(info, e.as_ptr()),
                        Err(_) => {
                            let msg = c"Extension initialization failed, but the error message could not be converted to a C string";
                            set_error_fn(info, msg.as_ptr());
                        }
                    }
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn ensure_rustls_provider_installs_default() {
        super::ensure_rustls_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }
}
