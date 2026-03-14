mod bind_utils;
mod client;
mod config;
mod convert;
mod copy;
mod ddl;
mod error;
mod params;
mod query;
mod runtime;
mod scan;
mod schema;
mod types;

pub use copy::register_copy_function;
pub use ddl::{SpannerDdlAsyncVTab, SpannerDdlVTab, SpannerOperationsVTab};
pub use query::SpannerQueryVTab;
pub use scan::SpannerScanVTab;

#[cfg(feature = "loadable-extension")]
fn extension_entrypoint(con: duckdb::Connection) -> Result<(), Box<dyn std::error::Error>> {
    con.register_table_function::<SpannerQueryVTab>("spanner_query_raw")?;
    con.register_table_function::<SpannerScanVTab>("spanner_scan")?;
    con.register_table_function::<SpannerDdlVTab>("spanner_ddl_raw")?;
    con.register_table_function::<SpannerDdlAsyncVTab>("spanner_ddl_async_raw")?;
    con.register_table_function::<SpannerOperationsVTab>("spanner_operations_raw")?;
    con.execute_batch(include_str!("macros.sql"))?;
    Ok(())
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
            duckdb::ffi::duckdb_rs_extension_api_init(info, access, "v1.2.0")?;
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
        config::register_config_options(raw_con);
        copy::register_copy_function(raw_con);
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
