mod bind_utils;
mod cache;
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
mod vector_size;

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
    // Test-only generated key; never used outside local signer smoke tests.
    const TEST_SERVICE_ACCOUNT_PKCS8: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCHCdylSt7aTImX
42b6tjowFEAQBlklbXhxdlFuirw63xSmy9QlONBWEGZ5l/Srd0y1iWthhCVf8xfZ
kwFnXQG4Mw7qObsLt62IsscptZ/EAB8zypAP2rVi6BHz7IP6Z5qnPoEEVOFdKxVF
PqrgIB81cru8ggf62lXHL1YILLhhOhUHZX9juFKGTBexwBteCCrIfHUfWOqRmi5r
1DWxtCS4scmuEI/uF+Nwx2n9hNctDfcY+H/wqjEwHOSH6RRxGB0CSbVOSHSZwqk4
Ak1KNfSsZDflQXgqEZk8mEnN3N9WdmN1XJLdn14JUmHYVBMn7pLt3lnf3WoImfcf
SKmsP8KZAgMBAAECggEABBo0xjF0De6hPnYhIMw1fh1/8kyugCBnNkXHKVHLuXbY
Q/HxV/2102uDDpmHlX7xNvY0mU+N84upN9w495ZyNXZ0fCYQMQXUp/UCuLSygBUo
JqcawTked8YkJ3tI10eAHSmmQ9AZ4+LOSYMQr/CYvGf6JgPzsS5HEehIILLy0Evd
8dGXIaDD8xwOUOVutjI7FOyJZ/Js0QNVnbqRCB2USeEAlRKNHrEXmMwOhyPW22hM
BqgPFpI7o9OK6ug1AvzggOO/XDBvMRfm44nJkqTepeSPObVr/GigXH3YFvDYPERR
/d+1fEEBaaajUSsd6OWku8gj7jFP9QJhaX+PXQF9BQKBgQC7AETAhSwI4uvjqOPZ
VvBNuF5TLmTL16nKEELxXQ/312+OK1ALyvJ1+tCh6ygnVnCrSI+ttAQACkHvAW3+
GIGwzYuo9I/6WOlMX0Zc/jPvIHl6DHQbOaXGM2flwsrvZcn557vw4t7gezrsYO8y
BfH8DOCLx/sVtB0VW3NodI0wXQKBgQC43VIi4b0yt2IYeoDN/WblV1O9k6TZIXRu
vwm8mDupGVsnk2B7mfEERwkAuRpHoOclmp/JIdvE8H03FvUjryjqCLnrfzukCwtW
Rz/uWXYPfwXjnFBBn6v0w5dWQMeH0jhfHPuw0lQC0qRHBecAn8X2YfzyUvafEVIR
6UDvpJQnbQKBgB+ftQDFxKOgFHpElnuryympkzIH933Nc+Y7B8cfkNK9+RyW0Iud
/5DaIKwxQ3IbmSQuOjYK6l5DXdEYccx1woDu0b551Vtl69ZBinmxd4DqAgEU2BG+
lv1Etj5RydXgZd7ARLVA+KYH0PgmkGzqOnkAiHy7DggmlICHHaY9h571AoGAYg7r
wZryK9PAUfGxHxLaIK7IuZd2asJnK1NkS8iIZPMROhXfqNCIWtd/PAXznakI0xaI
yTyPgZB7KtyfnZUM489LJ1KvBR3inppenASSLjgXnJtOqvCSWtvhC5yC+lWVF0ad
bzax32lyQEYuOVOGw2FIthUxwkCCwwNyMWugNqUCgYBOjrGJtCKdLXhPLz2Cwb76
b5657n39SvPLKgVGJzZpM/DlAwPm+oMXeAaZWvzoACHuCBA6AbLdIT6wMGmDdk2y
2kPfWy7WOtYHwsd3GiAYXI439PWFPumYX7HRpdTuSnjKxG8EvEkW+dsgdQwti9NV
HRVeBmUqB9ZvD2iFzjibxg==
-----END PRIVATE KEY-----"#;

    #[test]
    fn ensure_rustls_provider_installs_default() {
        super::ensure_rustls_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[tokio::test]
    async fn google_auth_service_account_uses_installed_provider() {
        use google_cloud_auth::credentials::service_account::Builder;

        super::ensure_rustls_crypto_provider();

        let key = serde_json::json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": TEST_SERVICE_ACCOUNT_PKCS8,
            "project_id": "test-project-id",
            "universe_domain": "googleapis.com",
        });
        let signer = Builder::new(key).build_signer().unwrap();
        let signature = signer.sign(b"duckdb-spanner").await.unwrap();

        assert!(!signature.is_empty());
    }
}
