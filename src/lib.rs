mod bind_utils;
mod cache;
mod client;
mod config;
mod connection;
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
mod streaming;
mod tables;
mod types;
mod vector_size;

pub use ddl::{SpannerDdlAsyncVTab, SpannerDdlVTab, SpannerOperationsVTab};
pub use query::SpannerQueryVTab;
pub use scan::SpannerScanVTab;
pub use tables::SpannerTablesVTab;

use duckdb::Connection;
use std::fmt;

#[cfg(test)]
thread_local! {
    static ALLOCATION_FAILURE: std::cell::Cell<Option<FailurePoint>> = const { std::cell::Cell::new(None) };
    static STATUS_FAILURE: std::cell::Cell<Option<FailurePoint>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct FailurePoint {
    stage: &'static str,
    remaining: usize,
}

#[cfg(test)]
fn should_fail_at(
    failure: &'static std::thread::LocalKey<std::cell::Cell<Option<FailurePoint>>>,
    stage: &'static str,
) -> bool {
    failure.with(|failure| {
        let Some(mut point) = failure.get() else {
            return false;
        };
        if point.stage != stage {
            return false;
        }
        if point.remaining == 1 {
            failure.set(None);
            true
        } else {
            point.remaining -= 1;
            failure.set(Some(point));
            false
        }
    })
}

#[cfg(test)]
fn should_fail_allocation(stage: &'static str) -> bool {
    should_fail_at(&ALLOCATION_FAILURE, stage)
}

#[cfg(not(test))]
const fn should_fail_allocation(_stage: &'static str) -> bool {
    false
}

#[cfg(test)]
fn should_fail_status(stage: &'static str) -> bool {
    should_fail_at(&STATUS_FAILURE, stage)
}

#[cfg(not(test))]
const fn should_fail_status(_stage: &'static str) -> bool {
    false
}

#[cfg(test)]
struct AllocationFailureGuard;

#[cfg(test)]
impl AllocationFailureGuard {
    fn set(stage: &'static str, occurrence: usize) -> Self {
        assert!(occurrence > 0);
        ALLOCATION_FAILURE.with(|failure| {
            assert!(failure
                .replace(Some(FailurePoint {
                    stage,
                    remaining: occurrence,
                }))
                .is_none());
        });
        Self
    }
}

#[cfg(test)]
impl Drop for AllocationFailureGuard {
    fn drop(&mut self) {
        ALLOCATION_FAILURE.with(|failure| failure.set(None));
    }
}

#[cfg(test)]
struct StatusFailureGuard;

#[cfg(test)]
impl StatusFailureGuard {
    fn set(stage: &'static str, occurrence: usize) -> Self {
        assert!(occurrence > 0);
        STATUS_FAILURE.with(|failure| {
            assert!(failure
                .replace(Some(FailurePoint {
                    stage,
                    remaining: occurrence,
                }))
                .is_none());
        });
        Self
    }
}

#[cfg(test)]
impl Drop for StatusFailureGuard {
    fn drop(&mut self) {
        STATUS_FAILURE.with(|failure| failure.set(None));
    }
}

const CONFIG_OPTION_NAMES: &[&str] = &[
    "spanner_project",
    "spanner_instance",
    "spanner_database",
    "spanner_database_path",
    "spanner_endpoint",
    "spanner_endpoint_mode",
    "spanner_admin_endpoint",
    "spanner_stream_idle_timeout_secs",
];

const FUNCTION_NAMES: &[&str] = &[
    "spanner_value",
    "spanner_typed",
    "spanner_params",
    "interval_to_iso8601",
    "spanner_query_raw",
    "spanner_scan",
    "spanner_ddl_raw",
    "spanner_ddl_async_raw",
    "spanner_operations_raw",
    "spanner_tables",
    "spanner_query",
    "spanner_ddl",
    "spanner_ddl_async",
    "spanner_operations",
];

/// A deterministic extension-registration failure.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct RegistrationError {
    stage: &'static str,
    name: String,
    detail: String,
}

impl RegistrationError {
    pub(crate) fn new(
        stage: &'static str,
        name: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            stage,
            name: name.into(),
            detail: detail.into(),
        }
    }

    pub fn stage(&self) -> &str {
        self.stage
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "extension registration failed at stage '{}' for '{}': {}",
            self.stage, self.name, self.detail
        )
    }
}

impl std::error::Error for RegistrationError {}

fn preflight_registration(con: &Connection) -> Result<(), RegistrationError> {
    for name in CONFIG_OPTION_NAMES {
        let exists: bool = con
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM duckdb_settings() WHERE lower(name) = lower(?))",
                [name],
                |row| row.get(0),
            )
            .map_err(|error| {
                RegistrationError::new("preflight config", *name, error.to_string())
            })?;
        if exists {
            return Err(RegistrationError::new(
                "preflight config",
                *name,
                "name is already registered",
            ));
        }
    }

    for name in FUNCTION_NAMES {
        let exists: bool = con
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM duckdb_functions() WHERE lower(function_name) = lower(?))",
                [name],
                |row| row.get(0),
            )
            .map_err(|error| RegistrationError::new("preflight function", *name, error.to_string()))?;
        if exists {
            return Err(RegistrationError::new(
                "preflight function",
                *name,
                "name is already registered",
            ));
        }
    }

    Ok(())
}

fn register_table_function<T: duckdb::vtab::VTab>(
    con: &Connection,
    name: &'static str,
) -> Result<(), RegistrationError> {
    if should_fail_status("register table function") {
        return Err(RegistrationError::new(
            "register table function",
            name,
            "injected DuckDB error status",
        ));
    }
    con.register_table_function::<T>(name)
        .map_err(|error| RegistrationError::new("register table function", name, error.to_string()))
}

fn register_extension_functions(con: &Connection) -> Result<(), RegistrationError> {
    ensure_rustls_crypto_provider();
    register_table_function::<SpannerQueryVTab>(con, "spanner_query_raw")?;
    register_table_function::<SpannerScanVTab>(con, "spanner_scan")?;
    register_table_function::<SpannerDdlVTab>(con, "spanner_ddl_raw")?;
    register_table_function::<SpannerDdlAsyncVTab>(con, "spanner_ddl_async_raw")?;
    register_table_function::<SpannerOperationsVTab>(con, "spanner_operations_raw")?;
    register_table_function::<SpannerTablesVTab>(con, "spanner_tables")?;
    register_sql_macros(con)?;
    Ok(())
}

fn preflight_autocommit(con: &Connection) -> Result<(), RegistrationError> {
    // duckdb-rs 1.10504.0 hardcodes Connection::is_autocommit() to true. DuckDB
    // assigns each autocommit statement a new transaction ID, while consecutive
    // statements inside an explicit transaction retain the same ID.
    let current_id = || {
        con.query_row("SELECT current_transaction_id()", [], |row| {
            row.get::<_, u64>(0)
        })
        .map_err(|error| {
            RegistrationError::new(
                "preflight transaction",
                "spanner extension",
                format!("failed to inspect transaction state: {error}"),
            )
        })
    };
    let first = current_id()?;
    let second = current_id()?;
    if first == second {
        return Err(RegistrationError::new(
            "preflight transaction",
            "spanner extension",
            "registration requires an autocommit connection",
        ));
    }
    Ok(())
}

struct OwnedDuckDbResult(duckdb::ffi::duckdb_result);

impl OwnedDuckDbResult {
    fn as_mut_ptr(&mut self) -> *mut duckdb::ffi::duckdb_result {
        &mut self.0
    }
}

impl Drop for OwnedDuckDbResult {
    fn drop(&mut self) {
        unsafe { duckdb::ffi::duckdb_destroy_result(&mut self.0) };
    }
}

unsafe fn execute_raw_query(
    raw_con: duckdb::ffi::duckdb_connection,
    query: &std::ffi::CStr,
    purpose: &'static str,
) -> Result<OwnedDuckDbResult, RegistrationError> {
    let mut result = std::mem::MaybeUninit::<duckdb::ffi::duckdb_result>::zeroed();
    let status = unsafe { duckdb::ffi::duckdb_query(raw_con, query.as_ptr(), result.as_mut_ptr()) };
    // DuckDB requires destroying the result even when duckdb_query fails.
    let mut result = OwnedDuckDbResult(unsafe { result.assume_init() });
    if status != duckdb::ffi::DuckDBSuccess {
        let error = unsafe { duckdb::ffi::duckdb_result_error(result.as_mut_ptr()) };
        let detail = if error.is_null() {
            format!("duckdb_query returned status {status} without an error message")
        } else {
            unsafe { std::ffi::CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned()
        };
        return Err(RegistrationError::new(
            "preflight transaction",
            purpose,
            detail,
        ));
    }
    Ok(result)
}

unsafe fn raw_transaction_id(
    raw_con: duckdb::ffi::duckdb_connection,
) -> Result<u64, RegistrationError> {
    let mut result = unsafe {
        execute_raw_query(
            raw_con,
            c"SELECT current_transaction_id()",
            "raw registration connection",
        )?
    };
    let columns = unsafe { duckdb::ffi::duckdb_column_count(result.as_mut_ptr()) };
    let rows = unsafe { duckdb::ffi::duckdb_row_count(result.as_mut_ptr()) };
    if columns != 1 || rows != 1 {
        return Err(RegistrationError::new(
            "preflight transaction",
            "raw registration connection",
            format!("transaction probe returned {columns} columns and {rows} rows"),
        ));
    }
    if unsafe { duckdb::ffi::duckdb_value_is_null(result.as_mut_ptr(), 0, 0) } {
        return Err(RegistrationError::new(
            "preflight transaction",
            "raw registration connection",
            "transaction probe returned NULL",
        ));
    }
    Ok(unsafe { duckdb::ffi::duckdb_value_uint64(result.as_mut_ptr(), 0, 0) })
}

unsafe fn preflight_raw_autocommit(
    raw_con: duckdb::ffi::duckdb_connection,
) -> Result<(), RegistrationError> {
    let first = unsafe { raw_transaction_id(raw_con)? };
    let second = unsafe { raw_transaction_id(raw_con)? };
    if first == second {
        return Err(RegistrationError::new(
            "preflight transaction",
            "raw registration connection",
            "registration requires an autocommit connection",
        ));
    }
    Ok(())
}

fn register_sql_macros(con: &Connection) -> Result<(), RegistrationError> {
    if should_fail_status("register SQL macros") {
        return Err(RegistrationError::new(
            "register SQL macros",
            "spanner macros",
            "injected DuckDB error status",
        ));
    }
    con.execute_batch("BEGIN TRANSACTION").map_err(|error| {
        RegistrationError::new(
            "begin macro transaction",
            "spanner macros",
            error.to_string(),
        )
    })?;
    if let Err(error) = con.execute_batch(include_str!("macros.sql")) {
        let detail = match con.execute_batch("ROLLBACK") {
            Ok(()) => error.to_string(),
            Err(rollback_error) => format!(
                "{}; macro transaction rollback also failed: {}",
                error, rollback_error
            ),
        };
        return Err(RegistrationError::new(
            "register SQL macros",
            "spanner macros",
            detail,
        ));
    }
    con.execute_batch("COMMIT").map_err(|error| {
        let rollback = con.execute_batch("ROLLBACK");
        let detail = match rollback {
            Ok(()) => error.to_string(),
            Err(rollback_error) => format!(
                "{}; macro transaction rollback also failed: {}",
                error, rollback_error
            ),
        };
        RegistrationError::new("commit macro transaction", "spanner macros", detail)
    })
}

unsafe fn register_checked_c_api_extensions(
    raw_con: duckdb::ffi::duckdb_connection,
) -> Result<(), RegistrationError> {
    ensure_rustls_crypto_provider();
    unsafe {
        let config_options = config::prepare_config_options()?;
        let copy_function = copy::prepare_copy_function(true)?;
        let scalar_functions = scalars::prepare_scalars_c_api()?;

        // DuckDB neither exposes COPY catalog introspection nor rejects a
        // duplicate COPY name. Registration claims the global "spanner" format
        // and silently replaces an existing handler; success proves installation,
        // not collision freedom. Register preflight-visible config state first so
        // every path that can leave COPY behind is rejected on a fresh retry.
        config_options.register(raw_con)?;
        copy_function.register(raw_con)?;
        scalar_functions.register(raw_con)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum InitializationState {
    New,
    Preflighted,
    Registering(&'static str),
    Failed(RegistrationError),
    Ready,
}

/// Per-attempt state. A fresh host retry creates a new value and is rejected by
/// preflight if an earlier attempt left any irreversible catalog state behind.
/// Config registration is the first mutation, so later non-introspectable COPY
/// state is always accompanied by a detectable config residue.
struct Initialization {
    state: InitializationState,
}

impl Initialization {
    fn new() -> Self {
        Self {
            state: InitializationState::New,
        }
    }

    fn run(
        &mut self,
        db: duckdb::ffi::duckdb_database,
        raw_con: duckdb::ffi::duckdb_connection,
        con: &Connection,
    ) -> Result<(), RegistrationError> {
        if self.state != InitializationState::New {
            return Err(RegistrationError::new(
                "initialization state",
                "spanner",
                format!("initialization cannot run from state {:?}", self.state),
            ));
        }

        let result: Result<(), RegistrationError> = (|| unsafe {
            preflight_autocommit(con)?;
            preflight_raw_autocommit(raw_con)?;
            preflight_registration(con)?;
            self.state = InitializationState::Preflighted;
            self.state = InitializationState::Registering("C API extensions");
            register_checked_c_api_extensions(raw_con)?;
            self.state = InitializationState::Registering("table functions and macros");
            register_extension_functions(con)?;
            self.state = InitializationState::Registering("replacement scan");
            // This stable C API returns no status and has no removal API. It is
            // deliberately the final irreversible initialization action.
            replacement::register_replacement_scan(db);
            Ok(())
        })();

        match result {
            Ok(()) => {
                self.state = InitializationState::Ready;
                Ok(())
            }
            Err(error) => {
                self.state = InitializationState::Failed(error.clone());
                Err(error)
            }
        }
    }
}

/// Register the complete Spanner extension surface on one DuckDB database.
///
/// This is the only public Rust registration API. It owns preflight,
/// initialization state, config/COPY/scalar registration, table functions,
/// transactional macros, and replacement-scan installation.
///
/// DuckDB has no stable public removal API or shared transaction for the native
/// registrations. SQL macros use their own rollback-capable transaction. If a
/// later stage fails, earlier native registrations remain, but config-first
/// ordering makes that residue visible to preflight so a fresh retry cannot
/// report success. Replacement-scan registration is last because DuckDB returns
/// no status for it.
///
/// # Safety
/// `db` and `raw_con` must be valid handles for the same database as `con` and
/// must remain valid for this call. Both `raw_con` and `con` must be in
/// autocommit mode. This function borrows and never disconnects `raw_con`.
pub unsafe fn register_spanner_extension(
    db: duckdb::ffi::duckdb_database,
    raw_con: duckdb::ffi::duckdb_connection,
    con: &Connection,
) -> Result<(), RegistrationError> {
    if db.is_null() {
        return Err(RegistrationError::new(
            "validate handles",
            "duckdb database",
            "database handle is null",
        ));
    }
    if raw_con.is_null() {
        return Err(RegistrationError::new(
            "validate handles",
            "duckdb registration connection",
            "connection handle is null",
        ));
    }
    Initialization::new().run(db, raw_con, con)
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

#[cfg(any(feature = "loadable-extension", test))]
struct OwnedDuckDbConnection(duckdb::ffi::duckdb_connection);

#[cfg(any(feature = "loadable-extension", test))]
impl OwnedDuckDbConnection {
    fn new(connection: duckdb::ffi::duckdb_connection) -> Self {
        Self(connection)
    }

    fn as_raw(&self) -> duckdb::ffi::duckdb_connection {
        self.0
    }
}

#[cfg(any(feature = "loadable-extension", test))]
impl Drop for OwnedDuckDbConnection {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { duckdb::ffi::duckdb_disconnect(&mut self.0) };
        }
    }
}

// ─── Manual C API init (replaces #[duckdb_entrypoint_c_api] macro) ───────────
//
// We write the init function manually because we need the raw duckdb_connection
// to register config options, and Connection.db is private.

#[cfg(any(feature = "loadable-extension", test))]
unsafe fn validate_extension_inputs(
    info: duckdb::ffi::duckdb_extension_info,
    access: *const duckdb::ffi::duckdb_extension_access,
) -> Result<(), RegistrationError> {
    if info.is_null() {
        return Err(RegistrationError::new(
            "validate extension access",
            "duckdb_extension_info",
            "pointer is null",
        ));
    }
    if access.is_null() {
        return Err(RegistrationError::new(
            "validate extension access",
            "duckdb_extension_access",
            "pointer is null",
        ));
    }
    if unsafe { (*access).get_api }.is_none() {
        return Err(RegistrationError::new(
            "validate extension access",
            "duckdb_extension_access.get_api",
            "required function pointer is null",
        ));
    }
    Ok(())
}

#[cfg(any(feature = "loadable-extension", test))]
unsafe fn report_extension_error(
    info: duckdb::ffi::duckdb_extension_info,
    access: *const duckdb::ffi::duckdb_extension_access,
    message: &str,
) {
    if info.is_null() || access.is_null() {
        return;
    }
    let Some(set_error) = (unsafe { (*access).set_error }) else {
        return;
    };
    let message = message.replace('\0', "\\0");
    let message = std::ffi::CString::new(message)
        .unwrap_or_else(|_| c"Extension initialization failed".to_owned());
    unsafe { set_error(info, message.as_ptr()) };
}

#[cfg(any(feature = "loadable-extension", test))]
unsafe fn run_extension_entrypoint<F, E>(
    info: duckdb::ffi::duckdb_extension_info,
    access: *const duckdb::ffi::duckdb_extension_access,
    initialize: F,
) -> bool
where
    F: FnOnce() -> Result<bool, E>,
    E: std::fmt::Display,
{
    if let Err(error) = unsafe { validate_extension_inputs(info, access) } {
        unsafe { report_extension_error(info, access, &error.to_string()) };
        return false;
    }

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(initialize)) {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            unsafe { report_extension_error(info, access, &error.to_string()) };
            false
        }
        Err(payload) => {
            let detail = if let Some(message) = payload.downcast_ref::<&str>() {
                *message
            } else if let Some(message) = payload.downcast_ref::<String>() {
                message.as_str()
            } else {
                "non-string panic payload"
            };
            let message = format!("panic during extension initialization: {detail}");
            unsafe { report_extension_error(info, access, &message) };
            false
        }
    }
}

#[cfg(feature = "loadable-extension")]
unsafe fn spanner_init_c_api_internal(
    info: duckdb::ffi::duckdb_extension_info,
    access: *const duckdb::ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn std::error::Error>> {
    unsafe {
        validate_extension_inputs(info, access)?;
        let have_api_struct =
            duckdb::ffi::duckdb_rs_extension_api_init(info, access, MIN_DUCKDB_C_API_VERSION)?;
        if !have_api_struct {
            return Ok(false);
        }

        let get_database = (*access).get_database.ok_or_else(|| {
            RegistrationError::new(
                "resolve database",
                "duckdb_extension_access.get_database",
                "function pointer is null",
            )
        })?;
        let db_ptr = get_database(info);
        if db_ptr.is_null() {
            return Err(RegistrationError::new(
                "resolve database",
                "duckdb_extension_access.get_database",
                "returned a null database pointer",
            )
            .into());
        }
        let db: duckdb::ffi::duckdb_database = *db_ptr;
        if db.is_null() {
            return Err(RegistrationError::new(
                "resolve database",
                "duckdb database handle",
                "database handle is null",
            )
            .into());
        }

        // Keep independent raw and Rust connections because duckdb-rs does not
        // expose its raw connection. Both target the same database catalog.
        let mut raw_con: duckdb::ffi::duckdb_connection = std::ptr::null_mut();
        let rc = duckdb::ffi::duckdb_connect(db, &mut raw_con);
        let raw_con = OwnedDuckDbConnection::new(raw_con);
        if rc != duckdb::ffi::DuckDBSuccess || raw_con.as_raw().is_null() {
            return Err(RegistrationError::new(
                "connect",
                "registration connection",
                format!("duckdb_connect returned status {rc}"),
            )
            .into());
        }

        let connection = duckdb::Connection::open_from_raw(db.cast()).map_err(|error| {
            RegistrationError::new(
                "connect",
                "duckdb-rs registration connection",
                error.to_string(),
            )
        })?;
        register_spanner_extension(db, raw_con.as_raw(), &connection)?;

        Ok(true)
    }
}

#[cfg(feature = "loadable-extension")]
#[unsafe(no_mangle)]
/// # Safety
///
/// `info` and `access` must be valid values supplied by DuckDB's extension loader.
/// `access` must remain valid for the duration of this call and be compatible with `info`.
pub unsafe extern "C" fn spanner_init_c_api(
    info: duckdb::ffi::duckdb_extension_info,
    access: *const duckdb::ffi::duckdb_extension_access,
) -> bool {
    unsafe { run_extension_entrypoint(info, access, || spanner_init_c_api_internal(info, access)) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::register_config_options;
    use crate::copy::register_copy_function;

    thread_local! {
        static REPORTED_EXTENSION_ERROR: std::cell::RefCell<Option<String>> = const { std::cell::RefCell::new(None) };
    }

    unsafe extern "C" fn test_get_api(
        _info: duckdb::ffi::duckdb_extension_info,
        _version: *const std::os::raw::c_char,
    ) -> *const std::ffi::c_void {
        std::ptr::null()
    }

    unsafe extern "C" fn capture_extension_error(
        _info: duckdb::ffi::duckdb_extension_info,
        error: *const std::os::raw::c_char,
    ) {
        let error = unsafe { std::ffi::CStr::from_ptr(error) }
            .to_string_lossy()
            .into_owned();
        REPORTED_EXTENSION_ERROR.with(|reported| *reported.borrow_mut() = Some(error));
    }

    fn take_reported_extension_error() -> Option<String> {
        REPORTED_EXTENSION_ERROR.with(|reported| reported.borrow_mut().take())
    }

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
        ensure_rustls_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[test]
    fn extension_access_validation_rejects_nulls_and_missing_get_api() {
        let mut info = duckdb::ffi::_duckdb_extension_info {
            internal_ptr: std::ptr::null_mut(),
        };
        let access_without_api = duckdb::ffi::duckdb_extension_access {
            set_error: Some(capture_extension_error),
            get_database: None,
            get_api: None,
        };

        let error = unsafe { validate_extension_inputs(std::ptr::null_mut(), std::ptr::null()) }
            .unwrap_err();
        assert_eq!(error.name(), "duckdb_extension_info");
        let error = unsafe { validate_extension_inputs(&mut info, std::ptr::null()) }.unwrap_err();
        assert_eq!(error.name(), "duckdb_extension_access");
        let error =
            unsafe { validate_extension_inputs(&mut info, &access_without_api) }.unwrap_err();
        assert_eq!(error.name(), "duckdb_extension_access.get_api");

        let valid_access = duckdb::ffi::duckdb_extension_access {
            get_api: Some(test_get_api),
            ..access_without_api
        };
        unsafe { validate_extension_inputs(&mut info, &valid_access) }.unwrap();
    }

    #[test]
    fn extension_error_reporting_never_dereferences_null_access() {
        let mut info = duckdb::ffi::_duckdb_extension_info {
            internal_ptr: std::ptr::null_mut(),
        };
        let access = duckdb::ffi::duckdb_extension_access {
            set_error: Some(capture_extension_error),
            get_database: None,
            get_api: Some(test_get_api),
        };

        unsafe {
            report_extension_error(std::ptr::null_mut(), &access, "ignored");
            report_extension_error(&mut info, std::ptr::null(), "ignored");
            report_extension_error(&mut info, &access, "failed\0with context");
        }
        let reported = take_reported_extension_error();
        assert_eq!(reported.as_deref(), Some("failed\\0with context"));
    }

    #[test]
    fn extension_entrypoint_wrapper_reports_bool_error_and_panic() {
        let mut info = duckdb::ffi::_duckdb_extension_info {
            internal_ptr: std::ptr::null_mut(),
        };
        let access = duckdb::ffi::duckdb_extension_access {
            set_error: Some(capture_extension_error),
            get_database: None,
            get_api: Some(test_get_api),
        };

        assert!(unsafe {
            run_extension_entrypoint(&mut info, &access, || Ok::<bool, String>(true))
        });
        assert_eq!(take_reported_extension_error(), None);

        assert!(!unsafe {
            run_extension_entrypoint(&mut info, &access, || Ok::<bool, String>(false))
        });
        assert_eq!(take_reported_extension_error(), None);

        assert!(!unsafe {
            run_extension_entrypoint(&mut info, &access, || {
                Err::<bool, String>("handled\0error".to_string())
            })
        });
        assert_eq!(
            take_reported_extension_error().as_deref(),
            Some("handled\\0error")
        );

        assert!(!unsafe {
            run_extension_entrypoint(&mut info, &access, || -> Result<bool, String> {
                panic!("injected entrypoint panic")
            })
        });
        assert_eq!(
            take_reported_extension_error().as_deref(),
            Some("panic during extension initialization: injected entrypoint panic")
        );
    }

    #[test]
    fn extension_entrypoint_wrapper_rejects_invalid_inputs_without_initializing() {
        let called = std::cell::Cell::new(false);
        let mut info = duckdb::ffi::_duckdb_extension_info {
            internal_ptr: std::ptr::null_mut(),
        };
        let valid_access = duckdb::ffi::duckdb_extension_access {
            set_error: Some(capture_extension_error),
            get_database: None,
            get_api: Some(test_get_api),
        };
        let missing_api = duckdb::ffi::duckdb_extension_access {
            get_api: None,
            ..valid_access
        };

        assert!(!unsafe {
            run_extension_entrypoint(std::ptr::null_mut(), &valid_access, || {
                called.set(true);
                Ok::<bool, String>(true)
            })
        });
        assert!(!called.get());
        assert_eq!(take_reported_extension_error(), None);

        assert!(!unsafe {
            run_extension_entrypoint(&mut info, std::ptr::null(), || {
                called.set(true);
                Ok::<bool, String>(true)
            })
        });
        assert!(!called.get());
        assert_eq!(take_reported_extension_error(), None);

        assert!(!unsafe {
            run_extension_entrypoint(&mut info, &missing_api, || {
                called.set(true);
                Ok::<bool, String>(true)
            })
        });
        assert!(!called.get());
        assert!(take_reported_extension_error()
            .unwrap()
            .contains("duckdb_extension_access.get_api"));
    }

    #[tokio::test]
    async fn google_client_tls_transport_uses_installed_provider() {
        use google_cloud_auth::credentials::service_account::Builder;
        use google_cloud_spanner::client::Spanner;

        ensure_rustls_crypto_provider();

        let key = serde_json::json!({
            "client_email": "test-client-email",
            "private_key_id": "test-private-key-id",
            "private_key": TEST_SERVICE_ACCOUNT_PKCS8,
            "project_id": "test-project-id",
            "universe_domain": "googleapis.com",
        });
        let credentials = Builder::new(key).build().unwrap();
        // Building the transport initializes the authenticated rustls path, but
        // creating a Spanner client is lazy and does not issue an RPC.
        let client = Spanner::builder()
            .with_endpoint("https://localhost.invalid")
            .with_credentials(credentials)
            .build()
            .await
            .unwrap();

        drop(client);
    }

    struct OwnedTestDatabase(duckdb::ffi::duckdb_database);

    impl Drop for OwnedTestDatabase {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { duckdb::ffi::duckdb_close(&mut self.0) };
            }
        }
    }

    struct RegistrationHandles {
        connection: Option<Connection>,
        raw_connection: Option<OwnedDuckDbConnection>,
        database: Option<OwnedTestDatabase>,
    }

    impl RegistrationHandles {
        fn database(&self) -> duckdb::ffi::duckdb_database {
            self.database.as_ref().unwrap().0
        }

        fn raw_connection(&self) -> duckdb::ffi::duckdb_connection {
            self.raw_connection.as_ref().unwrap().as_raw()
        }

        fn connection(&self) -> &Connection {
            self.connection.as_ref().unwrap()
        }
    }

    impl Drop for RegistrationHandles {
        fn drop(&mut self) {
            drop(self.connection.take());
            drop(self.raw_connection.take());
            drop(self.database.take());
        }
    }

    fn open_registration_handles() -> RegistrationHandles {
        let mut db = std::ptr::null_mut();
        let status = unsafe { duckdb::ffi::duckdb_open(c":memory:".as_ptr(), &mut db) };
        let database = OwnedTestDatabase(db);
        assert_eq!(status, duckdb::ffi::DuckDBSuccess);
        assert!(!database.0.is_null());

        let mut raw_con = std::ptr::null_mut();
        let status = unsafe { duckdb::ffi::duckdb_connect(db, &mut raw_con) };
        let raw_connection = OwnedDuckDbConnection::new(raw_con);
        assert_eq!(status, duckdb::ffi::DuckDBSuccess);
        assert!(!raw_connection.as_raw().is_null());
        let con = unsafe { Connection::open_from_raw(db) }.unwrap();
        RegistrationHandles {
            connection: Some(con),
            raw_connection: Some(raw_connection),
            database: Some(database),
        }
    }

    #[test]
    fn allocation_failures_report_stage_and_name() {
        for (failure, expected_stage, expected_name) in [
            ("config option", "allocate config option", "spanner_project"),
            (
                "config option type",
                "allocate config option type",
                "spanner_project",
            ),
            (
                "config option default",
                "allocate config option default",
                "spanner_project",
            ),
        ] {
            let _failure = AllocationFailureGuard::set(failure, 1);
            let error = unsafe { config::prepare_config_options() }.err().unwrap();
            assert_eq!(error.stage, expected_stage);
            assert_eq!(error.name, expected_name);
        }

        {
            let _failure = AllocationFailureGuard::set("COPY function", 1);
            let error = unsafe { copy::prepare_copy_function(true) }.err().unwrap();
            assert_eq!(error.stage, "allocate COPY function");
            assert_eq!(error.name, "spanner");
        }

        for (failure, expected_stage) in [
            ("scalar function set", "allocate scalar function set"),
            ("scalar function", "allocate scalar function"),
            ("scalar return type", "allocate scalar return type"),
            ("scalar parameter type", "allocate scalar parameter type"),
        ] {
            let _failure = AllocationFailureGuard::set(failure, 1);
            let error = unsafe { scalars::prepare_scalars_c_api() }.err().unwrap();
            assert_eq!(error.stage, expected_stage);
            assert_eq!(error.name, "spanner_value");
        }
    }

    #[test]
    fn c_api_registration_statuses_are_propagated() {
        unsafe {
            let handles = open_registration_handles();
            let raw_con = handles.raw_connection();

            // DuckDB 1.5.4 accepts duplicate COPY names. Both success statuses
            // are checked; this is why config is the production retry barrier.
            register_copy_function(raw_con, false).unwrap();
            register_copy_function(raw_con, false).unwrap();

            register_config_options(raw_con).unwrap();
            let config_error = register_config_options(raw_con).unwrap_err();
            assert_eq!(config_error.stage, "register config option");
            assert_eq!(config_error.name, "spanner_project");

            // Scalar sets also permit duplicate names; preflight, rather than
            // duplicate status, is the retry defense for this surface.
            scalars::register_scalars_c_api(raw_con).unwrap();
            scalars::register_scalars_c_api(raw_con).unwrap();
        }
    }

    #[test]
    fn stream_idle_timeout_config_registers_bigint_default_and_overrides() {
        unsafe {
            let handles = open_registration_handles();
            let raw_con = handles.raw_connection();
            let con = handles.connection();
            register_config_options(raw_con).unwrap();

            let setting = || {
                con.query_row(
                    "SELECT current_setting('spanner_stream_idle_timeout_secs')::BIGINT",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap()
            };
            assert_eq!(setting(), 900);

            con.execute_batch("SET spanner_stream_idle_timeout_secs = 42")
                .unwrap();
            assert_eq!(setting(), 42);

            con.execute_batch("SET spanner_stream_idle_timeout_secs = 0")
                .unwrap();
            assert_eq!(setting(), 0);

            let error = con
                .execute_batch("SET spanner_stream_idle_timeout_secs = 9223372036854775808")
                .unwrap_err();
            assert!(error.to_string().contains("INT64"), "{error}");
        }
    }

    #[test]
    fn injected_c_api_status_failures_report_stage_and_name() {
        unsafe {
            let handles = open_registration_handles();
            let raw_con = handles.raw_connection();

            {
                let _failure = StatusFailureGuard::set("register config option", 1);
                let error = register_config_options(raw_con).unwrap_err();
                assert_eq!(error.stage, "register config option");
                assert_eq!(error.name, "spanner_project");
            }
            {
                let _failure = StatusFailureGuard::set("register COPY function", 1);
                let error = register_copy_function(raw_con, false).unwrap_err();
                assert_eq!(error.stage, "register COPY function");
                assert_eq!(error.name, "spanner");
            }
            {
                let _failure = StatusFailureGuard::set("register scalar function", 1);
                let error = scalars::register_scalars_c_api(raw_con).unwrap_err();
                assert_eq!(error.stage, "register scalar function");
                assert_eq!(error.name, "spanner_value");
            }
        }
    }

    #[test]
    fn preflight_conflict_marks_failed_and_retry_cannot_report_ready() {
        unsafe {
            let handles = open_registration_handles();
            let db = handles.database();
            let raw_con = handles.raw_connection();
            let con = handles.connection();
            register_config_options(raw_con).unwrap();

            let mut initialization = Initialization::new();
            let error = initialization.run(db, raw_con, con).unwrap_err();
            assert_eq!(error.stage, "preflight config");
            assert_eq!(error.name, "spanner_project");
            assert!(matches!(
                initialization.state,
                InitializationState::Failed(_)
            ));

            let retry_error = initialization.run(db, raw_con, con).unwrap_err();
            assert_eq!(retry_error.stage, "initialization state");
            assert!(!matches!(initialization.state, InitializationState::Ready));
        }
    }

    #[test]
    fn function_conflicts_are_preflighted_before_registration() {
        {
            let handles = open_registration_handles();
            let db = handles.database();
            let raw_con = handles.raw_connection();
            let con = handles.connection();
            con.execute_batch("CREATE MACRO spanner_query(x) AS x")
                .unwrap();

            let mut initialization = Initialization::new();
            let error = initialization.run(db, raw_con, con).unwrap_err();
            assert_eq!(error.stage, "preflight function");
            assert_eq!(error.name, "spanner_query");
            let settings_count: i64 = con
                .query_row(
                    "SELECT count(*) FROM duckdb_settings() WHERE name LIKE 'spanner_%'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(settings_count, 0);
        }
    }

    #[test]
    fn fresh_attempt_rejects_every_observable_partial_registration_class() {
        unsafe {
            let handles = open_registration_handles();
            let db = handles.database();
            let raw_con = handles.raw_connection();
            let con = handles.connection();
            register_config_options(raw_con).unwrap();
            let mut retry = Initialization::new();
            let error = retry.run(db, raw_con, con).unwrap_err();
            assert_eq!(error.stage, "preflight config");
            assert!(matches!(retry.state, InitializationState::Failed(_)));
        }

        unsafe {
            let handles = open_registration_handles();
            let db = handles.database();
            let raw_con = handles.raw_connection();
            let con = handles.connection();
            scalars::register_scalars_c_api(raw_con).unwrap();
            let mut retry = Initialization::new();
            let error = retry.run(db, raw_con, con).unwrap_err();
            assert_eq!(error.stage, "preflight function");
            assert_eq!(error.name, "spanner_value");
        }

        {
            let handles = open_registration_handles();
            let db = handles.database();
            let raw_con = handles.raw_connection();
            let con = handles.connection();
            con.register_table_function::<SpannerQueryVTab>("spanner_query_raw")
                .unwrap();
            let mut retry = Initialization::new();
            let error = retry.run(db, raw_con, con).unwrap_err();
            assert_eq!(error.stage, "preflight function");
            assert_eq!(error.name, "spanner_query_raw");
        }

        {
            let handles = open_registration_handles();
            let db = handles.database();
            let raw_con = handles.raw_connection();
            let con = handles.connection();
            con.execute_batch("CREATE MACRO spanner_query(x) AS TABLE SELECT x")
                .unwrap();
            let mut retry = Initialization::new();
            let error = retry.run(db, raw_con, con).unwrap_err();
            assert_eq!(error.stage, "preflight function");
            assert_eq!(error.name, "spanner_query");
        }
    }

    #[test]
    fn fresh_attempt_rejects_failures_after_each_irreversible_stage() {
        for (failure, expected_stage) in [
            ("register COPY function", "register COPY function"),
            ("register scalar function", "register scalar function"),
            ("register table function", "register table function"),
            ("register SQL macros", "register SQL macros"),
        ] {
            {
                let handles = open_registration_handles();
                let db = handles.database();
                let raw_con = handles.raw_connection();
                let con = handles.connection();
                let mut attempt = Initialization::new();
                {
                    let _failure = StatusFailureGuard::set(failure, 1);
                    let error = attempt.run(db, raw_con, con).unwrap_err();
                    assert_eq!(error.stage, expected_stage);
                    assert!(matches!(attempt.state, InitializationState::Failed(_)));
                }

                let mut retry = Initialization::new();
                let retry_error = retry.run(db, raw_con, con).unwrap_err();
                assert_eq!(retry_error.stage, "preflight config");
                assert_eq!(retry_error.name, "spanner_project");
                assert!(matches!(retry.state, InitializationState::Failed(_)));
            }
        }
    }

    #[test]
    fn counted_failures_after_second_registration_leave_retry_barrier() {
        for (stage, expected_name) in [
            ("register config option", "spanner_instance"),
            ("register scalar function", "spanner_typed"),
            ("register table function", "spanner_scan"),
        ] {
            let handles = open_registration_handles();
            let db = handles.database();
            let raw_con = handles.raw_connection();
            let con = handles.connection();
            let mut attempt = Initialization::new();
            {
                let _failure = StatusFailureGuard::set(stage, 2);
                let error = attempt.run(db, raw_con, con).unwrap_err();
                assert_eq!(error.name(), expected_name);
                assert!(matches!(attempt.state, InitializationState::Failed(_)));
                assert!(!matches!(attempt.state, InitializationState::Ready));
            }

            let mut retry = Initialization::new();
            let retry_error = retry.run(db, raw_con, con).unwrap_err();
            assert_eq!(retry_error.stage(), "preflight config");
            assert_eq!(retry_error.name(), "spanner_project");
            assert!(matches!(retry.state, InitializationState::Failed(_)));
            assert!(!matches!(retry.state, InitializationState::Ready));
        }
    }

    #[test]
    fn registration_rejects_caller_owned_transaction_before_mutation() {
        let handles = open_registration_handles();
        let db = handles.database();
        let raw_con = handles.raw_connection();
        let con = handles.connection();
        con.execute_batch("BEGIN TRANSACTION").unwrap();

        let error = unsafe { register_spanner_extension(db, raw_con, con) }.unwrap_err();
        assert_eq!(error.stage(), "preflight transaction");
        let settings: i64 = con
            .query_row(
                "SELECT count(*) FROM duckdb_settings() WHERE name LIKE 'spanner_%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(settings, 0);
        let functions: i64 = con
            .query_row(
                "SELECT count(*) FROM duckdb_functions() WHERE function_name LIKE 'spanner_%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(functions, 0);
        con.execute_batch("ROLLBACK").unwrap();
    }

    #[test]
    fn registration_rejects_raw_connection_transaction_before_mutation() {
        let handles = open_registration_handles();
        let db = handles.database();
        let raw_con = handles.raw_connection();
        let con = handles.connection();
        drop(
            unsafe { execute_raw_query(raw_con, c"BEGIN TRANSACTION", "test raw connection") }
                .unwrap(),
        );

        let error = unsafe { register_spanner_extension(db, raw_con, con) }.unwrap_err();
        assert_eq!(error.stage(), "preflight transaction");
        assert_eq!(error.name(), "raw registration connection");
        let settings: i64 = con
            .query_row(
                "SELECT count(*) FROM duckdb_settings() WHERE name LIKE 'spanner_%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(settings, 0);
        let functions: i64 = con
            .query_row(
                "SELECT count(*) FROM duckdb_functions() WHERE function_name LIKE 'spanner_%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(functions, 0);

        drop(unsafe { execute_raw_query(raw_con, c"ROLLBACK", "test raw connection") }.unwrap());
    }

    #[test]
    fn macro_batch_failure_rolls_back_earlier_macros() {
        let con = Connection::open_in_memory().unwrap();
        con.execute_batch("CREATE MACRO spanner_ddl(x) AS TABLE SELECT x")
            .unwrap();
        let error = register_sql_macros(&con).unwrap_err();
        assert_eq!(error.stage, "register SQL macros");

        let count: i64 = con
            .query_row(
                "SELECT count(*) FROM duckdb_functions() WHERE function_name = 'spanner_query'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn successful_initialization_reaches_ready_and_registers_surface() {
        {
            let handles = open_registration_handles();
            let db = handles.database();
            let raw_con = handles.raw_connection();
            let con = handles.connection();
            let mut initialization = Initialization::new();
            initialization.run(db, raw_con, con).unwrap();
            assert_eq!(initialization.state, InitializationState::Ready);

            con.execute_batch("SET spanner_project = 'registration-test'")
                .unwrap();
            let interval: String = con
                .query_row("SELECT interval_to_iso8601(INTERVAL '1 day')", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(interval, "P1D");
            let macro_count: i64 = con
                .query_row(
                    "SELECT count(*) FROM duckdb_functions() WHERE function_name = 'spanner_query' AND function_type = 'table_macro'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(macro_count, 1);
        }
    }
}
