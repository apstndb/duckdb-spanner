//! Copy Function API (COPY TO) for writing data to Spanner.
//!
//! Usage:
//! ```sql
//! COPY my_table TO 'SpannerTable' (FORMAT spanner);
//! COPY my_table TO 'SpannerTable' (FORMAT spanner,
//!     database_path 'projects/p/instances/i/databases/d',
//!     dialect 'googlesql',
//!     mode 'insert_or_update',
//!     batch_size 500);
//! ```
//!
//! ## batch_size and mutation limits
//!
//! Spanner limits commits to **80,000 mutations** including affected cells and
//! secondary index entries. The same row can therefore consume many mutations.
//! `batch_size` is capped at 80,000 rows as an absolute guard, but the default
//! of 1000 is more conservative and wide/indexed tables should use less.

use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;
use std::time::Duration;

use duckdb::ffi;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::retry_policy::{Aip194Strict, RetryPolicyExt};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::mutation::Mutation;
use google_cloud_spanner::transaction::{BasicTransactionRetryPolicy, WriteOnlyTransaction};
use google_cloud_spanner::types::{Type, TypeCode};
use google_cloud_spanner::Error as SpannerClientError;
use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;
use prost_types::value::Kind;
use prost_types::{ListValue, Value};

use crate::client;
use crate::config;
use crate::runtime;
use crate::schema;
use crate::RegistrationError;

const DEFAULT_BATCH_SIZE: usize = 1000;
const MAX_BATCH_SIZE: usize = 80_000;
const EXPLICIT_EMULATOR_WRITE_MAX_ATTEMPTS: u32 = 5;
const WRITE_TRANSACTION_MAX_ATTEMPTS: u32 = 5;
const WRITE_TRANSACTION_TOTAL_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_BEGIN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_COMMIT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_RPC_RETRY_MAX_ATTEMPTS: u32 = 3;
const WRITE_RPC_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

// These guards mirror the caller-owned handles documented by DuckDB's C API.
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
    OwnedDuckDbValue,
    ffi::duckdb_value,
    ffi::duckdb_destroy_value
);
owned_duckdb_handle!(
    OwnedDuckDbLogicalType,
    ffi::duckdb_logical_type,
    ffi::duckdb_destroy_logical_type
);
owned_duckdb_handle!(
    OwnedDuckDbClientContext,
    ffi::duckdb_client_context,
    ffi::duckdb_destroy_client_context
);
owned_duckdb_handle!(
    OwnedDuckDbCopyFunction,
    ffi::duckdb_copy_function,
    ffi::duckdb_destroy_copy_function
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

// ─── Types ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum MutationMode {
    Insert,
    Update,
    InsertOrUpdate,
    Replace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EmulatorRetryRoute {
    None,
    Sdk,
    ExplicitEndpointFallback,
}

impl MutationMode {
    fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "insert" => Ok(Self::Insert),
            "update" => Ok(Self::Update),
            "insert_or_update" => Ok(Self::InsertOrUpdate),
            "replace" => Ok(Self::Replace),
            _ => Err(format!(
                "Invalid mode '{s}': must be 'insert', 'update', 'insert_or_update', or 'replace'"
            )),
        }
    }
}

/// Metadata about a source column needed for reading values from DuckDB vectors.
#[derive(Clone)]
struct ColumnMeta {
    type_id: u32,
    /// DECIMAL scale (0 for non-decimal types).
    decimal_scale: u8,
    /// DECIMAL internal storage type (0 for non-decimal types).
    decimal_internal_type: u32,
    /// Fixed length for DuckDB ARRAY columns (0 for non-ARRAY types).
    array_size: usize,
    /// Child metadata for DuckDB LIST/ARRAY columns.
    child: Option<Box<ColumnMeta>>,
    /// Field metadata for DuckDB STRUCT columns.
    struct_fields: Vec<StructFieldMeta>,
    /// Target Spanner column type code (populated during GlobalInit).
    spanner_type_code: TypeCode,
}

#[derive(Clone)]
struct StructFieldMeta {
    name: String,
    column: ColumnMeta,
}

/// Bind-phase data stored for the lifetime of the COPY operation.
struct CopyBindData {
    database_path: String,
    endpoint: Option<String>,
    dialect: DatabaseDialect,
    mode: MutationMode,
    batch_size: usize,
    target_columns: Option<Vec<String>>,
    columns: Vec<ColumnMeta>,
}

/// Per-registration state attached to the copy function via `extra_info`.
struct CopyExtraInfo {
    /// When false, skip session config lookups during bind (avoids DuckDB SIGABRT
    /// when spanner_* config options were not registered on this database).
    config_enabled: bool,
}

/// Global state created during init and shared across sink calls.
struct CopyGlobalState {
    client: Arc<DatabaseClient>,
    emulator_retry_route: EmulatorRetryRoute,
    table_name: String,
    column_names: Vec<String>,
    mode: MutationMode,
    batch_size: usize,
    columns: Vec<ColumnMeta>,
    buffer: Vec<Mutation>,
    rows_written: u64,
    failure: Option<String>,
}

// ─── Registration ───────────────────────────────────────────────────────────

/// A fully allocated COPY-function builder that has not mutated DuckDB yet.
pub(crate) struct PreparedCopyFunction(OwnedDuckDbCopyFunction);

impl PreparedCopyFunction {
    /// # Safety
    /// `con` must be a valid `duckdb_connection`.
    pub(crate) unsafe fn register(
        self,
        con: ffi::duckdb_connection,
    ) -> Result<(), RegistrationError> {
        let rc = if crate::should_fail_status("register COPY function") {
            ffi::DuckDBError
        } else {
            unsafe { ffi::duckdb_register_copy_function(con, self.0.as_raw()) }
        };
        if rc != ffi::DuckDBSuccess {
            return Err(RegistrationError::new(
                "register COPY function",
                "spanner",
                format!("DuckDB returned status {rc}"),
            ));
        }
        Ok(())
    }
}

pub(crate) unsafe fn prepare_copy_function(
    config_enabled: bool,
) -> Result<PreparedCopyFunction, RegistrationError> {
    crate::ensure_rustls_crypto_provider();

    unsafe {
        let copy_fn = OwnedDuckDbCopyFunction::from_raw(ffi::duckdb_create_copy_function());
        if copy_fn.as_raw().is_null() || crate::should_fail_allocation("COPY function") {
            return Err(RegistrationError::new(
                "allocate COPY function",
                "spanner",
                "duckdb_create_copy_function returned null",
            ));
        }

        let name = c"spanner";
        ffi::duckdb_copy_function_set_name(copy_fn.as_raw(), name.as_ptr());
        let extra = Box::new(CopyExtraInfo { config_enabled });
        ffi::duckdb_copy_function_set_extra_info(
            copy_fn.as_raw(),
            Box::into_raw(extra) as *mut c_void,
            Some(drop_box::<CopyExtraInfo>),
        );
        ffi::duckdb_copy_function_set_bind(copy_fn.as_raw(), Some(copy_bind));
        ffi::duckdb_copy_function_set_global_init(copy_fn.as_raw(), Some(copy_global_init));
        ffi::duckdb_copy_function_set_sink(copy_fn.as_raw(), Some(copy_sink));
        ffi::duckdb_copy_function_set_finalize(copy_fn.as_raw(), Some(copy_finalize));

        Ok(PreparedCopyFunction(copy_fn))
    }
}

/// Register the `spanner` copy function on a raw DuckDB connection.
///
/// Set `config_enabled` when `register_config_options` was also called on the
/// same database so COPY bind can fall back to `SET spanner_*` session defaults.
///
/// # Safety
/// `con` must be a valid `duckdb_connection`.
#[cfg(test)]
pub(crate) unsafe fn register_copy_function(
    con: ffi::duckdb_connection,
    config_enabled: bool,
) -> Result<(), RegistrationError> {
    unsafe { prepare_copy_function(config_enabled)?.register(con) }
}

// ─── Callbacks ──────────────────────────────────────────────────────────────

unsafe fn copy_config_enabled(info: ffi::duckdb_copy_function_bind_info) -> bool {
    let extra = ffi::duckdb_copy_function_bind_get_extra_info(info) as *const CopyExtraInfo;
    if extra.is_null() {
        return false;
    }
    (*extra).config_enabled
}

unsafe extern "C" fn copy_bind(info: ffi::duckdb_copy_function_bind_info) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        copy_bind_inner(info)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => unsafe { set_bind_error(info, &e) },
        Err(_) => unsafe { set_bind_error(info, "panic in spanner copy bind") },
    }
}

unsafe extern "C" fn copy_global_init(info: ffi::duckdb_copy_function_global_init_info) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        copy_global_init_inner(info)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => unsafe { set_global_init_error(info, &e) },
        Err(_) => unsafe { set_global_init_error(info, "panic in spanner copy global init") },
    }
}

unsafe extern "C" fn copy_sink(
    info: ffi::duckdb_copy_function_sink_info,
    chunk: ffi::duckdb_data_chunk,
) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        copy_sink_inner(info, chunk)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => unsafe { set_sink_error(info, &e) },
        Err(_) => unsafe {
            let state_ptr = ffi::duckdb_copy_function_sink_get_global_state(info);
            let error = record_copy_callback_panic(state_ptr, "sink");
            set_sink_error(info, &error);
        },
    }
}

unsafe extern "C" fn copy_finalize(info: ffi::duckdb_copy_function_finalize_info) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        copy_finalize_inner(info)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => unsafe { set_finalize_error(info, &e) },
        Err(_) => unsafe {
            let state_ptr = ffi::duckdb_copy_function_finalize_get_global_state(info);
            let error = record_copy_callback_panic(state_ptr, "finalize");
            set_finalize_error(info, &error);
        },
    }
}

// ─── Inner implementations ──────────────────────────────────────────────────

unsafe fn copy_bind_inner(info: ffi::duckdb_copy_function_bind_info) -> Result<(), String> {
    // Parse COPY options
    let options_val = OwnedDuckDbValue::from_raw(ffi::duckdb_copy_function_bind_get_options(info));
    let opts = extract_options(options_val.as_raw());

    // Get client context for config fallback
    let ctx =
        OwnedDuckDbClientContext::from_raw(ffi::duckdb_copy_function_bind_get_client_context(info));
    let have_ctx = !ctx.as_raw().is_null();
    let config_enabled = unsafe { copy_config_enabled(info) };

    let cfg = |name: &str| -> Option<String> {
        if have_ctx && config_enabled {
            unsafe { config::get_config_string_from_context(ctx.as_raw(), name) }
        } else {
            None
        }
    };

    // Resolve database path: options → component parts → config
    let database_path = resolve_copy_database_path(&opts, cfg)?;

    let endpoint = option_value(&opts, "endpoint")
        .map(ToOwned::to_owned)
        .or_else(|| cfg("spanner_endpoint"));

    let dialect = option_value(&opts, "dialect")
        .map(schema::parse_dialect)
        .transpose()
        .map_err(|e| e.to_string())?
        .unwrap_or(DatabaseDialect::Unspecified);

    let mode = match option_value(&opts, "mode") {
        Some(m) => MutationMode::parse(m)?,
        None => MutationMode::InsertOrUpdate,
    };

    let batch_size = parse_batch_size(option_value(&opts, "batch_size"))?;

    // Collect source column type metadata
    let column_count = ffi::duckdb_copy_function_bind_get_column_count(info) as usize;
    let mut columns = Vec::with_capacity(column_count);

    for i in 0..column_count {
        let logical_type = OwnedDuckDbLogicalType::from_raw(
            ffi::duckdb_copy_function_bind_get_column_type(info, i as u64),
        );
        let column = column_meta_from_logical_type(logical_type.as_raw())?;
        columns.push(column);
    }

    let data = Box::new(CopyBindData {
        database_path,
        endpoint,
        dialect,
        mode,
        batch_size,
        target_columns: opts.get("columns").cloned(),
        columns,
    });

    ffi::duckdb_copy_function_bind_set_bind_data(
        info,
        Box::into_raw(data) as *mut c_void,
        Some(drop_box::<CopyBindData>),
    );

    Ok(())
}

unsafe fn copy_global_init_inner(
    info: ffi::duckdb_copy_function_global_init_info,
) -> Result<(), String> {
    // File path = Spanner table name
    let file_path_ptr = ffi::duckdb_copy_function_global_init_get_file_path(info);
    if file_path_ptr.is_null() {
        return Err("No table name specified (file path is null)".to_string());
    }
    let table_name = CStr::from_ptr(file_path_ptr).to_string_lossy().into_owned();
    if table_name.is_empty() {
        return Err("Table name cannot be empty".to_string());
    }

    // Get bind data
    let bind_ptr = ffi::duckdb_copy_function_global_init_get_bind_data(info);
    if bind_ptr.is_null() {
        return Err("Bind data is null".to_string());
    }
    let bind_data = &*(bind_ptr as *const CopyBindData);
    validate_batch_size(bind_data.batch_size)?;

    // Connect to Spanner
    let database_path = bind_data.database_path.clone();
    let endpoint = bind_data.endpoint.clone();
    let client = runtime::run(async move {
        client::get_or_create_client(&database_path, endpoint.as_deref()).await
    })
    .map_err(|e| format!("Runtime error: {e}"))?
    .map_err(|e| format!("Failed to connect to Spanner: {e}"))?;

    // An explicit dialect avoids metadata discovery for endpoints that do not
    // expose INFORMATION_SCHEMA.DATABASE_OPTIONS. COPY discovery retains
    // unsupported generated columns so they can be excluded before conversion.
    let schema_client = Arc::clone(&client);
    let schema_table_name = table_name.clone();
    let schema_dialect = bind_data.dialect.clone();
    let schema_database_path = bind_data.database_path.clone();
    let schema_endpoint = bind_data.endpoint.clone();
    let schema_columns = runtime::run(async move {
        schema::discover_table_schema_for_copy(
            &schema_client,
            &schema_table_name,
            schema_dialect,
            &schema_database_path,
            schema_endpoint.as_deref(),
        )
        .await
    })
    .map_err(|e| format!("Runtime error: {e}"))?
    .map_err(|e| format!("Schema discovery failed for table '{table_name}': {e}"))?;

    // Map the COPY source columns to the writable Spanner target columns,
    // excluding generated columns (Spanner rejects writes to them).
    //
    // The DuckDB COPY C API does not expose source column aliases in this version,
    // so unnamed COPY operations map positionally. The explicit `columns` COPY
    // option supplies Spanner target names in source-column order when needed.
    let targets = resolve_copy_columns(
        &schema_columns,
        bind_data.target_columns.as_deref(),
        bind_data.columns.len(),
        &table_name,
    )?;

    let column_names: Vec<String> = targets.iter().map(|c| c.name.clone()).collect();

    // Enrich DuckDB column metadata with Spanner target type codes
    let mut columns = bind_data.columns.clone();
    for (col, target) in columns.iter_mut().zip(targets.iter()) {
        apply_spanner_type(col, &target.spanner_type)?;
    }

    let sdk_emulator_mode = std::env::var("SPANNER_EMULATOR_HOST")
        .ok()
        .is_some_and(|host| !host.is_empty());
    let emulator_retry_route =
        emulator_retry_route(bind_data.endpoint.as_deref(), sdk_emulator_mode);
    let state = Box::new(CopyGlobalState {
        client,
        emulator_retry_route,
        table_name,
        column_names,
        mode: bind_data.mode,
        batch_size: bind_data.batch_size,
        columns,
        // Grow with actual input rather than trusting an unbounded user option as capacity.
        buffer: Vec::new(),
        rows_written: 0,
        failure: None,
    });

    ffi::duckdb_copy_function_global_init_set_global_state(
        info,
        Box::into_raw(state) as *mut c_void,
        Some(drop_box::<CopyGlobalState>),
    );

    Ok(())
}

unsafe fn copy_sink_inner(
    info: ffi::duckdb_copy_function_sink_info,
    chunk: ffi::duckdb_data_chunk,
) -> Result<(), String> {
    let state_ptr = ffi::duckdb_copy_function_sink_get_global_state(info);
    if state_ptr.is_null() {
        return Err(copy_failure_message("sink", "global state is null", 0));
    }
    // DuckDB 1.5.4's C COPY adapter leaves execution_mode unset, so this
    // PhysicalCopyToFile sink is serial and the global state is not aliased.
    let state = &mut *state_ptr.cast::<CopyGlobalState>();
    if let Some(failure) = &state.failure {
        return Err(failure.clone());
    }

    if let Err(cause) = copy_sink_chunk(state, chunk) {
        return Err(record_copy_failure(state, "sink", &cause));
    }

    Ok(())
}

unsafe fn copy_sink_chunk(
    state: &mut CopyGlobalState,
    chunk: ffi::duckdb_data_chunk,
) -> Result<(), String> {
    let row_count = ffi::duckdb_data_chunk_get_size(chunk) as usize;
    if row_count == 0 {
        return Ok(());
    }

    let col_count = state.columns.len();

    // The pinned DuckDB C COPY adapter flattens the chunk before this callback.
    let vectors: Vec<ffi::duckdb_vector> = (0..col_count)
        .map(|i| ffi::duckdb_data_chunk_get_vector(chunk, i as u64))
        .collect();

    for row_idx in 0..row_count {
        let mut values = Vec::with_capacity(col_count);
        for (vector, column) in vectors.iter().zip(&state.columns) {
            let val = read_duckdb_value(*vector, row_idx, column)?;
            values.push(val);
        }
        state.buffer.push(build_mutation(
            state.mode,
            &state.table_name,
            &state.column_names,
            values,
        ));

        if state.buffer.len() >= state.batch_size {
            flush_buffer(state)?;
        }
    }

    Ok(())
}

unsafe fn copy_finalize_inner(info: ffi::duckdb_copy_function_finalize_info) -> Result<(), String> {
    let state_ptr = ffi::duckdb_copy_function_finalize_get_global_state(info);
    if state_ptr.is_null() {
        return Ok(());
    }
    let state = &mut *state_ptr.cast::<CopyGlobalState>();
    if let Some(failure) = &state.failure {
        return Err(failure.clone());
    }

    if let Err(cause) = flush_buffer(state) {
        return Err(record_copy_failure(state, "finalize", &cause));
    }

    eprintln!(
        "[duckdb-spanner] COPY TO '{}': {} rows written",
        state.table_name, state.rows_written
    );

    Ok(())
}

fn copy_failure_message(phase: &str, cause: &str, rows_written: u64) -> String {
    format!(
        "COPY {phase} failed: {cause}; rows_written={rows_written}; earlier batches remain committed"
    )
}

fn record_copy_failure(state: &mut CopyGlobalState, phase: &str, cause: &str) -> String {
    if let Some(failure) = &state.failure {
        return failure.clone();
    }

    // A failed COPY must not let finalize commit rows that were only buffered.
    state.buffer.clear();
    let failure = copy_failure_message(phase, cause, state.rows_written);
    state.failure = Some(failure.clone());
    failure
}

unsafe fn record_copy_callback_panic(state_ptr: *mut c_void, phase: &str) -> String {
    if state_ptr.is_null() {
        return copy_failure_message(phase, "callback panicked", 0);
    }

    let state = &mut *state_ptr.cast::<CopyGlobalState>();
    record_copy_failure(state, phase, "callback panicked")
}

unsafe fn column_meta_from_logical_type(
    logical_type: ffi::duckdb_logical_type,
) -> Result<ColumnMeta, String> {
    if logical_type.is_null() {
        return Err("DuckDB returned null logical type for COPY source column".to_string());
    }

    let type_id = ffi::duckdb_get_type_id(logical_type);
    let (decimal_scale, decimal_internal_type) = if type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL
    {
        (
            ffi::duckdb_decimal_scale(logical_type),
            ffi::duckdb_decimal_internal_type(logical_type),
        )
    } else {
        (0, 0)
    };

    let (array_size, child, struct_fields) = match type_id {
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST => {
            let child_type =
                OwnedDuckDbLogicalType::from_raw(ffi::duckdb_list_type_child_type(logical_type));
            let child = column_meta_from_logical_type(child_type.as_raw())?;
            (0, Some(Box::new(child)), Vec::new())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
            let child_type =
                OwnedDuckDbLogicalType::from_raw(ffi::duckdb_array_type_child_type(logical_type));
            let child = column_meta_from_logical_type(child_type.as_raw())?;
            (
                ffi::duckdb_array_type_array_size(logical_type) as usize,
                Some(Box::new(child)),
                Vec::new(),
            )
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
            let field_count = ffi::duckdb_struct_type_child_count(logical_type) as usize;
            let mut fields = Vec::with_capacity(field_count);
            for field_idx in 0..field_count {
                let name_ptr = OwnedDuckDbString::from_raw(ffi::duckdb_struct_type_child_name(
                    logical_type,
                    field_idx as u64,
                ));
                if name_ptr.as_ptr().is_null() {
                    return Err(format!("DuckDB STRUCT field {field_idx} has no name"));
                }
                let name = CStr::from_ptr(name_ptr.as_ptr())
                    .to_string_lossy()
                    .into_owned();

                let child_type = OwnedDuckDbLogicalType::from_raw(
                    ffi::duckdb_struct_type_child_type(logical_type, field_idx as u64),
                );
                let column = column_meta_from_logical_type(child_type.as_raw())?;
                fields.push(StructFieldMeta { name, column });
            }
            (0, None, fields)
        }
        _ => (0, None, Vec::new()),
    };

    Ok(ColumnMeta {
        type_id,
        decimal_scale,
        decimal_internal_type,
        array_size,
        child,
        struct_fields,
        spanner_type_code: TypeCode::Unspecified,
    })
}

fn apply_spanner_type(col: &mut ColumnMeta, spanner_type: &Type) -> Result<(), String> {
    col.spanner_type_code = spanner_type.code();
    let source_is_array = col.type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST
        || col.type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY;

    if source_is_array {
        if spanner_type.code() != TypeCode::Array {
            return Err(format!(
                "DuckDB LIST/ARRAY source column requires a Spanner ARRAY target, got TypeCode {:?}",
                spanner_type.code()
            ));
        }

        let child = col.child.as_deref_mut().ok_or_else(|| {
            "DuckDB LIST/ARRAY source column is missing child metadata".to_string()
        })?;
        let elem_type = spanner_type
            .array_element_type()
            .ok_or_else(|| "Spanner ARRAY target is missing element type metadata".to_string())?;
        apply_spanner_type(child, &elem_type)?;
    }

    if col.type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT && spanner_type.code() != TypeCode::Json {
        return Err(format!(
            "DuckDB STRUCT source column requires a Spanner JSON target, got TypeCode {:?}",
            spanner_type.code()
        ));
    }

    Ok(())
}

// ─── Source → target column mapping ─────────────────────────────────────────

/// Map the COPY source columns to the writable Spanner target columns.
///
/// Generated columns (which Spanner rejects writes to) are always excluded from
/// the writable target set. Two mapping strategies are supported:
///
/// * **By name** (`source_names = Some(..)`): each
///   source column is matched to a target column case-insensitively. A source
///   column with no target match — or one that resolves to a generated column —
///   is a hard error listing the available writable target columns.
/// * **Positional** (`source_names = None`): the source
///   columns are mapped to the writable target columns in ordinal order, and only
///   the arity is validated. This is the fallback used at runtime because the
///   DuckDB COPY C API does not expose source column names in this version.
///
/// Returns one target [`schema::ColumnInfo`] per source column, in source order,
/// which the caller uses both as the mutation column list and to enrich the
/// source metadata with Spanner target types.
fn resolve_copy_columns(
    schema_columns: &[schema::ColumnInfo],
    source_names: Option<&[String]>,
    source_count: usize,
    table: &str,
) -> Result<Vec<schema::ColumnInfo>, String> {
    let writable: Vec<&schema::ColumnInfo> =
        schema_columns.iter().filter(|c| !c.is_generated).collect();
    let generated_count = schema_columns.len() - writable.len();

    if let Some(column) = writable
        .iter()
        .find(|column| column.spanner_type.code() == TypeCode::Unspecified)
    {
        return Err(format!(
            "Writable column '{}' in Spanner table '{table}' has an unsupported type",
            column.name
        ));
    }

    // By-name mapping requires a name for every source column.
    if let Some(names) = source_names {
        if names.len() != source_count {
            return Err(format!(
                "Column count mismatch: COPY option columns has {} name(s), but source has \
                 {source_count} column(s)",
                names.len()
            ));
        }
        if names.iter().any(|n| n.is_empty()) {
            return Err("COPY option columns must not contain empty column names".to_string());
        }
        let mut seen_names = std::collections::HashMap::new();
        for name in names {
            let normalized = name.to_ascii_lowercase();
            if let Some(first_name) = seen_names.insert(normalized, name) {
                return Err(format!(
                    "COPY source column '{name}' duplicates source column '{first_name}'. \
                     Source column names must be unique for by-name mapping"
                ));
            }
        }
        let mut resolved = Vec::with_capacity(names.len());
        for name in names {
            // Match against ALL target columns (including generated ones) so a
            // source column pointing at a generated column yields a precise
            // error instead of a generic "no match".
            match schema_columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
            {
                Some(c) if c.is_generated => {
                    return Err(format!(
                        "COPY source column '{name}' maps to generated column '{}' in \
                         Spanner table '{table}', which cannot be written",
                        c.name
                    ));
                }
                Some(c) => resolved.push(c.clone()),
                None => {
                    return Err(format!(
                        "COPY source column '{name}' has no matching column in Spanner \
                         table '{table}'. Available target columns: {}",
                        join_column_names(&writable)
                    ));
                }
            }
        }
        return Ok(resolved);
    }

    // Positional fallback over the writable (non-generated) target columns.
    if source_count != writable.len() {
        let gen_note = if generated_count > 0 {
            format!(" ({generated_count} generated column(s) excluded)")
        } else {
            String::new()
        };
        return Err(format!(
            "Column count mismatch: source has {source_count} column(s), but Spanner table \
             '{table}' has {} writable column(s){gen_note}",
            writable.len()
        ));
    }

    Ok(writable.into_iter().cloned().collect())
}

fn join_column_names(columns: &[&schema::ColumnInfo]) -> String {
    columns
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

// ─── Option extraction ──────────────────────────────────────────────────────

/// Extract options from the COPY statement's options value.
///
/// DuckDB returns options as a STRUCT where each field is a LIST of values.
/// Scalar options use their first value, while list-valued options retain every
/// value. In particular, `columns ['Value', 'Id']` must preserve both names.
unsafe fn extract_options(
    options_val: ffi::duckdb_value,
) -> std::collections::HashMap<String, Vec<String>> {
    let mut opts = std::collections::HashMap::new();

    if options_val.is_null() || ffi::duckdb_is_null_value(options_val) {
        return opts;
    }

    // NOTE: duckdb_get_value_type returns an internal reference — do NOT destroy it.
    let options_type = ffi::duckdb_get_value_type(options_val);
    let type_id = ffi::duckdb_get_type_id(options_type);

    if type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT {
        let child_count = ffi::duckdb_struct_type_child_count(options_type);
        for i in 0..child_count {
            let name_ptr =
                OwnedDuckDbString::from_raw(ffi::duckdb_struct_type_child_name(options_type, i));
            if name_ptr.as_ptr().is_null() {
                continue;
            }
            let name = CStr::from_ptr(name_ptr.as_ptr())
                .to_string_lossy()
                .to_ascii_lowercase();

            // Skip 'format' — already consumed by DuckDB
            if name == "format" {
                continue;
            }

            let child_val =
                OwnedDuckDbValue::from_raw(ffi::duckdb_get_struct_child(options_val, i));
            if !child_val.as_raw().is_null() && !ffi::duckdb_is_null_value(child_val.as_raw()) {
                // Child may be a LIST; try to get the first element.
                // NOTE: duckdb_get_value_type returns an internal reference — do NOT destroy it.
                let child_type = ffi::duckdb_get_value_type(child_val.as_raw());
                let child_type_id = ffi::duckdb_get_type_id(child_type);

                if child_type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST {
                    let list_size = ffi::duckdb_get_list_size(child_val.as_raw());
                    let mut values = Vec::with_capacity(list_size as usize);
                    for index in 0..list_size {
                        let elem = OwnedDuckDbValue::from_raw(ffi::duckdb_get_list_child(
                            child_val.as_raw(),
                            index,
                        ));
                        if let Some(s) = value_to_string_allow_empty(elem.as_raw()) {
                            values.push(s);
                        }
                    }
                    opts.insert(name, values);
                } else if let Some(s) = value_to_string(child_val.as_raw()) {
                    opts.insert(name, vec![s]);
                }
            }
        }
    } else if type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP {
        let map_size = ffi::duckdb_get_map_size(options_val);
        for i in 0..map_size {
            let key = OwnedDuckDbValue::from_raw(ffi::duckdb_get_map_key(options_val, i));
            let val = OwnedDuckDbValue::from_raw(ffi::duckdb_get_map_value(options_val, i));
            if let (Some(k), Some(v)) =
                (value_to_string(key.as_raw()), value_to_string(val.as_raw()))
            {
                let key = k.to_ascii_lowercase();
                if key != "format" {
                    opts.insert(key, vec![v]);
                }
            }
        }
    }

    // NOTE: options_type is an internal reference from duckdb_get_value_type — do NOT destroy it.
    opts
}

fn option_value<'a>(
    opts: &'a std::collections::HashMap<String, Vec<String>>,
    name: &str,
) -> Option<&'a str> {
    opts.get(name)?.first().map(String::as_str)
}

fn parse_batch_size(value: Option<&str>) -> Result<usize, String> {
    let batch_size = match value {
        Some(value) => value
            .parse::<usize>()
            .map_err(|e| format!("Invalid batch_size '{value}': {e}"))?,
        None => DEFAULT_BATCH_SIZE,
    };
    validate_batch_size(batch_size)?;
    Ok(batch_size)
}

fn validate_batch_size(batch_size: usize) -> Result<(), String> {
    if batch_size == 0 {
        return Err("Invalid batch_size '0': must be greater than zero".to_string());
    }
    if batch_size > MAX_BATCH_SIZE {
        return Err(format!(
            "Invalid batch_size '{batch_size}': must not exceed {MAX_BATCH_SIZE} rows; use a lower value for multi-column or indexed tables"
        ));
    }
    Ok(())
}

/// Extract a string from a `duckdb_value`.
unsafe fn value_to_string(val: ffi::duckdb_value) -> Option<String> {
    value_to_string_allow_empty(val).filter(|s| !s.is_empty())
}

/// Extract a string while preserving empty values for list-valued options.
unsafe fn value_to_string_allow_empty(val: ffi::duckdb_value) -> Option<String> {
    if val.is_null() || ffi::duckdb_is_null_value(val) {
        return None;
    }
    let c_str = OwnedDuckDbString::from_raw(ffi::duckdb_get_varchar(val));
    if c_str.as_ptr().is_null() {
        return None;
    }
    let s = CStr::from_ptr(c_str.as_ptr())
        .to_string_lossy()
        .into_owned();
    Some(s)
}

/// Resolve the Spanner database resource path from COPY options and config.
///
/// Resolution order (matching `bind_utils::resolve_database_path`):
/// 1. project/instance/database options, with config fallback for omitted components
/// 2. database_path option
/// 3. spanner_project/spanner_instance/spanner_database config
/// 4. spanner_database_path config
/// 5. error
fn resolve_copy_database_path(
    opts: &std::collections::HashMap<String, Vec<String>>,
    cfg: impl Fn(&str) -> Option<String>,
) -> Result<String, String> {
    let project_opt = option_value(opts, "project").map(ToOwned::to_owned);
    let instance_opt = option_value(opts, "instance").map(ToOwned::to_owned);
    let database_opt = option_value(opts, "database").map(ToOwned::to_owned);
    let database_path_opt = option_value(opts, "database_path").map(ToOwned::to_owned);

    if database_path_opt.is_some()
        && (project_opt.is_some() || instance_opt.is_some() || database_opt.is_some())
    {
        return Err(
            "cannot combine database_path with project, instance, or database COPY options"
                .to_string(),
        );
    }

    if project_opt.is_some() || instance_opt.is_some() || database_opt.is_some() {
        let project = project_opt.or_else(|| cfg("spanner_project"));
        let instance = instance_opt.or_else(|| cfg("spanner_instance"));
        let database = database_opt.or_else(|| cfg("spanner_database"));
        let p = project.ok_or(
            "project is required when instance or database is specified. \
             Use project option or SET spanner_project = '...'",
        )?;
        let i = instance.ok_or(
            "instance is required when project or database is specified. \
             Use instance option or SET spanner_instance = '...'",
        )?;
        let d = database.ok_or(
            "database is required when project or instance is specified. \
             Use database option or SET spanner_database = '...'",
        )?;
        return Ok(format!("projects/{p}/instances/{i}/databases/{d}"));
    }

    if let Some(path) = database_path_opt {
        return Ok(path);
    }

    let project = cfg("spanner_project");
    let instance = cfg("spanner_instance");
    let database = cfg("spanner_database");

    if project.is_some() || instance.is_some() || database.is_some() {
        let p = project.ok_or(
            "project is required when instance or database is specified. \
             Use project option or SET spanner_project = '...'",
        )?;
        let i = instance.ok_or(
            "instance is required when project or database is specified. \
             Use instance option or SET spanner_instance = '...'",
        )?;
        let d = database.ok_or(
            "database is required when project or instance is specified. \
             Use database option or SET spanner_database = '...'",
        )?;
        return Ok(format!("projects/{p}/instances/{i}/databases/{d}"));
    }

    if let Some(path) = cfg("spanner_database_path") {
        return Ok(path);
    }

    Err(
        "No database specified. Use database_path, project/instance/database options, \
         or SET spanner_database_path"
            .to_string(),
    )
}

// ─── Buffer flush / mutation building ───────────────────────────────────────

fn emulator_retry_route(endpoint: Option<&str>, sdk_emulator_mode: bool) -> EmulatorRetryRoute {
    // The pinned SDK marks only SPANNER_EMULATOR_HOST clients as emulator;
    // explicit endpoints need the compatibility retry below.
    if sdk_emulator_mode {
        EmulatorRetryRoute::Sdk
    } else if endpoint.is_some() {
        EmulatorRetryRoute::ExplicitEndpointFallback
    } else {
        EmulatorRetryRoute::None
    }
}

fn flush_buffer(state: &mut CopyGlobalState) -> Result<(), String> {
    if state.buffer.is_empty() {
        return Ok(());
    }

    let mutations = std::mem::take(&mut state.buffer);
    let count = mutations.len();

    runtime::run(write_mutations(
        Arc::clone(&state.client),
        mutations,
        state.emulator_retry_route,
    ))
    .map_err(|e| {
        format!("Runtime error: {e}; the final batch's commit outcome may be unknown")
    })??;

    state.rows_written += count as u64;
    Ok(())
}

async fn write_mutations(
    client: Arc<DatabaseClient>,
    mutations: Vec<Mutation>,
    emulator_retry_route: EmulatorRetryRoute,
) -> Result<(), String> {
    let mut attempt = 1;
    loop {
        match build_copy_write_transaction(&client)
            .write(mutations.clone())
            .await
        {
            Ok(_) => return Ok(()),
            Err(e)
                if emulator_retry_route == EmulatorRetryRoute::ExplicitEndpointFallback
                    && attempt < EXPLICIT_EMULATOR_WRITE_MAX_ATTEMPTS
                    && is_internal_emulator_schema_error(&e) =>
            {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(format_spanner_write_error(&e)),
        }
    }
}

fn copy_transaction_retry_policy() -> BasicTransactionRetryPolicy {
    BasicTransactionRetryPolicy::new()
        .with_max_attempts(WRITE_TRANSACTION_MAX_ATTEMPTS)
        .with_total_timeout(WRITE_TRANSACTION_TOTAL_TIMEOUT)
}

fn build_copy_write_transaction(client: &DatabaseClient) -> WriteOnlyTransaction {
    client
        .write_only_transaction()
        .with_retry_policy(copy_transaction_retry_policy())
        .with_begin_attempt_timeout(WRITE_BEGIN_ATTEMPT_TIMEOUT)
        .with_begin_retry_policy(
            Aip194Strict
                .with_time_limit(WRITE_RPC_RETRY_TIMEOUT)
                .with_attempt_limit(WRITE_RPC_RETRY_MAX_ATTEMPTS),
        )
        .with_commit_attempt_timeout(WRITE_COMMIT_ATTEMPT_TIMEOUT)
        .with_commit_retry_policy(
            Aip194Strict
                .with_time_limit(WRITE_RPC_RETRY_TIMEOUT)
                .with_attempt_limit(WRITE_RPC_RETRY_MAX_ATTEMPTS),
        )
        .build()
}

fn format_spanner_write_error(err: &SpannerClientError) -> String {
    let mut message = format!("Spanner write error: {err}");
    if is_ambiguous_write_error(err) {
        message.push_str("; the final batch's commit outcome is unknown");
    }
    message
}

fn is_ambiguous_write_error(err: &SpannerClientError) -> bool {
    err.is_timeout()
        || err.is_exhausted()
        || err.is_deserialization()
        || err.is_io()
        || err.status().is_some_and(|status| {
            matches!(
                status.code,
                Code::Cancelled
                    | Code::DeadlineExceeded
                    | Code::Unknown
                    | Code::Internal
                    | Code::Unavailable
                    | Code::DataLoss
            )
        })
}

fn is_internal_emulator_schema_error(err: &SpannerClientError) -> bool {
    err.status().is_some_and(|status| {
        status.code == Code::Internal
            && status.message.contains("Schema generation")
            && status
                .message
                .contains("was not registered with the Action Manager")
    })
}

fn build_mutation(
    mode: MutationMode,
    table: &str,
    columns: &[String],
    values: Vec<Value>,
) -> Mutation {
    let mut builder = match mode {
        MutationMode::Insert => Mutation::new_insert_builder(table),
        MutationMode::Update => Mutation::new_update_builder(table),
        MutationMode::InsertOrUpdate => Mutation::new_insert_or_update_builder(table),
        MutationMode::Replace => Mutation::new_replace_builder(table),
    };

    for (column, value) in columns.iter().zip(values.iter()) {
        builder = builder.set(column.as_str()).to(value);
    }
    builder.build()
}

// ─── DuckDB vector → Spanner proto Value conversion ─────────────────────────

/// Read a value from a DuckDB vector at the given row and convert to Spanner proto Value.
///
/// # Safety
/// - `data` must point to valid vector data of the appropriate type
/// - `validity` may be null (meaning all rows are valid)
/// - `row_idx` must be within the chunk's row count
unsafe fn read_duckdb_value(
    vector: ffi::duckdb_vector,
    row_idx: usize,
    col: &ColumnMeta,
) -> Result<Value, String> {
    let data = ffi::duckdb_vector_get_data(vector);
    let validity = ffi::duckdb_vector_get_validity(vector);

    // NULL check
    if !validity.is_null() && !ffi::duckdb_validity_row_is_valid(validity, row_idx as u64) {
        return Ok(Value {
            kind: Some(Kind::NullValue(0)),
        });
    }

    let kind = match col.type_id {
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => {
            let v = *data.cast::<bool>().add(row_idx);
            Kind::BoolValue(v)
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => {
            let v = *data.cast::<i8>().add(row_idx);
            Kind::StringValue((v as i64).to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
            let v = *data.cast::<i16>().add(row_idx);
            Kind::StringValue((v as i64).to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
            let v = *data.cast::<i32>().add(row_idx);
            Kind::StringValue((v as i64).to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
            let v = *data.cast::<i64>().add(row_idx);
            Kind::StringValue(v.to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => {
            let v = *data.cast::<u8>().add(row_idx);
            Kind::StringValue((v as i64).to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
            let v = *data.cast::<u16>().add(row_idx);
            Kind::StringValue((v as i64).to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => {
            let v = *data.cast::<u32>().add(row_idx);
            Kind::StringValue((v as i64).to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
            let v = *data.cast::<u64>().add(row_idx);
            Kind::StringValue(v.to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => {
            let v = *data.cast::<i128>().add(row_idx);
            Kind::StringValue(v.to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => {
            let v = *data.cast::<u128>().add(row_idx);
            Kind::StringValue(v.to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => {
            let v = *data.cast::<f32>().add(row_idx);
            if v.is_nan() {
                Kind::StringValue("NaN".to_string())
            } else if v.is_infinite() {
                Kind::StringValue(if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string())
            } else {
                Kind::NumberValue(v as f64)
            }
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => {
            let v = *data.cast::<f64>().add(row_idx);
            if v.is_nan() {
                Kind::StringValue("NaN".to_string())
            } else if v.is_infinite() {
                Kind::StringValue(if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string())
            } else {
                Kind::NumberValue(v)
            }
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => {
            let str_ptr = data.cast::<ffi::duckdb_string_t>().add(row_idx);
            let s = read_duckdb_string(str_ptr);
            Kind::StringValue(s)
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BLOB => {
            let str_ptr = data.cast::<ffi::duckdb_string_t>().add(row_idx);
            let bytes = read_duckdb_bytes(str_ptr);
            use base64::Engine;
            Kind::StringValue(base64::engine::general_purpose::STANDARD.encode(&bytes))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_DATE => {
            let days = *data.cast::<i32>().add(row_idx);
            Kind::StringValue(epoch_days_to_date_string(days)?)
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP | ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => {
            let micros = *data.cast::<i64>().add(row_idx);
            Kind::StringValue(epoch_micros_to_rfc3339(micros)?)
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {
            let secs = *data.cast::<i64>().add(row_idx);
            Kind::StringValue(epoch_micros_to_rfc3339(secs * 1_000_000)?)
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => {
            let millis = *data.cast::<i64>().add(row_idx);
            Kind::StringValue(epoch_micros_to_rfc3339(millis * 1_000)?)
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => {
            let nanos = *data.cast::<i64>().add(row_idx);
            let nanos_i128 = nanos as i128;
            let ts = time::OffsetDateTime::from_unix_timestamp_nanos(nanos_i128)
                .map_err(|e| format!("Timestamp overflow: {e}"))?;
            let s = ts
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| format!("Timestamp format error: {e}"))?;
            Kind::StringValue(s)
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
            let raw_i128 = read_decimal_raw(data, row_idx, col.decimal_internal_type);
            // If the target Spanner column is FLOAT64 or FLOAT32, convert to NumberValue.
            // Spanner FLOAT64 rejects StringValue (except for NaN/Infinity).
            if col.spanner_type_code == TypeCode::Float64
                || col.spanner_type_code == TypeCode::Float32
            {
                let s = decimal_i128_to_string(raw_i128, col.decimal_scale);
                let f: f64 = s
                    .parse()
                    .map_err(|e| format!("DECIMAL to FLOAT64 conversion failed: {e}"))?;
                Kind::NumberValue(f)
            } else {
                Kind::StringValue(decimal_i128_to_string(raw_i128, col.decimal_scale))
            }
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID => {
            let raw = *data.cast::<u128>().add(row_idx);
            // Reverse the MSB flip DuckDB applies for sort ordering
            let uuid_bits = raw ^ (1u128 << 127);
            Kind::StringValue(uuid::Uuid::from_u128(uuid_bits).to_string())
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST => {
            let child = col
                .child
                .as_deref()
                .ok_or_else(|| "DuckDB LIST source column is missing child metadata".to_string())?;
            let entry = *data.cast::<ffi::duckdb_list_entry>().add(row_idx);
            let child_vector = ffi::duckdb_list_vector_get_child(vector);
            let mut values = Vec::with_capacity(entry.length as usize);
            for child_idx in entry.offset..entry.offset + entry.length {
                values.push(read_duckdb_value(child_vector, child_idx as usize, child)?);
            }
            Kind::ListValue(ListValue { values })
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
            let child = col.child.as_deref().ok_or_else(|| {
                "DuckDB ARRAY source column is missing child metadata".to_string()
            })?;
            let child_vector = ffi::duckdb_array_vector_get_child(vector);
            let start = row_idx
                .checked_mul(col.array_size)
                .ok_or_else(|| "DuckDB ARRAY child offset overflow".to_string())?;
            let mut values = Vec::with_capacity(col.array_size);
            for child_idx in start..start + col.array_size {
                values.push(read_duckdb_value(child_vector, child_idx, child)?);
            }
            Kind::ListValue(ListValue { values })
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
            let json = read_duckdb_json_value(vector, row_idx, col)?;
            let s = serde_json::to_string(&json)
                .map_err(|e| format!("STRUCT to JSON conversion failed: {e}"))?;
            Kind::StringValue(s)
        }
        _ => {
            return Err(format!(
                "Unsupported DuckDB type {} for COPY TO spanner",
                col.type_id
            ));
        }
    };

    Ok(Value { kind: Some(kind) })
}

unsafe fn read_duckdb_json_value(
    vector: ffi::duckdb_vector,
    row_idx: usize,
    col: &ColumnMeta,
) -> Result<serde_json::Value, String> {
    let data = ffi::duckdb_vector_get_data(vector);
    let validity = ffi::duckdb_vector_get_validity(vector);

    if !validity.is_null() && !ffi::duckdb_validity_row_is_valid(validity, row_idx as u64) {
        return Ok(serde_json::Value::Null);
    }

    match col.type_id {
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => {
            let v = *data.cast::<bool>().add(row_idx);
            Ok(serde_json::Value::Bool(v))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => {
            let v = *data.cast::<i8>().add(row_idx);
            Ok(serde_json::Value::Number((v as i64).into()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
            let v = *data.cast::<i16>().add(row_idx);
            Ok(serde_json::Value::Number((v as i64).into()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
            let v = *data.cast::<i32>().add(row_idx);
            Ok(serde_json::Value::Number((v as i64).into()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => {
            let v = *data.cast::<i64>().add(row_idx);
            Ok(serde_json::Value::Number(v.into()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => {
            let v = *data.cast::<u8>().add(row_idx);
            Ok(serde_json::Value::Number((v as u64).into()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => {
            let v = *data.cast::<u16>().add(row_idx);
            Ok(serde_json::Value::Number((v as u64).into()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => {
            let v = *data.cast::<u32>().add(row_idx);
            Ok(serde_json::Value::Number((v as u64).into()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => {
            let v = *data.cast::<u64>().add(row_idx);
            Ok(serde_json::Value::Number(v.into()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => {
            let v = *data.cast::<i128>().add(row_idx);
            Ok(serde_json::Value::String(v.to_string()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UHUGEINT => {
            let v = *data.cast::<u128>().add(row_idx);
            Ok(serde_json::Value::String(v.to_string()))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => {
            let v = *data.cast::<f32>().add(row_idx);
            json_number_or_string(v as f64)
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => {
            let v = *data.cast::<f64>().add(row_idx);
            json_number_or_string(v)
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => {
            let str_ptr = data.cast::<ffi::duckdb_string_t>().add(row_idx);
            Ok(serde_json::Value::String(read_duckdb_string(str_ptr)))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BLOB => {
            let str_ptr = data.cast::<ffi::duckdb_string_t>().add(row_idx);
            let bytes = read_duckdb_bytes(str_ptr);
            use base64::Engine;
            Ok(serde_json::Value::String(
                base64::engine::general_purpose::STANDARD.encode(&bytes),
            ))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_DATE => {
            let days = *data.cast::<i32>().add(row_idx);
            Ok(serde_json::Value::String(epoch_days_to_date_string(days)?))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP | ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => {
            let micros = *data.cast::<i64>().add(row_idx);
            Ok(serde_json::Value::String(epoch_micros_to_rfc3339(micros)?))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {
            let secs = *data.cast::<i64>().add(row_idx);
            Ok(serde_json::Value::String(epoch_micros_to_rfc3339(
                secs * 1_000_000,
            )?))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => {
            let millis = *data.cast::<i64>().add(row_idx);
            Ok(serde_json::Value::String(epoch_micros_to_rfc3339(
                millis * 1_000,
            )?))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => {
            let nanos = *data.cast::<i64>().add(row_idx);
            let ts = time::OffsetDateTime::from_unix_timestamp_nanos(nanos as i128)
                .map_err(|e| format!("Timestamp overflow: {e}"))?;
            let s = ts
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|e| format!("Timestamp format error: {e}"))?;
            Ok(serde_json::Value::String(s))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => {
            let raw_i128 = read_decimal_raw(data, row_idx, col.decimal_internal_type);
            Ok(serde_json::Value::String(decimal_i128_to_string(
                raw_i128,
                col.decimal_scale,
            )))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID => {
            let raw = *data.cast::<u128>().add(row_idx);
            let uuid_bits = raw ^ (1u128 << 127);
            Ok(serde_json::Value::String(
                uuid::Uuid::from_u128(uuid_bits).to_string(),
            ))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST => {
            let child = col
                .child
                .as_deref()
                .ok_or_else(|| "DuckDB LIST source column is missing child metadata".to_string())?;
            let entry = *data.cast::<ffi::duckdb_list_entry>().add(row_idx);
            let child_vector = ffi::duckdb_list_vector_get_child(vector);
            let mut values = Vec::with_capacity(entry.length as usize);
            for child_idx in entry.offset..entry.offset + entry.length {
                values.push(read_duckdb_json_value(
                    child_vector,
                    child_idx as usize,
                    child,
                )?);
            }
            Ok(serde_json::Value::Array(values))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => {
            let child = col.child.as_deref().ok_or_else(|| {
                "DuckDB ARRAY source column is missing child metadata".to_string()
            })?;
            let child_vector = ffi::duckdb_array_vector_get_child(vector);
            let start = row_idx
                .checked_mul(col.array_size)
                .ok_or_else(|| "DuckDB ARRAY child offset overflow".to_string())?;
            let mut values = Vec::with_capacity(col.array_size);
            for child_idx in start..start + col.array_size {
                values.push(read_duckdb_json_value(child_vector, child_idx, child)?);
            }
            Ok(serde_json::Value::Array(values))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => {
            let mut object = serde_json::Map::with_capacity(col.struct_fields.len());
            for (field_idx, field) in col.struct_fields.iter().enumerate() {
                let child_vector = ffi::duckdb_struct_vector_get_child(vector, field_idx as u64);
                let value = read_duckdb_json_value(child_vector, row_idx, &field.column)?;
                object.insert(field.name.clone(), value);
            }
            Ok(serde_json::Value::Object(object))
        }
        _ => Err(format!(
            "Unsupported DuckDB type {} in STRUCT to JSON conversion",
            col.type_id
        )),
    }
}

fn json_number_or_string(v: f64) -> Result<serde_json::Value, String> {
    if v.is_finite() {
        serde_json::Number::from_f64(v)
            .map(serde_json::Value::Number)
            .ok_or_else(|| format!("Invalid JSON number: {v}"))
    } else if v.is_nan() {
        Ok(serde_json::Value::String("NaN".to_string()))
    } else {
        Ok(serde_json::Value::String(
            if v > 0.0 { "Infinity" } else { "-Infinity" }.to_string(),
        ))
    }
}

/// Read a DECIMAL raw value as i128, handling different internal storage types.
unsafe fn read_decimal_raw(data: *mut c_void, row_idx: usize, internal_type: u32) -> i128 {
    match internal_type {
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => *data.cast::<i16>().add(row_idx) as i128,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => *data.cast::<i32>().add(row_idx) as i128,
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => *data.cast::<i64>().add(row_idx) as i128,
        _ => *data.cast::<i128>().add(row_idx), // HUGEINT for width > 18
    }
}

// ─── String/blob reading from DuckDB vectors ────────────────────────────────

unsafe fn read_duckdb_string(str_ptr: *mut ffi::duckdb_string_t) -> String {
    let len = ffi::duckdb_string_t_length(*str_ptr) as usize;
    let data = ffi::duckdb_string_t_data(str_ptr);
    let bytes = std::slice::from_raw_parts(data.cast::<u8>(), len);
    String::from_utf8_lossy(bytes).into_owned()
}

unsafe fn read_duckdb_bytes(str_ptr: *mut ffi::duckdb_string_t) -> Vec<u8> {
    let len = ffi::duckdb_string_t_length(*str_ptr) as usize;
    let data = ffi::duckdb_string_t_data(str_ptr);
    std::slice::from_raw_parts(data.cast::<u8>(), len).to_vec()
}

// ─── Scalar conversion helpers ──────────────────────────────────────────────

fn epoch_days_to_date_string(days: i32) -> Result<String, String> {
    let epoch = time::Date::from_calendar_date(1970, time::Month::January, 1)
        .map_err(|e| format!("Date error: {e}"))?;
    let date = epoch
        .checked_add(time::Duration::days(days as i64))
        .ok_or_else(|| format!("Date overflow for epoch days: {days}"))?;
    let (year, month, day) = date.to_calendar_date();
    Ok(format!("{:04}-{:02}-{:02}", year, month as u8, day))
}

fn epoch_micros_to_rfc3339(micros: i64) -> Result<String, String> {
    let nanos = (micros as i128) * 1_000;
    let ts = time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(|e| format!("Timestamp overflow for micros {micros}: {e}"))?;
    ts.format(&time::format_description::well_known::Rfc3339)
        .map_err(|e| format!("Timestamp format error: {e}"))
}

fn decimal_i128_to_string(raw: i128, scale: u8) -> String {
    if scale == 0 {
        return raw.to_string();
    }

    let divisor = 10i128.pow(scale as u32);
    let is_negative = raw < 0;
    let abs = if is_negative {
        if raw == i128::MIN {
            // Practically unreachable for DECIMAL(38,9), but handle gracefully
            return format!("-{}", (raw as u128).wrapping_neg());
        }
        (-raw) as u128
    } else {
        raw as u128
    };

    let divisor_u = divisor as u128;
    let integer_part = abs / divisor_u;
    let fractional_part = abs % divisor_u;

    if fractional_part == 0 {
        if is_negative {
            format!("-{integer_part}")
        } else {
            format!("{integer_part}")
        }
    } else {
        let frac_str = format!("{:0>width$}", fractional_part, width = scale as usize);
        let frac_trimmed = frac_str.trim_end_matches('0');
        if is_negative {
            format!("-{integer_part}.{frac_trimmed}")
        } else {
            format!("{integer_part}.{frac_trimmed}")
        }
    }
}

// ─── Error setters ──────────────────────────────────────────────────────────

fn copy_callback_error_message(msg: &str) -> CString {
    let sanitized = msg.replace('\0', "\\0");
    CString::new(sanitized).unwrap_or_else(|_| c"spanner COPY callback failed".to_owned())
}

unsafe fn set_bind_error(info: ffi::duckdb_copy_function_bind_info, msg: &str) {
    let c_msg = copy_callback_error_message(msg);
    ffi::duckdb_copy_function_bind_set_error(info, c_msg.as_ptr());
}

unsafe fn set_global_init_error(info: ffi::duckdb_copy_function_global_init_info, msg: &str) {
    let c_msg = copy_callback_error_message(msg);
    ffi::duckdb_copy_function_global_init_set_error(info, c_msg.as_ptr());
}

unsafe fn set_sink_error(info: ffi::duckdb_copy_function_sink_info, msg: &str) {
    let c_msg = copy_callback_error_message(msg);
    ffi::duckdb_copy_function_sink_set_error(info, c_msg.as_ptr());
}

unsafe fn set_finalize_error(info: ffi::duckdb_copy_function_finalize_info, msg: &str) {
    let c_msg = copy_callback_error_message(msg);
    ffi::duckdb_copy_function_finalize_set_error(info, c_msg.as_ptr());
}

// ─── Memory management ─────────────────────────────────────────────────────

unsafe extern "C" fn drop_box<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut T));
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_spanner::types as spanner_types;

    #[test]
    fn copy_callback_error_message_sanitizes_nul() {
        let message = copy_callback_error_message("bind failed\0with context");
        assert_eq!(message.to_str().unwrap(), "bind failed\\0with context");
    }

    fn col(name: &str, is_generated: bool) -> schema::ColumnInfo {
        schema::ColumnInfo {
            name: name.to_string(),
            spanner_type: spanner_types::int64(),
            is_generated,
        }
    }

    fn unspecified_col(name: &str, is_generated: bool) -> schema::ColumnInfo {
        schema::ColumnInfo {
            name: name.to_string(),
            spanner_type: Type::default(),
            is_generated,
        }
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn options(values: &[(&str, &str)]) -> std::collections::HashMap<String, Vec<String>> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), vec![(*value).to_string()]))
            .collect()
    }

    #[test]
    fn positional_excludes_generated_columns() {
        // DDL order: Id, FullName (generated), Name. Writable = Id, Name.
        let schema = vec![col("Id", false), col("FullName", true), col("Name", false)];
        let resolved = resolve_copy_columns(&schema, None, 2, "T").unwrap();
        let got: Vec<&str> = resolved.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(got, vec!["Id", "Name"]);
        assert!(resolved
            .iter()
            .all(|column| column.spanner_type.code() != TypeCode::Unspecified));
    }

    #[test]
    fn positional_excludes_generated_unspecified_placeholder() {
        let schema = vec![
            col("Id", false),
            unspecified_col("SearchTokens", true),
            col("Name", false),
        ];
        let resolved = resolve_copy_columns(&schema, None, 2, "T").unwrap();

        assert_eq!(
            resolved
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Id", "Name"]
        );
        assert!(resolved
            .iter()
            .all(|column| column.spanner_type.code() != TypeCode::Unspecified));
    }

    #[test]
    fn rejects_writable_unspecified_placeholder() {
        let schema = vec![col("Id", false), unspecified_col("SearchTokens", false)];
        let err = resolve_copy_columns(&schema, None, 2, "T").unwrap_err();

        assert!(err.contains("Writable column 'SearchTokens'"), "{err}");
        assert!(err.contains("unsupported type"), "{err}");
    }

    #[test]
    fn positional_arity_mismatch_mentions_generated() {
        let schema = vec![col("Id", false), col("Gen", true), col("Name", false)];
        // Source provides 3 columns but only 2 are writable.
        let err = resolve_copy_columns(&schema, None, 3, "T").unwrap_err();
        assert!(err.contains("2 writable column(s)"), "{err}");
        assert!(err.contains("1 generated column(s) excluded"), "{err}");
    }

    #[test]
    fn positional_arity_mismatch_without_generated() {
        let schema = vec![col("Id", false), col("Name", false)];
        let err = resolve_copy_columns(&schema, None, 3, "T").unwrap_err();
        assert!(err.contains("2 writable column(s)"), "{err}");
        assert!(!err.contains("generated"), "{err}");
    }

    #[test]
    fn by_name_matches_case_insensitively_and_reorders() {
        // DDL order: Id, Name, Value. Source order differs and casing differs.
        let schema = vec![col("Id", false), col("Name", false), col("Value", false)];
        let src = names(&["value", "ID", "name"]);
        let resolved = resolve_copy_columns(&schema, Some(&src), 3, "T").unwrap();
        let got: Vec<&str> = resolved.iter().map(|c| c.name.as_str()).collect();
        // Result follows SOURCE order, using canonical target names.
        assert_eq!(got, vec!["Value", "Id", "Name"]);
    }

    #[test]
    fn explicit_columns_count_must_match_source_count() {
        let schema = vec![col("Id", false), col("Name", false)];
        let columns = names(&["Name"]);
        let err = resolve_copy_columns(&schema, Some(&columns), 2, "T").unwrap_err();
        assert!(err.contains("Column count mismatch"), "{err}");
        assert!(err.contains("columns has 1 name(s)"), "{err}");
        assert!(err.contains("source has 2 column(s)"), "{err}");
    }

    #[test]
    fn by_name_rejects_duplicate_source_names() {
        let schema = vec![col("Id", false), col("Name", false)];
        let src = names(&["Id", "Id"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("'Id' duplicates source column 'Id'"), "{err}");
        assert!(err.contains("must be unique"), "{err}");
    }

    #[test]
    fn by_name_rejects_case_insensitive_duplicate_source_names() {
        let schema = vec![col("Id", false), col("Name", false)];
        let src = names(&["Id", "id"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("'id' duplicates source column 'Id'"), "{err}");
    }

    #[test]
    fn by_name_unmatched_source_errors_with_available_columns() {
        let schema = vec![col("Id", false), col("Name", false)];
        let src = names(&["Id", "Missing"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("'Missing' has no matching column"), "{err}");
        // Available list contains the writable target columns.
        assert!(err.contains("Id, Name"), "{err}");
    }

    #[test]
    fn by_name_source_to_generated_column_errors() {
        let schema = vec![col("Id", false), unspecified_col("Gen", true)];
        let src = names(&["Id", "gen"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("maps to generated column 'Gen'"), "{err}");
    }

    #[test]
    fn by_name_excludes_generated_from_available_list() {
        let schema = vec![col("Id", false), col("Gen", true), col("Name", false)];
        let src = names(&["Id", "Nope"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        // Generated column must not appear in the "available target columns" hint.
        assert!(err.contains("Id, Name"), "{err}");
        assert!(!err.contains("Gen"), "{err}");
    }

    #[test]
    fn explicit_columns_rejects_empty_name() {
        let schema = vec![col("Id", false), col("Name", false)];
        let src = names(&["Id", ""]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("must not contain empty column names"), "{err}");
    }

    #[test]
    fn database_path_cannot_be_combined_with_component_options() {
        let opts = options(&[
            ("database_path", "projects/p/instances/i/databases/d"),
            ("project", "p"),
        ]);
        let err = resolve_copy_database_path(&opts, |_| None).unwrap_err();
        assert_eq!(
            err,
            "cannot combine database_path with project, instance, or database COPY options"
        );
    }

    #[test]
    fn batch_size_must_be_positive() {
        assert_eq!(parse_batch_size(None).unwrap(), DEFAULT_BATCH_SIZE);
        assert_eq!(parse_batch_size(Some("1")).unwrap(), 1);
        assert_eq!(
            parse_batch_size(Some("0")).unwrap_err(),
            "Invalid batch_size '0': must be greater than zero"
        );
    }

    #[test]
    fn batch_size_is_capped_before_allocation() {
        assert_eq!(
            parse_batch_size(Some(&MAX_BATCH_SIZE.to_string())).unwrap(),
            MAX_BATCH_SIZE
        );
        let value = (MAX_BATCH_SIZE + 1).to_string();
        let error = parse_batch_size(Some(&value)).unwrap_err();
        assert!(error.contains("must not exceed 80000 rows"), "{error}");
    }

    #[test]
    fn partial_progress_error_is_explicit() {
        assert_eq!(
            copy_failure_message("sink", "conversion failed", 42),
            "COPY sink failed: conversion failed; rows_written=42; earlier batches remain committed"
        );
    }

    #[test]
    fn emulator_retry_route_uses_sdk_for_environment_configuration() {
        assert_eq!(emulator_retry_route(None, true), EmulatorRetryRoute::Sdk);
        assert_eq!(
            emulator_retry_route(Some("localhost:9010"), true),
            EmulatorRetryRoute::Sdk
        );
        assert_eq!(
            emulator_retry_route(Some("localhost:9010"), false),
            EmulatorRetryRoute::ExplicitEndpointFallback
        );
        assert_eq!(emulator_retry_route(None, false), EmulatorRetryRoute::None);
    }

    #[test]
    fn copy_transaction_retry_policy_is_finite() {
        let policy = copy_transaction_retry_policy();
        assert_eq!(policy.max_attempts(), WRITE_TRANSACTION_MAX_ATTEMPTS);
        assert_eq!(policy.total_timeout(), WRITE_TRANSACTION_TOTAL_TIMEOUT);
    }

    #[test]
    fn ambiguous_write_error_reports_unknown_commit_outcome() {
        let err = SpannerClientError::service(
            Status::default()
                .set_code(Code::DeadlineExceeded)
                .set_message("deadline exceeded"),
        );
        assert!(is_ambiguous_write_error(&err));
        assert!(
            format_spanner_write_error(&err).contains("commit outcome is unknown"),
            "{}",
            format_spanner_write_error(&err)
        );

        let err = SpannerClientError::service(
            Status::default()
                .set_code(Code::AlreadyExists)
                .set_message("already exists"),
        );
        assert!(!is_ambiguous_write_error(&err));
        assert!(!format_spanner_write_error(&err).contains("unknown"));

        let err = SpannerClientError::timeout(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "commit response timed out",
        ));
        assert!(is_ambiguous_write_error(&err));
    }

    #[test]
    fn detects_emulator_internal_schema_error() {
        let err =
            SpannerClientError::service(Status::default().set_code(Code::Internal).set_message(
                "INTERNAL: Schema generation 0 was not registered with the Action Manager",
            ));
        assert!(is_internal_emulator_schema_error(&err));

        let err = SpannerClientError::service(
            Status::default()
                .set_code(Code::Internal)
                .set_message("different internal error"),
        );
        assert!(!is_internal_emulator_schema_error(&err));
    }
}
