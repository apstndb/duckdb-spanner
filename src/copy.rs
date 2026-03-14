//! Copy Function API (COPY TO) for writing data to Spanner.
//!
//! Usage:
//! ```sql
//! COPY my_table TO 'SpannerTable' (FORMAT spanner);
//! COPY my_table TO 'SpannerTable' (FORMAT spanner,
//!     database_path 'projects/p/instances/i/databases/d',
//!     mode 'insert_or_update',
//!     batch_size 500);
//! ```
//!
//! ## batch_size and mutation limits
//!
//! Each row produces one mutation per column. Spanner limits commits to
//! **80,000 mutations** (including secondary index entries). The default
//! `batch_size` of 1000 rows is conservative; for wide tables or tables
//! with many secondary indexes, consider lowering it.

use std::ffi::{c_void, CStr, CString};
use std::sync::Arc;

use duckdb::ffi;
use google_cloud_googleapis::spanner::admin::database::v1::DatabaseDialect;
use google_cloud_googleapis::spanner::v1::mutation;
use google_cloud_googleapis::spanner::v1::Mutation;
use google_cloud_spanner::client::Client;
use prost_types::value::Kind;
use prost_types::{ListValue, Value};

use crate::client;
use crate::config;
use crate::runtime;
use crate::schema;

const DEFAULT_BATCH_SIZE: usize = 1000;

// ─── Types ──────────────────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum MutationMode {
    Insert,
    Update,
    InsertOrUpdate,
    Replace,
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
}

/// Bind-phase data stored for the lifetime of the COPY operation.
struct CopyBindData {
    database_path: String,
    endpoint: Option<String>,
    mode: MutationMode,
    batch_size: usize,
    columns: Vec<ColumnMeta>,
}

/// Global state created during init and shared across sink calls.
struct CopyGlobalState {
    client: Arc<Client>,
    table_name: String,
    column_names: Vec<String>,
    mode: MutationMode,
    batch_size: usize,
    columns: Vec<ColumnMeta>,
    buffer: Vec<ListValue>,
    rows_written: u64,
}

// ─── Registration ───────────────────────────────────────────────────────────

/// Register the `spanner` copy function on a raw DuckDB connection.
///
/// # Safety
/// `con` must be a valid `duckdb_connection`.
pub unsafe fn register_copy_function(con: ffi::duckdb_connection) {
    unsafe {
        let copy_fn = ffi::duckdb_create_copy_function();

        let name = c"spanner";
        ffi::duckdb_copy_function_set_name(copy_fn, name.as_ptr());
        ffi::duckdb_copy_function_set_bind(copy_fn, Some(copy_bind));
        ffi::duckdb_copy_function_set_global_init(copy_fn, Some(copy_global_init));
        ffi::duckdb_copy_function_set_sink(copy_fn, Some(copy_sink));
        ffi::duckdb_copy_function_set_finalize(copy_fn, Some(copy_finalize));

        let rc = ffi::duckdb_register_copy_function(con, copy_fn);
        if rc != ffi::DuckDBSuccess {
            eprintln!("[duckdb-spanner] Failed to register copy function");
        }
        ffi::duckdb_destroy_copy_function(&mut { copy_fn });
    }
}

// ─── Callbacks ──────────────────────────────────────────────────────────────

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
        Err(_) => unsafe { set_sink_error(info, "panic in spanner copy sink") },
    }
}

unsafe extern "C" fn copy_finalize(info: ffi::duckdb_copy_function_finalize_info) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        copy_finalize_inner(info)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => unsafe { set_finalize_error(info, &e) },
        Err(_) => unsafe { set_finalize_error(info, "panic in spanner copy finalize") },
    }
}

// ─── Inner implementations ──────────────────────────────────────────────────

unsafe fn copy_bind_inner(
    info: ffi::duckdb_copy_function_bind_info,
) -> Result<(), String> {
    // Parse COPY options
    let options_val = ffi::duckdb_copy_function_bind_get_options(info);
    let opts = extract_options(options_val);
    ffi::duckdb_destroy_value(&mut { options_val });

    // Get client context for config fallback
    let ctx = ffi::duckdb_copy_function_bind_get_client_context(info);
    let have_ctx = !ctx.is_null();

    let cfg = |name: &str| -> Option<String> {
        if have_ctx {
            unsafe { config::get_config_string_from_context(ctx, name) }
        } else {
            None
        }
    };

    // Resolve database path: options → component parts → config
    let database_path = resolve_copy_database_path(&opts, &cfg)?;

    let endpoint = opts
        .get("endpoint")
        .cloned()
        .or_else(|| cfg("spanner_endpoint"));

    if have_ctx {
        ffi::duckdb_destroy_client_context(&mut { ctx });
    }

    let mode = match opts.get("mode") {
        Some(m) => MutationMode::parse(m)?,
        None => MutationMode::InsertOrUpdate,
    };

    let batch_size = match opts.get("batch_size") {
        Some(s) => s
            .parse::<usize>()
            .map_err(|e| format!("Invalid batch_size: {e}"))?,
        None => DEFAULT_BATCH_SIZE,
    };

    // Collect source column type metadata
    let column_count = ffi::duckdb_copy_function_bind_get_column_count(info) as usize;
    let mut columns = Vec::with_capacity(column_count);

    for i in 0..column_count {
        let logical_type = ffi::duckdb_copy_function_bind_get_column_type(info, i as u64);
        let type_id = ffi::duckdb_get_type_id(logical_type);

        let (decimal_scale, decimal_internal_type) =
            if type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL {
                (
                    ffi::duckdb_decimal_scale(logical_type),
                    ffi::duckdb_decimal_internal_type(logical_type),
                )
            } else {
                (0, 0)
            };

        ffi::duckdb_destroy_logical_type(&mut { logical_type });

        columns.push(ColumnMeta {
            type_id,
            decimal_scale,
            decimal_internal_type,
        });
    }

    let data = Box::new(CopyBindData {
        database_path,
        endpoint,
        mode,
        batch_size,
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

    // Connect to Spanner
    let client = runtime::block_on(client::get_or_create_client(
        &bind_data.database_path,
        bind_data.endpoint.as_deref(),
    ))
    .map_err(|e| format!("Runtime error: {e}"))?
    .map_err(|e| format!("Failed to connect to Spanner: {e}"))?;

    // Discover table schema (auto-detect dialect)
    let schema_columns = runtime::block_on(schema::discover_table_schema(
        &client,
        &table_name,
        DatabaseDialect::Unspecified,
    ))
    .map_err(|e| format!("Runtime error: {e}"))?
    .map_err(|e| format!("Schema discovery failed for table '{table_name}': {e}"))?;

    // Validate column count matches
    if bind_data.columns.len() != schema_columns.len() {
        return Err(format!(
            "Column count mismatch: source has {} columns, Spanner table '{}' has {} columns",
            bind_data.columns.len(),
            table_name,
            schema_columns.len()
        ));
    }

    let column_names: Vec<String> = schema_columns.iter().map(|c| c.name.clone()).collect();

    let state = Box::new(CopyGlobalState {
        client,
        table_name,
        column_names,
        mode: bind_data.mode,
        batch_size: bind_data.batch_size,
        columns: bind_data.columns.clone(),
        buffer: Vec::with_capacity(bind_data.batch_size),
        rows_written: 0,
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
        return Err("Global state is null".to_string());
    }
    let state = &mut *(state_ptr as *mut CopyGlobalState);

    let row_count = ffi::duckdb_data_chunk_get_size(chunk) as usize;
    if row_count == 0 {
        return Ok(());
    }

    let col_count = state.columns.len();

    // Pre-fetch vector data/validity pointers (these don't change within a chunk)
    let vectors: Vec<ffi::duckdb_vector> = (0..col_count)
        .map(|i| ffi::duckdb_data_chunk_get_vector(chunk, i as u64))
        .collect();
    let data_ptrs: Vec<*mut c_void> = vectors.iter().map(|v| ffi::duckdb_vector_get_data(*v)).collect();
    let validity_ptrs: Vec<*mut u64> =
        vectors.iter().map(|v| ffi::duckdb_vector_get_validity(*v)).collect();

    for row_idx in 0..row_count {
        let mut values = Vec::with_capacity(col_count);
        for col_idx in 0..col_count {
            let val = read_duckdb_value(
                data_ptrs[col_idx],
                validity_ptrs[col_idx],
                row_idx,
                &state.columns[col_idx],
            )?;
            values.push(val);
        }
        state.buffer.push(ListValue { values });

        if state.buffer.len() >= state.batch_size {
            flush_buffer(state)?;
        }
    }

    Ok(())
}

unsafe fn copy_finalize_inner(
    info: ffi::duckdb_copy_function_finalize_info,
) -> Result<(), String> {
    let state_ptr = ffi::duckdb_copy_function_finalize_get_global_state(info);
    if state_ptr.is_null() {
        return Ok(());
    }
    let state = &mut *(state_ptr as *mut CopyGlobalState);

    flush_buffer(state)?;

    eprintln!(
        "[duckdb-spanner] COPY TO '{}': {} rows written",
        state.table_name, state.rows_written
    );

    Ok(())
}

// ─── Option extraction ──────────────────────────────────────────────────────

/// Extract options from the COPY statement's options value.
///
/// DuckDB returns options as a STRUCT where each field is a LIST of values.
/// We take the first element of each list as a string.
unsafe fn extract_options(
    options_val: ffi::duckdb_value,
) -> std::collections::HashMap<String, String> {
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
            let name_ptr = ffi::duckdb_struct_type_child_name(options_type, i);
            if name_ptr.is_null() {
                continue;
            }
            let name = CStr::from_ptr(name_ptr).to_string_lossy().to_lowercase();
            ffi::duckdb_free(name_ptr as *mut _);

            // Skip 'format' — already consumed by DuckDB
            if name == "format" {
                continue;
            }

            let child_val = ffi::duckdb_get_struct_child(options_val, i);
            if !child_val.is_null() && !ffi::duckdb_is_null_value(child_val) {
                // Child may be a LIST; try to get the first element.
                // NOTE: duckdb_get_value_type returns an internal reference — do NOT destroy it.
                let child_type = ffi::duckdb_get_value_type(child_val);
                let child_type_id = ffi::duckdb_get_type_id(child_type);

                if child_type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST {
                    let list_size = ffi::duckdb_get_list_size(child_val);
                    if list_size > 0 {
                        let elem = ffi::duckdb_get_list_child(child_val, 0);
                        if let Some(s) = value_to_string(elem) {
                            opts.insert(name, s);
                        }
                        ffi::duckdb_destroy_value(&mut { elem });
                    }
                } else if let Some(s) = value_to_string(child_val) {
                    opts.insert(name, s);
                }
            }
            ffi::duckdb_destroy_value(&mut { child_val });
        }
    } else if type_id == ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP {
        let map_size = ffi::duckdb_get_map_size(options_val);
        for i in 0..map_size {
            let key = ffi::duckdb_get_map_key(options_val, i);
            let val = ffi::duckdb_get_map_value(options_val, i);
            if let (Some(k), Some(v)) = (value_to_string(key), value_to_string(val)) {
                if k.to_lowercase() != "format" {
                    opts.insert(k.to_lowercase(), v);
                }
            }
            ffi::duckdb_destroy_value(&mut { key });
            ffi::duckdb_destroy_value(&mut { val });
        }
    }

    // NOTE: options_type is an internal reference from duckdb_get_value_type — do NOT destroy it.
    opts
}

/// Extract a string from a `duckdb_value`.
unsafe fn value_to_string(val: ffi::duckdb_value) -> Option<String> {
    if val.is_null() || ffi::duckdb_is_null_value(val) {
        return None;
    }
    let c_str = ffi::duckdb_get_varchar(val);
    if c_str.is_null() {
        return None;
    }
    let s = CStr::from_ptr(c_str).to_string_lossy().into_owned();
    ffi::duckdb_free(c_str as *mut _);
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Resolve the Spanner database resource path from COPY options and config.
///
/// Resolution order (matching `bind_utils::resolve_database_path`):
/// 1. project/instance/database (option → config) — if any is present, all three required
/// 2. database_path option
/// 3. spanner_database_path config
/// 4. error
fn resolve_copy_database_path(
    opts: &std::collections::HashMap<String, String>,
    cfg: impl Fn(&str) -> Option<String>,
) -> Result<String, String> {
    let project = opts
        .get("project")
        .cloned()
        .or_else(|| cfg("spanner_project"));
    let instance = opts
        .get("instance")
        .cloned()
        .or_else(|| cfg("spanner_instance"));
    let database = opts
        .get("database")
        .cloned()
        .or_else(|| cfg("spanner_database"));

    if project.is_some() || instance.is_some() || database.is_some() {
        let p = project.ok_or(
            "project is required when instance or database is specified. \
             Use project option or SET spanner_project = '...'")?;
        let i = instance.ok_or(
            "instance is required when project or database is specified. \
             Use instance option or SET spanner_instance = '...'")?;
        let d = database.ok_or(
            "database is required when project or instance is specified. \
             Use database option or SET spanner_database = '...'")?;
        return Ok(format!("projects/{p}/instances/{i}/databases/{d}"));
    }

    if let Some(path) = opts.get("database_path").cloned() {
        return Ok(path);
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

fn flush_buffer(state: &mut CopyGlobalState) -> Result<(), String> {
    if state.buffer.is_empty() {
        return Ok(());
    }

    let rows = std::mem::take(&mut state.buffer);
    let count = rows.len();
    let mutation = build_mutation(state.mode, &state.table_name, &state.column_names, rows);

    let result = runtime::block_on(state.client.apply(vec![mutation]))
        .map_err(|e| format!("Runtime error: {e}"))?;
    result.map_err(|e| format!("Spanner apply error: {e}"))?;

    state.rows_written += count as u64;
    Ok(())
}

fn build_mutation(
    mode: MutationMode,
    table: &str,
    columns: &[String],
    rows: Vec<ListValue>,
) -> Mutation {
    let write = mutation::Write {
        table: table.to_string(),
        columns: columns.to_vec(),
        values: rows,
    };

    let operation = match mode {
        MutationMode::Insert => mutation::Operation::Insert(write),
        MutationMode::Update => mutation::Operation::Update(write),
        MutationMode::InsertOrUpdate => mutation::Operation::InsertOrUpdate(write),
        MutationMode::Replace => mutation::Operation::Replace(write),
    };

    Mutation {
        operation: Some(operation),
    }
}

// ─── DuckDB vector → Spanner proto Value conversion ─────────────────────────

/// Read a value from a DuckDB vector at the given row and convert to Spanner proto Value.
///
/// # Safety
/// - `data` must point to valid vector data of the appropriate type
/// - `validity` may be null (meaning all rows are valid)
/// - `row_idx` must be within the chunk's row count
unsafe fn read_duckdb_value(
    data: *mut c_void,
    validity: *mut u64,
    row_idx: usize,
    col: &ColumnMeta,
) -> Result<Value, String> {
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
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP
        | ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => {
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
            Kind::StringValue(decimal_i128_to_string(raw_i128, col.decimal_scale))
        }
        ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID => {
            let raw = *data.cast::<u128>().add(row_idx);
            // Reverse the MSB flip DuckDB applies for sort ordering
            let uuid_bits = raw ^ (1u128 << 127);
            Kind::StringValue(uuid::Uuid::from_u128(uuid_bits).to_string())
        }
        _ => {
            return Err(format!(
                "Unsupported DuckDB type {} for COPY TO spanner",
                col.type_id
            ));
        }
    };

    Ok(Value {
        kind: Some(kind),
    })
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
    let bytes = std::slice::from_raw_parts(data as *const u8, len);
    String::from_utf8_lossy(bytes).into_owned()
}

unsafe fn read_duckdb_bytes(str_ptr: *mut ffi::duckdb_string_t) -> Vec<u8> {
    let len = ffi::duckdb_string_t_length(*str_ptr) as usize;
    let data = ffi::duckdb_string_t_data(str_ptr);
    std::slice::from_raw_parts(data as *const u8, len).to_vec()
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

unsafe fn set_bind_error(info: ffi::duckdb_copy_function_bind_info, msg: &str) {
    if let Ok(c_msg) = CString::new(msg) {
        ffi::duckdb_copy_function_bind_set_error(info, c_msg.as_ptr());
    }
}

unsafe fn set_global_init_error(info: ffi::duckdb_copy_function_global_init_info, msg: &str) {
    if let Ok(c_msg) = CString::new(msg) {
        ffi::duckdb_copy_function_global_init_set_error(info, c_msg.as_ptr());
    }
}

unsafe fn set_sink_error(info: ffi::duckdb_copy_function_sink_info, msg: &str) {
    if let Ok(c_msg) = CString::new(msg) {
        ffi::duckdb_copy_function_sink_set_error(info, c_msg.as_ptr());
    }
}

unsafe fn set_finalize_error(info: ffi::duckdb_copy_function_finalize_info, msg: &str) {
    if let Ok(c_msg) = CString::new(msg) {
        ffi::duckdb_copy_function_finalize_set_error(info, c_msg.as_ptr());
    }
}

// ─── Memory management ─────────────────────────────────────────────────────

unsafe extern "C" fn drop_box<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        drop(Box::from_raw(ptr as *mut T));
    }
}
