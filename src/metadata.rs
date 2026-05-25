//! C API table functions for Spanner metadata.

use std::ffi::{c_void, CStr, CString};

use duckdb::ffi;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;

use crate::{client, config, runtime};

#[derive(Clone)]
struct SpannerTableRow {
    table_schema: String,
    table_name: String,
    table_type: String,
}

struct SpannerTablesBindData {
    rows: Vec<SpannerTableRow>,
}

struct SpannerTablesInitData {
    offset: usize,
}

/// Register C API metadata table functions on a raw DuckDB connection.
///
/// # Safety
/// `con` must be a valid `duckdb_connection`.
pub unsafe fn register_metadata_table_functions(con: ffi::duckdb_connection) {
    unsafe {
        register_spanner_tables(con);
    }
}

unsafe fn register_spanner_tables(con: ffi::duckdb_connection) {
    let table_fn = ffi::duckdb_create_table_function();

    let name = c"spanner_tables";
    ffi::duckdb_table_function_set_name(table_fn, name.as_ptr());

    add_varchar_named_parameter(table_fn, "database_path");
    add_varchar_named_parameter(table_fn, "project");
    add_varchar_named_parameter(table_fn, "instance");
    add_varchar_named_parameter(table_fn, "database");
    add_varchar_named_parameter(table_fn, "endpoint");

    ffi::duckdb_table_function_set_bind(table_fn, Some(spanner_tables_bind));
    ffi::duckdb_table_function_set_init(table_fn, Some(spanner_tables_init));
    ffi::duckdb_table_function_set_function(table_fn, Some(spanner_tables_func));

    let rc = ffi::duckdb_register_table_function(con, table_fn);
    if rc != ffi::DuckDBSuccess {
        eprintln!("[duckdb-spanner] Failed to register spanner_tables table function");
    }
    ffi::duckdb_destroy_table_function(&mut { table_fn });
}

unsafe fn add_varchar_named_parameter(table_fn: ffi::duckdb_table_function, name: &str) {
    let c_name = CString::new(name).unwrap();
    let ty = ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
    ffi::duckdb_table_function_add_named_parameter(table_fn, c_name.as_ptr(), ty);
    ffi::duckdb_destroy_logical_type(&mut { ty });
}

unsafe extern "C" fn spanner_tables_bind(info: ffi::duckdb_bind_info) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        spanner_tables_bind_inner(info)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => unsafe { set_bind_error(info, &e) },
        Err(_) => unsafe { set_bind_error(info, "panic in spanner_tables bind") },
    }
}

unsafe fn spanner_tables_bind_inner(info: ffi::duckdb_bind_info) -> Result<(), String> {
    add_result_varchar_column(info, "table_schema");
    add_result_varchar_column(info, "table_name");
    add_result_varchar_column(info, "table_type");

    let mut ctx: ffi::duckdb_client_context = std::ptr::null_mut();
    ffi::duckdb_table_function_get_client_context(info, &mut ctx);
    let cfg = |name: &str| -> Option<String> {
        if ctx.is_null() {
            None
        } else {
            unsafe { config::get_config_string_from_context(ctx, name) }
        }
    };

    let database_path = resolve_database_path(info, &cfg);
    let endpoint = get_named_string(info, "endpoint").or_else(|| cfg("spanner_endpoint"));

    if !ctx.is_null() {
        ffi::duckdb_destroy_client_context(&mut ctx);
    }
    let database_path = database_path?;

    let rows = runtime::block_on(async {
        let client = client::get_or_create_client(&database_path, endpoint.as_deref()).await?;
        list_tables(&client).await
    })
    .map_err(|e| format!("Runtime error: {e}"))?
    .map_err(|e| format!("Failed to list Spanner tables: {e}"))?;

    ffi::duckdb_bind_set_cardinality(info, rows.len() as u64, true);
    let data = Box::new(SpannerTablesBindData { rows });
    ffi::duckdb_bind_set_bind_data(
        info,
        Box::into_raw(data) as *mut c_void,
        Some(drop_box::<SpannerTablesBindData>),
    );
    Ok(())
}

fn add_result_varchar_column(info: ffi::duckdb_bind_info, name: &str) {
    unsafe {
        let c_name = CString::new(name).unwrap();
        let ty = ffi::duckdb_create_logical_type(ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR);
        ffi::duckdb_bind_add_result_column(info, c_name.as_ptr(), ty);
        ffi::duckdb_destroy_logical_type(&mut { ty });
    }
}

async fn list_tables(client: &Client) -> Result<Vec<SpannerTableRow>, crate::error::SpannerError> {
    let mut tx = client.single().await?;
    let stmt = Statement::new(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
         FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_TYPE = 'BASE TABLE' \
         ORDER BY TABLE_SCHEMA, TABLE_NAME",
    );
    let mut iter = tx
        .query(stmt)
        .await
        .map_err(crate::error::SpannerError::Grpc)?;

    let mut rows = Vec::new();
    while let Some(row) = iter
        .next()
        .await
        .map_err(crate::error::SpannerError::Grpc)?
    {
        rows.push(SpannerTableRow {
            table_schema: row.column::<String>(0)?,
            table_name: row.column::<String>(1)?,
            table_type: row.column::<String>(2)?,
        });
    }
    Ok(rows)
}

fn resolve_database_path(
    info: ffi::duckdb_bind_info,
    cfg: &impl Fn(&str) -> Option<String>,
) -> Result<String, String> {
    let project_arg = get_named_string(info, "project");
    let instance_arg = get_named_string(info, "instance");
    let database_arg = get_named_string(info, "database");

    if project_arg.is_some() || instance_arg.is_some() || database_arg.is_some() {
        let project = project_arg.or_else(|| cfg("spanner_project"));
        let instance = instance_arg.or_else(|| cfg("spanner_instance"));
        let database = database_arg.or_else(|| cfg("spanner_database"));
        let p = project.ok_or(
            "project is required when instance or database is specified. \
             Use project := '...' or SET spanner_project = '...'",
        )?;
        let i = instance.ok_or(
            "instance is required when project or database is specified. \
             Use instance := '...' or SET spanner_instance = '...'",
        )?;
        let d = database.ok_or(
            "database is required when project or instance is specified. \
             Use database := '...' or SET spanner_database = '...'",
        )?;
        return Ok(format!("projects/{p}/instances/{i}/databases/{d}"));
    }

    if let Some(path) = get_named_string(info, "database_path") {
        return Ok(path);
    }

    let project = cfg("spanner_project");
    let instance = cfg("spanner_instance");
    let database = cfg("spanner_database");

    if project.is_some() || instance.is_some() || database.is_some() {
        let p = project.ok_or(
            "project is required when instance or database is specified. \
             Use project := '...' or SET spanner_project = '...'",
        )?;
        let i = instance.ok_or(
            "instance is required when project or database is specified. \
             Use instance := '...' or SET spanner_instance = '...'",
        )?;
        let d = database.ok_or(
            "database is required when project or instance is specified. \
             Use database := '...' or SET spanner_database = '...'",
        )?;
        return Ok(format!("projects/{p}/instances/{i}/databases/{d}"));
    }

    if let Some(path) = cfg("spanner_database_path") {
        return Ok(path);
    }

    Err("No database specified. Use database_path := '...', project/instance/database named parameters, or SET spanner_database_path".to_string())
}

fn get_named_string(info: ffi::duckdb_bind_info, name: &str) -> Option<String> {
    unsafe {
        let c_name = CString::new(name).ok()?;
        let value = ffi::duckdb_bind_get_named_parameter(info, c_name.as_ptr());
        let result = duckdb_value_to_string(value);
        if !value.is_null() {
            ffi::duckdb_destroy_value(&mut { value });
        }
        result
    }
}

unsafe fn duckdb_value_to_string(value: ffi::duckdb_value) -> Option<String> {
    if value.is_null() || ffi::duckdb_is_null_value(value) {
        return None;
    }

    let c_str = ffi::duckdb_get_varchar(value);
    if c_str.is_null() {
        return None;
    }

    let value = CStr::from_ptr(c_str).to_string_lossy().into_owned();
    ffi::duckdb_free(c_str as *mut _);
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

unsafe extern "C" fn spanner_tables_init(info: ffi::duckdb_init_info) {
    let data = Box::new(SpannerTablesInitData { offset: 0 });
    unsafe {
        ffi::duckdb_init_set_init_data(
            info,
            Box::into_raw(data) as *mut c_void,
            Some(drop_box::<SpannerTablesInitData>),
        );
    }
}

unsafe extern "C" fn spanner_tables_func(
    info: ffi::duckdb_function_info,
    output: ffi::duckdb_data_chunk,
) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
        spanner_tables_func_inner(info, output)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(e)) => unsafe { set_function_error(info, &e) },
        Err(_) => unsafe { set_function_error(info, "panic in spanner_tables function") },
    }
}

unsafe fn spanner_tables_func_inner(
    info: ffi::duckdb_function_info,
    output: ffi::duckdb_data_chunk,
) -> Result<(), String> {
    let bind_ptr = ffi::duckdb_function_get_bind_data(info);
    if bind_ptr.is_null() {
        return Err("spanner_tables bind data is null".to_string());
    }
    let init_ptr = ffi::duckdb_function_get_init_data(info);
    if init_ptr.is_null() {
        return Err("spanner_tables init data is null".to_string());
    }

    let bind_data = &*(bind_ptr as *const SpannerTablesBindData);
    let init_data = &mut *(init_ptr as *mut SpannerTablesInitData);
    if init_data.offset >= bind_data.rows.len() {
        ffi::duckdb_data_chunk_set_size(output, 0);
        return Ok(());
    }

    let capacity = ffi::duckdb_vector_size() as usize;
    let end = (init_data.offset + capacity).min(bind_data.rows.len());
    let count = end - init_data.offset;

    let schema_vec = ffi::duckdb_data_chunk_get_vector(output, 0);
    let name_vec = ffi::duckdb_data_chunk_get_vector(output, 1);
    let type_vec = ffi::duckdb_data_chunk_get_vector(output, 2);

    for (out_idx, row) in bind_data.rows[init_data.offset..end].iter().enumerate() {
        assign_string(schema_vec, out_idx, &row.table_schema);
        assign_string(name_vec, out_idx, &row.table_name);
        assign_string(type_vec, out_idx, &row.table_type);
    }

    init_data.offset = end;
    ffi::duckdb_data_chunk_set_size(output, count as u64);
    Ok(())
}

unsafe fn assign_string(vector: ffi::duckdb_vector, row_idx: usize, value: &str) {
    ffi::duckdb_vector_assign_string_element_len(
        vector,
        row_idx as u64,
        value.as_ptr().cast(),
        value.len() as u64,
    );
}

unsafe fn set_bind_error(info: ffi::duckdb_bind_info, msg: &str) {
    if let Ok(c_msg) = CString::new(msg) {
        ffi::duckdb_bind_set_error(info, c_msg.as_ptr());
    }
}

unsafe fn set_function_error(info: ffi::duckdb_function_info, msg: &str) {
    if let Ok(c_msg) = CString::new(msg) {
        ffi::duckdb_function_set_error(info, c_msg.as_ptr());
    }
}

unsafe extern "C" fn drop_box<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        unsafe {
            drop(Box::from_raw(ptr as *mut T));
        }
    }
}
