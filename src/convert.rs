use duckdb::core::{DataChunkHandle, FlatVector, Inserter, ListVector, StructVector};
use duckdb::ffi;
use google_cloud_spanner::model;
use google_cloud_spanner::result::Row;
use google_cloud_spanner::types::{Type, TypeCode};
use google_cloud_spanner::value::{Kind, Value as SpannerValue};
use time::OffsetDateTime;

use crate::error::SpannerError;
use crate::schema::ColumnInfo;
use crate::types::is_pg_numeric;

const EPOCH_DATE: time::Date = time::macros::date!(1970 - 01 - 01);

// DuckDB 1.5.0 caps a single vector allocation at 128 GiB. Result conversion
// can write any supported physical type, whose widest inline representation is
// 16 bytes (including LIST entries, VARCHAR, INTERVAL, and HUGEINT). Keeping the
// element count within this conservative bound prevents the C++ reserve path
// from throwing across the C API boundary.
const DUCKDB_MAX_VECTOR_BYTES: u64 = 1 << 37;
const DUCKDB_MAX_RESULT_CHILDREN: u64 =
    DUCKDB_MAX_VECTOR_BYTES / std::mem::size_of::<ffi::duckdb_list_entry>() as u64;

/// Extract the raw `duckdb_vector` handle from a FlatVector.
///
/// duckdb-rs 1.10504.0 does not expose the underlying `duckdb_vector` handle,
/// which is needed for `duckdb_unsafe_vector_assign_string_element_len`.
/// `FlatVector<'_>` currently stores `ptr` first, followed by capacity and the
/// lifetime marker. We read the first pointer-sized value and debug-check it
/// against the public data-pointer accessor.
unsafe fn flat_vector_raw(vector: &FlatVector<'_>) -> ffi::duckdb_vector {
    let candidate = *(vector as *const _ as *const ffi::duckdb_vector);
    debug_assert_eq!(
        ffi::duckdb_vector_get_data(candidate) as usize,
        vector.as_mut_ptr::<u8>() as usize,
        "FlatVector field layout assumption violated — ptr is not at offset 0"
    );
    candidate
}

/// Extract the raw `duckdb_vector` handle from a ListVector.
///
/// duckdb-rs 1.10504.0 does not expose a capacity-aware nested-list accessor.
/// `ListVector<'_>` stores its entries `FlatVector` first, whose raw pointer is
/// also its first field. This is the same pinned layout dependency used by
/// [`flat_vector_raw`].
unsafe fn list_vector_raw(vector: &ListVector<'_>) -> ffi::duckdb_vector {
    let candidate = *(vector as *const _ as *const ffi::duckdb_vector);
    debug_assert_eq!(
        ffi::duckdb_list_vector_get_size(candidate) as usize,
        vector.len(),
        "ListVector field layout assumption violated - entries are not at offset 0"
    );
    candidate
}

/// Assign a string to a FlatVector without UTF-8 validation.
///
/// Spanner guarantees that STRING and JSON values are valid UTF-8, so we can
/// skip the validation that `duckdb_vector_assign_string_element_len` performs.
/// This uses `duckdb_unsafe_vector_assign_string_element_len` from the v1.5.0 C API.
///
/// # Safety
/// - `idx` must be within the vector's allocated capacity
/// - `idx` and `s.len()` must be representable by DuckDB's `idx_t`
/// - The caller must guarantee the input bytes are valid UTF-8 (Spanner does this)
unsafe fn unsafe_assign_string(vector: &mut FlatVector<'_>, idx: usize, s: &str) {
    let raw_vector = flat_vector_raw(vector);
    ffi::duckdb_unsafe_vector_assign_string_element_len(
        raw_vector,
        idx as u64,
        s.as_ptr() as *const _,
        s.len() as u64,
    );
}

fn checked_duckdb_index(
    value: usize,
    description: &str,
    context: &dyn Fn() -> String,
) -> Result<ffi::idx_t, SpannerError> {
    ffi::idx_t::try_from(value).map_err(|_| {
        SpannerError::Conversion(format!(
            "{description} {value} is not representable by DuckDB idx_t ({})",
            context()
        ))
    })
}

fn checked_child_count(
    current: usize,
    additional: usize,
    context: &dyn Fn() -> String,
) -> Result<usize, SpannerError> {
    let total = current.checked_add(additional).ok_or_else(|| {
        SpannerError::Conversion(format!("nested child count overflow ({})", context()))
    })?;
    let total_idx = checked_duckdb_index(total, "nested child count", context)?;
    if total_idx > DUCKDB_MAX_RESULT_CHILDREN {
        return Err(SpannerError::Conversion(format!(
            "nested child count {total} exceeds DuckDB's safe result-vector capacity of {DUCKDB_MAX_RESULT_CHILDREN} ({})",
            context()
        )));
    }
    Ok(total)
}

fn ensure_vector_index(
    idx: usize,
    capacity: usize,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    checked_duckdb_index(idx, "vector index", context)?;
    if idx >= capacity {
        return Err(SpannerError::Conversion(format!(
            "vector index {idx} exceeds allocated capacity {capacity} ({})",
            context()
        )));
    }
    Ok(())
}

fn ensure_row_count(row_count: usize, context: &dyn Fn() -> String) -> Result<(), SpannerError> {
    checked_duckdb_index(row_count, "row count", context)?;
    let capacity = rows_capacity_hint();
    if row_count > capacity {
        return Err(SpannerError::Conversion(format!(
            "row count {row_count} exceeds DuckDB vector capacity {capacity} ({})",
            context()
        )));
    }
    Ok(())
}

fn set_list_entry_checked(
    list_vector: &mut ListVector<'_>,
    row_capacity: usize,
    row_idx: usize,
    offset: usize,
    length: usize,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    ensure_vector_index(row_idx, row_capacity, context)?;
    let offset_idx = checked_duckdb_index(offset, "list child offset", context)?;
    let length_idx = checked_duckdb_index(length, "list entry length", context)?;
    checked_child_count(offset, length, context)?;

    let raw_vector = unsafe { list_vector_raw(list_vector) };
    let data = unsafe { ffi::duckdb_vector_get_data(raw_vector) };
    if data.is_null() {
        return Err(SpannerError::Conversion(format!(
            "DuckDB returned a null LIST entry buffer ({})",
            context()
        )));
    }
    let entries = unsafe {
        std::slice::from_raw_parts_mut(data.cast::<ffi::duckdb_list_entry>(), row_capacity)
    };
    entries[row_idx] = ffi::duckdb_list_entry {
        offset: offset_idx,
        length: length_idx,
    };
    Ok(())
}

fn reserve_list_child(
    list_vector: &ListVector<'_>,
    required_capacity: usize,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    checked_child_count(0, required_capacity, context)?;
    let required_capacity =
        checked_duckdb_index(required_capacity, "required list child capacity", context)?;

    let state =
        unsafe { ffi::duckdb_list_vector_reserve(list_vector_raw(list_vector), required_capacity) };
    if state != ffi::DuckDBSuccess {
        return Err(SpannerError::Conversion(format!(
            "DuckDB failed to reserve {required_capacity} list child values ({})",
            context()
        )));
    }
    Ok(())
}

fn set_list_len_after_reserve(
    list_vector: &ListVector<'_>,
    new_len: usize,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    reserve_list_child(list_vector, new_len, context)?;
    let new_len = checked_duckdb_index(new_len, "list child length", context)?;
    let state = unsafe { ffi::duckdb_list_vector_set_size(list_vector_raw(list_vector), new_len) };
    if state != ffi::DuckDBSuccess {
        return Err(SpannerError::Conversion(format!(
            "DuckDB failed to set list child length to {new_len} ({})",
            context()
        )));
    }
    Ok(())
}

/// Human-readable name for an official Spanner `Value` kind, for error messages.
fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Null => "Null",
        Kind::Number => "Number",
        Kind::String => "String",
        Kind::Bool => "Bool",
        Kind::Struct => "Struct",
        Kind::List => "List",
    }
}

/// Build a `Conversion` error describing a proto `Kind` that does not match the
/// expected Spanner/DuckDB type, including where (column/field/row) it happened.
///
/// Note: `Kind::NullValue` must never reach this helper — it is a legitimate NULL,
/// handled separately by callers before they classify a value as a mismatch.
fn type_mismatch_error(expected: &str, kind: Kind, context: &str) -> SpannerError {
    SpannerError::Conversion(format!(
        "expected {expected} but got Spanner Value::{} ({context})",
        kind_name(kind)
    ))
}

fn row_value<'a>(
    row: &'a Row,
    spanner_col_idx: usize,
    column_name: &str,
    row_idx: usize,
) -> Result<&'a SpannerValue, SpannerError> {
    row.raw_values().get(spanner_col_idx).ok_or_else(|| {
        SpannerError::Conversion(format!(
            "column '{column_name}', row {row_idx}: Spanner column index {spanner_col_idx} out of range"
        ))
    })
}

/// Write a batch of Spanner Rows into a DuckDB DataChunkHandle.
/// All columns are written at matching indices (col 0 → output col 0, etc.).
pub fn write_rows_to_chunk(
    output: &mut DataChunkHandle,
    rows: &[Row],
    columns: &[ColumnInfo],
) -> Result<(), SpannerError> {
    if rows.is_empty() {
        output.set_len(0);
        return Ok(());
    }
    ensure_row_count(rows.len(), &|| "result batch".to_string())?;

    for (col_idx, col) in columns.iter().enumerate() {
        write_column_from_rows(output, col_idx, rows, col_idx, &col.spanner_type, &col.name)?;
    }

    output.set_len(rows.len());
    Ok(())
}

/// Write a single column from rows into the output DataChunkHandle.
///
/// - `output_col_idx`: index in the DuckDB output (may differ from spanner index when projected)
/// - `spanner_col_idx`: index in the Spanner Row
/// - `spanner_type`: the Spanner Type proto for this column
///
/// This is the unified entry point used by both spanner_query and spanner_scan.
///
/// `column_name` is used only to enrich Conversion error messages on type mismatch.
pub fn write_column_from_rows(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[Row],
    spanner_col_idx: usize,
    spanner_type: &Type,
    column_name: &str,
) -> Result<(), SpannerError> {
    ensure_row_count(rows.len(), &|| format!("column '{column_name}'"))?;

    let type_code = spanner_type.code();

    match type_code {
        TypeCode::Array => write_array_column(
            output,
            output_col_idx,
            rows,
            spanner_col_idx,
            spanner_type,
            column_name,
        ),
        TypeCode::Struct => write_struct_column(
            output,
            output_col_idx,
            rows,
            spanner_col_idx,
            spanner_type,
            column_name,
        ),
        _ => write_scalar_column(
            output,
            output_col_idx,
            rows,
            spanner_col_idx,
            spanner_type,
            column_name,
        ),
    }
}

/// Write a column of scalar values by extracting raw proto values from each Row.
fn write_scalar_column(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[Row],
    spanner_col_idx: usize,
    spanner_type: &Type,
    column_name: &str,
) -> Result<(), SpannerError> {
    let mut vector = output.flat_vector(output_col_idx);
    for (i, row) in rows.iter().enumerate() {
        let val = row_value(row, spanner_col_idx, column_name, i)?;
        if val.kind() == Kind::Null {
            vector.set_null(i);
        } else {
            write_raw_scalar_to_flat(&mut vector, i, val, spanner_type, &|| {
                format!("column '{column_name}', row {i}")
            })?;
        }
    }
    Ok(())
}

// ─── ARRAY / STRUCT support ─────────────────────────────────────────────────

fn write_array_column(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[Row],
    spanner_col_idx: usize,
    spanner_type: &Type,
    column_name: &str,
) -> Result<(), SpannerError> {
    let element_type = spanner_type
        .array_element_type()
        .ok_or_else(|| SpannerError::Conversion("ARRAY without element type".to_string()))?;

    let mut list_vector = output.list_vector(output_col_idx);

    // First pass: collect all raw values and compute total child count
    let mut raw_values: Vec<Option<Vec<SpannerValue>>> = Vec::with_capacity(rows.len());
    let mut total_children = 0usize;

    for (i, row) in rows.iter().enumerate() {
        let val = row_value(row, spanner_col_idx, column_name, i)?;
        if val.kind() == Kind::Null {
            raw_values.push(None);
        } else if let Some(list) = val.try_as_list() {
            total_children = checked_child_count(total_children, list.len(), &|| {
                format!("column '{column_name}', row {i}")
            })?;
            raw_values.push(Some(list.iter().cloned().collect()));
        } else {
            return Err(type_mismatch_error(
                "ARRAY (List)",
                val.kind(),
                &format!("column '{column_name}', row {i}"),
            ));
        }
    }

    // Set list entries before obtaining the child. Parent entries use the
    // regular DuckDB row-vector capacity; flattened children are reserved
    // separately below.
    let mut offset = 0usize;
    let row_capacity = rows_capacity_hint();
    for (i, opt_values) in raw_values.iter().enumerate() {
        let length = opt_values.as_ref().map_or(0, Vec::len);
        set_list_entry_checked(&mut list_vector, row_capacity, i, offset, length, &|| {
            format!("column '{column_name}', row {i}")
        })?;
        if opt_values.is_none() {
            list_vector.set_null(i);
        }
        offset = checked_child_count(offset, length, &|| {
            format!("column '{column_name}', row {i}")
        })?;
    }

    debug_assert_eq!(offset, total_children);
    let elem_type_code = element_type.code();

    match elem_type_code {
        TypeCode::Struct => {
            let struct_type = element_type.struct_type().ok_or_else(|| {
                SpannerError::Conversion("STRUCT without struct_type".to_string())
            })?;
            set_list_len_after_reserve(&list_vector, total_children, &|| {
                format!("column '{column_name}' ARRAY<STRUCT> child")
            })?;
            let mut child_struct = list_vector.struct_child(total_children);
            reset_struct_list_sizes(&child_struct, struct_type)?;
            let mut flat_idx = 0usize;
            for (row_idx, values) in raw_values.iter().enumerate() {
                let Some(values) = values else { continue };
                for (elem_idx, val) in values.iter().enumerate() {
                    write_raw_struct_value(
                        &mut child_struct,
                        total_children,
                        flat_idx,
                        val,
                        struct_type,
                        &|| {
                            format!(
                                "column '{column_name}', row {row_idx}, array element {elem_idx}"
                            )
                        },
                    )?;
                    flat_idx += 1;
                }
            }
        }
        TypeCode::Array => {
            let inner_elem_type = element_type.array_element_type().ok_or_else(|| {
                SpannerError::Conversion("Nested ARRAY without element type".to_string())
            })?;
            set_list_len_after_reserve(&list_vector, total_children, &|| {
                format!("column '{column_name}' nested ARRAY child")
            })?;
            let mut child_list = list_vector.list_child();
            child_list.set_len(0);
            let mut flat_idx = 0usize;
            for (row_idx, values) in raw_values.iter().enumerate() {
                let Some(values) = values else { continue };
                for (elem_idx, val) in values.iter().enumerate() {
                    write_raw_list_value(
                        &mut child_list,
                        total_children,
                        flat_idx,
                        val,
                        &inner_elem_type,
                        &|| {
                            format!(
                                "column '{column_name}', row {row_idx}, array element {elem_idx}"
                            )
                        },
                    )?;
                    flat_idx += 1;
                }
            }
        }
        _ => {
            set_list_len_after_reserve(&list_vector, total_children, &|| {
                format!("column '{column_name}' ARRAY child")
            })?;
            let mut child = list_vector.child(total_children);
            let mut flat_idx = 0usize;
            for (row_idx, values) in raw_values.iter().enumerate() {
                let Some(values) = values else { continue };
                for (elem_idx, val) in values.iter().enumerate() {
                    write_raw_scalar_to_flat(&mut child, flat_idx, val, &element_type, &|| {
                        format!("column '{column_name}', row {row_idx}, array element {elem_idx}")
                    })?;
                    flat_idx += 1;
                }
            }
        }
    }

    Ok(())
}

fn write_struct_column(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[Row],
    spanner_col_idx: usize,
    spanner_type: &Type,
    column_name: &str,
) -> Result<(), SpannerError> {
    let struct_type = spanner_type
        .struct_type()
        .ok_or_else(|| SpannerError::Conversion("STRUCT without struct_type".to_string()))?;

    let mut struct_vector = output.struct_vector(output_col_idx);
    let row_capacity = rows_capacity_hint();
    reset_struct_list_sizes(&struct_vector, struct_type)?;

    for (i, row) in rows.iter().enumerate() {
        let val = row_value(row, spanner_col_idx, column_name, i)?;
        if val.kind() == Kind::Null {
            ensure_vector_index(i, row_capacity, &|| {
                format!("column '{column_name}', row {i}")
            })?;
            struct_vector.set_null(i);
        } else {
            write_raw_struct_value(
                &mut struct_vector,
                row_capacity,
                i,
                val,
                struct_type,
                &|| format!("column '{column_name}', row {i}"),
            )?;
        }
    }
    Ok(())
}

/// Reset flattened list lengths retained by a reused DuckDB chunk.
///
/// Reserving a struct vector recursively resizes list-entry storage, but list
/// child lengths are independent append cursors and survive cardinality changes.
fn reset_struct_list_sizes(
    struct_vector: &StructVector<'_>,
    struct_type: &model::StructType,
) -> Result<(), SpannerError> {
    for (field_idx, field) in struct_type.fields.iter().enumerate() {
        let field_type = field
            .r#type
            .as_ref()
            .ok_or_else(|| SpannerError::Conversion("STRUCT field without type".to_string()))?;
        match TypeCode::from(field_type.code.clone()) {
            TypeCode::Struct => {
                let nested_type = field_type.struct_type.as_deref().ok_or_else(|| {
                    SpannerError::Conversion("Nested STRUCT without struct_type".to_string())
                })?;
                let child = struct_vector.struct_vector_child(field_idx);
                reset_struct_list_sizes(&child, nested_type)?;
            }
            TypeCode::Array => {
                struct_vector.list_vector_child(field_idx).set_len(0);
            }
            _ => {}
        }
    }
    Ok(())
}

/// Write a single raw proto Value as a struct into a StructVector at the given row index.
///
/// `context` lazily describes the enclosing location (column/row/array element) for
/// error messages; it is only invoked on a type mismatch, keeping the hot path free
/// of per-row string allocation.
fn write_raw_struct_value(
    struct_vector: &mut StructVector<'_>,
    row_capacity: usize,
    row_idx: usize,
    value: &SpannerValue,
    struct_type: &model::StructType,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    ensure_vector_index(row_idx, row_capacity, context)?;

    if value.kind() == Kind::Null {
        struct_vector.set_null(row_idx);
        return Ok(());
    }

    let values = value
        .try_as_list()
        .ok_or_else(|| type_mismatch_error("STRUCT (List)", value.kind(), &context()))?;

    for (field_idx, field) in struct_type.fields.iter().enumerate() {
        let field_type = field
            .r#type
            .as_ref()
            .ok_or_else(|| SpannerError::Conversion("STRUCT field without type".to_string()))?;
        let field_type = Type::from((**field_type).clone());
        let field_type_code = field_type.code();
        let field_context = || format!("{}, field '{}'", context(), field.name);

        let field_value = values.get(field_idx);

        match field_type_code {
            TypeCode::Struct => {
                let mut child = struct_vector.struct_vector_child(field_idx);
                if let Some(val) = field_value {
                    let inner_struct_type = field_type.struct_type().ok_or_else(|| {
                        SpannerError::Conversion("Nested STRUCT without struct_type".to_string())
                    })?;
                    write_raw_struct_value(
                        &mut child,
                        row_capacity,
                        row_idx,
                        val,
                        inner_struct_type,
                        &field_context,
                    )?;
                } else {
                    child.set_null(row_idx);
                }
            }
            TypeCode::Array => {
                let mut child = struct_vector.list_vector_child(field_idx);
                if let Some(val) = field_value {
                    let elem_type = field_type.array_element_type().ok_or_else(|| {
                        SpannerError::Conversion("ARRAY without element type".to_string())
                    })?;
                    write_raw_list_value(
                        &mut child,
                        row_capacity,
                        row_idx,
                        val,
                        &elem_type,
                        &field_context,
                    )?;
                } else {
                    child.set_null(row_idx);
                }
            }
            _ => {
                let mut child = struct_vector.child(field_idx, row_capacity);
                if let Some(val) = field_value {
                    write_raw_scalar_to_flat(
                        &mut child,
                        row_idx,
                        val,
                        &field_type,
                        &field_context,
                    )?;
                } else {
                    child.set_null(row_idx);
                }
            }
        }
    }
    Ok(())
}

/// Write a single raw proto Value as a list entry into a ListVector at the given row index.
///
/// `context` lazily describes the enclosing location (column/row/field) for error
/// messages; it is only invoked on a type mismatch.
fn write_raw_list_value(
    list_vector: &mut ListVector<'_>,
    row_capacity: usize,
    row_idx: usize,
    value: &SpannerValue,
    element_type: &Type,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    ensure_vector_index(row_idx, row_capacity, context)?;

    if value.kind() == Kind::Null {
        let current_len = list_vector.len();
        set_list_entry_checked(list_vector, row_capacity, row_idx, current_len, 0, context)?;
        list_vector.set_null(row_idx);
        return Ok(());
    }

    let values = value
        .try_as_list()
        .ok_or_else(|| type_mismatch_error("ARRAY (List)", value.kind(), &context()))?;

    let current_len = list_vector.len();
    let new_len = checked_child_count(current_len, values.len(), context)?;
    set_list_entry_checked(
        list_vector,
        row_capacity,
        row_idx,
        current_len,
        values.len(),
        context,
    )?;

    match element_type.code() {
        TypeCode::Struct => {
            let struct_type = element_type.struct_type().ok_or_else(|| {
                SpannerError::Conversion("STRUCT without struct_type".to_string())
            })?;
            set_list_len_after_reserve(list_vector, new_len, context)?;
            let mut child_struct = list_vector.struct_child(new_len);
            if current_len == 0 {
                reset_struct_list_sizes(&child_struct, struct_type)?;
            }
            for (j, val) in values.iter().enumerate() {
                let elem_context = || format!("{}, array element {j}", context());
                write_raw_struct_value(
                    &mut child_struct,
                    new_len,
                    current_len + j,
                    val,
                    struct_type,
                    &elem_context,
                )?;
            }
        }
        TypeCode::Array => {
            let inner_element_type = element_type.array_element_type().ok_or_else(|| {
                SpannerError::Conversion("Nested ARRAY without element type".to_string())
            })?;
            set_list_len_after_reserve(list_vector, new_len, context)?;
            let mut child_list = list_vector.list_child();
            if current_len == 0 {
                child_list.set_len(0);
            }
            for (j, val) in values.iter().enumerate() {
                let elem_context = || format!("{}, array element {j}", context());
                write_raw_list_value(
                    &mut child_list,
                    new_len,
                    current_len + j,
                    val,
                    &inner_element_type,
                    &elem_context,
                )?;
            }
        }
        _ => {
            set_list_len_after_reserve(list_vector, new_len, context)?;
            let mut child = list_vector.child(new_len);
            for (j, val) in values.iter().enumerate() {
                let elem_context = || format!("{}, array element {j}", context());
                write_raw_scalar_to_flat(
                    &mut child,
                    current_len + j,
                    val,
                    element_type,
                    &elem_context,
                )?;
            }
        }
    }
    Ok(())
}

// ─── Raw scalar conversion (proto Value → DuckDB FlatVector) ────────────────

/// Write a single raw proto scalar Value to a FlatVector at the given index.
///
/// # Safety of raw pointer writes
///
/// Uses typed mutable slices for fixed-size types because duckdb-rs does not
/// provide safe setters for them. The index is validated against the wrapper's
/// allocation capacity before any validity or data write.
///
/// TODO: Replace with safe API when duckdb-rs provides a safe setter for
/// fixed-size types. See duckdb/duckdb-rs#414 (vtab safety RFC).
fn write_raw_scalar_to_flat(
    vector: &mut FlatVector<'_>,
    idx: usize,
    value: &SpannerValue,
    spanner_type: &Type,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    ensure_vector_index(idx, vector.capacity(), context)?;

    // Null is legitimate regardless of expected type, never a mismatch.
    if value.kind() == Kind::Null {
        vector.set_null(idx);
        return Ok(());
    }

    match spanner_type.code() {
        TypeCode::Bool => {
            if let Some(v) = value.try_as_bool() {
                unsafe { vector.as_mut_slice::<bool>()[idx] = v }
            } else {
                return Err(type_mismatch_error("BOOL (Bool)", value.kind(), &context()));
            }
        }
        TypeCode::Int64 | TypeCode::Enum => {
            if let Some(s) = value.try_as_string() {
                let v: i64 = s
                    .parse()
                    .map_err(|e| SpannerError::Conversion(format!("INT64 parse error: {e}")))?;
                unsafe { vector.as_mut_slice::<i64>()[idx] = v }
            } else {
                let expected = if spanner_type.code() == TypeCode::Enum {
                    "ENUM (String)"
                } else {
                    "INT64 (String)"
                };
                return Err(type_mismatch_error(expected, value.kind(), &context()));
            }
        }
        TypeCode::Float32 => {
            if let Some(v) = value.try_as_f64() {
                unsafe { vector.as_mut_slice::<f32>()[idx] = v as f32 }
            } else if let Some(s) = value.try_as_string() {
                // Handle NaN, Infinity, -Infinity
                let v: f32 = s
                    .parse()
                    .map_err(|e| SpannerError::Conversion(format!("FLOAT32 parse error: {e}")))?;
                unsafe { vector.as_mut_slice::<f32>()[idx] = v }
            } else {
                return Err(type_mismatch_error(
                    "FLOAT32 (Number or String)",
                    value.kind(),
                    &context(),
                ));
            }
        }
        TypeCode::Float64 => {
            if let Some(v) = value.try_as_f64() {
                unsafe { vector.as_mut_slice::<f64>()[idx] = v }
            } else if let Some(s) = value.try_as_string() {
                let v: f64 = s
                    .parse()
                    .map_err(|e| SpannerError::Conversion(format!("FLOAT64 parse error: {e}")))?;
                unsafe { vector.as_mut_slice::<f64>()[idx] = v }
            } else {
                return Err(type_mismatch_error(
                    "FLOAT64 (Number or String)",
                    value.kind(),
                    &context(),
                ));
            }
        }
        TypeCode::Numeric => {
            if let Some(s) = value.try_as_string() {
                if is_pg_numeric(spanner_type) {
                    // PostgreSQL numeric can be NaN or exceed DuckDB's
                    // DECIMAL(38,9) precision and scale. The bind path maps
                    // this annotated type to VARCHAR, preserving the exact
                    // server representation for callers to cast deliberately.
                    checked_duckdb_index(s.len(), "string length", context)?;
                    unsafe { unsafe_assign_string(vector, idx, s) }
                    return Ok(());
                }
                use bigdecimal::{BigDecimal, ToPrimitive};
                use std::str::FromStr;
                let bd = BigDecimal::from_str(s)
                    .map_err(|e| SpannerError::Conversion(format!("NUMERIC parse error: {e}")))?;
                // DuckDB DECIMAL(38,9): fixed-point i128 with scale=9.
                // with_scale(9) shifts to 9 decimal places, into_bigint_and_scale
                // gives the unscaled BigInt directly (no string round-trip).
                let (unscaled, _scale) = bd.with_scale(9).into_bigint_and_scale();
                let int_val: i128 = unscaled.to_i128().ok_or_else(|| {
                    SpannerError::Conversion(format!("NUMERIC value out of i128 range: {s}"))
                })?;
                unsafe { vector.as_mut_slice::<i128>()[idx] = int_val }
            } else {
                return Err(type_mismatch_error(
                    "NUMERIC (String)",
                    value.kind(),
                    &context(),
                ));
            }
        }
        TypeCode::String | TypeCode::Json => {
            if let Some(s) = value.try_as_string() {
                // SAFETY: Spanner guarantees STRING and JSON values are valid UTF-8.
                checked_duckdb_index(s.len(), "string length", context)?;
                unsafe { unsafe_assign_string(vector, idx, s) }
            } else {
                return Err(type_mismatch_error(
                    "STRING/JSON (String)",
                    value.kind(),
                    &context(),
                ));
            }
        }
        TypeCode::Bytes | TypeCode::Proto => {
            if let Some(s) = value.try_as_string() {
                let bytes = base64_decode(s)?;
                checked_duckdb_index(bytes.len(), "binary length", context)?;
                vector.insert(idx, bytes.as_slice());
            } else {
                return Err(type_mismatch_error(
                    "BYTES/PROTO (String, base64)",
                    value.kind(),
                    &context(),
                ));
            }
        }
        TypeCode::Date => {
            if let Some(s) = value.try_as_string() {
                let format = time::format_description::well_known::Iso8601::DATE;
                let date = time::Date::parse(s, &format)
                    .map_err(|e| SpannerError::Conversion(format!("DATE parse error: {e}")))?;
                let days = (date - EPOCH_DATE).whole_days() as i32;
                unsafe { vector.as_mut_slice::<i32>()[idx] = days }
            } else {
                return Err(type_mismatch_error(
                    "DATE (String)",
                    value.kind(),
                    &context(),
                ));
            }
        }
        TypeCode::Timestamp => {
            if let Some(s) = value.try_as_string() {
                let ts = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .map_err(|e| {
                    SpannerError::Conversion(format!("TIMESTAMP parse error: {e}"))
                })?;
                let micros = (ts.unix_timestamp_nanos() / 1_000) as i64;
                unsafe { vector.as_mut_slice::<i64>()[idx] = micros }
            } else {
                return Err(type_mismatch_error(
                    "TIMESTAMP (String)",
                    value.kind(),
                    &context(),
                ));
            }
        }
        TypeCode::Uuid => {
            if let Some(s) = value.try_as_string() {
                let parsed = uuid::Uuid::parse_str(s)
                    .map_err(|e| SpannerError::Conversion(format!("UUID parse error: {e}")))?;
                // DuckDB stores UUIDs as hugeint with the MSB flipped for sort ordering.
                // See: duckdb/src/common/types/uuid.cpp — UUIDToUHugeint / UHugeintToUUID.
                // See also: duckdb/duckdb-rs#519 (UUID value correctness),
                //           duckdb/duckdb-rs#585 (no duckdb_bind_uuid in C API).
                let val = parsed.as_u128() ^ (1u128 << 127);
                unsafe { vector.as_mut_slice::<u128>()[idx] = val }
            } else {
                return Err(type_mismatch_error(
                    "UUID (String)",
                    value.kind(),
                    &context(),
                ));
            }
        }
        TypeCode::Interval => {
            if let Some(s) = value.try_as_string() {
                let interval = parse_iso8601_interval(s)?;
                unsafe { vector.as_mut_slice::<DuckDBInterval>()[idx] = interval }
            } else {
                return Err(type_mismatch_error(
                    "INTERVAL (String)",
                    value.kind(),
                    &context(),
                ));
            }
        }
        _ => {
            // Unknown/unspecified Spanner type code: best-effort fallback to string.
            if let Some(s) = value.try_as_string() {
                // SAFETY: Spanner only returns valid UTF-8 string values.
                checked_duckdb_index(s.len(), "string length", context)?;
                unsafe { unsafe_assign_string(vector, idx, s) }
            } else {
                return Err(type_mismatch_error(
                    "STRING (String, fallback for unrecognized type code)",
                    value.kind(),
                    &context(),
                ));
            }
        }
    }
    Ok(())
}

// ─── Interval parsing ───────────────────────────────────────────────────────

/// DuckDB interval layout: {months: i32, days: i32, micros: i64} = 16 bytes.
/// Must be #[repr(C)] to match DuckDB's `duckdb_interval` / `interval_t` memory layout.
#[derive(Debug, Default, Clone, Copy)]
#[repr(C)]
struct DuckDBInterval {
    months: i32,
    days: i32,
    micros: i64,
}

/// Parse ISO 8601 duration string into DuckDB interval components.
/// Format: P[nY][nM][nD][T[nH][nM][nS]]
/// Examples: "P1Y2M3DT4H5M6S", "P1Y", "PT1H30M", "P0-3 0 0:0:0" (Spanner format)
fn parse_iso8601_interval(s: &str) -> Result<DuckDBInterval, SpannerError> {
    let s = s.trim();

    // Spanner can return intervals in a different format: "P<years>-<months> <days> <hours>:<minutes>:<seconds>"
    // Try the Spanner-specific format first
    if let Some(result) = try_parse_spanner_interval(s) {
        return Ok(result);
    }

    // Standard ISO 8601 format: P[nY][nM][nD][T[nH][nM][n.nS]]
    if !s.starts_with('P') && !s.starts_with('p') {
        return Err(SpannerError::Conversion(format!(
            "Invalid interval format: {s}"
        )));
    }

    let mut interval = DuckDBInterval::default();
    let s = &s[1..]; // skip 'P'
    let (date_part, time_part) = if let Some(t_pos) = s.find(['T', 't']) {
        (&s[..t_pos], Some(&s[t_pos + 1..]))
    } else {
        (s, None)
    };

    // Parse date part: [nY][nM][nD]
    parse_date_components(date_part, &mut interval)?;

    // Parse time part: [nH][nM][n.nS]
    if let Some(time_str) = time_part {
        parse_time_components(time_str, &mut interval)?;
    }

    Ok(interval)
}

fn try_parse_spanner_interval(s: &str) -> Option<DuckDBInterval> {
    // Spanner format: "P<years>-<months> <days> <hours>:<minutes>:<seconds>"
    let s = s.strip_prefix('P').or_else(|| s.strip_prefix('p'))?;
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 3 {
        return None;
    }

    // Parse "years-months" (separator is the last '-', so "-1-2" => years=-1, months=2)
    let sep = parts[0].rfind('-')?;
    if sep == 0 {
        return None;
    }
    let years: i32 = parts[0][..sep].parse().ok()?;
    let months: i32 = parts[0][sep + 1..].parse().ok()?;

    // Parse days
    let days: i32 = parts[1].parse().ok()?;

    // Parse "hours:minutes:seconds"
    let hms: Vec<&str> = parts[2].split(':').collect();
    if hms.len() != 3 {
        return None;
    }
    let hours: i64 = hms[0].parse().ok()?;
    let minutes: i64 = hms[1].parse().ok()?;
    let seconds: f64 = hms[2].parse().ok()?;

    let micros = hours * 3_600_000_000 + minutes * 60_000_000 + (seconds * 1_000_000.0) as i64;

    Some(DuckDBInterval {
        months: years * 12 + months,
        days,
        micros,
    })
}

fn parse_date_components(s: &str, interval: &mut DuckDBInterval) -> Result<(), SpannerError> {
    let mut num_start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            'Y' | 'y' => {
                let n: i32 = s[num_start..i].parse().map_err(|_| {
                    SpannerError::Conversion(format!("Invalid year in interval: {s}"))
                })?;
                interval.months += n * 12;
                num_start = i + 1;
            }
            'M' | 'm' => {
                let n: i32 = s[num_start..i].parse().map_err(|_| {
                    SpannerError::Conversion(format!("Invalid month in interval: {s}"))
                })?;
                interval.months += n;
                num_start = i + 1;
            }
            'D' | 'd' => {
                let n: i32 = s[num_start..i].parse().map_err(|_| {
                    SpannerError::Conversion(format!("Invalid day in interval: {s}"))
                })?;
                interval.days += n;
                num_start = i + 1;
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_time_components(s: &str, interval: &mut DuckDBInterval) -> Result<(), SpannerError> {
    let mut num_start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            'H' | 'h' => {
                let n: i64 = s[num_start..i].parse().map_err(|_| {
                    SpannerError::Conversion(format!("Invalid hours in interval: {s}"))
                })?;
                interval.micros += n * 3_600_000_000;
                num_start = i + 1;
            }
            'M' | 'm' => {
                let n: i64 = s[num_start..i].parse().map_err(|_| {
                    SpannerError::Conversion(format!("Invalid minutes in interval: {s}"))
                })?;
                interval.micros += n * 60_000_000;
                num_start = i + 1;
            }
            'S' | 's' => {
                let n: f64 = s[num_start..i].parse().map_err(|_| {
                    SpannerError::Conversion(format!("Invalid seconds in interval: {s}"))
                })?;
                interval.micros += (n * 1_000_000.0) as i64;
                num_start = i + 1;
            }
            _ => {}
        }
    }
    Ok(())
}

fn base64_decode(s: &str) -> Result<Vec<u8>, SpannerError> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| SpannerError::Conversion(format!("base64 decode error: {e}")))
}

/// Hint for child vector capacity based on DuckDB's runtime chunk size.
fn rows_capacity_hint() -> usize {
    crate::vector_size::runtime_vector_size()
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    #[test]
    fn pre_epoch_timestamp_fractional_seconds() {
        let s = "1969-12-31T23:59:59.5Z";
        let ts = OffsetDateTime::parse(s, &Rfc3339).unwrap();
        let micros = (ts.unix_timestamp_nanos() / 1_000) as i64;
        assert_eq!(micros, -500_000);
    }

    #[test]
    fn spanner_negative_year_month_interval() {
        let interval = try_parse_spanner_interval("P-1-2 0 0:0:0").expect("parse");
        assert_eq!(interval.months, -10); // -1 year, +2 months
        assert_eq!(interval.days, 0);
        assert_eq!(interval.micros, 0);
    }

    #[test]
    fn iso8601_negative_interval_round_trip() {
        let interval = parse_iso8601_interval("PT-1H-30M").expect("parse");
        assert_eq!(interval.micros, -90 * 60 * 1_000_000);
    }

    // ─── Conversion-error classification (issue #11) ───────────────────────

    #[test]
    fn kind_name_covers_all_variants() {
        assert_eq!(kind_name(Kind::Null), "Null");
        assert_eq!(kind_name(Kind::Number), "Number");
        assert_eq!(kind_name(Kind::String), "String");
        assert_eq!(kind_name(Kind::Bool), "Bool");
        assert_eq!(kind_name(Kind::Struct), "Struct");
        assert_eq!(kind_name(Kind::List), "List");
    }

    #[test]
    fn type_mismatch_error_includes_expected_actual_and_context() {
        let value = value_of(ProtoKind::BoolValue(true));
        let err = type_mismatch_error("INT64 (String)", value.kind(), "column 'age', row 3");
        let msg = err.to_string();
        assert!(msg.contains("INT64 (String)"), "message: {msg}");
        assert!(msg.contains("Bool"), "message: {msg}");
        assert!(msg.contains("column 'age', row 3"), "message: {msg}");
    }

    // ─── Real-vector conversion tests (issue #11) ──────────────────────────
    //
    // These exercise the production write paths end-to-end against real DuckDB
    // vectors, so they fail if a mismatch is silently NULLed instead of erroring.
    // DuckDB vectors are constructible in unit tests because the `bundled`
    // dev-dependency links the DuckDB C library (see Cargo.toml).

    use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
    use google_cloud_spanner::value::ToValue;
    use prost_types::value::Kind as ProtoKind;

    fn scalar_type(code: TypeCode) -> Type {
        model::Type::new()
            .set_code(model::TypeCode::from(i32::from(code)))
            .into()
    }

    fn struct_type_proto(fields: Vec<(&str, Type)>) -> Type {
        let fields = fields
            .into_iter()
            .map(|(name, ty)| {
                let ty: model::Type = ty.into();
                model::struct_type::Field::new().set_name(name).set_type(ty)
            })
            .collect::<Vec<_>>();
        model::Type::new()
            .set_code(model::TypeCode::Struct)
            .set_struct_type(model::StructType::new().set_fields(fields))
            .into()
    }

    fn array_type_proto(element_type: Type) -> Type {
        let element_type: model::Type = element_type.into();
        model::Type::new()
            .set_code(model::TypeCode::Array)
            .set_array_element_type(element_type)
            .into()
    }

    fn value_of(kind: ProtoKind) -> SpannerValue {
        prost_types::Value { kind: Some(kind) }.to_value()
    }

    fn value_of_absent_kind() -> SpannerValue {
        prost_types::Value { kind: None }.to_value()
    }

    fn write_scalar_one(
        logical_type: LogicalTypeHandle,
        spanner_type: Type,
        value: SpannerValue,
        column_name: &str,
    ) -> (DataChunkHandle, Result<(), SpannerError>) {
        let chunk = DataChunkHandle::new(&[logical_type]);
        let result = {
            let mut vector = chunk.flat_vector(0);
            write_raw_scalar_to_flat(&mut vector, 0, &value, &spanner_type, &|| {
                format!("column '{column_name}', row 0")
            })
        };
        (chunk, result)
    }

    fn write_list_one(
        child_logical_type: LogicalTypeHandle,
        element_type: Type,
        value: SpannerValue,
        column_name: &str,
    ) -> (DataChunkHandle, Result<(), SpannerError>) {
        let chunk = DataChunkHandle::new(&[LogicalTypeHandle::list(&child_logical_type)]);
        chunk.set_len(1);
        let result = {
            let mut vector = chunk.list_vector(0);
            write_raw_list_value(
                &mut vector,
                rows_capacity_hint(),
                0,
                &value,
                &element_type,
                &|| format!("column '{column_name}', row 0"),
            )
        };
        (chunk, result)
    }

    fn write_struct_one(
        logical_type: LogicalTypeHandle,
        spanner_type: Type,
        value: SpannerValue,
        column_name: &str,
    ) -> (DataChunkHandle, Result<(), SpannerError>) {
        let chunk = DataChunkHandle::new(&[logical_type]);
        let struct_type = spanner_type
            .struct_type()
            .expect("test struct type")
            .clone();
        let result = {
            let mut vector = chunk.struct_vector(0);
            write_raw_struct_value(
                &mut vector,
                rows_capacity_hint(),
                0,
                &value,
                &struct_type,
                &|| format!("column '{column_name}', row 0"),
            )
        };
        (chunk, result)
    }

    #[test]
    fn array_of_struct_reserves_oversized_children_and_preserves_nulls_and_order() {
        let count = rows_capacity_hint() + 257;
        let struct_type = struct_type_proto(vec![
            ("ordinal", scalar_type(TypeCode::Int64)),
            ("flag", scalar_type(TypeCode::Bool)),
        ]);
        let logical_struct = LogicalTypeHandle::struct_type(&[
            ("ordinal", LogicalTypeId::Bigint.into()),
            ("flag", LogicalTypeId::Boolean.into()),
        ]);

        let values = (0..count)
            .map(|i| {
                if i % 509 == 0 {
                    prost_types::Value {
                        kind: Some(ProtoKind::NullValue(0)),
                    }
                } else {
                    let flag = if i % 127 == 0 {
                        ProtoKind::NullValue(0)
                    } else {
                        ProtoKind::BoolValue(i % 2 == 0)
                    };
                    prost_types::Value {
                        kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                            values: vec![
                                prost_types::Value {
                                    kind: Some(ProtoKind::StringValue(i.to_string())),
                                },
                                prost_types::Value { kind: Some(flag) },
                            ],
                        })),
                    }
                }
            })
            .collect();
        let value = value_of(ProtoKind::ListValue(prost_types::ListValue { values }));

        let (chunk, result) = write_list_one(logical_struct, struct_type, value, "items");
        result.expect("oversized ARRAY<STRUCT> conversion");

        let list = chunk.list_vector(0);
        assert_eq!(list.get_entry(0), (0, count));
        assert_eq!(list.len(), count);

        let struct_validity = list.child(count);
        let struct_child = list.struct_child(count);
        let ordinals = struct_child.child(0, count);
        let flags = struct_child.child(1, count);
        let ordinal_values = unsafe { ordinals.as_slice::<i64>() };
        let flag_values = unsafe { flags.as_slice::<bool>() };

        for i in 0..count {
            let struct_is_null = i % 509 == 0;
            assert_eq!(
                struct_validity.row_is_null(i as u64),
                struct_is_null,
                "struct nullness at element {i}"
            );
            if struct_is_null {
                continue;
            }

            assert_eq!(ordinal_values[i], i as i64, "ordinal at element {i}");
            let flag_is_null = i % 127 == 0;
            assert_eq!(
                flags.row_is_null(i as u64),
                flag_is_null,
                "flag nullness at element {i}"
            );
            if !flag_is_null {
                assert_eq!(flag_values[i], i % 2 == 0, "flag at element {i}");
            }
        }
    }

    #[test]
    fn nested_array_reserves_oversized_list_entry_children() {
        let count = rows_capacity_hint() + 17;
        let values = (0..count)
            .map(|i| prost_types::Value {
                kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(ProtoKind::StringValue(i.to_string())),
                    }],
                })),
            })
            .collect();
        let value = value_of(ProtoKind::ListValue(prost_types::ListValue { values }));
        let logical_inner = LogicalTypeHandle::list(&LogicalTypeId::Bigint.into());

        let (chunk, result) = write_list_one(
            logical_inner,
            array_type_proto(scalar_type(TypeCode::Int64)),
            value,
            "nested",
        );
        result.expect("oversized nested ARRAY conversion");

        let outer = chunk.list_vector(0);
        assert_eq!(outer.get_entry(0), (0, count));
        let inner = outer.list_child();
        assert_eq!(inner.len(), count);
        let inner_entries = unsafe {
            std::slice::from_raw_parts(
                ffi::duckdb_vector_get_data(list_vector_raw(&inner))
                    .cast::<ffi::duckdb_list_entry>(),
                count,
            )
        };
        let values = inner.child(count);
        let values = unsafe { values.as_slice::<i64>() };
        for i in 0..count {
            assert_eq!(inner_entries[i].offset, i as u64);
            assert_eq!(inner_entries[i].length, 1);
            assert_eq!(values[i], i as i64);
        }
    }

    #[test]
    fn reused_struct_chunk_resets_nested_list_offsets() {
        let array_type = array_type_proto(scalar_type(TypeCode::Int64));
        let spanner_type = struct_type_proto(vec![("values", array_type)]);
        let struct_type = spanner_type.struct_type().unwrap().clone();
        let logical_type = LogicalTypeHandle::struct_type(&[(
            "values",
            LogicalTypeHandle::list(&LogicalTypeId::Bigint.into()),
        )]);
        let chunk = DataChunkHandle::new(&[logical_type]);
        chunk.set_len(1);

        let batches = [vec!["10", "11", "12"], vec!["20"]];
        for batch in batches {
            let value = value_of(ProtoKind::ListValue(prost_types::ListValue {
                values: vec![prost_types::Value {
                    kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                        values: batch
                            .iter()
                            .map(|value| prost_types::Value {
                                kind: Some(ProtoKind::StringValue((*value).to_string())),
                            })
                            .collect(),
                    })),
                }],
            }));

            let mut vector = chunk.struct_vector(0);
            reset_struct_list_sizes(&vector, &struct_type).unwrap();
            write_raw_struct_value(
                &mut vector,
                rows_capacity_hint(),
                0,
                &value,
                &struct_type,
                &|| "column 'payload', row 0".to_string(),
            )
            .unwrap();
        }

        let vector = chunk.struct_vector(0);
        let nested = vector.list_vector_child(0);
        assert_eq!(nested.get_entry(0), (0, 1));
        assert_eq!(nested.len(), 1);
        assert_eq!(unsafe { nested.child(1).as_slice::<i64>() }[0], 20);
    }

    #[test]
    fn unrepresentable_nested_child_count_returns_conversion_error() {
        let too_many = usize::try_from(DUCKDB_MAX_RESULT_CHILDREN + 1).unwrap();
        let result = checked_child_count(0, too_many, &|| "test value".to_string());
        match result {
            Err(SpannerError::Conversion(message)) => {
                assert!(message.contains("safe result-vector capacity"));
                assert!(message.contains("test value"));
            }
            other => panic!("expected Conversion error, got {other:?}"),
        }
    }

    #[test]
    fn scalar_mismatch_returns_conversion_error_with_context() {
        // BOOL column receiving a String value must error, not silently NULL.
        let (_chunk, result) = write_scalar_one(
            LogicalTypeId::Boolean.into(),
            scalar_type(TypeCode::Bool),
            value_of(ProtoKind::StringValue("true".to_string())),
            "flag",
        );
        match result {
            Err(SpannerError::Conversion(msg)) => {
                assert!(msg.contains("BOOL"), "message: {msg}");
                assert!(msg.contains("String"), "message: {msg}");
                assert!(msg.contains("column 'flag', row 0"), "message: {msg}");
            }
            other => panic!("expected Conversion error, got {other:?}"),
        }
    }

    #[test]
    fn scalar_null_value_writes_null_not_error() {
        // NullValue is a legitimate NULL, never a mismatch.
        let (chunk, result) = write_scalar_one(
            LogicalTypeId::Boolean.into(),
            scalar_type(TypeCode::Bool),
            value_of(ProtoKind::NullValue(0)),
            "flag",
        );
        result.expect("NullValue must write NULL, not error");
        assert!(
            chunk.flat_vector(0).row_is_null(0),
            "row 0 should be NULL after a NullValue write"
        );
    }

    #[test]
    fn scalar_absent_kind_writes_null_not_error() {
        // `kind: None` (absent) is handled as NULL by write_raw_scalar_to_flat.
        let (chunk, result) = write_scalar_one(
            LogicalTypeId::Boolean.into(),
            scalar_type(TypeCode::Bool),
            value_of_absent_kind(),
            "flag",
        );
        result.expect("absent kind must write NULL, not error");
        assert!(
            chunk.flat_vector(0).row_is_null(0),
            "row 0 should be NULL for absent kind"
        );
    }

    #[test]
    fn pg_numeric_special_values_preserve_their_wire_strings() {
        for value in ["NaN", "123456789012345678901234567890123456789.123456789"] {
            let (chunk, result) = write_scalar_one(
                LogicalTypeId::Varchar.into(),
                google_cloud_spanner::types::pg_numeric(),
                value_of(ProtoKind::StringValue(value.to_string())),
                "num",
            );
            result.expect("PG numeric should be written as VARCHAR");
            let vector = chunk.flat_vector(0);
            let mut string = unsafe { vector.as_mut_ptr::<ffi::duckdb_string_t>().read() };
            let len = unsafe { ffi::duckdb_string_t_length(string) } as usize;
            let data = unsafe { ffi::duckdb_string_t_data(&mut string) };
            let written = unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len) };
            assert_eq!(written, value.as_bytes());
        }
    }

    #[test]
    fn list_element_mismatch_returns_conversion_error_with_context() {
        // ARRAY<BOOL> whose element is a String value must error on the element.
        let list_value = value_of(ProtoKind::ListValue(prost_types::ListValue {
            values: vec![prost_types::Value {
                kind: Some(ProtoKind::StringValue("nope".to_string())),
            }],
        }));
        let (_chunk, result) = write_list_one(
            LogicalTypeId::Boolean.into(),
            scalar_type(TypeCode::Bool),
            list_value,
            "arr",
        );
        match result {
            Err(SpannerError::Conversion(msg)) => {
                assert!(msg.contains("BOOL"), "message: {msg}");
                assert!(msg.contains("String"), "message: {msg}");
                assert!(msg.contains("row 0, array element 0"), "message: {msg}");
            }
            other => panic!("expected Conversion error, got {other:?}"),
        }
    }

    #[test]
    fn struct_field_mismatch_returns_conversion_error_with_context() {
        // STRUCT<flag BOOL> whose field value is a String value must error on the field.
        let struct_value = value_of(ProtoKind::ListValue(prost_types::ListValue {
            values: vec![prost_types::Value {
                kind: Some(ProtoKind::StringValue("nope".to_string())),
            }],
        }));
        let logical = LogicalTypeHandle::struct_type(&[("flag", LogicalTypeId::Boolean.into())]);
        let (_chunk, result) = write_struct_one(
            logical,
            struct_type_proto(vec![("flag", scalar_type(TypeCode::Bool))]),
            struct_value,
            "st",
        );
        match result {
            Err(SpannerError::Conversion(msg)) => {
                assert!(msg.contains("BOOL"), "message: {msg}");
                assert!(msg.contains("String"), "message: {msg}");
                assert!(msg.contains("field 'flag'"), "message: {msg}");
                assert!(msg.contains("row 0"), "message: {msg}");
            }
            other => panic!("expected Conversion error, got {other:?}"),
        }
    }

    #[test]
    fn array_column_null_value_writes_null_not_error() {
        // A NullValue at the ARRAY column level is a legitimate NULL. This shares
        // the `Some(Kind::NullValue(_)) | None` arm in write_array_column, so it
        // also guards the absent-kind consistency fix (issue #11 review item 2).
        let (_chunk, result) = write_list_one(
            LogicalTypeId::Boolean.into(),
            scalar_type(TypeCode::Bool),
            value_of(ProtoKind::NullValue(0)),
            "arr",
        );
        result.expect("array NullValue must write NULL, not error");
    }

    #[test]
    fn array_column_scalar_kind_is_mismatch_not_null() {
        // A non-list, non-null kind at the ARRAY column level is a genuine
        // mismatch (only NullValue / absent kind are treated as NULL).
        let (_chunk, result) = write_list_one(
            LogicalTypeId::Boolean.into(),
            scalar_type(TypeCode::Bool),
            value_of(ProtoKind::BoolValue(true)),
            "arr",
        );
        match result {
            Err(SpannerError::Conversion(msg)) => {
                assert!(msg.contains("ARRAY"), "message: {msg}");
                assert!(msg.contains("Bool"), "message: {msg}");
                assert!(msg.contains("column 'arr', row 0"), "message: {msg}");
            }
            other => panic!("expected Conversion error, got {other:?}"),
        }
    }

    #[test]
    fn conversion_error_display_matches_error_rs_format() {
        let err = SpannerError::Conversion("boom".to_string());
        assert_eq!(err.to_string(), "Type conversion error: boom");
    }
}
