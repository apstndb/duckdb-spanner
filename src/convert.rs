use duckdb::core::{DataChunkHandle, FlatVector, Inserter, ListVector, StructVector};
use duckdb::ffi;
use duckdb::types::Decimal;
use google_cloud_spanner::model;
use google_cloud_spanner::result::Row;
use google_cloud_spanner::types::{Type, TypeCode};
use google_cloud_spanner::value::{Kind, Value as SpannerValue};
use time::OffsetDateTime;

use crate::error::SpannerError;
use crate::schema::ColumnInfo;
use crate::types::is_pg_numeric;

const EPOCH_DATE: time::Date = time::macros::date!(1970 - 01 - 01);

// Bound flattened result children to 64 MiB of DuckDB LIST entries. This is a
// practical per-result budget (4,194,304 entries on current DuckDB) rather than
// a theoretical engine allocation limit, and keeps oversized result materialization
// from reaching an unchecked C API allocation path.
const MAX_FLATTENED_CHILDREN: usize =
    64 * 1024 * 1024 / std::mem::size_of::<ffi::duckdb_list_entry>();

/// Extract the raw `duckdb_vector` handle from a FlatVector.
///
/// duckdb-rs 1.10505.0 does not expose the underlying `duckdb_vector` handle,
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
    checked_duckdb_index(total, "nested child count", context)?;
    if total > MAX_FLATTENED_CHILDREN {
        return Err(SpannerError::Conversion(format!(
            "nested child count {total} exceeds the 64 MiB flattened result budget of {MAX_FLATTENED_CHILDREN} entries ({})",
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

/// Reserve the outer list's child before constructing its nested ListVector.
/// duckdb-rs 1.10505.0 carries this reservation into `list_child()`, so nested
/// arrays are no longer limited to DuckDB's standard vector size.
fn reserve_nested_list_child(
    list_vector: &ListVector<'_>,
    entry_count: usize,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    list_vector.try_reserve(entry_count).map_err(|error| {
        SpannerError::Conversion(format!(
            "failed to reserve {entry_count} nested ARRAY entries: {error} ({})",
            context()
        ))
    })
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
    checked_duckdb_index(offset, "list child offset", context)?;
    checked_duckdb_index(length, "list entry length", context)?;
    checked_child_count(offset, length, context)?;

    list_vector.set_entry(row_idx, offset, length);
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

enum PreflightState {
    Scalar,
    Array {
        child_count: usize,
        element: Box<PreflightState>,
    },
    Struct {
        fields: Vec<PreflightState>,
    },
}

impl PreflightState {
    fn for_type(spanner_type: &Type, context: &dyn Fn() -> String) -> Result<Self, SpannerError> {
        match spanner_type.code() {
            TypeCode::Array => {
                let element_type = spanner_type.array_element_type().ok_or_else(|| {
                    SpannerError::Conversion(format!("ARRAY without element type ({})", context()))
                })?;
                let element_context = || format!("{}, array element type", context());
                Ok(Self::Array {
                    child_count: 0,
                    element: Box::new(Self::for_type(&element_type, &element_context)?),
                })
            }
            TypeCode::Struct => {
                let struct_type = spanner_type.struct_type().ok_or_else(|| {
                    SpannerError::Conversion(format!("STRUCT without struct_type ({})", context()))
                })?;
                Ok(Self::Struct {
                    fields: Self::for_struct_type(struct_type, context)?,
                })
            }
            code if is_supported_scalar_type(code) => Ok(Self::Scalar),
            other => Err(SpannerError::Conversion(format!(
                "unsupported Spanner TypeCode {other:?} ({})",
                context()
            ))),
        }
    }

    fn for_struct_type(
        struct_type: &model::StructType,
        context: &dyn Fn() -> String,
    ) -> Result<Vec<Self>, SpannerError> {
        struct_type
            .fields
            .iter()
            .enumerate()
            .map(|(field_idx, field)| {
                let field_name = if field.name.is_empty() {
                    format!("field {field_idx}")
                } else {
                    field.name.clone()
                };
                let field_context = || format!("{}, field '{field_name}'", context());
                let field_type = field.r#type.as_ref().ok_or_else(|| {
                    SpannerError::Conversion(format!(
                        "STRUCT field '{field_name}' without type ({})",
                        context()
                    ))
                })?;
                Self::for_type(&Type::from((**field_type).clone()), &field_context)
            })
            .collect()
    }
}

fn preflight_value(
    value: &SpannerValue,
    spanner_type: &Type,
    state: &mut PreflightState,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    if value.kind() == Kind::Null {
        return Ok(());
    }

    match spanner_type.code() {
        TypeCode::Array => {
            let element_type = spanner_type.array_element_type().ok_or_else(|| {
                SpannerError::Conversion(format!("ARRAY without element type ({})", context()))
            })?;
            let PreflightState::Array {
                child_count,
                element,
            } = state
            else {
                return Err(SpannerError::Conversion(format!(
                    "internal ARRAY preflight state mismatch ({})",
                    context()
                )));
            };
            preflight_list_value(value, &element_type, child_count, element, context)
        }
        TypeCode::Struct => {
            let struct_type = spanner_type.struct_type().ok_or_else(|| {
                SpannerError::Conversion(format!("STRUCT without struct_type ({})", context()))
            })?;
            let PreflightState::Struct { fields } = state else {
                return Err(SpannerError::Conversion(format!(
                    "internal STRUCT preflight state mismatch ({})",
                    context()
                )));
            };
            preflight_struct_value(value, struct_type, fields, context)
        }
        _ => {
            if !matches!(state, PreflightState::Scalar) {
                return Err(SpannerError::Conversion(format!(
                    "internal scalar preflight state mismatch ({})",
                    context()
                )));
            }
            prepare_scalar_value(value, spanner_type, context).map(|_| ())
        }
    }
}

fn preflight_list_value(
    value: &SpannerValue,
    element_type: &Type,
    child_count: &mut usize,
    element_state: &mut PreflightState,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    if value.kind() == Kind::Null {
        return Ok(());
    }
    let values = value
        .try_as_list()
        .ok_or_else(|| type_mismatch_error("ARRAY (List)", value.kind(), &context()))?;
    let new_child_count = checked_child_count(*child_count, values.len(), context)?;

    for (element_idx, element_value) in values.iter().enumerate() {
        let element_context = || format!("{}, array element {element_idx}", context());
        preflight_value(element_value, element_type, element_state, &element_context)?;
    }
    *child_count = new_child_count;
    Ok(())
}

fn preflight_struct_value(
    value: &SpannerValue,
    struct_type: &model::StructType,
    field_states: &mut [PreflightState],
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    if value.kind() == Kind::Null {
        return Ok(());
    }
    let values = value
        .try_as_list()
        .ok_or_else(|| type_mismatch_error("STRUCT (List)", value.kind(), &context()))?;

    validate_struct_value_arity(values.len(), struct_type.fields.len(), context)?;

    debug_assert_eq!(field_states.len(), struct_type.fields.len());
    for (field_idx, (field, field_state)) in struct_type
        .fields
        .iter()
        .zip(field_states.iter_mut())
        .enumerate()
    {
        let field_type = field.r#type.as_ref().ok_or_else(|| {
            SpannerError::Conversion(format!(
                "STRUCT field '{}' without type ({})",
                field.name,
                context()
            ))
        })?;
        let field_type = Type::from((**field_type).clone());
        let field_context = || format!("{}, field '{}'", context(), field.name);

        let field_value = values.get(field_idx).ok_or_else(|| {
            SpannerError::Conversion(format!(
                "STRUCT field value is missing after arity validation ({})",
                field_context()
            ))
        })?;
        preflight_value(field_value, &field_type, field_state, &field_context)?;
    }
    Ok(())
}

fn validate_struct_value_arity(
    value_count: usize,
    field_count: usize,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    if value_count != field_count {
        return Err(SpannerError::Conversion(format!(
            "STRUCT value has {value_count} values for {field_count} fields ({})",
            context()
        )));
    }
    Ok(())
}

pub(crate) trait ConversionRow {
    fn conversion_values(&self) -> &[SpannerValue];
}

impl ConversionRow for Row {
    fn conversion_values(&self) -> &[SpannerValue] {
        self.raw_values()
    }
}

pub(crate) fn preflight_column_from_rows<R: ConversionRow>(
    rows: &[R],
    spanner_col_idx: usize,
    spanner_type: &Type,
    column_name: &str,
) -> Result<(), SpannerError> {
    ensure_row_count(rows.len(), &|| format!("column '{column_name}'"))?;
    let mut state = PreflightState::for_type(spanner_type, &|| format!("column '{column_name}'"))?;
    for (row_idx, row) in rows.iter().enumerate() {
        let value = row_value(row, spanner_col_idx, column_name, row_idx)?;
        preflight_value(value, spanner_type, &mut state, &|| {
            format!("column '{column_name}', row {row_idx}")
        })?;
    }
    Ok(())
}

fn row_value<'a, R: ConversionRow>(
    row: &'a R,
    spanner_col_idx: usize,
    column_name: &str,
    row_idx: usize,
) -> Result<&'a SpannerValue, SpannerError> {
    row.conversion_values().get(spanner_col_idx).ok_or_else(|| {
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
    ensure_row_count(rows.len(), &|| "result batch".to_string())?;

    // Validate every column before publishing any part of the batch. Calling
    // the public single-column entry point here would validate too late, after
    // earlier columns had already been written.
    for (col_idx, col) in columns.iter().enumerate() {
        preflight_column_from_rows(rows, col_idx, &col.spanner_type, &col.name)?;
    }

    if rows.is_empty() {
        output.set_len(0);
        return Ok(());
    }

    for (col_idx, col) in columns.iter().enumerate() {
        write_preflighted_column_from_rows(
            output,
            col_idx,
            rows,
            col_idx,
            &col.spanner_type,
            &col.name,
        )?;
    }

    output.set_len(rows.len());
    Ok(())
}

pub(crate) fn write_preflighted_column_from_rows<R: ConversionRow>(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[R],
    spanner_col_idx: usize,
    spanner_type: &Type,
    column_name: &str,
) -> Result<(), SpannerError> {
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
fn write_scalar_column<R: ConversionRow>(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[R],
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

fn write_array_column<R: ConversionRow>(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[R],
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

    let elem_type_code = element_type.code();
    let element_struct_type = if elem_type_code == TypeCode::Struct {
        let struct_type = element_type
            .struct_type()
            .ok_or_else(|| SpannerError::Conversion("STRUCT without struct_type".to_string()))?;
        Some(struct_type)
    } else {
        None
    };
    if elem_type_code == TypeCode::Array {
        reserve_nested_list_child(&list_vector, total_children, &|| {
            format!("column '{column_name}' nested ARRAY child")
        })?;
    }

    // Parent entries use DuckDB's regular row-vector capacity. Scalar and
    // struct children obtain their requested capacity through public APIs below.
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

    match elem_type_code {
        TypeCode::Struct => {
            let struct_type = element_struct_type
                .expect("STRUCT metadata was validated before writing list entries");
            let mut child_struct = list_vector.struct_child(total_children);
            reset_struct_list_sizes(&child_struct, struct_type)?;
            list_vector.set_len(total_children);
            let mut flat_idx = 0usize;
            for (row_idx, values) in raw_values.iter().enumerate() {
                let Some(values) = values else { continue };
                for (elem_idx, val) in values.iter().enumerate() {
                    write_raw_struct_value_prevalidated(
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
            let mut child_list = list_vector.list_child();
            child_list.set_len(0);
            list_vector.set_len(total_children);
            let mut flat_idx = 0usize;
            for (row_idx, values) in raw_values.iter().enumerate() {
                let Some(values) = values else { continue };
                for (elem_idx, val) in values.iter().enumerate() {
                    write_raw_list_value_prevalidated(
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
            let mut child = list_vector.child(total_children);
            list_vector.set_len(total_children);
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

fn write_struct_column<R: ConversionRow>(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[R],
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
            write_raw_struct_value_prevalidated(
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
#[cfg(test)]
fn write_raw_struct_value(
    struct_vector: &mut StructVector<'_>,
    row_capacity: usize,
    row_idx: usize,
    value: &SpannerValue,
    struct_type: &model::StructType,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    ensure_vector_index(row_idx, row_capacity, context)?;
    let mut field_states = PreflightState::for_struct_type(struct_type, context)?;
    preflight_struct_value(value, struct_type, &mut field_states, context)?;
    write_raw_struct_value_prevalidated(
        struct_vector,
        row_capacity,
        row_idx,
        value,
        struct_type,
        context,
    )
}

fn write_raw_struct_value_prevalidated(
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
    validate_struct_value_arity(values.len(), struct_type.fields.len(), context)?;

    for (field_idx, field) in struct_type.fields.iter().enumerate() {
        let field_type = field
            .r#type
            .as_ref()
            .ok_or_else(|| SpannerError::Conversion("STRUCT field without type".to_string()))?;
        let field_type = Type::from((**field_type).clone());
        let field_type_code = field_type.code();
        let field_context = || format!("{}, field '{}'", context(), field.name);

        let field_value = values.get(field_idx).ok_or_else(|| {
            SpannerError::Conversion(format!(
                "STRUCT field value is missing after arity validation ({})",
                field_context()
            ))
        })?;

        match field_type_code {
            TypeCode::Struct => {
                let mut child = struct_vector.struct_vector_child(field_idx);
                let inner_struct_type = field_type.struct_type().ok_or_else(|| {
                    SpannerError::Conversion("Nested STRUCT without struct_type".to_string())
                })?;
                write_raw_struct_value_prevalidated(
                    &mut child,
                    row_capacity,
                    row_idx,
                    field_value,
                    inner_struct_type,
                    &field_context,
                )?;
            }
            TypeCode::Array => {
                let mut child = struct_vector.list_vector_child(field_idx);
                let elem_type = field_type.array_element_type().ok_or_else(|| {
                    SpannerError::Conversion("ARRAY without element type".to_string())
                })?;
                write_raw_list_value_prevalidated(
                    &mut child,
                    row_capacity,
                    row_idx,
                    field_value,
                    &elem_type,
                    &field_context,
                )?;
            }
            _ => {
                let mut child = struct_vector.child(field_idx, row_capacity);
                write_raw_scalar_to_flat(
                    &mut child,
                    row_idx,
                    field_value,
                    &field_type,
                    &field_context,
                )?;
            }
        }
    }
    Ok(())
}

/// Write a single raw proto Value as a list entry into a ListVector at the given row index.
///
/// `context` lazily describes the enclosing location (column/row/field) for error
/// messages; it is only invoked on a type mismatch.
#[cfg(test)]
fn write_raw_list_value(
    list_vector: &mut ListVector<'_>,
    row_capacity: usize,
    row_idx: usize,
    value: &SpannerValue,
    element_type: &Type,
    context: &dyn Fn() -> String,
) -> Result<(), SpannerError> {
    ensure_vector_index(row_idx, row_capacity, context)?;
    let element_context = || format!("{}, array element type", context());
    let mut element_state = PreflightState::for_type(element_type, &element_context)?;
    let mut child_count = list_vector.len();
    preflight_list_value(
        value,
        element_type,
        &mut child_count,
        &mut element_state,
        context,
    )?;
    write_raw_list_value_prevalidated(
        list_vector,
        row_capacity,
        row_idx,
        value,
        element_type,
        context,
    )
}

fn write_raw_list_value_prevalidated(
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
    let elem_type_code = element_type.code();
    let element_struct_type = if elem_type_code == TypeCode::Struct {
        let struct_type = element_type
            .struct_type()
            .ok_or_else(|| SpannerError::Conversion("STRUCT without struct_type".to_string()))?;
        Some(struct_type)
    } else {
        None
    };
    if elem_type_code == TypeCode::Array {
        reserve_nested_list_child(list_vector, new_len, context)?;
    }
    set_list_entry_checked(
        list_vector,
        row_capacity,
        row_idx,
        current_len,
        values.len(),
        context,
    )?;

    match elem_type_code {
        TypeCode::Struct => {
            let struct_type = element_struct_type
                .expect("STRUCT metadata was validated before writing the list entry");
            let mut child_struct = list_vector.struct_child(new_len);
            if current_len == 0 {
                reset_struct_list_sizes(&child_struct, struct_type)?;
            }
            list_vector.set_len(new_len);
            for (j, val) in values.iter().enumerate() {
                let elem_context = || format!("{}, array element {j}", context());
                write_raw_struct_value_prevalidated(
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
            let mut child_list = list_vector.list_child();
            if current_len == 0 {
                child_list.set_len(0);
            }
            list_vector.set_len(new_len);
            for (j, val) in values.iter().enumerate() {
                let elem_context = || format!("{}, array element {j}", context());
                write_raw_list_value_prevalidated(
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
            let mut child = list_vector.child(new_len);
            list_vector.set_len(new_len);
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

enum PreparedScalar<'a> {
    Null,
    Bool(bool),
    I32(i32),
    I64(i64),
    I128(i128),
    U128(u128),
    F32(f32),
    F64(f64),
    String(&'a str),
    Bytes(Vec<u8>),
    Interval(DuckDBInterval),
}

fn is_supported_scalar_type(type_code: TypeCode) -> bool {
    matches!(
        type_code,
        TypeCode::Bool
            | TypeCode::Int64
            | TypeCode::Float32
            | TypeCode::Float64
            | TypeCode::Numeric
            | TypeCode::String
            | TypeCode::Json
            | TypeCode::Bytes
            | TypeCode::Proto
            | TypeCode::Date
            | TypeCode::Timestamp
            | TypeCode::Enum
            | TypeCode::Uuid
            | TypeCode::Interval
    )
}

fn prepare_scalar_value<'a>(
    value: &'a SpannerValue,
    spanner_type: &Type,
    context: &dyn Fn() -> String,
) -> Result<PreparedScalar<'a>, SpannerError> {
    let type_code = spanner_type.code();
    if !is_supported_scalar_type(type_code) {
        return Err(SpannerError::Conversion(format!(
            "unsupported Spanner TypeCode {type_code:?} ({})",
            context()
        )));
    }

    if value.kind() == Kind::Null {
        return Ok(PreparedScalar::Null);
    }

    match type_code {
        TypeCode::Bool => value
            .try_as_bool()
            .map(PreparedScalar::Bool)
            .ok_or_else(|| type_mismatch_error("BOOL (Bool)", value.kind(), &context())),
        TypeCode::Int64 | TypeCode::Enum => {
            let expected = if spanner_type.code() == TypeCode::Enum {
                "ENUM (String)"
            } else {
                "INT64 (String)"
            };
            let value = value
                .try_as_string()
                .ok_or_else(|| type_mismatch_error(expected, value.kind(), &context()))?
                .parse::<i64>()
                .map_err(|e| SpannerError::Conversion(format!("INT64 parse error: {e}")))?;
            Ok(PreparedScalar::I64(value))
        }
        TypeCode::Float32 => {
            let value = if let Some(value) = value.try_as_f64() {
                value as f32
            } else if let Some(value) = value.try_as_string() {
                value
                    .parse::<f32>()
                    .map_err(|e| SpannerError::Conversion(format!("FLOAT32 parse error: {e}")))?
            } else {
                return Err(type_mismatch_error(
                    "FLOAT32 (Number or String)",
                    value.kind(),
                    &context(),
                ));
            };
            Ok(PreparedScalar::F32(value))
        }
        TypeCode::Float64 => {
            let value = if let Some(value) = value.try_as_f64() {
                value
            } else if let Some(value) = value.try_as_string() {
                value
                    .parse::<f64>()
                    .map_err(|e| SpannerError::Conversion(format!("FLOAT64 parse error: {e}")))?
            } else {
                return Err(type_mismatch_error(
                    "FLOAT64 (Number or String)",
                    value.kind(),
                    &context(),
                ));
            };
            Ok(PreparedScalar::F64(value))
        }
        TypeCode::Numeric => {
            let value = value
                .try_as_string()
                .ok_or_else(|| type_mismatch_error("NUMERIC (String)", value.kind(), &context()))?;
            if is_pg_numeric(spanner_type) {
                checked_duckdb_index(value.len(), "string length", context)?;
                return Ok(PreparedScalar::String(value));
            }

            use bigdecimal::{BigDecimal, ToPrimitive};
            use std::str::FromStr;
            let decimal = BigDecimal::from_str(value)
                .map_err(|e| SpannerError::Conversion(format!("NUMERIC parse error: {e}")))?;
            let (unscaled, _scale) = decimal.with_scale(9).into_bigint_and_scale();
            let unscaled = unscaled.to_i128().ok_or_else(|| {
                SpannerError::Conversion(format!("NUMERIC value out of i128 range: {value}"))
            })?;
            let decimal = Decimal::new(38, 9, unscaled).map_err(|error| {
                SpannerError::Conversion(format!(
                    "NUMERIC value does not fit DuckDB DECIMAL(38,9): {value}: {error}"
                ))
            })?;
            Ok(PreparedScalar::I128(decimal.value()))
        }
        TypeCode::String | TypeCode::Json => {
            let value = value.try_as_string().ok_or_else(|| {
                type_mismatch_error("STRING/JSON (String)", value.kind(), &context())
            })?;
            checked_duckdb_index(value.len(), "string length", context)?;
            Ok(PreparedScalar::String(value))
        }
        TypeCode::Bytes | TypeCode::Proto => {
            let value = value.try_as_string().ok_or_else(|| {
                type_mismatch_error("BYTES/PROTO (String, base64)", value.kind(), &context())
            })?;
            let value = base64_decode(value)?;
            checked_duckdb_index(value.len(), "binary length", context)?;
            Ok(PreparedScalar::Bytes(value))
        }
        TypeCode::Date => {
            let value = value
                .try_as_string()
                .ok_or_else(|| type_mismatch_error("DATE (String)", value.kind(), &context()))?;
            let format = time::format_description::well_known::Iso8601::DATE;
            let date = time::Date::parse(value, &format)
                .map_err(|e| SpannerError::Conversion(format!("DATE parse error: {e}")))?;
            Ok(PreparedScalar::I32((date - EPOCH_DATE).whole_days() as i32))
        }
        TypeCode::Timestamp => {
            let value = value.try_as_string().ok_or_else(|| {
                type_mismatch_error("TIMESTAMP (String)", value.kind(), &context())
            })?;
            let timestamp =
                OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
                    .map_err(|e| SpannerError::Conversion(format!("TIMESTAMP parse error: {e}")))?;
            Ok(PreparedScalar::I64(
                (timestamp.unix_timestamp_nanos() / 1_000) as i64,
            ))
        }
        TypeCode::Uuid => {
            let value = value
                .try_as_string()
                .ok_or_else(|| type_mismatch_error("UUID (String)", value.kind(), &context()))?;
            let value = uuid::Uuid::parse_str(value)
                .map_err(|e| SpannerError::Conversion(format!("UUID parse error: {e}")))?
                .as_u128()
                ^ (1u128 << 127);
            Ok(PreparedScalar::U128(value))
        }
        TypeCode::Interval => {
            let value = value.try_as_string().ok_or_else(|| {
                type_mismatch_error("INTERVAL (String)", value.kind(), &context())
            })?;
            Ok(PreparedScalar::Interval(parse_iso8601_interval(value)?))
        }
        other => Err(SpannerError::Conversion(format!(
            "unsupported Spanner TypeCode {other:?} ({})",
            context()
        ))),
    }
}

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
    match prepare_scalar_value(value, spanner_type, context)? {
        PreparedScalar::Null => vector.set_null(idx),
        PreparedScalar::Bool(value) => unsafe { vector.as_mut_slice::<bool>()[idx] = value },
        PreparedScalar::I32(value) => unsafe { vector.as_mut_slice::<i32>()[idx] = value },
        PreparedScalar::I64(value) => unsafe { vector.as_mut_slice::<i64>()[idx] = value },
        PreparedScalar::I128(value) => unsafe { vector.as_mut_slice::<i128>()[idx] = value },
        PreparedScalar::U128(value) => unsafe { vector.as_mut_slice::<u128>()[idx] = value },
        PreparedScalar::F32(value) => unsafe { vector.as_mut_slice::<f32>()[idx] = value },
        PreparedScalar::F64(value) => unsafe { vector.as_mut_slice::<f64>()[idx] = value },
        PreparedScalar::String(value) => {
            // Spanner STRING-like values are guaranteed valid UTF-8.
            unsafe { unsafe_assign_string(vector, idx, value) }
        }
        PreparedScalar::Bytes(value) => vector.insert(idx, value.as_slice()),
        PreparedScalar::Interval(value) => unsafe {
            vector.as_mut_slice::<DuckDBInterval>()[idx] = value
        },
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
    let original = s.trim();

    // Spanner can return intervals in a different format: "P<years>-<months> <days> <hours>:<minutes>:<seconds>"
    // Try the Spanner-specific format first
    if let Some(result) = try_parse_spanner_interval(original) {
        return Ok(result);
    }

    // Standard ISO 8601 format: P[nY][nM][nD][T[nH][nM][n.nS]]
    if !original.starts_with('P') && !original.starts_with('p') {
        return Err(invalid_interval(original));
    }

    let body = &original[1..]; // skip 'P'
    let (date_part, time_part) = if let Some(t_pos) = body.find(['T', 't']) {
        (&body[..t_pos], Some(&body[t_pos + 1..]))
    } else {
        (body, None)
    };

    let mut parts = IntervalParts::default();
    let mut component_count = parse_iso_components(date_part, false, original, &mut parts)?;
    if let Some(time_str) = time_part {
        if time_str.is_empty() {
            return Err(invalid_interval(original));
        }
        component_count += parse_iso_components(time_str, true, original, &mut parts)?;
    }
    if component_count == 0 {
        return Err(invalid_interval(original));
    }

    parts.finish(original)
}

fn try_parse_spanner_interval(s: &str) -> Option<DuckDBInterval> {
    // Spanner format: "P<years>-<months> <days> <hours>:<minutes>:<seconds>"
    parse_spanner_interval(s).ok()
}

#[derive(Default)]
struct IntervalParts {
    months: i128,
    days: i128,
    micros: i128,
}

impl IntervalParts {
    fn finish(self, s: &str) -> Result<DuckDBInterval, SpannerError> {
        Ok(DuckDBInterval {
            months: i32::try_from(self.months).map_err(|_| invalid_interval(s))?,
            days: i32::try_from(self.days).map_err(|_| invalid_interval(s))?,
            micros: i64::try_from(self.micros).map_err(|_| invalid_interval(s))?,
        })
    }
}

fn invalid_interval(s: &str) -> SpannerError {
    SpannerError::Conversion(format!("Invalid interval format: {s}"))
}

fn parse_spanner_interval(s: &str) -> Result<DuckDBInterval, SpannerError> {
    let original = s;
    let s = s
        .strip_prefix('P')
        .or_else(|| s.strip_prefix('p'))
        .ok_or_else(|| invalid_interval(original))?;
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 3 {
        return Err(invalid_interval(original));
    }

    let months = parse_spanner_year_month(parts[0], original)?;
    let days = parse_integer(parts[1], original)?;

    let hms: Vec<&str> = parts[2].split(':').collect();
    if hms.len() != 3 {
        return Err(invalid_interval(original));
    }
    let (negative_time, hours) = parse_group_sign(hms[0], original)?;
    let hours = parse_unsigned_integer(hours, original)?
        .checked_mul(3_600_000_000)
        .ok_or_else(|| invalid_interval(original))?;
    let minutes_value = parse_unsigned_integer(hms[1], original)?;
    if minutes_value >= 60 {
        return Err(invalid_interval(original));
    }
    let minutes = minutes_value
        .checked_mul(60_000_000)
        .ok_or_else(|| invalid_interval(original))?;
    let seconds_whole = hms[2].split_once('.').map_or(hms[2], |(whole, _)| whole);
    if parse_unsigned_integer(seconds_whole, original)? >= 60 {
        return Err(invalid_interval(original));
    }
    let seconds = parse_seconds_micros(hms[2], original)?;
    let micros_magnitude = hours
        .checked_add(minutes)
        .and_then(|value| value.checked_add(seconds))
        .ok_or_else(|| invalid_interval(original))?;
    let micros = if negative_time {
        micros_magnitude
            .checked_neg()
            .ok_or_else(|| invalid_interval(original))?
    } else {
        micros_magnitude
    };

    IntervalParts {
        months,
        days,
        micros,
    }
    .finish(original)
}

fn parse_integer(s: &str, original: &str) -> Result<i128, SpannerError> {
    s.parse::<i128>().map_err(|_| invalid_interval(original))
}

fn parse_unsigned_integer(s: &str, original: &str) -> Result<i128, SpannerError> {
    if s.is_empty() || !s.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid_interval(original));
    }
    parse_integer(s, original)
}

fn parse_group_sign<'a>(s: &'a str, original: &str) -> Result<(bool, &'a str), SpannerError> {
    let (negative, unsigned) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if unsigned.is_empty() {
        return Err(invalid_interval(original));
    }
    Ok((negative, unsigned))
}

fn parse_spanner_year_month(s: &str, original: &str) -> Result<i128, SpannerError> {
    let (negative, unsigned) = parse_group_sign(s, original)?;
    let (years, months) = unsigned
        .split_once('-')
        .ok_or_else(|| invalid_interval(original))?;
    if months.contains('-') {
        return Err(invalid_interval(original));
    }
    let years = parse_unsigned_integer(years, original)?;
    let months = parse_unsigned_integer(months, original)?;
    if months >= 12 {
        return Err(invalid_interval(original));
    }
    let magnitude = years
        .checked_mul(12)
        .and_then(|value| value.checked_add(months))
        .ok_or_else(|| invalid_interval(original))?;
    if negative {
        magnitude
            .checked_neg()
            .ok_or_else(|| invalid_interval(original))
    } else {
        Ok(magnitude)
    }
}

fn parse_seconds_micros(s: &str, original: &str) -> Result<i128, SpannerError> {
    let (negative, unsigned) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let (whole, fraction) = if let Some((whole, fraction)) = unsigned.split_once('.') {
        if fraction.is_empty() {
            return Err(invalid_interval(original));
        }
        (whole, fraction)
    } else {
        (unsigned, "")
    };
    // Spanner allows nanosecond precision. DuckDB intervals store microseconds,
    // so discard the final three digits deterministically instead of using a
    // floating-point conversion.
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || (!fraction.is_empty() && !fraction.bytes().all(|byte| byte.is_ascii_digit()))
        || fraction.len() > 9
    {
        return Err(invalid_interval(original));
    }

    let whole = whole
        .parse::<i128>()
        .map_err(|_| invalid_interval(original))?
        .checked_mul(1_000_000)
        .ok_or_else(|| invalid_interval(original))?;
    let fraction = if fraction.is_empty() {
        0
    } else {
        let micros_fraction = &fraction[..fraction.len().min(6)];
        let value = micros_fraction
            .parse::<i128>()
            .map_err(|_| invalid_interval(original))?;
        value
            .checked_mul(10_i128.pow((6 - micros_fraction.len()) as u32))
            .ok_or_else(|| invalid_interval(original))?
    };
    let magnitude = whole
        .checked_add(fraction)
        .ok_or_else(|| invalid_interval(original))?;
    if negative {
        magnitude
            .checked_neg()
            .ok_or_else(|| invalid_interval(original))
    } else {
        Ok(magnitude)
    }
}

fn parse_iso_components(
    s: &str,
    time: bool,
    original: &str,
    parts: &mut IntervalParts,
) -> Result<usize, SpannerError> {
    let bytes = s.as_bytes();
    let mut pos = 0;
    let mut last_rank = 0;
    let mut count = 0;

    while pos < bytes.len() {
        let start = pos;
        if matches!(bytes[pos], b'+' | b'-') {
            pos += 1;
        }
        let digits_start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }
        if digits_start == pos {
            return Err(invalid_interval(original));
        }
        if pos < bytes.len() && bytes[pos] == b'.' {
            pos += 1;
            let fraction_start = pos;
            while pos < bytes.len() && bytes[pos].is_ascii_digit() {
                pos += 1;
            }
            if fraction_start == pos {
                return Err(invalid_interval(original));
            }
        }
        if pos == bytes.len() {
            return Err(invalid_interval(original));
        }

        let number_end = pos;
        let unit = bytes[pos] as char;
        pos += 1;
        let rank = match (time, unit) {
            (false, 'Y' | 'y') | (true, 'H' | 'h') => 1,
            (false, 'M' | 'm') | (true, 'M' | 'm') => 2,
            (false, 'D' | 'd') | (true, 'S' | 's') => 3,
            _ => return Err(invalid_interval(original)),
        };
        if rank <= last_rank {
            return Err(invalid_interval(original));
        }
        let number = &s[start..number_end];
        let value = if unit == 'S' || unit == 's' {
            parse_seconds_micros(number, original)?
        } else {
            if number.contains('.') {
                return Err(invalid_interval(original));
            }
            parse_integer(number, original)?
        };
        match (time, unit) {
            (false, 'Y' | 'y') => {
                let value = value
                    .checked_mul(12)
                    .ok_or_else(|| invalid_interval(original))?;
                parts.months = parts
                    .months
                    .checked_add(value)
                    .ok_or_else(|| invalid_interval(original))?;
            }
            (false, 'M' | 'm') => {
                parts.months = parts
                    .months
                    .checked_add(value)
                    .ok_or_else(|| invalid_interval(original))?;
            }
            (false, 'D' | 'd') => {
                parts.days = parts
                    .days
                    .checked_add(value)
                    .ok_or_else(|| invalid_interval(original))?;
            }
            (true, 'H' | 'h') => {
                let value = value
                    .checked_mul(3_600_000_000)
                    .ok_or_else(|| invalid_interval(original))?;
                parts.micros = parts
                    .micros
                    .checked_add(value)
                    .ok_or_else(|| invalid_interval(original))?;
            }
            (true, 'M' | 'm') => {
                let value = value
                    .checked_mul(60_000_000)
                    .ok_or_else(|| invalid_interval(original))?;
                parts.micros = parts
                    .micros
                    .checked_add(value)
                    .ok_or_else(|| invalid_interval(original))?;
            }
            (true, 'S' | 's') => {
                parts.micros = parts
                    .micros
                    .checked_add(value)
                    .ok_or_else(|| invalid_interval(original))?;
            }
            _ => return Err(invalid_interval(original)),
        }
        last_rank = rank;
        count += 1;
    }

    Ok(count)
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
        assert_eq!(interval.months, -14);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.micros, 0);

        let interval = try_parse_spanner_interval("P0-0 0 -1:30:0").expect("parse");
        assert_eq!(interval.micros, -90 * 60 * 1_000_000);

        let interval = try_parse_spanner_interval("P0-0 0 -0:30:10").expect("parse");
        assert_eq!(interval.micros, -(30 * 60 + 10) * 1_000_000);
    }

    #[test]
    fn iso8601_negative_interval_round_trip() {
        let interval = parse_iso8601_interval("PT-1H-30M").expect("parse");
        assert_eq!(interval.micros, -90 * 60 * 1_000_000);
    }

    #[test]
    fn interval_parser_preserves_valid_iso_and_spanner_forms() {
        let interval = parse_iso8601_interval("P1Y2M3DT4H5M6S").expect("parse");
        assert_eq!(interval.months, 14);
        assert_eq!(interval.days, 3);
        assert_eq!(interval.micros, 14_706_000_000);

        let interval = parse_iso8601_interval("PT1H30M").expect("parse");
        assert_eq!(interval.micros, 90 * 60 * 1_000_000);

        let interval = parse_iso8601_interval("p1y2m3dt4h5m6s").expect("parse");
        assert_eq!(interval.months, 14);
        assert_eq!(interval.days, 3);
        assert_eq!(interval.micros, 14_706_000_000);

        let interval = parse_iso8601_interval("P0-3 0 0:0:0").expect("parse");
        assert_eq!(interval.months, 3);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.micros, 0);
    }

    #[test]
    fn interval_parser_rejects_empty_unknown_and_trailing_input() {
        for input in [
            "P",
            "PT",
            "Pgarbage",
            "P1Yjunk",
            "P1Y2Mgarbage",
            "PT1Hjunk",
            "PT1H2H",
            "P1D2Y",
        ] {
            assert!(
                parse_iso8601_interval(input).is_err(),
                "expected {input:?} to be rejected"
            );
        }
    }

    #[test]
    fn interval_parser_rejects_non_finite_and_more_than_nanosecond_precision() {
        for input in [
            "PTNaNS",
            "PTInfinityS",
            "PT-InfinityS",
            "P0-0 0 0:0:NaNS",
            "P0-0 0 0:0:1.",
            "PT0.0000000001S",
            "PT1.1234567890S",
            "P0-0 0 0:0:1.1234567890",
            "P0-12 0 0:0:0",
            "P0-0 0 0:60:0",
            "P0-0 0 0:0:60",
            "P0-0 0 0:-1:0",
        ] {
            assert!(
                parse_iso8601_interval(input).is_err(),
                "expected {input:?} to be rejected"
            );
        }

        let interval = parse_iso8601_interval("PT1.123456S").expect("parse");
        assert_eq!(interval.micros, 1_123_456);

        let interval = parse_iso8601_interval("PT1.123456789S").expect("parse");
        assert_eq!(interval.micros, 1_123_456);

        let interval =
            parse_iso8601_interval("P0-0 0 0:0:1.123456789").expect("parse Spanner form");
        assert_eq!(interval.micros, 1_123_456);

        let interval = parse_iso8601_interval("PT-0.000000001S").expect("parse");
        assert_eq!(interval.micros, 0);
    }

    #[test]
    fn interval_parser_checks_fraction_and_integer_boundaries() {
        let interval = parse_iso8601_interval("PT9223372036854.775807S").expect("parse");
        assert_eq!(interval.micros, i64::MAX);

        let interval = parse_iso8601_interval("PT-9223372036854.775808S").expect("parse");
        assert_eq!(interval.micros, i64::MIN);

        for input in [
            "PT9223372036854.775808S",
            "PT-9223372036854.775809S",
            "P178956971Y",
            "P0-0 2147483648 0:0:0",
            "PT2562047789H",
            "P0-0 0 0:153722867281:0",
            "P0-0 0 2562047789:0:0",
        ] {
            assert!(
                parse_iso8601_interval(input).is_err(),
                "expected {input:?} to be rejected"
            );
        }
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

    const LIST_ENTRY_SENTINEL: (usize, usize) = (7, 11);

    fn write_list_one_with_sentinel_entry(
        child_logical_type: LogicalTypeHandle,
        element_type: Type,
        value: SpannerValue,
        column_name: &str,
    ) -> (DataChunkHandle, Result<(), SpannerError>) {
        let chunk = DataChunkHandle::new(&[LogicalTypeHandle::list(&child_logical_type)]);
        chunk.set_len(1);
        let result = {
            let mut vector = chunk.list_vector(0);
            vector.set_entry(0, LIST_ENTRY_SENTINEL.0, LIST_ENTRY_SENTINEL.1);
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

    fn assert_sentinel_list_unpublished(chunk: &DataChunkHandle) {
        let list = chunk.list_vector(0);
        assert_eq!(list.len(), 0, "failed conversion changed list length");
        assert_eq!(
            list.get_entry(0),
            LIST_ENTRY_SENTINEL,
            "failed conversion overwrote the parent list entry"
        );
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
    fn array_of_struct_array_fields_preserves_offsets_nulls_and_reuse() {
        fn proto_list(values: Vec<prost_types::Value>) -> prost_types::Value {
            prost_types::Value {
                kind: Some(ProtoKind::ListValue(prost_types::ListValue { values })),
            }
        }

        fn proto_int(value: i64) -> prost_types::Value {
            prost_types::Value {
                kind: Some(ProtoKind::StringValue(value.to_string())),
            }
        }

        fn proto_null() -> prost_types::Value {
            prost_types::Value {
                kind: Some(ProtoKind::NullValue(0)),
            }
        }

        fn struct_value(
            direct: prost_types::Value,
            nested: prost_types::Value,
        ) -> prost_types::Value {
            proto_list(vec![direct, proto_list(vec![nested])])
        }

        let nested_type = struct_type_proto(vec![(
            "values",
            array_type_proto(scalar_type(TypeCode::Int64)),
        )]);
        let element_type = struct_type_proto(vec![
            ("direct", array_type_proto(scalar_type(TypeCode::Int64))),
            ("nested", nested_type),
        ]);
        let logical_nested = LogicalTypeHandle::struct_type(&[(
            "values",
            LogicalTypeHandle::list(&LogicalTypeId::Bigint.into()),
        )]);
        let logical_struct = LogicalTypeHandle::struct_type(&[
            (
                "direct",
                LogicalTypeHandle::list(&LogicalTypeId::Bigint.into()),
            ),
            ("nested", logical_nested),
        ]);
        let chunk = DataChunkHandle::new(&[LogicalTypeHandle::list(&logical_struct)]);
        chunk.set_len(1);

        let first = value_of(ProtoKind::ListValue(prost_types::ListValue {
            values: vec![
                struct_value(
                    proto_list(vec![proto_int(10), proto_int(11)]),
                    proto_list(vec![proto_int(100)]),
                ),
                struct_value(proto_null(), proto_null()),
                struct_value(
                    proto_list(vec![proto_int(20)]),
                    proto_list(vec![proto_int(200), proto_int(201)]),
                ),
            ],
        }));
        {
            let mut list = chunk.list_vector(0);
            write_raw_list_value(
                &mut list,
                rows_capacity_hint(),
                0,
                &first,
                &element_type,
                &|| "column 'items', row 0".to_string(),
            )
            .unwrap();
        }

        {
            let outer = chunk.list_vector(0);
            assert_eq!(outer.get_entry(0), (0, 3));
            assert_eq!(outer.len(), 3);
            let structs = outer.struct_child(3);

            let direct_validity = structs.child(0, 3);
            let direct = structs.list_vector_child(0);
            assert_eq!(direct.get_entry(0), (0, 2));
            assert_eq!(direct.get_entry(1), (2, 0));
            assert_eq!(direct.get_entry(2), (2, 1));
            assert_eq!(direct.len(), 3);
            assert!(direct_validity.row_is_null(1));
            assert_eq!(unsafe { direct.child(3).as_slice::<i64>() }, &[10, 11, 20]);

            let nested_struct = structs.struct_vector_child(1);
            let nested_validity = nested_struct.child(0, 3);
            let nested = nested_struct.list_vector_child(0);
            assert_eq!(nested.get_entry(0), (0, 1));
            assert_eq!(nested.get_entry(1), (1, 0));
            assert_eq!(nested.get_entry(2), (1, 2));
            assert_eq!(nested.len(), 3);
            assert!(nested_validity.row_is_null(1));
            assert_eq!(
                unsafe { nested.child(3).as_slice::<i64>() },
                &[100, 200, 201]
            );
        }

        // Reuse the same output storage for a shorter batch. The write path must
        // reset both direct and recursively nested list append cursors.
        chunk.list_vector(0).set_len(0);
        let second = value_of(ProtoKind::ListValue(prost_types::ListValue {
            values: vec![
                struct_value(
                    proto_list(vec![proto_int(30)]),
                    proto_list(vec![proto_int(300), proto_int(301)]),
                ),
                struct_value(proto_null(), proto_null()),
            ],
        }));
        {
            let mut list = chunk.list_vector(0);
            write_raw_list_value(
                &mut list,
                rows_capacity_hint(),
                0,
                &second,
                &element_type,
                &|| "column 'items', row 0".to_string(),
            )
            .unwrap();
        }

        let outer = chunk.list_vector(0);
        assert_eq!(outer.get_entry(0), (0, 2));
        assert_eq!(outer.len(), 2);
        let structs = outer.struct_child(2);

        let direct_validity = structs.child(0, 2);
        let direct = structs.list_vector_child(0);
        assert_eq!(direct.get_entry(0), (0, 1));
        assert_eq!(direct.get_entry(1), (1, 0));
        assert_eq!(direct.len(), 1);
        assert!(direct_validity.row_is_null(1));
        assert_eq!(unsafe { direct.child(1).as_slice::<i64>() }, &[30]);

        let nested_struct = structs.struct_vector_child(1);
        let nested_validity = nested_struct.child(0, 2);
        let nested = nested_struct.list_vector_child(0);
        assert_eq!(nested.get_entry(0), (0, 2));
        assert_eq!(nested.get_entry(1), (2, 0));
        assert_eq!(nested.len(), 2);
        assert!(nested_validity.row_is_null(1));
        assert_eq!(unsafe { nested.child(2).as_slice::<i64>() }, &[300, 301]);
    }

    #[test]
    fn array_of_struct_array_fields_reserve_more_than_one_standard_vector() {
        fn proto_list(values: Vec<prost_types::Value>) -> prost_types::Value {
            prost_types::Value {
                kind: Some(ProtoKind::ListValue(prost_types::ListValue { values })),
            }
        }

        let count = rows_capacity_hint() + 1;
        let nested_struct_type = struct_type_proto(vec![(
            "values",
            array_type_proto(scalar_type(TypeCode::Int64)),
        )]);
        let struct_type = struct_type_proto(vec![
            ("direct", array_type_proto(scalar_type(TypeCode::Int64))),
            ("nested", nested_struct_type),
        ]);
        let logical_nested_struct = LogicalTypeHandle::struct_type(&[(
            "values",
            LogicalTypeHandle::list(&LogicalTypeId::Bigint.into()),
        )]);
        let logical_struct = LogicalTypeHandle::struct_type(&[
            (
                "direct",
                LogicalTypeHandle::list(&LogicalTypeId::Bigint.into()),
            ),
            ("nested", logical_nested_struct),
        ]);
        let struct_value = prost_types::Value {
            kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                values: vec![
                    proto_list(Vec::new()),
                    proto_list(vec![proto_list(Vec::new())]),
                ],
            })),
        };
        let value = value_of(ProtoKind::ListValue(prost_types::ListValue {
            values: vec![struct_value; count],
        }));

        let (chunk, result) = write_list_one(logical_struct, struct_type, value, "items");
        result.unwrap();

        let outer = chunk.list_vector(0);
        assert_eq!(outer.get_entry(0), (0, count));
        let structs = outer.struct_child(count);
        let direct = structs.list_vector_child(0);
        assert_eq!(direct.len(), 0);
        assert_eq!(direct.get_entry(count - 1), (0, 0));
        let nested_struct = structs.struct_vector_child(1);
        let nested = nested_struct.list_vector_child(0);
        assert_eq!(nested.len(), 0);
        assert_eq!(nested.get_entry(count - 1), (0, 0));
    }

    #[test]
    fn deep_nested_array_reserves_more_than_one_standard_vector() {
        let count = rows_capacity_hint() + 1;
        let inner_arrays = (0..count)
            .map(|_| prost_types::Value {
                kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                    values: Vec::new(),
                })),
            })
            .collect();
        let value = value_of(ProtoKind::ListValue(prost_types::ListValue {
            values: vec![prost_types::Value {
                kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                    values: inner_arrays,
                })),
            }],
        }));
        let logical_inner =
            LogicalTypeHandle::list(&LogicalTypeHandle::list(&LogicalTypeId::Bigint.into()));

        let (chunk, result) = write_list_one(
            logical_inner,
            array_type_proto(array_type_proto(scalar_type(TypeCode::Int64))),
            value,
            "nested",
        );
        result.unwrap();

        let outer = chunk.list_vector(0);
        assert_eq!(outer.get_entry(0), (0, 1));
        let middle = outer.list_child();
        assert_eq!(middle.get_entry(0), (0, count));
        let inner = middle.list_child();
        assert_eq!(inner.len(), 0);
        assert_eq!(inner.get_entry(count - 1), (0, 0));
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
    fn oversized_flattened_child_count_returns_conversion_error() {
        let too_many = MAX_FLATTENED_CHILDREN + 1;
        let result = checked_child_count(0, too_many, &|| "test value".to_string());
        match result {
            Err(SpannerError::Conversion(message)) => {
                assert!(message.contains("64 MiB flattened result budget"));
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
    fn numeric_rejects_i128_value_outside_decimal_38_domain() {
        let value = "100000000000000000000000000000.000000000";
        let (_chunk, result) = write_scalar_one(
            LogicalTypeHandle::decimal(38, 9),
            scalar_type(TypeCode::Numeric),
            value_of(ProtoKind::StringValue(value.to_string())),
            "amount",
        );
        match result {
            Err(SpannerError::Conversion(message)) => {
                assert!(message.contains("DECIMAL(38,9)"), "message: {message}");
                assert!(message.contains("39 digits"), "message: {message}");
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
    fn unknown_scalar_type_errors_even_for_null_values() {
        let unknown_type = scalar_type(TypeCode::Unknown(99));
        let preflight_error = match PreflightState::for_type(&unknown_type, &|| {
            "column 'future_value'".to_string()
        }) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("unknown result type passed conversion preflight"),
        };
        assert!(preflight_error.contains("Unknown(99)"), "{preflight_error}");

        for kind in [
            ProtoKind::StringValue("value".to_string()),
            ProtoKind::NullValue(0),
        ] {
            let (_chunk, result) = write_scalar_one(
                LogicalTypeId::Varchar.into(),
                unknown_type.clone(),
                value_of(kind),
                "future_value",
            );
            match result {
                Err(SpannerError::Conversion(message)) => {
                    assert!(message.contains("Unknown(99)"), "message: {message}");
                    assert!(
                        message.contains("column 'future_value', row 0"),
                        "message: {message}"
                    );
                }
                other => panic!("expected Conversion error, got {other:?}"),
            }
        }
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
    fn deep_list_type_error_leaves_parent_list_unpublished() {
        let element_type = struct_type_proto(vec![(
            "flags",
            array_type_proto(scalar_type(TypeCode::Bool)),
        )]);
        let logical_struct = LogicalTypeHandle::struct_type(&[(
            "flags",
            LogicalTypeHandle::list(&LogicalTypeId::Boolean.into()),
        )]);
        let value = value_of(ProtoKind::ListValue(prost_types::ListValue {
            values: vec![prost_types::Value {
                kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                            values: vec![
                                prost_types::Value {
                                    kind: Some(ProtoKind::BoolValue(true)),
                                },
                                prost_types::Value {
                                    kind: Some(ProtoKind::StringValue("nope".to_string())),
                                },
                            ],
                        })),
                    }],
                })),
            }],
        }));

        let (chunk, result) =
            write_list_one_with_sentinel_entry(logical_struct, element_type, value, "items");
        match result {
            Err(SpannerError::Conversion(message)) => {
                assert!(message.contains("BOOL"), "message: {message}");
                assert!(message.contains("String"), "message: {message}");
                assert!(message.contains("field 'flags'"), "message: {message}");
                assert!(message.contains("array element 1"), "message: {message}");
            }
            other => panic!("expected Conversion error, got {other:?}"),
        }
        assert_sentinel_list_unpublished(&chunk);
    }

    #[test]
    fn deep_struct_metadata_error_leaves_parent_list_unpublished() {
        let malformed_nested_struct: Type = model::Type::new()
            .set_code(model::TypeCode::Struct)
            .set_struct_type(
                model::StructType::new()
                    .set_fields(vec![model::struct_type::Field::new().set_name("broken")]),
            )
            .into();
        let element_type = struct_type_proto(vec![("payload", malformed_nested_struct)]);
        let logical_nested =
            LogicalTypeHandle::struct_type(&[("broken", LogicalTypeId::Boolean.into())]);
        let logical_struct = LogicalTypeHandle::struct_type(&[("payload", logical_nested)]);
        let value = value_of(ProtoKind::ListValue(prost_types::ListValue {
            values: vec![prost_types::Value {
                kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                    values: vec![prost_types::Value {
                        kind: Some(ProtoKind::ListValue(prost_types::ListValue {
                            values: vec![prost_types::Value {
                                kind: Some(ProtoKind::BoolValue(true)),
                            }],
                        })),
                    }],
                })),
            }],
        }));

        let (chunk, result) =
            write_list_one_with_sentinel_entry(logical_struct, element_type, value, "items");
        match result {
            Err(SpannerError::Conversion(message)) => {
                assert!(
                    message.contains("STRUCT field 'broken' without type"),
                    "message: {message}"
                );
                assert!(message.contains("field 'payload'"), "message: {message}");
                assert!(message.contains("column 'items'"), "message: {message}");
            }
            other => panic!("expected Conversion error, got {other:?}"),
        }
        assert_sentinel_list_unpublished(&chunk);
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
    fn struct_value_arity_mismatch_returns_conversion_error() {
        let spanner_type = struct_type_proto(vec![
            ("first", scalar_type(TypeCode::Int64)),
            ("second", scalar_type(TypeCode::Int64)),
        ]);

        for values in [
            vec![prost_types::Value {
                kind: Some(ProtoKind::StringValue("1".to_string())),
            }],
            vec![
                prost_types::Value {
                    kind: Some(ProtoKind::StringValue("1".to_string())),
                },
                prost_types::Value {
                    kind: Some(ProtoKind::StringValue("2".to_string())),
                },
                prost_types::Value {
                    kind: Some(ProtoKind::StringValue("3".to_string())),
                },
            ],
        ] {
            let struct_value = value_of(ProtoKind::ListValue(prost_types::ListValue { values }));
            let logical = LogicalTypeHandle::struct_type(&[
                ("first", LogicalTypeId::Bigint.into()),
                ("second", LogicalTypeId::Bigint.into()),
            ]);
            let (_chunk, result) =
                write_struct_one(logical, spanner_type.clone(), struct_value, "payload");
            match result {
                Err(SpannerError::Conversion(message)) => {
                    assert!(message.contains("STRUCT value has"), "message: {message}");
                    assert!(message.contains("for 2 fields"), "message: {message}");
                    assert!(
                        message.contains("column 'payload', row 0"),
                        "message: {message}"
                    );
                }
                other => panic!("expected Conversion error, got {other:?}"),
            }
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
