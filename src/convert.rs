use duckdb::core::{DataChunkHandle, Inserter, ListVector, StructVector};
use duckdb::ffi;
use google_cloud_googleapis::spanner::v1::{Type, TypeCode};
use google_cloud_spanner::row::{Row, TryFromValue};
use prost_types::value::Kind;
use time::OffsetDateTime;

use crate::error::SpannerError;
use crate::schema::ColumnInfo;

const EPOCH_DATE: time::Date = time::macros::date!(1970 - 01 - 01);

/// Extract the raw `duckdb_vector` handle from a FlatVector.
///
/// FlatVector wraps `{ptr: duckdb_vector, capacity: usize}` with `ptr` as the
/// first field. We read the first pointer-sized value. A debug assertion verifies
/// correctness by cross-checking with `duckdb_vector_get_data`.
unsafe fn flat_vector_raw(vector: &duckdb::core::FlatVector) -> ffi::duckdb_vector {
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
/// - The caller must guarantee the input bytes are valid UTF-8 (Spanner does this)
unsafe fn unsafe_assign_string(vector: &mut duckdb::core::FlatVector, idx: usize, s: &str) {
    let raw_vector = flat_vector_raw(vector);
    ffi::duckdb_unsafe_vector_assign_string_element_len(
        raw_vector,
        idx as u64,
        s.as_ptr() as *const i8,
        s.len() as u64,
    );
}

/// Newtype wrapper for extracting raw prost_types::Value from a Spanner Row.
/// All type conversions go through this: Row → RawValue (proto) → DuckDB vector.
pub struct RawValue(pub prost_types::Value);

impl TryFromValue for RawValue {
    fn try_from(value: &prost_types::Value, _field: &google_cloud_googleapis::spanner::v1::struct_type::Field) -> Result<Self, google_cloud_spanner::row::Error> {
        Ok(RawValue(value.clone()))
    }
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

    for (col_idx, col) in columns.iter().enumerate() {
        write_column_from_rows(output, col_idx, rows, col_idx, &col.spanner_type)?;
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
pub fn write_column_from_rows(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[Row],
    spanner_col_idx: usize,
    spanner_type: &Type,
) -> Result<(), SpannerError> {
    let type_code = TypeCode::try_from(spanner_type.code).unwrap_or(TypeCode::Unspecified);

    match type_code {
        TypeCode::Array => {
            write_array_column(output, output_col_idx, rows, spanner_col_idx, spanner_type)
        }
        TypeCode::Struct => {
            write_struct_column(output, output_col_idx, rows, spanner_col_idx, spanner_type)
        }
        _ => write_scalar_column(output, output_col_idx, rows, spanner_col_idx, type_code),
    }
}

/// Write a column of scalar values by extracting raw proto values from each Row.
fn write_scalar_column(
    output: &mut DataChunkHandle,
    output_col_idx: usize,
    rows: &[Row],
    spanner_col_idx: usize,
    type_code: TypeCode,
) -> Result<(), SpannerError> {
    let mut vector = output.flat_vector(output_col_idx);
    for (i, row) in rows.iter().enumerate() {
        match row.column::<Option<RawValue>>(spanner_col_idx)? {
            None => vector.set_null(i),
            Some(RawValue(val)) => {
                write_raw_scalar_to_flat(&mut vector, i, &val, type_code)?;
            }
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
) -> Result<(), SpannerError> {
    let element_type = spanner_type
        .array_element_type
        .as_ref()
        .ok_or_else(|| SpannerError::Conversion("ARRAY without element type".to_string()))?;

    let mut list_vector = output.list_vector(output_col_idx);

    // First pass: collect all raw values and compute total child count
    let mut raw_values: Vec<Option<Vec<prost_types::Value>>> = Vec::with_capacity(rows.len());
    let mut total_children = 0usize;

    for row in rows {
        match row.column::<Option<RawValue>>(spanner_col_idx)? {
            None => raw_values.push(None),
            Some(RawValue(val)) => {
                if let Some(Kind::ListValue(list)) = val.kind {
                    total_children += list.values.len();
                    raw_values.push(Some(list.values));
                } else if let Some(Kind::NullValue(_)) = val.kind {
                    raw_values.push(None);
                } else {
                    raw_values.push(Some(Vec::new()));
                }
            }
        }
    }

    // Set list entries and write child values
    let mut offset = 0usize;
    let elem_type_code =
        TypeCode::try_from(element_type.code).unwrap_or(TypeCode::Unspecified);

    // For nested types, handle differently
    match elem_type_code {
        TypeCode::Struct => {
            for (i, opt_values) in raw_values.iter().enumerate() {
                match opt_values {
                    None => {
                        list_vector.set_entry(i, offset, 0);
                        list_vector.set_null(i);
                    }
                    Some(values) => {
                        list_vector.set_entry(i, offset, values.len());
                        offset += values.len();
                    }
                }
            }
            list_vector.set_len(total_children);
            // Write struct children
            let struct_type = element_type
                .struct_type
                .as_ref()
                .ok_or_else(|| SpannerError::Conversion("STRUCT without struct_type".to_string()))?;
            let mut child_struct = list_vector.struct_child(total_children);
            let mut flat_idx = 0usize;
            for values in raw_values.iter().flatten() {
                for val in values {
                    write_raw_struct_value(&mut child_struct, flat_idx, val, struct_type)?;
                    flat_idx += 1;
                }
            }
        }
        TypeCode::Array => {
            // Nested arrays - write each element as a list within the child
            for (i, opt_values) in raw_values.iter().enumerate() {
                match opt_values {
                    None => {
                        list_vector.set_entry(i, offset, 0);
                        list_vector.set_null(i);
                    }
                    Some(values) => {
                        list_vector.set_entry(i, offset, values.len());
                        offset += values.len();
                    }
                }
            }
            list_vector.set_len(total_children);
            let inner_elem_type = element_type.array_element_type.as_ref().ok_or_else(|| {
                SpannerError::Conversion("Nested ARRAY without element type".to_string())
            })?;
            let mut child_list = list_vector.list_child();
            let mut flat_idx = 0usize;
            for values in raw_values.iter().flatten() {
                for val in values {
                    write_raw_list_value(&mut child_list, flat_idx, val, inner_elem_type)?;
                    flat_idx += 1;
                }
            }
        }
        _ => {
            // Scalar element type - use FlatVector child
            for (i, opt_values) in raw_values.iter().enumerate() {
                match opt_values {
                    None => {
                        list_vector.set_entry(i, offset, 0);
                        list_vector.set_null(i);
                    }
                    Some(values) => {
                        list_vector.set_entry(i, offset, values.len());
                        offset += values.len();
                    }
                }
            }
            list_vector.set_len(total_children);
            let mut child = list_vector.child(total_children);
            let mut flat_idx = 0usize;
            for values in raw_values.iter().flatten() {
                for val in values {
                    write_raw_scalar_to_flat(&mut child, flat_idx, val, elem_type_code)?;
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
) -> Result<(), SpannerError> {
    let struct_type = spanner_type
        .struct_type
        .as_ref()
        .ok_or_else(|| SpannerError::Conversion("STRUCT without struct_type".to_string()))?;

    let mut struct_vector = output.struct_vector(output_col_idx);

    for (i, row) in rows.iter().enumerate() {
        match row.column::<Option<RawValue>>(spanner_col_idx)? {
            None => struct_vector.set_null(i),
            Some(RawValue(val)) => {
                write_raw_struct_value(&mut struct_vector, i, &val, struct_type)?;
            }
        }
    }
    Ok(())
}

/// Write a single raw proto Value as a struct into a StructVector at the given row index.
fn write_raw_struct_value(
    struct_vector: &mut StructVector,
    row_idx: usize,
    value: &prost_types::Value,
    struct_type: &google_cloud_googleapis::spanner::v1::StructType,
) -> Result<(), SpannerError> {
    let values = match &value.kind {
        Some(Kind::ListValue(list)) => &list.values,
        Some(Kind::NullValue(_)) | None => {
            struct_vector.set_null(row_idx);
            return Ok(());
        }
        _ => {
            struct_vector.set_null(row_idx);
            return Ok(());
        }
    };

    for (field_idx, field) in struct_type.fields.iter().enumerate() {
        let field_type = field
            .r#type
            .as_ref()
            .ok_or_else(|| SpannerError::Conversion("STRUCT field without type".to_string()))?;
        let field_type_code =
            TypeCode::try_from(field_type.code).unwrap_or(TypeCode::Unspecified);

        let field_value = values.get(field_idx);

        match field_type_code {
            TypeCode::Struct => {
                let mut child = struct_vector.struct_vector_child(field_idx);
                if let Some(val) = field_value {
                    let inner_struct_type = field_type.struct_type.as_ref().ok_or_else(|| {
                        SpannerError::Conversion("Nested STRUCT without struct_type".to_string())
                    })?;
                    write_raw_struct_value(&mut child, row_idx, val, inner_struct_type)?;
                } else {
                    child.set_null(row_idx);
                }
            }
            TypeCode::Array => {
                let mut child = struct_vector.list_vector_child(field_idx);
                if let Some(val) = field_value {
                    let elem_type = field_type.array_element_type.as_ref().ok_or_else(|| {
                        SpannerError::Conversion("ARRAY without element type".to_string())
                    })?;
                    write_raw_list_value(&mut child, row_idx, val, elem_type)?;
                } else {
                    child.set_null(row_idx);
                }
            }
            _ => {
                let mut child = struct_vector.child(field_idx, rows_capacity_hint());
                if let Some(val) = field_value {
                    write_raw_scalar_to_flat(&mut child, row_idx, val, field_type_code)?;
                } else {
                    child.set_null(row_idx);
                }
            }
        }
    }
    Ok(())
}

/// Write a single raw proto Value as a list entry into a ListVector at the given row index.
fn write_raw_list_value(
    list_vector: &mut ListVector,
    row_idx: usize,
    value: &prost_types::Value,
    element_type: &Type,
) -> Result<(), SpannerError> {
    let values = match &value.kind {
        Some(Kind::ListValue(list)) => &list.values,
        Some(Kind::NullValue(_)) | None => {
            list_vector.set_null(row_idx);
            list_vector.set_entry(row_idx, 0, 0);
            return Ok(());
        }
        _ => {
            list_vector.set_null(row_idx);
            list_vector.set_entry(row_idx, 0, 0);
            return Ok(());
        }
    };

    let elem_type_code =
        TypeCode::try_from(element_type.code).unwrap_or(TypeCode::Unspecified);

    let current_len = list_vector.len();
    list_vector.set_entry(row_idx, current_len, values.len());

    match elem_type_code {
        TypeCode::Struct => {
            let struct_type = element_type.struct_type.as_ref().ok_or_else(|| {
                SpannerError::Conversion("STRUCT without struct_type".to_string())
            })?;
            let new_len = current_len + values.len();
            list_vector.set_len(new_len);
            let mut child_struct = list_vector.struct_child(new_len);
            for (j, val) in values.iter().enumerate() {
                write_raw_struct_value(&mut child_struct, current_len + j, val, struct_type)?;
            }
        }
        _ => {
            let new_len = current_len + values.len();
            list_vector.set_len(new_len);
            let mut child = list_vector.child(new_len);
            for (j, val) in values.iter().enumerate() {
                write_raw_scalar_to_flat(&mut child, current_len + j, val, elem_type_code)?;
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
/// Uses `unsafe { *vector.as_mut_ptr::<T>().add(idx) = v }` for numeric types because
/// duckdb-rs does not provide a safe setter for fixed-size types (only `insert()` for
/// varchar/blob). The caller guarantees `idx < batch_size` where batch_size was set via
/// `output.set_len()`, so the write is within the vector's allocated capacity.
///
/// TODO: Replace with safe API when duckdb-rs provides a safe setter for
/// fixed-size types. See duckdb/duckdb-rs#414 (vtab safety RFC).
fn write_raw_scalar_to_flat(
    vector: &mut duckdb::core::FlatVector,
    idx: usize,
    value: &prost_types::Value,
    type_code: TypeCode,
) -> Result<(), SpannerError> {
    match &value.kind {
        Some(Kind::NullValue(_)) | None => {
            vector.set_null(idx);
            return Ok(());
        }
        _ => {}
    }

    match type_code {
        TypeCode::Bool => {
            if let Some(Kind::BoolValue(v)) = &value.kind {
                unsafe { *vector.as_mut_ptr::<bool>().add(idx) = *v }
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::Int64 | TypeCode::Enum => {
            if let Some(Kind::StringValue(s)) = &value.kind {
                let v: i64 = s.parse().map_err(|e| {
                    SpannerError::Conversion(format!("INT64 parse error: {e}"))
                })?;
                unsafe { *vector.as_mut_ptr::<i64>().add(idx) = v }
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::Float32 => {
            if let Some(Kind::NumberValue(v)) = &value.kind {
                unsafe { *vector.as_mut_ptr::<f32>().add(idx) = *v as f32 }
            } else if let Some(Kind::StringValue(s)) = &value.kind {
                // Handle NaN, Infinity, -Infinity
                let v: f32 = s.parse().map_err(|e| {
                    SpannerError::Conversion(format!("FLOAT32 parse error: {e}"))
                })?;
                unsafe { *vector.as_mut_ptr::<f32>().add(idx) = v }
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::Float64 => {
            if let Some(Kind::NumberValue(v)) = &value.kind {
                unsafe { *vector.as_mut_ptr::<f64>().add(idx) = *v }
            } else if let Some(Kind::StringValue(s)) = &value.kind {
                let v: f64 = s.parse().map_err(|e| {
                    SpannerError::Conversion(format!("FLOAT64 parse error: {e}"))
                })?;
                unsafe { *vector.as_mut_ptr::<f64>().add(idx) = v }
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::Numeric => {
            if let Some(Kind::StringValue(s)) = &value.kind {
                use bigdecimal::{BigDecimal, ToPrimitive};
                use std::str::FromStr;
                let bd = BigDecimal::from_str(s).map_err(|e| {
                    SpannerError::Conversion(format!("NUMERIC parse error: {e}"))
                })?;
                // DuckDB DECIMAL(38,9): fixed-point i128 with scale=9.
                // with_scale(9) shifts to 9 decimal places, into_bigint_and_scale
                // gives the unscaled BigInt directly (no string round-trip).
                let (unscaled, _scale) = bd.with_scale(9).into_bigint_and_scale();
                let int_val: i128 = unscaled.to_i128().ok_or_else(|| {
                    SpannerError::Conversion(format!("NUMERIC value out of i128 range: {s}"))
                })?;
                unsafe { *vector.as_mut_ptr::<i128>().add(idx) = int_val }
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::String | TypeCode::Json => {
            if let Some(Kind::StringValue(s)) = &value.kind {
                // SAFETY: Spanner guarantees STRING and JSON values are valid UTF-8.
                unsafe { unsafe_assign_string(vector, idx, s) }
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::Bytes | TypeCode::Proto => {
            if let Some(Kind::StringValue(s)) = &value.kind {
                let bytes = base64_decode(s)?;
                vector.insert(idx, bytes.as_slice());
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::Date => {
            if let Some(Kind::StringValue(s)) = &value.kind {
                let format = time::format_description::well_known::Iso8601::DATE;
                let date = time::Date::parse(s, &format).map_err(|e| {
                    SpannerError::Conversion(format!("DATE parse error: {e}"))
                })?;
                let days = (date - EPOCH_DATE).whole_days() as i32;
                unsafe { *vector.as_mut_ptr::<i32>().add(idx) = days }
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::Timestamp => {
            if let Some(Kind::StringValue(s)) = &value.kind {
                let ts = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .map_err(|e| {
                        SpannerError::Conversion(format!("TIMESTAMP parse error: {e}"))
                    })?;
                let micros = ts.unix_timestamp() * 1_000_000 + (ts.nanosecond() / 1_000) as i64;
                unsafe { *vector.as_mut_ptr::<i64>().add(idx) = micros }
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::Uuid => {
            if let Some(Kind::StringValue(s)) = &value.kind {
                let parsed = uuid::Uuid::parse_str(s).map_err(|e| {
                    SpannerError::Conversion(format!("UUID parse error: {e}"))
                })?;
                // DuckDB stores UUIDs as hugeint with the MSB flipped for sort ordering.
                // See: duckdb/src/common/types/uuid.cpp — UUIDToUHugeint / UHugeintToUUID.
                // See also: duckdb/duckdb-rs#519 (UUID value correctness),
                //           duckdb/duckdb-rs#585 (no duckdb_bind_uuid in C API).
                let val = parsed.as_u128() ^ (1u128 << 127);
                unsafe { *vector.as_mut_ptr::<u128>().add(idx) = val }
            } else {
                vector.set_null(idx);
            }
        }
        TypeCode::Interval => {
            if let Some(Kind::StringValue(s)) = &value.kind {
                let interval = parse_iso8601_interval(s)?;
                unsafe { *vector.as_mut_ptr::<DuckDBInterval>().add(idx) = interval }
            } else {
                vector.set_null(idx);
            }
        }
        _ => {
            // Unknown: try as string
            if let Some(Kind::StringValue(s)) = &value.kind {
                // SAFETY: Spanner only returns valid UTF-8 string values.
                unsafe { unsafe_assign_string(vector, idx, s) }
            } else {
                vector.set_null(idx);
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

    // Parse "years-months"
    let ym: Vec<&str> = parts[0].split('-').collect();
    if ym.len() != 2 {
        return None;
    }
    let years: i32 = ym[0].parse().ok()?;
    let months: i32 = ym[1].parse().ok()?;

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

/// Hint for child vector capacity. DuckDB's vector standard size.
fn rows_capacity_hint() -> usize {
    2048
}
