use crate::{duck::DataChunkHandle, error::SpannerError, types::is_pg_numeric};
use duckdb_neo::{
    ToValue,
    connection::FFILink,
    value::{Value, ValueInput},
};
use google_cloud_spanner::{
    model,
    result::Row,
    types::{Type, TypeCode},
    value::{Kind, Value as SpannerValue},
};
use time::OffsetDateTime;
const EPOCH_DATE: time::Date = time::macros::date!(1970 - 01 - 01);
const MAX_FLATTENED_CHILDREN: usize = 64 * 1024 * 1024 / 16;
fn checked_duckdb_index(
    value: usize,
    description: &str,
    context: &dyn Fn() -> String,
) -> Result<u64, SpannerError> {
    u64::try_from(value).map_err(|_| {
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

pub(crate) fn write_preflighted_column_from_rows<R: ConversionRow>(
    output: &mut DataChunkHandle,
    col: usize,
    rows: &[R],
    source: usize,
    ty: &Type,
    name: &str,
) -> Result<(), SpannerError> {
    if rows.len() > output.capacity()? {
        return Err(SpannerError::Conversion(format!(
            "column '{name}': row count exceeds output capacity"
        )));
    }
    let ctx = output.context;
    let values = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            decode_value(ctx, row_value(row, source, name, i)?, ty, &|| {
                format!("column '{name}', row {i}")
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    output.set_len(rows.len())?;
    for (row, value) in values.into_iter().enumerate() {
        output.write(col, row, value)?;
    }
    Ok(())
}
fn decode_value(
    ctx: &dyn FFILink,
    value: &SpannerValue,
    ty: &Type,
    location: &dyn Fn() -> String,
) -> Result<Value, SpannerError> {
    if value.kind() == Kind::Null {
        return Ok(Value::null(
            ctx,
            &crate::types::spanner_type_to_logical(ty)?.build(ctx)?,
        )?);
    }
    if ty.code() == TypeCode::Array {
        let child_type = ty
            .array_element_type()
            .ok_or_else(|| SpannerError::Conversion("missing array metadata".into()))?;
        let child_logical = crate::types::spanner_type_to_logical(&child_type)?.build(ctx)?;
        let children = value
            .try_as_list()
            .ok_or_else(|| type_mismatch_error("ARRAY", value.kind(), &location()))?
            .iter()
            .enumerate()
            .map(|(i, v)| {
                decode_value(ctx, v, &child_type, &|| {
                    format!("{}, array element {i}", location())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(duckdb_neo::connection::FFILink::create_value(
            ctx,
            ValueInput::List {
                child_type: &child_logical,
                children: &children,
            },
        )?);
    }
    if ty.code() == TypeCode::Struct {
        let meta = ty
            .struct_type()
            .ok_or_else(|| SpannerError::Conversion("missing struct metadata".into()))?;
        let values = value
            .try_as_list()
            .ok_or_else(|| type_mismatch_error("STRUCT", value.kind(), &location()))?;
        validate_struct_value_arity(values.len(), meta.fields.len(), location)?;
        let mut children = Vec::with_capacity(values.len());
        for (field, value) in meta.fields.iter().zip(values.iter()) {
            let field_type = field
                .r#type
                .as_ref()
                .ok_or_else(|| SpannerError::Conversion("missing struct field metadata".into()))?;
            children.push(decode_value(
                ctx,
                value,
                &Type::from((**field_type).clone()),
                &|| format!("{}, field '{}'", location(), field.name),
            )?);
        }
        let names = meta
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>();
        return Ok(duckdb_neo::connection::FFILink::create_value(
            ctx,
            ValueInput::Struct {
                names: &names,
                children: &children,
            },
        )?);
    }
    let input = match prepare_scalar_value(value, ty, location)? {
        PreparedScalar::Null => {
            return Ok(Value::null(
                ctx,
                &crate::types::spanner_type_to_logical(ty)?.build(ctx)?,
            )?);
        }
        PreparedScalar::Bool(v) => ValueInput::Bool(v),
        PreparedScalar::I32(v) => ValueInput::Date(v),
        PreparedScalar::I64(v) if ty.code() == TypeCode::Timestamp => ValueInput::TimestampTz(v),
        PreparedScalar::I64(v) => ValueInput::BigInt(v),
        PreparedScalar::I128(v) => ValueInput::Decimal {
            value: v,
            width: 38,
            scale: 9,
        },
        PreparedScalar::U128(v) => ValueInput::Uuid(v as i128),
        PreparedScalar::F32(v) => ValueInput::Float(v),
        PreparedScalar::F64(v) => ValueInput::Double(v),
        PreparedScalar::String(v) => {
            let value = v.value(ctx)?;
            return if ty.code() == TypeCode::Json {
                Ok(value.cast(ctx, &crate::types::spanner_type_to_logical(ty)?.build(ctx)?)?)
            } else {
                Ok(value)
            };
        }
        PreparedScalar::Bytes(v) => {
            return Ok(duckdb_neo::connection::FFILink::create_value(
                ctx,
                ValueInput::Blob(&v),
            )?);
        }
        PreparedScalar::Interval(v) => ValueInput::Interval {
            months: v.months,
            days: v.days,
            micros: v.micros,
        },
    };
    Ok(duckdb_neo::connection::FFILink::create_value(ctx, input)?)
}
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
            let decimal = duckdb_neo::types::DecimalValue::new(unscaled, 38, 9);
            decimal.check().map_err(|e| {
                SpannerError::Conversion(format!(
                    "NUMERIC value does not fit DuckDB DECIMAL(38,9): {value}: {e}"
                ))
            })?;
            Ok(PreparedScalar::I128(decimal.value))
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

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;

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
        let value = google_cloud_spanner::value::ToValue::to_value(&true);
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
    use google_cloud_spanner::value::ToValue as SpannerToValue;
    struct TestRow(Vec<SpannerValue>);
    impl ConversionRow for TestRow {
        fn conversion_values(&self) -> &[SpannerValue] {
            &self.0
        }
    }
    fn scalar_type(code: TypeCode) -> Type {
        model::Type::new()
            .set_code(model::TypeCode::from(i32::from(code)))
            .into()
    }
    fn array_type(child: Type) -> Type {
        let child: model::Type = child.into();
        model::Type::new()
            .set_code(model::TypeCode::Array)
            .set_array_element_type(child)
            .into()
    }
    fn struct_type(fields: Vec<(&str, Type)>) -> Type {
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
    fn list(values: Vec<SpannerValue>) -> SpannerValue {
        values.to_value()
    }
    fn integer(n: i64) -> SpannerValue {
        n.to_value()
    }
    fn null() -> SpannerValue {
        Option::<i64>::None.to_value()
    }
    fn write_values(
        output: &mut DataChunkHandle,
        ty: &Type,
        values: Vec<SpannerValue>,
    ) -> Result<(), SpannerError> {
        let rows = values
            .into_iter()
            .map(|v| TestRow(vec![v]))
            .collect::<Vec<_>>();
        preflight_column_from_rows(&rows, 0, ty, "payload")?;
        write_preflighted_column_from_rows(output, 0, &rows, 0, ty, "payload")
    }
    fn output<'a>(
        context: &'a impl duckdb_neo::connection::FFILink,
        ty: &Type,
    ) -> DataChunkHandle<'a> {
        DataChunkHandle::new(
            context,
            &[crate::types::spanner_type_to_logical(ty).unwrap()],
        )
    }
    fn json_at(output: &DataChunkHandle, row: usize, ty: &Type) -> serde_json::Value {
        crate::scalars::structural_json(
            &output.value_at(0, row).unwrap(),
            &crate::types::spanner_type_to_logical(ty).unwrap(),
        )
        .unwrap()
    }
    #[test]
    fn nested_arrays_of_structs_exceed_standard_vector_and_keep_nulls_and_order() {
        let context = crate::test_support::connection();
        let ty = array_type(struct_type(vec![
            ("id", scalar_type(TypeCode::Int64)),
            ("items", array_type(scalar_type(TypeCode::Int64))),
        ]));
        let mut out = output(&context, &ty);
        let count = out.capacity().unwrap() + 17;
        let items = (0..count)
            .map(|i| {
                list(vec![
                    integer(i as i64),
                    if i % 2 == 0 {
                        list(vec![integer(i as i64), null()])
                    } else {
                        null()
                    },
                ])
            })
            .collect::<Vec<_>>();
        write_values(&mut out, &ty, vec![list(items), null(), list(vec![])]).unwrap();
        assert_eq!(out.len().unwrap(), 3);
        let data = json_at(&out, 0, &ty);
        assert_eq!(data.as_array().unwrap().len(), count);
        for i in [0, 1, count - 1] {
            assert_eq!(data[i]["id"], serde_json::json!(i));
            assert_eq!(
                data[i]["items"],
                if i % 2 == 0 {
                    serde_json::json!([i, null])
                } else {
                    serde_json::Value::Null
                }
            );
        }
        assert!(json_at(&out, 1, &ty).is_null());
        assert_eq!(json_at(&out, 2, &ty), serde_json::json!([]));
        // Reuse the native chunk with a smaller value and ensure no stale child
        // offsets, array entries, or null masks become visible.
        write_values(
            &mut out,
            &ty,
            vec![list(vec![list(vec![integer(9), list(vec![integer(8)])])])],
        )
        .unwrap();
        assert_eq!(out.len().unwrap(), 1);
        assert_eq!(
            json_at(&out, 0, &ty),
            serde_json::json!([{"id":9,"items":[8]}])
        );
    }
    #[test]
    fn deeply_nested_arrays_keep_values_beyond_one_vector() {
        let context = crate::test_support::connection();
        let ty = array_type(array_type(array_type(scalar_type(TypeCode::Int64))));
        let mut out = output(&context, &ty);
        let count = out.capacity().unwrap() + 3;
        let leaf = list((0..count).map(|i| integer(i as i64)).collect());
        write_values(
            &mut out,
            &ty,
            vec![list(vec![list(vec![leaf, null()]), list(vec![])])],
        )
        .unwrap();
        let value = json_at(&out, 0, &ty);
        assert_eq!(value[0][0].as_array().unwrap().len(), count);
        assert_eq!(value[0][0][count - 1], serde_json::json!(count - 1));
        assert!(value[0][1].is_null());
        assert_eq!(value[1], serde_json::json!([]));
    }
    #[test]
    fn scalar_mismatch_preserves_an_existing_native_value() {
        let context = crate::test_support::connection();
        let ty = scalar_type(TypeCode::Int64);
        let mut out = output(&context, &ty);
        write_values(&mut out, &ty, vec![integer(42)]).unwrap();
        let error = write_values(&mut out, &ty, vec![true.to_value()])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("INT64")
                && error.contains("Bool")
                && error.contains("column 'payload', row 0"),
            "{error}"
        );
        assert_eq!(out.len().unwrap(), 1);
        assert_eq!(out.value_at(0, 0).unwrap().get::<i64>().unwrap(), Some(42));
    }
    #[test]
    fn numeric_outside_decimal38_domain_is_rejected_before_write() {
        let context = crate::test_support::connection();
        let ty = scalar_type(TypeCode::Numeric);
        let mut out = output(&context, &ty);
        let error = write_values(
            &mut out,
            &ty,
            vec!["170141183460469231731687303715.884105727".to_value()],
        )
        .unwrap_err();
        assert!(error.to_string().contains("NUMERIC"), "{error}");
        assert_eq!(out.len().unwrap(), 0);
    }
    #[test]
    fn scalar_and_array_null_and_absent_values_remain_null() {
        let context = crate::test_support::connection();
        for ty in [
            scalar_type(TypeCode::Int64),
            array_type(scalar_type(TypeCode::Int64)),
        ] {
            let mut out = output(&context, &ty);
            write_values(
                &mut out,
                &ty,
                vec![null(), prost_types::Value { kind: None }.to_value()],
            )
            .unwrap();
            assert_eq!(out.len().unwrap(), 2);
            assert!(out.value_at(0, 0).unwrap().is_null().unwrap());
            assert!(out.value_at(0, 1).unwrap().is_null().unwrap());
        }
    }
    #[test]
    fn unknown_scalar_type_is_rejected_even_for_null() {
        let rows = [TestRow(vec![null()])];
        let error = preflight_column_from_rows(&rows, 0, &Type::default(), "unknown").unwrap_err();
        assert!(
            error.to_string().contains("unsupported Spanner TypeCode"),
            "{error}"
        );
    }
    #[test]
    fn pg_numeric_special_and_wide_values_preserve_wire_text() {
        let context = crate::test_support::connection();
        let ty = google_cloud_spanner::types::pg_numeric();
        let mut out = output(&context, &ty);
        let values = [
            "NaN",
            "Infinity",
            "-Infinity",
            "123456789012345678901234567890123456789.123456789",
        ];
        write_values(&mut out, &ty, values.iter().map(|s| s.to_value()).collect()).unwrap();
        for (row, expected) in values.iter().enumerate() {
            assert_eq!(
                out.value_at(0, row)
                    .unwrap()
                    .get::<String>()
                    .unwrap()
                    .as_deref(),
                Some(*expected)
            );
        }
    }
    #[test]
    fn malformed_deep_values_fail_before_replacing_parent() {
        let context = crate::test_support::connection();
        let ty = array_type(struct_type(vec![(
            "flags",
            array_type(scalar_type(TypeCode::Bool)),
        )]));
        let mut out = output(&context, &ty);
        write_values(
            &mut out,
            &ty,
            vec![list(vec![list(vec![list(vec![true.to_value()])])])],
        )
        .unwrap();
        let previous = json_at(&out, 0, &ty);
        let invalid = list(vec![list(vec![list(vec!["not-a-bool".to_value()])])]);
        let error = write_values(&mut out, &ty, vec![invalid])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("BOOL")
                && error.contains("field 'flags'")
                && error.contains("array element"),
            "{error}"
        );
        assert_eq!(json_at(&out, 0, &ty), previous);
    }
    #[test]
    fn struct_arity_mismatch_rejects_missing_and_extra_fields() {
        let ty = struct_type(vec![
            ("id", scalar_type(TypeCode::Int64)),
            ("flag", scalar_type(TypeCode::Bool)),
        ]);
        for value in [
            list(vec![integer(1)]),
            list(vec![integer(1), true.to_value(), integer(3)]),
        ] {
            let error =
                preflight_column_from_rows(&[TestRow(vec![value])], 0, &ty, "item").unwrap_err();
            assert!(error.to_string().contains("values for 2 fields"), "{error}");
        }
    }
    #[test]
    fn scalar_in_array_position_is_an_error_not_null() {
        let ty = array_type(scalar_type(TypeCode::Int64));
        let error =
            preflight_column_from_rows(&[TestRow(vec![integer(7)])], 0, &ty, "items").unwrap_err();
        assert!(
            error.to_string().contains("ARRAY") && error.to_string().contains("column 'items'"),
            "{error}"
        );
    }
    #[test]
    fn flattened_budget_and_overflow_are_contextual() {
        assert_eq!(
            checked_child_count(MAX_FLATTENED_CHILDREN - 1, 1, &|| "column 'items'".into())
                .unwrap(),
            MAX_FLATTENED_CHILDREN
        );
        for (current, additional) in [(MAX_FLATTENED_CHILDREN, 1), (usize::MAX, 1)] {
            let error =
                checked_child_count(current, additional, &|| "column 'items'".into()).unwrap_err();
            assert!(error.to_string().contains("column 'items'"), "{error}");
        }
    }
    #[test]
    fn missing_composite_metadata_is_rejected_even_for_null() {
        for code in [TypeCode::Array, TypeCode::Struct] {
            let error = preflight_column_from_rows(
                &[TestRow(vec![null()])],
                0,
                &scalar_type(code),
                "payload",
            )
            .unwrap_err();
            assert!(error.to_string().contains("column 'payload'"), "{error}");
        }
    }
}
