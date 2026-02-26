use google_cloud_googleapis::spanner::v1::{Type, TypeCode};
use google_cloud_spanner::statement::{Statement, ToKind};
use prost_types::value::Kind;

use crate::error::SpannerError;
use crate::types;

// ─── Wrapper types for Spanner types without built-in ToKind impls ──────────
//
// Most Spanner types are transmitted as StringValue in the proto.
// These wrappers let us call `stmt.add_param(key, &wrapper)` and get the
// correct `param_types` entry without patching gcloud-spanner.

macro_rules! define_string_param {
    ($(#[$meta:meta])* $name:ident, $code:expr) => {
        $(#[$meta])*
        struct $name(Option<String>);

        impl ToKind for $name {
            fn to_kind(&self) -> Kind {
                match &self.0 {
                    Some(s) => Kind::StringValue(s.clone()),
                    None => Kind::NullValue(0),
                }
            }
            fn get_type() -> Type where Self: Sized {
                Type { code: $code as i32, ..Default::default() }
            }
        }
    };
}

define_string_param!(BytesParam, TypeCode::Bytes);
define_string_param!(DateParam, TypeCode::Date);
define_string_param!(TimestampParam, TypeCode::Timestamp);
define_string_param!(NumericParam, TypeCode::Numeric);
define_string_param!(JsonParam, TypeCode::Json);
define_string_param!(UuidParam, TypeCode::Uuid);
define_string_param!(IntervalParam, TypeCode::Interval);

/// FLOAT32 uses NumberValue in the proto, not StringValue.
struct Float32Param(Option<f32>);

impl ToKind for Float32Param {
    fn to_kind(&self) -> Kind {
        match self.0 {
            Some(v) => Kind::NumberValue(v as f64),
            None => Kind::NullValue(0),
        }
    }
    fn get_type() -> Type
    where
        Self: Sized,
    {
        Type {
            code: TypeCode::Float32 as i32,
            ..Default::default()
        }
    }
}

// ─── Public API ─────────────────────────────────────────────────────────────

/// Create a Statement from SQL and an optional JSON params string.
///
/// Each entry in the JSON object is either:
/// - A plain value (type inferred from JSON):
///   `{"age": 25, "name": "Alice", "active": true}`
/// - A typed value with explicit Spanner type:
///   `{"id": {"value": null, "type": "INT64"}, "score": {"value": 42, "type": "FLOAT64"}}`
///
/// Type inference for plain values:
/// - `true`/`false` → BOOL
/// - integer → INT64
/// - float → FLOAT64
/// - string → STRING
/// - `null` → STRING NULL
/// - array/object → error (use typed form instead)
///
/// Typed values use `parse_spanner_type` for the `"type"` field.
/// Supported: all scalar types + ARRAY\<scalar\>.
/// STRUCT parameters are not supported (requires gcloud-spanner upstream changes).
pub fn create_statement(
    sql: &str,
    params_json: Option<&str>,
) -> Result<Statement, SpannerError> {
    let params_json = match params_json {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(Statement::new(sql)),
    };

    let params: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(params_json).map_err(|e| {
            SpannerError::Other(format!("Failed to parse params JSON: {e}"))
        })?;

    let mut stmt = Statement::new(sql);

    for (key, value) in &params {
        add_json_param(&mut stmt, key, value)?;
    }

    Ok(stmt)
}

// ─── Dispatch ───────────────────────────────────────────────────────────────

/// Add a single JSON entry as a parameter to a Statement.
fn add_json_param(
    stmt: &mut Statement,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), SpannerError> {
    if let serde_json::Value::Object(obj) = value {
        if obj.contains_key("type") {
            return add_typed_param(stmt, key, obj);
        }
        return Err(SpannerError::Other(format!(
            "Unsupported JSON object for parameter '{key}': \
             objects must have a \"type\" key (e.g., {{\"value\": null, \"type\": \"INT64\"}})"
        )));
    }

    add_plain_param(stmt, key, value)
}

/// Plain value — type inferred from JSON.
fn add_plain_param(
    stmt: &mut Statement,
    key: &str,
    value: &serde_json::Value,
) -> Result<(), SpannerError> {
    match value {
        serde_json::Value::Bool(b) => stmt.add_param(key, b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                stmt.add_param(key, &i);
            } else if let Some(f) = n.as_f64() {
                stmt.add_param(key, &f);
            } else {
                return Err(SpannerError::Other(format!(
                    "Unsupported number value for parameter '{key}': {n}"
                )));
            }
        }
        serde_json::Value::String(s) => stmt.add_param(key, s),
        serde_json::Value::Null => stmt.add_param(key, &Option::<String>::None),
        serde_json::Value::Array(_) => {
            return Err(SpannerError::Other(format!(
                "Plain JSON array for parameter '{key}': use typed form \
                 e.g., {{\"value\": [1,2,3], \"type\": \"ARRAY<INT64>\"}}"
            )));
        }
        serde_json::Value::Object(_) => unreachable!("handled in add_json_param"),
    }
    Ok(())
}

/// Typed value — `{"value": ..., "type": "..."}`.
fn add_typed_param(
    stmt: &mut Statement,
    key: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), SpannerError> {
    let type_str = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            SpannerError::Other(format!(
                "\"type\" field for parameter '{key}' must be a string"
            ))
        })?;

    let spanner_type = types::parse_spanner_type(type_str);
    let type_code = TypeCode::try_from(spanner_type.code).unwrap_or(TypeCode::Unspecified);

    let value = obj.get("value").unwrap_or(&serde_json::Value::Null);

    match type_code {
        TypeCode::Array => {
            let elem_type = spanner_type.array_element_type.as_ref().ok_or_else(|| {
                SpannerError::Other(format!(
                    "ARRAY type for parameter '{key}' must specify element type \
                     (e.g., \"ARRAY<INT64>\")"
                ))
            })?;
            add_array_param(stmt, key, value, elem_type)
        }
        TypeCode::Struct => Err(SpannerError::Other(format!(
            "STRUCT parameters are not yet supported (parameter '{key}')"
        ))),
        _ => add_scalar_param(stmt, key, value, type_code),
    }
}

// ─── Scalar binding ─────────────────────────────────────────────────────────

/// Bind a scalar JSON value, coercing to the given Spanner type.
fn add_scalar_param(
    stmt: &mut Statement,
    key: &str,
    value: &serde_json::Value,
    type_code: TypeCode,
) -> Result<(), SpannerError> {
    if value.is_null() {
        return add_null_scalar(stmt, key, type_code);
    }

    match type_code {
        TypeCode::Bool => {
            let b = value.as_bool().ok_or_else(|| param_err(key, "BOOL", value))?;
            stmt.add_param(key, &b);
        }
        TypeCode::Int64 => {
            let i = value.as_i64().ok_or_else(|| param_err(key, "INT64", value))?;
            stmt.add_param(key, &i);
        }
        TypeCode::Float32 => {
            let f = value.as_f64().ok_or_else(|| param_err(key, "FLOAT32", value))?;
            stmt.add_param(key, &Float32Param(Some(f as f32)));
        }
        TypeCode::Float64 => {
            let f = value.as_f64().ok_or_else(|| param_err(key, "FLOAT64", value))?;
            stmt.add_param(key, &f);
        }
        TypeCode::String => {
            let s = json_to_string_coerce(value);
            stmt.add_param(key, &s);
        }
        TypeCode::Bytes => {
            let s = require_string(key, "BYTES", value)?;
            stmt.add_param(key, &BytesParam(Some(s.to_string())));
        }
        TypeCode::Date => {
            let s = require_string(key, "DATE", value)?;
            stmt.add_param(key, &DateParam(Some(s.to_string())));
        }
        TypeCode::Timestamp => {
            let s = require_string(key, "TIMESTAMP", value)?;
            stmt.add_param(key, &TimestampParam(Some(s.to_string())));
        }
        TypeCode::Numeric => {
            let s = match value {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => return Err(param_err(key, "NUMERIC", value)),
            };
            stmt.add_param(key, &NumericParam(Some(s)));
        }
        TypeCode::Json => {
            // String → pass as-is; other JSON values → serialize
            let s = match value {
                serde_json::Value::String(s) => s.clone(),
                other => serde_json::to_string(other).map_err(|e| {
                    SpannerError::Other(format!("Failed to serialize JSON param '{key}': {e}"))
                })?,
            };
            stmt.add_param(key, &JsonParam(Some(s)));
        }
        TypeCode::Uuid => {
            let s = require_string(key, "UUID", value)?;
            stmt.add_param(key, &UuidParam(Some(s.to_string())));
        }
        TypeCode::Interval => {
            let s = require_string(key, "INTERVAL", value)?;
            stmt.add_param(key, &IntervalParam(Some(s.to_string())));
        }
        _ => {
            return Err(SpannerError::Other(format!(
                "Unsupported Spanner type {type_code:?} for parameter '{key}'"
            )));
        }
    }
    Ok(())
}

/// Bind a typed NULL scalar. Dispatches to the correct wrapper type so that
/// `param_types` gets the right TypeCode.
fn add_null_scalar(stmt: &mut Statement, key: &str, type_code: TypeCode) -> Result<(), SpannerError> {
    match type_code {
        TypeCode::Bool => stmt.add_param(key, &Option::<bool>::None),
        TypeCode::Int64 => stmt.add_param(key, &Option::<i64>::None),
        TypeCode::Float32 => stmt.add_param(key, &Float32Param(None)),
        TypeCode::Float64 => stmt.add_param(key, &Option::<f64>::None),
        TypeCode::String => stmt.add_param(key, &Option::<String>::None),
        TypeCode::Bytes => stmt.add_param(key, &BytesParam(None)),
        TypeCode::Date => stmt.add_param(key, &DateParam(None)),
        TypeCode::Timestamp => stmt.add_param(key, &TimestampParam(None)),
        TypeCode::Numeric => stmt.add_param(key, &NumericParam(None)),
        TypeCode::Json => stmt.add_param(key, &JsonParam(None)),
        TypeCode::Uuid => stmt.add_param(key, &UuidParam(None)),
        TypeCode::Interval => stmt.add_param(key, &IntervalParam(None)),
        _ => {
            return Err(SpannerError::Other(format!(
                "Unsupported Spanner type {type_code:?} for NULL parameter '{key}'"
            )));
        }
    }
    Ok(())
}

// ─── Array binding ──────────────────────────────────────────────────────────

/// Bind a JSON array (or null) as an ARRAY parameter.
///
/// Uses `Option<T>` elements to support NULLs within arrays: `[1, null, 3]`.
/// For types with built-in ToKind (bool, i64, f64, String), uses those directly.
/// For wrapper types, uses `Vec<Option<WrapperType>>` which also implements ToKind
/// via the blanket `impl<T: ToKind> ToKind for Vec<T>`.
fn add_array_param(
    stmt: &mut Statement,
    key: &str,
    value: &serde_json::Value,
    elem_type: &Type,
) -> Result<(), SpannerError> {
    let elem_code = TypeCode::try_from(elem_type.code).unwrap_or(TypeCode::Unspecified);

    if value.is_null() {
        return add_null_array(stmt, key, elem_code);
    }

    let arr = value.as_array().ok_or_else(|| {
        SpannerError::Other(format!(
            "Expected JSON array or null for ARRAY parameter '{key}', got {value}"
        ))
    })?;

    match elem_code {
        TypeCode::Bool => {
            let v = parse_array(key, arr, |_, v| {
                v.as_bool().ok_or_else(|| param_err(key, "BOOL", v))
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::Int64 => {
            let v = parse_array(key, arr, |_, v| {
                v.as_i64().ok_or_else(|| param_err(key, "INT64", v))
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::Float32 => {
            let v = parse_array(key, arr, |_, v| {
                let f = v.as_f64().ok_or_else(|| param_err(key, "FLOAT32", v))?;
                Ok(Float32Param(Some(f as f32)))
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::Float64 => {
            let v = parse_array(key, arr, |_, v| {
                v.as_f64().ok_or_else(|| param_err(key, "FLOAT64", v))
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::String => {
            let v = parse_array(key, arr, |_, v| {
                Ok(json_to_string_coerce(v))
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::Bytes => {
            let v = parse_array(key, arr, |k, v| {
                Ok(BytesParam(Some(require_string(k, "BYTES", v)?.to_string())))
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::Date => {
            let v = parse_array(key, arr, |k, v| {
                Ok(DateParam(Some(require_string(k, "DATE", v)?.to_string())))
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::Timestamp => {
            let v = parse_array(key, arr, |k, v| {
                Ok(TimestampParam(Some(
                    require_string(k, "TIMESTAMP", v)?.to_string(),
                )))
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::Numeric => {
            let v = parse_array(key, arr, |k, v| match v {
                serde_json::Value::String(s) => Ok(NumericParam(Some(s.clone()))),
                serde_json::Value::Number(n) => Ok(NumericParam(Some(n.to_string()))),
                _ => Err(param_err(k, "NUMERIC", v)),
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::Json => {
            let v = parse_array(key, arr, |k, v| match v {
                serde_json::Value::String(s) => Ok(JsonParam(Some(s.clone()))),
                other => {
                    let s = serde_json::to_string(other).map_err(|e| {
                        SpannerError::Other(format!(
                            "Failed to serialize JSON array element for '{k}': {e}"
                        ))
                    })?;
                    Ok(JsonParam(Some(s)))
                }
            })?;
            stmt.add_param(key, &v);
        }
        TypeCode::Uuid => {
            let v = parse_array(key, arr, |k, v| {
                Ok(UuidParam(Some(require_string(k, "UUID", v)?.to_string())))
            })?;
            stmt.add_param(key, &v);
        }
        _ => {
            return Err(SpannerError::Other(format!(
                "Unsupported array element type for parameter '{key}': {elem_code:?}"
            )));
        }
    }
    Ok(())
}

/// Bind a typed NULL array.
fn add_null_array(
    stmt: &mut Statement,
    key: &str,
    elem_code: TypeCode,
) -> Result<(), SpannerError> {
    match elem_code {
        TypeCode::Bool => stmt.add_param(key, &Option::<Vec<Option<bool>>>::None),
        TypeCode::Int64 => stmt.add_param(key, &Option::<Vec<Option<i64>>>::None),
        TypeCode::Float32 => stmt.add_param(key, &Option::<Vec<Option<Float32Param>>>::None),
        TypeCode::Float64 => stmt.add_param(key, &Option::<Vec<Option<f64>>>::None),
        TypeCode::String => stmt.add_param(key, &Option::<Vec<Option<String>>>::None),
        TypeCode::Bytes => stmt.add_param(key, &Option::<Vec<Option<BytesParam>>>::None),
        TypeCode::Date => stmt.add_param(key, &Option::<Vec<Option<DateParam>>>::None),
        TypeCode::Timestamp => stmt.add_param(key, &Option::<Vec<Option<TimestampParam>>>::None),
        TypeCode::Numeric => stmt.add_param(key, &Option::<Vec<Option<NumericParam>>>::None),
        TypeCode::Json => stmt.add_param(key, &Option::<Vec<Option<JsonParam>>>::None),
        TypeCode::Uuid => stmt.add_param(key, &Option::<Vec<Option<UuidParam>>>::None),
        _ => {
            return Err(SpannerError::Other(format!(
                "Unsupported array element type for NULL ARRAY parameter '{key}': {elem_code:?}"
            )));
        }
    }
    Ok(())
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Parse a JSON array into `Vec<Option<T>>`, allowing null elements.
fn parse_array<T>(
    key: &str,
    arr: &[serde_json::Value],
    convert: impl Fn(&str, &serde_json::Value) -> Result<T, SpannerError>,
) -> Result<Vec<Option<T>>, SpannerError> {
    arr.iter()
        .map(|v| {
            if v.is_null() {
                Ok(None)
            } else {
                convert(key, v).map(Some)
            }
        })
        .collect()
}

/// Coerce any JSON value to a String (for STRING type).
fn json_to_string_coerce(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// Require a JSON string value; error otherwise.
fn require_string<'a>(
    key: &str,
    type_name: &str,
    value: &'a serde_json::Value,
) -> Result<&'a str, SpannerError> {
    value
        .as_str()
        .ok_or_else(|| param_err(key, type_name, value))
}

fn param_err(key: &str, type_name: &str, value: &serde_json::Value) -> SpannerError {
    SpannerError::Other(format!(
        "Cannot convert {value} to {type_name} for parameter '{key}'"
    ))
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Plain values ──

    #[test]
    fn test_no_params() {
        create_statement("SELECT 1", None).unwrap();
    }

    #[test]
    fn test_empty_params() {
        create_statement("SELECT 1", Some("")).unwrap();
    }

    #[test]
    fn test_plain_values() {
        create_statement(
            "SELECT @a, @b, @c, @d",
            Some(r#"{"a": 42, "b": "hello", "c": true, "d": 3.14}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_null_param() {
        create_statement("SELECT @x", Some(r#"{"x": null}"#)).unwrap();
    }

    // ── Typed scalars ──

    #[test]
    fn test_typed_null_int64() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": null, "type": "INT64"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_null_float32() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"type": "FLOAT32"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_null_date() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"type": "DATE"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_null_json() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"type": "JSON"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_null_uuid() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"type": "UUID"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_coercion_float64() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": 42, "type": "FLOAT64"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_float32() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": 1.5, "type": "FLOAT32"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_date() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": "2024-01-15", "type": "DATE"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_timestamp() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": "2024-06-15T10:30:00Z", "type": "TIMESTAMP"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_numeric_string() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": "123.456", "type": "NUMERIC"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_numeric_number() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": 123.456, "type": "NUMERIC"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_bytes() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": "aGVsbG8=", "type": "BYTES"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_json_string() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": "{\"key\": \"val\"}", "type": "JSON"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_json_object() {
        // Non-string JSON values get serialized
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": {"key": "val"}, "type": "JSON"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_uuid() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": "550e8400-e29b-41d4-a716-446655440000", "type": "UUID"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_interval() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": "P1Y2M3D", "type": "INTERVAL"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_mixed_plain_and_typed() {
        create_statement(
            "SELECT @a, @b",
            Some(r#"{"a": 42, "b": {"value": null, "type": "DATE"}}"#),
        )
        .unwrap();
    }

    // ── Typed arrays ──

    #[test]
    fn test_array_int64() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": [1, 2, 3], "type": "ARRAY<INT64>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_array_string() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": ["a", "b"], "type": "ARRAY<STRING>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_array_bool() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": [true, false], "type": "ARRAY<BOOL>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_array_float64() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": [1.5, 2.5], "type": "ARRAY<FLOAT64>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_array_float32() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": [1.5, 2.5], "type": "ARRAY<FLOAT32>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_array_date() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": ["2024-01-15", "2024-06-15"], "type": "ARRAY<DATE>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_array_json() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": ["{}", "[]"], "type": "ARRAY<JSON>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_array_with_nulls() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": [1, null, 3], "type": "ARRAY<INT64>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_array_null() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": null, "type": "ARRAY<INT64>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_array_empty() {
        create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": [], "type": "ARRAY<INT64>"}}"#),
        )
        .unwrap();
    }

    // ── Error cases ──

    #[test]
    fn test_invalid_json() {
        assert!(create_statement("SELECT @x", Some("not json")).is_err());
    }

    #[test]
    fn test_plain_array_rejected() {
        assert!(create_statement("SELECT @x", Some(r#"{"x": [1,2,3]}"#)).is_err());
    }

    #[test]
    fn test_untyped_object_rejected() {
        assert!(create_statement("SELECT @x", Some(r#"{"x": {"a": 1}}"#)).is_err());
    }

    #[test]
    fn test_type_field_must_be_string() {
        assert!(
            create_statement("SELECT @x", Some(r#"{"x": {"value": 1, "type": 123}}"#)).is_err()
        );
    }

    #[test]
    fn test_struct_rejected() {
        assert!(create_statement(
            "SELECT @x",
            Some(r#"{"x": {"value": null, "type": "STRUCT<name STRING>"}}"#)
        )
        .is_err());
    }

    // ── Round-trip tests: DuckDB spanner_value() output → params.rs ──
    //
    // These use the exact JSON that DuckDB's spanner_value() macro generates
    // via spanner_params(STRUCT), to verify params.rs can parse them correctly.

    #[test]
    fn test_roundtrip_bool() {
        // spanner_value(true) → {"value":true,"type":"BOOL"}
        create_statement("SELECT @v", Some(r#"{"v":{"value":true,"type":"BOOL"}}"#)).unwrap();
    }

    #[test]
    fn test_roundtrip_int64_from_tinyint() {
        // spanner_value(1::TINYINT) → {"value":1,"type":"INT64"}
        create_statement("SELECT @v", Some(r#"{"v":{"value":1,"type":"INT64"}}"#)).unwrap();
    }

    #[test]
    fn test_roundtrip_int64_from_bigint() {
        // spanner_value(9999999999::BIGINT) → {"value":9999999999,"type":"INT64"}
        create_statement("SELECT @v", Some(r#"{"v":{"value":9999999999,"type":"INT64"}}"#)).unwrap();
    }

    #[test]
    fn test_roundtrip_int64_from_uinteger() {
        // spanner_value(4294967295::UINTEGER) → {"value":4294967295,"type":"INT64"}
        create_statement("SELECT @v", Some(r#"{"v":{"value":4294967295,"type":"INT64"}}"#)).unwrap();
    }

    #[test]
    fn test_roundtrip_numeric_from_ubigint() {
        // spanner_value(18446744073709551615::UBIGINT)
        // → {"value":"18446744073709551615","type":"NUMERIC"}
        // Cast to VARCHAR in the macro to preserve precision.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"18446744073709551615","type":"NUMERIC"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_numeric_from_hugeint() {
        // spanner_value(170141183460469231731687303715884105727::HUGEINT)
        // → {"value":"170141183460469231731687303715884105727","type":"NUMERIC"}
        // Cast to VARCHAR in the macro to preserve full precision.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"170141183460469231731687303715884105727","type":"NUMERIC"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_float32() {
        // spanner_value(1.5::FLOAT) → {"value":1.5,"type":"FLOAT32"}
        create_statement("SELECT @v", Some(r#"{"v":{"value":1.5,"type":"FLOAT32"}}"#)).unwrap();
    }

    #[test]
    fn test_roundtrip_float64() {
        // spanner_value(3.14::DOUBLE) → {"value":3.14,"type":"FLOAT64"}
        create_statement("SELECT @v", Some(r#"{"v":{"value":3.14,"type":"FLOAT64"}}"#)).unwrap();
    }

    #[test]
    fn test_roundtrip_numeric_from_decimal() {
        // spanner_value(123.456789::DECIMAL(38,9)) → {"value":"123.456789000","type":"NUMERIC"}
        // Cast to VARCHAR in the macro to preserve precision.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"123.456789000","type":"NUMERIC"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_string() {
        // spanner_value('hello'::VARCHAR) → {"value":"hello","type":"STRING"}
        create_statement("SELECT @v", Some(r#"{"v":{"value":"hello","type":"STRING"}}"#)).unwrap();
    }

    #[test]
    fn test_roundtrip_bytes_from_blob() {
        // spanner_value('\xDEAD'::BLOB) → {"value":"3kFE","type":"BYTES"}
        // base64-encoded by DuckDB's base64() in the macro.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"3kFE","type":"BYTES"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_date() {
        // spanner_value('2024-01-15'::DATE) → {"value":"2024-01-15","type":"DATE"}
        // OK: format matches Spanner expectations.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"2024-01-15","type":"DATE"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_timestamp() {
        // spanner_value('2024-06-15 10:30:00'::TIMESTAMP)
        // → {"value":"2024-06-15T10:30:00.000000Z","type":"TIMESTAMP"}
        // Converted to RFC3339 by strftime in the macro.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"2024-06-15T10:30:00.000000Z","type":"TIMESTAMP"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_timestamptz() {
        // spanner_value('2024-06-15T10:30:00Z'::TIMESTAMPTZ)
        // → {"value":"2024-06-15T10:30:00.000000Z","type":"TIMESTAMP"}
        // Converted to RFC3339 (UTC) by strftime in the macro.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"2024-06-15T10:30:00.000000Z","type":"TIMESTAMP"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_uuid() {
        // spanner_value('550e...'::UUID)
        // → {"value":"550e8400-e29b-41d4-a716-446655440000","type":"UUID"}
        // OK: format matches Spanner expectations.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"550e8400-e29b-41d4-a716-446655440000","type":"UUID"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_interval() {
        // spanner_value(INTERVAL 1 DAY) → {"value":"P1D","type":"INTERVAL"}
        // Converted to ISO 8601 by interval_to_iso8601 in the macro.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"P1D","type":"INTERVAL"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_json() {
        // spanner_value('{"key":1}'::JSON) → {"value":{"key":1},"type":"JSON"}
        // Non-string value: our code serializes it back to string.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":{"key":1},"type":"JSON"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_time_as_string() {
        // spanner_value('12:30:00'::TIME) → {"value":"12:30:00","type":"STRING"}
        // Maps to STRING since Spanner has no TIME type. OK.
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"12:30:00","type":"STRING"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_roundtrip_null_bigint() {
        // spanner_value(NULL::BIGINT) → {"value":null,"type":"INT64"}
        create_statement("SELECT @v", Some(r#"{"v":{"value":null,"type":"INT64"}}"#)).unwrap();
    }

    #[test]
    fn test_roundtrip_null_date() {
        // spanner_value(NULL::DATE) → {"value":null,"type":"DATE"}
        create_statement("SELECT @v", Some(r#"{"v":{"value":null,"type":"DATE"}}"#)).unwrap();
    }
}
