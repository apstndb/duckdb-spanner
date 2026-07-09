use google_cloud_spanner::model::request_options::Priority;
use google_cloud_spanner::statement::{Statement, StatementBuilder};
use google_cloud_spanner::types::{self as spanner_types, Type, TypeCode};

use crate::error::SpannerError;
use crate::types;

/// Create a Statement from SQL and an optional JSON params string.
///
/// Each entry in the JSON object is either:
/// - A plain value (type inferred from JSON)
/// - A typed value with explicit Spanner type, e.g.
///   `{"id": {"value": null, "type": "INT64"}}`
pub fn create_statement(sql: &str, params_json: Option<&str>) -> Result<Statement, SpannerError> {
    create_statement_inner(sql, params_json, None)
}

pub fn create_statement_with_priority(
    sql: &str,
    params_json: Option<&str>,
    priority: Option<Priority>,
) -> Result<Statement, SpannerError> {
    create_statement_inner(sql, params_json, priority)
}

fn create_statement_inner(
    sql: &str,
    params_json: Option<&str>,
    priority: Option<Priority>,
) -> Result<Statement, SpannerError> {
    let mut builder = Statement::builder(sql);
    if let Some(priority) = priority {
        builder = builder.set_priority(priority);
    }

    let params_json = match params_json {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(builder.build()),
    };

    let params: serde_json::Map<String, serde_json::Value> = serde_json::from_str(params_json)
        .map_err(|e| SpannerError::Other(format!("Failed to parse params JSON: {e}")))?;

    for (key, value) in &params {
        builder = add_json_param(builder, key, value)?;
    }

    Ok(builder.build())
}

fn add_json_param(
    builder: StatementBuilder,
    key: &str,
    value: &serde_json::Value,
) -> Result<StatementBuilder, SpannerError> {
    if let serde_json::Value::Object(obj) = value {
        if obj.contains_key("type") {
            return add_typed_param(builder, key, obj);
        }
        return Err(SpannerError::Other(format!(
            "Unsupported JSON object for parameter '{key}': \
             objects must have a \"type\" key (e.g., {{\"value\": null, \"type\": \"INT64\"}})"
        )));
    }

    add_plain_param(builder, key, value)
}

fn add_plain_param(
    builder: StatementBuilder,
    key: &str,
    value: &serde_json::Value,
) -> Result<StatementBuilder, SpannerError> {
    let builder = match value {
        serde_json::Value::Bool(b) => builder.add_param(key, b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                builder.add_param(key, &i)
            } else if let Some(f) = n.as_f64() {
                builder.add_param(key, &f)
            } else {
                return Err(SpannerError::Other(format!(
                    "Unsupported number value for parameter '{key}': {n}"
                )));
            }
        }
        serde_json::Value::String(s) => builder.add_param(key, s),
        serde_json::Value::Null => {
            builder.add_typed_param(key, &Option::<String>::None, spanner_types::string())
        }
        serde_json::Value::Array(_) => {
            return Err(SpannerError::Other(format!(
                "Plain JSON array for parameter '{key}': use typed form \
                 e.g., {{\"value\": [1,2,3], \"type\": \"ARRAY<INT64>\"}}"
            )));
        }
        serde_json::Value::Object(_) => unreachable!("handled in add_json_param"),
    };
    Ok(builder)
}

fn add_typed_param(
    builder: StatementBuilder,
    key: &str,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<StatementBuilder, SpannerError> {
    let type_str = obj.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
        SpannerError::Other(format!(
            "\"type\" field for parameter '{key}' must be a string"
        ))
    })?;

    let spanner_type = types::parse_spanner_type(type_str);
    let value = obj.get("value").unwrap_or(&serde_json::Value::Null);

    match spanner_type.code() {
        TypeCode::Array => add_array_param(builder, key, value, spanner_type),
        TypeCode::Struct => Err(SpannerError::Other(format!(
            "STRUCT parameters are not yet supported (parameter '{key}')"
        ))),
        _ => add_scalar_param(builder, key, value, spanner_type),
    }
}

fn add_scalar_param(
    builder: StatementBuilder,
    key: &str,
    value: &serde_json::Value,
    spanner_type: Type,
) -> Result<StatementBuilder, SpannerError> {
    if value.is_null() {
        return Ok(builder.add_typed_param(key, &Option::<String>::None, spanner_type));
    }

    let builder = match spanner_type.code() {
        TypeCode::Bool => {
            let b = value
                .as_bool()
                .ok_or_else(|| param_err(key, "BOOL", value))?;
            builder.add_typed_param(key, &b, spanner_type)
        }
        TypeCode::Int64 => {
            let i = value
                .as_i64()
                .ok_or_else(|| param_err(key, "INT64", value))?;
            builder.add_typed_param(key, &i, spanner_type)
        }
        TypeCode::Float32 => {
            let f = value
                .as_f64()
                .ok_or_else(|| param_err(key, "FLOAT32", value))? as f32;
            builder.add_typed_param(key, &f, spanner_type)
        }
        TypeCode::Float64 => {
            let f = value
                .as_f64()
                .ok_or_else(|| param_err(key, "FLOAT64", value))?;
            builder.add_typed_param(key, &f, spanner_type)
        }
        TypeCode::String => {
            let s = json_to_string_coerce(value);
            builder.add_typed_param(key, &s, spanner_type)
        }
        TypeCode::Bytes => {
            let s = require_string(key, "BYTES", value)?;
            builder.add_typed_param(key, &s, spanner_type)
        }
        TypeCode::Date => {
            let s = require_string(key, "DATE", value)?;
            builder.add_typed_param(key, &s, spanner_type)
        }
        TypeCode::Timestamp => {
            let s = require_string(key, "TIMESTAMP", value)?;
            builder.add_typed_param(key, &s, spanner_type)
        }
        TypeCode::Numeric => {
            let s = match value {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                _ => return Err(param_err(key, "NUMERIC", value)),
            };
            builder.add_typed_param(key, &s, spanner_type)
        }
        TypeCode::Json => {
            let s = match value {
                serde_json::Value::String(s) => s.clone(),
                other => serde_json::to_string(other).map_err(|e| {
                    SpannerError::Other(format!("Failed to serialize JSON param '{key}': {e}"))
                })?,
            };
            builder.add_typed_param(key, &s, spanner_type)
        }
        TypeCode::Uuid => {
            let s = require_string(key, "UUID", value)?;
            builder.add_typed_param(key, &s, spanner_type)
        }
        TypeCode::Interval => {
            let s = require_string(key, "INTERVAL", value)?;
            builder.add_typed_param(key, &s, spanner_type)
        }
        other => {
            return Err(SpannerError::Other(format!(
                "Unsupported Spanner type {other:?} for parameter '{key}'"
            )));
        }
    };
    Ok(builder)
}

fn add_array_param(
    builder: StatementBuilder,
    key: &str,
    value: &serde_json::Value,
    spanner_type: Type,
) -> Result<StatementBuilder, SpannerError> {
    if value.is_null() {
        return Ok(builder.add_typed_param(key, &Option::<String>::None, spanner_type));
    }

    let elem_type = spanner_type.array_element_type().ok_or_else(|| {
        SpannerError::Other(format!(
            "ARRAY type for parameter '{key}' must specify element type \
             (e.g., \"ARRAY<INT64>\")"
        ))
    })?;
    let elem_code = elem_type.code();

    let arr = value.as_array().ok_or_else(|| {
        SpannerError::Other(format!(
            "Expected JSON array or null for ARRAY parameter '{key}', got {value}"
        ))
    })?;

    let builder = match elem_code {
        TypeCode::Bool => {
            let v = parse_array(key, arr, |_, v| {
                v.as_bool().ok_or_else(|| param_err(key, "BOOL", v))
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Int64 => {
            let v = parse_array(key, arr, |_, v| {
                v.as_i64().ok_or_else(|| param_err(key, "INT64", v))
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Float32 => {
            let v = parse_array(key, arr, |_, v| {
                let f = v.as_f64().ok_or_else(|| param_err(key, "FLOAT32", v))?;
                Ok(f as f32)
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Float64 => {
            let v = parse_array(key, arr, |_, v| {
                v.as_f64().ok_or_else(|| param_err(key, "FLOAT64", v))
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::String => {
            let v = parse_array(key, arr, |_, v| Ok(json_to_string_coerce(v)))?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Bytes => {
            let v = parse_array(key, arr, |k, v| {
                Ok(require_string(k, "BYTES", v)?.to_string())
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Date => {
            let v = parse_array(key, arr, |k, v| {
                Ok(require_string(k, "DATE", v)?.to_string())
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Timestamp => {
            let v = parse_array(key, arr, |k, v| {
                Ok(require_string(k, "TIMESTAMP", v)?.to_string())
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Numeric => {
            let v = parse_array(key, arr, |k, v| match v {
                serde_json::Value::String(s) => Ok(s.clone()),
                serde_json::Value::Number(n) => Ok(n.to_string()),
                _ => Err(param_err(k, "NUMERIC", v)),
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Json => {
            let v = parse_array(key, arr, |k, v| match v {
                serde_json::Value::String(s) => Ok(s.clone()),
                other => serde_json::to_string(other).map_err(|e| {
                    SpannerError::Other(format!(
                        "Failed to serialize JSON array element for '{k}': {e}"
                    ))
                }),
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Uuid => {
            let v = parse_array(key, arr, |k, v| {
                Ok(require_string(k, "UUID", v)?.to_string())
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Interval => {
            let v = parse_array(key, arr, |k, v| {
                Ok(require_string(k, "INTERVAL", v)?.to_string())
            })?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        other => {
            return Err(SpannerError::Other(format!(
                "Unsupported array element type for parameter '{key}': {other:?}"
            )));
        }
    };
    Ok(builder)
}

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

fn json_to_string_coerce(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_params() {
        create_statement("SELECT 1", None).unwrap();
    }

    #[test]
    fn test_plain_values() {
        create_statement(
            "SELECT @a, @b, @c, @d",
            Some(r#"{"a": true, "b": 42, "c": 3.14, "d": "hello"}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_plain_null_is_typed_string_null() {
        create_statement("SELECT @x", Some(r#"{"x": null}"#)).unwrap();
    }

    #[test]
    fn test_typed_scalar_values() {
        create_statement("SELECT @v", Some(r#"{"v":{"value":true,"type":"BOOL"}}"#)).unwrap();
        create_statement("SELECT @v", Some(r#"{"v":{"value":1,"type":"INT64"}}"#)).unwrap();
        create_statement("SELECT @v", Some(r#"{"v":{"value":1.5,"type":"FLOAT32"}}"#)).unwrap();
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":"2024-01-15","type":"DATE"}}"#),
        )
        .unwrap();
        create_statement("SELECT @v", Some(r#"{"v":{"value":null,"type":"INT64"}}"#)).unwrap();
    }

    #[test]
    fn test_typed_arrays() {
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":[1,null,3],"type":"ARRAY<INT64>"}}"#),
        )
        .unwrap();
        create_statement(
            "SELECT @v",
            Some(r#"{"v":{"value":["a",null,"c"],"type":"ARRAY<STRING>"}}"#),
        )
        .unwrap();
    }

    #[test]
    fn test_rejects_bad_json_and_plain_arrays() {
        assert!(create_statement("SELECT @x", Some("not json")).is_err());
        assert!(create_statement("SELECT @x", Some(r#"{"x": [1,2,3]}"#)).is_err());
    }
}
