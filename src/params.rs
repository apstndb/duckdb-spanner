use google_cloud_spanner::model::request_options::Priority;
use google_cloud_spanner::statement::{Statement, StatementBuilder};
use google_cloud_spanner::types::{self as spanner_types, Type, TypeCode};

use crate::error::SpannerError;
use crate::types;

pub(crate) const JSON_TRANSPORT_FORMAT: &str = "$duckdb_spanner_json_format";
pub(crate) const JSON_TRANSPORT_FORMAT_V1: &str = "json-text-v1";

/// Create a Statement from SQL and an optional JSON params string.
///
/// Each entry in the JSON object is either:
/// - A plain value (type inferred from JSON)
/// - A typed value with explicit Spanner type, e.g.
///   `{"id": {"value": null, "type": "INT64"}}`
///
/// Explicit type strings are parsed before the statement is built. Unknown or
/// malformed names are rejected instead of being silently sent as STRING.
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

    let spanner_type = types::parse_spanner_type(type_str).map_err(|error| {
        SpannerError::Other(format!(
            "Invalid Spanner type for parameter '{key}': {error}"
        ))
    })?;
    let value = obj.get("value").unwrap_or(&serde_json::Value::Null);
    let json_text_transport = match obj.get(JSON_TRANSPORT_FORMAT) {
        None => false,
        Some(serde_json::Value::String(format)) if format == JSON_TRANSPORT_FORMAT_V1 => true,
        Some(other) => {
            return Err(SpannerError::Other(format!(
                "Unsupported JSON transport format for parameter '{key}': {other}"
            )));
        }
    };

    match spanner_type.code() {
        TypeCode::Array => add_array_param(builder, key, value, spanner_type, json_text_transport),
        TypeCode::Struct => Err(SpannerError::Other(format!(
            "STRUCT parameters are not yet supported (parameter '{key}')"
        ))),
        _ => add_scalar_param(builder, key, value, spanner_type, json_text_transport),
    }
}

fn add_scalar_param(
    builder: StatementBuilder,
    key: &str,
    value: &serde_json::Value,
    spanner_type: Type,
    json_text_transport: bool,
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
            let value = float_param_value(key, "FLOAT32", value, true)?;
            builder.add_typed_param(key, &value, spanner_type)
        }
        TypeCode::Float64 => {
            let value = float_param_value(key, "FLOAT64", value, false)?;
            builder.add_typed_param(key, &value, spanner_type)
        }
        TypeCode::String => {
            let s = if json_text_transport {
                json_text_to_string(key, value)?
            } else {
                json_to_string_coerce(value)
            };
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
            let s = if json_text_transport {
                require_valid_json_text(key, value)?.to_string()
            } else {
                json_param_string(key, value, "JSON param")?
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
    json_text_transport: bool,
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
            let v = parse_float_array(key, arr, "FLOAT32", true)?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::Float64 => {
            let v = parse_float_array(key, arr, "FLOAT64", false)?;
            builder.add_typed_param(key, &v, spanner_type)
        }
        TypeCode::String => {
            let v = if json_text_transport {
                parse_array(key, arr, json_text_to_string)?
            } else {
                parse_array(key, arr, |_, v| Ok(json_to_string_coerce(v)))?
            };
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
            let v = if json_text_transport {
                parse_array(key, arr, |k, v| {
                    Ok(require_valid_json_text(k, v)?.to_string())
                })?
            } else {
                parse_array(key, arr, |k, v| {
                    json_param_string(k, v, "JSON array element")
                })?
            };
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

fn float_param_value(
    key: &str,
    type_name: &str,
    value: &serde_json::Value,
    narrow_to_f32: bool,
) -> Result<prost_types::Value, SpannerError> {
    use prost_types::value::Kind;

    let kind = match value {
        serde_json::Value::Number(number) => {
            let parsed = number
                .as_f64()
                .ok_or_else(|| param_err(key, type_name, value))?;
            if narrow_to_f32 {
                let narrowed = parsed as f32;
                if parsed.is_finite() && !narrowed.is_finite() {
                    return Err(param_err(key, type_name, value));
                }
                Kind::NumberValue(narrowed as f64)
            } else {
                Kind::NumberValue(parsed)
            }
        }
        serde_json::Value::String(s) if matches!(s.as_str(), "NaN" | "Infinity" | "-Infinity") => {
            Kind::StringValue(s.clone())
        }
        _ => return Err(param_err(key, type_name, value)),
    };
    Ok(prost_types::Value { kind: Some(kind) })
}

fn parse_float_array(
    key: &str,
    values: &[serde_json::Value],
    type_name: &str,
    narrow_to_f32: bool,
) -> Result<Vec<prost_types::Value>, SpannerError> {
    values
        .iter()
        .map(|value| {
            if value.is_null() {
                Ok(prost_types::Value {
                    kind: Some(prost_types::value::Kind::NullValue(0)),
                })
            } else {
                float_param_value(key, type_name, value, narrow_to_f32)
            }
        })
        .collect()
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

fn json_param_string(
    key: &str,
    value: &serde_json::Value,
    description: &str,
) -> Result<String, SpannerError> {
    match value {
        // Preserve the low-level API's existing contract: an untagged string is
        // already serialized JSON text. Scalar helpers tag structural strings.
        serde_json::Value::String(value) => Ok(value.clone()),
        other => serde_json::to_string(other).map_err(|error| {
            SpannerError::Other(format!(
                "Failed to serialize {description} for '{key}': {error}"
            ))
        }),
    }
}

fn json_text_to_string(key: &str, value: &serde_json::Value) -> Result<String, SpannerError> {
    let text = require_valid_json_text(key, value)?;
    let value: serde_json::Value = serde_json::from_str(text).map_err(|error| {
        SpannerError::Other(format!(
            "Invalid tagged JSON text for parameter '{key}': {error}"
        ))
    })?;
    Ok(json_to_string_coerce(&value))
}

fn require_valid_json_text<'a>(
    key: &str,
    value: &'a serde_json::Value,
) -> Result<&'a str, SpannerError> {
    let text = require_string(key, "tagged JSON text", value)?;
    serde_json::from_str::<serde_json::Value>(text).map_err(|error| {
        SpannerError::Other(format!(
            "Invalid tagged JSON text for parameter '{key}': {error}"
        ))
    })?;
    Ok(text)
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
        create_statement(
            "SELECT @v",
            Some(
                r#"{"v":{"$duckdb_spanner_json_format":"json-text-v1","value":"\"abc\"","type":"JSON"}}"#,
            ),
        )
        .unwrap();
        create_statement(
            "SELECT @v",
            Some(
                r#"{"v":{"$duckdb_spanner_json_format":"json-text-v1","value":"null","type":"JSON"}}"#,
            ),
        )
        .unwrap();
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
        create_statement(
            "SELECT @v",
            Some(
                r#"{"v":{"$duckdb_spanner_json_format":"json-text-v1","value":["\"abc\"","null",null],"type":"ARRAY<JSON>"}}"#,
            ),
        )
        .unwrap();
    }

    #[test]
    fn test_typed_non_finite_float_values() {
        create_statement(
            "SELECT @f32_nan, @f64_inf",
            Some(
                r#"{"f32_nan":{"value":"NaN","type":"FLOAT32"},"f64_inf":{"value":"Infinity","type":"FLOAT64"}}"#,
            ),
        )
        .unwrap();
        create_statement(
            "SELECT @f32_values, @f64_values",
            Some(
                r#"{"f32_values":{"value":["NaN",1.5,null,"-Infinity"],"type":"ARRAY<FLOAT32>"},"f64_values":{"value":["Infinity",-1.5],"type":"ARRAY<FLOAT64>"}}"#,
            ),
        )
        .unwrap();

        for special in ["NaN", "Infinity", "-Infinity"] {
            let json = serde_json::Value::String(special.to_string());
            let value = float_param_value("v", "FLOAT64", &json, false).unwrap();
            assert!(matches!(
                value.kind,
                Some(prost_types::value::Kind::StringValue(ref actual)) if actual == special
            ));
        }
    }

    #[test]
    fn test_rejects_bad_json_and_plain_arrays() {
        assert!(create_statement("SELECT @x", Some("not json")).is_err());
        assert!(create_statement("SELECT @x", Some(r#"{"x": [1,2,3]}"#)).is_err());
    }

    #[test]
    fn untagged_json_object_cannot_collide_with_transport_metadata() {
        let value = serde_json::json!({
            "$duckdb_spanner_json_format": "json-text-v1",
            "$duckdb_spanner_json_value": {"a": 1}
        });
        let encoded = json_param_string("v", &value, "JSON param").unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
            value
        );
    }

    #[test]
    fn tagged_json_text_is_validated_locally() {
        assert!(
            create_statement(
                "SELECT @v",
                Some(
                    r#"{"v":{"$duckdb_spanner_json_format":"json-text-v1","value":"not-json","type":"JSON"}}"#,
                ),
            )
            .unwrap_err()
            .to_string()
            .contains("Invalid tagged JSON text")
        );
        assert!(
            create_statement(
                "SELECT @v",
                Some(
                    r#"{"v":{"$duckdb_spanner_json_format":"json-text-v1","value":["{}","not-json"],"type":"ARRAY<JSON>"}}"#,
                ),
            )
            .unwrap_err()
            .to_string()
            .contains("Invalid tagged JSON text")
        );
    }

    #[test]
    fn rejects_unknown_and_malformed_explicit_type_before_statement_build() {
        for type_name in ["NOT_A_TYPE", "ARRAY<INT64"] {
            let params = format!(r#"{{"v":{{"value":null,"type":"{type_name}"}}}}"#);
            let error = create_statement("SELECT @v", Some(&params)).unwrap_err();
            let message = error.to_string();
            assert!(message.contains("parameter 'v'"));
            assert!(message.contains(type_name));
            assert!(!message.contains("falling back to STRING"));
        }
    }
}
