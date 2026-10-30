//! Native v2 scalar callbacks and owned-value parameter conversion.
use crate::duck::{LogicalTypeHandle, LogicalTypeId};
use base64::Engine;
use duckdb_neo::{
    DuckDBType, Result as NeoResult,
    connection::Context,
    data_chunk::VectorCollection,
    enums::FunctionProperty,
    extension::Extension,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    types::*,
    value::Value as NeoValue,
    vector::{Unknown, Vector},
};
use serde_json::{Map, Value, json};
use time::OffsetDateTime;
const TIMESTAMP_NS_INFINITY_ERROR: &str = "TIMESTAMP_NS infinity is not a valid Spanner TIMESTAMP";
#[derive(Clone, Copy)]
enum Operation {
    Value,
    Typed,
    Params,
    Interval,
}
struct Scalar(Operation);
impl ScalarCallbacks for Scalar {
    type BindData = ();
    type InitData = ();
    fn exec(
        &self,
        _: Option<&()>,
        _: Option<&mut ()>,
        _ctx: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> NeoResult<()> {
        crate::vtab_safety::guard_callback("SpannerScalar", "exec", || {
            let count = input.row_count()?;
            let ty = LogicalTypeHandle::from_neo(input.get_untyped_vector_at(0)?.logical_type())?;
            match self.0 {
                Operation::Value => validate_spanner_value_type(&ty, "spanner_value", false)?,
                Operation::Typed => validate_spanner_value_type(&ty, "spanner_typed", true)?,
                Operation::Params if ty.id() != LogicalTypeId::SqlNull => {
                    validate_params_struct_type(&ty)?
                }
                _ => {}
            }
            let mut strings = Vec::with_capacity(count);
            for row in 0..count {
                let value = input.get_untyped_vector_at(0)?.get_value_slow(row)?;
                let text = match self.0 {
                    Operation::Value => Some(serde_json::to_string(&typed_param_envelope(
                        value_json(&value, &ty)?,
                        spanner_type_name(&ty).unwrap_or_else(|| format!("{:?}", ty.id())),
                        json_type_flags(&ty).0 || json_type_flags(&ty).1,
                    ))?),
                    Operation::Typed => {
                        let type_value = input.get_untyped_vector_at(1)?.get_value_slow(row)?;
                        match type_value.get::<String>()? {
                            Some(name) => Some(serde_json::to_string(&typed_param_envelope(
                                value_json(&value, &ty)?,
                                name,
                                json_type_flags(&ty).0 || json_type_flags(&ty).1,
                            ))?),
                            None => None,
                        }
                    }
                    Operation::Params => {
                        if value.is_null()? {
                            None
                        } else {
                            let mut params = Map::new();
                            for i in 0..ty.num_children() {
                                let field = ty.child(i);
                                let child = value.get_child(i)?;
                                params.insert(
                                    ty.child_name(i),
                                    struct_field_to_param_json(
                                        value_json(&child, &field)?,
                                        &field,
                                        is_json_type(&field),
                                    ),
                                );
                            }
                            Some(serde_json::to_string(&Value::Object(params))?)
                        }
                    }
                    Operation::Interval => value
                        .get::<IntervalValue>()?
                        .map(|v| duckdb_interval_to_iso8601(v.months, v.days, v.micros)),
                };
                strings.push(text);
            }
            let mut output = output.cast::<String>()?;
            output.set_size(count)?;
            let mut writer = output.writer()?;
            for (row, text) in strings.iter().enumerate() {
                writer.set(row, text.as_deref())?;
            }
            Ok(())
        })
        .map_err(|e| duckdb_neo::Error::invalid_input(e.to_string()))
    }
}
pub(crate) fn register(ext: &Extension) -> NeoResult<()> {
    for (name, op) in [
        ("spanner_value", Operation::Value),
        ("spanner_typed", Operation::Typed),
        ("spanner_params", Operation::Params),
        ("interval_to_iso8601", Operation::Interval),
    ] {
        let parameter = if matches!(op, Operation::Interval) {
            FunctionParameter::new::<IntervalValue>("value")
        } else {
            FunctionParameter::new::<Any>("value")
        };
        let mut signature = SignatureBuilder::new().param(parameter);
        if matches!(op, Operation::Typed) {
            signature = signature.param(FunctionParameter::new::<String>("type"));
        }
        ScalarFunctionBuilder::new(
            name,
            signature.return_type(String::logical_type(ext)?),
            Scalar(op),
        )
        .set_property(FunctionProperty::HasSpecialNullHandling(true))
        .register(ext)?;
    }
    Ok(())
}
pub(crate) fn value_json(
    value: &NeoValue,
    ty: &LogicalTypeHandle,
) -> std::result::Result<Value, Box<dyn std::error::Error>> {
    if value.is_null()? {
        return Ok(Value::Null);
    }
    if is_json_type(ty) {
        let text = value.get::<String>()?.ok_or("missing non-NULL JSON")?;
        let parsed: Value = serde_json::from_str(&text)?;
        return Ok(Value::String(serde_json::to_string(&parsed)?));
    }
    if matches!(ty.id(), LogicalTypeId::List | LogicalTypeId::Array) {
        let n = value.child_count()?;
        if n > 4_194_304 {
            return Err("parameter child count exceeds64MiB entry budget".into());
        }
        return Ok(Value::Array(
            (0..n)
                .map(|i| value_json(&value.get_child(i)?, &ty.child(0)))
                .collect::<std::result::Result<_, _>>()?,
        ));
    }
    macro_rules! get {
        ($t:ty) => {
            value.get::<$t>()?.ok_or("missing non-NULL value")?
        };
    }
    use LogicalTypeId::*;
    Ok(match ty.id() {
        Boolean => json!(get!(bool)),
        Tinyint => json!(get!(i8)),
        Smallint => json!(get!(i16)),
        Integer => json!(get!(i32)),
        Bigint => json!(get!(i64)),
        UTinyint => json!(get!(u8)),
        USmallint => json!(get!(u16)),
        UInteger => json!(get!(u32)),
        UBigint => Value::String(get!(u64).to_string()),
        Hugeint => Value::String(get!(i128).to_string()),
        UHugeint => Value::String(get!(u128).to_string()),
        Float => float_to_json_value(get!(f32) as f64)?,
        Double => float_to_json_value(get!(f64))?,
        Decimal => {
            let d = get!(DecimalValue);
            Value::String(format_decimal128(d.value, u32::from(d.scale)))
        }
        Date => Value::String(epoch_days_to_date_string(get!(DateValue).0)?),
        Timestamp => Value::String(micros_to_spanner_timestamp_string(get!(TimestampValue).0)?),
        TimestampTZ => Value::String(micros_to_spanner_timestamp_string(
            get!(TimestampTzValue).0,
        )?),
        TimestampS => Value::String(micros_to_spanner_timestamp_string(
            get!(TimestampSecValue)
                .0
                .checked_mul(1_000_000)
                .ok_or("TIMESTAMP_S overflow")?,
        )?),
        TimestampMs => Value::String(micros_to_spanner_timestamp_string(
            get!(TimestampMsValue)
                .0
                .checked_mul(1000)
                .ok_or("TIMESTAMP_MS overflow")?,
        )?),
        TimestampNs => {
            let nanos = get!(TimestampNsValue).0;
            // DuckDB2.0 timestamp_base.hpp uses +/-i64::MAX as infinity;
            // do not turn them into finite dates in1677/2262.
            if nanos == i64::MAX || nanos == -i64::MAX {
                return Err(TIMESTAMP_NS_INFINITY_ERROR.into());
            }
            Value::String(
                OffsetDateTime::from_unix_timestamp_nanos(i128::from(nanos))?
                    .format(&time::format_description::well_known::Rfc3339)?,
            )
        }
        Varchar => Value::String(get!(String)),
        Blob => Value::String(base64::engine::general_purpose::STANDARD.encode(get!(BlobValue).0)),
        Bit => Value::String(
            base64::engine::general_purpose::STANDARD
                .encode(bit_physical_to_blob(&get!(BitValue).0)?),
        ),
        Uuid => Value::String(uuid::Uuid::from_bytes(get!(UuidValueRaw).to_bytes()).to_string()),
        Time => Value::String(micros_to_time_string(get!(TimeValue).0)?),
        TimeNs => Value::String(nanos_to_time_string(get!(TimeNsValue).0)?),
        Interval => {
            let i = get!(IntervalValue);
            Value::String(duckdb_interval_to_iso8601(i.months, i.days, i.micros))
        }
        id => return Err(format!("Unsupported DuckDB value type {id:?}").into()),
    })
}
/// Structural JSON conversion for COPY. Logical JSON children are parsed once;
/// ordinary strings remain strings, including envelope-like user text.
pub(crate) fn structural_json(
    value: &NeoValue,
    ty: &LogicalTypeHandle,
) -> std::result::Result<Value, Box<dyn std::error::Error>> {
    if value.is_null()? {
        return Ok(Value::Null);
    }
    if is_json_type(ty) {
        return Ok(serde_json::from_str(
            &value.get::<String>()?.ok_or("missing JSON")?,
        )?);
    }
    match ty.id() {
        LogicalTypeId::Struct => {
            let mut fields = Map::new();
            for i in 0..ty.num_children() {
                fields.insert(
                    ty.child_name(i),
                    structural_json(&value.get_child(i)?, &ty.child(i))?,
                );
            }
            Ok(Value::Object(fields))
        }
        LogicalTypeId::List | LogicalTypeId::Array => {
            let count = value.child_count()?;
            if count > 4_194_304 {
                return Err("COPY JSON child count exceeds64MiB entry budget".into());
            }
            Ok(Value::Array(
                (0..count)
                    .map(|i| structural_json(&value.get_child(i)?, &ty.child(0)))
                    .collect::<std::result::Result<_, _>>()?,
            ))
        }
        _ => copy_scalar_json(value, ty),
    }
}

/// COPY's established JSON formatting differs intentionally from parameter
/// envelopes: unsigned64 is a JSON number, decimals trim fractional zeros and
/// timestamps use RFC3339 precision. Keep these paths distinct.
pub(crate) fn copy_scalar_json(
    value: &NeoValue,
    ty: &LogicalTypeHandle,
) -> std::result::Result<Value, Box<dyn std::error::Error>> {
    if value.is_null()? {
        return Ok(Value::Null);
    }
    use LogicalTypeId::*;
    let timestamp = match ty.id() {
        Timestamp => Some((
            value.get::<TimestampValue>()?.ok_or("missing timestamp")?.0,
            1000_i128,
        )),
        TimestampTZ => Some((
            value
                .get::<TimestampTzValue>()?
                .ok_or("missing timestamp")?
                .0,
            1000_i128,
        )),
        TimestampS => Some((
            value
                .get::<TimestampSecValue>()?
                .ok_or("missing timestamp")?
                .0,
            1_000_000_000_i128,
        )),
        TimestampMs => Some((
            value
                .get::<TimestampMsValue>()?
                .ok_or("missing timestamp")?
                .0,
            1_000_000_i128,
        )),
        TimestampNs => Some((
            value
                .get::<TimestampNsValue>()?
                .ok_or("missing timestamp")?
                .0,
            1_i128,
        )),
        _ => None,
    };
    if let Some((raw, unit)) = timestamp {
        if raw == i64::MAX || raw == -i64::MAX {
            return Err("COPY does not support infinite DuckDB timestamps".into());
        }
        let nanos = i128::from(raw)
            .checked_mul(unit)
            .ok_or("COPY timestamp overflow")?;
        return Ok(Value::String(
            OffsetDateTime::from_unix_timestamp_nanos(nanos)?
                .format(&time::format_description::well_known::Rfc3339)?,
        ));
    }
    if ty.id() == UBigint {
        return Ok(json!(
            value.get::<u64>()?.ok_or("missing unsigned integer")?
        ));
    }
    if ty.id() == Decimal {
        let d = value.get::<DecimalValue>()?.ok_or("missing decimal")?;
        let formatted = format_decimal128(d.value, u32::from(d.scale));
        return Ok(Value::String(if d.scale == 0 {
            formatted
        } else {
            formatted
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        }));
    }
    value_json(value, ty)
}
fn scalar_type_name(id: LogicalTypeId) -> Option<&'static str> {
    match id {
        LogicalTypeId::Boolean => Some("BOOL"),
        LogicalTypeId::Tinyint
        | LogicalTypeId::Smallint
        | LogicalTypeId::Integer
        | LogicalTypeId::Bigint
        | LogicalTypeId::UTinyint
        | LogicalTypeId::USmallint
        | LogicalTypeId::UInteger => Some("INT64"),
        LogicalTypeId::UBigint | LogicalTypeId::Hugeint | LogicalTypeId::UHugeint => {
            Some("NUMERIC")
        }
        LogicalTypeId::Float => Some("FLOAT32"),
        LogicalTypeId::Double => Some("FLOAT64"),
        LogicalTypeId::Varchar => Some("STRING"),
        LogicalTypeId::Blob | LogicalTypeId::Bit => Some("BYTES"),
        LogicalTypeId::Date => Some("DATE"),
        LogicalTypeId::Timestamp
        | LogicalTypeId::TimestampS
        | LogicalTypeId::TimestampMs
        | LogicalTypeId::TimestampNs
        | LogicalTypeId::TimestampTZ => Some("TIMESTAMP"),
        LogicalTypeId::Uuid => Some("UUID"),
        LogicalTypeId::Interval => Some("INTERVAL"),
        LogicalTypeId::Time | LogicalTypeId::TimeNs => Some("STRING"),
        LogicalTypeId::Decimal => Some("NUMERIC"),
        _ => None,
    }
}

fn logical_scalar_type_name(ty: &LogicalTypeHandle) -> Option<&'static str> {
    if is_json_type(ty) {
        Some("JSON")
    } else {
        scalar_type_name(ty.id())
    }
}

fn is_json_type(ty: &LogicalTypeHandle) -> bool {
    ty.id() == LogicalTypeId::Varchar
        && ty
            .get_alias()
            .is_some_and(|alias| alias.eq_ignore_ascii_case("JSON"))
}

fn json_type_flags(ty: &LogicalTypeHandle) -> (bool, bool) {
    let value_is_json = is_json_type(ty);
    let child_is_json = match ty.id() {
        LogicalTypeId::List | LogicalTypeId::Array => is_json_type(&ty.child(0)),
        _ => false,
    };
    (value_is_json, child_is_json)
}

fn validate_spanner_value_type(
    ty: &LogicalTypeHandle,
    function: &str,
    allow_sql_null: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    match ty.id() {
        LogicalTypeId::List | LogicalTypeId::Array => {
            let child = ty.child(0);
            if logical_scalar_type_name(&child).is_some()
                || (allow_sql_null && child.id() == LogicalTypeId::SqlNull)
            {
                Ok(())
            } else {
                Err(format!(
                    "Unsupported nested DuckDB type {:?} for {function}; LIST and ARRAY elements must be supported scalar types",
                    child.id()
                )
                .into())
            }
        }
        LogicalTypeId::SqlNull if allow_sql_null => Ok(()),
        _ if logical_scalar_type_name(ty).is_some() => Ok(()),
        id => Err(format!(
            "Unsupported DuckDB type {id:?} for {function}; supported inputs are scalar, LIST, and ARRAY values with scalar elements"
        )
        .into()),
    }
}

fn validate_params_struct_type(ty: &LogicalTypeHandle) -> Result<(), Box<dyn std::error::Error>> {
    if ty.id() != LogicalTypeId::Struct {
        return Err(format!(
            "spanner_params requires a STRUCT argument, got {:?}",
            ty.id()
        )
        .into());
    }

    for field_idx in 0..ty.num_children() {
        let name = ty.child_name(field_idx);
        let child = ty.child(field_idx);
        if logical_scalar_type_name(&child).is_none()
            && child.id() != LogicalTypeId::SqlNull
            && !is_supported_param_container(&child)
        {
            return Err(format!(
                "Unsupported DuckDB type {:?} for spanner_params field '{name}'; fields must be supported scalar, LIST, or ARRAY values",
                child.id()
            )
            .into());
        }
    }
    Ok(())
}

fn spanner_type_name(ty: &LogicalTypeHandle) -> Option<String> {
    match ty.id() {
        LogicalTypeId::List | LogicalTypeId::Array => {
            let child = ty.child(0);
            Some(format!(
                "ARRAY<{}>",
                logical_scalar_type_name(&child).unwrap_or("UNKNOWN")
            ))
        }
        _ => logical_scalar_type_name(ty).map(str::to_string),
    }
}

fn is_supported_param_container(ty: &LogicalTypeHandle) -> bool {
    matches!(ty.id(), LogicalTypeId::List | LogicalTypeId::Array)
        && logical_scalar_type_name(&ty.child(0)).is_some()
}

fn typed_param_envelope(
    value: Value,
    type_name: impl Into<String>,
    json_text_transport: bool,
) -> Value {
    let mut object = Map::new();
    object.insert("type".to_string(), Value::String(type_name.into()));
    object.insert("value".to_string(), value);
    if json_text_transport {
        object.insert(
            crate::params::JSON_TRANSPORT_FORMAT.to_string(),
            Value::String(crate::params::JSON_TRANSPORT_FORMAT_V1.to_string()),
        );
    }
    Value::Object(object)
}

fn epoch_days_to_date_string(days: i32) -> Result<String, Box<dyn std::error::Error>> {
    let epoch = time::Date::from_calendar_date(1970, time::Month::January, 1)?;
    let date = epoch
        .checked_add(time::Duration::days(days as i64))
        .ok_or_else(|| format!("Date overflow for epoch days: {days}"))?;
    let (year, month, day) = date.to_calendar_date();
    Ok(format!("{:04}-{:02}-{:02}", year, month as u8, day))
}

fn float_to_json_value(value: f64) -> Result<Value, Box<dyn std::error::Error>> {
    if value.is_finite() {
        let number = serde_json::Number::from_f64(value)
            .ok_or_else(|| format!("Invalid JSON number: {value}"))?;
        Ok(Value::Number(number))
    } else if value.is_nan() {
        Ok(Value::String("NaN".to_string()))
    } else if value.is_sign_positive() {
        Ok(Value::String("Infinity".to_string()))
    } else {
        Ok(Value::String("-Infinity".to_string()))
    }
}

fn micros_to_time_string(micros: i64) -> Result<String, Box<dyn std::error::Error>> {
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    if !(0..MICROS_PER_DAY).contains(&micros) {
        return Err(format!("TIME value is outside one day: {micros} microseconds").into());
    }

    let hour = micros / 3_600_000_000;
    let minute = micros % 3_600_000_000 / 60_000_000;
    let second = micros % 60_000_000 / 1_000_000;
    let fractional = micros % 1_000_000;
    Ok(format!("{hour:02}:{minute:02}:{second:02}.{fractional:06}"))
}

fn nanos_to_time_string(nanos: i64) -> Result<String, Box<dyn std::error::Error>> {
    const NANOS_PER_DAY: i64 = 86_400_000_000_000;
    if !(0..NANOS_PER_DAY).contains(&nanos) {
        return Err(format!("TIME_NS value is outside one day: {nanos} nanoseconds").into());
    }

    let hour = nanos / 3_600_000_000_000;
    let minute = nanos % 3_600_000_000_000 / 60_000_000_000;
    let second = nanos % 60_000_000_000 / 1_000_000_000;
    let fractional = nanos % 1_000_000_000;
    Ok(format!("{hour:02}:{minute:02}:{second:02}.{fractional:09}"))
}

fn bit_physical_to_blob(physical: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if physical.len() < 2 {
        return Err("BIT value is missing its DuckDB storage header".into());
    }
    let padding = usize::from(physical[0]);
    if padding >= 8 {
        return Err(format!("BIT padding count {padding} is invalid").into());
    }
    let significant_bits = 8 - padding;
    let mask = if significant_bits == 8 {
        0xff
    } else {
        (1u16 << significant_bits) - 1
    } as u8;
    let mut bytes = Vec::with_capacity(physical.len() - 1);
    bytes.push(physical[1] & mask);
    bytes.extend_from_slice(&physical[2..]);
    Ok(bytes)
}

fn struct_field_to_param_json(value: Value, ty: &LogicalTypeHandle, is_json: bool) -> Value {
    if is_json {
        return typed_param_envelope(value, "JSON", true);
    }

    if matches!(ty.id(), LogicalTypeId::List | LogicalTypeId::Array) {
        let type_name = spanner_type_name(ty).unwrap_or_else(|| "ARRAY<UNKNOWN>".to_string());
        // ARRAY<JSON> elements are canonical JSON text. The transport tag tells
        // params.rs to parse that text instead of treating each element as a
        // JSON string value. Other arrays stay untagged.
        let child_is_json = is_json_type(&ty.child(0));
        return typed_param_envelope(value, type_name, child_is_json);
    }

    if value.is_null()
        && let Some(type_name) = spanner_type_name(ty)
    {
        return typed_param_envelope(Value::Null, type_name, false);
    }

    if matches!(
        ty.id(),
        LogicalTypeId::Decimal
            | LogicalTypeId::UBigint
            | LogicalTypeId::Hugeint
            | LogicalTypeId::UHugeint
    ) {
        return json!({"value": value, "type": "NUMERIC"});
    }

    if matches!(ty.id(), LogicalTypeId::Float | LogicalTypeId::Double)
        && matches!(&value, Value::String(s) if matches!(s.as_str(), "NaN" | "Infinity" | "-Infinity"))
    {
        let type_name = if ty.id() == LogicalTypeId::Float {
            "FLOAT32"
        } else {
            "FLOAT64"
        };
        return json!({"value": value, "type": type_name});
    }

    // Untyped parameters are inferred by Spanner, which defaults to INT64 when
    // the SQL expression has no type. A JSON string such as a VARCHAR, DATE,
    // or BYTES value would then fail as "could not parse ... as an integer".
    // The envelope carries the DuckDB field type and keeps VARCHAR text intact,
    // including text that looks like another envelope.
    if json_string_needs_explicit_type(ty)
        && let Some(type_name) = spanner_type_name(ty)
    {
        return typed_param_envelope(value, type_name, false);
    }

    // BOOL and FLOAT are not inferred as those types. In `SELECT @v AS col`
    // Spanner defaults an untyped parameter to INT64.
    if matches!(
        ty.id(),
        LogicalTypeId::Boolean | LogicalTypeId::Float | LogicalTypeId::Double
    ) && let Some(type_name) = spanner_type_name(ty)
    {
        return typed_param_envelope(value, type_name, false);
    }
    value
}

fn json_string_needs_explicit_type(ty: &LogicalTypeHandle) -> bool {
    matches!(
        ty.id(),
        LogicalTypeId::Varchar
            | LogicalTypeId::Date
            | LogicalTypeId::Timestamp
            | LogicalTypeId::TimestampS
            | LogicalTypeId::TimestampMs
            | LogicalTypeId::TimestampNs
            | LogicalTypeId::TimestampTZ
            | LogicalTypeId::Uuid
            | LogicalTypeId::Interval
            | LogicalTypeId::Blob
            | LogicalTypeId::Bit
            | LogicalTypeId::Time
            | LogicalTypeId::TimeNs
    )
}

fn format_decimal128(value: i128, scale: u32) -> String {
    if scale == 0 {
        return value.to_string();
    }
    let negative = value < 0;
    let abs = value.unsigned_abs();
    let divisor = 10u128.pow(scale);
    let whole = abs / divisor;
    let frac = abs % divisor;
    let frac_str = format!("{frac:0scale$}", scale = scale as usize);
    if negative {
        format!("-{whole}.{frac_str}")
    } else {
        format!("{whole}.{frac_str}")
    }
}

fn micros_to_spanner_timestamp_string(micros: i64) -> Result<String, Box<dyn std::error::Error>> {
    let secs = micros.div_euclid(1_000_000);
    let micros_rem = micros.rem_euclid(1_000_000);
    let dt = OffsetDateTime::from_unix_timestamp(secs)?;
    let (year, month, day) = dt.to_calendar_date();
    let time = dt.time();
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}Z",
        year = year,
        month = u8::from(month),
        day = day,
        hour = time.hour(),
        minute = time.minute(),
        second = time.second(),
        micros = micros_rem,
    ))
}

pub fn duckdb_interval_to_iso8601(months: i32, days: i32, micros: i64) -> String {
    let years = months.div_euclid(12);
    let months = months.rem_euclid(12);

    let hours = micros.div_euclid(3_600_000_000);
    let rem = micros.rem_euclid(3_600_000_000);
    let minutes = rem.div_euclid(60_000_000);
    let rem = rem.rem_euclid(60_000_000);
    let seconds = rem / 1_000_000;
    let frac_micros = rem.rem_euclid(1_000_000);

    if years == 0
        && months == 0
        && days == 0
        && hours == 0
        && minutes == 0
        && seconds == 0
        && frac_micros == 0
    {
        return "PT0S".to_string();
    }

    let mut result = String::from("P");
    if years != 0 {
        result.push_str(&format!("{years}Y"));
    }
    if months != 0 {
        result.push_str(&format!("{months}M"));
    }
    if days != 0 {
        result.push_str(&format!("{days}D"));
    }
    if hours != 0 || minutes != 0 || seconds != 0 || frac_micros != 0 {
        result.push('T');
        if hours != 0 {
            result.push_str(&format!("{hours}H"));
        }
        if minutes != 0 {
            result.push_str(&format!("{minutes}M"));
        }
        if seconds != 0 || frac_micros != 0 {
            result.push_str(&seconds.to_string());
            if frac_micros != 0 {
                result.push('.');
                result.push_str(&format!("{frac_micros:06}"));
            }
            result.push('S');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;
    fn open_test_connection() -> Connection {
        crate::register_static_extension().unwrap();
        Connection::open_in_memory().unwrap()
    }
    fn query_string(conn: &Connection, expression: &str) -> String {
        conn.query_row(&format!("SELECT {expression}"), [], |row| row.get(0))
            .unwrap()
    }

    fn assert_query_error(conn: &Connection, expression: &str, expected: &str) {
        let error = conn
            .query_row(&format!("SELECT {expression}"), [], |row| {
                row.get::<_, String>(0)
            })
            .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "expected error containing {expected:?}, got {error}"
        );
    }

    #[test]
    fn test_interval_one_day() {
        assert_eq!(duckdb_interval_to_iso8601(0, 1, 0), "P1D");
    }

    #[test]
    fn test_interval_year_month() {
        assert_eq!(duckdb_interval_to_iso8601(15, 0, 0), "P1Y3M");
    }

    #[test]
    fn test_interval_zero() {
        assert_eq!(duckdb_interval_to_iso8601(0, 0, 0), "PT0S");
    }

    #[test]
    fn test_interval_negative_hours() {
        // -90 minutes decomposes as -2h + 30m under euclidean component semantics
        assert_eq!(
            duckdb_interval_to_iso8601(0, 0, -90 * 60 * 1_000_000),
            "PT-2H30M"
        );
    }

    #[test]
    fn test_spanner_typed_null_type_is_sql_null() {
        let conn = open_test_connection();
        for expression in [
            "spanner_typed(INTERVAL '1 day', NULL)",
            "spanner_typed(NULL::INTERVAL, NULL)",
            "spanner_typed(42::BIGINT, NULL)",
            "spanner_typed([INTERVAL '1 day'], NULL)",
        ] {
            let result: Option<String> = conn
                .query_row(&format!("SELECT {expression}"), [], |row| row.get(0))
                .unwrap();
            assert_eq!(result, None, "{expression}");
        }

        assert_eq!(
            query_string(&conn, "spanner_typed(INTERVAL '1 day', 'INTERVAL')"),
            r#"{"type":"INTERVAL","value":"P1D"}"#
        );
        assert_eq!(
            query_string(&conn, "spanner_typed(NULL::INTERVAL, 'INTERVAL')"),
            r#"{"type":"INTERVAL","value":null}"#
        );

        let mixed: String = conn
            .query_row(
                "SELECT string_agg(COALESCE(spanner_typed(v, t), 'NULL'), '|' ORDER BY i) FROM (
                    SELECT * FROM (VALUES
                        (1, INTERVAL '1 day', NULL::VARCHAR),
                        (2, INTERVAL '1 day', 'INTERVAL'),
                        (3, NULL::INTERVAL, NULL::VARCHAR),
                        (4, NULL::INTERVAL, 'INTERVAL')
                    ) rows(i, v, t)
                )",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            mixed,
            r#"NULL|{"type":"INTERVAL","value":"P1D"}|NULL|{"type":"INTERVAL","value":null}"#
        );

        let null_type = query_string(
            &conn,
            "spanner_params({'i': spanner_typed(INTERVAL '1 day', NULL)})",
        );
        let null_type: serde_json::Value = serde_json::from_str(&null_type).unwrap();
        assert_eq!(null_type["i"]["type"], "STRING");
        assert!(null_type["i"]["value"].is_null());

        let explicit = query_string(
            &conn,
            "spanner_params({'i': spanner_typed(INTERVAL '1 day', 'INTERVAL')})",
        );
        let explicit: serde_json::Value = serde_json::from_str(&explicit).unwrap();
        assert_eq!(explicit["i"]["type"], "STRING");
        assert_eq!(
            explicit["i"]["value"],
            r#"{"type":"INTERVAL","value":"P1D"}"#
        );
    }

    #[test]
    fn test_spanner_value_null_interval() {
        let conn = open_test_connection();

        let json: String = conn
            .query_row("SELECT spanner_value(NULL::INTERVAL)", [], |r| r.get(0))
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "INTERVAL");
        assert!(parsed["value"].is_null());
    }

    #[test]
    fn test_interval_to_iso8601_null() {
        let conn = open_test_connection();

        let result: Option<String> = conn
            .query_row("SELECT interval_to_iso8601(NULL::INTERVAL)", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_spanner_value_interval_array() {
        let conn = open_test_connection();

        let json: String = conn
            .query_row(
                "SELECT spanner_value([INTERVAL '1 day', INTERVAL '1 year 3 months'])",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "ARRAY<INTERVAL>");
        assert_eq!(parsed["value"][0], "P1D");
        assert_eq!(parsed["value"][1], "P1Y3M");
    }

    #[test]
    fn test_spanner_value_utinyint() {
        let conn = open_test_connection();

        let json: String = conn
            .query_row("SELECT spanner_value(42::UTINYINT)", [], |r| r.get(0))
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "INT64");
        assert_eq!(parsed["value"], 42);
    }

    #[test]
    fn test_scalar_type_name_int64() {
        assert_eq!(scalar_type_name(LogicalTypeId::Bigint), Some("INT64"));
    }

    #[test]
    fn test_spanner_type_name_list() {
        let list = LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Bigint));
        assert_eq!(spanner_type_name(&list), Some("ARRAY<INT64>".to_string()));
    }

    #[test]
    fn test_spanner_value_null_bigint() {
        let conn = open_test_connection();

        let json: String = conn
            .query_row("SELECT spanner_value(NULL::BIGINT)", [], |r| r.get(0))
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "INT64");
        assert!(parsed["value"].is_null());
    }

    #[test]
    fn test_spanner_value_scalar() {
        let conn = open_test_connection();

        let json: String = conn
            .query_row("SELECT spanner_value(42::BIGINT)", [], |r| r.get(0))
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["type"], "INT64");
        assert_eq!(parsed["value"], 42);
    }

    #[test]
    fn test_spanner_value_exact_physical_types() {
        let conn = open_test_connection();
        let cases = [
            (
                "spanner_value('340282366920938463463374607431768211455'::UHUGEINT)",
                r#"{"type":"NUMERIC","value":"340282366920938463463374607431768211455"}"#,
            ),
            (
                "spanner_value(UUID '550e8400-e29b-41d4-a716-446655440000')",
                r#"{"type":"UUID","value":"550e8400-e29b-41d4-a716-446655440000"}"#,
            ),
            (
                "spanner_value(TIME '12:34:56.123456')",
                r#"{"type":"STRING","value":"12:34:56.123456"}"#,
            ),
            (
                "spanner_value('12345678901234567890123456789.123456789'::DECIMAL(38,9))",
                r#"{"type":"NUMERIC","value":"12345678901234567890123456789.123456789"}"#,
            ),
            (
                "spanner_value('2024-01-15 12:34:56'::TIMESTAMP_S)",
                r#"{"type":"TIMESTAMP","value":"2024-01-15T12:34:56.000000Z"}"#,
            ),
            (
                "spanner_value('2024-01-15 12:34:56.123456789'::TIMESTAMP_NS)",
                r#"{"type":"TIMESTAMP","value":"2024-01-15T12:34:56.123456789Z"}"#,
            ),
            (
                "spanner_value('1969-12-31 23:59:59.123456789'::TIMESTAMP_NS)",
                r#"{"type":"TIMESTAMP","value":"1969-12-31T23:59:59.123456789Z"}"#,
            ),
            (
                "spanner_value(NULL::TIMESTAMP_NS)",
                r#"{"type":"TIMESTAMP","value":null}"#,
            ),
        ];

        for (expression, expected) in cases {
            assert_eq!(query_string(&conn, expression), expected, "{expression}");
        }
    }

    #[test]
    fn test_bit_bytes_match_duckdb_blob_cast() {
        let conn = open_test_connection();
        let bits = vec![
            "'10101010'::BIT".to_string(),
            "'101'::BIT".to_string(),
            "'0'::BIT".to_string(),
            "'11111111'::BIT".to_string(),
            // Longer than DuckDB's inline string storage.
            format!("'{}'::BIT", "10".repeat(100)),
        ];
        for bit in bits {
            let encoded = query_string(&conn, &format!("spanner_value({bit})"));
            let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
            let expected = query_string(&conn, &format!("to_base64({bit}::BLOB)"));
            assert_eq!(parsed["type"], "BYTES", "{bit}");
            assert_eq!(parsed["value"], expected, "{bit}");

            let typed = query_string(&conn, &format!("spanner_typed({bit}, 'BYTES')"));
            let typed: serde_json::Value = serde_json::from_str(&typed).unwrap();
            assert_eq!(typed["value"], expected, "typed {bit}");
        }

        assert_eq!(
            query_string(&conn, "spanner_value(NULL::BIT)"),
            r#"{"type":"BYTES","value":null}"#
        );
        let list = query_string(
            &conn,
            "spanner_value(['101'::BIT, NULL::BIT, '10101010'::BIT])",
        );
        let list: serde_json::Value = serde_json::from_str(&list).unwrap();
        assert_eq!(list["type"], "ARRAY<BYTES>");
        assert_eq!(
            list["value"][0],
            query_string(&conn, "to_base64('101'::BIT::BLOB)")
        );
        assert!(list["value"][1].is_null());
        assert_eq!(
            list["value"][2],
            query_string(&conn, "to_base64('10101010'::BIT::BLOB)")
        );

        let array = query_string(
            &conn,
            "spanner_value(['101'::BIT, '10101010'::BIT]::BIT[2])",
        );
        let array: serde_json::Value = serde_json::from_str(&array).unwrap();
        assert_eq!(array["type"], "ARRAY<BYTES>");
        assert_eq!(
            array["value"][0],
            query_string(&conn, "to_base64('101'::BIT::BLOB)")
        );
        assert_eq!(
            array["value"][1],
            query_string(&conn, "to_base64('10101010'::BIT::BLOB)")
        );

        let blob = query_string(&conn, "spanner_value(from_hex('00AA'))");
        assert_eq!(blob, r#"{"type":"BYTES","value":"AKo="}"#);
    }

    #[test]
    fn test_timestamp_ns_infinity_is_rejected() {
        let conn = open_test_connection();
        let rejected = [
            "spanner_value('infinity'::TIMESTAMP_NS)",
            "spanner_value('-infinity'::TIMESTAMP_NS)",
            "spanner_typed('infinity'::TIMESTAMP_NS, 'TIMESTAMP')",
            "spanner_typed('-infinity'::TIMESTAMP_NS, 'TIMESTAMP')",
            "spanner_params({'v': 'infinity'::TIMESTAMP_NS})",
            "spanner_params({'v': '-infinity'::TIMESTAMP_NS})",
            "spanner_value(['infinity'::TIMESTAMP_NS])",
            "spanner_value(['-infinity'::TIMESTAMP_NS])",
            "spanner_value(['2024-01-15 12:34:56.123456789'::TIMESTAMP_NS, 'infinity'::TIMESTAMP_NS]::TIMESTAMP_NS[2])",
            "spanner_typed(['infinity'::TIMESTAMP_NS], 'ARRAY<TIMESTAMP>')",
            "spanner_params({'v': spanner_value(['infinity'::TIMESTAMP_NS])})",
        ];
        for expression in rejected {
            assert_query_error(&conn, expression, TIMESTAMP_NS_INFINITY_ERROR);
        }

        assert_eq!(
            query_string(
                &conn,
                "spanner_value([NULL::TIMESTAMP_NS, '2024-01-15 12:34:56.123456789'::TIMESTAMP_NS])"
            ),
            r#"{"type":"ARRAY<TIMESTAMP>","value":[null,"2024-01-15T12:34:56.123456789Z"]}"#
        );
    }

    #[test]
    fn test_spanner_value_distinguishes_null_and_empty_containers() {
        let conn = open_test_connection();
        let cases = [
            (
                "spanner_value(NULL::BIGINT[])",
                r#"{"type":"ARRAY<INT64>","value":null}"#,
            ),
            (
                "spanner_value([]::BIGINT[])",
                r#"{"type":"ARRAY<INT64>","value":[]}"#,
            ),
            (
                "spanner_value(NULL::BIGINT[2])",
                r#"{"type":"ARRAY<INT64>","value":null}"#,
            ),
            (
                "spanner_value([NULL, NULL]::BIGINT[2])",
                r#"{"type":"ARRAY<INT64>","value":[null,null]}"#,
            ),
        ];

        for (expression, expected) in cases {
            assert_eq!(query_string(&conn, expression), expected, "{expression}");
        }
    }

    #[test]
    fn test_spanner_value_non_finite_floats() {
        let conn = open_test_connection();
        let cases = [
            (
                "spanner_value('NaN'::FLOAT)",
                r#"{"type":"FLOAT32","value":"NaN"}"#,
            ),
            (
                "spanner_value('Infinity'::FLOAT)",
                r#"{"type":"FLOAT32","value":"Infinity"}"#,
            ),
            (
                "spanner_value('-Infinity'::FLOAT)",
                r#"{"type":"FLOAT32","value":"-Infinity"}"#,
            ),
            (
                "spanner_value('NaN'::DOUBLE)",
                r#"{"type":"FLOAT64","value":"NaN"}"#,
            ),
            (
                "spanner_value('Infinity'::DOUBLE)",
                r#"{"type":"FLOAT64","value":"Infinity"}"#,
            ),
            (
                "spanner_value('-Infinity'::DOUBLE)",
                r#"{"type":"FLOAT64","value":"-Infinity"}"#,
            ),
        ];

        for (expression, expected) in cases {
            assert_eq!(query_string(&conn, expression), expected, "{expression}");
        }
    }

    #[test]
    fn test_spanner_value_json_alias_preserves_structure() {
        let conn = open_test_connection();

        assert_eq!(
            query_string(&conn, r#"spanner_value(json('{"a":1}'))"#),
            r#"{"$duckdb_spanner_json_format":"json-text-v1","type":"JSON","value":"{\"a\":1}"}"#
        );
        let helper_in_params = query_string(
            &conn,
            r#"spanner_params({'x': spanner_value(json('{"a":1}'))})"#,
        );
        let helper_json: serde_json::Value = serde_json::from_str(&helper_in_params).unwrap();
        assert_eq!(helper_json["x"]["type"], "STRING");
        assert!(
            helper_json["x"]["value"]
                .as_str()
                .is_some_and(|text| text.contains("\"type\":\"JSON\"")),
            "helper output inside spanner_params stays STRING text, got {helper_json}"
        );
        let plain_json = query_string(&conn, r#"spanner_params({'x': json('{"a":1}')})"#);
        assert_eq!(
            plain_json,
            r#"{"x":{"$duckdb_spanner_json_format":"json-text-v1","type":"JSON","value":"{\"a\":1}"}}"#
        );
        crate::params::create_statement("SELECT @x", Some(&plain_json)).unwrap();

        assert_eq!(
            query_string(&conn, r#"spanner_value(json('"abc"'))"#),
            r#"{"$duckdb_spanner_json_format":"json-text-v1","type":"JSON","value":"\"abc\""}"#
        );
        assert_eq!(
            query_string(&conn, "spanner_value(json('null'))"),
            r#"{"$duckdb_spanner_json_format":"json-text-v1","type":"JSON","value":"null"}"#
        );
        assert_eq!(
            query_string(&conn, r#"spanner_typed(json('"abc"'), 'STRING')"#),
            r#"{"$duckdb_spanner_json_format":"json-text-v1","type":"STRING","value":"\"abc\""}"#
        );
        let as_string = query_string(
            &conn,
            r#"spanner_params({'x': spanner_typed(json('"abc"'), 'STRING')})"#,
        );
        let as_string_json: serde_json::Value = serde_json::from_str(&as_string).unwrap();
        assert_eq!(as_string_json["x"]["type"], "STRING");
        assert!(as_string_json["x"]["value"].is_string());
        let migrated = query_string(
            &conn,
            r#"json_object('x', spanner_typed(json('"abc"'), 'STRING')::JSON)"#,
        );
        let migrated_json: serde_json::Value = serde_json::from_str(&migrated).unwrap();
        assert_eq!(migrated_json["x"]["type"], "STRING");
        assert!(migrated_json["x"].is_object());
        crate::params::create_statement("SELECT @x", Some(&migrated)).unwrap();

        let json_array = query_string(
            &conn,
            r#"spanner_params({'x': [json('"abc"'), json('null'), NULL::JSON]})"#,
        );
        assert_eq!(
            json_array,
            r#"{"x":{"$duckdb_spanner_json_format":"json-text-v1","type":"ARRAY<JSON>","value":["\"abc\"","null",null]}}"#
        );
        crate::params::create_statement("SELECT @x", Some(&json_array)).unwrap();
    }

    #[test]
    fn test_spanner_params_preserves_exact_numeric_types() {
        let conn = open_test_connection();

        assert_eq!(
            query_string(
                &conn,
                "spanner_params({'x': '12345678901234567890123456789.123456789'::DECIMAL(38,9)})",
            ),
            r#"{"x":{"type":"NUMERIC","value":"12345678901234567890123456789.123456789"}}"#
        );
        assert_eq!(
            query_string(
                &conn,
                "spanner_params({'x': '18446744073709551615'::UBIGINT})",
            ),
            r#"{"x":{"type":"NUMERIC","value":"18446744073709551615"}}"#
        );
        assert_eq!(
            query_string(&conn, "spanner_params({'x': [1, 2]::BIGINT[]})"),
            r#"{"x":{"type":"ARRAY<INT64>","value":[1,2]}}"#
        );
        let helper_array = query_string(&conn, "spanner_params({'x': spanner_value([1, 2])})");
        let helper_array: serde_json::Value = serde_json::from_str(&helper_array).unwrap();
        assert_eq!(helper_array["x"]["type"], "STRING");
        assert!(helper_array["x"]["value"].is_string());

        let non_finite = query_string(&conn, "spanner_params({'x': 'NaN'::DOUBLE})");
        assert_eq!(non_finite, r#"{"x":{"type":"FLOAT64","value":"NaN"}}"#);
        crate::params::create_statement("SELECT @x", Some(&non_finite)).unwrap();
    }

    #[test]
    fn test_unsupported_physical_types_return_errors() {
        let conn = open_test_connection();

        assert_query_error(
            &conn,
            "spanner_value({'a': 1})",
            "Unsupported DuckDB type Struct for spanner_value",
        );
        assert_query_error(
            &conn,
            "spanner_typed({'a': 1}, 'JSON')",
            "Unsupported DuckDB type Struct for spanner_typed",
        );
        assert_query_error(
            &conn,
            "spanner_value([[1, 2]])",
            "Unsupported nested DuckDB type List for spanner_value",
        );
        assert_query_error(
            &conn,
            "spanner_value([{'a': 1}])",
            "Unsupported nested DuckDB type Struct for spanner_value",
        );
        assert_eq!(
            query_string(&conn, "spanner_params({'x': [1, 2]})"),
            r#"{"x":{"type":"ARRAY<INT64>","value":[1,2]}}"#
        );
        assert_query_error(
            &conn,
            "spanner_params({'x': {'a': 1}})",
            "Unsupported DuckDB type Struct for spanner_params field 'x'",
        );
    }

    #[test]
    fn test_scalar_callback_catches_panics() {
        struct Panicking;
        impl ScalarCallbacks for Panicking {
            type BindData = ();
            type InitData = ();
            fn exec(
                &self,
                _: Option<&()>,
                _: Option<&mut ()>,
                _: &Context,
                _: &mut VectorCollection,
                _: Vector<'_, Unknown>,
            ) -> NeoResult<()> {
                crate::vtab_safety::guard_callback("test_panicking_scalar", "exec", || {
                    panic!("intentional scalar panic")
                })
                .map_err(|error| duckdb_neo::Error::invalid_input(error.to_string()))
            }
        }
        let conn = crate::test_support::connection();
        ScalarFunctionBuilder::new(
            "test_panicking_scalar",
            SignatureBuilder::new()
                .param(FunctionParameter::new::<i32>("v"))
                .return_type(String::logical_type(&conn).unwrap()),
            Panicking,
        )
        .register(&conn)
        .unwrap();
        let error = conn
            .execute(
                "SELECT test_panicking_scalar(1)",
                duckdb_neo::Parameters::None,
            )
            .unwrap_err()
            .to_string();
        assert!(error.contains("intentional scalar panic"), "{error}");
        conn.execute("SELECT 42", duckdb_neo::Parameters::None)
            .unwrap();
    }

    #[test]
    fn test_spanner_params_scalar() {
        let conn = open_test_connection();

        let absent: Option<String> = conn
            .query_row("SELECT spanner_params(NULL)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(absent, None);
        let typed_absent: Option<String> = conn
            .query_row("SELECT spanner_params(NULL::STRUCT(x BIGINT))", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(typed_absent, None);

        let json: String = conn
            .query_row(
                "SELECT spanner_params({'age': 25::BIGINT, 'name': 'Alice'})",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["name"]["type"], "STRING");
        assert_eq!(parsed["name"]["value"], "Alice");
        assert_eq!(parsed["age"], 25);
        assert_eq!(
            query_string(&conn, "spanner_params({'x': NULL::BIGINT})"),
            r#"{"x":{"type":"INT64","value":null}}"#
        );
        assert_eq!(
            query_string(&conn, "spanner_params({'x': true})"),
            r#"{"x":{"type":"BOOL","value":true}}"#
        );
        assert_eq!(
            query_string(&conn, "spanner_params({'x': 1.5::FLOAT})"),
            r#"{"x":{"type":"FLOAT32","value":1.5}}"#
        );
        assert_eq!(
            query_string(&conn, "spanner_params({'x': 3.125::DOUBLE})"),
            r#"{"x":{"type":"FLOAT64","value":3.125}}"#
        );
    }

    #[test]
    fn test_interval_to_iso8601_scalar() {
        let conn = open_test_connection();

        let s: String = conn
            .query_row("SELECT interval_to_iso8601(INTERVAL '1 day')", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(s, "P1D");
    }

    #[test]
    fn test_varchar_envelope_struct_does_not_promote_literals() {
        let conn = open_test_connection();
        let literal = query_string(
            &conn,
            r#"spanner_params({'wrapper': '{"type":"INT64","value":1}', 'plain': 'hello', 'value_only': '{"value":1}'})"#,
        );
        let parsed: serde_json::Value = serde_json::from_str(&literal).unwrap();
        assert_eq!(parsed["wrapper"]["type"], "STRING");
        assert_eq!(parsed["wrapper"]["value"], r#"{"type":"INT64","value":1}"#);
        assert_eq!(parsed["plain"]["type"], "STRING");
        assert_eq!(parsed["plain"]["value"], "hello");
        assert_eq!(parsed["value_only"]["type"], "STRING");
        assert_eq!(parsed["value_only"]["value"], r#"{"value":1}"#);

        let composed = query_string(
            &conn,
            "spanner_params({'x': 42::BIGINT, 'y': NULL::INTEGER})",
        );
        let composed: serde_json::Value = serde_json::from_str(&composed).unwrap();
        assert_eq!(composed["x"], 42);
        assert_eq!(composed["y"], json!({"type": "INT64", "value": null}));
        let helper = query_string(&conn, "spanner_params({'x': spanner_value(42::BIGINT)})");
        let helper: serde_json::Value = serde_json::from_str(&helper).unwrap();
        assert_eq!(helper["x"]["type"], "STRING");
        assert!(helper["x"]["value"].is_string());

        let envelope = r#"{"type":"INT64","value":9}"#;
        let wrapper = r#"{"tag":"spanner-value-v1","envelope":"{\"type\":\"INT64\",\"value\":9}"}"#;
        for label in [envelope, wrapper] {
            let direct = query_string(&conn, &format!("spanner_params({{'x': '{label}'}})"));
            let direct: serde_json::Value = serde_json::from_str(&direct).unwrap();
            assert_eq!(direct["x"]["type"], "STRING", "direct {label}");
            assert_eq!(direct["x"]["value"], label, "direct {label}");

            let coalesced = query_string(
                &conn,
                &format!("spanner_params({{'x': COALESCE(NULL::VARCHAR, '{label}')}})"),
            );
            let coalesced: serde_json::Value = serde_json::from_str(&coalesced).unwrap();
            assert_eq!(coalesced["x"]["type"], "STRING", "coalesce {label}");
            assert_eq!(coalesced["x"]["value"], label, "coalesce {label}");

            let cased = query_string(
                &conn,
                &format!(
                    "spanner_params({{'x': CASE WHEN false THEN 'plain' ELSE '{label}' END}})"
                ),
            );
            let cased: serde_json::Value = serde_json::from_str(&cased).unwrap();
            assert_eq!(cased["x"]["type"], "STRING", "case {label}");
            assert_eq!(cased["x"]["value"], label, "case {label}");

            let unioned = query_string(
                &conn,
                &format!(
                    "string_agg(spanner_params({{'x': v}}), '|' ORDER BY v = 'plain' DESC) FROM (SELECT 'plain' AS v UNION ALL SELECT '{label}')"
                ),
            );
            let rows: Vec<serde_json::Value> = unioned
                .split('|')
                .map(|row| serde_json::from_str(row).unwrap())
                .collect();
            assert_eq!(rows[0]["x"]["type"], "STRING");
            assert_eq!(rows[0]["x"]["value"], "plain");
            assert_eq!(rows[1]["x"]["type"], "STRING");
            assert_eq!(rows[1]["x"]["value"], label, "union {label}");
        }

        conn.execute_batch("CREATE TABLE envelope_text AS SELECT 'plain' AS v")
            .unwrap();
        conn.execute("INSERT INTO envelope_text VALUES (?)", [envelope])
            .unwrap();
        let stored = query_string(
            &conn,
            "string_agg(spanner_params({'x': v}), '|' ORDER BY v = 'plain' DESC) FROM envelope_text",
        );
        let stored: Vec<serde_json::Value> = stored
            .split('|')
            .map(|row| serde_json::from_str(row).unwrap())
            .collect();
        assert_eq!(stored[0]["x"]["type"], "STRING");
        assert_eq!(stored[0]["x"]["value"], "plain");
        assert_eq!(stored[1]["x"]["type"], "STRING");
        assert_eq!(stored[1]["x"]["value"], envelope);

        // DuckDB rejects PREPARE of this SELECT (`Unexpected prepared
        // parameter`). The client prepared-statement path is the one callers
        // use, and it still binds the value as VARCHAR.
        let prepared: String = conn
            .query_row("SELECT spanner_params({'x': ?})", [wrapper], |row| {
                row.get(0)
            })
            .unwrap();
        let prepared: serde_json::Value = serde_json::from_str(&prepared).unwrap();
        assert_eq!(prepared["x"]["type"], "STRING");
        assert_eq!(prepared["x"]["value"], wrapper);
        assert_query_error(
            &conn,
            "spanner_params({'x': {'a': 1}})",
            "Unsupported DuckDB type Struct",
        );
    }
}
