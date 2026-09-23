//! Native scalar functions replacing SQL macros for Spanner parameter helpers.

use base64::Engine;
use duckdb::arrow::array::{Array, StringArray};
use duckdb::core::{
    DataChunkHandle, FlatVector, ListVector, LogicalTypeHandle, LogicalTypeId, StructVector,
};
use duckdb::ffi::{
    duckdb_date, duckdb_hugeint, duckdb_interval, duckdb_string_t, duckdb_time, duckdb_time_ns,
    duckdb_timestamp, duckdb_uhugeint,
};
use duckdb::types::DuckString;
use duckdb::vscalar::{ScalarFunctionSignature, VScalar};
use duckdb::vtab::arrow::{WritableVector, write_arrow_array_to_vector};
use serde_json::{Map, Value, json};
use time::OffsetDateTime;

use crate::RegistrationError;

const TIMESTAMP_NS_INFINITY_ERROR: &str = "TIMESTAMP_NS infinity is not a valid Spanner TIMESTAMP";

pub struct SpannerValueScalar;
pub struct SpannerTypedScalar;
pub struct SpannerParamsScalar;
pub struct IntervalToIso8601Scalar;

/// Register scalars on a raw connection with null-input special handling.
///
/// # Safety
/// `raw_con` must be a valid `duckdb_connection`.
pub(crate) struct PreparedScalars(Vec<(&'static str, OwnedScalarFunctionSet)>);

impl PreparedScalars {
    /// # Safety
    /// `raw_con` must be a valid `duckdb_connection`.
    pub(crate) unsafe fn register(
        self,
        raw_con: duckdb::ffi::duckdb_connection,
    ) -> Result<(), RegistrationError> {
        for (name, set) in self.0 {
            let rc = if crate::should_fail_status("register scalar function") {
                duckdb::ffi::DuckDBError
            } else {
                unsafe { duckdb::ffi::duckdb_register_scalar_function_set(raw_con, set.0) }
            };
            if rc != duckdb::ffi::DuckDBSuccess {
                return Err(RegistrationError::new(
                    "register scalar function",
                    name,
                    format!("DuckDB returned status {rc}"),
                ));
            }
        }
        Ok(())
    }
}

struct OwnedScalarFunctionSet(duckdb::ffi::duckdb_scalar_function_set);

impl Drop for OwnedScalarFunctionSet {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { duckdb::ffi::duckdb_destroy_scalar_function_set(&mut self.0) };
        }
    }
}

struct OwnedScalarFunction(duckdb::ffi::duckdb_scalar_function);

impl Drop for OwnedScalarFunction {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { duckdb::ffi::duckdb_destroy_scalar_function(&mut self.0) };
        }
    }
}

struct OwnedLogicalType(duckdb::ffi::duckdb_logical_type);

impl Drop for OwnedLogicalType {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { duckdb::ffi::duckdb_destroy_logical_type(&mut self.0) };
        }
    }
}

pub(crate) unsafe fn prepare_scalars_c_api() -> Result<PreparedScalars, RegistrationError> {
    unsafe {
        Ok(PreparedScalars(vec![
            (
                "spanner_value",
                prepare_nullable_scalar::<SpannerValueScalar>(
                    "spanner_value",
                    &[duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_ANY],
                )?,
            ),
            (
                "spanner_typed",
                prepare_nullable_scalar::<SpannerTypedScalar>(
                    "spanner_typed",
                    &[
                        duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_ANY,
                        duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
                    ],
                )?,
            ),
            (
                "spanner_params",
                prepare_nullable_scalar::<SpannerParamsScalar>(
                    "spanner_params",
                    &[duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_ANY],
                )?,
            ),
            (
                "interval_to_iso8601",
                prepare_nullable_scalar::<IntervalToIso8601Scalar>(
                    "interval_to_iso8601",
                    &[duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL],
                )?,
            ),
        ]))
    }
}

#[cfg(test)]
pub(crate) unsafe fn register_scalars_c_api(
    raw_con: duckdb::ffi::duckdb_connection,
) -> Result<(), RegistrationError> {
    unsafe { prepare_scalars_c_api()?.register(raw_con) }
}

unsafe fn prepare_nullable_scalar<S: VScalar>(
    name: &str,
    param_type_ids: &[u32],
) -> Result<OwnedScalarFunctionSet, RegistrationError>
where
    S::State: Default,
{
    unsafe {
        use std::ffi::CString;

        use duckdb::core::DataChunkHandle;
        use duckdb::ffi::{
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, DuckDBSuccess, duckdb_add_scalar_function_to_set,
            duckdb_create_logical_type, duckdb_create_scalar_function,
            duckdb_create_scalar_function_set, duckdb_data_chunk, duckdb_function_info,
            duckdb_scalar_function_add_parameter, duckdb_scalar_function_set_error,
            duckdb_scalar_function_set_extra_info, duckdb_scalar_function_set_function,
            duckdb_scalar_function_set_name, duckdb_scalar_function_set_return_type,
            duckdb_scalar_function_set_special_handling, duckdb_scalar_function_set_volatile,
            duckdb_vector,
        };

        unsafe extern "C" fn drop_scalar_state<T>(ptr: *mut std::ffi::c_void) {
            let _ = unsafe { Box::from_raw(ptr.cast::<T>()) };
        }

        unsafe extern "C" fn scalar_invoke<S: VScalar>(
            info: duckdb_function_info,
            input: duckdb_data_chunk,
            mut output: duckdb_vector,
        ) where
            S::State: Default,
        {
            unsafe {
                let extra =
                    duckdb::ffi::duckdb_scalar_function_get_extra_info(info) as *const S::State;
                let state = if extra.is_null() {
                    &S::State::default()
                } else {
                    &*extra
                };
                #[repr(C)]
                struct UnownedChunk {
                    ptr: duckdb_data_chunk,
                    owned: bool,
                }
                let mut chunk: DataChunkHandle = std::mem::transmute(UnownedChunk {
                    ptr: input,
                    owned: false,
                });
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    S::invoke(state, &mut chunk, &mut output)
                }));
                // DataChunkHandle's layout is private in duckdb-rs. This wrapper is borrowed
                // from DuckDB, so it must never run Drop, including after a caught panic.
                std::mem::forget(chunk);

                let error = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e.to_string()),
                    Err(payload) => Some(format!(
                        "Rust panic in DuckDB scalar function: {}",
                        panic_payload_message(payload.as_ref())
                    )),
                };
                if let Some(error) = error {
                    let msg = CString::new(error.replace('\0', "\\0"))
                        .expect("escaped scalar error cannot contain NUL");
                    duckdb_scalar_function_set_error(info, msg.as_ptr());
                }
            }
        }

        let c_name = CString::new(name).expect("scalar name must be valid UTF-8");
        let set = OwnedScalarFunctionSet(duckdb_create_scalar_function_set(c_name.as_ptr()));
        if set.0.is_null() || crate::should_fail_allocation("scalar function set") {
            return Err(RegistrationError::new(
                "allocate scalar function set",
                name,
                "duckdb_create_scalar_function_set returned null",
            ));
        }
        let scalar_function = OwnedScalarFunction(duckdb_create_scalar_function());
        if scalar_function.0.is_null() || crate::should_fail_allocation("scalar function") {
            return Err(RegistrationError::new(
                "allocate scalar function",
                name,
                "duckdb_create_scalar_function returned null",
            ));
        }
        duckdb_scalar_function_set_name(scalar_function.0, c_name.as_ptr());
        let return_type =
            OwnedLogicalType(duckdb_create_logical_type(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR));
        if return_type.0.is_null() || crate::should_fail_allocation("scalar return type") {
            return Err(RegistrationError::new(
                "allocate scalar return type",
                name,
                "duckdb_create_logical_type returned null",
            ));
        }
        duckdb_scalar_function_set_return_type(scalar_function.0, return_type.0);
        for type_id in param_type_ids {
            let param_type = OwnedLogicalType(duckdb_create_logical_type(*type_id));
            if param_type.0.is_null() || crate::should_fail_allocation("scalar parameter type") {
                return Err(RegistrationError::new(
                    "allocate scalar parameter type",
                    name,
                    format!("duckdb_create_logical_type returned null for type {type_id}"),
                ));
            }
            duckdb_scalar_function_add_parameter(scalar_function.0, param_type.0);
        }
        duckdb_scalar_function_set_function(scalar_function.0, Some(scalar_invoke::<S>));
        duckdb_scalar_function_set_special_handling(scalar_function.0);
        if S::volatile() {
            duckdb_scalar_function_set_volatile(scalar_function.0);
        }
        let state = Box::new(S::State::default());
        duckdb_scalar_function_set_extra_info(
            scalar_function.0,
            Box::into_raw(state) as *mut std::ffi::c_void,
            Some(drop_scalar_state::<S::State>),
        );
        let rc = duckdb_add_scalar_function_to_set(set.0, scalar_function.0);
        if rc != DuckDBSuccess {
            return Err(RegistrationError::new(
                "add scalar overload",
                name,
                format!("DuckDB returned status {rc}"),
            ));
        }
        Ok(set)
    }
}

fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.as_str()
    } else {
        "non-string panic payload"
    }
}

fn varchar_sig(params: Vec<LogicalTypeHandle>) -> ScalarFunctionSignature {
    ScalarFunctionSignature::exact(params, LogicalTypeHandle::from(LogicalTypeId::Varchar))
}

impl VScalar for SpannerValueScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let logical_type = input.flat_vector(0).logical_type();
        let len = input.len();

        validate_spanner_value_type(&logical_type, "spanner_value", false)?;

        if logical_type.id() == LogicalTypeId::Interval {
            let vec = input.flat_vector(0);
            let mut strings = Vec::with_capacity(len);
            for row in 0..len {
                if vec.row_is_null(row as u64) {
                    let obj = json!({"value": null, "type": "INTERVAL"});
                    strings.push(serde_json::to_string(&obj)?);
                } else {
                    let interval =
                        unsafe { vec.as_slice_with_len::<duckdb_interval>(row + 1)[row] };
                    let iso =
                        duckdb_interval_to_iso8601(interval.months, interval.days, interval.micros);
                    let obj = json!({"value": iso, "type": "INTERVAL"});
                    strings.push(serde_json::to_string(&obj)?);
                }
            }
            return write_string_array(&strings, output);
        }

        write_spanner_value_strings(input, 0, &logical_type, output, |ty| {
            spanner_type_name(ty).unwrap_or_else(|| format!("{:?}", ty.id()))
        })
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![varchar_sig(vec![LogicalTypeHandle::from(
            LogicalTypeId::Any,
        )])]
    }
}

impl VScalar for SpannerTypedScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let value_type = input.flat_vector(0).logical_type();
        let len = input.len();

        validate_spanner_value_type(&value_type, "spanner_typed", true)?;

        let mut types: Vec<Option<String>> = Vec::with_capacity(len);
        {
            let type_vec = input.flat_vector(1);
            for row in 0..len {
                if type_vec.row_is_null(row as u64) {
                    types.push(None);
                } else {
                    types.push(Some(read_varchar_at(&type_vec, row)?));
                }
            }
        }
        let (value_is_json, child_is_json) = json_type_flags(&value_type);
        let mut strings: Vec<Option<String>> = Vec::with_capacity(len);
        for (row, typ) in types.into_iter().enumerate() {
            // A NULL type argument is SQL NULL for every value type. INTERVAL
            // must not substitute its own type name; an explicit non-NULL type
            // still builds the envelope, including a typed NULL value.
            let Some(typ) = typ else {
                strings.push(None);
                continue;
            };
            let value =
                vector_to_json_value(input, 0, row, &value_type, value_is_json, child_is_json)?;
            let obj = typed_param_envelope(value, typ, value_is_json || child_is_json);
            strings.push(Some(serde_json::to_string(&obj)?));
        }

        let array: std::sync::Arc<dyn Array> = std::sync::Arc::new(StringArray::from(strings));
        write_arrow_array_to_vector(&array, output)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![varchar_sig(vec![
            LogicalTypeHandle::from(LogicalTypeId::Any),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])]
    }
}

impl VScalar for SpannerParamsScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let struct_ty = input.flat_vector(0).logical_type();
        if struct_ty.id() == LogicalTypeId::SqlNull {
            let values: Vec<Option<String>> = vec![None; len];
            let array: std::sync::Arc<dyn Array> = std::sync::Arc::new(StringArray::from(values));
            return write_arrow_array_to_vector(&array, output);
        }
        validate_params_struct_type(&struct_ty)?;
        let null_rows: Vec<bool> = {
            let vector = input.flat_vector(0);
            (0..len).map(|row| vector.row_is_null(row as u64)).collect()
        };
        let struct_vec = input.struct_vector(0);
        let fields: Vec<ParamField> = (0..struct_ty.num_children())
            .map(|field_idx| {
                let ty = struct_ty.child(field_idx);
                ParamField {
                    name: struct_ty.child_name(field_idx),
                    is_json: is_json_type(&ty),
                    ty,
                }
            })
            .collect();

        let mut strings: Vec<Option<String>> = Vec::with_capacity(len);
        for (row, is_null) in null_rows.into_iter().enumerate() {
            if is_null {
                strings.push(None);
                continue;
            }
            let map = struct_row_to_map(&struct_vec, row, &fields, len)?;
            strings.push(Some(serde_json::to_string(&Value::Object(map))?));
        }

        let array: std::sync::Arc<dyn Array> = std::sync::Arc::new(StringArray::from(strings));
        write_arrow_array_to_vector(&array, output)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![varchar_sig(vec![LogicalTypeHandle::from(
            LogicalTypeId::Any,
        )])]
    }
}

impl VScalar for IntervalToIso8601Scalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();
        let vec = input.flat_vector(0);
        let mut values: Vec<Option<String>> = Vec::with_capacity(len);
        for row in 0..len {
            if vec.row_is_null(row as u64) {
                values.push(None);
            } else {
                let interval = unsafe { vec.as_slice_with_len::<duckdb_interval>(row + 1)[row] };
                values.push(Some(duckdb_interval_to_iso8601(
                    interval.months,
                    interval.days,
                    interval.micros,
                )));
            }
        }
        let array: std::sync::Arc<dyn Array> = std::sync::Arc::new(StringArray::from(values));
        write_arrow_array_to_vector(&array, output)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![varchar_sig(vec![LogicalTypeHandle::from(
            LogicalTypeId::Interval,
        )])]
    }
}

fn write_string_array(
    strings: &[String],
    output: &mut dyn WritableVector,
) -> Result<(), Box<dyn std::error::Error>> {
    let array: std::sync::Arc<dyn Array> = std::sync::Arc::new(StringArray::from(strings.to_vec()));
    write_arrow_array_to_vector(&array, output)
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

struct ParamField {
    name: String,
    ty: LogicalTypeHandle,
    is_json: bool,
}

fn is_supported_param_container(ty: &LogicalTypeHandle) -> bool {
    matches!(ty.id(), LogicalTypeId::List | LogicalTypeId::Array)
        && logical_scalar_type_name(&ty.child(0)).is_some()
}

fn struct_field_json_value(
    struct_vec: &StructVector,
    field_idx: usize,
    row: usize,
    cap: usize,
    field: &ParamField,
) -> Result<Value, Box<dyn std::error::Error>> {
    let (value_is_json, child_is_json) = json_type_flags(&field.ty);
    match field.ty.id() {
        LogicalTypeId::List => {
            let list_vec = struct_vec.list_vector_child(field_idx);
            if list_vec.row_is_null(row as u64) {
                Ok(Value::Null)
            } else {
                list_vector_to_json_value(&list_vec, row, &field.ty, child_is_json)
            }
        }
        LogicalTypeId::Array => {
            let array_vec = struct_vec.array_vector_child(field_idx);
            if array_vec.row_is_null(row as u64) {
                Ok(Value::Null)
            } else {
                array_vector_to_json_value(&array_vec, row, &field.ty, child_is_json)
            }
        }
        _ => {
            let child_vec = struct_vec.child(field_idx, cap);
            flat_vector_to_json_value(&child_vec, row, &field.ty, value_is_json)
        }
    }
}

fn struct_row_to_map(
    struct_vec: &StructVector,
    row: usize,
    fields: &[ParamField],
    cap: usize,
) -> Result<Map<String, Value>, Box<dyn std::error::Error>> {
    let mut map = Map::new();
    for (field_idx, field) in fields.iter().enumerate() {
        let value = struct_field_json_value(struct_vec, field_idx, row, cap, field)?;
        map.insert(
            field.name.clone(),
            struct_field_to_param_json(value, &field.ty, field.is_json),
        );
    }
    Ok(map)
}

fn write_spanner_value_strings(
    input: &mut DataChunkHandle,
    col: usize,
    ty: &LogicalTypeHandle,
    output: &mut dyn WritableVector,
    type_name: impl Fn(&LogicalTypeHandle) -> String,
) -> Result<(), Box<dyn std::error::Error>> {
    let len = input.len();
    let (value_is_json, child_is_json) = json_type_flags(ty);
    let mut strings = Vec::with_capacity(len);
    for row in 0..len {
        let value = vector_to_json_value(input, col, row, ty, value_is_json, child_is_json)?;
        let obj = typed_param_envelope(value, type_name(ty), value_is_json || child_is_json);
        strings.push(serde_json::to_string(&obj)?);
    }
    write_string_array(&strings, output)
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

fn vector_to_json_value(
    input: &mut DataChunkHandle,
    col: usize,
    row: usize,
    ty: &LogicalTypeHandle,
    value_is_json: bool,
    child_is_json: bool,
) -> Result<Value, Box<dyn std::error::Error>> {
    if input.flat_vector(col).row_is_null(row as u64) {
        return Ok(Value::Null);
    }

    match ty.id() {
        LogicalTypeId::List => {
            let list_vec = input.list_vector(col);
            list_vector_to_json_value(&list_vec, row, ty, child_is_json)
        }
        LogicalTypeId::Array => {
            let array_vec = input.array_vector(col);
            array_vector_to_json_value(&array_vec, row, ty, child_is_json)
        }
        _ => {
            let vec = input.flat_vector(col);
            flat_vector_to_json_value(&vec, row, ty, value_is_json)
        }
    }
}

fn list_vector_to_json_value(
    list_vec: &ListVector,
    row: usize,
    ty: &LogicalTypeHandle,
    child_is_json: bool,
) -> Result<Value, Box<dyn std::error::Error>> {
    let (offset, length) = list_vec.get_entry(row);
    if length == 0 {
        return Ok(Value::Array(Vec::new()));
    }
    let child_ty = ty.child(0);
    let child = list_vec.child(offset + length);
    let mut out = Vec::with_capacity(length);
    for j in 0..length {
        out.push(flat_vector_to_json_value(
            &child,
            offset + j,
            &child_ty,
            child_is_json,
        )?);
    }
    Ok(Value::Array(out))
}

fn array_vector_to_json_value(
    array_vec: &duckdb::core::ArrayVector,
    row: usize,
    ty: &LogicalTypeHandle,
    child_is_json: bool,
) -> Result<Value, Box<dyn std::error::Error>> {
    let array_size = array_vec.get_array_size() as usize;
    let child_ty = ty.child(0);
    let base = row * array_size;
    let child = array_vec.child(base + array_size);
    let mut out = Vec::with_capacity(array_size);
    for j in 0..array_size {
        out.push(flat_vector_to_json_value(
            &child,
            base + j,
            &child_ty,
            child_is_json,
        )?);
    }
    Ok(Value::Array(out))
}

fn flat_vector_to_json_value(
    vec: &FlatVector,
    row: usize,
    ty: &LogicalTypeHandle,
    is_json: bool,
) -> Result<Value, Box<dyn std::error::Error>> {
    if vec.row_is_null(row as u64) {
        return Ok(Value::Null);
    }

    if is_json {
        let json_text = read_varchar_at(vec, row)?;
        let value: Value = serde_json::from_str(&json_text)
            .map_err(|e| format!("Invalid DuckDB JSON value: {e}"))?;
        return Ok(Value::String(serde_json::to_string(&value)?));
    }

    match ty.id() {
        LogicalTypeId::Boolean => Ok(json!(unsafe {
            vec.as_slice_with_len::<bool>(row + 1)[row]
        })),
        LogicalTypeId::Tinyint => Ok(json!(
            unsafe { vec.as_slice_with_len::<i8>(row + 1)[row] } as i64
        )),
        LogicalTypeId::Smallint => Ok(json!(
            unsafe { vec.as_slice_with_len::<i16>(row + 1)[row] } as i64
        )),
        LogicalTypeId::Integer => Ok(json!(unsafe { vec.as_slice_with_len::<i32>(row + 1)[row] })),
        LogicalTypeId::Bigint => Ok(json!(unsafe { vec.as_slice_with_len::<i64>(row + 1)[row] })),
        LogicalTypeId::UTinyint => Ok(json!(
            unsafe { vec.as_slice_with_len::<u8>(row + 1)[row] } as i64
        )),
        LogicalTypeId::USmallint => Ok(json!(
            unsafe { vec.as_slice_with_len::<u16>(row + 1)[row] } as i64
        )),
        LogicalTypeId::UInteger => Ok(json!(
            unsafe { vec.as_slice_with_len::<u32>(row + 1)[row] } as i64
        )),
        LogicalTypeId::UBigint => Ok(Value::String(
            unsafe { vec.as_slice_with_len::<u64>(row + 1)[row] }.to_string(),
        )),
        LogicalTypeId::Hugeint => {
            let raw = unsafe { vec.as_slice_with_len::<duckdb_hugeint>(row + 1)[row] };
            Ok(Value::String(hugeint_to_i128(raw).to_string()))
        }
        LogicalTypeId::UHugeint => {
            let raw = unsafe { vec.as_slice_with_len::<duckdb_uhugeint>(row + 1)[row] };
            Ok(Value::String(uhugeint_to_u128(raw).to_string()))
        }
        LogicalTypeId::Decimal => Ok(Value::String(decimal_vector_to_string(vec, row, ty)?)),
        LogicalTypeId::Date => {
            let days = unsafe { vec.as_slice_with_len::<duckdb_date>(row + 1)[row].days };
            Ok(Value::String(epoch_days_to_date_string(days)?))
        }
        LogicalTypeId::Timestamp | LogicalTypeId::TimestampTZ => {
            let micros = unsafe { vec.as_slice_with_len::<duckdb_timestamp>(row + 1)[row].micros };
            Ok(Value::String(micros_to_spanner_timestamp_string(micros)?))
        }
        LogicalTypeId::TimestampS => {
            let seconds = unsafe { vec.as_slice_with_len::<i64>(row + 1)[row] };
            let micros = seconds
                .checked_mul(1_000_000)
                .ok_or_else(|| format!("TIMESTAMP_S overflow for seconds {seconds}"))?;
            Ok(Value::String(micros_to_spanner_timestamp_string(micros)?))
        }
        LogicalTypeId::TimestampMs => {
            let millis = unsafe { vec.as_slice_with_len::<i64>(row + 1)[row] };
            let micros = millis
                .checked_mul(1_000)
                .ok_or_else(|| format!("TIMESTAMP_MS overflow for milliseconds {millis}"))?;
            Ok(Value::String(micros_to_spanner_timestamp_string(micros)?))
        }
        LogicalTypeId::TimestampNs => {
            let nanos = unsafe { vec.as_slice_with_len::<i64>(row + 1)[row] };
            // DuckDB stores ±infinity as the i64 nanosecond sentinels. Those
            // values are inside OffsetDateTime's range, so formatting them
            // without this check emits a finite year-2262 or year-1677 timestamp.
            let finite = unsafe {
                duckdb::ffi::duckdb_is_finite_timestamp_ns(duckdb::ffi::duckdb_timestamp_ns {
                    nanos,
                })
            };
            if !finite {
                return Err(TIMESTAMP_NS_INFINITY_ERROR.into());
            }
            let timestamp = OffsetDateTime::from_unix_timestamp_nanos(nanos as i128)?;
            // Preserve the source's nanosecond precision. Other timestamp
            // physical types intentionally use their microsecond-exact helper.
            Ok(Value::String(
                timestamp.format(&time::format_description::well_known::Rfc3339)?,
            ))
        }
        LogicalTypeId::Blob => {
            let bytes = read_blob_at(vec, row)?;
            Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(bytes),
            ))
        }
        LogicalTypeId::Bit => {
            // DuckDB's physical BIT string starts with a padding-count byte.
            // CAST(bit AS BLOB) strips that header and masks the padding bits.
            let physical = read_blob_at(vec, row)?;
            let bytes = bit_physical_to_blob(&physical)?;
            Ok(Value::String(
                base64::engine::general_purpose::STANDARD.encode(bytes),
            ))
        }
        LogicalTypeId::Float => {
            float_to_json_value(unsafe { vec.as_slice_with_len::<f32>(row + 1)[row] } as f64)
        }
        LogicalTypeId::Double => {
            float_to_json_value(unsafe { vec.as_slice_with_len::<f64>(row + 1)[row] })
        }
        LogicalTypeId::Varchar => Ok(Value::String(read_varchar_at(vec, row)?)),
        LogicalTypeId::Uuid => {
            let raw = unsafe { vec.as_slice_with_len::<duckdb_hugeint>(row + 1)[row] };
            let uuid_bits = (hugeint_to_i128(raw) as u128) ^ (1u128 << 127);
            Ok(Value::String(uuid::Uuid::from_u128(uuid_bits).to_string()))
        }
        LogicalTypeId::Time => {
            let micros = unsafe { vec.as_slice_with_len::<duckdb_time>(row + 1)[row].micros };
            Ok(Value::String(micros_to_time_string(micros)?))
        }
        LogicalTypeId::TimeNs => {
            let nanos = unsafe { vec.as_slice_with_len::<duckdb_time_ns>(row + 1)[row].nanos };
            Ok(Value::String(nanos_to_time_string(nanos)?))
        }
        LogicalTypeId::Interval => {
            let interval = unsafe { vec.as_slice_with_len::<duckdb_interval>(row + 1)[row] };
            Ok(Value::String(duckdb_interval_to_iso8601(
                interval.months,
                interval.days,
                interval.micros,
            )))
        }
        id => Err(format!("Unsupported DuckDB physical vector type {id:?}").into()),
    }
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

fn hugeint_to_i128(value: duckdb_hugeint) -> i128 {
    ((value.upper as i128) << 64) | value.lower as i128
}

fn uhugeint_to_u128(value: duckdb_uhugeint) -> u128 {
    ((value.upper as u128) << 64) | value.lower as u128
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

fn decimal_vector_to_string(
    vec: &FlatVector,
    row: usize,
    ty: &LogicalTypeHandle,
) -> Result<String, Box<dyn std::error::Error>> {
    let internal_type = decimal_internal_type(ty.decimal_width());
    let data = unsafe { vec.as_mut_ptr::<std::ffi::c_void>() };
    let raw = unsafe { read_decimal_raw(data, row, internal_type) };
    Ok(format_decimal128(raw, ty.decimal_scale() as u32))
}

fn decimal_internal_type(width: u8) -> u32 {
    if width <= 4 {
        duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT
    } else if width <= 9 {
        duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER
    } else if width <= 18 {
        duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT
    } else {
        duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT
    }
}

unsafe fn read_decimal_raw(
    data: *mut std::ffi::c_void,
    row_idx: usize,
    internal_type: u32,
) -> i128 {
    unsafe {
        match internal_type {
            duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => {
                *data.cast::<i16>().add(row_idx) as i128
            }
            duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => {
                *data.cast::<i32>().add(row_idx) as i128
            }
            duckdb::ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => *data.cast::<i64>().add(row_idx) as i128,
            _ => hugeint_to_i128(*data.cast::<duckdb_hugeint>().add(row_idx)),
        }
    }
}

/// Convert DuckDB's physical BIT storage to the bytes produced by `CAST(bit AS BLOB)`.
///
/// The first byte is the count of padding bits in the following byte. The
/// remaining bytes are the payload, with padding bits set. `Bit::BitToBlob`
/// keeps the low `(8 - padding)` bits of that first payload byte and copies
/// the rest unchanged.
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

fn read_blob_at(vec: &FlatVector, row: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let strings = unsafe { vec.as_slice_with_len::<duckdb_string_t>(row + 1) };
    let mut s = strings[row];
    let len = unsafe { duckdb::ffi::duckdb_string_t_length(s) } as usize;
    let data = unsafe { duckdb::ffi::duckdb_string_t_data(&mut s) };
    Ok(unsafe { std::slice::from_raw_parts(data.cast::<u8>(), len).to_vec() })
}

fn read_varchar_at(vec: &FlatVector, row: usize) -> Result<String, Box<dyn std::error::Error>> {
    let strings = unsafe { vec.as_slice_with_len::<duckdb_string_t>(row + 1) };
    let mut s = strings[row];
    Ok(DuckString::new(&mut s).as_str().to_string())
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

    struct PanickingScalar;

    impl VScalar for PanickingScalar {
        type State = ();

        fn invoke(
            _: &Self::State,
            _: &mut DataChunkHandle,
            _: &mut dyn WritableVector,
        ) -> Result<(), Box<dyn std::error::Error>> {
            panic!("intentional scalar panic")
        }

        fn signatures() -> Vec<ScalarFunctionSignature> {
            vec![varchar_sig(vec![LogicalTypeHandle::from(
                LogicalTypeId::Integer,
            )])]
        }
    }

    fn open_test_connection() -> Connection {
        use duckdb::ffi::{self, DuckDBSuccess};
        unsafe {
            let mut db = std::ptr::null_mut();
            let r = ffi::duckdb_open_ext(
                c":memory:".as_ptr(),
                &mut db,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            assert_eq!(r, DuckDBSuccess, "duckdb_open_ext failed");
            let mut raw_con = std::ptr::null_mut();
            let rc = ffi::duckdb_connect(db, &mut raw_con);
            assert_eq!(rc, DuckDBSuccess, "duckdb_connect failed");
            register_scalars_c_api(raw_con).unwrap();
            PreparedScalars(vec![(
                "test_panicking_scalar",
                prepare_nullable_scalar::<PanickingScalar>(
                    "test_panicking_scalar",
                    &[ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER],
                )
                .unwrap(),
            )])
            .register(raw_con)
            .unwrap();
            ffi::duckdb_disconnect(&mut raw_con);
            Connection::open_from_raw(db).unwrap()
        }
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

        assert_eq!(
            query_string(
                &conn,
                "spanner_params({'i': spanner_typed(INTERVAL '1 day', NULL)})",
            ),
            r#"{"i":null}"#
        );
        assert_eq!(
            query_string(
                &conn,
                "spanner_params({'i': spanner_typed(INTERVAL '1 day', 'INTERVAL')})",
            ),
            r#"{"i":{"type":"INTERVAL","value":"P1D"}}"#
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
        let conn = open_test_connection();

        assert_query_error(
            &conn,
            "test_panicking_scalar(1)",
            "Rust panic in DuckDB scalar function: intentional scalar panic",
        );
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
