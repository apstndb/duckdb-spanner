//! One native v2 table-function adapter. No classic handles or callback implementation.
//!
//! Spanner binding and streaming remain independent of callback lifetimes. Schema
//! descriptors are owned Rust data; only this boundary materializes factory-bound
//! DuckDB types. Borrowed contexts/chunks never enter bind or execution state.
use duckdb_neo::{
    DuckDBType, Parameters, ToValue,
    bind_arguments::BindArgument,
    connection::{Context, FFILink},
    data_chunk::{DataChunk, DataChunkRef},
    logical_type::{LogicalType, LogicalTypeID},
    signature::{FunctionParameter, FunctionParameterKind, SignatureBuilder},
    table_function::{
        BindFunctionHandle, ExecColumnInfo, InitColumnData, TableFunctionBuilder,
        TableFunctionCallbacks,
    },
    value::Value as NeoValue,
};
use std::{error::Error, fmt, marker::PhantomData};
pub type CallbackResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalTypeId {
    SqlNull,
    Boolean,
    Tinyint,
    Smallint,
    Integer,
    Bigint,
    Hugeint,
    UTinyint,
    USmallint,
    UInteger,
    UBigint,
    UHugeint,
    Float,
    Double,
    Varchar,
    Blob,
    Bit,
    Date,
    Time,
    TimeNs,
    Timestamp,
    TimestampS,
    TimestampMs,
    TimestampNs,
    TimestampTZ,
    TimestampTZNs,
    Interval,
    Uuid,
    Decimal,
    List,
    Array,
    Struct,
    Map,
    Any,
    Other,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalTypeHandle {
    id: LogicalTypeId,
    children: Vec<(String, Self)>,
    width: u8,
    scale: u8,
    size: usize,
    alias: Option<String>,
}
impl From<LogicalTypeId> for LogicalTypeHandle {
    fn from(id: LogicalTypeId) -> Self {
        Self {
            id,
            children: vec![],
            width: 0,
            scale: 0,
            size: 0,
            alias: None,
        }
    }
}
impl LogicalTypeHandle {
    pub fn id(&self) -> LogicalTypeId {
        self.id
    }
    pub fn decimal(width: u8, scale: u8) -> Self {
        Self {
            width,
            scale,
            ..Self::from(LogicalTypeId::Decimal)
        }
    }
    pub fn list(child: &Self) -> Self {
        Self {
            children: vec![(String::new(), child.clone())],
            ..Self::from(LogicalTypeId::List)
        }
    }
    pub fn struct_type(fields: &[(&str, Self)]) -> Self {
        Self {
            children: fields
                .iter()
                .map(|(n, t)| (n.to_string(), t.clone()))
                .collect(),
            ..Self::from(LogicalTypeId::Struct)
        }
    }
    pub fn child(&self, i: usize) -> Self {
        self.children[i].1.clone()
    }
    pub fn child_name(&self, i: usize) -> String {
        self.children[i].0.clone()
    }
    pub fn num_children(&self) -> usize {
        self.children.len()
    }
    pub fn set_alias(&mut self, name: &str) {
        self.alias = Some(name.to_owned());
    }
    pub fn get_alias(&self) -> Option<String> {
        self.alias.clone()
    }
    #[cfg(test)]
    pub fn array_size(&self) -> usize {
        self.size
    }
    pub fn sql(&self) -> String {
        use LogicalTypeId::*;
        if let Some(alias) = &self.alias {
            return alias.clone();
        }
        match self.id {
            List => format!("{}[]", self.child(0).sql()),
            Array => format!("{}[{}]", self.child(0).sql(), self.size),
            Struct => format!(
                "STRUCT({})",
                self.children
                    .iter()
                    .map(|(n, t)| format!("\"{}\" {}", n.replace('"', "\"\""), t.sql()))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Decimal => format!("DECIMAL({},{})", self.width, self.scale),
            id => match id {
                SqlNull => "NULL",
                Boolean => "BOOLEAN",
                Tinyint => "TINYINT",
                Smallint => "SMALLINT",
                Integer => "INTEGER",
                Bigint => "BIGINT",
                Hugeint => "HUGEINT",
                UTinyint => "UTINYINT",
                USmallint => "USMALLINT",
                UInteger => "UINTEGER",
                UBigint => "UBIGINT",
                UHugeint => "UHUGEINT",
                Float => "FLOAT",
                Double => "DOUBLE",
                Varchar => "VARCHAR",
                Blob => "BLOB",
                Bit => "BIT",
                Date => "DATE",
                Time => "TIME",
                TimeNs => "TIME_NS",
                Timestamp => "TIMESTAMP",
                TimestampS => "TIMESTAMP_S",
                TimestampMs => "TIMESTAMP_MS",
                TimestampNs => "TIMESTAMP_NS",
                TimestampTZ => "TIMESTAMPTZ",
                TimestampTZNs => "TIMESTAMP_TZ_NS",
                Interval => "INTERVAL",
                Uuid => "UUID",
                Any => "ANY",
                _ => "INVALID",
            }
            .into(),
        }
    }
    pub fn build(&self, ctx: &(impl FFILink + ?Sized)) -> duckdb_neo::Result<LogicalType> {
        // Aliases are labels on storage types, not catalog type names. Build
        // children directly so nested aliases are never reparsed as SQL types.
        let base = match self.id {
            LogicalTypeId::List | LogicalTypeId::Array => {
                let child = self.child(0).build(ctx)?;
                let child = NeoValue::from_logical_type(ctx, &child)?;
                if self.id == LogicalTypeId::List {
                    ctx.logical_type_create("LIST", Parameters::positional(&[&child]))?
                } else {
                    let size = u64::try_from(self.size)
                        .map_err(|_| {
                            duckdb_neo::Error::invalid_input("ARRAY length does not fit u64")
                        })?
                        .value(ctx)?;
                    ctx.logical_type_create("ARRAY", Parameters::positional(&[&child, &size]))?
                }
            }
            LogicalTypeId::Struct => {
                // Named type parameters cannot express duplicate empty names.
                // Infer the STRUCT type from typed NULL children instead; the
                // public value constructor preserves Spanner's unnamed fields.
                let children = self
                    .children
                    .iter()
                    .map(|(_, ty)| NeoValue::null(ctx, &ty.build(ctx)?))
                    .collect::<duckdb_neo::Result<Vec<_>>>()?;
                let names = self
                    .children
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>();
                ctx.create_value(duckdb_neo::value::ValueInput::Struct {
                    names: &names,
                    children: &children,
                })?
                .logical_type()?
            }
            LogicalTypeId::Any => {
                ctx.logical_type_create_from_id(LogicalTypeID::Any, Parameters::None)?
            }
            LogicalTypeId::SqlNull => {
                ctx.logical_type_create_from_id(LogicalTypeID::SqlNull, Parameters::None)?
            }
            _ => {
                let mut storage = self.clone();
                storage.alias = None;
                LogicalType::from_text(ctx, &storage.sql())?
            }
        };
        match &self.alias {
            Some(alias) => base.with_alias(ctx, alias),
            None => Ok(base),
        }
    }
    pub fn from_neo(ty: &LogicalType) -> duckdb_neo::Result<Self> {
        Self::from_neo_depth(ty, 0)
    }
    fn from_neo_depth(ty: &LogicalType, depth: usize) -> duckdb_neo::Result<Self> {
        if depth > 64 {
            return Err(duckdb_neo::Error::invalid_input(
                "DuckDB type nesting exceeds64",
            ));
        }
        use LogicalTypeID as N;
        use LogicalTypeId as T;
        let id = match ty.id()? {
            N::SqlNull => T::SqlNull,
            N::Boolean => T::Boolean,
            N::TinyInt => T::Tinyint,
            N::SmallInt => T::Smallint,
            N::Integer => T::Integer,
            N::BigInt => T::Bigint,
            N::HugeInt => T::Hugeint,
            N::UTinyInt => T::UTinyint,
            N::USmallInt => T::USmallint,
            N::UInteger => T::UInteger,
            N::UBigInt => T::UBigint,
            N::UHugeInt => T::UHugeint,
            N::Float => T::Float,
            N::Double => T::Double,
            N::Varchar => T::Varchar,
            N::Blob => T::Blob,
            N::Bit => T::Bit,
            N::Date => T::Date,
            N::Time => T::Time,
            N::TimeNs => T::TimeNs,
            N::Timestamp => T::Timestamp,
            N::TimestampSec => T::TimestampS,
            N::TimestampMs => T::TimestampMs,
            N::TimestampNs => T::TimestampNs,
            N::TimestampTz => T::TimestampTZ,
            N::TimestampTzNs => T::TimestampTZNs,
            N::Interval => T::Interval,
            N::Uuid => T::Uuid,
            N::Decimal => T::Decimal,
            N::List => T::List,
            N::Array => T::Array,
            N::Struct => T::Struct,
            N::Map => T::Map,
            N::Any => T::Any,
            _ => T::Other,
        };
        let mut result = Self::from(id);
        if ty.to_text()?.eq_ignore_ascii_case("JSON") {
            result.set_alias("JSON");
        }
        if id == T::Decimal {
            let d = duckdb_neo::types::DecimalSignature::from_logical_type(ty)?;
            result.width = d.width;
            result.scale = d.scale;
        }
        if matches!(id, T::List | T::Array | T::Struct) {
            for i in 0..ty.param_count()? {
                let (name, value) = ty.get_param(i)?;
                if id == T::Array && i == 1 {
                    let length = value.get::<i64>()?.ok_or_else(|| {
                        duckdb_neo::Error::invalid_input("ARRAY length metadata is NULL")
                    })?;
                    result.size = usize::try_from(length).map_err(|_| {
                        duckdb_neo::Error::invalid_input(
                            "ARRAY length metadata is negative or exceeds usize",
                        )
                    })?;
                    if result.size == 0 {
                        return Err(duckdb_neo::Error::invalid_input(
                            "ARRAY length metadata must be positive",
                        ));
                    }
                    continue;
                }
                // TYPE parameters expose LogicalType through the public value getter.
                // Do not inspect an internal TYPE value or its storage layout.
                let child = value
                    .get::<LogicalType>()?
                    .ok_or_else(|| duckdb_neo::Error::api_error("missing child TYPE metadata"))?;
                result
                    .children
                    .push((name.to_owned(), Self::from_neo_depth(&child, depth + 1)?));
            }
        }
        Ok(result)
    }
}
impl fmt::Display for LogicalTypeHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.sql())
    }
}

/// Owned constant arguments, decoded before the domain bind callback.
#[derive(Clone, Debug)]
pub struct Value {
    id: LogicalTypeId,
    text: String,
    number: i64,
    null: bool,
    children: Option<Vec<Self>>,
}
impl Value {
    pub fn from_neo(value: NeoValue) -> duckdb_neo::Result<Self> {
        let ty = LogicalTypeHandle::from_neo(&value.logical_type()?)?;
        let null = value.is_null()?;
        let number = if null {
            0
        } else {
            match ty.id() {
                LogicalTypeId::Boolean => i64::from(value.get::<bool>()?.unwrap_or_default()),
                LogicalTypeId::Integer => i64::from(value.get::<i32>()?.unwrap_or_default()),
                LogicalTypeId::Bigint => value.get::<i64>()?.unwrap_or_default(),
                _ => 0,
            }
        };
        let children = if !null && matches!(ty.id(), LogicalTypeId::List | LogicalTypeId::Array) {
            Some(
                value
                    .children()?
                    .into_iter()
                    .map(Self::from_neo)
                    .collect::<duckdb_neo::Result<_>>()?,
            )
        } else {
            None
        };
        Ok(Self {
            id: ty.id(),
            text: if null {
                String::new()
            } else if ty.id() == LogicalTypeId::Varchar {
                value.get::<String>()?.unwrap_or_default()
            } else {
                value.dbg_string()?
            },
            number,
            null,
            children,
        })
    }
    pub fn is_null(&self) -> bool {
        self.null
    }
    pub fn to_int64(&self) -> i64 {
        self.number
    }
    pub fn logical_type_id(&self) -> LogicalTypeId {
        self.id
    }
    pub fn to_list(&self) -> Option<Vec<Self>> {
        self.children.clone()
    }
}
impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

pub struct BindInfo<'a> {
    pub context: &'a Context,
    pub(crate) settings: crate::config::Settings,
    handle: BindFunctionHandle<'a>,
    params: Vec<Value>,
    named: Vec<(String, Value)>,
}
impl BindInfo<'_> {
    pub fn get_parameter(&self, i: usize) -> Value {
        self.params[i].clone()
    }
    pub fn get_named_parameter(&self, name: &str) -> Option<Value> {
        self.named
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.clone())
    }
    pub fn add_result_column(&self, name: &str, ty: LogicalTypeHandle) -> duckdb_neo::Result<()> {
        self.handle
            .add_result_column(name, &ty.build(self.context)?)
    }
    pub fn set_cardinality(&self, n: u64, exact: bool) -> duckdb_neo::Result<()> {
        self.handle.set_cardinality(n as usize, exact)
    }
}
pub struct InitInfo<'a> {
    pub context: &'a Context,
    bind: &'a dyn std::any::Any,
    columns: Vec<usize>,
    #[cfg(test)]
    extra: Option<&'a (dyn std::any::Any + Send + Sync)>,
}
impl InitInfo<'_> {
    #[cfg(test)]
    pub fn get_extra_info<T: 'static>(&self) -> Option<&T> {
        self.extra?.downcast_ref()
    }
    pub fn get_bind_data<T: 'static>(&self) -> &T {
        self.bind.downcast_ref().expect("table adapter bind type")
    }
    pub fn get_column_indices(&self) -> Vec<u64> {
        self.columns.iter().map(|i| *i as u64).collect()
    }
    pub fn set_max_threads(&self, n: usize) {
        // Global init supplies one worker. A future core must not silently
        // request parallel callback execution that this adapter cannot honor.
        assert_eq!(n, 1, "Spanner v2 table adapter supports one worker");
    }
}
pub struct TableFunctionInfo<'a, T: VTab> {
    bind: &'a T::BindData,
    init: &'a T::InitData,
}
impl<'a, T: VTab> TableFunctionInfo<'a, T> {
    pub fn get_bind_data(&self) -> &'a T::BindData {
        self.bind
    }
    pub fn get_init_data(&self) -> &'a T::InitData {
        self.init
    }
}
// The owned variant lets conversion tests exercise the same Neo vectors without
// synthesizing or retaining a callback Context. Release builds have one variant.
enum ChunkStorage<'a> {
    Borrowed(DataChunkRef<'a>),
    #[cfg(test)]
    Owned(DataChunk<'a>),
}
impl<'a> std::ops::Deref for ChunkStorage<'a> {
    type Target = DataChunkRef<'a>;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(chunk) => chunk,
            #[cfg(test)]
            Self::Owned(chunk) => chunk,
        }
    }
}
impl ChunkStorage<'_> {
    fn vector_mut(
        &mut self,
        col: usize,
    ) -> duckdb_neo::Result<duckdb_neo::vector::Vector<'_, duckdb_neo::vector::Unknown>> {
        match self {
            Self::Borrowed(chunk) => chunk.get_vector_at_mut(col),
            #[cfg(test)]
            Self::Owned(chunk) => chunk.get_vector_at_mut(col),
        }
    }
}
pub struct DataChunkHandle<'a> {
    chunk: ChunkStorage<'a>,
    pub context: &'a dyn FFILink,
}
impl<'a> DataChunkHandle<'a> {
    #[cfg(test)]
    pub fn new(context: &'a impl FFILink, types: &[LogicalTypeHandle]) -> Self {
        let types = types
            .iter()
            .map(|ty| ty.build(context))
            .collect::<duckdb_neo::Result<Vec<_>>>()
            .unwrap();
        Self {
            chunk: ChunkStorage::Owned(DataChunk::create(context, types, true).unwrap()),
            context,
        }
    }
    #[cfg(test)]
    pub fn len(&self) -> duckdb_neo::Result<usize> {
        self.chunk.row_count()
    }
    #[cfg(test)]
    pub fn num_columns(&self) -> usize {
        self.chunk.col_count()
    }
    #[cfg(test)]
    pub fn value_at(&self, col: usize, row: usize) -> duckdb_neo::Result<NeoValue> {
        self.chunk
            .get_vector_at::<duckdb_neo::vector::Unknown>(col)?
            .get_value_slow(row)
    }
    pub fn capacity(&self) -> duckdb_neo::Result<usize> {
        self.chunk.capacity()
    }
    pub fn set_len(&mut self, n: usize) -> duckdb_neo::Result<()> {
        if n > 0 && self.chunk.col_count() == 0 {
            return Err(duckdb_neo::Error::invalid_input(
                "C API v2 cannot publish rows without an output vector",
            ));
        }
        for i in 0..self.chunk.col_count() {
            self.chunk.vector_mut(i)?.set_size(n)?;
        }
        Ok(())
    }
    pub fn write(&mut self, col: usize, row: usize, value: NeoValue) -> duckdb_neo::Result<()> {
        self.chunk.vector_mut(col)?.write_value_slow(row, value)
    }
    pub fn set<T: ToValue>(&mut self, col: usize, row: usize, value: T) -> duckdb_neo::Result<()> {
        self.write(col, row, value.value(self.context)?)
    }
}
pub trait VTab: Send + Sync + 'static {
    type BindData: Send + Sync + 'static;
    type InitData: Send + Sync + 'static;
    fn bind(bind: &BindInfo) -> CallbackResult<Self::BindData>;
    fn init(init: &InitInfo) -> CallbackResult<Self::InitData>;
    fn func(info: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> CallbackResult<()>
    where
        Self: Sized;
    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        None
    }
    fn supports_pushdown() -> bool {
        false
    }
}
struct TableAdapter<T> {
    marker: PhantomData<T>,
    #[cfg(test)]
    extra: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
}
fn error(e: Box<dyn Error>) -> duckdb_neo::Error {
    duckdb_neo::Error::invalid_input(e.to_string())
}
impl<T: VTab> TableFunctionCallbacks for TableAdapter<T> {
    type BindData = T::BindData;
    type GlobalState = ();
    type LocalState = T::InitData;
    fn bind(
        &self,
        ctx: &Context,
        args: Vec<BindArgument>,
        handle: BindFunctionHandle<'_>,
    ) -> duckdb_neo::Result<T::BindData> {
        let positional = T::parameters().unwrap_or_default().len();
        let names = T::named_parameters().unwrap_or_default();
        let mut values = Vec::with_capacity(args.len());
        for arg in args {
            let Some(value) = arg.value else {
                return Err(duckdb_neo::Error{code:duckdb_neo::error::DuckDBError::QueryParameterNotResolved,message:"Spanner table arguments must be constant; prepared parameters are resolved at execution".into()});
            };
            values.push(Value::from_neo(value)?);
        }
        let named = values
            .split_off(positional)
            .into_iter()
            .zip(names)
            .map(|(v, (n, _))| (n, v))
            .collect();
        crate::vtab_safety::guard_callback("SpannerTable", "bind", || {
            T::bind(&BindInfo {
                context: ctx,
                settings: crate::config::Settings::read(ctx)?,
                handle,
                params: values,
                named,
            })
        })
        .map_err(error)
    }
    fn init_local_state(
        &self,
        bind: &T::BindData,
        _: Option<&()>,
        ctx: &Context,
        columns: InitColumnData<'_>,
    ) -> duckdb_neo::Result<Option<T::InitData>> {
        let indices = (0..columns.count()?)
            .map(|i| columns.get_column_index(i))
            .collect::<duckdb_neo::Result<Vec<_>>>()?;
        crate::vtab_safety::guard_callback("SpannerTable", "init", || {
            T::init(&InitInfo {
                context: ctx,
                bind,
                columns: indices,
                #[cfg(test)]
                extra: self.extra.as_deref(),
            })
        })
        .map(Some)
        .map_err(error)
    }
    fn init_global_state(
        &self,
        _: &T::BindData,
        _: &Context,
        _: InitColumnData<'_>,
    ) -> duckdb_neo::Result<(Option<()>, Option<usize>)> {
        Ok((None, Some(1)))
    }
    fn exec(
        &self,
        bind: &T::BindData,
        _: Option<&()>,
        init: Option<&mut T::InitData>,
        ctx: &Context,
        output: DataChunkRef<'_>,
        _: ExecColumnInfo<'_>,
    ) -> duckdb_neo::Result<()> {
        let init =
            init.ok_or_else(|| duckdb_neo::Error::api_error("Spanner scan state missing"))?;
        crate::vtab_safety::guard_callback("SpannerTable", "exec", || {
            T::func(
                &TableFunctionInfo { bind, init },
                &mut DataChunkHandle {
                    chunk: ChunkStorage::Borrowed(output),
                    context: ctx,
                },
            )
        })
        .map_err(error)
    }
}
fn table_builder<T: VTab>(
    link: &impl FFILink,
    name: &str,
    callback: TableAdapter<T>,
) -> duckdb_neo::Result<TableFunctionBuilder<TableAdapter<T>>> {
    let mut signature = SignatureBuilder::new();
    for (i, ty) in T::parameters().unwrap_or_default().into_iter().enumerate() {
        signature = signature.param(
            FunctionParameter::with_type(format!("arg{i}"), ty.build(link)?)
                .kind(FunctionParameterKind::PositionalOnly),
        );
    }
    for (name, ty) in T::named_parameters().unwrap_or_default() {
        // This declared parameter type coerces the NULL default. ANY macro
        // parameters need typed defaults instead (see register_macros).
        signature = signature.param(
            FunctionParameter::with_type(name, ty.build(link)?)
                .kind(FunctionParameterKind::Named)
                .default_value(Option::<String>::None),
        );
    }
    Ok(TableFunctionBuilder::new(name, signature, callback)
        .with_projection_pushdown(T::supports_pushdown()))
}
pub fn register<T: VTab>(
    ext: &duckdb_neo::extension::Extension,
    name: &str,
) -> duckdb_neo::Result<()> {
    table_builder::<T>(
        ext,
        name,
        TableAdapter {
            marker: PhantomData,
            #[cfg(test)]
            extra: None,
        },
    )?
    .register(ext)
}
#[cfg(test)]
pub fn register_for_tests<T: VTab>(
    connection: &duckdb_neo::connection::Connection,
    name: &str,
    extra: Option<std::sync::Arc<dyn std::any::Any + Send + Sync>>,
) -> duckdb_neo::Result<()> {
    table_builder::<T>(
        connection,
        name,
        TableAdapter {
            marker: PhantomData,
            extra,
        },
    )?
    .register(connection)
}
pub fn capacity(ctx: &Context) -> duckdb_neo::Result<usize> {
    DataChunk::create(ctx, [i32::logical_type(ctx)?], false)?.capacity()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn array_length_parameter_contract_probe() {
        let connection = crate::test_support::connection();
        let native = LogicalType::from_text(&connection, "BIGINT[3]").unwrap();
        let (_, length) = native.get_param(1).unwrap();
        eprintln!(
            "ARRAY length type={}, signed={:?}, unsigned={:?}",
            length.logical_type().unwrap().to_text().unwrap(),
            length.get::<i64>(),
            length.get::<u64>()
        );
        assert_eq!(length.get::<i64>().unwrap(), Some(3));
        assert_eq!(
            LogicalTypeHandle::from_neo(&native).unwrap().array_size(),
            3
        );
    }
    #[test]
    fn aliases_keep_base_storage_without_catalog_type_lookup_at_any_depth() {
        let connection = crate::test_support::connection();
        let mut alias = LogicalTypeHandle::from(LogicalTypeId::Varchar);
        alias.set_alias("__spanner_unregistered_label");
        assert!(LogicalType::from_text(&connection, "__spanner_unregistered_label").is_err());
        let leaf = alias.build(&connection).unwrap();
        assert_eq!(leaf.id().unwrap(), LogicalTypeID::Varchar);
        assert_eq!(leaf.to_text().unwrap(), "__spanner_unregistered_label");
        let array = LogicalTypeHandle {
            size: 2,
            children: vec![(String::new(), alias.clone())],
            ..LogicalTypeHandle::from(LogicalTypeId::Array)
        };
        let structure = LogicalTypeHandle::struct_type(&[
            ("list", LogicalTypeHandle::list(&alias)),
            ("array", array),
        ]);
        let native = structure.build(&connection).unwrap();
        for index in 0..2 {
            let (_, child) = native.get_param(index).unwrap();
            let ty = child.get::<LogicalType>().unwrap().unwrap();
            let (_, leaf) = ty.get_param(0).unwrap();
            let leaf = leaf.get::<LogicalType>().unwrap().unwrap();
            assert_eq!(leaf.id().unwrap(), LogicalTypeID::Varchar);
            assert_eq!(leaf.to_text().unwrap(), "__spanner_unregistered_label");
        }
    }
    #[test]
    fn struct_factory_keeps_empty_and_unnamed_fields() {
        let connection = crate::test_support::connection();
        let empty = LogicalTypeHandle::struct_type(&[])
            .build(&connection)
            .unwrap();
        assert_eq!(empty.id().unwrap(), LogicalTypeID::Struct);
        assert_eq!(empty.param_count().unwrap(), 0);
        let unnamed = LogicalTypeHandle::struct_type(&[
            ("", LogicalTypeId::Bigint.into()),
            ("", LogicalTypeId::Varchar.into()),
        ])
        .build(&connection)
        .unwrap();
        assert_eq!(unnamed.param_count().unwrap(), 2);
        for index in 0..2 {
            assert_eq!(unnamed.get_param(index).unwrap().0, "");
        }
        let ordered = LogicalTypeHandle::struct_type(&[
            ("z big", LogicalTypeId::Bigint.into()),
            ("a\"small", LogicalTypeId::Varchar.into()),
        ])
        .build(&connection)
        .unwrap();
        assert_eq!(ordered.get_param(0).unwrap().0, "z big");
        assert_eq!(ordered.get_param(1).unwrap().0, "a\"small");
    }
}
