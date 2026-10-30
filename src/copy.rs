//! Native v2 COPY: bounded Spanner mutation windows committed only from flush.
use crate::{
    client, config,
    connection::{ConnectionProfile, EndpointMode},
    duck::{LogicalTypeHandle, LogicalTypeId},
    runtime, schema,
};
use duckdb_neo::{
    Result as NeoResult,
    column_data_collection::ColumnDataCollection,
    connection::Context,
    copy_function::{CopyFunctionBuilder, CopyToBindInfo, CopyToFunctionCallbacks},
    extension::Extension,
    value::Value as NeoValue,
};
use google_cloud_gax::{error::rpc::Code, retry_policy::RetryPolicyExt};
use google_cloud_spanner::{
    Error as SpannerClientError,
    client::DatabaseClient,
    mutation::Mutation,
    retry_policy::SpannerRetryPolicy,
    transaction::{BasicTransactionRetryPolicy, WriteOnlyTransaction},
    types::{Type, TypeCode},
};
use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;
use prost_types::{ListValue, Value, value::Kind};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
const DEFAULT_BATCH_SIZE: usize = 1000;
const MAX_BATCH_SIZE: usize = 80_000;
const EXPLICIT_EMULATOR_WRITE_MAX_ATTEMPTS: u32 = 5;
const WRITE_TRANSACTION_MAX_ATTEMPTS: u32 = 5;
const WRITE_TRANSACTION_TOTAL_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_BEGIN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_COMMIT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_RPC_RETRY_MAX_ATTEMPTS: u32 = 3;
const WRITE_RPC_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const COPY_OPTION_NAMES: &[&str] = &[
    "database_path",
    "project",
    "instance",
    "database",
    "endpoint",
    "endpoint_mode",
    "dialect",
    "mode",
    "mutation_batch_size",
    "columns",
];

#[derive(Clone, Copy)]
enum MutationMode {
    Insert,
    Update,
    InsertOrUpdate,
    Replace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EmulatorRetryRoute {
    None,
    Sdk,
    ExplicitEndpointFallback,
}

impl MutationMode {
    fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "insert" => Ok(Self::Insert),
            "update" => Ok(Self::Update),
            "insert_or_update" => Ok(Self::InsertOrUpdate),
            "replace" => Ok(Self::Replace),
            _ => Err(format!(
                "Invalid mode '{s}': must be 'insert', 'update', 'insert_or_update', or 'replace'"
            )),
        }
    }
}

#[derive(Clone)]
struct ColumnMeta {
    ty: LogicalTypeHandle,
    target: Type,
}
struct CopyBindData {
    profile: ConnectionProfile,
    dialect: DatabaseDialect,
    mode: MutationMode,
    batch_size: usize,
    target_columns: Option<Vec<String>>,
    columns: Vec<LogicalTypeHandle>,
}
struct Copy;
fn guard<T>(phase: &str, f: impl FnOnce() -> Result<T, String>) -> NeoResult<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
        .unwrap_or_else(|_| Err(format!("COPY {phase} callback panicked")))
        .map_err(|message| duckdb_neo::Error::invalid_input(message.replace('\0', "\\0")))
}
impl CopyToFunctionCallbacks for Copy {
    type BindData = CopyBindData;
    type InitData = Mutex<CopyGlobalState>;
    type BatchData = ();
    fn bind(&self, ctx: &Context, info: &CopyToBindInfo) -> NeoResult<CopyBindData> {
        guard("bind", || {
            let mut opts = HashMap::new();
            for (key, value) in info.options().map_err(|e| e.to_string())? {
                let id = value
                    .logical_type()
                    .and_then(|t| t.id())
                    .map_err(|e| e.to_string())?;
                let mut values = Vec::new();
                if !value.is_null().map_err(|e| e.to_string())? {
                    if matches!(
                        id,
                        duckdb_neo::logical_type::LogicalTypeID::List
                            | duckdb_neo::logical_type::LogicalTypeID::Tuple
                    ) {
                        for child in value.children().map_err(|e| e.to_string())? {
                            values.push(child.dbg_string().map_err(|e| e.to_string())?);
                        }
                    } else {
                        values.push(value.dbg_string().map_err(|e| e.to_string())?);
                    }
                }
                if opts.insert(key.to_ascii_lowercase(), values).is_some() {
                    return Err(format!("duplicate COPY option {key}"));
                }
            }
            validate_copy_option_names(&opts)?;
            let settings = config::Settings::read(ctx).map_err(|e| e.to_string())?;
            let cfg = |name: &str| settings.string(name);
            let path = resolve_copy_database_path(&opts, cfg)?;
            let endpoint = option_value(&opts, "endpoint")
                .map(str::to_owned)
                .or_else(|| cfg("spanner_endpoint"));
            let mode = option_value(&opts, "endpoint_mode")
                .map(str::to_owned)
                .or_else(|| cfg("spanner_endpoint_mode"));
            let env = std::env::var("SPANNER_EMULATOR_HOST").ok();
            let profile =
                ConnectionProfile::resolve(path, endpoint, mode.as_deref(), None, env.as_deref())
                    .and_then(|p| p.with_tls_settings(cfg))
                    .map_err(|e| e.to_string())?;
            let dialect = option_value(&opts, "dialect")
                .map(schema::parse_dialect)
                .transpose()
                .map_err(|e| e.to_string())?
                .unwrap_or(DatabaseDialect::Unspecified);
            let mode = option_value(&opts, "mode")
                .map(MutationMode::parse)
                .transpose()?
                .unwrap_or(MutationMode::InsertOrUpdate);
            let batch_size = parse_batch_size(option_value(&opts, "mutation_batch_size"))?;
            let mut columns = Vec::new();
            for i in 0..info.column_count().map_err(|e| e.to_string())? {
                let (_, ty) = info.get_column(i).map_err(|e| e.to_string())?;
                let ty = LogicalTypeHandle::from_neo(&ty).map_err(|e| e.to_string())?;
                validate_source(&ty)?;
                columns.push(ty);
            }
            Ok(CopyBindData {
                profile,
                dialect,
                mode,
                batch_size,
                target_columns: opts.get("columns").cloned(),
                columns,
            })
        })
    }
    fn init(
        &self,
        _: &Context,
        bind: &CopyBindData,
        path: &str,
    ) -> NeoResult<Mutex<CopyGlobalState>> {
        guard("init", || {
            if path.is_empty() {
                return Err("Table name cannot be empty".into());
            }
            let profile = bind.profile.clone();
            let client = runtime::run_bounded(
                "Spanner COPY client setup",
                runtime::METADATA_DISCOVERY_TIMEOUT,
                async move { client::get_or_create_client(&profile).await },
            )
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
            let schema_client = Arc::clone(&client);
            let table_name = path.to_owned();
            let dialect = bind.dialect.clone();
            let profile = bind.profile.clone();
            let schema_columns = runtime::run_bounded(
                "Spanner COPY schema discovery",
                runtime::METADATA_DISCOVERY_TIMEOUT,
                async move {
                    schema::discover_table_schema_for_copy(
                        &schema_client,
                        &table_name,
                        dialect,
                        &profile,
                    )
                    .await
                },
            )
            .map_err(|e| format!("Schema discovery failed: {e}"))?
            .map_err(|e| format!("Schema discovery failed: {e}"))?;
            // Preserve positional defaults; the explicit columns option chooses by-name mapping.
            let targets = resolve_copy_columns(
                &schema_columns,
                bind.target_columns.as_deref(),
                bind.columns.len(),
                path,
            )?;
            let mut columns = Vec::new();
            for (source, target) in bind.columns.iter().zip(&targets) {
                validate_target(source, &target.spanner_type)?;
                columns.push(ColumnMeta {
                    ty: source.clone(),
                    target: target.spanner_type.clone(),
                });
            }
            let route = if bind.profile.endpoint_mode() == EndpointMode::Emulator {
                emulator_retry_route(
                    bind.profile.data_endpoint(),
                    bind.profile.sdk_emulator_mode(),
                )
            } else {
                EmulatorRetryRoute::None
            };
            Ok(Mutex::new(CopyGlobalState {
                client,
                emulator_retry_route: route,
                table_name: path.into(),
                column_names: targets.iter().map(|t| t.name.clone()).collect(),
                mode: bind.mode,
                batch_size: bind.batch_size,
                columns,
                buffer: Vec::new(),
                rows_written: 0,
                failure: None,
            }))
        })
    }
    fn batch_size(
        &self,
        _: &Context,
        bind: &CopyBindData,
    ) -> NeoResult<Option<std::num::NonZeroUsize>> {
        Ok(std::num::NonZeroUsize::new(bind.batch_size))
    }
    fn batch(
        &self,
        _: &Context,
        _: &CopyBindData,
        _: &Mutex<CopyGlobalState>,
        _: &ColumnDataCollection<'_>,
    ) -> NeoResult<()> {
        // DuckDB retains this input until flush. Do not copy an entire engine
        // batch into mutations here: engine BATCH_SIZE is only an independent hint.
        Ok(())
    }
    fn flush(
        &self,
        ctx: &Context,
        _: &CopyBindData,
        state: &Mutex<CopyGlobalState>,
        input: &ColumnDataCollection<'_>,
        _: &mut (),
    ) -> NeoResult<()> {
        let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(failure) = &state.failure {
            return Err(duckdb_neo::Error::invalid_input(failure.clone()));
        }
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<(), String> {
                let mut scan = input.to_scan(ctx).map_err(|e| e.to_string())?;
                while let Some(chunk) = scan.next_chunk().map_err(|e| e.to_string())? {
                    for row in 0..chunk.row_count().map_err(|e| e.to_string())? {
                        let mut values = Vec::with_capacity(state.columns.len());
                        for (i, column) in state.columns.iter().enumerate() {
                            let value = chunk
                                .get_vector_at::<duckdb_neo::vector::Unknown>(i)
                                .and_then(|v| v.get_value_slow(row))
                                .map_err(|e| e.to_string())?;
                            values.push(to_wire(&value, &column.ty, &column.target)?);
                        }
                        let mutation = build_mutation(
                            state.mode,
                            &state.table_name,
                            &state.column_names,
                            values,
                        );
                        state.buffer.push(mutation);
                        if state.buffer.len() >= state.batch_size {
                            flush_buffer(&mut state)?;
                        }
                    }
                }
                // Even the short final window belongs to flush, not finalize.
                flush_buffer(&mut state)
            }))
            .unwrap_or_else(|_| Err("callback panicked".into()));
        result.map_err(|cause| {
            duckdb_neo::Error::invalid_input(record_copy_failure(&mut state, "flush", &cause))
        })
    }
    fn finalize(
        &self,
        _: &Context,
        _: &CopyBindData,
        state: &Mutex<CopyGlobalState>,
    ) -> NeoResult<()> {
        guard("finalize", || {
            let state = state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(failure) = &state.failure {
                return Err(failure.clone());
            }
            if !state.buffer.is_empty() {
                return Err("COPY finalize found unflushed mutations; refusing to commit".into());
            }
            eprintln!(
                "[duckdb-spanner] COPY TO '{}': {} rows written",
                state.table_name, state.rows_written
            );
            Ok(())
        })
    }
}
pub(crate) fn register(ext: &Extension) -> NeoResult<()> {
    CopyFunctionBuilder::new("spanner", Copy).register(ext)
}
fn validate_source(ty: &LogicalTypeHandle) -> Result<(), String> {
    use LogicalTypeId::*;
    match ty.id() {
        Interval => {
            return Err("DuckDB INTERVAL source values are not supported by COPY TO Spanner: Spanner INTERVAL is query-only; use interval_to_iso8601(column) and copy VARCHAR to STRING".into());
        }
        Map | Any | Other | Bit | Time | TimeNs | TimestampTZNs => {
            return Err(format!(
                "Unsupported DuckDB type {:?} for COPY TO spanner",
                ty.id()
            ));
        }
        _ => {}
    }
    for i in 0..ty.num_children() {
        validate_source(&ty.child(i))?;
    }
    Ok(())
}
fn validate_target(source: &LogicalTypeHandle, target: &Type) -> Result<(), String> {
    use LogicalTypeId::*;
    if matches!(source.id(), Float | Double) && target.code() == TypeCode::Numeric {
        return Err("DuckDB FLOAT/DOUBLE source values cannot be copied to a Spanner NUMERIC target exactly; explicitly cast to DECIMAL".into());
    }
    if matches!(source.id(), List | Array) {
        if target.code() != TypeCode::Array {
            return Err("DuckDB LIST/ARRAY source column requires a Spanner ARRAY target".into());
        }
        let element = target
            .array_element_type()
            .ok_or("Spanner ARRAY target missing element metadata")?;
        validate_target(&source.child(0), &element)?;
    }
    if source.id() == Struct && target.code() != TypeCode::Json {
        return Err("DuckDB STRUCT source column requires a Spanner JSON target".into());
    }
    Ok(())
}
fn to_wire(value: &NeoValue, ty: &LogicalTypeHandle, target: &Type) -> Result<Value, String> {
    if value.is_null().map_err(|e| e.to_string())? {
        return Ok(Value {
            kind: Some(Kind::NullValue(0)),
        });
    }
    use LogicalTypeId::*;
    if matches!(ty.id(), List | Array) {
        let target = target
            .array_element_type()
            .ok_or("missing ARRAY target metadata")?;
        let n = value.child_count().map_err(|e| e.to_string())?;
        if n > 4_194_304 {
            return Err("COPY array exceeds64MiB entry budget".into());
        }
        let values = (0..n)
            .map(|i| {
                to_wire(
                    &value.get_child(i).map_err(|e| e.to_string())?,
                    &ty.child(0),
                    &target,
                )
            })
            .collect::<Result<_, _>>()?;
        return Ok(Value {
            kind: Some(Kind::ListValue(ListValue { values })),
        });
    }
    if ty.id() == Struct {
        let json = crate::scalars::structural_json(value, ty).map_err(|e| e.to_string())?;
        return Ok(Value {
            kind: Some(Kind::StringValue(
                serde_json::to_string(&json).map_err(|e| e.to_string())?,
            )),
        });
    }
    let json = crate::scalars::copy_scalar_json(value, ty).map_err(|e| e.to_string())?;
    let kind = match json {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(v) => Kind::BoolValue(v),
        serde_json::Value::Number(v) if matches!(ty.id(), Float | Double) => {
            Kind::NumberValue(v.as_f64().ok_or("invalid float")?)
        }
        serde_json::Value::Number(v) => Kind::StringValue(v.to_string()),
        serde_json::Value::String(v)
            if ty.id() == Decimal
                && matches!(target.code(), TypeCode::Float32 | TypeCode::Float64) =>
        {
            Kind::NumberValue(
                v.parse()
                    .map_err(|e| format!("DECIMAL to FLOAT64 conversion failed: {e}"))?,
            )
        }
        serde_json::Value::String(v) => Kind::StringValue(v),
        _ => return Err("unexpected composite scalar wire value".into()),
    };
    Ok(Value { kind: Some(kind) })
}
struct CopyGlobalState {
    client: Arc<DatabaseClient>,
    emulator_retry_route: EmulatorRetryRoute,
    table_name: String,
    column_names: Vec<String>,
    mode: MutationMode,
    batch_size: usize,
    columns: Vec<ColumnMeta>,
    buffer: Vec<Mutation>,
    rows_written: u64,
    failure: Option<String>,
}

#[derive(Clone, Copy)]
struct CopyBatchContext {
    confirmed_rows: u64,
    batch_rows: usize,
}

impl CopyBatchContext {
    fn append_to(self, message: &mut String) {
        let first_row = self.confirmed_rows.saturating_add(1);
        let last_row = self.confirmed_rows.saturating_add(self.batch_rows as u64);
        message.push_str(&format!(
            "; batch_rows={}; batch_row_range={first_row}..={last_row}; \
             confirmed_rows_before_batch={}",
            self.batch_rows, self.confirmed_rows
        ));
    }
}

fn copy_failure_message(phase: &str, cause: &str, rows_written: u64) -> String {
    let cause = cause.replace('\0', "\\0");
    format!(
        "COPY {phase} failed: {cause}; rows_written={rows_written}; earlier batches remain committed"
    )
}

fn record_copy_failure(state: &mut CopyGlobalState, phase: &str, cause: &str) -> String {
    if let Some(failure) = &state.failure {
        return failure.clone();
    }

    // A failed COPY must not let finalize commit rows that were only buffered.
    state.buffer.clear();
    let failure = copy_failure_message(phase, cause, state.rows_written);
    state.failure = Some(failure.clone());
    failure
}

fn resolve_copy_columns(
    schema_columns: &[schema::ColumnInfo],
    source_names: Option<&[String]>,
    source_count: usize,
    table: &str,
) -> Result<Vec<schema::ColumnInfo>, String> {
    let writable: Vec<&schema::ColumnInfo> =
        schema_columns.iter().filter(|c| !c.is_generated).collect();
    let generated_count = schema_columns.len() - writable.len();

    if let Some(column) = writable
        .iter()
        .find(|column| column.spanner_type.code() == TypeCode::Unspecified)
    {
        return Err(format!(
            "Writable column '{}' in Spanner table '{table}' has an unsupported type",
            column.name
        ));
    }

    // By-name mapping requires a name for every source column.
    if let Some(names) = source_names {
        if names.len() != source_count {
            return Err(format!(
                "Column count mismatch: COPY option columns has {} name(s), but source has \
                 {source_count} column(s)",
                names.len()
            ));
        }
        if names.iter().any(|n| n.is_empty()) {
            return Err("COPY option columns must not contain empty column names".to_string());
        }
        let mut seen_names = std::collections::HashMap::new();
        for name in names {
            let normalized = name.to_ascii_lowercase();
            if let Some(first_name) = seen_names.insert(normalized, name) {
                return Err(format!(
                    "COPY source column '{name}' duplicates source column '{first_name}'. \
                     Source column names must be unique for by-name mapping"
                ));
            }
        }
        let mut resolved = Vec::with_capacity(names.len());
        for name in names {
            // Match against ALL target columns (including generated ones) so a
            // source column pointing at a generated column yields a precise
            // error instead of a generic "no match".
            match schema_columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
            {
                Some(c) if c.is_generated => {
                    return Err(format!(
                        "COPY source column '{name}' maps to generated column '{}' in \
                         Spanner table '{table}', which cannot be written",
                        c.name
                    ));
                }
                Some(c) => resolved.push(c.clone()),
                None => {
                    return Err(format!(
                        "COPY source column '{name}' has no matching column in Spanner \
                         table '{table}'. Available target columns: {}",
                        join_column_names(&writable)
                    ));
                }
            }
        }
        return Ok(resolved);
    }

    // Positional fallback over the writable (non-generated) target columns.
    if source_count != writable.len() {
        let gen_note = if generated_count > 0 {
            format!(" ({generated_count} generated column(s) excluded)")
        } else {
            String::new()
        };
        return Err(format!(
            "Column count mismatch: source has {source_count} column(s), but Spanner table \
             '{table}' has {} writable column(s){gen_note}",
            writable.len()
        ));
    }

    Ok(writable.into_iter().cloned().collect())
}

fn join_column_names(columns: &[&schema::ColumnInfo]) -> String {
    columns
        .iter()
        .map(|c| c.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn option_value<'a>(
    opts: &'a std::collections::HashMap<String, Vec<String>>,
    name: &str,
) -> Option<&'a str> {
    opts.get(name)?.first().map(String::as_str)
}

fn validate_copy_option_names(
    opts: &std::collections::HashMap<String, Vec<String>>,
) -> Result<(), String> {
    let mut unknown: Vec<&str> = opts
        .keys()
        .filter_map(|name| (!COPY_OPTION_NAMES.contains(&name.as_str())).then_some(name.as_str()))
        .collect();
    unknown.sort_unstable();

    if unknown.is_empty() {
        return Ok(());
    }

    Err(format!(
        "Unknown COPY FORMAT spanner option(s): {}. Recognized options: {}",
        unknown.join(", "),
        COPY_OPTION_NAMES.join(", ")
    ))
}

fn parse_batch_size(value: Option<&str>) -> Result<usize, String> {
    let batch_size = match value {
        Some(value) => value
            .parse::<usize>()
            .map_err(|e| format!("Invalid mutation_batch_size '{value}': {e}"))?,
        None => DEFAULT_BATCH_SIZE,
    };
    validate_batch_size(batch_size)?;
    Ok(batch_size)
}

fn validate_batch_size(batch_size: usize) -> Result<(), String> {
    if batch_size == 0 {
        return Err("Invalid mutation_batch_size '0': must be greater than zero".to_string());
    }
    if batch_size > MAX_BATCH_SIZE {
        return Err(format!(
            "Invalid mutation_batch_size '{batch_size}': must not exceed {MAX_BATCH_SIZE} rows; use a lower value for multi-column or indexed tables"
        ));
    }
    Ok(())
}

fn resolve_copy_database_path(
    opts: &std::collections::HashMap<String, Vec<String>>,
    cfg: impl Fn(&str) -> Option<String>,
) -> Result<String, String> {
    let project_opt = option_value(opts, "project").map(ToOwned::to_owned);
    let instance_opt = option_value(opts, "instance").map(ToOwned::to_owned);
    let database_opt = option_value(opts, "database").map(ToOwned::to_owned);
    let database_path_opt = option_value(opts, "database_path").map(ToOwned::to_owned);

    if database_path_opt.is_some()
        && (project_opt.is_some() || instance_opt.is_some() || database_opt.is_some())
    {
        return Err(
            "cannot combine database_path with project, instance, or database COPY options"
                .to_string(),
        );
    }

    if project_opt.is_some() || instance_opt.is_some() || database_opt.is_some() {
        let project = project_opt.or_else(|| cfg("spanner_project"));
        let instance = instance_opt.or_else(|| cfg("spanner_instance"));
        let database = database_opt.or_else(|| cfg("spanner_database"));
        let p = project.ok_or(
            "project is required when instance or database is specified. \
             Use project option or SET spanner_project = '...'",
        )?;
        let i = instance.ok_or(
            "instance is required when project or database is specified. \
             Use instance option or SET spanner_instance = '...'",
        )?;
        let d = database.ok_or(
            "database is required when project or instance is specified. \
             Use database option or SET spanner_database = '...'",
        )?;
        return Ok(format!("projects/{p}/instances/{i}/databases/{d}"));
    }

    if let Some(path) = database_path_opt {
        return Ok(path);
    }

    let project = cfg("spanner_project");
    let instance = cfg("spanner_instance");
    let database = cfg("spanner_database");

    if project.is_some() || instance.is_some() || database.is_some() {
        let p = project.ok_or(
            "project is required when instance or database is specified. \
             Use project option or SET spanner_project = '...'",
        )?;
        let i = instance.ok_or(
            "instance is required when project or database is specified. \
             Use instance option or SET spanner_instance = '...'",
        )?;
        let d = database.ok_or(
            "database is required when project or instance is specified. \
             Use database option or SET spanner_database = '...'",
        )?;
        return Ok(format!("projects/{p}/instances/{i}/databases/{d}"));
    }

    if let Some(path) = cfg("spanner_database_path") {
        return Ok(path);
    }

    Err(
        "No database specified. Use database_path, project/instance/database options, \
         or SET spanner_database_path"
            .to_string(),
    )
}

fn emulator_retry_route(endpoint: Option<&str>, sdk_emulator_mode: bool) -> EmulatorRetryRoute {
    // The pinned SDK marks only SPANNER_EMULATOR_HOST clients as emulator;
    // explicit endpoints need the compatibility retry below.
    if sdk_emulator_mode {
        EmulatorRetryRoute::Sdk
    } else if endpoint.is_some() {
        EmulatorRetryRoute::ExplicitEndpointFallback
    } else {
        EmulatorRetryRoute::None
    }
}

fn flush_buffer(state: &mut CopyGlobalState) -> Result<(), String> {
    if state.buffer.is_empty() {
        return Ok(());
    }

    let mutations = std::mem::take(&mut state.buffer);
    let count = mutations.len();
    let context = CopyBatchContext {
        confirmed_rows: state.rows_written,
        batch_rows: count,
    };

    runtime::run(write_mutations(
        Arc::clone(&state.client),
        mutations,
        state.emulator_retry_route,
        context,
    ))
    .map_err(|e| {
        let mut message =
            format!("Runtime error: {e}; the final batch's write outcome may be unknown");
        context.append_to(&mut message);
        message
    })??;

    state.rows_written += count as u64;
    Ok(())
}

async fn write_mutations(
    client: Arc<DatabaseClient>,
    mutations: Vec<Mutation>,
    emulator_retry_route: EmulatorRetryRoute,
    context: CopyBatchContext,
) -> Result<(), String> {
    let mut attempt = 1;
    loop {
        match build_copy_write_transaction(&client)
            .write(mutations.clone())
            .await
        {
            Ok(_) => return Ok(()),
            Err(e)
                if emulator_retry_route == EmulatorRetryRoute::ExplicitEndpointFallback
                    && attempt < EXPLICIT_EMULATOR_WRITE_MAX_ATTEMPTS
                    && is_internal_emulator_schema_error(&e) =>
            {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(format_spanner_write_error(&e, context)),
        }
    }
}

fn copy_transaction_retry_policy() -> BasicTransactionRetryPolicy {
    BasicTransactionRetryPolicy::new()
        .with_max_attempts(WRITE_TRANSACTION_MAX_ATTEMPTS)
        .with_total_timeout(WRITE_TRANSACTION_TOTAL_TIMEOUT)
}

fn build_copy_write_transaction(client: &DatabaseClient) -> WriteOnlyTransaction {
    client
        .write_only_transaction()
        .with_retry_policy(copy_transaction_retry_policy())
        .with_begin_attempt_timeout(WRITE_BEGIN_ATTEMPT_TIMEOUT)
        .with_begin_retry_policy(
            SpannerRetryPolicy::new()
                .with_time_limit(WRITE_RPC_RETRY_TIMEOUT)
                .with_attempt_limit(WRITE_RPC_RETRY_MAX_ATTEMPTS),
        )
        .with_commit_attempt_timeout(WRITE_COMMIT_ATTEMPT_TIMEOUT)
        // The SDK's write path marks Commit idempotent and documents replay
        // protection. Keep those RPC retries bounded; after they are exhausted,
        // the transaction layer retries only explicit ABORTED results.
        .with_commit_retry_policy(
            SpannerRetryPolicy::new()
                .with_time_limit(WRITE_RPC_RETRY_TIMEOUT)
                .with_attempt_limit(WRITE_RPC_RETRY_MAX_ATTEMPTS),
        )
        .build()
}

fn format_spanner_write_error(err: &SpannerClientError, context: CopyBatchContext) -> String {
    let mut message = format!("Spanner write error: {err}");
    if is_ambiguous_write_error(err) {
        message.push_str("; the final batch's write outcome is unknown");
    }
    context.append_to(&mut message);
    message
}

fn is_ambiguous_write_error(err: &SpannerClientError) -> bool {
    if is_internal_emulator_schema_error(err) {
        return false;
    }

    err.is_timeout()
        || err.is_exhausted()
        || err.is_deserialization()
        || err.is_transport()
        || err.is_io()
        || err.status().is_some_and(|status| {
            matches!(
                status.code,
                Code::Cancelled
                    | Code::DeadlineExceeded
                    | Code::Unknown
                    | Code::Internal
                    | Code::Unavailable
                    | Code::DataLoss
            )
        })
}

fn is_internal_emulator_schema_error(err: &SpannerClientError) -> bool {
    err.status().is_some_and(|status| {
        status.code == Code::Internal
            && status.message.contains("Schema generation")
            && status
                .message
                .contains("was not registered with the Action Manager")
    })
}

fn build_mutation(
    mode: MutationMode,
    table: &str,
    columns: &[String],
    values: Vec<Value>,
) -> Mutation {
    let mut builder = match mode {
        MutationMode::Insert => Mutation::new_insert_builder(table),
        MutationMode::Update => Mutation::new_update_builder(table),
        MutationMode::InsertOrUpdate => Mutation::new_insert_or_update_builder(table),
        MutationMode::Replace => Mutation::new_replace_builder(table),
    };

    for (column, value) in columns.iter().zip(values.iter()) {
        builder = builder.set(column.as_str()).to(value);
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_cloud_gax::error::rpc::Status;
    use google_cloud_gax::options::RequestOptions;
    use google_cloud_gax::response::Response;
    use google_cloud_spanner::client::Spanner;
    use google_cloud_spanner::model as spanner_model;
    use google_cloud_spanner::stub::Spanner as SpannerStub;
    use google_cloud_spanner::types as spanner_types;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const DIRECT_STUB_TEST_TIMEOUT: Duration = Duration::from_secs(5);
    const FAKE_DATABASE: &str = "projects/p/instances/i/databases/d";
    const FAKE_SESSION: &str = "projects/p/instances/i/databases/d/sessions/copy-test";

    #[derive(Clone, Copy, Debug)]
    enum CommitBehavior {
        AbortedThenSuccess,
        Unavailable,
    }

    #[derive(Debug, Default)]
    struct StubCallCounts {
        create_session: AtomicUsize,
        begin_transaction: AtomicUsize,
        commit: AtomicUsize,
    }

    #[derive(Clone, Debug)]
    struct FakeSpannerStub {
        behavior: CommitBehavior,
        calls: Arc<StubCallCounts>,
    }

    impl FakeSpannerStub {
        fn new(behavior: CommitBehavior) -> Self {
            Self {
                behavior,
                calls: Arc::new(StubCallCounts::default()),
            }
        }

        fn assert_rpc_options(options: &RequestOptions, expected_timeout: Duration) {
            assert_eq!(options.idempotent(), Some(true));
            assert_eq!(options.attempt_timeout(), &Some(expected_timeout));
            assert!(options.retry_policy().is_some());
        }
    }

    impl SpannerStub for FakeSpannerStub {
        fn create_session(
            &self,
            req: spanner_model::CreateSessionRequest,
            options: RequestOptions,
        ) -> impl Future<Output = google_cloud_spanner::Result<Response<spanner_model::Session>>> + Send
        {
            let calls = Arc::clone(&self.calls);
            async move {
                assert_eq!(req.database, FAKE_DATABASE);
                assert_eq!(options.idempotent(), Some(true));
                assert!(options.retry_policy().is_some());
                assert_eq!(
                    calls.create_session.fetch_add(1, Ordering::SeqCst),
                    0,
                    "database client must create exactly one session"
                );
                Ok(Response::from(
                    spanner_model::Session::new().set_name(FAKE_SESSION),
                ))
            }
        }

        fn begin_transaction(
            &self,
            req: spanner_model::BeginTransactionRequest,
            options: RequestOptions,
        ) -> impl Future<
            Output = google_cloud_spanner::Result<Response<spanner_model::Transaction>>,
        > + Send {
            let calls = Arc::clone(&self.calls);
            async move {
                assert_eq!(req.session, FAKE_SESSION);
                Self::assert_rpc_options(&options, WRITE_BEGIN_ATTEMPT_TIMEOUT);
                let attempt = calls.begin_transaction.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(Response::from(
                    spanner_model::Transaction::new().set_id(vec![attempt as u8]),
                ))
            }
        }

        fn commit(
            &self,
            req: spanner_model::CommitRequest,
            options: RequestOptions,
        ) -> impl Future<
            Output = google_cloud_spanner::Result<Response<spanner_model::CommitResponse>>,
        > + Send {
            let behavior = self.behavior;
            let calls = Arc::clone(&self.calls);
            async move {
                assert_eq!(req.session, FAKE_SESSION);
                Self::assert_rpc_options(&options, WRITE_COMMIT_ATTEMPT_TIMEOUT);
                let attempt = calls.commit.fetch_add(1, Ordering::SeqCst);
                match (behavior, attempt) {
                    (CommitBehavior::AbortedThenSuccess, 0) => Err(SpannerClientError::service(
                        Status::default()
                            .set_code(Code::Aborted)
                            .set_message("transaction aborted"),
                    )),
                    (CommitBehavior::AbortedThenSuccess, _) => {
                        Ok(Response::from(spanner_model::CommitResponse::new()))
                    }
                    (CommitBehavior::Unavailable, _) => Err(SpannerClientError::service(
                        Status::default()
                            .set_code(Code::Unavailable)
                            .set_message("write response unavailable"),
                    )),
                }
            }
        }
    }

    async fn fake_database_client(stub: FakeSpannerStub) -> Arc<DatabaseClient> {
        let spanner = Spanner::from_stub(stub);
        let client = spanner
            .database_client(FAKE_DATABASE)
            .build()
            .await
            .expect("build fake database client");
        Arc::new(client)
    }

    fn fake_mutations(count: usize) -> Vec<Mutation> {
        (0..count)
            .map(|id| {
                Mutation::new_insert_or_update_builder("CopyTarget")
                    .set("Id")
                    .to(id as i64)
                    .build()
            })
            .collect()
    }

    #[test]
    fn copy_callback_error_message_sanitizes_nul() {
        let message = guard("bind", || {
            Err::<(), String>("bind failed\0with context".into())
        })
        .unwrap_err()
        .to_string();
        assert!(message.contains("bind failed\\0with context"), "{message}");
        let failure = copy_failure_message("flush", "commit failed\0with context", 1);
        assert!(
            failure.contains("commit failed\\0with context"),
            "{failure}"
        );
        assert!(!failure.contains('\0'));
    }

    fn col(name: &str, is_generated: bool) -> schema::ColumnInfo {
        schema::ColumnInfo {
            name: name.to_string(),
            spanner_type: spanner_types::int64(),
            is_generated,
        }
    }

    fn unspecified_col(name: &str, is_generated: bool) -> schema::ColumnInfo {
        schema::ColumnInfo {
            name: name.to_string(),
            spanner_type: Type::default(),
            is_generated,
        }
    }

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn options(values: &[(&str, &str)]) -> std::collections::HashMap<String, Vec<String>> {
        values
            .iter()
            .map(|(key, value)| ((*key).to_string(), vec![(*value).to_string()]))
            .collect()
    }

    #[test]
    fn positional_excludes_generated_columns() {
        // DDL order: Id, FullName (generated), Name. Writable = Id, Name.
        let schema = vec![col("Id", false), col("FullName", true), col("Name", false)];
        let resolved = resolve_copy_columns(&schema, None, 2, "T").unwrap();
        let got: Vec<&str> = resolved.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(got, vec!["Id", "Name"]);
        assert!(
            resolved
                .iter()
                .all(|column| column.spanner_type.code() != TypeCode::Unspecified)
        );
    }

    #[test]
    fn positional_excludes_generated_unspecified_placeholder() {
        let schema = vec![
            col("Id", false),
            unspecified_col("SearchTokens", true),
            col("Name", false),
        ];
        let resolved = resolve_copy_columns(&schema, None, 2, "T").unwrap();

        assert_eq!(
            resolved
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Id", "Name"]
        );
        assert!(
            resolved
                .iter()
                .all(|column| column.spanner_type.code() != TypeCode::Unspecified)
        );
    }

    #[test]
    fn rejects_writable_unspecified_placeholder() {
        let schema = vec![col("Id", false), unspecified_col("SearchTokens", false)];
        let err = resolve_copy_columns(&schema, None, 2, "T").unwrap_err();

        assert!(err.contains("Writable column 'SearchTokens'"), "{err}");
        assert!(err.contains("unsupported type"), "{err}");
    }

    #[test]
    fn positional_arity_mismatch_mentions_generated() {
        let schema = vec![col("Id", false), col("Gen", true), col("Name", false)];
        // Source provides 3 columns but only 2 are writable.
        let err = resolve_copy_columns(&schema, None, 3, "T").unwrap_err();
        assert!(err.contains("2 writable column(s)"), "{err}");
        assert!(err.contains("1 generated column(s) excluded"), "{err}");
    }

    #[test]
    fn positional_arity_mismatch_without_generated() {
        let schema = vec![col("Id", false), col("Name", false)];
        let err = resolve_copy_columns(&schema, None, 3, "T").unwrap_err();
        assert!(err.contains("2 writable column(s)"), "{err}");
        assert!(!err.contains("generated"), "{err}");
    }

    #[test]
    fn by_name_matches_case_insensitively_and_reorders() {
        // DDL order: Id, Name, Value. Source order differs and casing differs.
        let schema = vec![col("Id", false), col("Name", false), col("Value", false)];
        let src = names(&["value", "ID", "name"]);
        let resolved = resolve_copy_columns(&schema, Some(&src), 3, "T").unwrap();
        let got: Vec<&str> = resolved.iter().map(|c| c.name.as_str()).collect();
        // Result follows SOURCE order, using canonical target names.
        assert_eq!(got, vec!["Value", "Id", "Name"]);
    }

    #[test]
    fn explicit_columns_count_must_match_source_count() {
        let schema = vec![col("Id", false), col("Name", false)];
        let columns = names(&["Name"]);
        let err = resolve_copy_columns(&schema, Some(&columns), 2, "T").unwrap_err();
        assert!(err.contains("Column count mismatch"), "{err}");
        assert!(err.contains("columns has 1 name(s)"), "{err}");
        assert!(err.contains("source has 2 column(s)"), "{err}");
    }

    #[test]
    fn by_name_rejects_duplicate_source_names() {
        let schema = vec![col("Id", false), col("Name", false)];
        let src = names(&["Id", "Id"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("'Id' duplicates source column 'Id'"), "{err}");
        assert!(err.contains("must be unique"), "{err}");
    }

    #[test]
    fn by_name_rejects_case_insensitive_duplicate_source_names() {
        let schema = vec![col("Id", false), col("Name", false)];
        let src = names(&["Id", "id"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("'id' duplicates source column 'Id'"), "{err}");
    }

    #[test]
    fn by_name_unmatched_source_errors_with_available_columns() {
        let schema = vec![col("Id", false), col("Name", false)];
        let src = names(&["Id", "Missing"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("'Missing' has no matching column"), "{err}");
        // Available list contains the writable target columns.
        assert!(err.contains("Id, Name"), "{err}");
    }

    #[test]
    fn by_name_source_to_generated_column_errors() {
        let schema = vec![col("Id", false), unspecified_col("Gen", true)];
        let src = names(&["Id", "gen"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("maps to generated column 'Gen'"), "{err}");
    }

    #[test]
    fn by_name_excludes_generated_from_available_list() {
        let schema = vec![col("Id", false), col("Gen", true), col("Name", false)];
        let src = names(&["Id", "Nope"]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        // Generated column must not appear in the "available target columns" hint.
        assert!(err.contains("Id, Name"), "{err}");
        assert!(!err.contains("Gen"), "{err}");
    }

    #[test]
    fn explicit_columns_rejects_empty_name() {
        let schema = vec![col("Id", false), col("Name", false)];
        let src = names(&["Id", ""]);
        let err = resolve_copy_columns(&schema, Some(&src), 2, "T").unwrap_err();
        assert!(err.contains("must not contain empty column names"), "{err}");
    }

    #[test]
    fn database_path_cannot_be_combined_with_component_options() {
        let opts = options(&[
            ("database_path", "projects/p/instances/i/databases/d"),
            ("project", "p"),
        ]);
        let err = resolve_copy_database_path(&opts, |_| None).unwrap_err();
        assert_eq!(
            err,
            "cannot combine database_path with project, instance, or database COPY options"
        );
    }

    #[test]
    fn copy_option_names_reject_unknown_names_deterministically() {
        let opts = options(&[
            ("columns", "Id"),
            ("database_path", "projects/p/instances/i/databases/d"),
            ("z_typo", "z"),
            ("a_typo", "a"),
        ]);
        let err = validate_copy_option_names(&opts).unwrap_err();
        assert_eq!(
            err,
            "Unknown COPY FORMAT spanner option(s): a_typo, z_typo. Recognized options: \
             database_path, project, instance, database, endpoint, endpoint_mode, dialect, mode, \
             mutation_batch_size, columns"
        );
    }

    #[test]
    fn copy_option_names_accept_columns_list_values() {
        let mut opts = options(&[("database_path", "projects/p/instances/i/databases/d")]);
        opts.insert("columns".to_string(), names(&["Value", "Id", "Name"]));

        validate_copy_option_names(&opts).unwrap();
        assert_eq!(opts["columns"], ["Value", "Id", "Name"]);
    }

    #[test]
    fn batch_size_must_be_positive() {
        assert_eq!(parse_batch_size(None).unwrap(), DEFAULT_BATCH_SIZE);
        assert_eq!(parse_batch_size(Some("1")).unwrap(), 1);
        assert_eq!(
            parse_batch_size(Some("0")).unwrap_err(),
            "Invalid mutation_batch_size '0': must be greater than zero"
        );
    }

    #[test]
    fn batch_size_is_capped_before_allocation() {
        assert_eq!(
            parse_batch_size(Some(&MAX_BATCH_SIZE.to_string())).unwrap(),
            MAX_BATCH_SIZE
        );
        let value = (MAX_BATCH_SIZE + 1).to_string();
        let error = parse_batch_size(Some(&value)).unwrap_err();
        assert!(error.contains("must not exceed 80000 rows"), "{error}");
    }

    #[test]
    fn partial_progress_error_is_explicit() {
        assert_eq!(
            copy_failure_message("sink", "conversion failed", 42),
            "COPY sink failed: conversion failed; rows_written=42; earlier batches remain committed"
        );
    }

    #[test]
    fn emulator_retry_route_uses_sdk_for_environment_configuration() {
        assert_eq!(emulator_retry_route(None, true), EmulatorRetryRoute::Sdk);
        assert_eq!(
            emulator_retry_route(Some("localhost:9010"), true),
            EmulatorRetryRoute::Sdk
        );
        assert_eq!(
            emulator_retry_route(Some("localhost:9010"), false),
            EmulatorRetryRoute::ExplicitEndpointFallback
        );
        assert_eq!(emulator_retry_route(None, false), EmulatorRetryRoute::None);
    }

    #[test]
    fn copy_transaction_retry_policy_is_finite() {
        let policy = copy_transaction_retry_policy();
        assert_eq!(policy.max_attempts(), WRITE_TRANSACTION_MAX_ATTEMPTS);
        assert_eq!(policy.total_timeout(), WRITE_TRANSACTION_TOTAL_TIMEOUT);
    }

    #[tokio::test]
    async fn direct_stub_aborted_write_retries_complete_transaction() {
        tokio::time::timeout(DIRECT_STUB_TEST_TIMEOUT, aborted_write_retry_scenario())
            .await
            .expect("ABORTED direct-stub test exceeded five seconds");
    }

    async fn aborted_write_retry_scenario() {
        let stub = FakeSpannerStub::new(CommitBehavior::AbortedThenSuccess);
        let calls = Arc::clone(&stub.calls);
        let client = fake_database_client(stub).await;
        let context = CopyBatchContext {
            confirmed_rows: 12,
            batch_rows: 2,
        };
        write_mutations(
            client,
            fake_mutations(context.batch_rows),
            EmulatorRetryRoute::None,
            context,
        )
        .await
        .expect("ABORTED write should retry and succeed");

        assert_eq!(calls.create_session.load(Ordering::SeqCst), 1);
        assert_eq!(calls.begin_transaction.load(Ordering::SeqCst), 2);
        assert_eq!(calls.commit.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn direct_stub_ambiguous_write_is_not_replayed_as_a_new_transaction() {
        tokio::time::timeout(DIRECT_STUB_TEST_TIMEOUT, ambiguous_write_scenario())
            .await
            .expect("ambiguous-write direct-stub test exceeded five seconds");
    }

    async fn ambiguous_write_scenario() {
        let stub = FakeSpannerStub::new(CommitBehavior::Unavailable);
        let calls = Arc::clone(&stub.calls);
        let client = fake_database_client(stub).await;
        let context = CopyBatchContext {
            confirmed_rows: 7,
            batch_rows: 3,
        };
        let error = write_mutations(
            client,
            fake_mutations(context.batch_rows),
            EmulatorRetryRoute::None,
            context,
        )
        .await
        .unwrap_err();

        // A direct stub sees one logical RPC call because transport retries are
        // executed below this boundary. It still exercises the SDK's real
        // WriteOnlyTransaction layer and proves Unavailable does not start a
        // second transaction after an ambiguous write result.
        assert_eq!(calls.create_session.load(Ordering::SeqCst), 1);
        assert_eq!(calls.begin_transaction.load(Ordering::SeqCst), 1);
        assert_eq!(calls.commit.load(Ordering::SeqCst), 1);
        assert!(error.contains("write response unavailable"), "{error}");
        assert!(error.contains("write outcome is unknown"), "{error}");
        assert!(error.contains("batch_rows=3"), "{error}");
        assert!(error.contains("batch_row_range=8..=10"), "{error}");
        assert!(error.contains("confirmed_rows_before_batch=7"), "{error}");
    }

    #[test]
    fn ambiguous_write_error_reports_unknown_batch_outcome() {
        let context = CopyBatchContext {
            confirmed_rows: 0,
            batch_rows: 1,
        };
        let err = SpannerClientError::service(
            Status::default()
                .set_code(Code::DeadlineExceeded)
                .set_message("deadline exceeded"),
        );
        assert!(is_ambiguous_write_error(&err));
        assert!(
            format_spanner_write_error(&err, context).contains("write outcome is unknown"),
            "{}",
            format_spanner_write_error(&err, context)
        );

        let err = SpannerClientError::service(
            Status::default()
                .set_code(Code::AlreadyExists)
                .set_message("already exists"),
        );
        assert!(!is_ambiguous_write_error(&err));
        assert!(!format_spanner_write_error(&err, context).contains("unknown"));

        let err = SpannerClientError::timeout(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "commit response timed out",
        ));
        assert!(is_ambiguous_write_error(&err));

        let err = SpannerClientError::transport(
            Default::default(),
            std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "commit response connection reset",
            ),
        );
        assert!(err.is_transport());
        assert!(!err.is_io());
        assert!(is_ambiguous_write_error(&err));
        assert!(format_spanner_write_error(&err, context).contains("write outcome is unknown"));
    }

    #[test]
    fn detects_emulator_internal_schema_error() {
        let err =
            SpannerClientError::service(Status::default().set_code(Code::Internal).set_message(
                "INTERNAL: Schema generation 0 was not registered with the Action Manager",
            ));
        assert!(is_internal_emulator_schema_error(&err));
        assert!(!is_ambiguous_write_error(&err));
        assert!(
            !format_spanner_write_error(
                &err,
                CopyBatchContext {
                    confirmed_rows: 0,
                    batch_rows: 1,
                }
            )
            .contains("unknown")
        );

        let err = SpannerClientError::service(
            Status::default()
                .set_code(Code::Internal)
                .set_message("different internal error"),
        );
        assert!(!is_internal_emulator_schema_error(&err));
    }
    fn descriptor(text: &str) -> LogicalTypeHandle {
        let context = crate::test_support::connection();
        let native = duckdb_neo::logical_type::LogicalType::from_text(&context, text).unwrap();
        LogicalTypeHandle::from_neo(&native).unwrap()
    }
    #[test]
    fn json_alias_is_kept_through_nested_copy_metadata() {
        let json = descriptor("JSON");
        assert_eq!(json.id(), LogicalTypeId::Varchar);
        assert_eq!(json.get_alias().as_deref(), Some("JSON"));
        assert_eq!(descriptor("VARCHAR").get_alias(), None);
        let nested = descriptor("STRUCT(obj JSON, text VARCHAR)");
        assert_eq!(nested.child(0).get_alias().as_deref(), Some("JSON"));
        assert_eq!(nested.child(1).get_alias(), None);
        assert_eq!(
            descriptor("JSON[]").child(0).get_alias().as_deref(),
            Some("JSON")
        );
        assert_eq!(descriptor("VARCHAR[2]").child(0).get_alias(), None);
    }
    #[test]
    fn interval_source_is_rejected_with_string_workaround() {
        for text in ["INTERVAL", "INTERVAL[]", "STRUCT(x INTERVAL[])"] {
            let error = validate_source(&descriptor(text)).unwrap_err();
            assert!(error.contains("INTERVAL is query-only"), "{error}");
            assert!(error.contains("interval_to_iso8601(column)"), "{error}");
        }
    }
    #[test]
    fn float_and_double_to_numeric_are_rejected_before_conversion() {
        for text in ["FLOAT", "DOUBLE"] {
            let error = validate_target(&descriptor(text), &spanner_types::numeric()).unwrap_err();
            assert!(
                error.contains("cannot be copied to a Spanner NUMERIC"),
                "{error}"
            );
            assert!(error.contains("DECIMAL"), "{error}");
            validate_target(&descriptor(text), &spanner_types::float64()).unwrap();
        }
    }
    #[test]
    fn fixed_array_and_nested_float_to_numeric_are_rejected_during_preflight() {
        let fixed = descriptor("FLOAT[3]");
        assert_eq!(fixed.id(), LogicalTypeId::Array);
        assert_eq!(fixed.array_size(), 3);
        assert_eq!(fixed.child(0).id(), LogicalTypeId::Float);
        assert!(validate_target(&fixed, &spanner_types::array(spanner_types::numeric())).is_err());
        let nested = descriptor("DOUBLE[2][]");
        assert_eq!(nested.child(0).array_size(), 2);
        assert!(
            validate_target(
                &nested,
                &spanner_types::array(spanner_types::array(spanner_types::numeric()))
            )
            .is_err()
        );
    }
    #[test]
    fn copy_timestamp_formatter_preserves_units_and_rejects_infinity() {
        use duckdb_neo::{connection::FFILink, value::ValueInput};
        let context = crate::test_support::connection();
        for (raw, expected) in [(-1, "1969-12-31T23:59:59Z"), (1, "1970-01-01T00:00:01Z")] {
            let value = context.create_value(ValueInput::TimestampSec(raw)).unwrap();
            assert_eq!(
                crate::scalars::copy_scalar_json(&value, &descriptor("TIMESTAMP_S")).unwrap(),
                serde_json::json!(expected)
            );
        }
        let value = context.create_value(ValueInput::TimestampNs(1)).unwrap();
        assert_eq!(
            crate::scalars::copy_scalar_json(&value, &descriptor("TIMESTAMP_NS")).unwrap(),
            serde_json::json!("1970-01-01T00:00:00.000000001Z")
        );
        for raw in [-i64::MAX, i64::MAX] {
            for (input, ty) in [
                (ValueInput::TimestampSec(raw), "TIMESTAMP_S"),
                (ValueInput::TimestampMs(raw), "TIMESTAMP_MS"),
                (ValueInput::Timestamp(raw), "TIMESTAMP"),
                (ValueInput::TimestampNs(raw), "TIMESTAMP_NS"),
            ] {
                let value = context.create_value(input).unwrap();
                let error = crate::scalars::copy_scalar_json(&value, &descriptor(ty)).unwrap_err();
                assert!(error.to_string().contains("infinite"), "{ty}: {error}");
            }
        }
        // Engine2 treats i64::MIN nanoseconds as finite; do not conflate it
        // with the actual negative-infinity sentinel (-i64::MAX).
        let value = context
            .create_value(ValueInput::TimestampNs(i64::MIN))
            .unwrap();
        assert!(crate::scalars::copy_scalar_json(&value, &descriptor("TIMESTAMP_NS")).is_ok());
    }
}
