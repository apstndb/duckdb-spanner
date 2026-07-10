use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab, Value};
use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_longrunning::model::{operation, Operation as InternalOperation};
use google_cloud_spanner_admin_database_v1::client::DatabaseAdmin;

use crate::error::SpannerError;
use crate::{bind_utils, runtime};

// ---------------------------------------------------------------------------
// Admin client cache
// ---------------------------------------------------------------------------

static ADMIN_CLIENT_CACHE: LazyLock<Mutex<HashMap<String, Arc<DatabaseAdmin>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Get or create a Spanner Admin client.
///
/// Admin clients are NOT per-database (unlike data clients). The cache key is
/// the endpoint string (or empty for default).
async fn get_or_create_admin_client(
    admin_endpoint: Option<&str>,
) -> Result<Arc<DatabaseAdmin>, SpannerError> {
    let cache_key = admin_endpoint.unwrap_or("").to_string();

    {
        let cache = ADMIN_CLIENT_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(client) = cache.get(&cache_key) {
            return Ok(Arc::clone(client));
        }
    }

    let mut builder = DatabaseAdmin::builder();
    let emulator_endpoint = admin_endpoint
        .map(str::to_owned)
        .or_else(|| std::env::var("SPANNER_EMULATOR_HOST").ok());
    if let Some(ep) = emulator_endpoint {
        builder = builder
            .with_endpoint(admin_endpoint_url(&ep))
            .with_credentials(Anonymous::new().build());
    }

    let client = builder.build().await?;
    let client = Arc::new(client);

    let mut cache = ADMIN_CLIENT_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let entry = cache
        .entry(cache_key)
        .or_insert_with(|| Arc::clone(&client));
    Ok(Arc::clone(entry))
}

fn emulator_endpoint(endpoint: Option<&str>) -> Option<String> {
    endpoint
        .map(str::to_owned)
        .or_else(|| std::env::var("SPANNER_EMULATOR_HOST").ok())
}

fn emulator_admin_endpoint(endpoint: Option<&str>, admin_endpoint: Option<&str>) -> Option<String> {
    admin_endpoint
        .map(admin_endpoint_url)
        .or_else(|| emulator_endpoint(endpoint).map(|ep| admin_endpoint_url(&ep)))
}

async fn update_database_ddl(
    admin: &DatabaseAdmin,
    database_path: String,
    statements: Vec<String>,
    endpoint: Option<&str>,
    admin_endpoint: Option<&str>,
) -> Result<InternalOperation, SpannerError> {
    if let Some(admin_endpoint) = emulator_admin_endpoint(endpoint, admin_endpoint) {
        return update_emulator_database_ddl(&admin_endpoint, &database_path, statements).await;
    }

    Ok(admin
        .update_database_ddl()
        .set_database(database_path)
        .set_statements(statements)
        .send()
        .await?)
}

async fn wait_database_operation(
    admin: &DatabaseAdmin,
    operation_name: &str,
    endpoint: Option<&str>,
    admin_endpoint: Option<&str>,
) -> Result<InternalOperation, SpannerError> {
    if let Some(admin_endpoint) = emulator_admin_endpoint(endpoint, admin_endpoint) {
        return wait_emulator_operation(&admin_endpoint, operation_name).await;
    }

    wait_operation(admin, operation_name).await
}

async fn update_emulator_database_ddl(
    admin_endpoint: &str,
    database_path: &str,
    statements: Vec<String>,
) -> Result<InternalOperation, SpannerError> {
    // google-cloud-rust's generated REST client currently sends system query
    // parameters that the Spanner emulator rejects for UpdateDatabaseDdl.
    let url = format!(
        "{}/v1/{}/ddl",
        admin_endpoint.trim_end_matches('/'),
        database_path
    );
    let client = reqwest::Client::new();
    let payload = serde_json::json!({ "statements": statements });
    const MAX_ATTEMPTS: u32 = 25;

    for attempt in 1..=MAX_ATTEMPTS {
        let response = client
            .patch(&url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| SpannerError::Other(format!("Failed to update emulator DDL: {e:?}")))?;
        let status = response.status();
        let body = response.text().await.map_err(|e| {
            SpannerError::Other(format!("Failed to read emulator DDL response: {e}"))
        })?;

        if status.is_success() {
            return operation_from_json(&body);
        }

        if body.contains("\"code\":9")
            && body.contains("Schema change operation rejected")
            && attempt < MAX_ATTEMPTS
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        return Err(SpannerError::Other(format!(
            "Failed to update emulator DDL: status={status}, body={body}"
        )));
    }

    Err(SpannerError::Other(
        "Failed to update emulator DDL: retry loop exhausted".to_string(),
    ))
}

async fn wait_emulator_operation(
    admin_endpoint: &str,
    operation_name: &str,
) -> Result<InternalOperation, SpannerError> {
    let url = format!(
        "{}/v1/{}",
        admin_endpoint.trim_end_matches('/'),
        operation_name
    );
    let client = reqwest::Client::new();

    loop {
        let response = client.get(&url).send().await.map_err(|e| {
            SpannerError::Other(format!("Failed to fetch emulator operation: {e:?}"))
        })?;
        let status = response.status();
        let body = response.text().await.map_err(|e| {
            SpannerError::Other(format!("Failed to read emulator operation response: {e}"))
        })?;

        if !status.is_success() {
            return Err(SpannerError::Other(format!(
                "Failed to fetch emulator operation: status={status}, body={body}"
            )));
        }

        let op = operation_from_json(&body)?;
        if op.done {
            return Ok(op);
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn operation_from_json(body: &str) -> Result<InternalOperation, SpannerError> {
    let json: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| SpannerError::Other(format!("Failed to parse emulator operation: {e}")))?;
    let name = json
        .get("name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            SpannerError::Other(format!(
                "Emulator operation response is missing a string name: {body}"
            ))
        })?;
    let done = json
        .get("done")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let mut op = InternalOperation::new().set_name(name).set_done(done);
    if let Some(error) = json.get("error") {
        let code = error
            .get("code")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or_default() as i32;
        let message = error
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Spanner DDL operation failed");
        op = op.set_error(
            google_cloud_rpc::model::Status::new()
                .set_code(code)
                .set_message(message),
        );
    }

    Ok(op)
}

fn ddl_operation_error(op: &InternalOperation) -> Option<String> {
    op.error().map(|status| {
        format!(
            "Spanner DDL operation {} failed: code={}, message={}",
            op.name, status.code, status.message
        )
    })
}

// ---------------------------------------------------------------------------
// Common named parameters for DDL / operations VTabs
// ---------------------------------------------------------------------------

fn database_named_parameters() -> Vec<(String, LogicalTypeHandle)> {
    vec![
        (
            "database_path".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "project".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "instance".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "database".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "endpoint".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "admin_endpoint".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
    ]
}

fn ddl_named_parameters() -> Vec<(String, LogicalTypeHandle)> {
    let mut params = database_named_parameters();
    params.push((
        "statements".to_string(),
        LogicalTypeHandle::list(&LogicalTypeHandle::from(LogicalTypeId::Varchar)),
    ));
    params
}

fn ddl_statements_from_bind(bind: &BindInfo) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let sql = bind.get_parameter(0);
    let statements = bind.get_named_parameter("statements");
    ddl_statements_from_values(&sql, statements.as_ref())
}

fn ddl_statements_from_values(
    sql: &Value,
    statements: Option<&Value>,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let sql_is_set = !sql.is_null();
    let statements_is_set = statements.is_some_and(|value| !value.is_null());

    match (sql_is_set, statements_is_set) {
        (true, false) => ddl_statements_from_value(sql),
        (false, true) => ddl_statements_from_value(statements.expect("checked above")),
        (true, true) => Err("specify either sql or statements, not both".into()),
        (false, false) => Err("spanner_ddl requires sql or statements".into()),
    }
}

fn ddl_statements_from_value(value: &Value) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if let Some(list) = value.to_list() {
        return ddl_statements_from_list(list);
    }
    if value.logical_type_id() != LogicalTypeId::Varchar {
        return Err(format!(
            "spanner_ddl sql must be VARCHAR or statements must be LIST<VARCHAR>, not {:?}",
            value.logical_type_id()
        )
        .into());
    }

    let statement = value.to_string();
    validate_ddl_statement(&statement)?;
    Ok(vec![statement])
}

fn ddl_statements_from_list(values: Vec<Value>) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    if values.is_empty() {
        return Err("spanner_ddl statements must contain at least one statement".into());
    }

    values
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            if value.is_null() {
                return Err(format!("spanner_ddl statements[{idx}] must not be NULL").into());
            }
            if value.logical_type_id() != LogicalTypeId::Varchar {
                return Err(format!(
                    "spanner_ddl statements[{idx}] must be VARCHAR, not {:?}",
                    value.logical_type_id()
                )
                .into());
            }
            let statement = value.to_string();
            validate_ddl_statement(&statement)?;
            Ok(statement)
        })
        .collect()
}

fn validate_ddl_statement(statement: &str) -> Result<(), Box<dyn std::error::Error>> {
    if statement.trim().is_empty() {
        return Err("spanner_ddl statements must not be empty".into());
    }
    Ok(())
}

fn cached_init_result<T>(
    cache: &Mutex<Option<Result<T, String>>>,
    run: impl FnOnce() -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>>
where
    T: Clone,
{
    let mut cached = cache.lock().unwrap_or_else(|e| e.into_inner());
    if cached.is_none() {
        *cached = Some(run().map_err(|e| e.to_string()));
    }

    match cached.as_ref().expect("cache populated above") {
        Ok(value) => Ok(value.clone()),
        Err(err) => Err(err.clone().into()),
    }
}

// ---------------------------------------------------------------------------
// SpannerDdlVTab — synchronous DDL execution
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct DdlBindData {
    database_path: String,
    statements: Vec<String>,
    endpoint: Option<String>,
    admin_endpoint: Option<String>,
    // DuckDB may call init() more than once for one bound table function.
    // Cache the full init outcome so repeated callers do not resend DDL and
    // see the same success or error from the first execution.
    cached_result: Arc<Mutex<Option<Result<DdlResult, String>>>>,
}

pub struct DdlInitData {
    result: Mutex<Option<DdlResult>>,
}

#[derive(Clone)]
struct DdlResult {
    operation_name: String,
    done: bool,
    duration_secs: f64,
}

pub struct SpannerDdlVTab;

impl VTab for SpannerDdlVTab {
    type BindData = DdlBindData;
    type InitData = DdlInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let statements = ddl_statements_from_bind(bind)?;
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);
        let admin_endpoint = bind_utils::resolve_admin_endpoint(bind);

        bind.add_result_column(
            "operation_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("done", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        bind.add_result_column(
            "duration_secs",
            LogicalTypeHandle::from(LogicalTypeId::Double),
        );

        Ok(DdlBindData {
            database_path,
            statements,
            endpoint,
            admin_endpoint,
            cached_result: Arc::new(Mutex::new(None)),
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<DdlBindData>() };

        let result = cached_init_result(&bind_data.cached_result, || {
            let database_path = bind_data.database_path.clone();
            let statements = bind_data.statements.clone();
            let endpoint = bind_data.endpoint.clone();
            let admin_endpoint = bind_data.admin_endpoint.clone();
            let effective_admin_endpoint =
                emulator_admin_endpoint(endpoint.as_deref(), admin_endpoint.as_deref());

            runtime::block_on(async {
                let admin = get_or_create_admin_client(effective_admin_endpoint.as_deref()).await?;

                let start = Instant::now();
                let op = update_database_ddl(
                    &admin,
                    database_path,
                    statements,
                    endpoint.as_deref(),
                    admin_endpoint.as_deref(),
                )
                .await?;

                let operation_name = op.name.clone();
                let op = wait_database_operation(
                    &admin,
                    &operation_name,
                    endpoint.as_deref(),
                    admin_endpoint.as_deref(),
                )
                .await?;
                if let Some(err) = ddl_operation_error(&op) {
                    return Err(SpannerError::Other(err));
                }

                let duration_secs = start.elapsed().as_secs_f64();

                Ok::<DdlResult, SpannerError>(DdlResult {
                    operation_name,
                    done: op.done,
                    duration_secs,
                })
            })?
            .map_err(Into::into)
        })?;

        init.set_max_threads(1);

        Ok(DdlInitData {
            result: Mutex::new(Some(result)),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        let result = init_data
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();

        match result {
            Some(r) => {
                let col_name = output.flat_vector(0);
                col_name.insert(0, &r.operation_name);
                let mut col_done = output.flat_vector(1);
                let mut col_duration = output.flat_vector(2);
                // SAFETY: the bind phase registers these columns as BOOLEAN and DOUBLE,
                // and index 0 is within the single-row output batch.
                unsafe {
                    col_done.as_mut_slice::<bool>()[0] = r.done;
                    col_duration.as_mut_slice::<f64>()[0] = r.duration_secs;
                }
                output.set_len(1);
            }
            None => {
                output.set_len(0);
            }
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Any), // sql TEXT or direct LIST<VARCHAR>
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(ddl_named_parameters())
    }
}

// ---------------------------------------------------------------------------
// SpannerDdlAsyncVTab — asynchronous DDL execution (returns immediately)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct DdlAsyncBindData {
    database_path: String,
    statements: Vec<String>,
    endpoint: Option<String>,
    admin_endpoint: Option<String>,
    // See the comment on `DdlBindData::cached_result`.
    cached_result: Arc<Mutex<Option<Result<DdlAsyncResult, String>>>>,
}

pub struct DdlAsyncInitData {
    result: Mutex<Option<DdlAsyncResult>>,
}

#[derive(Clone)]
struct DdlAsyncResult {
    operation_name: String,
    done: bool,
}

pub struct SpannerDdlAsyncVTab;

impl VTab for SpannerDdlAsyncVTab {
    type BindData = DdlAsyncBindData;
    type InitData = DdlAsyncInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let statements = ddl_statements_from_bind(bind)?;
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);
        let admin_endpoint = bind_utils::resolve_admin_endpoint(bind);

        bind.add_result_column(
            "operation_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("done", LogicalTypeHandle::from(LogicalTypeId::Boolean));

        Ok(DdlAsyncBindData {
            database_path,
            statements,
            endpoint,
            admin_endpoint,
            cached_result: Arc::new(Mutex::new(None)),
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<DdlAsyncBindData>() };

        let result = cached_init_result(&bind_data.cached_result, || {
            let database_path = bind_data.database_path.clone();
            let statements = bind_data.statements.clone();
            let endpoint = bind_data.endpoint.clone();
            let admin_endpoint = bind_data.admin_endpoint.clone();
            let effective_admin_endpoint =
                emulator_admin_endpoint(endpoint.as_deref(), admin_endpoint.as_deref());

            runtime::block_on(async {
                let admin = get_or_create_admin_client(effective_admin_endpoint.as_deref()).await?;

                let op = update_database_ddl(
                    &admin,
                    database_path,
                    statements,
                    endpoint.as_deref(),
                    admin_endpoint.as_deref(),
                )
                .await?;
                if let Some(err) = ddl_operation_error(&op) {
                    return Err(SpannerError::Other(err));
                }

                Ok::<DdlAsyncResult, SpannerError>(DdlAsyncResult {
                    operation_name: op.name,
                    done: op.done,
                })
            })?
            .map_err(Into::into)
        })?;

        init.set_max_threads(1);

        Ok(DdlAsyncInitData {
            result: Mutex::new(Some(result)),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        let result = init_data
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();

        match result {
            Some(r) => {
                let col_name = output.flat_vector(0);
                col_name.insert(0, &r.operation_name);
                let mut col_done = output.flat_vector(1);
                // SAFETY: the bind phase registers this column as BOOLEAN, and index 0
                // is within the single-row output batch.
                unsafe {
                    col_done.as_mut_slice::<bool>()[0] = r.done;
                }
                output.set_len(1);
            }
            None => {
                output.set_len(0);
            }
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Any), // sql TEXT or direct LIST<VARCHAR>
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(ddl_named_parameters())
    }
}

// ---------------------------------------------------------------------------
// SpannerOperationsVTab — list database long-running operations
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct OperationsBindData {
    database_path: String,
    endpoint: Option<String>,
    admin_endpoint: Option<String>,
    filter: Option<String>,
}

pub struct OperationsInitData {
    operations: Mutex<Vec<OperationRow>>,
}

struct OperationRow {
    name: String,
    done: bool,
    metadata_type: String,
    // NULL (not 0 / "") when the operation has no error, i.e. it either
    // hasn't finished yet or finished successfully.
    error_code: Option<i32>,
    error_message: Option<String>,
}

pub struct SpannerOperationsVTab;

impl VTab for SpannerOperationsVTab {
    type BindData = OperationsBindData;
    type InitData = OperationsInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);
        let admin_endpoint = bind_utils::resolve_admin_endpoint(bind);
        let filter = bind_utils::get_named_string(bind, "filter");

        bind.add_result_column("name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("done", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        bind.add_result_column(
            "metadata_type",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "error_code",
            LogicalTypeHandle::from(LogicalTypeId::Integer),
        );
        bind.add_result_column(
            "error_message",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        Ok(OperationsBindData {
            database_path,
            endpoint,
            admin_endpoint,
            filter,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<OperationsBindData>() };

        let database_path = bind_data.database_path.clone();
        let endpoint = bind_data.endpoint.clone();
        let admin_endpoint = bind_data.admin_endpoint.clone();
        let filter = bind_data.filter.clone();

        let ops = runtime::block_on(async {
            list_database_operations(
                &database_path,
                endpoint.as_deref(),
                admin_endpoint.as_deref(),
                filter.as_deref(),
            )
            .await
        })??;

        init.set_max_threads(1);

        Ok(OperationsInitData {
            operations: Mutex::new(ops),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();

        let mut ops = init_data
            .operations
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        if ops.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        // Drain at most one DuckDB data chunk per call.
        let batch_size = ops.len().min(crate::vector_size::runtime_vector_size());
        let batch: Vec<OperationRow> = ops.drain(..batch_size).collect();

        let col_name = output.flat_vector(0);
        let mut col_done = output.flat_vector(1);
        let col_metadata_type = output.flat_vector(2);
        let mut col_error_code = output.flat_vector(3);
        let mut col_error_message = output.flat_vector(4);

        // SAFETY: the bind phase registers `done` as BOOLEAN, and the loop
        // only writes indices within the drained batch size.
        let done_values = unsafe { col_done.as_mut_slice::<bool>() };
        for (i, row) in batch.iter().enumerate() {
            col_name.insert(i, &row.name);
            done_values[i] = row.done;
            col_metadata_type.insert(i, &row.metadata_type);
            if row.error_code.is_none() {
                col_error_code.set_null(i);
            }
            match &row.error_message {
                Some(msg) => col_error_message.insert(i, msg),
                None => col_error_message.set_null(i),
            }
        }
        // SAFETY: the bind phase registers `error_code` as INTEGER, and the loop
        // only writes indices within the drained batch size.
        let error_code_values = unsafe { col_error_code.as_mut_slice::<i32>() };
        for (i, row) in batch.iter().enumerate() {
            if let Some(code) = row.error_code {
                error_code_values[i] = code;
            }
        }

        output.set_len(batch.len());
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![]) // No positional parameters
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let mut params = database_named_parameters();
        params.push((
            "filter".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ));
        Some(params)
    }
}

fn operation_to_row(op: InternalOperation) -> OperationRow {
    let metadata_type = op
        .metadata
        .as_ref()
        .and_then(|m| m.type_url().map(str::to_string))
        .unwrap_or_default();

    let (error_code, error_message) = match &op.result {
        Some(operation::Result::Error(status)) => (Some(status.code), Some(status.message.clone())),
        _ => (None, None),
    };

    OperationRow {
        name: op.name,
        done: op.done,
        metadata_type,
        error_code,
        error_message,
    }
}

async fn wait_operation(
    admin: &DatabaseAdmin,
    operation_name: &str,
) -> Result<InternalOperation, SpannerError> {
    let mut op = admin
        .get_operation()
        .set_name(operation_name)
        .send()
        .await?;
    while !op.done {
        tokio::time::sleep(Duration::from_millis(500)).await;
        op = admin
            .get_operation()
            .set_name(operation_name)
            .send()
            .await?;
    }
    Ok(op)
}

async fn list_database_operations(
    database_path: &str,
    endpoint: Option<&str>,
    admin_endpoint: Option<&str>,
    filter: Option<&str>,
) -> Result<Vec<OperationRow>, SpannerError> {
    // The emulator only implements google.longrunning.Operations/ListOperations,
    // while real Spanner exposes DatabaseAdmin/ListDatabaseOperations with auth.
    let mut operations = match emulator_admin_endpoint(endpoint, admin_endpoint) {
        Some(admin_endpoint) => {
            list_emulator_database_operations(database_path, &admin_endpoint, filter).await?
        }
        None => list_real_database_operations(database_path, filter).await?,
    };

    // ListDatabaseOperations is instance-scoped, so keep the table function's
    // per-database contract by trimming unrelated operations client-side.
    let database_operation_prefix = database_operation_prefix(database_path);
    operations.retain(|op| op.name.starts_with(&database_operation_prefix));

    Ok(operations.into_iter().map(operation_to_row).collect())
}

async fn list_real_database_operations(
    database_path: &str,
    filter: Option<&str>,
) -> Result<Vec<InternalOperation>, SpannerError> {
    let admin = get_or_create_admin_client(None).await?;
    let mut items = admin
        .list_database_operations()
        .set_parent(instance_path_from_database_path(database_path)?.to_string())
        .set_filter(filter.unwrap_or_default())
        .by_item();

    let mut operations = Vec::new();
    while let Some(op) = items.next().await.transpose()? {
        operations.push(op);
    }
    Ok(operations)
}

async fn list_emulator_database_operations(
    database_path: &str,
    endpoint: &str,
    filter: Option<&str>,
) -> Result<Vec<InternalOperation>, SpannerError> {
    let admin = get_or_create_admin_client(Some(endpoint)).await?;
    let mut items = admin
        .list_operations()
        .set_name(format!("{database_path}/operations"))
        .set_filter(filter.unwrap_or_default())
        .by_item();

    let mut operations = Vec::new();
    while let Some(op) = items.next().await.transpose()? {
        operations.push(op);
    }

    Ok(operations)
}

fn admin_endpoint_url(endpoint: &str) -> String {
    let endpoint = endpoint.trim_end_matches('/');
    let endpoint = if endpoint.ends_with(":9010") {
        format!("{}:9020", endpoint.trim_end_matches(":9010"))
    } else {
        endpoint.to_string()
    };

    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint
    } else {
        format!("http://{endpoint}")
    }
}

fn database_operation_prefix(database_path: &str) -> String {
    format!("{database_path}/operations/")
}

fn instance_path_from_database_path(database_path: &str) -> Result<&str, SpannerError> {
    database_path
        .rsplit_once("/databases/")
        .map(|(prefix, _)| prefix)
        .ok_or_else(|| {
            SpannerError::Other(format!(
                "Invalid database path for operations listing: {database_path}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use duckdb::ffi::{
        duckdb_create_int64, duckdb_create_list_value, duckdb_create_logical_type,
        duckdb_create_null_value, duckdb_create_varchar, duckdb_destroy_logical_type,
        duckdb_destroy_value, duckdb_value, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
        DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    };
    use google_cloud_longrunning::model::Operation as InternalOperation;

    use super::{
        admin_endpoint_url, cached_init_result, database_operation_prefix, ddl_operation_error,
        ddl_statements_from_values, instance_path_from_database_path, operation_from_json,
    };

    #[test]
    fn test_ddl_statements_single_text_preserves_semicolons() {
        let sql = varchar_value("CREATE TABLE T (S STRING(MAX) DEFAULT ('a;b')) PRIMARY KEY (S)");
        assert_eq!(
            ddl_statements_from_values(&sql, None).unwrap(),
            vec!["CREATE TABLE T (S STRING(MAX) DEFAULT ('a;b')) PRIMARY KEY (S)".to_string()]
        );
    }

    #[test]
    fn test_ddl_statements_list() {
        let sql = null_value();
        let statements = varchar_list_value(&[
            "ALTER TABLE T ADD COLUMN A INT64",
            "ALTER TABLE T ADD COLUMN B STRING(MAX) DEFAULT ('a;b')",
        ]);
        assert_eq!(
            ddl_statements_from_values(&sql, Some(&statements)).unwrap(),
            vec![
                "ALTER TABLE T ADD COLUMN A INT64".to_string(),
                "ALTER TABLE T ADD COLUMN B STRING(MAX) DEFAULT ('a;b')".to_string(),
            ]
        );
    }

    #[test]
    fn test_ddl_statements_rejects_missing_or_ambiguous_inputs() {
        let sql = null_value();
        assert!(ddl_statements_from_values(&sql, None).is_err());

        let sql = varchar_value("CREATE TABLE T (Id INT64) PRIMARY KEY (Id)");
        let statements = varchar_list_value(&["ALTER TABLE T ADD COLUMN A INT64"]);
        assert!(ddl_statements_from_values(&sql, Some(&statements)).is_err());
    }

    #[test]
    fn test_ddl_statements_rejects_empty_or_non_varchar_list_entries() {
        let sql = null_value();
        let statements = varchar_list_value(&["ALTER TABLE T ADD COLUMN A INT64", "  "]);
        assert!(ddl_statements_from_values(&sql, Some(&statements)).is_err());

        let statements = bigint_list_value(&[1]);
        assert!(ddl_statements_from_values(&sql, Some(&statements)).is_err());

        let sql = bigint_value(1);
        assert!(ddl_statements_from_values(&sql, None).is_err());
    }

    #[test]
    fn test_cached_init_result_replays_success_without_rerun() {
        let cache = Mutex::new(None);
        let calls = AtomicUsize::new(0);

        let first = cached_init_result(&cache, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok("operation-1".to_string())
        })
        .unwrap();
        let second = cached_init_result(&cache, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok("operation-2".to_string())
        })
        .unwrap();

        assert_eq!(first, "operation-1");
        assert_eq!(second, "operation-1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_cached_init_result_replays_error_without_rerun() {
        let cache = Mutex::new(None);
        let calls = AtomicUsize::new(0);

        let first = cached_init_result::<String>(&cache, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err("first init failed".into())
        })
        .unwrap_err()
        .to_string();
        let second = cached_init_result::<String>(&cache, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok("operation-2".to_string())
        })
        .unwrap_err()
        .to_string();

        assert_eq!(first, "first init failed");
        assert_eq!(second, "first init failed");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_instance_path_from_database_path() {
        assert_eq!(
            instance_path_from_database_path("projects/p/instances/i/databases/d").unwrap(),
            "projects/p/instances/i"
        );
        assert!(instance_path_from_database_path("projects/p/instances/i").is_err());
    }

    #[test]
    fn test_database_operation_prefix() {
        assert_eq!(
            database_operation_prefix("projects/p/instances/i/databases/d"),
            "projects/p/instances/i/databases/d/operations/"
        );
    }

    #[test]
    fn test_admin_endpoint_url_maps_default_emulator_port_and_trims_slash() {
        assert_eq!(
            admin_endpoint_url("localhost:9010/"),
            "http://localhost:9020"
        );
        assert_eq!(
            admin_endpoint_url("http://127.0.0.1:9010/"),
            "http://127.0.0.1:9020"
        );
        assert_eq!(
            admin_endpoint_url("localhost:19020"),
            "http://localhost:19020"
        );
    }

    #[test]
    fn test_operation_from_json_preserves_error() {
        let op = operation_from_json(
            r#"{"name":"projects/p/instances/i/databases/d/operations/op1","done":true,"error":{"code":3,"message":"bad ddl"}}"#,
        )
        .unwrap();

        let err = ddl_operation_error(&op).unwrap();
        assert!(err.contains("code=3"));
        assert!(err.contains("bad ddl"));
    }

    #[test]
    fn test_database_operation_prefix_filters_other_databases() {
        let database_path = "projects/p/instances/i/databases/d";
        let prefix = database_operation_prefix(database_path);
        let operations = vec![
            InternalOperation::new().set_name(format!("{database_path}/operations/op-1")),
            InternalOperation::new()
                .set_name("projects/p/instances/i/databases/other/operations/op-2"),
        ];

        let matching_names: Vec<_> = operations
            .into_iter()
            .filter(|op| op.name.starts_with(&prefix))
            .map(|op| op.name)
            .collect();

        assert_eq!(
            matching_names,
            vec![format!("{database_path}/operations/op-1")]
        );
    }

    fn varchar_value(s: &str) -> duckdb::vtab::Value {
        let c_str = CString::new(s).unwrap();
        unsafe { duckdb::vtab::Value::from(duckdb_create_varchar(c_str.as_ptr())) }
    }

    fn null_value() -> duckdb::vtab::Value {
        unsafe { duckdb::vtab::Value::from(duckdb_create_null_value()) }
    }

    fn bigint_value(value: i64) -> duckdb::vtab::Value {
        unsafe { duckdb::vtab::Value::from(duckdb_create_int64(value)) }
    }

    fn varchar_list_value(values: &[&str]) -> duckdb::vtab::Value {
        let c_strings: Vec<_> = values
            .iter()
            .map(|value| CString::new(*value).unwrap())
            .collect();
        let duckdb_values: Vec<_> = c_strings
            .iter()
            .map(|value| unsafe { duckdb_create_varchar(value.as_ptr()) })
            .collect();

        list_value(DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR, duckdb_values)
    }

    fn bigint_list_value(values: &[i64]) -> duckdb::vtab::Value {
        let duckdb_values: Vec<_> = values
            .iter()
            .map(|value| unsafe { duckdb_create_int64(*value) })
            .collect();

        list_value(DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, duckdb_values)
    }

    fn list_value(
        child_type_id: duckdb::ffi::duckdb_type,
        mut values: Vec<duckdb_value>,
    ) -> duckdb::vtab::Value {
        unsafe {
            let mut child_type = duckdb_create_logical_type(child_type_id);
            let value =
                duckdb_create_list_value(child_type, values.as_mut_ptr(), values.len() as u64);
            duckdb_destroy_logical_type(&mut child_type);

            for mut value in values {
                duckdb_destroy_value(&mut value);
            }

            duckdb::vtab::Value::from(value)
        }
    }
}
