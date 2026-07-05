use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use google_cloud_gax::conn::Environment;
use google_cloud_gax::grpc::transport::Channel;
use google_cloud_googleapis::longrunning::operation;
use google_cloud_googleapis::longrunning::operations_client::OperationsClient;
use google_cloud_googleapis::longrunning::ListOperationsRequest;
use google_cloud_googleapis::longrunning::Operation as InternalOperation;
use google_cloud_googleapis::spanner::admin::database::v1::{
    ListDatabaseOperationsRequest, UpdateDatabaseDdlRequest,
};
use google_cloud_spanner::admin::client::Client as AdminClient;
use google_cloud_spanner::admin::AdminClientConfig;

use crate::error::SpannerError;
use crate::{bind_utils, runtime};

// ---------------------------------------------------------------------------
// Admin client cache
// ---------------------------------------------------------------------------

static ADMIN_CLIENT_CACHE: LazyLock<Mutex<HashMap<String, Arc<AdminClient>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static EMULATOR_OPERATIONS_CHANNEL_CACHE: LazyLock<Mutex<HashMap<String, Channel>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Get or create a Spanner Admin client.
///
/// Admin clients are NOT per-database (unlike data clients). The cache key is
/// the endpoint string (or empty for default).
async fn get_or_create_admin_client(
    endpoint: Option<&str>,
) -> Result<Arc<AdminClient>, SpannerError> {
    let cache_key = endpoint.unwrap_or("").to_string();

    {
        let cache = ADMIN_CLIENT_CACHE.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(client) = cache.get(&cache_key) {
            return Ok(Arc::clone(client));
        }
    }

    let config = match endpoint {
        Some(ep) => AdminClientConfig {
            environment: Environment::Emulator(ep.to_string()),
            ..Default::default()
        },
        None => {
            if std::env::var("SPANNER_EMULATOR_HOST").is_ok() {
                AdminClientConfig::default()
            } else {
                AdminClientConfig::default()
                    .with_auth()
                    .await
                    .map_err(|e| SpannerError::Other(format!("Auth error: {e}")))?
            }
        }
    };

    let client = AdminClient::new(config)
        .await
        .map_err(|e| SpannerError::Other(format!("Admin client error: {e}")))?;
    let client = Arc::new(client);

    let mut cache = ADMIN_CLIENT_CACHE.lock().unwrap_or_else(|e| e.into_inner());
    let entry = cache
        .entry(cache_key)
        .or_insert_with(|| Arc::clone(&client));
    Ok(Arc::clone(entry))
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
    ]
}

/// Split a single `spanner_ddl`/`spanner_ddl_async` TEXT argument into a batch of
/// DDL statements.
///
/// DuckDB table functions only accept scalar positional parameters here (no
/// `LIST(VARCHAR)` support in `bind_utils`), so batching is exposed by letting
/// callers pass multiple `;`-separated statements in the one TEXT argument,
/// e.g. `spanner_ddl('ALTER TABLE T ADD COLUMN A INT64; ALTER TABLE T ADD COLUMN B INT64')`.
/// All statements are sent to Spanner as a single `UpdateDatabaseDdl` request
/// (matching Spanner's native batch semantics: one longrunning operation covers
/// the whole batch, and it fails atomically if any statement is invalid).
///
/// Trailing/empty segments (from a trailing `;` or blank statements) are
/// dropped. This is a naive split on `;` and does not understand string
/// literals, so a `;` inside a DDL string literal (e.g. a `DEFAULT` value)
/// would be mis-split; this is rare in DDL and is documented in the README.
fn split_ddl_statements(sql: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let statements: Vec<String> = sql
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();

    if statements.is_empty() {
        return Err("spanner_ddl requires at least one non-empty DDL statement".into());
    }

    Ok(statements)
}

// ---------------------------------------------------------------------------
// SpannerDdlVTab — synchronous DDL execution
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct DdlBindData {
    database_path: String,
    statements: Vec<String>,
    endpoint: Option<String>,
    // DuckDB may call init() more than once for the same bound table function
    // (e.g. when the query is re-planned/re-initialized across execution
    // phases). Since sending the DDL batch to Spanner is a side effect, a
    // second execution must not resend it. `executed` is a one-shot guard:
    // only the init() call that flips it false->true actually issues the
    // request; later calls just reuse `cached_result`.
    executed: Arc<AtomicBool>,
    cached_result: Arc<Mutex<Option<DdlResult>>>,
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
        let sql = bind.get_parameter(0).to_string();
        let statements = split_ddl_statements(&sql)?;
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);

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
            executed: Arc::new(AtomicBool::new(false)),
            cached_result: Arc::new(Mutex::new(None)),
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<DdlBindData>() };

        // Only the first init() call executes the DDL batch; see the comment
        // on `DdlBindData::executed`.
        if bind_data
            .executed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let database_path = bind_data.database_path.clone();
            let statements = bind_data.statements.clone();
            let endpoint = bind_data.endpoint.clone();

            let result = runtime::block_on(async {
                let admin = get_or_create_admin_client(endpoint.as_deref()).await?;

                let req = UpdateDatabaseDdlRequest {
                    database: database_path,
                    statements,
                    operation_id: String::new(),
                    proto_descriptors: vec![],
                    ..Default::default()
                };

                let start = Instant::now();
                let mut op = admin
                    .database()
                    .update_database_ddl(req, None)
                    .await
                    .map_err(SpannerError::Grpc)?;

                let operation_name = op.name().to_string();

                // Wait for completion
                op.wait(None).await.map_err(SpannerError::Grpc)?;

                let duration_secs = start.elapsed().as_secs_f64();

                Ok::<DdlResult, SpannerError>(DdlResult {
                    operation_name,
                    done: true,
                    duration_secs,
                })
            })??;

            *bind_data
                .cached_result
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(result);
        }

        init.set_max_threads(1);

        let result = bind_data
            .cached_result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        Ok(DdlInitData {
            result: Mutex::new(result),
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
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // sql
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(database_named_parameters())
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
    // See the comment on `DdlBindData::executed`: guards against DuckDB
    // re-invoking init() and resending the DDL batch a second time.
    executed: Arc<AtomicBool>,
    cached_result: Arc<Mutex<Option<DdlAsyncResult>>>,
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
        let sql = bind.get_parameter(0).to_string();
        let statements = split_ddl_statements(&sql)?;
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);

        bind.add_result_column(
            "operation_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("done", LogicalTypeHandle::from(LogicalTypeId::Boolean));

        Ok(DdlAsyncBindData {
            database_path,
            statements,
            endpoint,
            executed: Arc::new(AtomicBool::new(false)),
            cached_result: Arc::new(Mutex::new(None)),
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<DdlAsyncBindData>() };

        // Only the first init() call executes the DDL batch; see the comment
        // on `DdlBindData::executed`.
        if bind_data
            .executed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            let database_path = bind_data.database_path.clone();
            let statements = bind_data.statements.clone();
            let endpoint = bind_data.endpoint.clone();

            let result = runtime::block_on(async {
                let admin = get_or_create_admin_client(endpoint.as_deref()).await?;

                let req = UpdateDatabaseDdlRequest {
                    database: database_path,
                    statements,
                    operation_id: String::new(),
                    proto_descriptors: vec![],
                    ..Default::default()
                };

                let op = admin
                    .database()
                    .update_database_ddl(req, None)
                    .await
                    .map_err(SpannerError::Grpc)?;

                Ok::<DdlAsyncResult, SpannerError>(DdlAsyncResult {
                    operation_name: op.name().to_string(),
                    done: op.done(),
                })
            })??;

            *bind_data
                .cached_result
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = Some(result);
        }

        init.set_max_threads(1);

        let result = bind_data
            .cached_result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        Ok(DdlAsyncInitData {
            result: Mutex::new(result),
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
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // sql
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(database_named_parameters())
    }
}

// ---------------------------------------------------------------------------
// SpannerOperationsVTab — list database long-running operations
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct OperationsBindData {
    database_path: String,
    endpoint: Option<String>,
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
            filter,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<OperationsBindData>() };

        let database_path = bind_data.database_path.clone();
        let endpoint = bind_data.endpoint.clone();
        let filter = bind_data.filter.clone();

        let ops = runtime::block_on(async {
            list_database_operations(&database_path, endpoint.as_deref(), filter.as_deref()).await
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

        // Drain up to 2048 rows per call
        let batch_size = ops.len().min(2048);
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
            match row.error_code {
                // SAFETY: the bind phase registers `error_code` as INTEGER, and
                // `i` is within the drained batch size.
                Some(code) => unsafe { *col_error_code.as_mut_ptr::<i32>().add(i) = code },
                None => col_error_code.set_null(i),
            }
            match &row.error_message {
                Some(msg) => col_error_message.insert(i, msg),
                None => col_error_message.set_null(i),
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
        .map(|m| m.type_url.clone())
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

async fn list_database_operations(
    database_path: &str,
    endpoint: Option<&str>,
    filter: Option<&str>,
) -> Result<Vec<OperationRow>, SpannerError> {
    // The emulator only implements google.longrunning.Operations/ListOperations,
    // while real Spanner exposes DatabaseAdmin/ListDatabaseOperations with auth.
    let mut operations = match endpoint
        .map(str::to_owned)
        .or_else(|| std::env::var("SPANNER_EMULATOR_HOST").ok())
    {
        Some(emulator_endpoint) => {
            list_emulator_database_operations(database_path, &emulator_endpoint, filter).await?
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
    let req = ListDatabaseOperationsRequest {
        parent: instance_path_from_database_path(database_path)?.to_string(),
        filter: filter.unwrap_or_default().to_string(),
        page_size: 0,
        page_token: String::new(),
    };
    // The gcloud-spanner admin helper eagerly follows next_page_token and
    // returns a flattened Vec<Operation>, so no extra pagination loop is needed here.
    admin
        .database()
        .list_database_operations(req, None)
        .await
        .map_err(SpannerError::Grpc)
}

async fn list_emulator_database_operations(
    database_path: &str,
    endpoint: &str,
    filter: Option<&str>,
) -> Result<Vec<InternalOperation>, SpannerError> {
    let channel = get_or_create_emulator_operations_channel(endpoint).await?;
    let mut client = OperationsClient::new(channel);
    let mut all_operations = Vec::new();
    let mut page_token = String::new();

    loop {
        let req = ListOperationsRequest {
            name: format!("{database_path}/operations"),
            filter: filter.unwrap_or_default().to_string(),
            page_size: 0,
            page_token: page_token.clone(),
        };

        let response = client
            .list_operations(req)
            .await
            .map_err(SpannerError::Grpc)?
            .into_inner();

        all_operations.extend(response.operations);

        if response.next_page_token.is_empty() {
            break;
        }
        page_token = response.next_page_token;
    }

    Ok(all_operations)
}

async fn get_or_create_emulator_operations_channel(
    endpoint: &str,
) -> Result<Channel, SpannerError> {
    let cache_key = emulator_endpoint_url(endpoint);

    {
        let cache = EMULATOR_OPERATIONS_CHANNEL_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(channel) = cache.get(&cache_key) {
            return Ok(channel.clone());
        }
    }

    let channel = Channel::from_shared(cache_key.clone())
        .map_err(|e| SpannerError::Other(format!("Invalid endpoint: {e}")))?
        .connect()
        .await
        .map_err(|e| SpannerError::Other(format!("Connect error: {e}")))?;

    let mut cache = EMULATOR_OPERATIONS_CHANNEL_CACHE
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let entry = cache.entry(cache_key).or_insert_with(|| channel.clone());
    Ok(entry.clone())
}

fn emulator_endpoint_url(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
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
    use google_cloud_googleapis::longrunning::Operation as InternalOperation;

    use super::{database_operation_prefix, instance_path_from_database_path, split_ddl_statements};

    #[test]
    fn test_split_ddl_statements_single() {
        assert_eq!(
            split_ddl_statements("CREATE TABLE T (Id INT64) PRIMARY KEY (Id)").unwrap(),
            vec!["CREATE TABLE T (Id INT64) PRIMARY KEY (Id)".to_string()]
        );
    }

    #[test]
    fn test_split_ddl_statements_batch() {
        let sql = "ALTER TABLE T ADD COLUMN A INT64; ALTER TABLE T ADD COLUMN B INT64";
        assert_eq!(
            split_ddl_statements(sql).unwrap(),
            vec![
                "ALTER TABLE T ADD COLUMN A INT64".to_string(),
                "ALTER TABLE T ADD COLUMN B INT64".to_string(),
            ]
        );
    }

    #[test]
    fn test_split_ddl_statements_trailing_semicolon_and_whitespace() {
        let sql = "  ALTER TABLE T ADD COLUMN A INT64;\nALTER TABLE T ADD COLUMN B INT64;  ;\n";
        assert_eq!(
            split_ddl_statements(sql).unwrap(),
            vec![
                "ALTER TABLE T ADD COLUMN A INT64".to_string(),
                "ALTER TABLE T ADD COLUMN B INT64".to_string(),
            ]
        );
    }

    #[test]
    fn test_split_ddl_statements_empty_input_errors() {
        assert!(split_ddl_statements("").is_err());
        assert!(split_ddl_statements("   ;  ; ").is_err());
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
    fn test_database_operation_prefix_filters_other_databases() {
        let database_path = "projects/p/instances/i/databases/d";
        let prefix = database_operation_prefix(database_path);
        let operations = vec![
            InternalOperation {
                name: format!("{database_path}/operations/op-1"),
                ..Default::default()
            },
            InternalOperation {
                name: "projects/p/instances/i/databases/other/operations/op-2".to_string(),
                ..Default::default()
            },
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
}
