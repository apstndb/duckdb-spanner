use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use google_cloud_gax::conn::Environment;
use google_cloud_googleapis::longrunning::operation;
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

/// Get or create a Spanner Admin client.
///
/// Admin clients are NOT per-database (unlike data clients). The cache key is
/// the endpoint string (or empty for default).
async fn get_or_create_admin_client(
    endpoint: Option<&str>,
) -> Result<Arc<AdminClient>, SpannerError> {
    let cache_key = endpoint.unwrap_or("").to_string();

    {
        let cache = ADMIN_CLIENT_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(client) = cache.get(&cache_key) {
            return Ok(Arc::clone(client));
        }
    }

    let config = match endpoint {
        Some(ep) => AdminClientConfig {
            environment: Environment::Emulator(ep.to_string()),
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

    let mut cache = ADMIN_CLIENT_CACHE
        .lock()
        .unwrap_or_else(|e| e.into_inner());
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

/// Extract the instance path (`projects/P/instances/I`) from a full database
/// path (`projects/P/instances/I/databases/D`).
fn instance_parent_from_database_path(database_path: &str) -> Result<String, SpannerError> {
    // Expected format: projects/<p>/instances/<i>/databases/<d>
    let parts: Vec<&str> = database_path.split('/').collect();
    if parts.len() >= 4 && parts[0] == "projects" && parts[2] == "instances" {
        Ok(format!("{}/{}/{}/{}", parts[0], parts[1], parts[2], parts[3]))
    } else {
        Err(SpannerError::Other(format!(
            "Invalid database path '{}': expected projects/<p>/instances/<i>/databases/<d>",
            database_path
        )))
    }
}

// ---------------------------------------------------------------------------
// SpannerDdlVTab — synchronous DDL execution
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct DdlBindData {
    database_path: String,
    sql: String,
    endpoint: Option<String>,
}

pub struct DdlInitData {
    result: Mutex<Option<DdlResult>>,
}

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
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);

        bind.add_result_column("operation_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("done", LogicalTypeHandle::from(LogicalTypeId::Boolean));
        bind.add_result_column("duration_secs", LogicalTypeHandle::from(LogicalTypeId::Double));

        Ok(DdlBindData {
            database_path,
            sql,
            endpoint,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<DdlBindData>() };

        let database_path = bind_data.database_path.clone();
        let sql = bind_data.sql.clone();
        let endpoint = bind_data.endpoint.clone();

        let result = runtime::block_on(async {
            let admin = get_or_create_admin_client(endpoint.as_deref()).await?;

            let req = UpdateDatabaseDdlRequest {
                database: database_path,
                statements: vec![sql],
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
                col_done.as_mut_slice::<bool>()[0] = r.done;
                let mut col_duration = output.flat_vector(2);
                col_duration.as_mut_slice::<f64>()[0] = r.duration_secs;
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
    sql: String,
    endpoint: Option<String>,
}

pub struct DdlAsyncInitData {
    result: Mutex<Option<DdlAsyncResult>>,
}

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
        let database_path = bind_utils::resolve_database_path(bind)?;
        let endpoint = bind_utils::resolve_endpoint(bind);

        bind.add_result_column("operation_name", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column("done", LogicalTypeHandle::from(LogicalTypeId::Boolean));

        Ok(DdlAsyncBindData {
            database_path,
            sql,
            endpoint,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<DdlAsyncBindData>() };

        let database_path = bind_data.database_path.clone();
        let sql = bind_data.sql.clone();
        let endpoint = bind_data.endpoint.clone();

        let result = runtime::block_on(async {
            let admin = get_or_create_admin_client(endpoint.as_deref()).await?;

            let req = UpdateDatabaseDdlRequest {
                database: database_path,
                statements: vec![sql],
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
                col_done.as_mut_slice::<bool>()[0] = r.done;
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
    error_code: i32,
    error_message: String,
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
        bind.add_result_column("error_code", LogicalTypeHandle::from(LogicalTypeId::Integer));
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
            let admin = get_or_create_admin_client(endpoint.as_deref()).await?;

            // ListDatabaseOperationsRequest needs the instance path as parent
            let parent = instance_parent_from_database_path(&database_path)?;

            let req = ListDatabaseOperationsRequest {
                parent,
                filter: filter.unwrap_or_default(),
                page_size: 0,
                page_token: String::new(),
            };

            let operations = admin
                .database()
                .list_database_operations(req, None)
                .await
                .map_err(SpannerError::Grpc)?;

            let rows: Vec<OperationRow> = operations
                .into_iter()
                .map(|op| operation_to_row(op))
                .collect();

            Ok::<Vec<OperationRow>, SpannerError>(rows)
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
        let col_error_message = output.flat_vector(4);

        for (i, row) in batch.iter().enumerate() {
            col_name.insert(i, &row.name);
            col_done.as_mut_slice::<bool>()[i] = row.done;
            col_metadata_type.insert(i, &row.metadata_type);
            col_error_code.as_mut_slice::<i32>()[i] = row.error_code;
            col_error_message.insert(i, &row.error_message);
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
        Some(operation::Result::Error(status)) => (status.code, status.message.clone()),
        _ => (0, String::new()),
    };

    OperationRow {
        name: op.name,
        done: op.done,
        metadata_type,
        error_code,
        error_message,
    }
}
