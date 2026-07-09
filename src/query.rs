use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::request_options::Priority;
use google_cloud_spanner::model::PartitionOptions;
use google_cloud_spanner::result::Row;
use google_cloud_spanner::statement::Statement;
use tokio::sync::mpsc;

use crate::error::SpannerError;
use crate::schema::ColumnInfo;
use crate::{client, convert, runtime, schema, types};

const BATCH_SIZE: usize = 2048;

pub struct QueryBindData {
    database: String,
    sql: String,
    endpoint: Option<String>,
    use_parallelism: bool,
    use_data_boost: bool,
    max_parallelism: Option<i64>,
    timestamp_bound: Option<crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
    params_json: Option<String>,
    columns: Vec<ColumnInfo>,
}

/// Streaming init data: rows arrive via an mpsc channel from a background tokio task.
pub struct QueryInitData {
    receiver: Mutex<mpsc::Receiver<Result<Row, SpannerError>>>,
}

pub struct SpannerQueryVTab;

impl VTab for SpannerQueryVTab {
    type BindData = QueryBindData;
    type InitData = QueryInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let sql = bind.get_parameter(0).to_string();
        let database = crate::bind_utils::resolve_database_path(bind)?;

        let endpoint = crate::bind_utils::resolve_endpoint(bind);
        let use_parallelism = crate::bind_utils::get_named_bool(bind, "use_parallelism", false);
        let use_data_boost = crate::bind_utils::get_named_bool(bind, "use_data_boost", false);
        let max_parallelism = crate::bind_utils::get_named_int64(bind, "max_parallelism");
        let timestamp_bound = crate::bind_utils::resolve_timestamp_bound(
            crate::bind_utils::get_named_int64(bind, "exact_staleness_secs"),
            crate::bind_utils::get_named_int64(bind, "max_staleness_secs"),
            crate::bind_utils::get_named_string(bind, "read_timestamp").as_deref(),
            crate::bind_utils::get_named_string(bind, "min_read_timestamp").as_deref(),
        )?;
        let priority = crate::bind_utils::get_named_string(bind, "priority")
            .map(|s| crate::bind_utils::parse_priority(&s))
            .transpose()?;
        let params_json = crate::bind_utils::get_named_string(bind, "params");

        // Discover output schema
        let columns = runtime::block_on(async {
            let client = client::get_or_create_client(&database, endpoint.as_deref()).await?;
            schema::discover_query_schema(&client, &sql, params_json.as_deref()).await
        })??;

        // Register output columns with DuckDB
        for col in &columns {
            let logical_type = types::spanner_type_to_logical(&col.spanner_type);
            bind.add_result_column(&col.name, logical_type);
        }

        Ok(QueryBindData {
            database,
            sql,
            endpoint,
            use_parallelism,
            use_data_boost,
            max_parallelism,
            timestamp_bound,
            priority,
            params_json,
            columns,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<QueryBindData>() };

        let (tx, rx) = mpsc::channel(BATCH_SIZE);

        // Clone bind data fields for the spawned task ('static requirement)
        let database = bind_data.database.clone();
        let endpoint = bind_data.endpoint.clone();
        let sql = bind_data.sql.clone();
        let params_json = bind_data.params_json.clone();
        let use_parallelism = bind_data.use_parallelism;
        let use_data_boost = bind_data.use_data_boost;
        let max_parallelism = bind_data.max_parallelism;
        let timestamp_bound = bind_data.timestamp_bound.clone();
        let priority = bind_data.priority.clone();

        runtime::spawn(async move {
            let rows_delivered = Arc::new(AtomicBool::new(false));
            let result: Result<(), SpannerError> = async {
                let client =
                    client::get_or_create_client(&database, endpoint.as_deref()).await?;

                if use_parallelism {
                    let partition_options =
                        max_parallelism.map(|max| PartitionOptions::new().set_max_partitions(max));

                    let stmt =
                        crate::params::create_statement_with_priority(&sql, params_json.as_deref(), priority.clone())?;
                    match stream_partitioned_query(
                        &client,
                        &tx,
                        stmt,
                        timestamp_bound.as_ref(),
                        priority.clone(),
                        use_data_boost,
                        partition_options,
                        rows_delivered.clone(),
                    )
                    .await
                    {
                        Ok(()) => return Ok(()),
                        Err(e) if !rows_delivered.load(Ordering::SeqCst) => {
                            eprintln!("[duckdb-spanner] Partitioned query failed: {e}, falling back to single query");
                        }
                        Err(e) => return Err(e),
                    }
                }

                let stmt =
                    crate::params::create_statement_with_priority(&sql, params_json.as_deref(), priority.clone())?;
                stream_single_query(
                    &client,
                    &tx,
                    stmt,
                    timestamp_bound.as_ref(),
                )
                .await
            }
            .await;

            if let Err(e) = result {
                let _ = tx.send(Err(e)).await;
            }
        })?;

        init.set_max_threads(1);

        Ok(QueryInitData {
            receiver: Mutex::new(rx),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let mut rx = init_data.receiver.lock().unwrap_or_else(|e| e.into_inner());

        // Block for the first row to ensure forward progress
        let first = match rx.blocking_recv() {
            Some(Ok(row)) => row,
            Some(Err(e)) => return Err(e.into()),
            None => {
                output.set_len(0);
                return Ok(());
            }
        };

        let mut batch = Vec::with_capacity(BATCH_SIZE);
        batch.push(first);

        // Fill the rest of the batch non-blocking
        while batch.len() < BATCH_SIZE {
            match rx.try_recv() {
                Ok(Ok(row)) => batch.push(row),
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => break, // Empty or Disconnected
            }
        }
        drop(rx);

        convert::write_rows_to_chunk(output, &batch, &bind_data.columns)?;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // sql
        ])
    }

    // Named parameters inspired by BigQuery EXTERNAL_QUERY / CloudSpannerProperties.
    // TODO: Add database_role once gcloud-spanner exposes creator_role in SessionConfig.
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            // Database identification
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
            // Query options
            (
                "use_parallelism".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
            (
                "use_data_boost".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
            (
                "max_parallelism".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Integer),
            ),
            (
                "exact_staleness_secs".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            ),
            (
                "max_staleness_secs".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            ),
            (
                "read_timestamp".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "min_read_timestamp".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "priority".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "params".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
        ])
    }
}

async fn stream_partitioned_query(
    client: &DatabaseClient,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    stmt: Statement,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
    _priority: Option<Priority>,
    data_boost_enabled: bool,
    partition_options: Option<PartitionOptions>,
    rows_delivered: Arc<AtomicBool>,
) -> Result<(), SpannerError> {
    let mut builder = client.batch_read_only_transaction();
    if let Some(tb) = timestamp_bound {
        builder = builder.set_timestamp_bound(tb.to_timestamp_bound());
    }
    let batch_tx = builder.build().await?;

    let partitions = batch_tx
        .partition_query(stmt, partition_options.unwrap_or_default())
        .await?;

    // Execute partitions concurrently, each with its own session from the pool.
    // Partitions embed the transaction selector, so any session can execute them
    // against the same consistent snapshot. Concurrency is capped by a semaphore
    // sized to the shared runtime's worker threads so we don't oversubscribe it
    // (max_parallelism above only bounds partition *creation*, not execution).
    let permits = runtime::worker_threads().min(partitions.len().max(1));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(permits));
    let mut join_set = tokio::task::JoinSet::new();
    for partition in partitions {
        let tx = tx.clone();
        let client = client.clone();
        let rows_delivered = rows_delivered.clone();
        let semaphore = semaphore.clone();
        join_set.spawn(async move {
            let _permit = semaphore
                .acquire()
                .await
                .map_err(|e| SpannerError::Other(format!("Semaphore closed: {e}")))?;
            let partition = partition.set_data_boost(data_boost_enabled);
            let mut iter = partition.execute(&client).await?;
            while let Some(row) = iter.next().await.transpose()? {
                rows_delivered.store(true, Ordering::SeqCst);
                if tx.send(Ok(row)).await.is_err() {
                    return Ok(()); // receiver dropped
                }
            }
            Ok::<(), SpannerError>(())
        });
    }

    // Invariant: partition tasks must be fully terminated before this function
    // returns. Client eviction close relies on Arc uniqueness => quiescence, so a
    // task still holding a `Client` clone and `ManagedSession` must not outlive
    // the streamer. `join_partitions` drains (aborts + awaits) every task on
    // error before returning; the caller reads `rows_delivered` only after we
    // return, so an aborted straggler can no longer emit a row past the fallback
    // decision.
    runtime::join_partitions(join_set).await
}

async fn stream_single_query(
    client: &DatabaseClient,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    stmt: Statement,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
) -> Result<(), SpannerError> {
    let mut builder = client.single_use();
    if let Some(tb) = timestamp_bound {
        builder = builder.set_timestamp_bound(tb.to_timestamp_bound());
    }
    let spanner_tx = builder.build();
    let mut iter = spanner_tx.execute_query(stmt).await?;

    while let Some(row) = iter.next().await.transpose()? {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(()); // receiver dropped
        }
    }
    Ok(())
}
