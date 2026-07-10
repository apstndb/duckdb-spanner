use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
use crate::{client, convert, runtime, schema, streaming, types, vector_size};

pub struct QueryBindData {
    database: String,
    sql: String,
    endpoint: Option<String>,
    parallelism_mode: crate::bind_utils::ParallelismMode,
    use_data_boost: bool,
    max_parallelism: Option<i64>,
    timestamp_bound: Option<crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
    params_json: Option<String>,
    columns: Vec<ColumnInfo>,
}

pub struct QueryInitData {
    streaming: streaming::StreamingState<Row>,
}

pub struct SpannerQueryVTab;

impl VTab for SpannerQueryVTab {
    type BindData = QueryBindData;
    type InitData = QueryInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let sql = bind.get_parameter(0).to_string();
        let database = crate::bind_utils::resolve_database_path(bind)?;

        let endpoint = crate::bind_utils::resolve_endpoint(bind);
        let legacy_use_parallelism =
            crate::bind_utils::get_named_bool(bind, "use_parallelism", false);
        let parallelism_mode = crate::bind_utils::resolve_parallelism_mode(
            crate::bind_utils::get_named_string(bind, "parallelism_mode").as_deref(),
            legacy_use_parallelism,
        )?;
        let use_data_boost = crate::bind_utils::get_named_bool(bind, "use_data_boost", false);
        let max_parallelism = crate::bind_utils::get_named_int64(bind, "max_parallelism");
        crate::bind_utils::validate_parallelism_options(
            parallelism_mode,
            use_data_boost,
            max_parallelism,
        )?;
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
            parallelism_mode,
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
        let vector_size = vector_size::runtime_vector_size();

        // Clone bind data fields for the spawned task ('static requirement)
        let database = bind_data.database.clone();
        let endpoint = bind_data.endpoint.clone();
        let sql = bind_data.sql.clone();
        let params_json = bind_data.params_json.clone();
        let parallelism_mode = bind_data.parallelism_mode;
        let use_data_boost = bind_data.use_data_boost;
        let max_parallelism = bind_data.max_parallelism;
        let timestamp_bound = bind_data.timestamp_bound.clone();
        let priority = bind_data.priority.clone();

        let streaming = streaming::StreamingState::spawn(vector_size, move |tx| async move {
            let rows_delivered = Arc::new(AtomicBool::new(false));
            let client = client::get_or_create_client(&database, endpoint.as_deref()).await?;

            if parallelism_mode.uses_partitioned_api() {
                let stmt = crate::params::create_statement_with_priority(
                    &sql,
                    params_json.as_deref(),
                    priority.clone(),
                )?;
                match stream_partitioned_query(
                    &client,
                    &tx,
                    stmt,
                    timestamp_bound.as_ref(),
                    use_data_boost,
                    max_parallelism,
                    rows_delivered.clone(),
                )
                .await
                {
                    Ok(()) => return Ok(()),
                    Err(e)
                        if parallelism_mode
                            .allows_fallback(rows_delivered.load(Ordering::SeqCst)) =>
                    {
                        eprintln!("[duckdb-spanner] Partitioned query failed: {e}, falling back to single query");
                    }
                    Err(e) => return Err(e),
                }
            }

            let stmt = crate::params::create_statement_with_priority(
                &sql,
                params_json.as_deref(),
                priority.clone(),
            )?;
            stream_single_query(&client, &tx, stmt, timestamp_bound.as_ref()).await
        })?;

        init.set_max_threads(1);

        Ok(QueryInitData { streaming })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let batch = match init_data.streaming.next_batch()? {
            Some(batch) => batch,
            None => {
                output.set_len(0);
                return Ok(());
            }
        };

        convert::write_rows_to_chunk(output, &batch, &bind_data.columns)?;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // sql
        ])
    }

    // Named parameters inspired by BigQuery EXTERNAL_QUERY / CloudSpannerProperties.
    // TODO: Add database_role once the official client exposes creator_role in SessionConfig.
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
                "parallelism_mode".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
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
    data_boost_enabled: bool,
    max_parallelism: Option<i64>,
    rows_delivered: Arc<AtomicBool>,
) -> Result<(), SpannerError> {
    let mut builder = client.batch_read_only_transaction();
    if let Some(tb) = timestamp_bound {
        builder = builder.set_timestamp_bound(tb.to_timestamp_bound());
    }
    let batch_tx = builder.build().await?;

    let partitions = batch_tx
        .partition_query(
            stmt,
            max_parallelism
                .map(|max| PartitionOptions::new().set_max_partitions(max))
                .unwrap_or_default(),
        )
        .await?;

    // Execute partitions concurrently, each with its own session from the pool.
    // Partitions embed the transaction selector, so any session can execute them
    // against the same consistent snapshot. `max_parallelism` is both the
    // partition-count hint above and a local worker cap.
    let permits = runtime::partition_worker_limit(partitions.len(), max_parallelism);
    let tx = tx.clone();
    let client = client.clone();
    runtime::run_bounded_partitions(partitions, permits, move |partition| {
        let tx = tx.clone();
        let client = client.clone();
        let rows_delivered = rows_delivered.clone();
        async move {
            let partition = partition.set_data_boost(data_boost_enabled);
            let mut iter = partition.execute(&client).await?;
            while let Some(row) = iter.next().await.transpose()? {
                rows_delivered.store(true, Ordering::SeqCst);
                if tx.send(Ok(row)).await.is_err() {
                    return Ok(()); // receiver dropped
                }
            }
            Ok::<(), SpannerError>(())
        }
    })
    .await

    // Invariant: partition tasks must be fully terminated before this function
    // returns. Client eviction close relies on Arc uniqueness => quiescence, so a
    // task still holding a `Client` clone and `ManagedSession` must not outlive
    // the streamer. The bounded runner drains (aborts + awaits) every task on
    // error before returning; the caller reads `rows_delivered` only after we
    // return, so an aborted straggler can no longer emit a row past the fallback
    // decision. If StreamingState drops this outer producer task, Tokio drops
    // this JoinSet and aborts every remaining partition task.
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
