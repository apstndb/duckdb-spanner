use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::key::KeySet;
use google_cloud_spanner::model::request_options::Priority;
use google_cloud_spanner::model::PartitionOptions;
use google_cloud_spanner::read::ReadRequest;
use google_cloud_spanner::result::Row;
use tokio::sync::mpsc;

use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;

use crate::error::SpannerError;
use crate::schema::ColumnInfo;
use crate::{client, convert, runtime, schema, streaming, types, vector_size};

pub struct ScanBindData {
    database: String,
    table: String,
    endpoint: Option<String>,
    parallelism_mode: crate::bind_utils::ParallelismMode,
    use_data_boost: bool,
    max_parallelism: Option<i64>,
    index: String,
    timestamp_bound: Option<crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
    columns: Vec<ColumnInfo>,
}

pub struct ScanInitData {
    streaming: streaming::StreamingState<Row>,
    projected_columns: Vec<usize>,
}

pub struct SpannerScanVTab;

impl VTab for SpannerScanVTab {
    type BindData = ScanBindData;
    type InitData = ScanInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let table = bind.get_parameter(0).to_string();
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
        let index = crate::bind_utils::get_named_string(bind, "index").unwrap_or_default();
        let timestamp_bound = crate::bind_utils::resolve_timestamp_bound(
            crate::bind_utils::get_named_int64(bind, "exact_staleness_secs"),
            crate::bind_utils::get_named_int64(bind, "max_staleness_secs"),
            crate::bind_utils::get_named_string(bind, "read_timestamp").as_deref(),
            crate::bind_utils::get_named_string(bind, "min_read_timestamp").as_deref(),
        )?;
        let priority = crate::bind_utils::get_named_string(bind, "priority")
            .map(|s| crate::bind_utils::parse_priority(&s))
            .transpose()?;
        let dialect = match crate::bind_utils::get_named_string(bind, "dialect") {
            Some(s) => schema::parse_dialect(&s)?,
            None => DatabaseDialect::Unspecified,
        };

        // Discover table schema from INFORMATION_SCHEMA
        // If dialect is specified, uses correct param syntax directly.
        // If not, auto-detects via INFORMATION_SCHEMA.SCHEMATA (one extra round-trip).
        let columns = runtime::block_on(async {
            let client = client::get_or_create_client(&database, endpoint.as_deref()).await?;
            schema::discover_table_schema(
                &client,
                &table,
                dialect.clone(),
                &database,
                endpoint.as_deref(),
            )
            .await
        })??;

        // Register output columns with DuckDB
        for col in &columns {
            let logical_type = types::spanner_type_to_logical(&col.spanner_type);
            bind.add_result_column(&col.name, logical_type);
        }

        Ok(ScanBindData {
            database,
            table,
            endpoint,
            parallelism_mode,
            use_data_boost,
            max_parallelism,
            index,
            timestamp_bound,
            priority,
            columns,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<ScanBindData>() };
        let vector_size = vector_size::runtime_vector_size();

        // Get projected column indices (DuckDB tells us which columns it needs)
        let projected_columns: Vec<usize> = init
            .get_column_indices()
            .iter()
            .map(|&idx| idx as usize)
            .collect();

        // Determine which columns to request from Spanner
        let column_names: Vec<String> = projected_columns
            .iter()
            .map(|&idx| bind_data.columns[idx].name.clone())
            .collect();

        // Clone bind data fields for the spawned task ('static requirement)
        let database = bind_data.database.clone();
        let endpoint = bind_data.endpoint.clone();
        let table = bind_data.table.clone();
        let index = bind_data.index.clone();
        let parallelism_mode = bind_data.parallelism_mode;
        let use_data_boost = bind_data.use_data_boost;
        let max_parallelism = bind_data.max_parallelism;
        let timestamp_bound = bind_data.timestamp_bound.clone();
        let priority = bind_data.priority.clone();

        let streaming = streaming::StreamingState::spawn(vector_size, move |tx| async move {
            let rows_delivered = Arc::new(AtomicBool::new(false));
            let client = client::get_or_create_client(&database, endpoint.as_deref()).await?;

            let col_refs: Vec<&str> = column_names.iter().map(|s| s.as_str()).collect();

            if parallelism_mode.uses_partitioned_api() {
                match stream_partitioned_read(
                    &client,
                    &tx,
                    &table,
                    &col_refs,
                    &index,
                    timestamp_bound.as_ref(),
                    priority.clone(),
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
                        eprintln!("[duckdb-spanner] Partitioned read failed: {e}, falling back to single read");
                    }
                    Err(e) => return Err(e),
                }
            }

            stream_single_read(
                &client,
                &tx,
                &table,
                &col_refs,
                &index,
                timestamp_bound.as_ref(),
                priority.clone(),
            )
            .await
        })?;

        init.set_max_threads(1);

        Ok(ScanInitData {
            streaming,
            projected_columns,
        })
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

        // Write projected columns using the unified conversion.
        // Spanner Read API returns columns in the order we requested (projected order),
        // so spanner_col_idx = i and output_col_idx = i.
        write_projected_rows(
            output,
            &batch,
            &bind_data.columns,
            &init_data.projected_columns,
        )?;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // table
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
            // Read options
            (
                "dialect".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
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
                "index".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
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
        ])
    }

    fn supports_pushdown() -> bool {
        true
    }
}

// ─── Partitioned Read ──────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn stream_partitioned_read(
    client: &DatabaseClient,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    table: &str,
    column_names: &[&str],
    index: &str,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
    data_boost_enabled: bool,
    max_parallelism: Option<i64>,
    rows_delivered: Arc<AtomicBool>,
) -> Result<(), SpannerError> {
    let mut batch_tx = client.batch_read_only_transaction();
    if let Some(tb) = timestamp_bound {
        batch_tx = batch_tx.set_timestamp_bound(tb.to_timestamp_bound());
    }
    let batch_tx = batch_tx.build().await?;

    let read = build_read_request(table, column_names, index, priority);
    let partitions = batch_tx
        .partition_read(
            read,
            max_parallelism
                .map(|max| PartitionOptions::default().set_max_partitions(max))
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
            let mut iter = partition
                .set_data_boost(data_boost_enabled)
                .execute(&client)
                .await?;
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

async fn stream_single_read(
    client: &DatabaseClient,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    table: &str,
    column_names: &[&str],
    index: &str,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
) -> Result<(), SpannerError> {
    let mut spanner_tx = client.single_use();
    if let Some(tb) = timestamp_bound {
        spanner_tx = spanner_tx.set_timestamp_bound(tb.to_timestamp_bound());
    }
    let spanner_tx = spanner_tx.build();

    let read = build_read_request(table, column_names, index, priority);
    let mut iter = spanner_tx.execute_read(read).await?;

    while let Some(row) = iter.next().await.transpose()? {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(()); // receiver dropped
        }
    }
    Ok(())
}

fn build_read_request(
    table: &str,
    column_names: &[&str],
    index: &str,
    priority: Option<Priority>,
) -> ReadRequest {
    let columns = column_names.iter().copied();
    let builder = ReadRequest::builder(table, columns);
    let mut builder = if index.is_empty() {
        builder.with_keys(KeySet::all())
    } else {
        builder.with_index(index, KeySet::all())
    };
    if let Some(priority) = priority {
        builder = builder.set_priority(priority);
    }
    builder.build()
}

// ─── Projected Row Writing ──────────────────────────────────────────────────

/// Write rows to output for projected columns using the unified conversion path.
///
/// Spanner Read API returns columns in the order we requested (matching projected_columns order).
/// DuckDB output vectors are indexed by the projected column index.
fn write_projected_rows(
    output: &mut DataChunkHandle,
    rows: &[Row],
    all_columns: &[ColumnInfo],
    projected_columns: &[usize],
) -> Result<(), SpannerError> {
    if rows.is_empty() {
        output.set_len(0);
        return Ok(());
    }

    // projected_columns[i] = original column index in bind_data.columns
    // Spanner row column index = i (columns requested in projected order)
    // DuckDB output vector index = i (output only contains projected columns)
    for (i, &orig_col_idx) in projected_columns.iter().enumerate() {
        let col_info = &all_columns[orig_col_idx];
        convert::write_column_from_rows(
            output,
            i,
            rows,
            i,
            &col_info.spanner_type,
            &col_info.name,
        )?;
    }

    output.set_len(rows.len());
    Ok(())
}
