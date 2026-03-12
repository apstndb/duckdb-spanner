use std::sync::Mutex;

use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use google_cloud_googleapis::spanner::v1::request_options::Priority;
use google_cloud_googleapis::spanner::v1::PartitionOptions;
use google_cloud_spanner::key;
use google_cloud_spanner::row::Row;
use google_cloud_spanner::transaction::{CallOptions, ReadOptions};
use tokio::sync::mpsc;

use google_cloud_googleapis::spanner::admin::database::v1::DatabaseDialect;

use crate::error::SpannerError;
use crate::schema::ColumnInfo;
use crate::{client, convert, runtime, schema, types};

const BATCH_SIZE: usize = 2048;

#[repr(C)]
pub struct ScanBindData {
    database: String,
    table: String,
    endpoint: Option<String>,
    dialect: DatabaseDialect,
    use_parallelism: bool,
    use_data_boost: bool,
    max_parallelism: Option<i64>,
    index: String,
    timestamp_bound: Option<crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
    columns: Vec<ColumnInfo>,
}

/// Streaming init data: rows arrive via an mpsc channel from a background tokio task.
pub struct ScanInitData {
    receiver: Mutex<mpsc::Receiver<Result<Row, SpannerError>>>,
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
        let use_parallelism = crate::bind_utils::get_named_bool(bind, "use_parallelism", false);
        let use_data_boost = crate::bind_utils::get_named_bool(bind, "use_data_boost", false);
        let max_parallelism = crate::bind_utils::get_named_int64(bind, "max_parallelism");
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
            schema::discover_table_schema(&client, &table, dialect).await
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
            dialect,
            use_parallelism,
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

        let (tx, rx) = mpsc::channel(BATCH_SIZE);

        // Clone bind data fields for the spawned task ('static requirement)
        let database = bind_data.database.clone();
        let endpoint = bind_data.endpoint.clone();
        let table = bind_data.table.clone();
        let index = bind_data.index.clone();
        let use_parallelism = bind_data.use_parallelism;
        let use_data_boost = bind_data.use_data_boost;
        let max_parallelism = bind_data.max_parallelism;
        let timestamp_bound = bind_data.timestamp_bound.clone();
        let priority = bind_data.priority;

        runtime::spawn(async move {
            let result: Result<(), SpannerError> = async {
                let client =
                    client::get_or_create_client(&database, endpoint.as_deref()).await?;

                let col_refs: Vec<&str> =
                    column_names.iter().map(|s| s.as_str()).collect();

                if use_parallelism {
                    let partition_options =
                        max_parallelism.map(|max| PartitionOptions {
                            max_partitions: max,
                            ..Default::default()
                        });

                    match stream_partitioned_read(
                        &client,
                        &tx,
                        &table,
                        &col_refs,
                        &index,
                        timestamp_bound.as_ref(),
                        priority,
                        use_data_boost,
                        partition_options,
                    )
                    .await
                    {
                        Ok(()) => return Ok(()),
                        Err(e) => {
                            eprintln!("[duckdb-spanner] Partitioned read failed: {e}, falling back to single read");
                        }
                    }
                }

                stream_single_read(
                    &client,
                    &tx,
                    &table,
                    &col_refs,
                    &index,
                    timestamp_bound.as_ref(),
                    priority,
                )
                .await
            }
            .await;

            if let Err(e) = result {
                let _ = tx.send(Err(e)).await;
            }
        })?;

        init.set_max_threads(1);

        Ok(ScanInitData {
            receiver: Mutex::new(rx),
            projected_columns,
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

        // Write projected columns using the unified conversion.
        // Spanner Read API returns columns in the order we requested (projected order),
        // so spanner_col_idx = i and output_col_idx = i.
        write_projected_rows(output, &batch, &bind_data.columns, &init_data.projected_columns)?;
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // table
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
    client: &google_cloud_spanner::client::Client,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    table: &str,
    column_names: &[&str],
    index: &str,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
    data_boost_enabled: bool,
    partition_options: Option<PartitionOptions>,
) -> Result<(), SpannerError> {
    let call_options = CallOptions {
        priority,
        ..Default::default()
    };

    let mut batch_tx = if let Some(tb) = timestamp_bound {
        client
            .batch_read_only_transaction_with_option(
                google_cloud_spanner::client::ReadOnlyTransactionOption {
                    timestamp_bound: tb.to_timestamp_bound(),
                    call_options: call_options.clone(),
                },
            )
            .await?
    } else {
        client.batch_read_only_transaction().await?
    };

    let read_options = ReadOptions {
        index: index.to_string(),
        call_options,
        ..Default::default()
    };

    let partitions = batch_tx
        .partition_read_with_option(
            table,
            column_names,
            key::all_keys(),
            partition_options,
            read_options,
            data_boost_enabled,
            None,
        )
        .await
        .map_err(SpannerError::Grpc)?;

    for partition in partitions {
        let mut iter = batch_tx
            .execute(partition, None)
            .await
            .map_err(SpannerError::Grpc)?;
        while let Some(row) = iter.next().await.map_err(SpannerError::Grpc)? {
            if tx.send(Ok(row)).await.is_err() {
                return Ok(()); // receiver dropped
            }
        }
    }
    Ok(())
}

async fn stream_single_read(
    client: &google_cloud_spanner::client::Client,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    table: &str,
    column_names: &[&str],
    index: &str,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
) -> Result<(), SpannerError> {
    let mut spanner_tx = if let Some(tb) = timestamp_bound {
        client
            .single_with_timestamp_bound(tb.to_timestamp_bound())
            .await?
    } else {
        client.single().await?
    };

    let read_options = ReadOptions {
        index: index.to_string(),
        call_options: CallOptions {
            priority,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut iter = spanner_tx
        .read_with_option(table, column_names, key::all_keys(), read_options)
        .await
        .map_err(SpannerError::Grpc)?;

    while let Some(row) = iter.next().await.map_err(SpannerError::Grpc)? {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(()); // receiver dropped
        }
    }
    Ok(())
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
        convert::write_column_from_rows(output, i, rows, i, &col_info.spanner_type)?;
    }

    output.set_len(rows.len());
    Ok(())
}
