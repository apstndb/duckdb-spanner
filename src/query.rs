use std::sync::Mutex;

use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use google_cloud_googleapis::spanner::v1::PartitionOptions;
use google_cloud_spanner::row::Row;
use google_cloud_spanner::statement::Statement;
use google_cloud_googleapis::spanner::v1::request_options::Priority;
use google_cloud_spanner::transaction::{CallOptions, QueryOptions};
use tokio::sync::mpsc;

use crate::error::SpannerError;
use crate::schema::ColumnInfo;
use crate::{client, convert, runtime, schema, types};

const BATCH_SIZE: usize = 2048;

#[repr(C)]
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
        let database = bind.get_parameter(0).to_string();
        let sql = bind.get_parameter(1).to_string();

        let endpoint = crate::bind_utils::get_named_string(bind, "endpoint");
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
            schema::discover_query_schema(
                &client,
                &sql,
                params_json.as_deref(),
            )
            .await
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
        let priority = bind_data.priority;

        runtime::spawn(async move {
            let result: Result<(), SpannerError> = async {
                let client =
                    client::get_or_create_client(&database, endpoint.as_deref()).await?;

                if use_parallelism {
                    let partition_options =
                        max_parallelism.map(|max| PartitionOptions {
                            max_partitions: max,
                            ..Default::default()
                        });

                    let stmt =
                        crate::params::create_statement(&sql, params_json.as_deref())?;
                    match stream_partitioned_query(
                        &client,
                        &tx,
                        stmt,
                        timestamp_bound.as_ref(),
                        priority,
                        use_data_boost,
                        partition_options,
                    )
                    .await
                    {
                        Ok(()) => return Ok(()),
                        Err(e) => {
                            eprintln!("[duckdb-spanner] Partitioned query failed: {e}, falling back to single query");
                        }
                    }
                }

                let stmt =
                    crate::params::create_statement(&sql, params_json.as_deref())?;
                stream_single_query(
                    &client,
                    &tx,
                    stmt,
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
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // database
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // sql
        ])
    }

    // Named parameters inspired by BigQuery EXTERNAL_QUERY / CloudSpannerProperties.
    // TODO: Add database_role once gcloud-spanner exposes creator_role in SessionConfig.
    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            (
                "endpoint".to_string(),
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
    client: &google_cloud_spanner::client::Client,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    stmt: Statement,
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

    let query_options = QueryOptions {
        call_options,
        ..Default::default()
    };

    let partitions = batch_tx
        .partition_query_with_option(
            stmt,
            partition_options,
            query_options,
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

async fn stream_single_query(
    client: &google_cloud_spanner::client::Client,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    stmt: Statement,
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

    let query_options = QueryOptions {
        call_options: CallOptions {
            priority,
            ..Default::default()
        },
        ..Default::default()
    };

    let mut iter = spanner_tx
        .query_with_option(stmt, query_options)
        .await
        .map_err(SpannerError::Grpc)?;

    while let Some(row) = iter.next().await.map_err(SpannerError::Grpc)? {
        if tx.send(Ok(row)).await.is_err() {
            return Ok(()); // receiver dropped
        }
    }
    Ok(())
}
