use std::collections::VecDeque;
use std::sync::Mutex;

use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use google_cloud_googleapis::spanner::v1::PartitionOptions;
use google_cloud_spanner::row::Row;
use google_cloud_spanner::statement::Statement;
use google_cloud_googleapis::spanner::v1::request_options::Priority;
use google_cloud_spanner::transaction::{CallOptions, QueryOptions};

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

// TODO: Stream rows incrementally instead of buffering all rows in memory.
// Currently init() fetches all rows into a VecDeque, which can use excessive
// memory for large result sets. Streaming would require holding an async
// iterator across sync func() calls.
pub struct QueryInitData {
    rows: Mutex<VecDeque<Row>>,
}

pub struct SpannerQueryVTab;

impl VTab for SpannerQueryVTab {
    type BindData = QueryBindData;
    type InitData = QueryInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let database = bind.get_parameter(0).to_string();
        let sql = bind.get_parameter(1).to_string();

        let endpoint = crate::bind_utils::get_named_string(bind, "endpoint");
        let use_parallelism = crate::bind_utils::get_named_bool(bind, "use_parallelism", true);
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

        // Parse params JSON once; create_statement is called per-execution path
        // because Statement is not Clone, but the JSON parsing validates early.
        let params_json = bind_data.params_json.as_deref();
        let sql = &bind_data.sql;

        let rows = runtime::block_on(async {
            let client = client::get_or_create_client(&bind_data.database, bind_data.endpoint.as_deref()).await?;

            if bind_data.use_parallelism {
                let partition_options = bind_data.max_parallelism.map(|max| PartitionOptions {
                    max_partitions: max,
                    ..Default::default()
                });

                let stmt = crate::params::create_statement(sql, params_json)?;
                match try_partitioned_query(
                    &client,
                    stmt,
                    bind_data.timestamp_bound.as_ref(),
                    bind_data.priority,
                    bind_data.use_data_boost,
                    partition_options,
                )
                .await
                {
                    Ok(rows) => return Ok(rows),
                    Err(e) => {
                        eprintln!("[duckdb-spanner] Partitioned query failed: {e}, falling back to single query");
                    }
                }
            }

            // Fallback: single query
            let stmt = crate::params::create_statement(sql, params_json)?;
            execute_single_query(&client, stmt, bind_data.timestamp_bound.as_ref(), bind_data.priority).await
        })??;

        // Allow DuckDB to use multiple threads for processing fetched rows
        let thread_count = std::cmp::max(1, rows.len() / BATCH_SIZE);
        let thread_count = std::cmp::min(thread_count, 8);
        init.set_max_threads(thread_count as u64);

        Ok(QueryInitData {
            rows: Mutex::new(rows),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let mut rows_queue = init_data.rows.lock().unwrap_or_else(|e| e.into_inner());

        if rows_queue.is_empty() {
            output.set_len(0);
            return Ok(());
        }

        let batch_size = std::cmp::min(rows_queue.len(), BATCH_SIZE);
        let batch: Vec<_> = rows_queue.drain(..batch_size).collect();
        drop(rows_queue);

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

async fn try_partitioned_query(
    client: &google_cloud_spanner::client::Client,
    stmt: Statement,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
    data_boost_enabled: bool,
    partition_options: Option<PartitionOptions>,
) -> Result<VecDeque<Row>, crate::error::SpannerError> {
    let call_options = CallOptions {
        priority,
        ..Default::default()
    };

    let mut tx = if let Some(tb) = timestamp_bound {
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

    let partitions = tx
        .partition_query_with_option(
            stmt,
            partition_options,
            query_options,
            data_boost_enabled,
            None,
        )
        .await
        .map_err(crate::error::SpannerError::Grpc)?;

    let mut rows = VecDeque::new();
    for partition in partitions {
        let mut iter = tx
            .execute(partition, None)
            .await
            .map_err(crate::error::SpannerError::Grpc)?;
        while let Some(row) = iter.next().await.map_err(crate::error::SpannerError::Grpc)? {
            rows.push_back(row);
        }
    }
    Ok(rows)
}

async fn execute_single_query(
    client: &google_cloud_spanner::client::Client,
    stmt: Statement,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
    priority: Option<Priority>,
) -> Result<VecDeque<Row>, crate::error::SpannerError> {
    let mut tx = if let Some(tb) = timestamp_bound {
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

    let mut iter = tx.query_with_option(stmt, query_options).await.map_err(crate::error::SpannerError::Grpc)?;

    let mut rows = VecDeque::new();
    while let Some(row) = iter.next().await.map_err(crate::error::SpannerError::Grpc)? {
        rows.push_back(row);
    }
    Ok(rows)
}
