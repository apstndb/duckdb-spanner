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
use tokio_util::sync::CancellationToken;

use crate::connection::ConnectionProfile;
use crate::error::SpannerError;
use crate::schema::ColumnInfo;
use crate::{client, convert, runtime, schema, streaming, types, vector_size, vtab_safety};

pub struct QueryBindData {
    profile: ConnectionProfile,
    sql: String,
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
        vtab_safety::guard_callback("SpannerQueryVTab", "bind", || {
            let sql = bind.get_parameter(0).to_string();
            let profile = crate::bind_utils::resolve_connection_profile(bind)?;
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
            let schema_profile = profile.clone();
            let schema_sql = sql.clone();
            let schema_params_json = params_json.clone();
            let columns = runtime::run(async move {
                let client = client::get_or_create_client(&schema_profile).await?;
                schema::discover_query_schema(&client, &schema_sql, schema_params_json.as_deref())
                    .await
            })??;

            // Register output columns with DuckDB
            for (index, col) in columns.iter().enumerate() {
                vtab_safety::validate_result_name(
                    &col.name,
                    &format!("query result column {index}"),
                )?;
                let logical_type =
                    types::spanner_type_to_logical(&col.spanner_type).map_err(|error| {
                        SpannerError::Other(format!(
                            "Invalid metadata for query result column {:?}: {error}",
                            col.name
                        ))
                    })?;
                bind.add_result_column(&col.name, logical_type);
            }

            Ok(QueryBindData {
                profile,
                sql,
                parallelism_mode,
                use_data_boost,
                max_parallelism,
                timestamp_bound,
                priority,
                params_json,
                columns,
            })
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        vtab_safety::guard_callback("SpannerQueryVTab", "init", || {
            let bind_data = unsafe { &*init.get_bind_data::<QueryBindData>() };
            let vector_size = vector_size::runtime_vector_size();

            // Clone bind data fields for the spawned task ('static requirement)
            let profile = bind_data.profile.clone();
            let sql = bind_data.sql.clone();
            let params_json = bind_data.params_json.clone();
            let parallelism_mode = bind_data.parallelism_mode;
            let use_data_boost = bind_data.use_data_boost;
            let max_parallelism = bind_data.max_parallelism;
            let timestamp_bound = bind_data.timestamp_bound.clone();
            let priority = bind_data.priority.clone();

            let streaming = streaming::StreamingState::spawn(
                vector_size,
                move |tx, cancellation| async move {
                    let rows_delivered = Arc::new(AtomicBool::new(false));
                    let client = tokio::select! {
                        _ = cancellation.cancelled() => return Ok(()),
                        client = client::get_or_create_client(&profile) => client?,
                    };

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
                            cancellation.clone(),
                        )
                        .await
                        {
                            Ok(()) => return Ok(()),
                            Err(e)
                                if should_fallback_after_partition_error(
                                    parallelism_mode,
                                    rows_delivered.load(Ordering::SeqCst),
                                    &cancellation,
                                ) =>
                            {
                                eprintln!("[duckdb-spanner] Partitioned query failed: {e}, falling back to single query");
                            }
                            Err(e) => return Err(e),
                        }
                    }

                    if cancellation.is_cancelled() {
                        return Ok(());
                    }
                    let stmt = crate::params::create_statement_with_priority(
                        &sql,
                        params_json.as_deref(),
                        priority.clone(),
                    )?;
                    stream_single_query(&client, &tx, stmt, timestamp_bound.as_ref(), &cancellation)
                        .await
                },
            )?;

            init.set_max_threads(1);

            Ok(QueryInitData { streaming })
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        vtab_safety::guard_callback("SpannerQueryVTab", "func", || {
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
        })
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
            (
                "endpoint_mode".to_string(),
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

#[allow(clippy::too_many_arguments)]
async fn stream_partitioned_query(
    client: &DatabaseClient,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    stmt: Statement,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
    data_boost_enabled: bool,
    max_parallelism: Option<i64>,
    rows_delivered: Arc<AtomicBool>,
    cancellation: CancellationToken,
) -> Result<(), SpannerError> {
    let mut builder = client.batch_read_only_transaction();
    if let Some(tb) = timestamp_bound {
        builder = builder.set_timestamp_bound(tb.to_timestamp_bound());
    }
    let batch_tx = tokio::select! {
        _ = cancellation.cancelled() => return Ok(()),
        batch_tx = builder.build() => batch_tx?,
    };

    let partitions = tokio::select! {
        _ = cancellation.cancelled() => return Ok(()),
        partitions = batch_tx.partition_query(
            stmt,
            max_parallelism
                .map(|max| PartitionOptions::new().set_max_partitions(max))
                .unwrap_or_default(),
        ) => partitions?,
    };

    // Execute partitions concurrently, each with its own session from the pool.
    // Partitions embed the transaction selector, so any session can execute them
    // against the same consistent snapshot. `max_parallelism` is both the
    // partition-count hint above and a local worker cap.
    let permits = runtime::partition_worker_limit(partitions.len(), max_parallelism);
    let tx = tx.clone();
    let client = client.clone();
    runtime::run_bounded_partitions(
        partitions,
        permits,
        cancellation,
        move |partition, cancellation| {
            let tx = tx.clone();
            let client = client.clone();
            let rows_delivered = rows_delivered.clone();
            async move {
                let partition = partition.set_data_boost(data_boost_enabled);
                let Some(iter) =
                    runtime::await_or_cancel(&cancellation, partition.execute(&client)).await
                else {
                    return Ok(());
                };
                let mut iter = iter?;
                loop {
                    let Some(next) = runtime::await_or_cancel(&cancellation, iter.next()).await
                    else {
                        return Ok(());
                    };
                    let Some(row) = next.transpose()? else {
                        break;
                    };
                    if !runtime::send_or_cancel(&cancellation, &tx, Ok(row)).await {
                        return Ok(()); // cancelled or receiver dropped
                    }
                    rows_delivered.store(true, Ordering::SeqCst);
                }
                Ok::<(), SpannerError>(())
            }
        },
    )
    .await

    // Invariant: partition tasks must be fully terminated before this function
    // returns. Client eviction close relies on Arc uniqueness => quiescence, so a
    // task still holding a `Client` clone and `ManagedSession` must not outlive
    // the streamer. The bounded runner drains (aborts + awaits) every task on
    // error before returning; the caller reads `rows_delivered` only after we
    // return, so an aborted straggler can no longer emit a row past the fallback
    // decision. StreamingState cancellation makes the bounded runner shut down
    // and await every remaining partition task before this producer completes.
}

async fn stream_single_query(
    client: &DatabaseClient,
    tx: &mpsc::Sender<Result<Row, SpannerError>>,
    stmt: Statement,
    timestamp_bound: Option<&crate::bind_utils::TimestampBoundConfig>,
    cancellation: &CancellationToken,
) -> Result<(), SpannerError> {
    let mut builder = client.single_use();
    if let Some(tb) = timestamp_bound {
        builder = builder.set_timestamp_bound(tb.to_timestamp_bound());
    }
    if cancellation.is_cancelled() {
        return Ok(());
    }
    let spanner_tx = builder.build();
    let Some(iter) = runtime::await_or_cancel(cancellation, spanner_tx.execute_query(stmt)).await
    else {
        return Ok(());
    };
    let mut iter = iter?;

    loop {
        let Some(next) = runtime::await_or_cancel(cancellation, iter.next()).await else {
            return Ok(());
        };
        let Some(row) = next.transpose()? else {
            break;
        };
        if !runtime::send_or_cancel(cancellation, tx, Ok(row)).await {
            return Ok(()); // cancelled or receiver dropped
        }
    }
    Ok(())
}

fn should_fallback_after_partition_error(
    parallelism_mode: crate::bind_utils::ParallelismMode,
    rows_delivered: bool,
    cancellation: &CancellationToken,
) -> bool {
    !cancellation.is_cancelled() && parallelism_mode.allows_fallback(rows_delivered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_prevents_auto_query_fallback() {
        let cancellation = CancellationToken::new();
        assert!(should_fallback_after_partition_error(
            crate::bind_utils::ParallelismMode::Auto,
            false,
            &cancellation,
        ));

        cancellation.cancel();
        assert!(!should_fallback_after_partition_error(
            crate::bind_utils::ParallelismMode::Auto,
            false,
            &cancellation,
        ));
    }
}
