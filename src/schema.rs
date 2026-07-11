use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};

use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::execute_sql_request::QueryMode;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::types::Type;
use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;
use tokio::sync::Notify;

use crate::error::SpannerError;
use crate::types;

/// Keep the dialect cache aligned with the client cache in `client.rs`.
const DIALECT_CACHE_CAPACITY: usize = 8;
/// Hard cap on distinct concurrent discovery waves. Overload deliberately
/// fails closed immediately instead of adding potentially indefinite bind-time
/// backpressure; callers can retry after any active wave completes.
const DIALECT_IN_FLIGHT_CAPACITY: usize = 8;
const DIALECT_LEADER_DROPPED_ERROR: &str =
    "Dialect discovery leader was cancelled or panicked before completing";

/// A completed dialect LRU and an independent bounded set of active discovery
/// waves. Completed entries may be evicted; active waves must never be evicted
/// because doing so would allow a second discovery for the same key.
struct DialectCache {
    completed: CompletedDialects,
    in_flight: HashMap<String, Arc<DialectFlight>>,
    in_flight_capacity: usize,
}

impl DialectCache {
    fn new(completed_capacity: usize, in_flight_capacity: usize) -> Self {
        assert!(
            completed_capacity >= 1,
            "dialect cache capacity must be at least 1"
        );
        assert!(
            in_flight_capacity >= 1,
            "dialect in-flight capacity must be at least 1"
        );
        Self {
            completed: CompletedDialects::new(completed_capacity),
            in_flight: HashMap::new(),
            in_flight_capacity,
        }
    }
}

/// A minimal LRU specialized for completed dialects. This is deliberately
/// separate from active waves, which have different eviction rules.
struct CompletedDialects {
    capacity: usize,
    tick: u64,
    entries: HashMap<String, (DatabaseDialect, u64)>,
}

impl CompletedDialects {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            tick: 0,
            entries: HashMap::new(),
        }
    }

    fn next_tick(&mut self) -> u64 {
        self.tick = self.tick.wrapping_add(1);
        self.tick
    }

    fn get(&mut self, key: &str) -> Option<DatabaseDialect> {
        let tick = self.next_tick();
        self.entries.get_mut(key).map(|(dialect, recency)| {
            *recency = tick;
            dialect.clone()
        })
    }

    fn insert(&mut self, key: String, dialect: DatabaseDialect) {
        let tick = self.next_tick();
        self.entries.insert(key, (dialect, tick));
        if self.entries.len() > self.capacity {
            let lru_key = self
                .entries
                .iter()
                .min_by_key(|(_, (_, recency))| *recency)
                .map(|(key, _)| key.clone());
            if let Some(key) = lru_key {
                self.entries.remove(&key);
            }
        }
    }
}

#[derive(Clone)]
enum DialectOutcome {
    Success(DatabaseDialect),
    Failure(DialectFailure),
}

impl DialectOutcome {
    fn into_result(self) -> Result<DatabaseDialect, SpannerError> {
        match self {
            Self::Success(dialect) => Ok(dialect),
            Self::Failure(error) => Err(error.into_spanner_error()),
        }
    }
}

/// Store failures in a cloneable form so every member of a wave sees the same
/// typed GAX error, including non-status predicates, metadata, and source chain.
#[derive(Clone)]
enum DialectFailure {
    GoogleCloud(Arc<google_cloud_gax::error::Error>),
    Other(String),
    LeaderDropped,
}

impl DialectFailure {
    fn from_spanner_error(error: SpannerError) -> Self {
        match error {
            SpannerError::GoogleCloud(error) => Self::GoogleCloud(error),
            error => Self::Other(error.to_string()),
        }
    }

    fn into_spanner_error(self) -> SpannerError {
        match self {
            Self::GoogleCloud(error) => SpannerError::GoogleCloud(error),
            Self::Other(message) => SpannerError::Other(message),
            Self::LeaderDropped => SpannerError::Other(DIALECT_LEADER_DROPPED_ERROR.to_string()),
        }
    }
}

/// Shared state for exactly one discovery wave.
struct DialectFlight {
    outcome: Mutex<Option<DialectOutcome>>,
    completed: Notify,
    #[cfg(test)]
    followers: std::sync::atomic::AtomicUsize,
}

impl DialectFlight {
    fn new() -> Self {
        Self {
            outcome: Mutex::new(None),
            completed: Notify::new(),
            #[cfg(test)]
            followers: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    fn record_follower(&self) {
        self.followers
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Publish the first terminal outcome and wake the whole wave. Idempotence
    /// matters during unwinding: if normal completion published before a later
    /// operation panicked, the leader guard must not overwrite that outcome.
    fn publish(&self, outcome: DialectOutcome) {
        let mut current = self.outcome.lock().unwrap_or_else(|e| e.into_inner());
        if current.is_some() {
            return;
        }
        *current = Some(outcome);
        drop(current);
        self.completed.notify_waiters();
    }

    async fn wait(&self) -> DialectOutcome {
        loop {
            // Register before inspecting the result so a completion between
            // these two operations cannot lose its wake-up.
            let notified = self.completed.notified();
            if let Some(outcome) = self
                .outcome
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                return outcome;
            }
            notified.await;
        }
    }
}

/// Owns cleanup responsibility while the discovery leader is awaiting I/O.
/// Dropping the leader future (including task abort or panic unwinding) runs
/// this synchronously, so no active wave can be stranded without an outcome.
struct DialectLeaderGuard<'a> {
    cache: &'a Mutex<DialectCache>,
    key: String,
    flight: Arc<DialectFlight>,
    armed: bool,
}

impl<'a> DialectLeaderGuard<'a> {
    fn new(cache: &'a Mutex<DialectCache>, key: String, flight: Arc<DialectFlight>) -> Self {
        Self {
            cache,
            key,
            flight,
            armed: true,
        }
    }

    fn complete(mut self, outcome: DialectOutcome) -> Result<DatabaseDialect, SpannerError> {
        self.flight.publish(outcome.clone());

        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if let DialectOutcome::Success(dialect) = &outcome {
            cache.completed.insert(self.key.clone(), dialect.clone());
        }
        remove_matching_flight(&mut cache, &self.key, &self.flight);

        self.armed = false;
        outcome.into_result()
    }
}

impl Drop for DialectLeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        self.flight
            .publish(DialectOutcome::Failure(DialectFailure::LeaderDropped));

        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        remove_matching_flight(&mut cache, &self.key, &self.flight);
    }
}

fn remove_matching_flight(cache: &mut DialectCache, key: &str, flight: &Arc<DialectFlight>) {
    if cache
        .in_flight
        .get(key)
        .is_some_and(|current| Arc::ptr_eq(current, flight))
    {
        cache.in_flight.remove(key);
    }
}

/// Cache of detected dialects, keyed by the same `(database, endpoint)` key
/// used by the client cache in `client.rs` (see `client::cache_key`).
///
/// `detect_dialect` issues a query, so callers that run it on every bind
/// (`spanner_scan`, `COPY TO ... (FORMAT spanner)`) look here first to avoid
/// a redundant round-trip against the same database. Concurrent misses share
/// one discovery wave. Failed waves are not cached, so a later bind retries.
static DIALECT_CACHE: LazyLock<Mutex<DialectCache>> = LazyLock::new(|| {
    Mutex::new(DialectCache::new(
        DIALECT_CACHE_CAPACITY,
        DIALECT_IN_FLIGHT_CAPACITY,
    ))
});

/// Column metadata discovered from Spanner schema.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub spanner_type: Type,
    /// True when the column is generated (INFORMATION_SCHEMA.IS_GENERATED != 'NEVER').
    /// Generated columns are computed by Spanner and reject writes, so COPY TO must
    /// exclude them from the target column list.
    pub is_generated: bool,
}

/// Parse a dialect string from user input.
pub fn parse_dialect(s: &str) -> Result<DatabaseDialect, Box<dyn std::error::Error>> {
    match s.to_ascii_lowercase().as_str() {
        "googlesql" => Ok(DatabaseDialect::GoogleStandardSql),
        "postgresql" => Ok(DatabaseDialect::Postgresql),
        _ => Err(format!("Invalid dialect '{s}': must be 'googlesql' or 'postgresql'").into()),
    }
}

/// Detect the database dialect.
///
/// Primary: query `INFORMATION_SCHEMA.DATABASE_OPTIONS` for the
/// `database_dialect` option, whose value is `GOOGLE_STANDARD_SQL` or
/// `POSTGRESQL`. The query uses only uppercase, unquoted identifiers, which
/// resolve correctly in both dialects (PostgreSQL-dialect Spanner databases
/// fold unquoted identifiers to lowercase, matching the lowercase
/// `information_schema` views), so a single query form works regardless of
/// the (as yet unknown) dialect.
///
/// If `DATABASE_OPTIONS` cannot be queried, this preserves the original error.
/// In particular, it does not infer a dialect from a `public` schema: a
/// GoogleSQL database can have a user-created schema with that name. Callers
/// targeting an endpoint without `DATABASE_OPTIONS` support must use an
/// explicit dialect where that option is available. Other callers return the
/// original discovery error instead of guessing.
///
/// Results are cached per `(database, endpoint)` key (see `client::cache_key`)
/// since callers such as `spanner_scan` and `COPY TO ... (FORMAT spanner)`
/// invoke this on every bind.
pub async fn detect_dialect(
    client: &DatabaseClient,
    database: &str,
    endpoint: Option<&str>,
) -> Result<DatabaseDialect, SpannerError> {
    let key = crate::client::cache_key(database, endpoint);
    get_or_detect_dialect(&DIALECT_CACHE, key, || {
        detect_dialect_via_database_options(client)
    })
    .await
}

/// Run dialect discovery once for the current wave. Successes are cached in
/// the completed LRU; failures are broadcast to all current waiters and are
/// retried only by a subsequent wave.
async fn get_or_detect_dialect<F, Fut>(
    cache: &Mutex<DialectCache>,
    key: String,
    detect: F,
) -> Result<DatabaseDialect, SpannerError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<DatabaseDialect, SpannerError>>,
{
    let (flight, leader) = {
        let mut cache = cache.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(dialect) = cache.completed.get(&key) {
            return Ok(dialect);
        }
        if let Some(flight) = cache.in_flight.get(&key) {
            #[cfg(test)]
            flight.record_follower();
            (Arc::clone(flight), false)
        } else {
            if cache.in_flight.len() >= cache.in_flight_capacity {
                return Err(SpannerError::Other(format!(
                    "Dialect discovery overload: {} distinct database detections are already in progress",
                    cache.in_flight_capacity
                )));
            }
            let flight = Arc::new(DialectFlight::new());
            cache.in_flight.insert(key.clone(), Arc::clone(&flight));
            (flight, true)
        }
    };

    if !leader {
        return flight.wait().await.into_result();
    }

    let guard = DialectLeaderGuard::new(cache, key, flight);
    let outcome = match detect().await {
        Ok(dialect) => DialectOutcome::Success(dialect),
        Err(error) => DialectOutcome::Failure(DialectFailure::from_spanner_error(error)),
    };

    guard.complete(outcome)
}

/// Resolve an explicit dialect without invoking detection. The detector is a
/// closure so callers and tests can prove that overrides bypass metadata I/O.
pub(crate) async fn resolve_dialect<F, Fut>(
    dialect: DatabaseDialect,
    detect: F,
) -> Result<DatabaseDialect, SpannerError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<DatabaseDialect, SpannerError>>,
{
    if dialect == DatabaseDialect::Unspecified {
        detect().await
    } else {
        Ok(dialect)
    }
}

/// Query `INFORMATION_SCHEMA.DATABASE_OPTIONS` for the `database_dialect` option.
async fn detect_dialect_via_database_options(
    client: &DatabaseClient,
) -> Result<DatabaseDialect, SpannerError> {
    let stmt = Statement::builder(
        "SELECT OPTION_VALUE FROM INFORMATION_SCHEMA.DATABASE_OPTIONS \
         WHERE OPTION_NAME = 'database_dialect'",
    )
    .build();
    let tx = client.single_use().build();
    let mut iter = tx.execute_query(stmt).await?;
    match iter.next().await.transpose()? {
        Some(row) => {
            let value: String = row.try_get(0)?;
            parse_database_dialect_option(&value)
        }
        None => Err(SpannerError::Other(
            "No database_dialect row in INFORMATION_SCHEMA.DATABASE_OPTIONS".to_string(),
        )),
    }
}

/// Parse the `OPTION_VALUE` returned for the `database_dialect` row of
/// `INFORMATION_SCHEMA.DATABASE_OPTIONS`.
fn parse_database_dialect_option(value: &str) -> Result<DatabaseDialect, SpannerError> {
    let normalized = value
        .trim()
        .trim_matches(|c| c == '\'' || c == '"')
        .to_ascii_uppercase();
    match normalized.as_str() {
        "POSTGRESQL" => Ok(DatabaseDialect::Postgresql),
        "GOOGLE_STANDARD_SQL" => Ok(DatabaseDialect::GoogleStandardSql),
        other => Err(SpannerError::Other(format!(
            "Unrecognized database_dialect option value: {other}"
        ))),
    }
}

/// Discover the output schema of a SQL query using Plan mode.
pub async fn discover_query_schema(
    client: &DatabaseClient,
    sql: &str,
    params_json: Option<&str>,
) -> Result<Vec<ColumnInfo>, SpannerError> {
    let tx = client.single_use().build();
    let stmt = crate::params::create_statement(sql, params_json)?.set_query_mode(QueryMode::Plan);
    let mut iter = tx.execute_query(stmt).await?;

    // Consume the iterator to ensure metadata is fully populated
    while let Some(_row) = iter.next().await.transpose()? {}

    let metadata = iter.metadata().ok_or_else(|| {
        SpannerError::Other(format!("No column metadata returned for query: {sql}"))
    })?;
    if metadata.column_names().is_empty() {
        return Err(SpannerError::Other(format!(
            "No column metadata returned for query: {sql}"
        )));
    }

    let columns = metadata
        .column_names()
        .iter()
        .zip(metadata.column_types().iter())
        .map(|(name, spanner_type)| ColumnInfo {
            name: name.clone(),
            spanner_type: spanner_type.clone(),
            // Query result columns are never "generated" in the write sense.
            is_generated: false,
        })
        .collect();

    Ok(columns)
}

/// Discover table schema from INFORMATION_SCHEMA.COLUMNS.
///
/// If `dialect` is `Unspecified`, auto-detects via `detect_dialect` (one extra
/// round-trip, cached per `(database, endpoint)`). Specify `GoogleStandardSql`
/// or `Postgresql` to skip detection, including for endpoints that do not
/// support `INFORMATION_SCHEMA.DATABASE_OPTIONS`.
///
/// The `table` parameter can be:
/// - `"MyTable"` — resolves to the default schema ('' for GoogleSQL, 'public' for PG)
/// - `"myschema.MyTable"` — resolves to named schema `myschema`
pub async fn discover_table_schema(
    client: &DatabaseClient,
    table: &str,
    dialect: DatabaseDialect,
    database: &str,
    endpoint: Option<&str>,
) -> Result<Vec<ColumnInfo>, SpannerError> {
    let dialect = resolve_dialect(dialect, || detect_dialect(client, database, endpoint)).await?;

    let (schema_name, table_name) = split_schema_table(table);

    let stmt = build_columns_query(dialect.clone(), schema_name, table_name);
    let tx = client.single_use().build();
    let mut iter = tx.execute_query(stmt).await?;

    let mut columns = Vec::new();
    while let Some(row) = iter.next().await.transpose()? {
        let _table_schema: String = row.try_get(0)?;
        let name: String = row.try_get(1)?;
        let spanner_type_str: String = row.try_get(2)?;
        let is_generated_str: String = row.try_get(3)?;

        // Use the detected database dialect, not TABLE_SCHEMA (PG tables in named
        // schemas such as myschema.Users still use PostgreSQL type strings).
        let spanner_type = match dialect {
            DatabaseDialect::Postgresql => types::parse_pg_spanner_type(&spanner_type_str),
            _ => types::parse_spanner_type(&spanner_type_str),
        };

        // INFORMATION_SCHEMA.IS_GENERATED is 'NEVER' for ordinary columns and
        // 'ALWAYS' for generated columns in both the GoogleSQL and PostgreSQL
        // dialects. Anything other than 'NEVER' is treated as generated.
        let is_generated = !is_generated_str.eq_ignore_ascii_case("NEVER");

        columns.push(ColumnInfo {
            name,
            spanner_type,
            is_generated,
        });
    }

    if columns.is_empty() {
        let msg = if schema_name.is_empty() {
            let default_schema = default_schema_for_dialect(dialect);
            format!(
                "Table {table_name} not found in INFORMATION_SCHEMA (searched default schema {default_schema:?})"
            )
        } else {
            format!("Table {table_name} in schema {schema_name} not found in INFORMATION_SCHEMA")
        };
        return Err(SpannerError::Other(msg));
    }

    Ok(columns)
}

/// Build an INFORMATION_SCHEMA.COLUMNS query with dialect-appropriate parameter syntax.
///
/// GoogleSQL uses `@param`; PostgreSQL uses `$N` with param names `pN`.
fn build_columns_query(dialect: DatabaseDialect, schema_name: &str, table_name: &str) -> Statement {
    let (sql, schema_param, table_param) = columns_query_template(dialect.clone());
    let schema_value = schema_value_for_table(dialect, schema_name).to_string();
    Statement::builder(sql)
        .add_param(schema_param, &schema_value)
        .add_param(table_param, &table_name.to_string())
        .build()
}

fn columns_query_template(dialect: DatabaseDialect) -> (&'static str, &'static str, &'static str) {
    match dialect {
        DatabaseDialect::Postgresql => (
            "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE, IS_GENERATED \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = $1 AND TABLE_NAME = $2 \
             ORDER BY ORDINAL_POSITION",
            "p1",
            "p2",
        ),
        _ => (
            "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE, IS_GENERATED \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table \
             ORDER BY ORDINAL_POSITION",
            "schema",
            "table",
        ),
    }
}

fn schema_value_for_table(dialect: DatabaseDialect, schema_name: &str) -> &str {
    if schema_name.is_empty() {
        default_schema_for_dialect(dialect)
    } else {
        schema_name
    }
}

fn default_schema_for_dialect(dialect: DatabaseDialect) -> &'static str {
    match dialect {
        DatabaseDialect::Postgresql => "public",
        _ => "",
    }
}

/// Split a potentially schema-qualified table name into (schema, table).
///
/// - `"MyTable"` → `("", "MyTable")` (default schema)
/// - `"myschema.MyTable"` → `("myschema", "MyTable")`
fn split_schema_table(qualified_name: &str) -> (&str, &str) {
    match qualified_name.rsplit_once('.') {
        Some((schema, table)) => (schema, table),
        None => ("", qualified_name),
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_gax::error::Error as GaxError;

    use super::*;

    const CONCURRENCY_TEST_TIMEOUT: Duration = Duration::from_secs(2);

    fn rpc_error(code: Code) -> SpannerError {
        SpannerError::from(GaxError::service(
            Status::default().set_code(code).set_message("test"),
        ))
    }

    fn status_code(error: &SpannerError) -> Option<Code> {
        match error {
            SpannerError::GoogleCloud(error) => error.status().map(|status| status.code),
            _ => None,
        }
    }

    #[test]
    fn parse_database_dialect_option_recognizes_known_values() {
        assert_eq!(
            parse_database_dialect_option("GOOGLE_STANDARD_SQL").unwrap(),
            DatabaseDialect::GoogleStandardSql
        );
        assert_eq!(
            parse_database_dialect_option("POSTGRESQL").unwrap(),
            DatabaseDialect::Postgresql
        );
    }

    #[test]
    fn parse_database_dialect_option_normalizes_case_quotes_and_whitespace() {
        assert_eq!(
            parse_database_dialect_option("  'postgresql'  ").unwrap(),
            DatabaseDialect::Postgresql
        );
        assert_eq!(
            parse_database_dialect_option("\"google_standard_sql\"").unwrap(),
            DatabaseDialect::GoogleStandardSql
        );
    }

    #[test]
    fn parse_database_dialect_option_rejects_unknown_values() {
        assert!(parse_database_dialect_option("").is_err());
        assert!(parse_database_dialect_option("something_else").is_err());
    }

    #[test]
    fn parse_dialect_accepts_case_insensitive_names() {
        assert_eq!(
            parse_dialect("GoogleSQL").unwrap(),
            DatabaseDialect::GoogleStandardSql
        );
        assert_eq!(
            parse_dialect("PostgreSQL").unwrap(),
            DatabaseDialect::Postgresql
        );
        assert!(parse_dialect("mysql").is_err());
    }

    #[tokio::test]
    async fn explicit_dialect_bypasses_failing_detector() {
        let detector_calls = AtomicUsize::new(0);
        let dialect = resolve_dialect(DatabaseDialect::Postgresql, || async {
            detector_calls.fetch_add(1, Ordering::SeqCst);
            Err(SpannerError::Other("detector must be bypassed".to_string()))
        })
        .await
        .unwrap();

        assert_eq!(dialect, DatabaseDialect::Postgresql);
        assert_eq!(detector_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn split_schema_table_handles_qualified_and_bare_names() {
        assert_eq!(split_schema_table("MyTable"), ("", "MyTable"));
        assert_eq!(
            split_schema_table("myschema.MyTable"),
            ("myschema", "MyTable")
        );
    }

    #[test]
    fn build_columns_query_uses_dialect_default_schema_for_bare_names() {
        let (gsql_sql, gsql_schema_param, gsql_table_param) =
            columns_query_template(DatabaseDialect::GoogleStandardSql);
        assert!(gsql_sql.contains("TABLE_SCHEMA = @schema"));
        assert!(!gsql_sql.contains("TABLE_SCHEMA IN"));
        assert_eq!(gsql_schema_param, "schema");
        assert_eq!(gsql_table_param, "table");
        assert_eq!(
            schema_value_for_table(DatabaseDialect::GoogleStandardSql, ""),
            ""
        );

        let (pg_sql, pg_schema_param, pg_table_param) =
            columns_query_template(DatabaseDialect::Postgresql);
        assert!(pg_sql.contains("TABLE_SCHEMA = $1"));
        assert!(!pg_sql.contains("TABLE_SCHEMA IN"));
        assert_eq!(pg_schema_param, "p1");
        assert_eq!(pg_table_param, "p2");
        assert_eq!(
            schema_value_for_table(DatabaseDialect::Postgresql, ""),
            "public"
        );
    }

    #[test]
    fn build_columns_query_keeps_qualified_schema_exact() {
        assert_eq!(
            schema_value_for_table(DatabaseDialect::GoogleStandardSql, "public"),
            "public"
        );
        assert_eq!(
            schema_value_for_table(DatabaseDialect::Postgresql, "custom"),
            "custom"
        );
    }

    #[test]
    fn dialect_cache_key_matches_client_cache() {
        // Sanity check that the same (database, endpoint) pair used to key
        // the client cache also keys the dialect cache, so a cached client
        // and its cached dialect stay associated.
        let key_a = crate::client::cache_key("db1", Some("localhost:9010"));
        let key_b = crate::client::cache_key("db1", Some("localhost:9010"));
        assert_eq!(key_a, key_b);

        let key_c = crate::client::cache_key("db2", Some("localhost:9010"));
        assert_ne!(key_a, key_c);
    }

    #[tokio::test]
    async fn completed_dialect_cache_reuses_postgresql_discovery() {
        let cache = Mutex::new(DialectCache::new(2, 2));
        let primary_calls = AtomicUsize::new(0);
        let cached_calls = AtomicUsize::new(0);

        let dialect = get_or_detect_dialect(&cache, "database".to_string(), || async {
            primary_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DatabaseDialect::Postgresql)
        })
        .await
        .unwrap();
        assert_eq!(dialect, DatabaseDialect::Postgresql);

        let cached = get_or_detect_dialect(&cache, "database".to_string(), || async {
            cached_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DatabaseDialect::GoogleStandardSql)
        })
        .await
        .unwrap();
        assert_eq!(cached, DatabaseDialect::Postgresql);
        assert_eq!(primary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(cached_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrent_success_wave_runs_one_discovery() {
        const WAITERS: usize = 4;
        let cache = Arc::new(Mutex::new(DialectCache::new(2, 2)));
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let mut tasks = tokio::task::JoinSet::new();

        for _ in 0..WAITERS {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let release = Arc::clone(&release);
            tasks.spawn(async move {
                get_or_detect_dialect(&cache, "database".to_string(), || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Ok(DatabaseDialect::GoogleStandardSql)
                })
                .await
            });
        }

        wait_for_count(&calls, 1).await;
        let flight = active_flight(&cache, "database");
        wait_for_followers(&flight, WAITERS - 1).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.notify_waiters();

        while let Some(result) = join_next_bounded(&mut tasks).await {
            assert_eq!(result.unwrap().unwrap(), DatabaseDialect::GoogleStandardSql);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_failure_wave_shares_one_error_and_next_wave_retries() {
        const WAITERS: usize = 4;
        let cache = Arc::new(Mutex::new(DialectCache::new(2, 2)));
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let mut tasks = tokio::task::JoinSet::new();

        for _ in 0..WAITERS {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let release = Arc::clone(&release);
            tasks.spawn(async move {
                get_or_detect_dialect(&cache, "database".to_string(), || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Err::<DatabaseDialect, SpannerError>(rpc_error(Code::Unavailable))
                })
                .await
            });
        }

        wait_for_count(&calls, 1).await;
        let flight = active_flight(&cache, "database");
        wait_for_followers(&flight, WAITERS - 1).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.notify_waiters();

        let mut shared_error: Option<Arc<GaxError>> = None;
        while let Some(result) = join_next_bounded(&mut tasks).await {
            let error = result.unwrap().unwrap_err();
            assert_eq!(status_code(&error), Some(Code::Unavailable));
            let error = into_gax_error(error);
            if let Some(shared) = &shared_error {
                assert!(Arc::ptr_eq(shared, &error));
            } else {
                shared_error = Some(error);
            }
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(shared_error.is_some());

        let retried = get_or_detect_dialect(&cache, "database".to_string(), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(DatabaseDialect::Postgresql)
        })
        .await
        .unwrap();
        assert_eq!(retried, DatabaseDialect::Postgresql);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn service_error_wave_preserves_status_http_metadata_and_identity() {
        let status = Status::default()
            .set_code(Code::Unavailable)
            .set_message("service unavailable");
        let (leader, follower) = shared_gax_failure_wave(GaxError::service_with_http_metadata(
            status.clone(),
            Some(503),
            None,
        ))
        .await;

        assert!(Arc::ptr_eq(&leader, &follower));
        assert_eq!(leader.status(), Some(&status));
        assert_eq!(leader.http_status_code(), Some(503));
    }

    #[tokio::test]
    async fn transport_error_wave_preserves_http_metadata_and_identity() {
        let (leader, follower) =
            shared_gax_failure_wave(GaxError::http(502, Default::default(), Default::default()))
                .await;

        assert!(Arc::ptr_eq(&leader, &follower));
        assert!(leader.is_transport());
        assert_eq!(leader.http_status_code(), Some(502));
        assert!(leader.http_headers().is_some());
        assert!(leader.http_payload().is_some());
    }

    #[tokio::test]
    async fn timeout_and_exhausted_error_waves_preserve_predicates_sources_and_identity() {
        let (timeout_leader, timeout_follower) = shared_gax_failure_wave(GaxError::timeout(
            std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout source"),
        ))
        .await;
        assert!(Arc::ptr_eq(&timeout_leader, &timeout_follower));
        assert!(timeout_leader.is_timeout());
        assert!(timeout_leader.status().is_none());
        assert_eq!(
            timeout_leader.as_ref().source().unwrap().to_string(),
            "timeout source"
        );

        let (exhausted_leader, exhausted_follower) = shared_gax_failure_wave(GaxError::exhausted(
            std::io::Error::other("exhausted source"),
        ))
        .await;
        assert!(Arc::ptr_eq(&exhausted_leader, &exhausted_follower));
        assert!(exhausted_leader.is_exhausted());
        assert!(exhausted_leader.status().is_none());
        assert_eq!(
            exhausted_leader.as_ref().source().unwrap().to_string(),
            "exhausted source"
        );
    }

    #[tokio::test]
    async fn cancelled_leader_releases_waiters_and_allows_retry() {
        let cache = Arc::new(Mutex::new(DialectCache::new(2, 2)));
        let calls = Arc::new(AtomicUsize::new(0));
        let leader_release = Arc::new(tokio::sync::Notify::new());

        let leader = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let leader_release = Arc::clone(&leader_release);
            tokio::spawn(async move {
                get_or_detect_dialect(&cache, "database".to_string(), || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    leader_release.notified().await;
                    Ok(DatabaseDialect::GoogleStandardSql)
                })
                .await
            })
        };
        wait_for_count(&calls, 1).await;
        let flight = active_flight(&cache, "database");

        let follower = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                get_or_detect_dialect(&cache, "database".to_string(), || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(DatabaseDialect::Postgresql)
                })
                .await
            })
        };
        wait_for_followers(&flight, 1).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        leader.abort();
        assert!(join_bounded(leader).await.unwrap_err().is_cancelled());

        let follower_error = join_bounded(follower)
            .await
            .expect("follower task must not panic")
            .unwrap_err();
        assert_eq!(follower_error.to_string(), DIALECT_LEADER_DROPPED_ERROR);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.lock().unwrap().in_flight.is_empty());

        let retried = get_or_detect_dialect(&cache, "database".to_string(), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(DatabaseDialect::Postgresql)
        })
        .await
        .unwrap();
        assert_eq!(retried, DatabaseDialect::Postgresql);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let cache = cache.lock().unwrap();
        assert!(cache.in_flight.is_empty());
        assert!(cache.completed.entries.contains_key("database"));
    }

    #[tokio::test]
    async fn panicked_leader_releases_waiters() {
        let cache = Arc::new(Mutex::new(DialectCache::new(2, 2)));
        let calls = Arc::new(AtomicUsize::new(0));
        let panic_release = Arc::new(tokio::sync::Notify::new());

        let leader = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let panic_release = Arc::clone(&panic_release);
            tokio::spawn(async move {
                get_or_detect_dialect(&cache, "database".to_string(), || {
                    panic_after_release(calls, panic_release)
                })
                .await
            })
        };
        wait_for_count(&calls, 1).await;
        let flight = active_flight(&cache, "database");

        let follower = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                get_or_detect_dialect(&cache, "database".to_string(), || async {
                    Ok(DatabaseDialect::Postgresql)
                })
                .await
            })
        };
        wait_for_followers(&flight, 1).await;

        panic_release.notify_waiters();
        assert!(join_bounded(leader).await.unwrap_err().is_panic());

        let follower_error = join_bounded(follower)
            .await
            .expect("follower task must not panic")
            .unwrap_err();
        assert_eq!(follower_error.to_string(), DIALECT_LEADER_DROPPED_ERROR);
        assert!(cache.lock().unwrap().in_flight.is_empty());
    }

    #[test]
    fn stale_leader_guard_does_not_remove_replacement_generation() {
        let cache = Mutex::new(DialectCache::new(2, 2));
        let old_flight = Arc::new(DialectFlight::new());
        cache
            .lock()
            .unwrap()
            .in_flight
            .insert("database".to_string(), Arc::clone(&old_flight));
        let old_guard =
            DialectLeaderGuard::new(&cache, "database".to_string(), Arc::clone(&old_flight));

        let replacement = Arc::new(DialectFlight::new());
        cache
            .lock()
            .unwrap()
            .in_flight
            .insert("database".to_string(), Arc::clone(&replacement));
        drop(old_guard);

        let cache = cache.lock().unwrap();
        let current = cache.in_flight.get("database").unwrap();
        assert!(Arc::ptr_eq(current, &replacement));
        assert!(matches!(
            old_flight.outcome.lock().unwrap().as_ref(),
            Some(DialectOutcome::Failure(DialectFailure::LeaderDropped))
        ));
    }

    #[tokio::test]
    async fn in_flight_capacity_does_not_evict_active_waves_or_duplicate_detection() {
        let cache = Arc::new(Mutex::new(DialectCache::new(2, 2)));
        let calls = Arc::new(AtomicUsize::new(0));
        let release_a = Arc::new(tokio::sync::Notify::new());
        let release_b = Arc::new(tokio::sync::Notify::new());

        let first_a = start_blocked_discovery(
            Arc::clone(&cache),
            "a",
            Arc::clone(&calls),
            Arc::clone(&release_a),
        );
        let first_b = start_blocked_discovery(
            Arc::clone(&cache),
            "b",
            Arc::clone(&calls),
            Arc::clone(&release_b),
        );
        wait_for_count(&calls, 2).await;
        let flight_a = active_flight(&cache, "a");

        let duplicate_a = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                get_or_detect_dialect(&cache, "a".to_string(), || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(DatabaseDialect::GoogleStandardSql)
                })
                .await
            })
        };
        wait_for_followers(&flight_a, 1).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let full = get_or_detect_dialect(&cache, "c".to_string(), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(DatabaseDialect::GoogleStandardSql)
        })
        .await
        .unwrap_err();
        assert_eq!(
            full.to_string(),
            "Dialect discovery overload: 2 distinct database detections are already in progress"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        release_a.notify_waiters();
        assert_eq!(
            join_bounded(first_a).await.unwrap().unwrap(),
            DatabaseDialect::GoogleStandardSql
        );
        assert_eq!(
            join_bounded(duplicate_a).await.unwrap().unwrap(),
            DatabaseDialect::GoogleStandardSql
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let detected_c = get_or_detect_dialect(&cache, "c".to_string(), || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(DatabaseDialect::Postgresql)
        })
        .await
        .unwrap();
        assert_eq!(detected_c, DatabaseDialect::Postgresql);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        release_b.notify_waiters();
        assert_eq!(
            join_bounded(first_b).await.unwrap().unwrap(),
            DatabaseDialect::GoogleStandardSql
        );

        let cache = cache.lock().unwrap();
        assert!(cache.in_flight.is_empty());
        assert_eq!(cache.completed.entries.len(), 2);
        assert!(cache.completed.entries.contains_key("c"));
    }

    #[tokio::test]
    async fn database_options_errors_are_propagated_without_fallback_or_cache() {
        for code in [
            Code::Unavailable,
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::DeadlineExceeded,
            Code::InvalidArgument,
        ] {
            let cache = Mutex::new(DialectCache::new(1, 1));
            let returned = get_or_detect_dialect(&cache, "database".to_string(), || async move {
                Err::<DatabaseDialect, SpannerError>(rpc_error(code))
            })
            .await
            .unwrap_err();

            assert_eq!(status_code(&returned), Some(code));
            assert!(cache.lock().unwrap().completed.entries.is_empty());
        }

        let cache = Mutex::new(DialectCache::new(1, 1));
        let returned = get_or_detect_dialect(&cache, "database".to_string(), || async {
            Err::<DatabaseDialect, SpannerError>(SpannerError::Other(
                "malformed database_dialect response".to_string(),
            ))
        })
        .await
        .unwrap_err();

        assert!(matches!(
            returned,
            SpannerError::Other(message) if message == "malformed database_dialect response"
        ));
        assert!(cache.lock().unwrap().completed.entries.is_empty());
    }

    #[tokio::test]
    async fn dialect_cache_recovers_from_a_poisoned_mutex() {
        let cache = Mutex::new(DialectCache::new(1, 1));
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _guard = cache.lock().unwrap();
            panic!("poison the dialect cache mutex");
        }));
        assert!(panic.is_err());

        let dialect = get_or_detect_dialect(&cache, "database".to_string(), || async {
            Ok(DatabaseDialect::GoogleStandardSql)
        })
        .await
        .unwrap();
        assert_eq!(dialect, DatabaseDialect::GoogleStandardSql);
    }

    async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
        tokio::time::timeout(CONCURRENCY_TEST_TIMEOUT, async {
            while counter.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for concurrency test counter");
    }

    fn active_flight(cache: &Mutex<DialectCache>, key: &str) -> Arc<DialectFlight> {
        Arc::clone(
            cache
                .lock()
                .unwrap()
                .in_flight
                .get(key)
                .expect("expected an active dialect flight"),
        )
    }

    async fn wait_for_followers(flight: &DialectFlight, expected: usize) {
        tokio::time::timeout(CONCURRENCY_TEST_TIMEOUT, async {
            while flight.followers.load(Ordering::SeqCst) < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed out waiting for dialect followers");
    }

    async fn join_bounded<T>(
        handle: tokio::task::JoinHandle<T>,
    ) -> Result<T, tokio::task::JoinError> {
        tokio::time::timeout(CONCURRENCY_TEST_TIMEOUT, handle)
            .await
            .expect("timed out waiting for concurrency test task")
    }

    async fn join_next_bounded<T: 'static>(
        tasks: &mut tokio::task::JoinSet<T>,
    ) -> Option<Result<T, tokio::task::JoinError>> {
        tokio::time::timeout(CONCURRENCY_TEST_TIMEOUT, tasks.join_next())
            .await
            .expect("timed out waiting for concurrency test task set")
    }

    fn into_gax_error(error: SpannerError) -> Arc<GaxError> {
        match error {
            SpannerError::GoogleCloud(error) => error,
            error => panic!("expected typed Google Cloud error, got {error}"),
        }
    }

    async fn shared_gax_failure_wave(error: GaxError) -> (Arc<GaxError>, Arc<GaxError>) {
        let cache = Arc::new(Mutex::new(DialectCache::new(2, 2)));
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());

        let leader = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let release = Arc::clone(&release);
            tokio::spawn(async move {
                get_or_detect_dialect(&cache, "database".to_string(), || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    release.notified().await;
                    Err::<DatabaseDialect, SpannerError>(error.into())
                })
                .await
            })
        };
        wait_for_count(&calls, 1).await;
        let flight = active_flight(&cache, "database");

        let follower = {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                get_or_detect_dialect(&cache, "database".to_string(), || async {
                    Err(SpannerError::Other(
                        "follower unexpectedly became detector".to_string(),
                    ))
                })
                .await
            })
        };
        wait_for_followers(&flight, 1).await;
        release.notify_waiters();

        let leader = join_bounded(leader)
            .await
            .expect("leader task must not panic")
            .unwrap_err();
        let follower = join_bounded(follower)
            .await
            .expect("follower task must not panic")
            .unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        (into_gax_error(leader), into_gax_error(follower))
    }

    async fn panic_after_release(
        calls: Arc<AtomicUsize>,
        release: Arc<tokio::sync::Notify>,
    ) -> Result<DatabaseDialect, SpannerError> {
        calls.fetch_add(1, Ordering::SeqCst);
        release.notified().await;
        panic!("intentional dialect discovery panic");
    }

    fn start_blocked_discovery(
        cache: Arc<Mutex<DialectCache>>,
        key: &'static str,
        calls: Arc<AtomicUsize>,
        release: Arc<tokio::sync::Notify>,
    ) -> tokio::task::JoinHandle<Result<DatabaseDialect, SpannerError>> {
        tokio::spawn(async move {
            get_or_detect_dialect(&cache, key.to_string(), || async move {
                calls.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                Ok(DatabaseDialect::GoogleStandardSql)
            })
            .await
        })
    }
}
