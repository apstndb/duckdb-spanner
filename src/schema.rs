use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};

use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::model::execute_sql_request::QueryMode;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::types::Type;
use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;
use tokio::sync::OnceCell;

use crate::cache::LruCache;
use crate::error::SpannerError;
use crate::types;

/// Keep the dialect cache aligned with the client cache in `client.rs`.
const DIALECT_CACHE_CAPACITY: usize = 8;

type DialectSlot = Arc<OnceCell<DatabaseDialect>>;
type DialectCache = LruCache<String, DialectSlot>;

/// Cache of detected dialects, keyed by the same `(database, endpoint)` key
/// used by the client cache in `client.rs` (see `client::cache_key`).
///
/// `detect_dialect` issues a query, so callers that run it on every bind
/// (`spanner_scan`, `COPY TO ... (FORMAT spanner)`) look here first to avoid
/// a redundant round-trip against the same database. Each slot is a `OnceCell`
/// so concurrent misses for one key share the primary metadata query. Failed
/// queries do not populate the cell and retry normally on the next bind.
static DIALECT_CACHE: LazyLock<Mutex<DialectCache>> =
    LazyLock::new(|| Mutex::new(LruCache::new(DIALECT_CACHE_CAPACITY)));

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
    let (slot, _) = get_or_insert_dialect_slot(&DIALECT_CACHE, key);
    get_or_detect_dialect(&slot, || detect_dialect_via_database_options(client)).await
}

/// Look up or create a bounded cache slot without holding the mutex during
/// database I/O. Recovering a poisoned mutex is intentional: this cache is an
/// optimization and must not unwind through DuckDB's FFI boundary.
fn get_or_insert_dialect_slot(
    cache: &Mutex<DialectCache>,
    key: String,
) -> (DialectSlot, Option<DialectSlot>) {
    let mut cache = cache.lock().unwrap_or_else(|e| e.into_inner());
    cache.get_or_insert_with(key, || Arc::new(OnceCell::new()))
}

/// Run dialect discovery once for a cache slot. `OnceCell` stores only a
/// successful result, so RPC, authentication, transport, and response errors
/// remain retryable and are returned unchanged to the caller.
async fn get_or_detect_dialect<F, Fut>(
    slot: &DialectSlot,
    detect: F,
) -> Result<DatabaseDialect, SpannerError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<DatabaseDialect, SpannerError>>,
{
    Ok(slot.get_or_try_init(detect).await?.clone())
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
    let dialect = if dialect == DatabaseDialect::Unspecified {
        detect_dialect(client, database, endpoint).await?
    } else {
        dialect
    };

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
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use google_cloud_gax::error::rpc::{Code, Status};
    use google_cloud_spanner::Error as SpannerClientError;

    use super::*;

    fn rpc_error(code: Code) -> SpannerError {
        SpannerError::GoogleCloud(SpannerClientError::service(
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
    async fn database_options_caches_postgresql_discovery() {
        let slot = Arc::new(OnceCell::new());
        let primary_calls = AtomicUsize::new(0);
        let cached_calls = AtomicUsize::new(0);

        let dialect = get_or_detect_dialect(&slot, || async {
            primary_calls.fetch_add(1, Ordering::SeqCst);
            Ok(DatabaseDialect::Postgresql)
        })
        .await
        .unwrap();
        assert_eq!(dialect, DatabaseDialect::Postgresql);

        let cached = get_or_detect_dialect(&slot, || async {
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
    async fn database_options_caches_googlesql_discovery() {
        let slot = Arc::new(OnceCell::new());

        let dialect =
            get_or_detect_dialect(&slot, || async { Ok(DatabaseDialect::GoogleStandardSql) })
                .await
                .unwrap();

        assert_eq!(dialect, DatabaseDialect::GoogleStandardSql);
        assert_eq!(slot.get(), Some(&DatabaseDialect::GoogleStandardSql));
    }

    #[tokio::test]
    async fn concurrent_dialect_cache_misses_share_one_discovery() {
        let slot = Arc::new(OnceCell::new());
        let calls = Arc::new(AtomicUsize::new(0));

        let (first, second) = tokio::join!(
            get_or_detect_dialect(&slot, || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    Ok(DatabaseDialect::GoogleStandardSql)
                }
            }),
            get_or_detect_dialect(&slot, || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(DatabaseDialect::GoogleStandardSql)
                }
            }),
        );

        assert_eq!(first.unwrap(), DatabaseDialect::GoogleStandardSql);
        assert_eq!(second.unwrap(), DatabaseDialect::GoogleStandardSql);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
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
            let slot = Arc::new(OnceCell::new());
            let returned = get_or_detect_dialect(&slot, || async move {
                Err::<DatabaseDialect, SpannerError>(rpc_error(code))
            })
            .await
            .unwrap_err();

            assert_eq!(status_code(&returned), Some(code));
            assert!(slot.get().is_none());
        }

        let slot = Arc::new(OnceCell::new());
        let returned = get_or_detect_dialect(&slot, || async {
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
        assert!(slot.get().is_none());
    }

    #[test]
    fn dialect_cache_evicts_the_least_recently_used_slot() {
        let cache = Mutex::new(DialectCache::new(2));
        let (first, evicted) = get_or_insert_dialect_slot(&cache, "first".to_string());
        assert!(evicted.is_none());
        let (second, evicted) = get_or_insert_dialect_slot(&cache, "second".to_string());
        assert!(evicted.is_none());

        let (first_hit, evicted) = get_or_insert_dialect_slot(&cache, "first".to_string());
        assert!(evicted.is_none());
        assert!(Arc::ptr_eq(&first, &first_hit));

        let (_, evicted) = get_or_insert_dialect_slot(&cache, "third".to_string());
        let evicted = evicted.expect("the full cache must evict one slot");
        assert!(Arc::ptr_eq(&evicted, &second));
    }

    #[test]
    fn dialect_cache_recovers_from_a_poisoned_mutex() {
        let cache = Mutex::new(DialectCache::new(1));
        let panic = catch_unwind(AssertUnwindSafe(|| {
            let _guard = cache.lock().unwrap();
            panic!("poison the dialect cache mutex");
        }));
        assert!(panic.is_err());

        let (slot, evicted) = get_or_insert_dialect_slot(&cache, "database".to_string());
        assert!(evicted.is_none());
        assert!(slot.get().is_none());
    }
}
