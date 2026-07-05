use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use google_cloud_googleapis::spanner::admin::database::v1::DatabaseDialect;
use google_cloud_googleapis::spanner::v1::execute_sql_request::QueryMode;
use google_cloud_googleapis::spanner::v1::Type;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::QueryOptions;

use crate::error::SpannerError;
use crate::types;

/// Cache of detected dialects, keyed by the same `(database, endpoint)` key
/// used by the client cache in `client.rs` (see `client::cache_key`).
///
/// `detect_dialect` issues a query, so callers that run it on every bind
/// (`spanner_scan`, `COPY TO ... (FORMAT spanner)`) look here first to avoid
/// a redundant round-trip against the same database.
static DIALECT_CACHE: LazyLock<Mutex<HashMap<String, DatabaseDialect>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Column metadata discovered from Spanner schema.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub spanner_type: Type,
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
/// Fallback: if that query errors (e.g. an older emulator that doesn't
/// support `DATABASE_OPTIONS`), fall back to the previous heuristic of
/// checking for a `public` schema in `INFORMATION_SCHEMA.SCHEMATA`. This
/// heuristic can misdetect a GoogleSQL database with a user-created `public`
/// schema, so it's only used as a last resort.
///
/// Results are cached per `(database, endpoint)` key (see `client::cache_key`)
/// since callers such as `spanner_scan` and `COPY TO ... (FORMAT spanner)`
/// invoke this on every bind.
pub async fn detect_dialect(
    client: &Client,
    database: &str,
    endpoint: Option<&str>,
) -> Result<DatabaseDialect, SpannerError> {
    let key = crate::client::cache_key(database, endpoint);
    if let Some(dialect) = DIALECT_CACHE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
    {
        return Ok(*dialect);
    }

    let dialect = match detect_dialect_via_database_options(client).await {
        Ok(dialect) => dialect,
        Err(_) => detect_dialect_via_public_schema(client).await?,
    };

    DIALECT_CACHE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(key, dialect);

    Ok(dialect)
}

/// Query `INFORMATION_SCHEMA.DATABASE_OPTIONS` for the `database_dialect` option.
async fn detect_dialect_via_database_options(
    client: &Client,
) -> Result<DatabaseDialect, SpannerError> {
    let stmt = Statement::new(
        "SELECT OPTION_VALUE FROM INFORMATION_SCHEMA.DATABASE_OPTIONS \
         WHERE OPTION_NAME = 'database_dialect'",
    );
    let mut tx = client.single().await?;
    let mut iter = tx.query(stmt).await.map_err(SpannerError::Grpc)?;
    match iter.next().await.map_err(SpannerError::Grpc)? {
        Some(row) => {
            let value: String = row.column(0)?;
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
    match value {
        "POSTGRESQL" => Ok(DatabaseDialect::Postgresql),
        "GOOGLE_STANDARD_SQL" => Ok(DatabaseDialect::GoogleStandardSql),
        other => Err(SpannerError::Other(format!(
            "Unrecognized database_dialect option value: {other}"
        ))),
    }
}

/// Fallback heuristic: PostgreSQL-dialect databases have a 'public' schema;
/// GoogleSQL databases do not (unless the user created one, in which case
/// this misdetects -- see `detect_dialect_via_database_options` above, which
/// is tried first).
async fn detect_dialect_via_public_schema(client: &Client) -> Result<DatabaseDialect, SpannerError> {
    let stmt = Statement::new(
        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'public'",
    );
    let mut tx = client.single().await?;
    let mut iter = tx.query(stmt).await.map_err(SpannerError::Grpc)?;
    if iter.next().await.map_err(SpannerError::Grpc)?.is_some() {
        Ok(DatabaseDialect::Postgresql)
    } else {
        Ok(DatabaseDialect::GoogleStandardSql)
    }
}

/// Discover the output schema of a SQL query using Plan mode.
pub async fn discover_query_schema(
    client: &Client,
    sql: &str,
    params_json: Option<&str>,
) -> Result<Vec<ColumnInfo>, SpannerError> {
    let mut tx = client.single().await?;
    let stmt = crate::params::create_statement(sql, params_json)?;
    let options = QueryOptions {
        mode: QueryMode::Plan,
        ..Default::default()
    };
    let mut iter = tx.query_with_option(stmt, options).await?;

    // Consume the iterator to ensure metadata is fully populated
    while let Some(_row) = iter.next().await.map_err(SpannerError::Grpc)? {}

    let metadata = iter.columns_metadata();
    if metadata.is_empty() {
        return Err(SpannerError::Other(format!(
            "No column metadata returned for query: {sql}"
        )));
    }

    let columns = metadata
        .iter()
        .map(|field| ColumnInfo {
            name: field.name.clone(),
            spanner_type: field.r#type.clone().unwrap_or_default(),
        })
        .collect();

    Ok(columns)
}

/// Discover table schema from INFORMATION_SCHEMA.COLUMNS.
///
/// If `dialect` is `Unspecified`, auto-detects via `detect_dialect` (one extra
/// round-trip, cached per `(database, endpoint)`). Specify `GoogleStandardSql`
/// or `Postgresql` to skip detection.
///
/// The `table` parameter can be:
/// - `"MyTable"` — resolves to the default schema ('' for GoogleSQL, 'public' for PG)
/// - `"myschema.MyTable"` — resolves to named schema `myschema`
pub async fn discover_table_schema(
    client: &Client,
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

    let mut tx = client.single().await?;
    let stmt = build_columns_query(dialect, schema_name, table_name);
    let mut iter = tx.query(stmt).await.map_err(SpannerError::Grpc)?;

    let mut columns = Vec::new();
    while let Some(row) = iter.next().await.map_err(SpannerError::Grpc)? {
        let _table_schema: String = row.column(0)?;
        let name: String = row.column(1)?;
        let spanner_type_str: String = row.column(2)?;

        // Use the detected database dialect, not TABLE_SCHEMA (PG tables in named
        // schemas such as myschema.Users still use PostgreSQL type strings).
        let spanner_type = match dialect {
            DatabaseDialect::Postgresql => types::parse_pg_spanner_type(&spanner_type_str),
            _ => types::parse_spanner_type(&spanner_type_str),
        };

        columns.push(ColumnInfo { name, spanner_type });
    }

    if columns.is_empty() {
        let msg = if schema_name.is_empty() {
            format!("Table {table_name} not found in INFORMATION_SCHEMA (searched schemas '' and 'public')")
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
    if schema_name.is_empty() {
        let (sql, table_param) = match dialect {
            DatabaseDialect::Postgresql => (
                "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_SCHEMA IN ('', 'public') AND TABLE_NAME = $1 \
                 ORDER BY ORDINAL_POSITION",
                "p1",
            ),
            _ => (
                "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_SCHEMA IN ('', 'public') AND TABLE_NAME = @table \
                 ORDER BY ORDINAL_POSITION",
                "table",
            ),
        };
        let mut stmt = Statement::new(sql);
        stmt.add_param(table_param, &table_name.to_string());
        stmt
    } else {
        let (sql, schema_param, table_param) = match dialect {
            DatabaseDialect::Postgresql => (
                "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_SCHEMA = $1 AND TABLE_NAME = $2 \
                 ORDER BY ORDINAL_POSITION",
                "p1",
                "p2",
            ),
            _ => (
                "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table \
                 ORDER BY ORDINAL_POSITION",
                "schema",
                "table",
            ),
        };
        let mut stmt = Statement::new(sql);
        stmt.add_param(schema_param, &schema_name.to_string());
        stmt.add_param(table_param, &table_name.to_string());
        stmt
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
    use super::*;

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
    fn dialect_cache_is_keyed_like_client_cache() {
        // Sanity check that the same (database, endpoint) pair used to key
        // the client cache also keys the dialect cache, so a cached client
        // and its cached dialect stay associated.
        let key_a = crate::client::cache_key("db1", Some("localhost:9010"));
        let key_b = crate::client::cache_key("db1", Some("localhost:9010"));
        assert_eq!(key_a, key_b);

        let key_c = crate::client::cache_key("db2", Some("localhost:9010"));
        assert_ne!(key_a, key_c);

        DIALECT_CACHE
            .lock()
            .unwrap()
            .insert(key_a.clone(), DatabaseDialect::Postgresql);
        assert_eq!(
            DIALECT_CACHE.lock().unwrap().get(&key_b),
            Some(&DatabaseDialect::Postgresql)
        );
    }
}
