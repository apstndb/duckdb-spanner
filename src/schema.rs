use google_cloud_googleapis::spanner::admin::database::v1::DatabaseDialect;
use google_cloud_googleapis::spanner::v1::execute_sql_request::QueryMode;
use google_cloud_googleapis::spanner::v1::Type;
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner::transaction::QueryOptions;

use crate::error::SpannerError;
use crate::types;

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
        _ => Err(format!(
            "Invalid dialect '{s}': must be 'googlesql' or 'postgresql'"
        )
        .into()),
    }
}

/// Detect the database dialect by querying INFORMATION_SCHEMA.SCHEMATA.
///
/// PostgreSQL-dialect databases have a 'public' schema; GoogleSQL databases do not.
/// This query uses no parameters, so it works in both dialects.
pub async fn detect_dialect(client: &Client) -> Result<DatabaseDialect, SpannerError> {
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
/// If `dialect` is `Unspecified`, auto-detects by querying INFORMATION_SCHEMA.SCHEMATA first
/// (one extra round-trip). Specify `GoogleStandardSql` or `Postgresql` to skip detection.
///
/// The `table` parameter can be:
/// - `"MyTable"` — resolves to the default schema ('' for GoogleSQL, 'public' for PG)
/// - `"myschema.MyTable"` — resolves to named schema `myschema`
pub async fn discover_table_schema(
    client: &Client,
    table: &str,
    dialect: DatabaseDialect,
) -> Result<Vec<ColumnInfo>, SpannerError> {
    let dialect = if dialect == DatabaseDialect::Unspecified {
        detect_dialect(client).await?
    } else {
        dialect
    };

    let (schema_name, table_name) = split_schema_table(table);

    let mut tx = client.single().await?;
    let stmt = build_columns_query(dialect, schema_name, table_name);
    let mut iter = tx.query(stmt).await.map_err(SpannerError::Grpc)?;

    let mut columns = Vec::new();
    while let Some(row) = iter.next().await.map_err(SpannerError::Grpc)? {
        let table_schema: String = row.column(0)?;
        let name: String = row.column(1)?;
        let spanner_type_str: String = row.column(2)?;

        // Use dialect-appropriate type parser
        let spanner_type = if table_schema == "public" {
            types::parse_pg_spanner_type(&spanner_type_str)
        } else {
            types::parse_spanner_type(&spanner_type_str)
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
fn build_columns_query(
    dialect: DatabaseDialect,
    schema_name: &str,
    table_name: &str,
) -> Statement {
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
