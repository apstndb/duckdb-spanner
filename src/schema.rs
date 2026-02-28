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

/// Discover the output schema of a SQL query using Plan mode.
///
/// Falls back to normal execution if Plan mode returns empty metadata.
pub async fn discover_query_schema(
    client: &Client,
    sql: &str,
    params_json: Option<&str>,
) -> Result<Vec<ColumnInfo>, SpannerError> {
    match discover_via_plan(client, sql, params_json).await {
        Ok(cols) if !cols.is_empty() => return Ok(cols),
        Ok(_) => {
            eprintln!("[duckdb-spanner] Plan mode returned empty columns, falling back to execution");
        }
        Err(e) => {
            eprintln!("[duckdb-spanner] Plan mode failed: {e}, falling back to execution");
        }
    }

    discover_via_execution(client, sql, params_json).await
}

async fn discover_via_plan(
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
    let columns = metadata
        .iter()
        .map(|field| ColumnInfo {
            name: field.name.clone(),
            spanner_type: field.r#type.clone().unwrap_or_default(),
        })
        .collect();

    Ok(columns)
}

async fn discover_via_execution(
    client: &Client,
    sql: &str,
    params_json: Option<&str>,
) -> Result<Vec<ColumnInfo>, SpannerError> {
    let mut tx = client.single().await?;
    // Wrap in LIMIT 1 subquery to minimize data transfer
    let limited_sql = format!("SELECT * FROM ({sql}) AS _sub LIMIT 1");
    let stmt = crate::params::create_statement(&limited_sql, params_json)?;
    let mut iter = tx.query(stmt).await.map_err(SpannerError::Grpc)?;

    // Read first row to ensure metadata is populated
    let _ = iter.next().await.map_err(SpannerError::Grpc)?;

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
/// Returns columns in ordinal order. Parses SPANNER_TYPE strings into Type protos.
///
/// The `table` parameter can be:
/// - `"MyTable"` — resolves to the default schema (TABLE_SCHEMA = '' for GoogleSQL, 'public' for PG)
/// - `"myschema.MyTable"` — resolves to named schema `myschema`
///
/// Dialect detection is automatic: for unqualified table names, both GoogleSQL ('')
/// and PG ('public') default schemas are tried. The returned TABLE_SCHEMA value
/// determines which type parser to use.
pub async fn discover_table_schema(
    client: &Client,
    table: &str,
) -> Result<Vec<ColumnInfo>, SpannerError> {
    let (schema_name, table_name) = split_schema_table(table);

    let mut tx = client.single().await?;

    // For unqualified tables (empty schema), try both GoogleSQL ('') and PG ('public')
    // default schemas. Only one will match depending on the database dialect.
    // For qualified tables, use the explicit schema.
    let (sql, schemas) = if schema_name.is_empty() {
        (
            "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA IN ('', 'public') AND TABLE_NAME = @table \
             ORDER BY ORDINAL_POSITION",
            None,
        )
    } else {
        (
            "SELECT TABLE_SCHEMA, COLUMN_NAME, SPANNER_TYPE \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table \
             ORDER BY ORDINAL_POSITION",
            Some(schema_name),
        )
    };

    let mut stmt = Statement::new(sql);
    if let Some(schema) = schemas {
        stmt.add_param("schema", &schema.to_string());
    }
    stmt.add_param("table", &table_name.to_string());

    let mut iter = tx.query(stmt).await.map_err(SpannerError::Grpc)?;

    let mut columns = Vec::new();
    while let Some(row) = iter.next().await.map_err(SpannerError::Grpc)? {
        // Use positional access — we control the SELECT order, so this is safe
        // and works regardless of whether column names are upper or lowercase.
        let table_schema: String = row.column(0)?;
        let name: String = row.column(1)?;
        let spanner_type_str: String = row.column(2)?;

        // Detect dialect from TABLE_SCHEMA: '' = GoogleSQL, 'public' = PG
        let spanner_type = if table_schema == "public" {
            types::parse_pg_spanner_type(&spanner_type_str)
        } else {
            types::parse_spanner_type(&spanner_type_str)
        };

        columns.push(ColumnInfo { name, spanner_type });
    }

    if columns.is_empty() {
        return Err(SpannerError::Other(format!(
            "Table not found or has no columns: {table}"
        )));
    }

    Ok(columns)
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
