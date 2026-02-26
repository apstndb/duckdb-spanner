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
/// - `"MyTable"` — resolves to the default schema (TABLE_SCHEMA = '')
/// - `"myschema.MyTable"` — resolves to named schema `myschema`
pub async fn discover_table_schema(
    client: &Client,
    table: &str,
) -> Result<Vec<ColumnInfo>, SpannerError> {
    let (schema_name, table_name) = split_schema_table(table);

    let mut tx = client.single().await?;

    let mut stmt = Statement::new(
        "SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE, ORDINAL_POSITION \
         FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table \
         ORDER BY ORDINAL_POSITION",
    );
    stmt.add_param("schema", &schema_name.to_string());
    stmt.add_param("table", &table_name.to_string());

    let mut iter = tx.query(stmt).await.map_err(SpannerError::Grpc)?;

    let mut columns = Vec::new();
    while let Some(row) = iter.next().await.map_err(SpannerError::Grpc)? {
        let name: String = row.column_by_name("COLUMN_NAME")?;
        let spanner_type_str: String = row.column_by_name("SPANNER_TYPE")?;

        let spanner_type = types::parse_spanner_type(&spanner_type_str);

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
