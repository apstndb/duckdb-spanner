use std::sync::atomic::{AtomicUsize, Ordering};

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use google_cloud_spanner::client::DatabaseClient;
use google_cloud_spanner::statement::Statement;
use google_cloud_spanner_admin_database_v1::model::DatabaseDialect;

use crate::error::SpannerError;
use crate::{client, runtime, schema};

#[derive(Clone)]
struct TableRow {
    table_schema: String,
    table_name: String,
    table_type: String,
    parent_table_name: Option<String>,
}

struct TableFilters {
    table_type: String,
    schema: Option<String>,
    table_name: Option<String>,
}

pub struct TablesBindData {
    rows: Vec<TableRow>,
}

pub struct TablesInitData {
    offset: AtomicUsize,
}

pub struct SpannerTablesVTab;

impl VTab for SpannerTablesVTab {
    type BindData = TablesBindData;
    type InitData = TablesInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column(
            "table_schema",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "table_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "table_type",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "parent_table_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        let database = crate::bind_utils::resolve_database_path(bind)?;
        let endpoint = crate::bind_utils::resolve_endpoint(bind);
        let filters = TableFilters {
            // Preserve the original zero-argument behavior; callers opt into
            // views (or other INFORMATION_SCHEMA table types) explicitly.
            table_type: crate::bind_utils::get_named_string(bind, "table_type")
                .unwrap_or_else(|| "BASE TABLE".to_string()),
            schema: crate::bind_utils::get_named_string(bind, "schema"),
            table_name: crate::bind_utils::get_named_string(bind, "table_name"),
        };

        let rows = runtime::block_on(async {
            let client = client::get_or_create_client(&database, endpoint.as_deref()).await?;
            list_tables(&client, &database, endpoint.as_deref(), &filters).await
        })??;

        bind.set_cardinality(rows.len() as u64, true);
        Ok(TablesBindData { rows })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(TablesInitData {
            offset: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let offset = init_data.offset.load(Ordering::Relaxed);
        if offset >= bind_data.rows.len() {
            output.set_len(0);
            return Ok(());
        }

        let capacity = crate::vector_size::runtime_vector_size();
        let end = (offset + capacity).min(bind_data.rows.len());
        let count = end - offset;

        let schema_vec = output.flat_vector(0);
        let name_vec = output.flat_vector(1);
        let type_vec = output.flat_vector(2);
        let mut parent_vec = output.flat_vector(3);

        for (out_idx, row) in bind_data.rows[offset..end].iter().enumerate() {
            schema_vec.insert(out_idx, &row.table_schema);
            name_vec.insert(out_idx, &row.table_name);
            type_vec.insert(out_idx, &row.table_type);
            match &row.parent_table_name {
                Some(parent) => parent_vec.insert(out_idx, parent),
                None => parent_vec.set_null(out_idx),
            }
        }

        init_data.offset.store(end, Ordering::Relaxed);
        output.set_len(count);
        Ok(())
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
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
                "table_type".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "schema".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "table_name".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
        ])
    }
}

async fn list_tables(
    client: &DatabaseClient,
    database: &str,
    endpoint: Option<&str>,
    filters: &TableFilters,
) -> Result<Vec<TableRow>, SpannerError> {
    let tx = client.single_use().build();
    let dialect = schema::detect_dialect(client, database, endpoint).await?;
    let stmt = build_tables_query(dialect, filters);
    let mut iter = tx.execute_query(stmt).await?;

    let mut rows = Vec::new();
    while let Some(row) = iter.next().await.transpose()? {
        rows.push(TableRow {
            table_schema: row.try_get(0)?,
            table_name: row.try_get(1)?,
            table_type: row.try_get(2)?,
            parent_table_name: row.try_get(3)?,
        });
    }
    Ok(rows)
}

/// Build the metadata query with dialect-specific parameter syntax.
///
/// PostgreSQL-dialect Spanner requires numbered placeholders and parameter names
/// (`p1`, `p2`, ...), whereas GoogleSQL uses named `@parameter` placeholders.
/// Every supplied filter stays in the Spanner query so metadata is filtered
/// server-side before rows are materialized in DuckDB.
fn build_tables_query(dialect: DatabaseDialect, filters: &TableFilters) -> Statement {
    let mut predicates = Vec::new();
    let mut builder;

    match dialect {
        DatabaseDialect::Postgresql => {
            predicates.push("TABLE_TYPE = $1".to_string());
            if filters.schema.is_some() {
                predicates.push(format!("TABLE_SCHEMA = ${}", predicates.len() + 1));
            }
            if filters.table_name.is_some() {
                predicates.push(format!("TABLE_NAME = ${}", predicates.len() + 1));
            }

            builder = Statement::builder(format!(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, PARENT_TABLE_NAME \
                 FROM INFORMATION_SCHEMA.TABLES \
                 WHERE {} \
                 ORDER BY TABLE_SCHEMA, TABLE_NAME",
                predicates.join(" AND ")
            ))
            .add_param("p1", &filters.table_type);

            let mut parameter_index = 2;
            if let Some(schema) = &filters.schema {
                builder = builder.add_param(format!("p{parameter_index}"), schema);
                parameter_index += 1;
            }
            if let Some(table_name) = &filters.table_name {
                builder = builder.add_param(format!("p{parameter_index}"), table_name);
            }
        }
        _ => {
            predicates.push("TABLE_TYPE = @table_type".to_string());
            if filters.schema.is_some() {
                predicates.push("TABLE_SCHEMA = @schema".to_string());
            }
            if filters.table_name.is_some() {
                predicates.push("TABLE_NAME = @table_name".to_string());
            }

            builder = Statement::builder(format!(
                "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, PARENT_TABLE_NAME \
                 FROM INFORMATION_SCHEMA.TABLES \
                 WHERE {} \
                 ORDER BY TABLE_SCHEMA, TABLE_NAME",
                predicates.join(" AND ")
            ))
            .add_param("table_type", &filters.table_type);

            if let Some(schema) = &filters.schema {
                builder = builder.add_param("schema", schema);
            }
            if let Some(table_name) = &filters.table_name {
                builder = builder.add_param("table_name", table_name);
            }
        }
    }

    builder.build()
}
