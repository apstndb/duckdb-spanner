use std::sync::atomic::{AtomicUsize, Ordering};

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};
use google_cloud_spanner::client::Client;
use google_cloud_spanner::statement::Statement;

use crate::error::SpannerError;
use crate::{client, runtime};

#[derive(Clone)]
struct TableRow {
    table_schema: String,
    table_name: String,
    table_type: String,
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

        let database = crate::bind_utils::resolve_database_path(bind)?;
        let endpoint = crate::bind_utils::resolve_endpoint(bind);

        let rows = runtime::block_on(async {
            let client = client::get_or_create_client(&database, endpoint.as_deref()).await?;
            list_tables(&client).await
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

        let capacity = unsafe { duckdb::ffi::duckdb_vector_size() } as usize;
        let end = (offset + capacity).min(bind_data.rows.len());
        let count = end - offset;

        let schema_vec = output.flat_vector(0);
        let name_vec = output.flat_vector(1);
        let type_vec = output.flat_vector(2);

        for (out_idx, row) in bind_data.rows[offset..end].iter().enumerate() {
            schema_vec.insert(out_idx, &row.table_schema);
            name_vec.insert(out_idx, &row.table_name);
            type_vec.insert(out_idx, &row.table_type);
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
        ])
    }
}

async fn list_tables(client: &Client) -> Result<Vec<TableRow>, SpannerError> {
    let mut tx = client.single().await?;
    let stmt = Statement::new(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE \
         FROM INFORMATION_SCHEMA.TABLES \
         WHERE TABLE_TYPE = 'BASE TABLE' \
         ORDER BY TABLE_SCHEMA, TABLE_NAME",
    );
    let mut iter = tx.query(stmt).await.map_err(SpannerError::Grpc)?;

    let mut rows = Vec::new();
    while let Some(row) = iter.next().await.map_err(SpannerError::Grpc)? {
        rows.push(TableRow {
            table_schema: row.column::<String>(0)?,
            table_name: row.column::<String>(1)?,
            table_type: row.column::<String>(2)?,
        });
    }
    Ok(rows)
}
