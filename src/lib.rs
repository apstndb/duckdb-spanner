mod bind_utils;
mod client;
mod convert;
mod error;
mod params;
mod query;
mod runtime;
mod scan;
mod schema;
mod types;

pub use query::SpannerQueryVTab;
pub use scan::SpannerScanVTab;

#[cfg(feature = "loadable-extension")]
#[duckdb::duckdb_entrypoint_c_api(ext_name = "spanner", min_duckdb_version = "v1.2.0")]
pub fn extension_entrypoint(con: duckdb::Connection) -> Result<(), Box<dyn std::error::Error>> {
    con.register_table_function::<SpannerQueryVTab>("spanner_query_raw")?;
    con.register_table_function::<SpannerScanVTab>("spanner_scan")?;

    // Helper macros for constructing query parameter JSON.
    //
    // spanner_params(struct): convert a STRUCT to JSON params string
    //   spanner_params({'age': spanner_value(25), 'name': 'Alice'})
    //
    // spanner_value(val): auto-detect Spanner type from DuckDB type
    //   spanner_value('2024-01-15'::DATE) → {"value":"2024-01-15","type":"DATE"}
    //
    // spanner_typed(val, typ): explicit Spanner type
    //   spanner_typed(NULL, 'INT64') → {"value":null,"type":"INT64"}
    //
    // Usage:
    //   params := spanner_params({'id': spanner_value(42::BIGINT), 'name': 'Alice'})
    con.execute_batch(include_str!("macros.sql"))?;

    Ok(())
}
