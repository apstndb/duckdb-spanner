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
    con.execute_batch(include_str!("macros.sql"))?;
    Ok(())
}
