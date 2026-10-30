//! Chunk capacity comes from the public v2 factory, never an assumed ABI constant.
pub(crate) fn runtime_vector_size(
    context: &duckdb_neo::connection::Context,
) -> duckdb_neo::Result<usize> {
    crate::duck::capacity(context)
}
