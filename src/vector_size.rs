use std::sync::OnceLock;

/// Return the number of rows that fit in a DuckDB data chunk.
pub(crate) fn runtime_vector_size() -> usize {
    static VECTOR_SIZE: OnceLock<usize> = OnceLock::new();

    *VECTOR_SIZE.get_or_init(|| {
        // SAFETY: `duckdb_vector_size` takes no pointers and reads DuckDB's initialized
        // process-wide vector capacity. It does not retain Rust data or mutate a handle.
        unsafe { duckdb::ffi::duckdb_vector_size() as usize }
    })
}

#[cfg(test)]
mod tests {
    use super::runtime_vector_size;

    #[test]
    fn runtime_vector_size_is_nonzero() {
        assert!(runtime_vector_size() > 0);
    }
}
