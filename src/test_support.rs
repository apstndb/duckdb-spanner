//! Native v2 test allocation; never constructs a Context from private handles.
use duckdb_neo::{
    connection::Connection,
    environment::{Environment, StorageLocation},
};
pub fn connection() -> Connection {
    crate::register_static_extension().unwrap();
    let environment = Environment::new().unwrap();
    environment
        .open(StorageLocation::InMemory)
        .unwrap()
        .connect()
        .unwrap()
}
