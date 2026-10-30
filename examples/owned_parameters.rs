//! Offline demonstration of owned v2 STRUCT/LIST/fixed ARRAY parameters.
//! Run with: cargo run --features static-extension --example owned_parameters
use duckdb_neo::{
    DuckDBType, Parameters, ToValue,
    connection::FFILink,
    environment::{Environment, StorageLocation},
    value::{Value, ValueInput},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    duckdb_spanner::register_static_extension()?;
    let connection = Environment::new()?
        .open(StorageLocation::InMemory)?
        .connect()?;
    let integer = i64::logical_type(&connection)?;
    let date = duckdb_neo::types::DateValue::logical_type(&connection)?;
    let items = [
        1_i64.value(&connection)?,
        Value::null(&connection, &integer)?,
        3_i64.value(&connection)?,
    ];
    let fixed = [7_i64.value(&connection)?, 8_i64.value(&connection)?];
    let fields = [
        connection.create_value(ValueInput::List {
            child_type: &integer,
            children: &items,
        })?,
        connection.create_value(ValueInput::Array {
            child_type: &integer,
            children: &fixed,
        })?,
        Value::null(&connection, &date)?,
    ];
    let parameters = connection.create_value(ValueInput::Struct {
        names: &["items", "fixed", "missing_date"],
        children: &fields,
    })?;
    let statement = connection.prepare("SELECT spanner_params($p)", false)?;
    let mut result = statement.execute(Parameters::named(&[("p", &parameters)]))?;
    drop(parameters); // Execution retains its own snapshot, including children.
    let chunk = result.next_chunk()?.ok_or("expected a result chunk")?;
    {
        let reader = chunk.get_vector_at::<String>(0)?.reader()?;
        println!("{}", reader.get(0)?.ok_or("expected a parameter object")?);
    }
    drop(chunk);
    result.drain()?;
    Ok(())
}
