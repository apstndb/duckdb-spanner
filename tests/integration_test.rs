mod spanemuboost;

use std::sync::{Arc, OnceLock};

use duckdb::Connection;
use duckdb_spanner::{
    register_copy_function, SpannerDdlAsyncVTab, SpannerDdlVTab, SpannerOperationsVTab,
    SpannerQueryVTab, SpannerScanVTab,
};
use google_cloud_gax::conn::Environment;
use google_cloud_googleapis::spanner::admin::database::v1::DatabaseDialect;
use google_cloud_spanner::client::{Client, ClientConfig};
use google_cloud_spanner::statement::Statement;
use tokio::runtime::Runtime;

// ═══════════════════════════════════════════════════════════════════════════
// Shared test infrastructure
// ═══════════════════════════════════════════════════════════════════════════

/// Dedicated tokio runtime for test setup and Spanner client calls.
/// Separate from the library's TOKIO_RUNTIME to avoid any interaction.
fn test_runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    })
}

/// Start the Spanner emulator (once) with test tables and seed data.
fn get_emulator() -> &'static spanemuboost::SpanEmuBoost {
    static EMU: OnceLock<spanemuboost::SpanEmuBoost> = OnceLock::new();
    EMU.get_or_init(|| {
        test_runtime().block_on(async {
            spanemuboost::SpanEmuBoost::builder()
                .setup_ddls(vec![
                    "CREATE TABLE ScalarTypes (\
                        Id INT64 NOT NULL, BoolCol BOOL, Int64Col INT64, Float64Col FLOAT64, \
                        StringCol STRING(MAX), BytesCol BYTES(MAX), DateCol DATE, \
                        TimestampCol TIMESTAMP\
                    ) PRIMARY KEY (Id)"
                        .into(),
                    "CREATE TABLE EmptyTable (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (Id)"
                        .into(),
                    "CREATE TABLE NumericTypes (\
                        Id INT64 NOT NULL, NumCol NUMERIC, JsonCol JSON\
                    ) PRIMARY KEY (Id)"
                        .into(),
                    "CREATE TABLE ArrayTypes (\
                        Id INT64 NOT NULL, IntArray ARRAY<INT64>, StrArray ARRAY<STRING(MAX)>\
                    ) PRIMARY KEY (Id)"
                        .into(),
                    "CREATE TABLE UuidTypes (\
                        Id INT64 NOT NULL, UuidCol UUID\
                    ) PRIMARY KEY (Id)"
                        .into(),
                    "CREATE TABLE CopyTarget (\
                        Id INT64 NOT NULL, Name STRING(MAX), Value FLOAT64\
                    ) PRIMARY KEY (Id)"
                        .into(),
                    "CREATE TABLE CopyTypes (\
                        Id INT64 NOT NULL, BoolCol BOOL, Int64Col INT64, Float64Col FLOAT64, \
                        StringCol STRING(MAX), DateCol DATE, TimestampCol TIMESTAMP\
                    ) PRIMARY KEY (Id)"
                        .into(),
                ])
                .setup_dmls(vec![
                    // ScalarTypes
                    "INSERT INTO ScalarTypes (Id, BoolCol, Int64Col, Float64Col, StringCol, BytesCol, DateCol, TimestampCol) \
                     VALUES (1, true, 42, 3.125, 'hello', b'hello', DATE '2024-01-15', TIMESTAMP '2024-06-15T10:30:00Z')".into(),
                    "INSERT INTO ScalarTypes (Id, BoolCol, Int64Col, Float64Col, StringCol, BytesCol, DateCol, TimestampCol) \
                     VALUES (2, false, -100, 2.625, 'world', b'world', DATE '1999-12-31', TIMESTAMP '2000-01-01T00:00:00Z')".into(),
                    "INSERT INTO ScalarTypes (Id, BoolCol, Int64Col, Float64Col, StringCol, BytesCol, DateCol, TimestampCol) \
                     VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)".into(),
                    // NumericTypes
                    "INSERT INTO NumericTypes (Id, NumCol, JsonCol) VALUES (1, 123.456789, JSON '{\"key\": \"value\"}')".into(),
                    "INSERT INTO NumericTypes (Id, NumCol, JsonCol) VALUES (2, -99999.999999999, NULL)".into(),
                    // ArrayTypes
                    "INSERT INTO ArrayTypes (Id, IntArray, StrArray) VALUES (1, [10, 20, 30], ['a', 'b', 'c'])".into(),
                    "INSERT INTO ArrayTypes (Id, IntArray, StrArray) VALUES (2, [], [])".into(),
                    "INSERT INTO ArrayTypes (Id, IntArray, StrArray) VALUES (3, NULL, NULL)".into(),
                    // UuidTypes
                    "INSERT INTO UuidTypes (Id, UuidCol) VALUES (1, '550e8400-e29b-41d4-a716-446655440000')".into(),
                    "INSERT INTO UuidTypes (Id, UuidCol) VALUES (2, NULL)".into(),
                ])
                .start()
                .await
                .expect("Failed to start emulator")
        })
    })
}

/// Get a shared Spanner data client connected to the emulator.
fn spanner_client() -> Arc<Client> {
    static CLIENT: OnceLock<Arc<Client>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            let env = get_emulator();
            test_runtime().block_on(async {
                let config = ClientConfig {
                    environment: Environment::Emulator(env.emulator_host().to_string()),
                    ..Default::default()
                };
                Arc::new(Client::new(&env.database_path(), config).await.unwrap())
            })
        })
        .clone()
}

/// Execute a Spanner SQL query and return all rows.
fn exec_spanner(sql: &str) -> Vec<google_cloud_spanner::row::Row> {
    let client = spanner_client();
    test_runtime().block_on(async {
        let mut tx = client.single().await.unwrap();
        let stmt = Statement::new(sql);
        let mut iter = tx.query(stmt).await.unwrap();
        let mut rows = Vec::new();
        while let Some(row) = iter.next().await.unwrap() {
            rows.push(row);
        }
        rows
    })
}

/// Execute a Spanner SQL query expecting exactly one row.
fn exec_spanner_one(sql: &str) -> google_cloud_spanner::row::Row {
    let rows = exec_spanner(sql);
    assert_eq!(
        rows.len(),
        1,
        "Expected 1 row for SQL: {sql}, got {}",
        rows.len()
    );
    rows.into_iter().next().unwrap()
}

// DuckDB VTab helpers

fn create_duckdb_connection() -> Connection {
    let _ = get_emulator(); // ensure emulator is running
    let conn = Connection::open_in_memory().unwrap();
    conn.register_table_function::<SpannerQueryVTab>("spanner_query_raw")
        .unwrap();
    conn.register_table_function::<SpannerScanVTab>("spanner_scan")
        .unwrap();
    conn.register_table_function::<SpannerDdlVTab>("spanner_ddl_raw")
        .unwrap();
    conn.register_table_function::<SpannerDdlAsyncVTab>("spanner_ddl_async_raw")
        .unwrap();
    conn.register_table_function::<SpannerOperationsVTab>("spanner_operations_raw")
        .unwrap();
    // DuckDB v1.5.0+: core_functions/json are bundled but need explicit load.
    // ICU is NOT bundled (too large) and must be installed from the extension repo.
    // ICU is required for TIMESTAMPTZ operations (AT TIME ZONE) used in macros.sql.
    conn.execute_batch("\
        LOAD core_functions;\
        LOAD json;\
        INSTALL icu;\
        LOAD icu;\
    ").unwrap();
    conn.execute_batch(include_str!("../src/macros.sql"))
        .unwrap();
    conn
}

fn vtab_query_sql(spanner_sql: &str) -> String {
    let env = get_emulator();
    format!(
        "SELECT * FROM spanner_query('{}', database_path := '{}', endpoint := '{}')",
        spanner_sql,
        env.database_path(),
        env.emulator_host()
    )
}

fn vtab_query_sql_with(spanner_sql: &str, extra_params: &str) -> String {
    let env = get_emulator();
    format!(
        "SELECT * FROM spanner_query('{}', database_path := '{}', endpoint := '{}', {})",
        spanner_sql,
        env.database_path(),
        env.emulator_host(),
        extra_params
    )
}

fn vtab_scan_sql_with(table: &str, extra_params: &str) -> String {
    let env = get_emulator();
    format!(
        "SELECT * FROM spanner_scan('{}', database_path := '{}', endpoint := '{}', {})",
        table,
        env.database_path(),
        env.emulator_host(),
        extra_params
    )
}

fn vtab_scan_sql(table: &str) -> String {
    let env = get_emulator();
    format!(
        "SELECT * FROM spanner_scan('{}', database_path := '{}', endpoint := '{}')",
        table,
        env.database_path(),
        env.emulator_host()
    )
}

// ═══════════════════════════════════════════════════════════════════════════
// Spanner client direct tests — type verification via query expressions
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_spanner_bool() {
    let row = exec_spanner_one("SELECT TRUE AS col");
    assert!(row.column::<bool>(0).unwrap());

    let row = exec_spanner_one("SELECT FALSE AS col");
    assert!(!row.column::<bool>(0).unwrap());
}

#[test]
fn test_spanner_int64() {
    let row = exec_spanner_one("SELECT 42 AS col");
    assert_eq!(row.column::<i64>(0).unwrap(), 42);

    let row = exec_spanner_one("SELECT -100 AS col");
    assert_eq!(row.column::<i64>(0).unwrap(), -100);

    let row = exec_spanner_one("SELECT 9223372036854775807 AS col");
    assert_eq!(row.column::<i64>(0).unwrap(), i64::MAX);
}

#[test]
fn test_spanner_float64() {
    let row = exec_spanner_one("SELECT 3.125 AS col");
    let val = row.column::<f64>(0).unwrap();
    assert!((val - 3.125).abs() < 0.001, "expected ~3.125, got {val}");

    let row = exec_spanner_one("SELECT -2.625 AS col");
    let val = row.column::<f64>(0).unwrap();
    assert!((val + 2.625).abs() < 0.001, "expected ~-2.625, got {val}");
}

#[test]
fn test_spanner_float32() {
    let row = exec_spanner_one("SELECT CAST(1.5 AS FLOAT32) AS col");
    let val = row.column::<f64>(0).unwrap();
    assert!((val - 1.5).abs() < 0.001, "expected ~1.5, got {val}");

    let row = exec_spanner_one("SELECT CAST(-0.25 AS FLOAT32) AS col");
    let val = row.column::<f64>(0).unwrap();
    assert!((val + 0.25).abs() < 0.001, "expected ~-0.25, got {val}");
}

#[test]
fn test_spanner_string() {
    let row = exec_spanner_one("SELECT 'hello' AS col");
    assert_eq!(row.column::<String>(0).unwrap(), "hello");

    let row = exec_spanner_one("SELECT '' AS col");
    assert_eq!(row.column::<String>(0).unwrap(), "");
}

#[test]
fn test_spanner_bytes() {
    let row = exec_spanner_one("SELECT b'hello' AS col");
    assert_eq!(row.column::<Vec<u8>>(0).unwrap(), b"hello");

    let row = exec_spanner_one("SELECT b'' AS col");
    assert_eq!(row.column::<Vec<u8>>(0).unwrap(), b"");
}

#[test]
fn test_spanner_date() {
    let row = exec_spanner_one("SELECT DATE '2024-01-15' AS col");
    let date = row.column::<time::Date>(0).unwrap();
    assert_eq!(date.year(), 2024);
    assert_eq!(date.month(), time::Month::January);
    assert_eq!(date.day(), 15);

    let row = exec_spanner_one("SELECT DATE '1999-12-31' AS col");
    let date = row.column::<time::Date>(0).unwrap();
    assert_eq!(date.year(), 1999);
    assert_eq!(date.month(), time::Month::December);
    assert_eq!(date.day(), 31);
}

#[test]
fn test_spanner_timestamp() {
    let row = exec_spanner_one("SELECT TIMESTAMP '2024-06-15T10:30:00Z' AS col");
    let ts = row.column::<time::OffsetDateTime>(0).unwrap();
    assert_eq!(ts.year(), 2024);
    assert_eq!(ts.month(), time::Month::June);
    assert_eq!(ts.day(), 15);
    assert_eq!(ts.hour(), 10);
    assert_eq!(ts.minute(), 30);
}

#[test]
fn test_spanner_numeric() {
    let row = exec_spanner_one("SELECT NUMERIC '123.456789' AS col");
    let bd = row.column::<bigdecimal::BigDecimal>(0).unwrap();
    let expected: bigdecimal::BigDecimal = "123.456789".parse().unwrap();
    assert_eq!(bd, expected);

    let row = exec_spanner_one("SELECT NUMERIC '-99999.999999999' AS col");
    let bd = row.column::<bigdecimal::BigDecimal>(0).unwrap();
    let expected: bigdecimal::BigDecimal = "-99999.999999999".parse().unwrap();
    assert_eq!(bd, expected);
}

#[test]
fn test_spanner_json() {
    let row = exec_spanner_one("SELECT JSON '{\"key\": \"value\"}' AS col");
    let val = row.column::<String>(0).unwrap();
    assert!(val.contains("key"), "expected JSON with 'key', got {val}");
    assert!(
        val.contains("value"),
        "expected JSON with 'value', got {val}"
    );
}

#[test]
fn test_spanner_uuid() {
    // UUID requires a table column (no UUID literal support in queries)
    let row = exec_spanner_one("SELECT CAST(UuidCol AS STRING) FROM UuidTypes WHERE Id = 1");
    assert_eq!(
        row.column::<String>(0).unwrap(),
        "550e8400-e29b-41d4-a716-446655440000"
    );

    let row = exec_spanner_one("SELECT UuidCol IS NULL FROM UuidTypes WHERE Id = 2");
    assert!(row.column::<bool>(0).unwrap());
}

#[test]
fn test_spanner_interval() {
    let row = exec_spanner_one("SELECT INTERVAL 1 DAY AS col");
    let val = row.column::<String>(0).unwrap();
    assert!(!val.is_empty(), "expected non-empty interval string");

    let row = exec_spanner_one("SELECT INTERVAL 3 HOUR + INTERVAL 30 MINUTE AS col");
    let val = row.column::<String>(0).unwrap();
    assert!(!val.is_empty(), "expected non-empty interval string");

    let row = exec_spanner_one("SELECT INTERVAL 1 YEAR + INTERVAL 6 MONTH AS col");
    let val = row.column::<String>(0).unwrap();
    assert!(!val.is_empty(), "expected non-empty interval string");
}

#[test]
fn test_spanner_array_int64() {
    let row = exec_spanner_one("SELECT [10, 20, 30] AS col");
    assert_eq!(row.column::<Vec<i64>>(0).unwrap(), vec![10, 20, 30]);
}

#[test]
fn test_spanner_array_string() {
    let row = exec_spanner_one("SELECT ['a', 'b', 'c'] AS col");
    assert_eq!(
        row.column::<Vec<String>>(0).unwrap(),
        vec!["a", "b", "c"]
    );
}

#[test]
fn test_spanner_array_empty() {
    let row = exec_spanner_one("SELECT ARRAY<INT64>[] AS col");
    let val = row.column::<Vec<i64>>(0).unwrap();
    assert!(val.is_empty());
}

#[test]
fn test_spanner_null_scalar() {
    let row = exec_spanner_one("SELECT CAST(NULL AS STRING) AS col");
    assert!(row.column::<Option<String>>(0).unwrap().is_none());

    let row = exec_spanner_one("SELECT CAST(NULL AS INT64) AS col");
    assert!(row.column::<Option<i64>>(0).unwrap().is_none());

    let row = exec_spanner_one("SELECT CAST(NULL AS BOOL) AS col");
    assert!(row.column::<Option<bool>>(0).unwrap().is_none());

    let row = exec_spanner_one("SELECT CAST(NULL AS FLOAT64) AS col");
    assert!(row.column::<Option<f64>>(0).unwrap().is_none());

    let row = exec_spanner_one("SELECT CAST(NULL AS DATE) AS col");
    assert!(row.column::<Option<time::Date>>(0).unwrap().is_none());

    let row = exec_spanner_one("SELECT CAST(NULL AS TIMESTAMP) AS col");
    assert!(row
        .column::<Option<time::OffsetDateTime>>(0)
        .unwrap()
        .is_none());
}

#[test]
fn test_spanner_null_array() {
    let row = exec_spanner_one("SELECT CAST(NULL AS ARRAY<INT64>) AS col");
    assert!(row.column::<Option<Vec<i64>>>(0).unwrap().is_none());
}

#[test]
fn test_spanner_struct_expression() {
    // STRUCT via query expression (not a column type)
    let rows = exec_spanner(
        "SELECT ARRAY(SELECT AS STRUCT 'hello' AS name, 42 AS age) AS col",
    );
    assert_eq!(rows.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// Spanner client tests — table-backed queries
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_spanner_table_row_count() {
    let rows = exec_spanner("SELECT Id FROM ScalarTypes ORDER BY Id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].column::<i64>(0).unwrap(), 1);
    assert_eq!(rows[1].column::<i64>(0).unwrap(), 2);
    assert_eq!(rows[2].column::<i64>(0).unwrap(), 3);
}

#[test]
fn test_spanner_table_mixed_types() {
    let row = exec_spanner_one(
        "SELECT BoolCol, Int64Col, Float64Col, StringCol FROM ScalarTypes WHERE Id = 1",
    );
    assert!(row.column::<bool>(0).unwrap());
    assert_eq!(row.column::<i64>(1).unwrap(), 42);
    let f = row.column::<f64>(2).unwrap();
    assert!((f - 3.125).abs() < 0.001);
    assert_eq!(row.column::<String>(3).unwrap(), "hello");
}

#[test]
fn test_spanner_table_null_row() {
    let row = exec_spanner_one("SELECT BoolCol, Int64Col, StringCol FROM ScalarTypes WHERE Id = 3");
    assert!(row.column::<Option<bool>>(0).unwrap().is_none());
    assert!(row.column::<Option<i64>>(1).unwrap().is_none());
    assert!(row.column::<Option<String>>(2).unwrap().is_none());
}

#[test]
fn test_spanner_table_bytes() {
    let row =
        exec_spanner_one("SELECT BytesCol FROM ScalarTypes WHERE Id = 1");
    assert_eq!(row.column::<Vec<u8>>(0).unwrap(), b"hello");
}

#[test]
fn test_spanner_table_date_timestamp() {
    let row =
        exec_spanner_one("SELECT DateCol, TimestampCol FROM ScalarTypes WHERE Id = 1");
    let date = row.column::<time::Date>(0).unwrap();
    assert_eq!(date.year(), 2024);
    assert_eq!(date.month(), time::Month::January);
    assert_eq!(date.day(), 15);
    let ts = row.column::<time::OffsetDateTime>(1).unwrap();
    assert_eq!(ts.year(), 2024);
    assert_eq!(ts.month(), time::Month::June);
}

#[test]
fn test_spanner_table_numeric_json() {
    let row = exec_spanner_one("SELECT NumCol, JsonCol FROM NumericTypes WHERE Id = 1");
    let bd = row.column::<bigdecimal::BigDecimal>(0).unwrap();
    let expected: bigdecimal::BigDecimal = "123.456789".parse().unwrap();
    assert_eq!(bd, expected);
    let json = row.column::<String>(1).unwrap();
    assert!(json.contains("key"));
}

#[test]
fn test_spanner_table_array() {
    let row = exec_spanner_one("SELECT IntArray, StrArray FROM ArrayTypes WHERE Id = 1");
    assert_eq!(row.column::<Vec<i64>>(0).unwrap(), vec![10, 20, 30]);
    assert_eq!(
        row.column::<Vec<String>>(1).unwrap(),
        vec!["a", "b", "c"]
    );
}

#[test]
fn test_spanner_table_array_empty() {
    let row = exec_spanner_one("SELECT IntArray, StrArray FROM ArrayTypes WHERE Id = 2");
    assert!(row.column::<Vec<i64>>(0).unwrap().is_empty());
    assert!(row.column::<Vec<String>>(1).unwrap().is_empty());
}

#[test]
fn test_spanner_table_array_null() {
    let row = exec_spanner_one("SELECT IntArray, StrArray FROM ArrayTypes WHERE Id = 3");
    assert!(row.column::<Option<Vec<i64>>>(0).unwrap().is_none());
    assert!(row.column::<Option<Vec<String>>>(1).unwrap().is_none());
}

#[test]
fn test_spanner_empty_table() {
    let rows = exec_spanner("SELECT * FROM EmptyTable");
    assert!(rows.is_empty());
}

#[test]
fn test_spanner_filtering() {
    let rows = exec_spanner("SELECT StringCol FROM ScalarTypes WHERE Id = 2");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column::<String>(0).unwrap(), "world");

    let rows = exec_spanner("SELECT * FROM ScalarTypes WHERE Id = 999");
    assert!(rows.is_empty());
}

// ═══════════════════════════════════════════════════════════════════════════
// DuckDB VTab E2E tests — spanner_query
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_vtab_query_basic() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql("SELECT Id FROM ScalarTypes ORDER BY Id");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_vtab_query_types() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql(
        "SELECT BoolCol, Int64Col, Float64Col, StringCol FROM ScalarTypes WHERE Id = 1",
    );
    let (b, i, f, s): (bool, i64, f64, String) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap();
    assert!(b);
    assert_eq!(i, 42);
    assert!((f - 3.125).abs() < 0.001);
    assert_eq!(s, "hello");
}

#[test]
fn test_vtab_query_date_timestamp() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql(
        "SELECT CAST(DateCol AS STRING), CAST(TimestampCol AS STRING) FROM ScalarTypes WHERE Id = 1",
    );
    let (date_str, ts_str): (String, String) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!(date_str, "2024-01-15");
    assert!(
        ts_str.contains("2024-06-15"),
        "expected timestamp containing '2024-06-15', got {ts_str}"
    );
}

#[test]
fn test_vtab_query_numeric_json() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql(
        "SELECT CAST(NumCol AS STRING), JsonCol FROM NumericTypes WHERE Id = 1",
    );
    let (num, json): (String, String) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert!(
        num.contains("123.456789"),
        "expected numeric containing '123.456789', got {num}"
    );
    assert!(
        json.contains("key"),
        "expected JSON containing 'key', got {json}"
    );
}

#[test]
fn test_vtab_query_empty_table() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql("SELECT * FROM EmptyTable");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn test_vtab_query_null() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql("SELECT StringCol IS NULL FROM ScalarTypes WHERE Id = 3");
    let is_null: bool = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    assert!(is_null);
}

#[test]
fn test_vtab_query_duckdb_filtering() {
    let conn = create_duckdb_connection();
    let inner = vtab_query_sql("SELECT Id, StringCol FROM ScalarTypes");
    let count: i64 = conn
        .query_row(
            &format!("SELECT COUNT(*) FROM ({inner}) WHERE Id > 1"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
fn test_vtab_query_duckdb_aggregate() {
    let conn = create_duckdb_connection();
    let inner = vtab_query_sql("SELECT Int64Col FROM ScalarTypes");
    let sum: i64 = conn
        .query_row(
            &format!("SELECT SUM(Int64Col) FROM ({inner})"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    // 42 + (-100) + NULL = -58
    assert_eq!(sum, -58);
}

// ═══════════════════════════════════════════════════════════════════════════
// DuckDB VTab E2E tests — spanner_scan
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_vtab_scan_basic() {
    let conn = create_duckdb_connection();
    let sql = vtab_scan_sql("ScalarTypes");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_vtab_scan_empty_table() {
    let conn = create_duckdb_connection();
    let sql = vtab_scan_sql("EmptyTable");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn test_vtab_scan_projection() {
    let conn = create_duckdb_connection();
    let base = vtab_scan_sql("ScalarTypes");
    let val: String = conn
        .query_row(
            &format!("SELECT StringCol FROM ({base}) WHERE Id = 1"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(val, "hello");
}

#[test]
fn test_vtab_scan_types() {
    let conn = create_duckdb_connection();
    let base = vtab_scan_sql("ScalarTypes");

    let b: bool = conn
        .query_row(
            &format!("SELECT BoolCol FROM ({base}) WHERE Id = 1"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(b);

    let f: f64 = conn
        .query_row(
            &format!("SELECT Float64Col FROM ({base}) WHERE Id = 1"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!((f - 3.125).abs() < 0.001);
}

#[test]
fn test_vtab_endpoint_parameter() {
    let env = get_emulator();
    let conn = create_duckdb_connection();
    let sql = format!(
        "SELECT COUNT(*) FROM spanner_query('SELECT 1', database_path := '{}', endpoint := '{}')",
        env.database_path(),
        env.emulator_host(),
    );
    let count: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_vtab_query_float32() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql("SELECT CAST(1.5 AS FLOAT32) AS col");
    let val: f64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    assert!((val - 1.5).abs() < 0.001, "expected ~1.5, got {val}");
}

#[test]
fn test_vtab_query_bytes() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql("SELECT BytesCol FROM ScalarTypes WHERE Id = 1");
    let encoded: String = conn
        .query_row(
            &format!("SELECT base64(BytesCol) FROM ({sql})"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    // b'hello' = base64 'aGVsbG8='
    assert_eq!(encoded, "aGVsbG8=");
}

#[test]
fn test_vtab_query_uuid() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql("SELECT UuidCol FROM UuidTypes WHERE Id = 1");
    let val: String = conn
        .query_row(
            &format!("SELECT CAST(UuidCol AS VARCHAR) FROM ({sql})"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(val, "550e8400-e29b-41d4-a716-446655440000");
}

#[test]
fn test_vtab_query_interval() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql("SELECT INTERVAL 1 DAY AS col");
    let val: String = conn
        .query_row(
            &format!("SELECT CAST(col AS VARCHAR) FROM ({sql})"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(!val.is_empty(), "expected non-empty interval string");
}

#[test]
fn test_vtab_query_array() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql("SELECT IntArray FROM ArrayTypes WHERE Id = 1");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// Named parameter tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_vtab_query_exact_staleness() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql_with("SELECT 1 AS col", "exact_staleness_secs := 0");
    let val: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    assert_eq!(val, 1);
}

#[test]
fn test_vtab_query_no_parallelism() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql_with(
        "SELECT Id FROM ScalarTypes ORDER BY Id",
        "use_parallelism := false",
    );
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_vtab_scan_exact_staleness() {
    let conn = create_duckdb_connection();
    let sql = vtab_scan_sql_with("ScalarTypes", "exact_staleness_secs := 0");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// Query parameter tests — Spanner client direct
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_spanner_query_params() {
    let client = spanner_client();
    let rows = test_runtime().block_on(async {
        let mut tx = client.single().await.unwrap();
        let mut stmt =
            Statement::new("SELECT Id, StringCol FROM ScalarTypes WHERE Id = @id AND StringCol = @name");
        stmt.add_param("id", &1_i64);
        stmt.add_param("name", &"hello".to_string());
        let mut iter = tx.query(stmt).await.unwrap();
        let mut rows = Vec::new();
        while let Some(row) = iter.next().await.unwrap() {
            rows.push(row);
        }
        rows
    });
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column::<i64>(0).unwrap(), 1);
    assert_eq!(rows[0].column::<String>(1).unwrap(), "hello");
}

// ═══════════════════════════════════════════════════════════════════════════
// Query parameter tests — VTab E2E
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_vtab_query_params() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql_with(
        "SELECT Id, StringCol FROM ScalarTypes WHERE Id = @id AND StringCol = @name",
        "params := {'id': 1, 'name': 'hello'}",
    );
    let (id, name): (i64, String) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!(id, 1);
    assert_eq!(name, "hello");
}

#[test]
fn test_vtab_query_params_bool_float() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql_with(
        "SELECT BoolCol, Float64Col FROM ScalarTypes WHERE BoolCol = @b AND Float64Col = @f",
        "params := {'b': true, 'f': 3.125}",
    );
    let (b, f): (bool, f64) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert!(b);
    assert!((f - 3.125).abs() < 0.001);
}

// ═══════════════════════════════════════════════════════════════════════════
// Round-trip E2E tests — DuckDB spanner_value() → Spanner emulator → DuckDB
//
// Tests the full pipeline: DuckDB macro formats a typed JSON param,
// params.rs parses it and binds to a Spanner Statement, Spanner processes
// the query, and DuckDB reads back the result.
//
// Table-driven: each entry specifies a DuckDB param expression and an
// `Expected` variant that encodes both the expected DuckDB type and how
// to extract/compare the result value.
// ═══════════════════════════════════════════════════════════════════════════

/// Describes the expected DuckDB result from a Spanner round-trip.
///
/// Each variant encodes an expected DuckDB type and extraction method:
///   - `Value`     — generic: TYPEOF = type, CAST(col AS VARCHAR) = value
///   - `Base64`    — BLOB: TYPEOF = BLOB, base64(col) = value
///   - `Timestamp` — TIMESTAMPTZ: TYPEOF = TIMESTAMP WITH TIME ZONE, UTC strftime = value
///   - `Null`      — NULL: TYPEOF = type, col IS NULL
#[allow(dead_code)]
enum Expected<'a> {
    Value(&'a str, &'a str),
    Base64(&'a str),
    Timestamp(&'a str),
    Null(&'a str),
}

fn assert_roundtrip(conn: &Connection, param: &str, expected: Expected) {
    use Expected::*;

    let inner = vtab_query_sql_with(
        "SELECT @v AS col",
        &format!("params := {{'v': {param}}}"),
    );

    let (extract_sql, expected_type, expected_val) = match expected {
        Value(t, v) => ("CAST(col AS VARCHAR)", t, v),
        Base64(v) => ("base64(col)", "BLOB", v),
        Timestamp(v) => (
            "strftime(col AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ')",
            "TIMESTAMP WITH TIME ZONE",
            v,
        ),
        Null(t) => ("COALESCE(CAST(col AS VARCHAR), 'NULL')", t, "NULL"),
    };

    let actual_type: String = conn
        .query_row(
            &format!("SELECT TYPEOF(col) FROM ({inner})"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(actual_type, expected_type, "type mismatch for param: {param}");

    let val: String = conn
        .query_row(
            &format!("SELECT {extract_sql} FROM ({inner})"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(val, expected_val, "value mismatch for param: {param}");
}

macro_rules! roundtrip_tests {
    ($($name:ident: $param:expr => $expected:expr);* $(;)?) => {
        $(
            #[test]
            fn $name() {
                let conn = create_duckdb_connection();
                assert_roundtrip(&conn, $param, $expected);
            }
        )*
    };
}

use Expected::*;

roundtrip_tests! {
    test_roundtrip_e2e_bool:              "spanner_value(true)"                                => Value("BOOLEAN", "true");
    test_roundtrip_e2e_int64:             "spanner_value(42::BIGINT)"                          => Value("BIGINT", "42");
    test_roundtrip_e2e_int64_from_integer: "spanner_value(1000::INTEGER)"                      => Value("BIGINT", "1000");
    test_roundtrip_e2e_float64:           "spanner_value(3.125::DOUBLE)"                       => Value("DOUBLE", "3.125");
    test_roundtrip_e2e_float32:           "spanner_value(1.5::FLOAT)"                          => Value("FLOAT", "1.5");
    test_roundtrip_e2e_string:            "spanner_value('hello world')"                       => Value("VARCHAR", "hello world");
    test_roundtrip_e2e_date:              "spanner_value('2024-01-15'::DATE)"                   => Value("DATE", "2024-01-15");
    test_roundtrip_e2e_timestamp:         "spanner_value('2024-06-15T10:30:00Z'::TIMESTAMPTZ)" => Timestamp("2024-06-15T10:30:00Z");
    test_roundtrip_e2e_bytes:             "spanner_value('\\xDEAD'::BLOB)"                     => Base64("3kFE");
    test_roundtrip_e2e_numeric:           "spanner_value(123.456789::DECIMAL(38,9))"            => Value("DECIMAL(38,9)", "123.456789000");
    test_roundtrip_e2e_null_int64:        "spanner_value(NULL::BIGINT)"                        => Null("BIGINT");
    test_roundtrip_e2e_null_date:         "spanner_value(NULL::DATE)"                          => Null("DATE");
    test_roundtrip_e2e_spanner_typed:     "spanner_typed(42, 'INT64')"                         => Value("BIGINT", "42");
    test_roundtrip_e2e_array_int64:       "spanner_value([1, 2, 3])"                           => Value("BIGINT[]", "[1, 2, 3]");
    test_roundtrip_e2e_array_string:      "spanner_value(['a', 'b', 'c'])"                     => Value("VARCHAR[]", "[a, b, c]");
    test_roundtrip_e2e_array_float64:     "spanner_value([1.5, 2.5]::DOUBLE[])"                => Value("DOUBLE[]", "[1.5, 2.5]");
    test_roundtrip_e2e_array_bool:        "spanner_value([true, false])"                       => Value("BOOLEAN[]", "[true, false]");
    test_roundtrip_e2e_array_typed:       "spanner_typed([10, 20], 'ARRAY<INT64>')"            => Value("BIGINT[]", "[10, 20]")
}

// Multi-param tests — structurally different (multiple columns, table queries)

#[test]
fn test_roundtrip_e2e_mixed_params() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql_with(
        "SELECT Id, StringCol FROM ScalarTypes WHERE Id = @id AND StringCol = @name",
        "params := {'id': spanner_value(1::BIGINT), 'name': spanner_value('hello')}",
    );
    let (id, name): (i64, String) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!(id, 1);
    assert_eq!(name, "hello");
}

#[test]
fn test_roundtrip_e2e_plain_and_typed_mix() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql_with(
        "SELECT Id, StringCol FROM ScalarTypes WHERE Id = @id AND StringCol = @name",
        "params := {'id': 1, 'name': spanner_value('hello')}",
    );
    let (id, name): (i64, String) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!(id, 1);
    assert_eq!(name, "hello");
}

// ═══════════════════════════════════════════════════════════════════════════
// Error case tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_error_negative_exact_staleness_secs() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql_with("SELECT 1", "exact_staleness_secs := -1");
    let result = conn.query_row(&sql, [], |r| r.get::<_, i64>(0));
    assert!(result.is_err(), "negative exact_staleness_secs should fail");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("exact_staleness_secs must be non-negative"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_error_invalid_params_json() {
    let conn = create_duckdb_connection();
    let env = get_emulator();
    let sql = format!(
        "SELECT * FROM spanner_query_raw('SELECT 1', database_path := '{}', endpoint := '{}', params := '{{not valid json')",
        env.database_path(),
        env.emulator_host()
    );
    let result = conn.query_row(&sql, [], |r| r.get::<_, i64>(0));
    assert!(result.is_err(), "invalid params JSON should fail");
}

#[test]
fn test_error_nonexistent_table_scan() {
    let conn = create_duckdb_connection();
    let sql = vtab_scan_sql("NonExistentTable12345");
    let result = conn.query_row(&sql, [], |r| r.get::<_, i64>(0));
    assert!(result.is_err(), "scanning non-existent table should fail");
}

#[test]
fn test_error_invalid_sql_query() {
    let conn = create_duckdb_connection();
    let sql = vtab_query_sql("THIS IS NOT VALID SQL!!!");
    let result = conn.query_row(&sql, [], |r| r.get::<_, i64>(0));
    assert!(result.is_err(), "invalid SQL should fail");
}

// ═══════════════════════════════════════════════════════════════════════════
// DDL execution tests
// ═══════════════════════════════════════════════════════════════════════════

fn vtab_ddl_sql(ddl: &str) -> String {
    let env = get_emulator();
    format!(
        "SELECT * FROM spanner_ddl('{}', database_path := '{}', endpoint := '{}')",
        ddl.replace('\'', "''"),
        env.database_path(),
        env.emulator_host()
    )
}

fn vtab_ddl_async_sql(ddl: &str) -> String {
    let env = get_emulator();
    format!(
        "SELECT * FROM spanner_ddl_async('{}', database_path := '{}', endpoint := '{}')",
        ddl.replace('\'', "''"),
        env.database_path(),
        env.emulator_host()
    )
}

/// Comprehensive DDL test — runs all DDL operations sequentially in one test
/// to avoid Spanner's "concurrent schema change" rejection.
/// The emulator only allows one schema change at a time.
#[test]
fn test_ddl_operations() {
    let conn = create_duckdb_connection();

    // --- spanner_ddl (sync) ---

    // 1. CREATE TABLE
    let sql = vtab_ddl_sql(
        "CREATE TABLE DdlTest (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (Id)"
    );
    let (op_name, done, duration): (String, bool, f64) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap();
    assert!(!op_name.is_empty(), "operation_name should not be empty");
    assert!(done, "sync DDL should complete before returning");
    assert!(duration >= 0.0, "duration should be non-negative");

    // Verify table exists
    let verify_sql = vtab_query_sql("SELECT COUNT(*) AS cnt FROM DdlTest");
    let count: i64 = conn.query_row(&verify_sql, [], |r| r.get(0)).unwrap();
    assert_eq!(count, 0);

    // 2. ALTER TABLE — add column
    let alter_sql = vtab_ddl_sql("ALTER TABLE DdlTest ADD COLUMN Value FLOAT64");
    let (_, done, _): (String, bool, f64) = conn
        .query_row(&alter_sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap();
    assert!(done);

    let verify_sql = vtab_query_sql(
        "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS \
         WHERE table_name = ''DdlTest'' AND column_name = ''Value''"
    );
    let col_name: String = conn.query_row(&verify_sql, [], |r| r.get(0)).unwrap();
    assert_eq!(col_name, "Value");

    // 3. CREATE INDEX
    let index_sql = vtab_ddl_sql("CREATE INDEX DdlTestByName ON DdlTest(Name)");
    let (_, done, _): (String, bool, f64) = conn
        .query_row(&index_sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap();
    assert!(done);

    let verify_sql = vtab_query_sql(
        "SELECT index_name FROM INFORMATION_SCHEMA.INDEXES \
         WHERE table_name = ''DdlTest'' AND index_name = ''DdlTestByName''"
    );
    let idx_name: String = conn.query_row(&verify_sql, [], |r| r.get(0)).unwrap();
    assert_eq!(idx_name, "DdlTestByName");

    // 4. DROP INDEX then DROP TABLE
    let drop_idx_sql = vtab_ddl_sql("DROP INDEX DdlTestByName");
    conn.execute_batch(&format!("SELECT * FROM ({drop_idx_sql})")).unwrap();

    let drop_sql = vtab_ddl_sql("DROP TABLE DdlTest");
    let (_, done, _): (String, bool, f64) = conn
        .query_row(&drop_sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap();
    assert!(done);

    let verify_sql = vtab_query_sql(
        "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_name = ''DdlTest''"
    );
    let count: i64 = conn.query_row(&verify_sql, [], |r| r.get(0)).unwrap();
    assert_eq!(count, 0);

    // --- spanner_ddl_async ---

    // 5. Async CREATE TABLE
    let sql = vtab_ddl_async_sql(
        "CREATE TABLE DdlAsyncTest (Id INT64 NOT NULL, Data BYTES(MAX)) PRIMARY KEY (Id)"
    );
    let (op_name, done): (String, bool) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert!(!op_name.is_empty(), "operation_name should not be empty");
    // Emulator typically completes immediately
    eprintln!("DDL async: op={op_name}, done={done}");

    if !done {
        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    let verify_sql = vtab_query_sql("SELECT COUNT(*) AS cnt FROM DdlAsyncTest");
    let count: i64 = conn.query_row(&verify_sql, [], |r| r.get(0)).unwrap();
    assert_eq!(count, 0);

    // --- spanner_operations ---

    // 6. List operations via spanner_operations
    let env = get_emulator();
    let ops_sql = format!(
        "SELECT * FROM spanner_operations(database_path := '{}', endpoint := '{}')",
        env.database_path(),
        env.emulator_host()
    );
    let mut stmt = conn.prepare(&ops_sql).unwrap();
    let rows: Vec<(String, bool)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert!(!rows.is_empty(), "should have at least one operation after DDL");
    for (name, done) in &rows {
        assert!(!name.is_empty(), "operation name should not be empty");
        assert!(done, "all operations should be done");
    }

    // --- Error case ---

    // 7. Invalid DDL should produce an error
    let sql = vtab_ddl_sql("THIS IS NOT VALID DDL");
    let result = conn.query_row(&sql, [], |r| r.get::<_, String>(0));
    assert!(result.is_err(), "invalid DDL should fail");
}

// ═══════════════════════════════════════════════════════════════════════════
// PostgreSQL dialect infrastructure
// ═══════════════════════════════════════════════════════════════════════════

/// Start a PG dialect Spanner emulator (once) with test tables and seed data.
fn get_pg_emulator() -> &'static spanemuboost::SpanEmuBoost {
    static PG_EMU: OnceLock<spanemuboost::SpanEmuBoost> = OnceLock::new();
    PG_EMU.get_or_init(|| {
        test_runtime().block_on(async {
            spanemuboost::SpanEmuBoost::builder()
                .database_id("pg-database")
                .database_dialect(DatabaseDialect::Postgresql)
                .setup_ddls(vec![
                    "CREATE TABLE users (\
                        id bigint NOT NULL, \
                        name character varying(256), \
                        age bigint, \
                        PRIMARY KEY (id)\
                    )"
                    .into(),
                    "CREATE TABLE pgsql_types (\
                        id bigint NOT NULL, \
                        bool_col boolean, \
                        int_col bigint, \
                        float4_col real, \
                        float8_col double precision, \
                        num_col numeric, \
                        str_col character varying(256), \
                        bytes_col bytea, \
                        date_col date, \
                        ts_col timestamp with time zone, \
                        json_col jsonb, \
                        PRIMARY KEY (id)\
                    )"
                    .into(),
                ])
                .setup_dmls(vec![
                    "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)".into(),
                    "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)".into(),
                    "INSERT INTO users (id, name, age) VALUES (3, 'Charlie', NULL)".into(),
                    "INSERT INTO pgsql_types (id, bool_col, int_col, float4_col, float8_col, num_col, str_col, bytes_col, date_col, ts_col, json_col) \
                     VALUES (1, true, 42, 1.5, 3.125, 123.456789, 'hello', '\\x68656c6c6f', '2024-01-15', '2024-06-15T10:30:00Z', '{\"key\": \"value\"}')"
                    .into(),
                    "INSERT INTO pgsql_types (id, bool_col, int_col, float4_col, float8_col, num_col, str_col, bytes_col, date_col, ts_col, json_col) \
                     VALUES (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
                    .into(),
                ])
                .start()
                .await
                .expect("Failed to start PG dialect emulator")
        })
    })
}

fn pg_vtab_query_sql(spanner_sql: &str) -> String {
    let env = get_pg_emulator();
    format!(
        "SELECT * FROM spanner_query('{}', database_path := '{}', endpoint := '{}')",
        spanner_sql,
        env.database_path(),
        env.emulator_host()
    )
}

fn pg_vtab_query_sql_with(spanner_sql: &str, extra_params: &str) -> String {
    let env = get_pg_emulator();
    format!(
        "SELECT * FROM spanner_query('{}', database_path := '{}', endpoint := '{}', {})",
        spanner_sql,
        env.database_path(),
        env.emulator_host(),
        extra_params
    )
}

fn pg_vtab_scan_sql(table: &str) -> String {
    let env = get_pg_emulator();
    format!(
        "SELECT * FROM spanner_scan('{}', database_path := '{}', endpoint := '{}')",
        table,
        env.database_path(),
        env.emulator_host()
    )
}

fn pg_vtab_scan_sql_with(table: &str, extra_params: &str) -> String {
    let env = get_pg_emulator();
    format!(
        "SELECT * FROM spanner_scan('{}', database_path := '{}', endpoint := '{}', {})",
        table,
        env.database_path(),
        env.emulator_host(),
        extra_params
    )
}

fn create_pg_duckdb_connection() -> Connection {
    let _ = get_pg_emulator(); // ensure PG emulator is running
    let conn = Connection::open_in_memory().unwrap();
    conn.register_table_function::<SpannerQueryVTab>("spanner_query_raw")
        .unwrap();
    conn.register_table_function::<SpannerScanVTab>("spanner_scan")
        .unwrap();
    conn.register_table_function::<SpannerDdlVTab>("spanner_ddl_raw")
        .unwrap();
    conn.register_table_function::<SpannerDdlAsyncVTab>("spanner_ddl_async_raw")
        .unwrap();
    conn.register_table_function::<SpannerOperationsVTab>("spanner_operations_raw")
        .unwrap();
    conn.execute_batch("\
        LOAD core_functions;\
        LOAD json;\
        INSTALL icu;\
        LOAD icu;\
    ").unwrap();
    conn.execute_batch(include_str!("../src/macros.sql"))
        .unwrap();
    conn
}

// ═══════════════════════════════════════════════════════════════════════════
// PostgreSQL dialect — spanner_query tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_pg_vtab_query() {
    let conn = create_pg_duckdb_connection();
    let sql = pg_vtab_query_sql("SELECT id, name, age FROM users ORDER BY id");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);

    let (id, name): (i64, String) = conn
        .query_row(
            &format!("SELECT id, name FROM ({sql}) WHERE id = 1"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(id, 1);
    assert_eq!(name, "Alice");
}

#[test]
fn test_pg_vtab_query_types() {
    let conn = create_pg_duckdb_connection();
    let sql = pg_vtab_query_sql(
        "SELECT bool_col, int_col, float4_col, float8_col, str_col FROM pgsql_types WHERE id = 1",
    );
    let (b, i, f4, f8, s): (bool, i64, f64, f64, String) = conn
        .query_row(&sql, [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?))
        })
        .unwrap();
    assert!(b);
    assert_eq!(i, 42);
    assert!((f4 - 1.5).abs() < 0.001);
    assert!((f8 - 3.125).abs() < 0.001);
    assert_eq!(s, "hello");
}

#[test]
fn test_pg_vtab_query_null() {
    let conn = create_pg_duckdb_connection();
    let sql = pg_vtab_query_sql("SELECT bool_col IS NULL AS b_null, int_col IS NULL AS i_null FROM pgsql_types WHERE id = 2");
    let (b_null, i_null): (bool, bool) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert!(b_null);
    assert!(i_null);
}

#[test]
fn test_pg_vtab_query_params() {
    let conn = create_pg_duckdb_connection();
    let sql = pg_vtab_query_sql_with(
        "SELECT id, name FROM users WHERE id = $1 AND name = $2",
        "params := {'p1': 1, 'p2': 'Alice'}",
    );
    let (id, name): (i64, String) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap();
    assert_eq!(id, 1);
    assert_eq!(name, "Alice");
}

// ═══════════════════════════════════════════════════════════════════════════
// PostgreSQL dialect — spanner_scan tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_pg_vtab_scan() {
    let conn = create_pg_duckdb_connection();
    let sql = pg_vtab_scan_sql("users");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn test_pg_vtab_scan_types() {
    let conn = create_pg_duckdb_connection();
    let base = pg_vtab_scan_sql("pgsql_types");
    let (b, i, s): (bool, i64, String) = conn
        .query_row(
            &format!("SELECT bool_col, int_col, str_col FROM ({base}) WHERE id = 1"),
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert!(b);
    assert_eq!(i, 42);
    assert_eq!(s, "hello");
}

#[test]
fn test_pg_vtab_scan_projection() {
    let conn = create_pg_duckdb_connection();
    let base = pg_vtab_scan_sql("users");
    let val: String = conn
        .query_row(
            &format!("SELECT name FROM ({base}) WHERE id = 2"),
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(val, "Bob");
}

#[test]
fn test_pg_vtab_scan_exact_staleness() {
    let conn = create_pg_duckdb_connection();
    let sql = pg_vtab_scan_sql_with("users", "exact_staleness_secs := 0");
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM ({sql})"), [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// COPY TO tests
// ═══════════════════════════════════════════════════════════════════════════

/// Create a DuckDB connection with the copy function registered via C API.
fn create_duckdb_connection_with_copy() -> Connection {
    let _ = get_emulator();
    unsafe {
        let mut db: duckdb::ffi::duckdb_database = std::ptr::null_mut();
        let mut c_err: *mut std::os::raw::c_char = std::ptr::null_mut();
        let r = duckdb::ffi::duckdb_open_ext(
            c":memory:".as_ptr(),
            &mut db,
            std::ptr::null_mut(),
            &mut c_err,
        );
        assert_eq!(r, duckdb::ffi::DuckDBSuccess, "duckdb_open_ext failed");

        // Register copy function on a raw connection
        let mut raw_con: duckdb::ffi::duckdb_connection = std::ptr::null_mut();
        let rc = duckdb::ffi::duckdb_connect(db, &mut raw_con);
        assert_eq!(rc, duckdb::ffi::DuckDBSuccess, "duckdb_connect failed");
        register_copy_function(raw_con);
        duckdb::ffi::duckdb_disconnect(&mut raw_con);

        // Wrap in Connection for table functions and macros
        let conn = Connection::open_from_raw(db).unwrap();
        conn.register_table_function::<SpannerQueryVTab>("spanner_query_raw")
            .unwrap();
        conn.register_table_function::<SpannerScanVTab>("spanner_scan")
            .unwrap();
        conn.register_table_function::<SpannerDdlVTab>("spanner_ddl_raw")
            .unwrap();
        conn.register_table_function::<SpannerDdlAsyncVTab>("spanner_ddl_async_raw")
            .unwrap();
        conn.register_table_function::<SpannerOperationsVTab>("spanner_operations_raw")
            .unwrap();
        conn.execute_batch("\
            LOAD core_functions;\
            LOAD json;\
            INSTALL icu;\
            LOAD icu;\
        ").unwrap();
        conn.execute_batch(include_str!("../src/macros.sql"))
            .unwrap();
        conn
    }
}

#[test]
fn test_copy_to_registration() {
    // Verify the copy function is registered and DuckDB can parse COPY TO spanner syntax.
    // Uses raw C API to avoid Rust exception handling issues.
    unsafe {
        let mut db: duckdb::ffi::duckdb_database = std::ptr::null_mut();
        let r = duckdb::ffi::duckdb_open(c":memory:".as_ptr(), &mut db);
        assert_eq!(r, duckdb::ffi::DuckDBSuccess);

        let mut con: duckdb::ffi::duckdb_connection = std::ptr::null_mut();
        let rc = duckdb::ffi::duckdb_connect(db, &mut con);
        assert_eq!(rc, duckdb::ffi::DuckDBSuccess);

        register_copy_function(con);

        // Try COPY with invalid database — should fail with Spanner connection error, not crash
        let sql = std::ffi::CString::new(
            "COPY (SELECT CAST(1 AS BIGINT) AS a) TO 'T' (FORMAT spanner, database_path 'projects/p/instances/i/databases/d')"
        ).unwrap();
        let mut result = std::mem::MaybeUninit::zeroed();
        let r = duckdb::ffi::duckdb_query(con, sql.as_ptr(), result.as_mut_ptr());
        let mut result = result.assume_init();
        if r != duckdb::ffi::DuckDBSuccess {
            let err = duckdb::ffi::duckdb_result_error(&mut result);
            assert!(!err.is_null(), "error message should not be null");
            let err_str = std::ffi::CStr::from_ptr(err).to_string_lossy();
            eprintln!("Expected COPY error: {err_str}");
        }
        duckdb::ffi::duckdb_destroy_result(&mut result);
        duckdb::ffi::duckdb_disconnect(&mut con);
        duckdb::ffi::duckdb_close(&mut db);
    }
}

#[test]
fn test_copy_to_basic() {
    let conn = create_duckdb_connection_with_copy();
    let env = get_emulator();

    // COPY 3 rows to CopyTarget
    let sql = format!(
        "COPY (SELECT CAST(Id AS BIGINT) AS Id, Name, CAST(Value AS DOUBLE) AS Value \
         FROM (VALUES (100, 'alice', 1.5), (101, 'bob', 2.5), (102, 'charlie', 3.5)) AS t(Id, Name, Value)) \
         TO 'CopyTarget' (FORMAT spanner, database_path '{}', endpoint '{}')",
        env.database_path(),
        env.emulator_host()
    );
    conn.execute_batch(&sql).unwrap();

    // Read back via spanner_query (filter by Id range to avoid conflicts with other tests)
    let read_sql = vtab_query_sql("SELECT Id, Name, Value FROM CopyTarget WHERE Id BETWEEN 100 AND 102 ORDER BY Id");
    let rows: Vec<(i64, String, f64)> = conn
        .prepare(&read_sql)
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], (100, "alice".to_string(), 1.5));
    assert_eq!(rows[1], (101, "bob".to_string(), 2.5));
    assert_eq!(rows[2], (102, "charlie".to_string(), 3.5));
}

#[test]
fn test_copy_to_types() {
    let conn = create_duckdb_connection_with_copy();
    let env = get_emulator();

    let sql = format!(
        "COPY (SELECT \
            CAST(1 AS BIGINT) AS Id, \
            true AS BoolCol, \
            CAST(42 AS BIGINT) AS Int64Col, \
            CAST(3.125 AS DOUBLE) AS Float64Col, \
            'hello' AS StringCol, \
            DATE '2024-01-15' AS DateCol, \
            TIMESTAMPTZ '2024-06-15T10:30:00Z' AS TimestampCol \
        ) TO 'CopyTypes' (FORMAT spanner, database_path '{}', endpoint '{}')",
        env.database_path(),
        env.emulator_host()
    );
    conn.execute_batch(&sql).unwrap();

    // Read back and verify types
    let read_sql = vtab_query_sql(
        "SELECT Id, BoolCol, Int64Col, Float64Col, StringCol, DateCol, TimestampCol FROM CopyTypes WHERE Id = 1"
    );
    let (id, b, i, f, s): (i64, bool, i64, f64, String) = conn
        .query_row(
            &read_sql,
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap();
    assert_eq!(id, 1);
    assert!(b);
    assert_eq!(i, 42);
    assert!((f - 3.125).abs() < f64::EPSILON);
    assert_eq!(s, "hello");
}

#[test]
fn test_copy_to_insert_mode() {
    let conn = create_duckdb_connection_with_copy();
    let env = get_emulator();

    // Insert with mode 'insert'
    let sql = format!(
        "COPY (SELECT CAST(200 AS BIGINT) AS Id, 'insert_test' AS Name, CAST(9.9 AS DOUBLE) AS Value) \
         TO 'CopyTarget' (FORMAT spanner, database_path '{}', endpoint '{}', mode 'insert')",
        env.database_path(),
        env.emulator_host()
    );
    conn.execute_batch(&sql).unwrap();

    let read_sql = vtab_query_sql("SELECT Name FROM CopyTarget WHERE Id = 200");
    let name: String = conn.query_row(&read_sql, [], |r| r.get(0)).unwrap();
    assert_eq!(name, "insert_test");
}

#[test]
fn test_copy_to_with_nulls() {
    let conn = create_duckdb_connection_with_copy();
    let env = get_emulator();

    let sql = format!(
        "COPY (SELECT CAST(300 AS BIGINT) AS Id, CAST(NULL AS BOOLEAN) AS BoolCol, \
            CAST(NULL AS BIGINT) AS Int64Col, CAST(NULL AS DOUBLE) AS Float64Col, \
            CAST(NULL AS VARCHAR) AS StringCol, CAST(NULL AS DATE) AS DateCol, \
            CAST(NULL AS TIMESTAMPTZ) AS TimestampCol) \
         TO 'CopyTypes' (FORMAT spanner, database_path '{}', endpoint '{}')",
        env.database_path(),
        env.emulator_host()
    );
    conn.execute_batch(&sql).unwrap();

    let read_sql = vtab_query_sql(
        "SELECT BoolCol IS NULL, Int64Col IS NULL, StringCol IS NULL FROM CopyTypes WHERE Id = 300"
    );
    let (b_null, i_null, s_null): (bool, bool, bool) = conn
        .query_row(&read_sql, [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap();
    assert!(b_null);
    assert!(i_null);
    assert!(s_null);
}

#[test]
fn test_copy_to_column_count_mismatch() {
    let conn = create_duckdb_connection_with_copy();
    let env = get_emulator();

    // CopyTarget has 3 columns (Id, Name, Value); source has 2
    let sql = format!(
        "COPY (SELECT CAST(999 AS BIGINT) AS Id, 'oops' AS Name) \
         TO 'CopyTarget' (FORMAT spanner, database_path '{}', endpoint '{}')",
        env.database_path(),
        env.emulator_host()
    );
    let result = conn.execute_batch(&sql);
    assert!(result.is_err(), "column count mismatch should fail");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Column count mismatch"),
        "error should mention column count mismatch, got: {err_msg}"
    );
}

