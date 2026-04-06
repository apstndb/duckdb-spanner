# duckdb-spanner

A DuckDB extension for querying [Google Cloud Spanner](https://cloud.google.com/spanner) databases directly from DuckDB.

> **Experimental**: This extension is under active development. The API (function signatures, named parameters, configuration) may change without notice between versions. Do not depend on backward compatibility.

Both **GoogleSQL** and **PostgreSQL** dialect databases are supported transparently. Dialect detection is automatic — no configuration needed. `spanner_query` and `spanner_scan` work the same way regardless of the database dialect.

## Prerequisites

- [Rust](https://rustup.rs/) (stable)
- [DuckDB](https://duckdb.org/docs/installation/) CLI (for running the extension)
- Python 3 (for `make extension` metadata appending)
- Make
- `cargo-sweep` (recommended for cleaning stale build artifacts)

On macOS, prefer the bottled Homebrew binary:

```bash
brew install cargo-sweep
```

## Known Issues

- This project depends on a patched version of [gcloud-spanner](https://github.com/yoshidan/google-cloud-rust) via `[patch.crates-io]`.
  The upstream `RowIterator::try_recv()` discards metadata and stats when a `PartialResultSet` has empty values, which breaks `columns_metadata()` and `stats()` for `QueryMode::Plan`, empty result sets, and other scenarios.
  See [yoshidan/google-cloud-rust#428](https://github.com/yoshidan/google-cloud-rust/pull/428) for details.
  Additionally, `Client::get_session` and `RowIterator::new` are made public for concurrent partition execution.

- `database_role` ([fine-grained access control](https://cloud.google.com/spanner/docs/fgac-about)) is not yet supported as a named parameter. The upstream gcloud-spanner `SessionConfig` does not expose `creator_role` for session creation (the underlying `BatchCreateSessionsRequest.session_template` supports it, but the Rust client hardcodes it to `None`).

- Results are streamed via an internal channel. Memory usage is bounded regardless of result set size.

- `use_parallelism` enables the [partitioned API](https://cloud.google.com/spanner/docs/reads#read_data_in_parallel) (required for Data Boost). Partitions are executed concurrently, each with its own session from the pool.

## Installation

### Build from Source

Clone the repository with submodules:

```bash
git clone --recurse-submodules https://github.com/apstndb/duckdb-spanner.git
cd duckdb-spanner
```

Build the extension with metadata (recommended):

```bash
make extension
```

Or build with cargo directly (without extension metadata):

```bash
cargo build --features loadable-extension --release
```

The built extension is `spanner.duckdb_extension` (via `make extension`) or `target/release/libduckdb_spanner.dylib` (macOS) / `libduckdb_spanner.so` (Linux).

### Loading the Extension

This is an unsigned extension, so DuckDB must be started with the `-unsigned` flag:

```bash
duckdb -unsigned
```

Then load the extension:

```sql
LOAD 'path/to/spanner.duckdb_extension';
```

Or set the option programmatically before loading:

```sql
SET allow_unsigned_extensions = true;
LOAD 'path/to/spanner.duckdb_extension';
```

For a quick build-and-launch workflow:

```bash
make duckdb
```

`make duckdb` defaults the extension metadata version to your installed DuckDB CLI version. Override `DUCKDB_VERSION` explicitly if you need to target a different DuckDB release.

### Authentication

This extension uses [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials).
For local development with end-user credentials, install the [Google Cloud SDK](https://cloud.google.com/sdk) and run:

```bash
gcloud auth application-default login
```

When using the Spanner emulator, no authentication is required. Either set the `SPANNER_EMULATOR_HOST` environment variable or pass `endpoint` directly:

```bash
export SPANNER_EMULATOR_HOST=localhost:9010
```

## Getting Started

### Configure the Database

Set session-level defaults so you don't need to specify the database on every query:

```sql
SET spanner_project = 'myproj';
SET spanner_instance = 'myinst';
SET spanner_database = 'mydb';
```

Or use a full resource path:

```sql
SET spanner_database_path = 'projects/myproj/instances/myinst/databases/mydb';
```

### Run a SQL Query

```sql
SELECT * FROM spanner_query('SELECT Id, Name FROM Users WHERE Age > 20');
```

### Read a Table

```sql
SELECT * FROM spanner_scan('Users');
```

### Explicit Database Path

You can also pass the database path directly as a named parameter:

```sql
SELECT * FROM spanner_query(
    'SELECT Id, Name FROM Users',
    database_path := 'projects/myproj/instances/myinst/databases/mydb'
);

-- Or use individual components (overrides config defaults)
SELECT * FROM spanner_scan(
    'Users',
    project := 'myproj',
    instance := 'myinst',
    database := 'mydb'
);
```

### With the Spanner Emulator

```bash
export SPANNER_EMULATOR_HOST=localhost:9010
```

```sql
-- endpoint is inferred from SPANNER_EMULATOR_HOST
SET spanner_database_path = 'projects/test/instances/test/databases/test';
SELECT * FROM spanner_query('SELECT * FROM Users');

-- Or pass endpoint explicitly
SELECT * FROM spanner_query(
    'SELECT * FROM Users',
    database_path := 'projects/test/instances/test/databases/test',
    endpoint := 'localhost:9010'
);
```

## Table Functions

### `spanner_query`

Runs arbitrary Spanner SQL. Implemented as a [table macro](https://duckdb.org/docs/sql/statements/create_macro.html#table-macros) that wraps `spanner_query_raw`, accepting a STRUCT for `params` and automatically converting it to JSON.

```sql
SELECT * FROM spanner_query(
    'SELECT * FROM Users WHERE Age > @min_age',
    params := {'min_age': spanner_value(21::BIGINT)},
    exact_staleness_secs := 10,
    use_data_boost := true
);
```

### `spanner_scan`

Reads all rows from a table using the Spanner Read API.

```sql
SELECT * FROM spanner_scan('Users', index := 'UsersByName');
```

### Config Options

Session-level defaults can be set via `SET` statements. These are used when the corresponding named parameter is not specified.

| Config Option | Type | Description |
|---------------|------|-------------|
| `spanner_project` | VARCHAR | Default Google Cloud project ID |
| `spanner_instance` | VARCHAR | Default Spanner instance ID |
| `spanner_database` | VARCHAR | Default Spanner database ID |
| `spanner_database_path` | VARCHAR | Default full database resource path (`projects/P/instances/I/databases/D`) |
| `spanner_endpoint` | VARCHAR | Default gRPC endpoint |

### Database Resolution

The database is resolved in the following order (first match wins):

1. **`project`/`instance`/`database` named args** (mixed with config fallback) — if any component is specified via named arg or config, all three must resolve or an error is raised
2. **`database_path` named arg** — full resource path
3. **`spanner_database_path` config** — session-level default

Named args override config values for each individual component. For example:

```sql
SET spanner_project = 'myproj';
SET spanner_instance = 'myinst';
SET spanner_database = 'default_db';

-- Uses myproj/myinst/default_db
SELECT * FROM spanner_query('SELECT 1');

-- Overrides just the database component
SELECT * FROM spanner_query('SELECT 1', database := 'other_db');
```

### Named Parameters

Named parameters are inspired by BigQuery's [`EXTERNAL_QUERY`](https://cloud.google.com/bigquery/docs/reference/standard-sql/federated_query_functions#external_query) and [`CloudSpannerProperties`](https://cloud.google.com/bigquery/docs/reference/bigqueryconnection/rest/v1/projects.locations.connections#CloudSpannerProperties).

Both functions accept the following named parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database_path` | VARCHAR | (config) | Full database resource path (`projects/P/instances/I/databases/D`) |
| `project` | VARCHAR | (config) | Google Cloud project ID |
| `instance` | VARCHAR | (config) | Spanner instance ID |
| `database` | VARCHAR | (config) | Spanner database ID |
| `endpoint` | VARCHAR | (config) | Custom gRPC endpoint (e.g., `localhost:9010` for the emulator) |
| `use_parallelism` | BOOLEAN | `false` | Use partitioned query/read for parallel execution |
| `use_data_boost` | BOOLEAN | `false` | Enable [Data Boost](https://cloud.google.com/spanner/docs/databoost/databoost-overview) |
| `max_parallelism` | INTEGER | (default) | Maximum number of partitions |
| `exact_staleness_secs` | BIGINT | | Read at an exact staleness (seconds ago) |
| `max_staleness_secs` | BIGINT | | Read with bounded staleness (at most N seconds ago) |
| `read_timestamp` | VARCHAR | | Read at a specific timestamp ([RFC 3339](https://datatracker.ietf.org/doc/html/rfc3339), e.g., `'2024-01-15T10:30:00Z'`) |
| `min_read_timestamp` | VARCHAR | | Read at a timestamp no earlier than this ([RFC 3339](https://datatracker.ietf.org/doc/html/rfc3339)) |
| `priority` | VARCHAR | | Request priority: `'low'`, `'medium'`, or `'high'` |

At most one timestamp bound parameter can be specified. If none is set, Spanner uses a [strong read](https://cloud.google.com/spanner/docs/timestamp-bounds#strong) (the default).

`spanner_query` additionally accepts:

| Parameter | Type | Description |
|-----------|------|-------------|
| `params` | STRUCT | Query parameters (see [Query Parameters](#query-parameters)) |

`spanner_scan` additionally accepts:

| Parameter | Type | Description |
|-----------|------|-------------|
| `index` | VARCHAR | Secondary index name to use for the read |
| `dialect` | VARCHAR | Database dialect: `'googlesql'` or `'postgresql'` (auto-detected if omitted) |

### `spanner_ddl`

Executes a DDL statement synchronously and waits for completion.

```sql
SELECT * FROM spanner_ddl('CREATE TABLE Users (Id INT64 NOT NULL, Name STRING(MAX)) PRIMARY KEY (Id)');
```

Returns one row with `operation_name` (VARCHAR), `done` (BOOLEAN), and `duration_secs` (DOUBLE).

### `spanner_ddl_async`

Submits a DDL statement and returns immediately without waiting for completion.

```sql
SELECT * FROM spanner_ddl_async('CREATE INDEX UsersByName ON Users(Name)');
```

Returns one row with `operation_name` (VARCHAR) and `done` (BOOLEAN).

### `spanner_operations`

Lists DDL operations for a database via the `google.longrunning.Operations` API.

```sql
SELECT * FROM spanner_operations();
```

Returns rows with `operation_name` (VARCHAR), `done` (BOOLEAN), `metadata_type` (VARCHAR), and `error` (VARCHAR).

An optional `filter` parameter can be used:

```sql
SELECT * FROM spanner_operations(filter := 'done=true');
```

All DDL functions accept the same database identification parameters as `spanner_query` (`database_path`, `project`, `instance`, `database`, `endpoint`).

### `COPY TO` (Write to Spanner)

Write data to Spanner tables using the `COPY` statement with `FORMAT spanner`.

```sql
COPY my_table TO 'SpannerTable' (FORMAT spanner);
```

Options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `database_path` | VARCHAR | (config) | Full database resource path |
| `project` | VARCHAR | (config) | Google Cloud project ID |
| `instance` | VARCHAR | (config) | Spanner instance ID |
| `database` | VARCHAR | (config) | Spanner database ID |
| `endpoint` | VARCHAR | (config) | Custom gRPC endpoint |
| `mode` | VARCHAR | `insert_or_update` | Mutation mode: `insert`, `update`, `insert_or_update`, or `replace` |
| `batch_size` | VARCHAR | `1000` | Number of rows per commit |

The file path argument specifies the target Spanner table name. Source columns are mapped to Spanner columns by position (column count must match).

```sql
-- Write query results to a Spanner table
COPY (SELECT Id, Name, Age FROM source_data) TO 'Users' (FORMAT spanner);

-- Specify mutation mode
COPY my_table TO 'Users' (FORMAT spanner, mode 'insert');

-- With explicit database path
COPY my_table TO 'Users' (
    FORMAT spanner,
    database_path 'projects/p/instances/i/databases/d'
);
```

## Query Parameters

The `params` parameter accepts a STRUCT mapping parameter names to values. Two helper macros format values for Spanner type compatibility.

### `spanner_value(val)` -- Auto-Detect Type

Infers the Spanner type from DuckDB's `typeof()` and converts the value to a Spanner-compatible format.

```sql
spanner_value(42::BIGINT)                           -- {"value":42,"type":"INT64"}
spanner_value('2024-01-15'::DATE)                   -- {"value":"2024-01-15","type":"DATE"}
spanner_value('\xDEAD'::BLOB)                       -- {"value":"3kFE","type":"BYTES"}
spanner_value('2024-06-15T10:30:00Z'::TIMESTAMPTZ)  -- {"value":"2024-06-15T10:30:00.000000Z","type":"TIMESTAMP"}
spanner_value(NULL::BIGINT)                         -- {"value":null,"type":"INT64"}
```

See [DuckDB to Spanner type mapping](#duckdb-to-spanner-query-parameters) for the full conversion table.

### `spanner_typed(val, type_name)` -- Explicit Type

For cases where auto-detection is insufficient, specify the Spanner type name directly.

```sql
spanner_typed(NULL, 'INT64')    -- {"value":null,"type":"INT64"}
spanner_typed('abc', 'STRING')  -- {"value":"abc","type":"STRING"}
```

### Plain Values

Values without `spanner_value()` or `spanner_typed()` wrappers are passed as raw JSON. Types are inferred from the JSON representation (number, string, boolean, null).

```sql
-- Mix plain and typed values
params := {'id': 1, 'name': spanner_value('Alice')}
```

## Type Mapping

### DuckDB to Spanner (Query Parameters)

`spanner_value()` maps DuckDB types to Spanner types. Values are serialized as JSON internally:

| DuckDB Type | Spanner Type | JSON Value |
|-------------|-------------|------------|
| BOOLEAN | BOOL | boolean |
| TINYINT, SMALLINT, INTEGER, BIGINT | INT64 | number |
| FLOAT | FLOAT32 | number |
| DOUBLE | FLOAT64 | number |
| VARCHAR | STRING | string |
| DATE | DATE | string |
| TIMESTAMP | TIMESTAMP | string (RFC 3339: `%Y-%m-%dT%H:%M:%S.%fZ`) |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP | string (normalized to UTC, RFC 3339) |
| BLOB | BYTES | string (base64 encoded) |
| HUGEINT, UBIGINT, DECIMAL | NUMERIC | string (preserves precision) |
| UUID | UUID | string |
| INTERVAL | INTERVAL | string (ISO 8601 duration) |
| JSON | JSON | any JSON value |
| TIME | STRING | string |
| BIT | BYTES | string (base64 encoded) |
| T[] | ARRAY\<T\> | JSON array (elements follow scalar conversion rules) |

`spanner_value()` also supports array types:

```sql
spanner_value([1, 2, 3])             -- {"value":[1,2,3],"type":"ARRAY<INT64>"}
spanner_value(['a', 'b'])            -- {"value":["a","b"],"type":"ARRAY<STRING>"}
spanner_value([true, false])         -- {"value":[true,false],"type":"ARRAY<BOOL>"}
```

STRUCT parameters are not supported.

### Spanner to DuckDB (Query Results)

| Spanner Type | DuckDB Type |
|-------------|-------------|
| BOOL | BOOLEAN |
| INT64 | BIGINT |
| FLOAT32 | FLOAT |
| FLOAT64 | DOUBLE |
| NUMERIC | DECIMAL(38,9) |
| STRING | VARCHAR |
| JSON | VARCHAR (aliased as JSON) |
| BYTES | BLOB |
| DATE | DATE |
| TIMESTAMP | TIMESTAMP WITH TIME ZONE |
| UUID | UUID |
| INTERVAL | INTERVAL |
| ARRAY\<T\> | LIST(T) |
| STRUCT\<...\> | STRUCT(...) |
| PROTO | BLOB |
| ENUM | BIGINT |

## Testing

### Shell Tests

Shell tests run SQL queries against a DuckDB CLI with the extension loaded:

```bash
make test
```

### Integration Tests

Integration tests require a Spanner emulator:

```bash
# Start the emulator
make emulator-start

# Run integration tests (uses testcontainers, but the emulator must be available)
cargo test
```

### Cleaning Stale Build Artifacts

This repository includes `cargo-sweep`-based cleanup targets for large debug builds:

```bash
# Build the debug loadable extension first, then sweep artifacts older than 3 days
make build-sweep

# Preview artifacts older than 3 days
make sweep-dry-run

# Delete artifacts older than 3 days
make sweep
```

Override the retention window with `SWEEP_DAYS`:

```bash
make sweep SWEEP_DAYS=7
```

## Advanced

### `spanner_query_raw` -- Low-Level Table Function

The underlying table function that `spanner_query` wraps. Use this when:

- You have a pre-built JSON string for `params` (e.g., from application code)
- Table macro expansion causes issues in your client library or tooling

```sql
SELECT * FROM spanner_query_raw(
    'SELECT * FROM Users WHERE Age > @min_age',
    database_path := 'projects/myproj/instances/myinst/databases/mydb',
    params := '{"min_age": {"value": 21, "type": "INT64"}}'
);
```

The `params` parameter is a VARCHAR containing a JSON object. Use `spanner_params()` to build it from a STRUCT:

```sql
SELECT * FROM spanner_query_raw(
    'SELECT * FROM Users WHERE Age > @min_age',
    params := spanner_params({'min_age': spanner_value(21::BIGINT)})
);
```

### Registered Names

This extension registers the following names into the global DuckDB namespace.

**Table functions and macros:**

| Name | Kind | Description |
|------|------|-------------|
| `spanner_query` | table macro | Wraps `spanner_query_raw` with ergonomic params (see [Table Functions](#table-functions)) |
| `spanner_query_raw` | table function | Execute Spanner SQL (see [Low-Level Table Function](#spanner_query_raw----low-level-table-function)) |
| `spanner_scan` | table function | Read a Spanner table (see [Table Functions](#table-functions)) |
| `spanner_ddl` | table macro | Execute DDL synchronously (see [`spanner_ddl`](#spanner_ddl)) |
| `spanner_ddl_async` | table macro | Submit DDL asynchronously (see [`spanner_ddl_async`](#spanner_ddl_async)) |
| `spanner_operations` | table macro | List DDL operations (see [`spanner_operations`](#spanner_operations)) |
| `spanner` | copy function | Write to Spanner via `COPY TO` (see [`COPY TO`](#copy-to-write-to-spanner)) |
| `spanner_value(val)` | scalar macro | Auto-detect Spanner type from DuckDB type (see [Query Parameters](#query-parameters)) |
| `spanner_typed(val, type_name)` | scalar macro | Explicit Spanner type wrapper (see [Query Parameters](#query-parameters)) |
| `spanner_params(s)` | scalar macro | Convert a STRUCT to a JSON params string for `spanner_query_raw` |
| `interval_to_iso8601(i)` | scalar macro | Convert a DuckDB INTERVAL to an ISO 8601 duration string (e.g., `'P1Y3M'`, `'PT2H30M'`) |
| `_spanner_type_name(t)` | scalar macro | Internal: map a DuckDB type name (scalar or array) to a Spanner type name |
| `_spanner_scalar_type_name(t)` | scalar macro | Internal: scalar-only type name mapping used by `_spanner_type_name` |

Names prefixed with `_` are internal implementation details and may change without notice.

**Config options** (see [Config Options](#config-options)):

| Name | Type | Description |
|------|------|-------------|
| `spanner_project` | VARCHAR | Default Google Cloud project ID |
| `spanner_instance` | VARCHAR | Default Spanner instance ID |
| `spanner_database` | VARCHAR | Default Spanner database ID |
| `spanner_database_path` | VARCHAR | Default full database resource path |
| `spanner_endpoint` | VARCHAR | Default gRPC endpoint |

### Convenience Macro Pattern

With config options, convenience macros are often unnecessary. Just set your defaults once:

```sql
SET spanner_project = 'myproj';
SET spanner_instance = 'myinst';
SET spanner_database = 'mydb';
SET spanner_endpoint = 'localhost:9010';

SELECT * FROM spanner_query('SELECT * FROM Users');
SELECT * FROM spanner_query(
    'SELECT * FROM Users WHERE Age > @min',
    params := {'min': spanner_value(21::BIGINT)}
);
```

If you need a reusable macro that overrides specific config values:

```sql
CREATE MACRO my_query(
    sql, params := NULL, use_parallelism := NULL,
    use_data_boost := NULL, max_parallelism := NULL,
    exact_staleness_secs := NULL, max_staleness_secs := NULL,
    read_timestamp := NULL, min_read_timestamp := NULL,
    priority := NULL
) AS TABLE
SELECT * FROM spanner_query(
    sql,
    database_path := 'projects/myproj/instances/myinst/databases/mydb',
    endpoint := 'localhost:9010',
    params := params,
    use_parallelism := use_parallelism,
    use_data_boost := use_data_boost,
    max_parallelism := max_parallelism,
    exact_staleness_secs := exact_staleness_secs,
    max_staleness_secs := max_staleness_secs,
    read_timestamp := read_timestamp,
    min_read_timestamp := min_read_timestamp,
    priority := priority
);
```
