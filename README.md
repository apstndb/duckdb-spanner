# duckdb-spanner

A DuckDB extension for querying [Google Cloud Spanner](https://cloud.google.com/spanner) databases directly from DuckDB.

## Prerequisites

- [Rust](https://rustup.rs/) (stable)
- [DuckDB](https://duckdb.org/docs/installation/) CLI (for running the extension)
- Python 3 (for `make extension` metadata appending)
- Make

## Known Issues

- This project depends on a patched version of [gcloud-spanner](https://github.com/yoshidan/google-cloud-rust) via `[patch.crates-io]`.
  The upstream `RowIterator::try_recv()` discards metadata and stats when a `PartialResultSet` has empty values, which breaks `columns_metadata()` and `stats()` for `QueryMode::Plan`, empty result sets, and other scenarios.
  See [yoshidan/google-cloud-rust#428](https://github.com/yoshidan/google-cloud-rust/pull/428) for details.

- `database_role` ([fine-grained access control](https://cloud.google.com/spanner/docs/fgac-about)) is not yet supported as a named parameter. The upstream gcloud-spanner `SessionConfig` does not expose `creator_role` for session creation (the underlying `BatchCreateSessionsRequest.session_template` supports it, but the Rust client hardcodes it to `None`).

- Results are fully buffered in memory. For very large result sets, use Spanner-side `LIMIT` or `WHERE` clauses to control result size.

- `use_parallelism` enables the [partitioned API](https://cloud.google.com/spanner/docs/reads#read_data_in_parallel) (required for Data Boost), but partitions are currently executed sequentially. True concurrent partition execution is planned for a future release.

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

### Run a SQL Query

```sql
SELECT * FROM spanner_query(
    'projects/myproj/instances/myinst/databases/mydb',
    'SELECT Id, Name FROM Users WHERE Age > 20'
);
```

### Read a Table

```sql
SELECT * FROM spanner_scan(
    'projects/myproj/instances/myinst/databases/mydb',
    'Users'
);
```

### With the Spanner Emulator

```bash
export SPANNER_EMULATOR_HOST=localhost:9010
```

```sql
-- endpoint is inferred from SPANNER_EMULATOR_HOST
SELECT * FROM spanner_query(
    'projects/test/instances/test/databases/test',
    'SELECT * FROM Users'
);

-- Or pass endpoint explicitly
SELECT * FROM spanner_query(
    'projects/test/instances/test/databases/test',
    'SELECT * FROM Users',
    endpoint := 'localhost:9010'
);
```

## Table Functions

### `spanner_query`

Runs arbitrary Spanner SQL. Implemented as a [table macro](https://duckdb.org/docs/sql/statements/create_macro.html#table-macros) that wraps `spanner_query_raw`, accepting a STRUCT for `params` and automatically converting it to JSON.

```sql
SELECT * FROM spanner_query(
    'projects/myproj/instances/myinst/databases/mydb',
    'SELECT * FROM Users WHERE Age > @min_age',
    params := {'min_age': spanner_value(21::BIGINT)},
    exact_staleness_secs := 10,
    use_data_boost := true
);
```

### `spanner_scan`

Reads all rows from a table using the Spanner Read API.

```sql
SELECT * FROM spanner_scan(
    'projects/myproj/instances/myinst/databases/mydb',
    'Users',
    index := 'UsersByName'
);
```

### Named Parameters

Named parameters are inspired by BigQuery's [`EXTERNAL_QUERY`](https://cloud.google.com/bigquery/docs/reference/standard-sql/federated_query_functions#external_query) and [`CloudSpannerProperties`](https://cloud.google.com/bigquery/docs/reference/bigqueryconnection/rest/v1/projects.locations.connections#CloudSpannerProperties).

Both functions accept the following named parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `endpoint` | VARCHAR | (production) | Custom gRPC endpoint (e.g., `localhost:9010` for the emulator) |
| `use_parallelism` | BOOLEAN | `true` | Use partitioned query/read for parallel execution |
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

## Advanced

### `spanner_query_raw` -- Low-Level Table Function

The underlying table function that `spanner_query` wraps. Use this when:

- You have a pre-built JSON string for `params` (e.g., from application code)
- Table macro expansion causes issues in your client library or tooling

```sql
SELECT * FROM spanner_query_raw(
    'projects/myproj/instances/myinst/databases/mydb',
    'SELECT * FROM Users WHERE Age > @min_age',
    params := '{"min_age": {"value": 21, "type": "INT64"}}'
);
```

The `params` parameter is a VARCHAR containing a JSON object. Use `spanner_params()` to build it from a STRUCT:

```sql
SELECT * FROM spanner_query_raw(
    'projects/myproj/instances/myinst/databases/mydb',
    'SELECT * FROM Users WHERE Age > @min_age',
    params := spanner_params({'min_age': spanner_value(21::BIGINT)})
);
```

### Registered Names

This extension registers the following names into the global DuckDB namespace.

| Name | Kind | Description |
|------|------|-------------|
| `spanner_query` | table macro | Wraps `spanner_query_raw` with ergonomic params (see [Table Functions](#table-functions)) |
| `spanner_query_raw` | table function | Execute Spanner SQL (see [Low-Level Table Function](#spanner_query_raw----low-level-table-function)) |
| `spanner_scan` | table function | Read a Spanner table (see [Table Functions](#table-functions)) |
| `spanner_value(val)` | scalar macro | Auto-detect Spanner type from DuckDB type (see [Query Parameters](#query-parameters)) |
| `spanner_typed(val, type_name)` | scalar macro | Explicit Spanner type wrapper (see [Query Parameters](#query-parameters)) |
| `spanner_params(s)` | scalar macro | Convert a STRUCT to a JSON params string for `spanner_query_raw` |
| `interval_to_iso8601(i)` | scalar macro | Convert a DuckDB INTERVAL to an ISO 8601 duration string (e.g., `'P1Y3M'`, `'PT2H30M'`) |
| `_spanner_type_name(t)` | scalar macro | Internal: map a DuckDB type name (scalar or array) to a Spanner type name |
| `_spanner_scalar_type_name(t)` | scalar macro | Internal: scalar-only type name mapping used by `_spanner_type_name` |

Names prefixed with `_` are internal implementation details and may change without notice.

### Convenience Macro Pattern

You can define a convenience macro that hardcodes your database and endpoint:

```sql
CREATE MACRO my_query(
    sql, params := NULL, use_parallelism := NULL,
    use_data_boost := NULL, max_parallelism := NULL,
    exact_staleness_secs := NULL, max_staleness_secs := NULL,
    read_timestamp := NULL, min_read_timestamp := NULL,
    priority := NULL
) AS TABLE
SELECT * FROM spanner_query(
    'projects/myproj/instances/myinst/databases/mydb',
    sql,
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

-- Usage
SELECT * FROM my_query('SELECT * FROM Users');
SELECT * FROM my_query(
    'SELECT * FROM Users WHERE Age > @min',
    params := {'min': spanner_value(21::BIGINT)}
);
```
