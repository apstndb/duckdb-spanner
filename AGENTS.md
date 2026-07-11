# duckdb-spanner

Rust DuckDB extension for querying Google Cloud Spanner databases.

## Build & Test

```sh
cargo check                    # type-check
cargo test                     # unit tests + integration tests (requires Docker)
cargo test test_name           # run a single test by name substring
cargo build --release --features loadable-extension  # build loadable extension (.so/.dylib)
```

### SQLLogicTest (extension-ci-tools)

Requires Docker (Colima on macOS) for the Spanner emulator. `make test_release` starts the emulator, seeds the test database (`tests/setup_sqllogic_db.sh`), and runs `test/sql/*.test`.

```sh
make configure                 # venv + duckdb_sqllogictest-python (once)
make release test_release      # build extension + emulator + seed + test/sql/*.test
make test                      # alias for test_release
make test_debug                # debug build + tests
```

On macOS with Colima, set `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock` if Docker is not on the default socket.

Tests live in `test/sql/` (SQLLogicTest format): `spanner_smoke.test`, `spanner_query.test`, `spanner_scan.test`, `spanner_scalars.test`, `spanner_params.test`, `spanner_tables.test`, `spanner_replacement_scan.test`, `spanner_ddl.test`, `spanner_copy.test`. The runner loads the built extension via `--external-extension build/release/spanner.duckdb_extension`. Use `require spanner` in tests to load it. Emulator tests use `SPANNER_EMULATOR_HOST` (default `localhost:9010`) and `SET allow_extensions_metadata_mismatch=true` when loading via `--external-extension`.

**Run shared-emulator tests serially.** Only one of `make test`, `make test_release`, or `make test_debug` should hit the shared Spanner emulator at a time; concurrent runs cause flaky `Session not found` errors. Rust integration tests use testcontainers-managed emulators and can run independently. For a clean slate: `make emulator-stop && make emulator-start`.

### Rust integration tests

Integration tests use testcontainers to spin up a Spanner emulator.
Docker (Colima on macOS) must be running. `RUST_TEST_THREADS=4` is set in `.cargo/config.toml`.

## Architecture

- **Table functions**: `spanner_query` (SQL), `spanner_scan` (Read API), `spanner_ddl`/`spanner_ddl_async`, `spanner_operations`, `spanner_tables`
- **Scalar functions**: `spanner_value`, `spanner_typed`, `spanner_params`, `interval_to_iso8601` (native VScalar; replaces former SQL macros)
- **COPY TO**: `FORMAT spanner` via DuckDB Copy Function C API
- **Config options**: registered via DuckDB Config Options C API (`src/config.rs`)
- **SQL macros**: `src/macros.sql` (loaded at extension init)
- Manual C API init in `src/lib.rs` (not using `#[duckdb_entrypoint_c_api]` macro because raw `duckdb_connection` is needed for config/copy registration)
- `register_spanner_extension` orchestrates prepared config/COPY/scalar C registrations, VTabs/macros, and final replacement-scan installation as one checked initialization path

## Google Cloud Rust client

`Cargo.toml` depends on the official `googleapis/google-cloud-rust` Spanner crates.
Partitioned reads and queries use the official partition APIs in `src/query.rs` and `src/scan.rs`; no local client fork is required.

The upstream crates are currently consumed from a pinned git revision. Keep all `google-cloud-*` crate revisions aligned when updating them.

### Updating the pinned revision

1. Replace the revision on every direct `google-cloud-*` dependency sourced from `googleapis/google-cloud-rust` in `Cargo.toml`; do not mix revisions.
2. Run `cargo check` to refresh `Cargo.lock`, then run `make check-google-cloud-rust`. The guard checks the direct revision alignment and the normal dependency graph for every target, rejecting `aws-lc-rs` and `aws-lc-sys` so the Windows MinGW artifact remains buildable.
3. Run `cargo fmt --check`, `cargo test --lib`, and `cargo clippy --features loadable-extension`.
4. Run `make configure release test_release` and `cargo test --test integration_test` serially because both use the Spanner emulator.
5. Before merging, verify the distribution pipeline's platform-artifact matrix, including Windows MinGW, and both emulator jobs (`SQLLogicTest` and `Rust integration tests`).

## Test infrastructure

`tests/spanemuboost/mod.rs` is a Rust port of [apstndb/spanemuboost](https://github.com/apstndb/spanemuboost).
Design separates emulator (container + instance) from database creation:

- `SpanEmuBoost` = container + instance (shared across all tests)
- `SpanEmuDatabase` = individual database (via `create_database`)

Tests use separate databases for isolation:
- `get_gsql_db()` / `get_pg_db()` -- read-only tests (pre-created tables + seed data)
- `get_gsql_ddl_db()` / `get_pg_ddl_db()` -- DDL tests (empty, no schema conflicts)
- `get_pg_copy_db()` -- PG COPY TO tests

Both GoogleSQL and PostgreSQL dialect databases coexist on the same emulator instance.

## Dialect support

Supports both GoogleSQL and PostgreSQL dialect (auto-detected from Spanner metadata).
PG table names cannot start with `pg_` prefix.
